import asyncio
import datetime
import os
import sys
from enum import StrEnum, auto
from typing import Dict, List


class Shell(object):
    last_executions: Dict[str, List[str]] = dict()

    def __init__(self, heading: str, log: bool = False) -> None:
        self.heading = heading
        self.log = log

    @classmethod
    def print_last_executions(cls):
        for key, val in cls.last_executions.items():
            print(key + ':')
            for line in val:
                print('   ', line)

    async def _proc_stream(self,
                           variant: str, stream: asyncio.StreamReader | None) \
            -> None:
        started = datetime.datetime.now()
        last_lines: List[str] = list()
        heading = self.heading + ' ' + variant
        while stream:
            line = (await stream.readline()).decode()
            now = datetime.datetime.now()
            elapsed = ''
            delta = now - started
            if delta.days > 3:
                elapsed = f'{delta.days}d{delta.seconds // 3600}h'
            if delta.days > 0:
                elapsed = f'{delta.days * 24 + delta.seconds // 3600}h' + \
                    f'{delta.seconds % 3600 // 60:02d}m'
            elif delta.seconds >= 3600:
                elapsed = (f'{delta.seconds // 3600}h' +
                           f'{delta.seconds % 3600 // 60:02d}m')
            elif delta.seconds >= 60:
                elapsed = f'{delta.seconds // 60}m{delta.seconds % 60:02d}s'
            else:
                elapsed = f'{delta.seconds + delta.microseconds / 1000000:.2f}'
            if not line:
                print(heading, elapsed + ':', 'closed')
                break
            line = line.strip()
            last_lines.append(line)
            last_lines = last_lines[-10:]
            print(heading, elapsed + ':', line)
        if self.last_executions.get(heading):
            self.last_executions.pop(heading)
        if last_lines:
            self.last_executions[heading] = last_lines
        if self.log:
            with open('runall.log', 'a') as log_file:
                print(f'{started} - {now} {heading}:', file=log_file)
                for line in last_lines:
                    print('   ', line, file=log_file)

    async def run(self, command: str) -> None:
        async with asyncio.TaskGroup() as tg:
            print('Starting', command)
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            tg.create_task(self._proc_stream("stdout", proc.stdout))
            tg.create_task(self._proc_stream("stderr", proc.stderr))
        await proc.communicate()


class States(StrEnum):
    RESTART = auto()
    RUN = auto()
    START_SHUTDOWN = auto()
    SHUTDOWN = auto()


class RunAllScripts(object):
    """All scripts restarted.

    The general pattern for such a restart method is:
    while not States.SHUTDOWN:
        sleep
        with locks:
            if not States.RUN:
                continue
            do one iteration.
    """

    def __init__(self):
        self.state = States.RUN

        # These are semaphores, one for each purpose
        # This is also the order in which they shall always be aquired
        # should the need ever arise to hold many of them at the same time
        self.secondary_big_job = asyncio.Semaphore(1)
        self.spider_cache = asyncio.Lock()

    async def restart_node(self) -> None:
        restarted = False
        laps = 100000
        while self.state is not States.SHUTDOWN:
            await asyncio.sleep(1)
            if self.state not in [States.RUN, States.RESTART]:
                continue
            if self.state is States.RESTART and not restarted:
                await Shell('restart').run('./run.sh stop')
                await asyncio.sleep(10)
                restarted = True
            elif laps < 3 * 3600:
                laps += 1
                continue
            laps = 0
            await Shell('node').run('./run.sh start')
            if restarted:
                print('Node restarted')
                await asyncio.sleep(100)
                self.state = States.RUN
        print('No more restarts of node')

    def allow_debugger_arg(self, port: int) -> str:
        return '-agentlib:jdwp=' + ','.join(['transport=dt_socket',
                                             'server=y',
                                             'suspend=n',
                                             f'address=127.0.0.1:{port}'])

    async def upload(self) -> None:
        failed_attempts = 0
        while self.state is not States.SHUTDOWN:
            await asyncio.sleep(10)
            params = []
            uploading = os.path.exists('library-temp-index-1')
            if self.state is States.START_SHUTDOWN:
                print('Upload will be started forcing creation of USK')
                params.append("--force-create-usk")
                if uploading:
                    self.state = States.SHUTDOWN
            async with self.spider_cache:
                print('Starting Upload')
                await Shell('upload', True).run(
                    'exec nice java ' +
                    self.allow_debugger_arg(5005) + ' ' +
                    '-jar ../git/plugin-Library/dist/uploader.jar ' +
                    f'{" ".join(params)}')
            upload_not_completed = os.path.exists('library-temp-index-1')
            if uploading:
                if upload_not_completed:
                    failed_attempts += 1
                    print(f'Upload did not complete {failed_attempts}.')
                    await asyncio.sleep(60)
                    if failed_attempts > 10:
                        self.state = States.RESTART
                        failed_attempts //= 2
                        for i in range(1000):
                            await asyncio.sleep(1)
                            if self.state is not States.RESTART:
                                break
                else:
                    failed_attempts //= 2
            if upload_not_completed:
                pass
            else:
                await asyncio.sleep(10)
        print('No more uploads started')

    async def rotate(self) -> None:
        """Rotate every month on the 15th."""
        rotated_recently = False
        while self.state is not States.SHUTDOWN:
            await asyncio.sleep(40)
            if not datetime.datetime.now().day == 15:
                rotated_recently = False
                continue
            if rotated_recently:
                continue
            async with self.spider_cache:
                if self.state is not States.RUN:
                    continue
                print('Start rotating.')
                DIR_NAME = 'library-spider-pushed-data-cache'
                OLD_DIR_NAME = f'{DIR_NAME}.old'
                if os.path.exists(OLD_DIR_NAME):
                    counter = 1
                    while os.path.exists(
                            new_dir_name := f'{OLD_DIR_NAME}{counter}'):
                        counter += 1
                    print(f'Move {OLD_DIR_NAME} into {new_dir_name}')
                    os.rename(OLD_DIR_NAME, new_dir_name)
                NEW_DIR_NAME = f'{DIR_NAME}.new'
                if os.path.exists(NEW_DIR_NAME):
                    print(f'Remove the {NEW_DIR_NAME} that is in the way')
                    for file in os.scandir(NEW_DIR_NAME):
                        os.remove(file)
                    os.rmdir(NEW_DIR_NAME)
                await Shell('rotate').run(
                    f'cd {DIR_NAME} && ' +
                    'java ' +
                    self.allow_debugger_arg(5006) + ' ' +
                    '-cp ../../git/plugin-Library/dist/uploader.jar' +
                    ' freenet.library.uploader.MoveUsedPartsOfIndex' +
                    ' `cat ../library.index.lastpushed.chk`')
                print(f'Rename {DIR_NAME}, {OLD_DIR_NAME}')
                os.rename(DIR_NAME, OLD_DIR_NAME)
                print(f'Rename {NEW_DIR_NAME}, {DIR_NAME}')
                os.rename(NEW_DIR_NAME, DIR_NAME)
            await Shell('calculate size').run(
                f'( du {DIR_NAME}; ls {DIR_NAME} | wc -l )' +
                f' > {DIR_NAME}.size')
            print('Done rotating.')
            rotated_recently = True
        print('No more rotate started.')

    async def download(self) -> None:
        while self.state is not States.SHUTDOWN:
            await asyncio.sleep(30)
            async with self.secondary_big_job:
                if self.state is not States.RUN:
                    continue
                print('Starting DownloadOneEdition')
                pubkey = open('library.index.pubkey').read()
                assert pubkey.startswith('freenet:SSK')
                pubkey = 'USK' + pubkey[11:] + '/0/'
                cwd = os.getcwd()
                params = [
                    'freenet.library.uploader.DownloadOneEdition',
                    '5',
                    pubkey,
                    f'{cwd}/library-spider-pushed-data-cache',
                ]

                if self.state is States.SHUTDOWN:
                    break
                await Shell('download').run(
                    'exec nice -18 java ' +
                    self.allow_debugger_arg(5007) + ' ' +
                    '-cp ../git/plugin-Library/dist/uploader.jar ' +
                    f'{" ".join(params)}')
        print('No more downloads started')

    async def deletions(self) -> None:
        while self.state is not States.SHUTDOWN:
            await asyncio.sleep(50)
            async with self.secondary_big_job:
                if self.state is not States.RUN:
                    continue
                # Check if there are deletions left in the processed files
                async with asyncio.TaskGroup() as tg:
                    print('Checking for deletions left')
                    proc = await asyncio.create_subprocess_shell(
                        'strings library.index.filtered.* | ' +
                        'grep library.index.delete',
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE)
                    task_communicate = tg.create_task(proc.communicate())
                out, err = task_communicate.result()
                if err:
                    print('Checked for deletions stderr:', err,
                          file=sys.stderr)
                    break
                if not out.decode().strip():
                    print('No deletions queued')
                    await Shell('deletions').run(
                        'exec nice -20 java ' +
                        self.allow_debugger_arg(5008) + ' ' +
                        '-jar ../git/plugin-Library/dist/uploader.jar ' +
                        '--scan-for-deletions')
                else:
                    print('Deletions left')
        print('No more deletions started')

    def state_controller_print_help(self) -> None:
        print('Running states are:')
        print('* RUN (r)')
        print('* START_SHUTDOWN (n) - will shutdown after new USK is uploaded')
        print('* SHUTDOWN (s)')
        print('Pressing return moves to the next state.')
        print('* h or ? - this help')
        print('* l - Last lines of completed jobs')
        print()
        print(f'Current state is {self.state}')

    async def state_controller(self) -> None:
        self.state_controller_print_help()
        while True:
            c = await asyncio.to_thread(input)
            if c == 'r':
                print('Entering RUN')
                self.state = States.RUN
            elif c == 'n' or (c == '' and self.state is States.RUN):
                print('Entering START_SHUTDOWN')
                self.state = States.START_SHUTDOWN
            elif c == 's' or (c == '' and self.state is States.START_SHUTDOWN):
                print('Entering SHUTDOWN')
                self.state = States.SHUTDOWN
                break
            elif c == '' and self.state is States.SHUTDOWN:
                print('Is SHUTDOWN')
                break
            elif c == 'l':
                Shell.print_last_executions()
            elif c == 'h' or c == '?':
                print(f'Current state {self.state}')
                self.state_controller_print_help()
            elif c != '':
                print('Unknown command', c)

    async def run(self) -> None:
        input_task = asyncio.create_task(self.state_controller())

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.restart_node())
                tg.create_task(self.upload())
                tg.create_task(self.rotate())
                tg.create_task(self.download())
                tg.create_task(self.deletions())
        finally:
            input_task.cancel()
            Shell.print_last_executions()


asyncio.run(RunAllScripts().run())
