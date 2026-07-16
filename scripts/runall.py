import asyncio
import datetime
import os
import sys


async def proc_stream(heading: str, stream: asyncio.StreamReader | None) \
          -> None:
    started = datetime.datetime.now()
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
        print(heading, elapsed + ':', line)


async def shell(header: str, command: str) -> None:
    async with asyncio.TaskGroup() as tg:
        print('Starting', command)
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        tg.create_task(proc_stream(header + " stdout", proc.stdout))
        tg.create_task(proc_stream(header + " stderr", proc.stderr))
    await proc.communicate()


RESTART = object()
RUN = object()
START_SHUTDOWN = object()
SHUTDOWN = object()


class RunAllScripts(object):

    def __init__(self):
        self.state = RUN

    async def restart_node(self) -> None:
        restarted = False
        while self.state is not SHUTDOWN:
            await shell('node', './run.sh start')
            if restarted:
                await asyncio.sleep(100)
                self.state = RUN
            for i in range(3 * 3600):
                await asyncio.sleep(1)
                if self.state is SHUTDOWN:
                    break
                if not restarted and self.state is RESTART:
                    await shell('restart', './run.sh stop')
                    await asyncio.sleep(10)
                    restarted = True
                    break
        print('No more restarts of node')

    async def upload(self) -> None:
        await asyncio.sleep(10)
        failed_attempts = 0
        rotated_recently = False
        while self.state is not SHUTDOWN:
            if datetime.datetime.now().day == 15:
                if not rotated_recently:
                    # Rotate every month on the 15th
                    print('Start rotating.')
                    DIR_NAME = 'library-spider-pushed-data-cache'
                    OLD_DIR_NAME = f'{DIR_NAME}.old'
                    if os.path.exists(OLD_DIR_NAME):
                        counter = 1
                        while os.path.exists(new_dir_name := f'library-spider-pushed-data-cache.old{counter}'):
                            counter += 1
                        print(f'Move {OLD_DIR_NAME} into {new_dir_name}')
                        os.rename(OLD_DIR_NAME, new_dir_name)
                    NEW_DIR_NAME = f'{DIR_NAME}.new'
                    if os.path.exists(NEW_DIR_NAME):
                        print(f'Remove the {NEW_DIR_NAME} that is in the way')
                        for file in os.scandir(NEW_DIR_NAME):
                            os.remove(file)
                        os.rmdir(NEW_DIR_NAME)
                    await shell('rotate',
                                f'cd {DIR_NAME} && ' +
                                'java -cp ../../git/plugin-Library/dist/uploader.jar freenet.library.uploader.MoveUsedPartsOfIndex `cat ../library.index.lastpushed.chk`')
                    print(f'Rename {DIR_NAME}, {OLD_DIR_NAME}')
                    os.rename(DIR_NAME, OLD_DIR_NAME)
                    print(f'Rename {NEW_DIR_NAME}, {DIR_NAME}')
                    os.rename(NEW_DIR_NAME, DIR_NAME)
                    await shell('calculate size',
                                f'( du {DIR_NAME}; ls {DIR_NAME} | wc -l ) > {DIR_NAME}.size')
                    print('Done rotating.')
                    rotated_recently = True
            else:
                rotated_recently = False
            print('Starting Upload')
            params = []
            uploading = os.path.exists('library-temp-index-1')
            if self.state is START_SHUTDOWN:
                print('Upload will be started forcing creation of USK')
                params.append("--force-create-usk")
                if uploading:
                    self.state = SHUTDOWN
            await shell('upload',
                        'exec nice java ' +
                        '-jar ../git/plugin-Library/dist/uploader.jar ' +
                        f'{" ".join(params)}')
            upload_not_completed = os.path.exists('library-temp-index-1')
            if uploading:
                if upload_not_completed:
                    print('Upload did not complete.')
                    failed_attempts += 1
                    if failed_attempts > 10:
                        self.state = RESTART
                        failed_attempts //= 2
                        for i in range(1000):
                            await asyncio.sleep(1)
                            if self.state is not RESTART:
                                break
                else:
                    failed_attempts //= 2
            if upload_not_completed:
                await asyncio.sleep(10)
            else:
                await asyncio.sleep(30)
        print('No more uploads started')

    async def download(self) -> None:
        await asyncio.sleep(30)
        while self.state is RUN:
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
            await shell('download',
                        'exec nice -18 java ' +
                        '-cp ../git/plugin-Library/dist/uploader.jar ' +
                        f'{" ".join(params)}')
            if self.state is RUN:
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
                    print('Checked for deletions stderr:', err, file=sys.stderr)
                    break
                if not out.decode().strip():
                    print('No deletions queued')
                    await shell('deletions',
                                'exec nice -20 java' +
                                '-jar ../git/plugin-Library/dist/uploader.jar ' +
                                '--scan-for-deletions')
                else:
                    print('Deletions left')
            for i in range(1000):
                await asyncio.sleep(1)
                if self.state is SHUTDOWN:
                    break
        print('No more downloads started')

    def state_controller_print_help(self) -> None:
        print('Running states are:')
        print('* RUN (r)')
        print('* START_SHUTDOWN (n) - will shutdown after new USK is uploaded')
        print('* SHUTDOWN (s)')
        print('Pressing return moves to the next state.')

    async def state_controller(self) -> None:
        self.state_controller_print_help()
        while True:
            c = await asyncio.to_thread(input)
            if c == 'r':
                print('Entering RUN')
                self.state = RUN
            elif c == 'n' or (c == '' and self.state is RUN):
                print('Entering START_SHUTDOWN')
                self.state = START_SHUTDOWN
            elif c == 's' or (c == '' and self.state is START_SHUTDOWN):
                print('Entering SHUTDOWN')
                self.state = SHUTDOWN
                break
            elif c == '' and self.state is SHUTDOWN:
                print('Is SHUTDOWN')
                break
            elif c == 'h' or c == '?':
                self.state_controller_print_help()
            elif c != '':
                print('Unknown command', c)

    async def run(self) -> None:
        input_task = asyncio.create_task(self.state_controller())

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.restart_node())
            tg.create_task(self.upload())
            tg.create_task(self.download())

        input_task.cancel()


asyncio.run(RunAllScripts().run())
