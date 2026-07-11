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

state = RUN


async def restart_node() -> None:
    global state
    restarted = False
    while state is not SHUTDOWN:
        await shell('node', './run.sh start')
        if restarted:
            await asyncio.sleep(100)
            state = RUN
        for i in range(3 * 3600):
            await asyncio.sleep(1)
            if state is SHUTDOWN:
                break
            if not restarted and state is RESTART:
                await shell('restart', './run.sh stop')
                await asyncio.sleep(10)
                restarted = True
                break
    print('No more restarts of node')


async def upload() -> None:
    global state
    await asyncio.sleep(10)
    failed_attempts = 0
    while state is not SHUTDOWN:
        print('Starting Upload')
        params = []
        uploading = os.path.exists('library-temp-index-1')
        if state is START_SHUTDOWN:
            print('Upload will be started forcing creation of USK')
            params.append("--force-create-usk")
            if uploading:
                state = SHUTDOWN
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
                    state = RESTART
                    failed_attempts //= 2
                    for i in range(1000):
                        await asyncio.sleep(1)
                        if state is not RESTART:
                            break
            else:
                failed_attempts //= 2
        if upload_not_completed:
            await asyncio.sleep(10)
        else:
            await asyncio.sleep(30)
    print('No more uploads started')


async def download() -> None:
    await asyncio.sleep(30)
    while state is RUN:
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
        if state is RUN:
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
            if state is SHUTDOWN:
                break
    print('No more downloads started')


async def main() -> None:
    def state_controller_print_help() -> None:
        print('Running states are:')
        print('* RUN (r)')
        print('* START_SHUTDOWN (n) - will shutdown after new USK is uploaded')
        print('* SHUTDOWN (s)')
        print('Pressing return moves to the next state.')

    async def state_controller() -> None:
        state_controller_print_help()
        global state
        while True:
            c = await asyncio.to_thread(input)
            if c == 'r':
                print('Entering RUN')
                state = RUN
            elif c == 'n' or (c == '' and state is RUN):
                print('Entering START_SHUTDOWN')
                state = START_SHUTDOWN
            elif c == 's' or (c == '' and state is START_SHUTDOWN):
                print('Entering SHUTDOWN')
                state = SHUTDOWN
                break
            elif c == '' and state is SHUTDOWN:
                print('Is SHUTDOWN')
                break
            elif c == 'h' or c == '?':
                state_controller_print_help()
            elif c != '':
                print('Unknown command', c)

    input_task = asyncio.create_task(state_controller())

    async with asyncio.TaskGroup() as tg:
        tg.create_task(restart_node())
        tg.create_task(upload())
        tg.create_task(download())

    input_task.cancel()


asyncio.run(main())
