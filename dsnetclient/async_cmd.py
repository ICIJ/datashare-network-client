import asyncio
import sys
from cmd import Cmd

from aioconsole import ainput
import os.path
try:
    import readline
except ImportError:
    readline = None


histfile = os.path.expanduser('~/.dsnet_history')
histfile_size = 1000


class AsyncCmd(Cmd):
    def __init__(self, completekey='tab', stdin=None, stdout=None, loop=None) -> None:
        super().__init__(completekey, stdin, stdout)
        if loop is None:
            if sys.platform == 'win32':
                self.loop = asyncio.ProactorEventLoop()
            else:
                self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

    async def async_preloop(self) -> None:
        if readline and os.path.exists(histfile):
            readline.read_history_file(histfile)

    async def async_postloop(self) -> None:
        if readline:
            readline.set_history_length(histfile_size)
            readline.write_history_file(histfile)

    async def async_postcmd(self, stop, line):
        return stop

    async def async_cmdloop(self, intro=None):
        await self.async_preloop()
        try:
            import readline
            self.old_completer = readline.get_completer()
            readline.set_completer(self.complete)
            readline.parse_and_bind(self.completekey+": complete")
        except ImportError:
            pass
        try:
            if intro is not None:
                self.intro = intro
            if self.intro:
                self.stdout.write(str(self.intro)+"\n")
            stop = None
            while not stop:
                if self.cmdqueue:
                    line = self.cmdqueue.pop(0)
                else:
                    try:
                        line = await ainput(self.prompt)
                    except EOFError:
                        line = 'EOF'

                line = self.precmd(line)
                if 'help' in line:
                    self.onecmd(line)
                else:
                    stop = await self.async_onecmd(line)
                    stop = await self.async_postcmd(stop, line)
            await self.async_postloop()
        finally:
            try:
                import readline
                readline.set_completer(self.old_completer)
            except ImportError:
                pass

    async def async_onecmd(self, line):
        cmd, arg, line = self.parseline(line)
        if not line:
            return await self.async_emptyline()
        if cmd is None:
            return self.default(line)
        self.lastcmd = line
        if line == 'EOF':
            self.lastcmd = ''
        if cmd == '':
            return self.default(line)
        else:
            try:
                func = getattr(self, 'do_' + cmd)
            except AttributeError:
                return self.default(line)
            return await func(arg)

    async def async_emptyline(self) -> bool:
        return False