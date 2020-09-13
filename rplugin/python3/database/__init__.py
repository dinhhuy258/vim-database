from pynvim import Nvim, plugin, command
from asyncio import AbstractEventLoop, Lock, run_coroutine_threadsafe
from typing import Any, Awaitable, Callable

from .nvim import init_nvim
from .settings import load_settings
from .logging import log, init_log
from .executor_service import ExecutorService
from .database import (open_connection)


@plugin
class DatabasePlugin(object):

    def __init__(self, nvim: Nvim) -> None:
        self._nvim = nvim
        self._lock = Lock()
        self._executor = ExecutorService()
        init_nvim(self._nvim)
        init_log(self._nvim)
        self._settings = None

    def _submit(self, coro: Awaitable[None]) -> None:
        loop: AbstractEventLoop = self._nvim.loop

        def submit() -> None:
            future = run_coroutine_threadsafe(coro, loop)

            try:
                future.result()
            except Exception as e:
                log.exception("%s", str(e))

        self._executor.run_sync(submit)

    def _run(self, func: Callable[..., Awaitable[None]], *args: Any) -> None:

        async def run() -> None:
            async with self._lock:
                if self._settings is None:
                    self._settings = await load_settings()
                await func(self._settings, *args)

        self._submit(run())

    @command('VDOpenConnection')
    def open_connection_command(self) -> None:
        self._run(open_connection)
