import os
from pynvim import Nvim, plugin, command
from asyncio import AbstractEventLoop, Lock, run_coroutine_threadsafe
from typing import Any, Awaitable, Callable

from .nvim import init_nvim, get_global_var
from .settings import load_settings
from .logging import log, init_log
from .executor_service import ExecutorService
from .utils import create_folder_if_not_present
from .database import new_connection, show_connections


@plugin
class DatabasePlugin(object):

    def __init__(self, nvim: Nvim) -> None:
        self._nvim = nvim
        self._lock = Lock()
        self._executor = ExecutorService()
        init_nvim(self._nvim)
        init_log(self._nvim)
        self._settings = None
        database_workspace = get_global_var("database_workspace", os.getcwd())
        os.chdir(database_workspace)

        create_folder_if_not_present(os.path.join(os.path.expanduser("~"), ".vim-database"))

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

    @command('VDNewConnection')
    def new_connection_command(self) -> None:
        self._run(new_connection)

    @command('VDShowConnections')
    def show_connections_command(self) -> None:
        self._run(show_connections)
