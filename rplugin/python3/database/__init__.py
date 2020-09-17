import os
from pynvim import Nvim, plugin, command, function
from asyncio import AbstractEventLoop, Lock, run_coroutine_threadsafe
from typing import Any, Awaitable, Callable, Sequence

from .nvim import init_nvim, get_global_var
from .settings import load_settings
from .logging import log, init_log
from .executor_service import ExecutorService
from .utils import create_folder_if_not_present
from .database import (
    show_connections,
    show_databases,
    show_tables,
    quit,
    delete,
    new,
    info,
    select_connection,
)


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

    @command('VDShowConnections')
    def show_connections_command(self) -> None:
        self._run(show_connections)

    @command('VDShowDatabases')
    def show_databases_command(self) -> None:
        self._run(show_databases)

    @command('VDShowTables')
    def show_tables_command(self) -> None:
        self._run(show_tables)

    @function('VimDatabase_quit')
    def quit_function(self, args: Sequence[Any]) -> None:
        self._run(quit)

    @function('VimDatabase_show_connections')
    def show_connections_function(self, args: Sequence[Any]) -> None:
        self._run(show_connections)

    @function('VimDatabase_show_databases')
    def show_databases_function(self, args: Sequence[Any]) -> None:
        self._run(show_databases)

    @function('VimDatabase_show_tables')
    def show_tables_function(self, args: Sequence[Any]) -> None:
        self._run(show_tables)

    @function('VimDatabase_select_connection')
    def select_connection_function(self, args: Sequence[Any]) -> None:
        self._run(select_connection)

    @function('VimDatabase_delete')
    def delete_function(self, args: Sequence[Any]) -> None:
        self._run(delete)

    @function('VimDatabase_new')
    def new_function(self, args: Sequence[Any]) -> None:
        self._run(new)

    @function('VimDatabase_info')
    def info_function(self, args: Sequence[Any]) -> None:
        self._run(info)
