import os
from pynvim import Nvim, plugin, command, function
from asyncio import AbstractEventLoop, Lock, run_coroutine_threadsafe
from typing import Any, Awaitable, Callable, Sequence

from .states.state import init_state
from .utils.nvim import init_nvim, get_global_var
from .configs.config import load_config
from .logging import log, init_log
from .concurrents.executor_service import ExecutorService
from .utils.files import create_folder_if_not_present
from .database import (
    show_connections,
    show_databases,
    show_tables,
    toggle,
    lsp_config,
    quit,
    delete,
    new,
    copy,
    edit,
    show_update_query,
    show_copy_query,
    show_insert_query,
    info,
    select,
    new_filter,
    filter_column,
    sort,
    clear_filter_column,
    clear_filter,
    refresh,
    resize_database_window,
    show_query,
    toggle_query,
    quit_query,
    run_query,
    list_tables_fzf,
    show_table_content,
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
        self._state = init_state()
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
                    self._settings = await load_config()
                await func(self._settings, self._state, *args)

        self._submit(run())

    @command('VDToggleDatabase')
    def toggle_command(self) -> None:
        self._run(toggle)

    @command('VDToggleQuery')
    def toggle_query_command(self) -> None:
        self._run(toggle_query)

    @command('VDLSPConfig')
    def lsp_config_command(self) -> None:
        self._run(lsp_config)

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

    @function('VimDatabase_show_query')
    def show_query_function(self, args: Sequence[Any]) -> None:
        self._run(show_query)

    @function('VimDatabase_select')
    def select_function(self, args: Sequence[Any]) -> None:
        self._run(select)

    @function('VimDatabase_delete')
    def delete_function(self, args: Sequence[Any]) -> None:
        self._run(delete)

    @function('VimDatabase_new')
    def new_function(self, args: Sequence[Any]) -> None:
        self._run(new)

    @function('VimDatabase_copy')
    def copy_function(self, args: Sequence[Any]) -> None:
        self._run(copy)

    @function('VimDatabase_edit')
    def edit_function(self, args: Sequence[Any]) -> None:
        self._run(edit)

    @function('VimDatabase_show_update_query')
    def show_update_query_function(self, args: Sequence[Any]) -> None:
        self._run(show_update_query)

    @function('VimDatabase_show_copy_query')
    def show_copy_query_function(self, args: Sequence[Any]) -> None:
        self._run(show_copy_query)

    @function('VimDatabase_show_insert_query')
    def show_insert_query_function(self, args: Sequence[Any]) -> None:
        self._run(show_insert_query)

    @function('VimDatabase_info')
    def info_function(self, args: Sequence[Any]) -> None:
        self._run(info)

    @function('VimDatabase_filter')
    def filter_function(self, args: Sequence[Any]) -> None:
        self._run(new_filter)

    @function('VimDatabase_clear_filter')
    def clear_filter_function(self, args: Sequence[Any]) -> None:
        self._run(clear_filter)

    @function('VimDatabase_filter_column')
    def filter_column_function(self, args: Sequence[Any]) -> None:
        self._run(filter_column)

    @function('VimDatabase_sort')
    def sort_function(self, args: Sequence[Any]) -> None:
        self._run(sort, "ASC")

    @function('VimDatabase_sort_reverse')
    def sort_reverse_function(self, args: Sequence[Any]) -> None:
        self._run(sort, "DESC")

    @function('VimDatabase_clear_filter_column')
    def clear_filter_column_function(self, args: Sequence[Any]) -> None:
        self._run(clear_filter_column)

    @function('VimDatabase_refresh')
    def refresh_function(self, args: Sequence[Any]) -> None:
        self._run(refresh)

    @function('VimDatabase_bigger')
    def bigger_function(self, args: Sequence[Any]) -> None:
        self._run(resize_database_window, 2)

    @function('VimDatabase_smaller')
    def smaller_function(self, args: Sequence[Any]) -> None:
        self._run(resize_database_window, -2)

    @function('VimDatabase_list_tables_fzf')
    def list_tables_fzf_function(self, args: Sequence[Any]) -> None:
        self._run(list_tables_fzf)

    @function('VimDatabase_select_table_fzf')
    def select_table_fzf_table(self, args: Sequence[Any]) -> None:
        self._run(show_table_content, str(args[0]))

    @function('VimDatabaseQuery_quit')
    def quit_query_function(self, args: Sequence[Any]) -> None:
        self._run(quit_query)

    @function('VimDatabaseQuery_run_query')
    def run_query_function(self, args: Sequence[Any]) -> None:
        self._run(run_query)
