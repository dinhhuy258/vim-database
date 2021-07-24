import os
from asyncio import AbstractEventLoop, Lock, run_coroutine_threadsafe
from typing import Any, Awaitable, Callable, Sequence

from pynvim import Nvim, plugin, command, function

from .concurrents.executor_service import ExecutorService
from .configs.config import load_config
from .states.state import init_state, Mode
from .transitions.connection_ops import show_connections, select_connection, delete_connection, new_connection
from .transitions.database_ops import show_databases, select_database
from .transitions.query_ops import run_query, show_update_query, show_copy_query, show_insert_query
from .transitions.lsp_ops import lsp_config
from .transitions.table_ops import list_tables_fzf, describe_table, select_table, delete_table, describe_current_table, \
    show_tables, table_filter
from .transitions.data_ops import (
    copy,
    edit,
    filter_columns,
    order,
    show_table_data,
    delete_row,
    row_filter,
)
from .transitions.view_ops import (resize_database, close_query, show_query, toggle_query, close, toggle)
from .utils.files import create_folder_if_not_present
from .utils.log import log, init_log
from .utils.nvim import init_nvim, get_global_var


@plugin
class DatabasePlugin(object):

    def __init__(self, nvim: Nvim) -> None:
        self._nvim = nvim
        self._lock = Lock()
        self._executor = ExecutorService()
        init_nvim(self._nvim)
        init_log(self._nvim)
        self._configs = None
        self._state = None
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
                if self._configs is None:
                    self._configs = await load_config()
                if self._state is None:
                    self._state = await init_state()
                await func(self._configs, self._state, *args)

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
    def quit_function(self, _: Sequence[Any]) -> None:
        self._run(close)

    @function('VimDatabase_show_connections')
    def show_connections_function(self, _: Sequence[Any]) -> None:
        self._run(show_connections)

    @function('VimDatabase_show_databases')
    def show_databases_function(self, _: Sequence[Any]) -> None:
        self._run(show_databases)

    @function('VimDatabase_show_tables')
    def show_tables_function(self, _: Sequence[Any]) -> None:
        self._run(show_tables)

    @function('VimDatabase_show_query')
    def show_query_function(self, _: Sequence[Any]) -> None:
        self._run(show_query)

    @function('VimDatabase_select')
    def select_function(self, _: Sequence[Any]) -> None:
        if self._state.mode == Mode.CONNECTION and self._state.connections:
            self._run(select_connection)
        elif self._state.mode == Mode.DATABASE and self._state.databases:
            self._run(select_database)
        elif self._state.mode == Mode.TABLE and self._state.tables:
            self._run(select_table)
        elif self._state.mode == Mode.INFO_RESULT:
            self._run(show_table_data, self._state.selected_table)

    @function('VimDatabase_delete')
    def delete_function(self, _: Sequence[Any]) -> None:
        if self._state.mode == Mode.CONNECTION and self._state.connections:
            self._run(delete_connection)
        elif self._state.mode == Mode.TABLE and self._state.tables:
            self._run(delete_table)
        elif self._state.mode == Mode.TABLE_CONTENT_RESULT:
            self._run(delete_row)

    @function('VimDatabase_new')
    def new_function(self, _: Sequence[Any]) -> None:
        if self._state.mode == Mode.CONNECTION:
            self._run(new_connection)

    @function('VimDatabase_copy')
    def copy_function(self, _: Sequence[Any]) -> None:
        self._run(copy)

    @function('VimDatabase_edit')
    def edit_function(self, _: Sequence[Any]) -> None:
        self._run(edit)

    @function('VimDatabase_show_update_query')
    def show_update_query_function(self, _: Sequence[Any]) -> None:
        self._run(show_update_query)

    @function('VimDatabase_show_copy_query')
    def show_copy_query_function(self, _: Sequence[Any]) -> None:
        self._run(show_copy_query)

    @function('VimDatabase_show_insert_query')
    def show_insert_query_function(self, _: Sequence[Any]) -> None:
        self._run(show_insert_query)

    @function('VimDatabase_info')
    def info_function(self, _: Sequence[Any]) -> None:
        if self._state.mode == Mode.TABLE and self._state.tables:
            self._run(describe_current_table)
        elif self._state.mode == Mode.TABLE_CONTENT_RESULT:
            self._run(describe_table, self._state.selected_table)

    @function('VimDatabase_filter')
    def filter_function(self, _: Sequence[Any]) -> None:
        if self._state.mode == Mode.TABLE:
            self._run(table_filter)
        elif self._state.mode == Mode.TABLE_CONTENT_RESULT:
            self._run(row_filter)

    @function('VimDatabase_clear_filter')
    def clear_filter_function(self, _: Sequence[Any]) -> None:
        self._state.filtered_tables = None
        self._state.query_conditions = None
        self._state.filtered_columns.clear()
        log.info("[vim-database] All filters were cleared")

        if self._state.mode == Mode.TABLE:
            self._run(show_tables)
        elif self._state.mode == Mode.TABLE_CONTENT_RESULT:
            self._run(select_table, self._state.selected_table)

    @function('VimDatabase_filter_columns')
    def filter_columns_function(self, _: Sequence[Any]) -> None:
        self._run(filter_columns)

    @function('VimDatabase_order')
    def order_function(self, _: Sequence[Any]) -> None:
        self._run(order, "ASC")

    @function('VimDatabase_order_desc')
    def order_desc_function(self, _: Sequence[Any]) -> None:
        self._run(order, "DESC")

    @function('VimDatabase_clear_filter_column')
    def clear_filter_column_function(self, _: Sequence[Any]) -> None:
        if self._state.mode != Mode.TABLE_CONTENT_RESULT:
            return

        if self._state.filtered_columns:
            self._state.filtered_columns.clear()
            self._run(show_table_data, self._state.selected_table)

    @function('VimDatabase_refresh')
    def refresh_function(self, _: Sequence[Any]) -> None:
        if self._state.mode == Mode.DATABASE and self._state.databases:
            self._run(show_databases)
        elif self._state.mode == Mode.TABLE and self._state.tables:
            self._run(show_tables)
        elif self._state.mode == Mode.TABLE_CONTENT_RESULT:
            self._run(show_table_data, self._state.selected_table)
        elif self._state.mode == Mode.INFO_RESULT:
            self._run(describe_table, self._state.selected_table)

    @function('VimDatabase_bigger')
    def bigger_function(self, _: Sequence[Any]) -> None:
        self._run(resize_database, 2)

    @function('VimDatabase_smaller')
    def smaller_function(self, _: Sequence[Any]) -> None:
        self._run(resize_database, -2)

    @function('VimDatabase_list_tables_fzf')
    def list_tables_fzf_function(self, _: Sequence[Any]) -> None:
        self._run(list_tables_fzf)

    @function('VimDatabase_select_table_fzf')
    def select_table_fzf_table(self, args: Sequence[Any]) -> None:
        self._run(show_table_data, str(args[0]))

    @function('VimDatabaseQuery_quit')
    def quit_query_function(self, _: Sequence[Any]) -> None:
        self._run(close_query)

    @function('VimDatabaseQuery_run_query')
    def run_query_function(self, _: Sequence[Any]) -> None:
        self._run(run_query)
