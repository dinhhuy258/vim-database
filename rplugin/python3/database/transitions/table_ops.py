import re
from functools import partial
from typing import Optional, Tuple

from .shared.show_ascii_table import show_ascii_table
from .shared.show_table_data import show_table_data
from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import Mode, State
from ..utils.ascii_table import ascii_table
from ..utils.log import log
from ..utils.nvim import (
    async_call,
    confirm,
    get_input,
    set_cursor,
    render,
    call_function,
)
from ..views.database_window import (
    open_database_window,
    get_current_database_window_row,
)


async def list_tables_fzf(_: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    tables = await run_in_executor(partial(state.sql_client.get_tables, state.selected_database))

    await async_call(partial(call_function, "VimDatabaseSelectTables", tables))


async def describe_current_table(configs: UserConfig, state: State) -> None:
    table_idx = await async_call(partial(_get_table_index, state))
    if table_idx is None:
        return

    table = state.tables[table_idx]
    state.selected_table = table
    await describe_table(configs, state, table)


async def describe_table(configs: UserConfig, state: State, table: str) -> None:
    table_info = await run_in_executor(partial(state.sql_client.describe_table, state.selected_database, table))
    if table_info is None:
        return

    state.mode = Mode.INFO_RESULT
    await show_ascii_table(configs, table_info[0], table_info[1:])


async def select_table(configs: UserConfig, state: State) -> None:
    state.query_conditions = None
    state.filtered_columns.clear()
    state.order = None
    table_idx = await async_call(partial(_get_table_index, state))
    if table_idx is None:
        return
    table = state.tables[table_idx]

    await show_table_data(configs, state, table)


async def table_filter(configs: UserConfig, state: State) -> None:

    def get_filtered_tables() -> Optional[str]:
        _filtered_tables = state.filtered_tables if state.filtered_tables is not None else ""
        return get_input("New filter: ", _filtered_tables)

    filtered_tables = await async_call(get_filtered_tables)
    if filtered_tables:
        state.filtered_tables = filtered_tables.strip()
        await show_tables(configs, state)


async def show_tables(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    state.mode = Mode.TABLE
    window = await async_call(partial(open_database_window, configs))

    def _get_tables():
        return list(
            filter(lambda table: state.filtered_tables is None or re.search(state.filtered_tables, table),
                   state.sql_client.get_tables(state.selected_database)))

    state.tables = await run_in_executor(_get_tables)
    table_headers, table_rows, selected_idx = _get_tables_from_state(state)
    await async_call(partial(render, window, ascii_table(table_headers, table_rows)))
    await async_call(partial(set_cursor, window, (selected_idx + 4, 0)))


async def delete_table(configs: UserConfig, state: State) -> None:
    table_index = await async_call(partial(_get_table_index, state))
    if table_index is None:
        return

    table = state.tables[table_index]
    ans = await async_call(partial(confirm, "Do you want to delete table " + table + "?"))
    if not ans:
        return

    await run_in_executor(partial(state.sql_client.delete_table, state.selected_database, table))

    # Refresh tables
    await show_tables(configs, state)


def _get_table_index(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    table_size = len(state.tables)
    # Minus 4 for header of the table
    table_idx = row - 4
    if table_idx < 0 or table_idx >= table_size:
        return None

    return table_idx


def _get_tables_from_state(state: State) -> Tuple[list, list, int]:
    tables = []
    selected_idx = 0
    for idx, table in enumerate(state.tables):
        tables.append([table])
        if table == state.selected_table:
            selected_idx = idx

    return ["Table"], tables, selected_idx
