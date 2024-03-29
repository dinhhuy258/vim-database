from functools import partial
from typing import Optional, Tuple

from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..sql_clients.sql_client_factory import SqlClientFactory
from ..states.state import Mode, State
from ..utils.ascii_table import ascii_table
from ..utils.log import log
from ..utils.nvim import (
    async_call,
    set_cursor,
    render,
)
from ..views.database_window import (
    open_database_window,
    get_current_database_window_row,
)


async def show_databases(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    state.mode = Mode.DATABASE
    window = await async_call(partial(open_database_window, configs))

    state.databases = await run_in_executor(state.sql_client.get_databases)

    database_headers, database_rows, selected_index = _get_databases_from_state(state)
    await async_call(partial(render, window, ascii_table(database_headers, database_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def select_database(configs: UserConfig, state: State) -> None:
    database_idx = await async_call(partial(_get_database_index, state))
    if database_idx is None:
        return

    state.selected_database = state.databases[database_idx]
    state.sql_client = SqlClientFactory.create(state.selected_connection)

    # Update databases table
    window = await async_call(partial(open_database_window, configs))
    database_headers, database_rows, selected_index = _get_databases_from_state(state)
    await async_call(partial(render, window, ascii_table(database_headers, database_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))

    await show_databases(configs, state)


def _get_database_index(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    database_size = len(state.databases)
    # Minus 4 for header of the table
    database_index = row - 4
    if database_index < 0 or database_index >= database_size:
        return None

    return database_index


def _get_databases_from_state(state: State) -> Tuple[list, list, int]:
    databases = []
    selected_idx = 0
    for index, database in enumerate(state.databases):
        if state.selected_database == database:
            databases.append([database + " (*)"])
            selected_idx = index
        else:
            databases.append([database])

    return ["Database"], databases, selected_idx
