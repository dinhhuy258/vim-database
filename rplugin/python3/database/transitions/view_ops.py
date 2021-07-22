from functools import partial

from .shared.show_table_content import show_table_content
from ..configs.config import UserConfig
from ..database import delete_result
from ..states.state import Mode, State
from ..transitions.connection_ops import new_connection, delete_connection, select_connection, show_connections
from ..transitions.database_ops import show_databases, select_database
from ..transitions.table_ops import (show_tables, show_table_info, describe_table, delete_table, select_table)
from ..utils.log import log
from ..utils.nvim import (
    async_call,)
from ..views.database_window import (
    close_database_window,
    resize,
    is_database_window_open,
)
from ..views.query_window import (
    open_query_window,
    close_query_window,
    is_query_window_opened,
)


async def close(_: UserConfig, __: State) -> None:
    await async_call(close_database_window)


async def new(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.CONNECTION:
        await new_connection(configs, state)


async def info(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.TABLE and len(state.tables) != 0:
        await describe_table(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await show_table_info(configs, state, state.selected_table)


async def delete(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await delete_connection(configs, state)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await delete_table(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await delete_result(configs, state)


async def select(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await select_connection(configs, state)
    elif state.mode == Mode.DATABASE and len(state.databases) != 0:
        await select_database(configs, state)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await select_table(configs, state)
    elif state.mode == Mode.INFO_RESULT:
        await show_table_content(configs, state, state.selected_table)


async def refresh(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.DATABASE and len(state.databases) != 0:
        await show_databases(configs, state)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await show_tables(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await show_table_content(configs, state, state.selected_table)
    elif state.mode == Mode.INFO_RESULT:
        await show_table_info(configs, state, state.selected_table)


async def toggle(configs: UserConfig, state: State) -> None:
    is_window_open = await async_call(is_database_window_open)
    if is_window_open:
        await async_call(close_database_window)
        return

    if state.mode == Mode.DATABASE:
        await show_databases(configs, state)
    elif state.mode == Mode.TABLE:
        await show_tables(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await show_table_content(configs, state, state.selected_table)
    elif state.mode == Mode.INFO_RESULT:
        await show_table_info(configs, state, state.selected_table)
    else:
        # Fallback
        await show_connections(configs, state)


async def resize_database(_: UserConfig, __: State, direction: int) -> None:
    await async_call(partial(resize, direction))


async def show_query(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    await async_call(partial(open_query_window, configs))


async def close_query(_: UserConfig, __: State) -> None:
    await async_call(close_query_window)


async def toggle_query(configs: UserConfig, state: State) -> None:
    is_opened = await async_call(is_query_window_opened)
    if is_opened:
        await close_query(configs, state)
    else:
        await show_query(configs, state)