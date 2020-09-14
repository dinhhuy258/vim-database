from typing import Optional
from functools import partial
from dataclasses import dataclass
from enum import Enum
from .settings import Settings
from .logging import log
from .utils import is_file_exists, run_in_executor
from .ascii_table import ascii_table
from .connection import (
    Connection,
    ConnectionType,
    store_connection,
    delete_connection,
    get_connections,
    get_default_connection,
)
from .database_window import (
    open_database_window,
    close_database_window,
    is_database_window_open,
    get_current_database_window_row,
    render,
)
from .nvim import (
    async_call,
    confirm,
    get_input,
)


class Mode(Enum):
    UNKNOWN = 0
    CONNECTION = 1


@dataclass(frozen=False)
class State:
    mode: Mode
    connections: list
    selected_connection: Optional[Connection]


state: State = State(mode=Mode.UNKNOWN, connections=list(), selected_connection=None)


def _new_sqlite_connection() -> Optional[Connection]:
    name = get_input("Name: ")
    if not name:
        return None
    file = get_input("File: ")
    if not file:
        return None
    if not is_file_exists(file):
        log.info('[vim-database] File not found: ' + file)
        return None

    return Connection(name=name, connection_type=ConnectionType.SQLITE, database=file)


def _new_connection() -> Optional[Connection]:
    try:
        connection_type_value = get_input("Connection type (1: SQLite): ")
        if connection_type_value:
            connection_type = ConnectionType(int(connection_type_value))
            if connection_type is ConnectionType.SQLITE:
                return _new_sqlite_connection()
    except:
        log.info('[vim-database] Invalid connection type')
    return None


def _get_connection_index() -> Optional[int]:
    row = get_current_database_window_row()
    connections_size = len(state.connections)
    # Minus 4 for header of the table
    connection_index = row - 4
    if connection_index < 0 or connection_index >= len(state.connections):
        return None

    return connection_index


async def new_connection(settings: Settings) -> None:
    connection = await async_call(_new_connection)
    if connection is not None:
        await run_in_executor(partial(store_connection, connection))
        # Update connections table
        is_window_open = await async_call(is_database_window_open)
        if is_window_open and state.mode == Mode.CONNECTION:
            await show_connections(settings)
        log.info('[vim-database] Connection created')
    return None


async def show_connections(settings: Settings) -> None:
    window = await async_call(partial(open_database_window, settings))
    connection_headers = ["Name", "Type", "Database"]

    state.mode = Mode.CONNECTION
    connections = await run_in_executor(get_connections)
    state.connections = list(connections)

    def get_connection_datas():
        if state.selected_connection is None:
            state.selected_connection = get_default_connection()
        connection_datas = []
        for connection in state.connections:
            if state.selected_connection.name == connection.name:
                connection_datas.append(
                    [connection.name + " (*)",
                     connection.connection_type.to_string(), connection.database])
            else:
                connection_datas.append([connection.name, connection.connection_type.to_string(), connection.database])
        return connection_datas

    connection_datas = await run_in_executor(get_connection_datas)

    await async_call(partial(render, window, ascii_table(connection_headers, connection_datas)))


async def quit(settings: Settings) -> None:
    await async_call(close_database_window)


async def new(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION:
        return

    await new_connection(settings)


async def delete(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION or len(state.connections) == 0:
        return
    connection_index = await async_call(_get_connection_index)
    if connection_index is None:
        return

    connection = state.connections[connection_index]
    ans = await async_call(partial(confirm, "Do you want to delete connection " + connection.name + "?"))
    if ans == False:
        return

    await run_in_executor(partial(delete_connection, connection))
    if connection.name == state.selected_connection.name:
        state.selected_connection = None

    # Update connections table
    await show_connections(settings)


async def select_connection(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION or len(state.connections) == 0:
        return

    connection_index = await async_call(_get_connection_index)
    if connection_index is None:
        return

    state.selected_connection = state.connections[connection_index]

    # Update connections table
    await show_connections(settings)
