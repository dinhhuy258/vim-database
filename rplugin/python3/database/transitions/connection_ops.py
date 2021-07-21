from functools import partial
from typing import Optional, Tuple

from ..concurrents.executors import run_in_executor
from ..connection import (Connection, ConnectionType, store_connection, remove_connection)
from ..utils.log import log
from ..configs.config import UserConfig
from ..states.state import Mode, State
from ..utils.ascii_table import ascii_table
from ..utils.files import is_file_exists
from ..utils.nvim import (
    async_call,
    confirm,
    get_input,
    set_cursor,
    render,
)
from ..views.database_window import (open_database_window, get_current_database_window_row, is_database_window_open)


async def new_connection(settings: UserConfig, state: State) -> None:
    connection = await async_call(_new_connection)
    if connection is not None:
        # Store the connection
        await run_in_executor(partial(store_connection, connection))

        state.connections.append(connection)
        if state.selected_connection is None:
            state.selected_connection = connection

        # Update connections table
        is_window_open = await async_call(is_database_window_open)
        if is_window_open and state.mode == Mode.CONNECTION:
            await show_connections(settings, state)
        log.info('[vim-database] Connection created')
    return None


async def show_connections(settings: UserConfig, state: State) -> None:
    window = await async_call(partial(open_database_window, settings))
    state.mode = Mode.CONNECTION

    connection_headers, connection_rows, selected_index = _get_connection_datas_from_state(state)
    await async_call(partial(render, window, ascii_table(connection_headers, connection_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def select_connection(settings: UserConfig, state: State) -> None:
    if state.mode != Mode.CONNECTION or not state.connections:
        return

    connection_index = await async_call(partial(_get_connection_index, state))
    if connection_index is None:
        return

    state.selected_connection = state.connections[connection_index]
    state.selected_database = state.selected_connection.database

    # Update connections table
    window = await async_call(partial(open_database_window, settings))
    connection_headers, connection_rows, selected_index = _get_connection_datas_from_state(state)
    await async_call(partial(render, window, ascii_table(connection_headers, connection_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def delete_connection(settings: UserConfig, state: State) -> None:
    connection_index = await async_call(partial(_get_connection_index, state))
    if connection_index is None:
        return

    connection = state.connections[connection_index]

    ans = await async_call(partial(confirm, "Do you want to delete connection " + connection.name + "?"))
    if not ans:
        return

    await run_in_executor(partial(remove_connection, connection))

    del state.connections[connection_index]
    if connection.name == state.selected_connection.name:
        state.load_default_connection()

    # Update connections table
    await show_connections(settings, state)


def _get_connection_datas_from_state(state: State) -> Tuple[list, list, int]:
    connection_datas = []
    selected_index = 0
    for index, connection in enumerate(state.connections):
        if state.selected_connection.name == connection.name:
            selected_index = index
        connection_datas.append([
            connection.name + " (*)" if state.selected_connection.name == connection.name else connection.name,
            connection.connection_type.to_string(), "" if connection.host is None else connection.host,
            "" if connection.port is None else connection.port,
            "" if connection.username is None else connection.username,
            "" if connection.password is None else connection.password, connection.database
        ])

    return ["Name", "Type", "Host", "Port", "Username", "Password", "Database"], connection_datas, selected_index


def _get_connection_index(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    connections_size = len(state.connections)
    # Minus 4 for header of the table
    connection_index = row - 4
    if connection_index < 0 or connection_index >= connections_size:
        return None

    return connection_index


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

    return Connection(name=name,
                      connection_type=ConnectionType.SQLITE,
                      host=None,
                      port=None,
                      username=None,
                      password=None,
                      database=file)


def _new_mysql_or_postgresql_connection(connection_type: ConnectionType) -> Optional[Connection]:
    name = get_input("Name: ")
    if not name:
        return None
    host = get_input("Host: ")
    if not host:
        return None
    port = get_input("Port: ")
    if not port:
        return None
    username = get_input("Username: ")
    if not username:
        return None
    password = get_input("Password: ")
    if not password:
        return None
    database = get_input("Database: ")
    if not database:
        return None

    return Connection(name=name,
                      connection_type=connection_type,
                      host=host,
                      port=port,
                      username=username,
                      password=password,
                      database=database)


def _new_connection() -> Optional[Connection]:
    try:
        connection_type_value = get_input("Connection type (1: SQLite, 2: MySQL, 3: PostgreSQL): ")
        if connection_type_value:
            connection_type = ConnectionType(int(connection_type_value))
            if connection_type is ConnectionType.SQLITE:
                return _new_sqlite_connection()
            elif connection_type is ConnectionType.MYSQL:
                return _new_mysql_or_postgresql_connection(ConnectionType.MYSQL)
            elif connection_type is ConnectionType.POSTGRESQL:
                return _new_mysql_or_postgresql_connection(ConnectionType.POSTGRESQL)
    except:
        log.info('[vim-database] Invalid connection type')
    return None
