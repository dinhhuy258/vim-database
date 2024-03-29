from functools import partial
from typing import Optional, Tuple

from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..sql_clients.sql_client_factory import SqlClientFactory
from ..states.state import Mode, State
from ..storages.connection import (Connection, ConnectionType, store_connection, remove_connection)
from ..utils.ascii_table import ascii_table
from ..utils.files import is_file_exists
from ..utils.log import log
from ..utils.nvim import (
    async_call,
    confirm,
    get_input,
    set_cursor,
    render,
)
from ..views.database_window import (open_database_window, get_current_database_window_row)


async def new_connection(settings: UserConfig, state: State) -> None:
    connection = await async_call(_new_connection)
    if connection is not None:
        # Store the connection
        await run_in_executor(partial(store_connection, connection))

        state.connections.append(connection)
        if state.selected_connection is None:
            state.selected_connection = connection
            state.selected_database = connection.database
            state.sql_client = SqlClientFactory.create(state.selected_connection)

        # Refresh connections table
        await show_connections(settings, state)

        log.info('[vim-database] Connection created')

    return None


async def show_connections(settings: UserConfig, state: State) -> None:
    window = await async_call(partial(open_database_window, settings))
    state.mode = Mode.CONNECTION

    connection_headers, connection_rows, selected_idx = _get_connections_from_state(state)
    await async_call(partial(render, window, ascii_table(connection_headers, connection_rows)))
    await async_call(partial(set_cursor, window, (selected_idx + 4, 0)))


async def select_connection(settings: UserConfig, state: State) -> None:
    if state.mode != Mode.CONNECTION or not state.connections:
        return

    connection_idx = await async_call(partial(_get_connection_idx, state))
    if connection_idx is None:
        return

    state.selected_connection = state.connections[connection_idx]
    state.selected_database = state.selected_connection.database
    state.sql_client = SqlClientFactory.create(state.selected_connection)

    # Update connections table
    window = await async_call(partial(open_database_window, settings))
    connection_headers, connection_rows, selected_idx = _get_connections_from_state(state)
    await async_call(partial(render, window, ascii_table(connection_headers, connection_rows)))
    await async_call(partial(set_cursor, window, (selected_idx + 4, 0)))


async def edit_connection(configs: UserConfig, state: State) -> None:
    connection_idx = await async_call(partial(_get_connection_idx, state))
    if connection_idx is None:
        return

    old_connection = state.connections[connection_idx]
    connection: Optional[Connection] = None
    if old_connection.connection_type is ConnectionType.SQLITE:
        connection = await async_call(partial(_new_sqlite_connection, old_connection))
    else:
        connection = await async_call(
            partial(_new_mysql_or_postgresql_connection, old_connection.connection_type, old_connection))

    if connection is None:
        return

    # Delete old connection
    await run_in_executor(partial(remove_connection, old_connection))
    del state.connections[connection_idx]

    # Store the new connection
    await run_in_executor(partial(store_connection, connection))
    state.connections.append(connection)

    if old_connection.name == state.selected_connection.name:
        state.load_default_connection()

    # Refresh connections table
    await show_connections(configs, state)

    log.info('[vim-database] Connection updated')


async def delete_connection(settings: UserConfig, state: State) -> None:
    connection_idx = await async_call(partial(_get_connection_idx, state))
    if connection_idx is None:
        return

    connection = state.connections[connection_idx]

    ans = await async_call(partial(confirm, "Do you want to delete connection " + connection.name + "?"))
    if not ans:
        return

    await run_in_executor(partial(remove_connection, connection))

    del state.connections[connection_idx]
    if connection.name == state.selected_connection.name:
        state.load_default_connection()

    # Update connections table
    await show_connections(settings, state)

    log.info('[vim-database] Connection deleted')


def _get_connections_from_state(state: State) -> Tuple[list, list, int]:
    connections = []
    selected_idx = 0
    for index, connection in enumerate(state.connections):
        if state.selected_connection.name == connection.name:
            selected_idx = index
        connections.append([
            connection.name + " (*)" if state.selected_connection.name == connection.name else connection.name,
            connection.connection_type.to_string(), "" if connection.host is None else connection.host,
            "" if connection.port is None else connection.port,
            "" if connection.username is None else connection.username,
            "" if connection.password is None else connection.password, connection.database
        ])

    return ["Name", "Type", "Host", "Port", "Username", "Password", "Database"], connections, selected_idx


def _get_connection_idx(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    connections_size = len(state.connections)
    # Minus 4 for header of the table
    connection_idx = row - 4
    if connection_idx < 0 or connection_idx >= connections_size:
        return None

    return connection_idx


def _new_sqlite_connection(connection: Optional[Connection] = None) -> Optional[Connection]:
    name = get_input("Name: ", connection.name if connection else "")
    if not name:
        return None
    file = get_input("File: ", connection.database if connection else "")
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


def _new_mysql_or_postgresql_connection(connection_type: ConnectionType,
                                        connection: Optional[Connection] = None) -> Optional[Connection]:
    name = get_input("Name: ", connection.name if connection else "")
    if not name:
        return None
    host = get_input("Host: ", connection.host if connection else "")
    if not host:
        return None
    port = get_input("Port: ", connection.port if connection else "")
    if not port:
        return None
    username = get_input("Username: ", connection.username if connection else "")
    if not username:
        return None
    password = get_input("Password: ", connection.password if connection else "")
    if not password:
        return None
    database = get_input("Database: ", connection.database if connection else "")
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
