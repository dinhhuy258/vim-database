from typing import Optional
from functools import partial
from dataclasses import dataclass
from enum import Enum
from .settings import Settings
from .logging import log
from .utils import is_file_exists, run_in_executor
from .ascii_table import ascii_table
from .sql_client import SqlClient
from .sql_client_factory import SqlClientFactory
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
    DATABASE = 2
    TABLE = 3
    RESULT = 4


@dataclass(frozen=False)
class State:
    mode: Mode
    connections: list
    selected_connection: Optional[Connection]
    databases: list
    selected_database: Optional[str]
    tables: list


state: State = State(mode=Mode.UNKNOWN,
                     connections=list(),
                     selected_connection=None,
                     databases=list(),
                     selected_database=None,
                     tables=list())


async def _show_result(settings: Settings, headers: list, rows: list) -> None:
    state.mode = Mode.RESULT
    window = await async_call(partial(open_database_window, settings))

    await async_call(partial(render, window, ascii_table(headers, rows)))


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


async def _delete_connection(settings: Settings) -> None:
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


async def _delete_table(settings: Settings) -> None:
    table_index = await async_call(_get_table_index)
    if table_index is None:
        return

    table = state.tables[table_index]
    ans = await async_call(partial(confirm, "Do you want to delete table " + table + "?"))
    if ans == False:
        return

    def delete_table():
        sql_client = SqlClientFactory.create(state.selected_connection)
        sql_client.delete_table(state.selected_database, table)

    await run_in_executor(delete_table)

    # Update tables
    await show_tables(settings)


async def _describe_table(settings: Settings) -> None:
    table_index = await async_call(_get_table_index)
    if table_index is None:
        return

    table = state.tables[table_index]

    def get_table_info():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.describe_table(state.selected_database, table)

    table_info = await run_in_executor(get_table_info)
    if table_info is None:
        return

    await _show_result(settings, table_info[0], table_info[1:])


async def _show_table_content(settings: Settings) -> None:
    table_index = await async_call(_get_table_index)
    if table_index is None:
        return

    table = state.tables[table_index]

    def get_table_content():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.run_query(state.selected_database,
                                    "SELECT * FROM " + table + " LIMIT " + str(settings.results_limit))

    table_content = await run_in_executor(get_table_content)
    if table_content is None:
        return
    if len(table_content) == 0:
        log.info("[vim-database] No record found for table " + table)
        return

    await _show_result(settings, table_content[0], table_content[1:])


async def _select_connection(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION or len(state.connections) == 0:
        return

    connection_index = await async_call(_get_connection_index)
    if connection_index is None:
        return

    state.selected_connection = state.connections[connection_index]

    # Update connections table
    await show_connections(settings)


def _get_connection_index() -> Optional[int]:
    row = get_current_database_window_row()
    connections_size = len(state.connections)
    # Minus 4 for header of the table
    connection_index = row - 4
    if connection_index < 0 or connection_index >= len(state.connections):
        return None

    return connection_index


def _get_table_index() -> Optional[int]:
    row = get_current_database_window_row()
    table_size = len(state.tables)
    # Minus 4 for header of the table
    table_index = row - 4
    if table_index < 0 or table_index >= len(state.tables):
        return None

    return table_index


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

    def get_connection_datas():
        connections = get_connections()
        state.connections = list(connections)
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


async def show_databases(settings: Settings) -> None:
    if state.selected_connection is None:
        state.selected_connection = await run_in_executor(get_default_connection)

    if state.selected_connection is None:
        log.info("[vim-database] No connection found")
        return

    state.mode = Mode.DATABASE
    if state.selected_database is None:
        state.selected_database = state.selected_connection.database

    window = await async_call(partial(open_database_window, settings))
    database_headers = ["Database"]

    def get_database_datas():
        database_datas = []
        sql_client = SqlClientFactory.create(state.selected_connection)
        databases = sql_client.get_databases()
        state.databases = databases
        for database in databases:
            if state.selected_database == database:
                database_datas.append([database + " (*)"])
            else:
                database_datas.append([database])
        return database_datas

    database_datas = await run_in_executor(get_database_datas)

    await async_call(partial(render, window, ascii_table(database_headers, database_datas)))


async def show_tables(settings: Settings) -> None:
    if state.selected_connection is None:
        state.selected_connection = await run_in_executor(get_default_connection)

    if state.selected_connection is None:
        log.info("[vim-database] No connection found")
        return

    if state.selected_database is None:
        state.selected_database = state.selected_connection.database

    if state.selected_database is None:
        log.info("[vim-database] No database found")
        return

    state.mode = Mode.TABLE
    window = await async_call(partial(open_database_window, settings))
    table_headers = ["Table"]

    def get_table_datas():
        table_datas = []
        sql_client = SqlClientFactory.create(state.selected_connection)
        tables = sql_client.get_tables(state.selected_database)
        state.tables = tables
        for table in tables:
            table_datas.append([table])
        return table_datas

    table_datas = await run_in_executor(get_table_datas)

    await async_call(partial(render, window, ascii_table(table_headers, table_datas)))


async def quit(settings: Settings) -> None:
    await async_call(close_database_window)


async def new(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION:
        return

    await new_connection(settings)


async def info(settings: Settings) -> None:
    if state.mode == Mode.TABLE and len(state.tables) != 0:
        await _describe_table(settings)


async def delete(settings: Settings) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await _delete_connection(settings)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await _delete_table(settings)


async def select(settings: Settings) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await _select_connection(settings)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await _show_table_content(settings)
