import re
from typing import Optional, Tuple
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
    get_current_database_window_line,
    get_current_database_window_cursor,
    render,
)
from .query_window import (
    open_query_window,
    close_query_window,
    get_query,
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
    selected_table: Optional[str]
    result: Optional[Tuple[list, list]]
    filter_pattern: Optional[str]
    filter_column: Optional[str]
    filter_condition: Optional[str]


state: State = State(mode=Mode.UNKNOWN,
                     connections=list(),
                     selected_connection=None,
                     databases=list(),
                     selected_database=None,
                     tables=list(),
                     selected_table=None,
                     result=None,
                     filter_pattern=None,
                     filter_column=None,
                     filter_condition=None)


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

    return Connection(name=name,
                      connection_type=ConnectionType.SQLITE,
                      host=None,
                      port=None,
                      username=None,
                      password=None,
                      database=file)


def _new_mysql_connection() -> Optional[Connection]:
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
                      connection_type=ConnectionType.MYSQL,
                      host=host,
                      port=port,
                      username=username,
                      password=password,
                      database=database)


def _new_connection() -> Optional[Connection]:
    try:
        connection_type_value = get_input("Connection type (1: SQLite, 2: MySQL): ")
        if connection_type_value:
            connection_type = ConnectionType(int(connection_type_value))
            if connection_type is ConnectionType.SQLITE:
                return _new_sqlite_connection()
            elif connection_type is ConnectionType.MYSQL:
                return _new_mysql_connection()
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


async def _delete_result(settings: Settings) -> None:
    result_index = await async_call(_get_result_index)
    if result_index is None:
        return
    result_headers, result_rows = state.result

    def get_primary_key() -> Optional[str]:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.get_primary_key(state.selected_database, state.selected_table)

    primary_key = await run_in_executor(get_primary_key)
    if primary_key is None:
        log.info("[vim-database] No primary key found for table " + state.selected_table)
        return

    primary_key_index = -1
    header_index = 0
    for header in result_headers:
        if header == primary_key:
            primary_key_index = header_index
            break
        header_index = header_index + 1

    if primary_key_index == -1:
        log.info("[vim-database] No primary key found in result columns")
        return

    primary_key_value = result_rows[result_index][primary_key_index]

    ans = await async_call(
        partial(confirm, "DELETE FROM " + state.selected_table + " WHERE " + primary_key + " = " + primary_key_value))
    if ans == False:
        return

    def delete() -> bool:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.delete(state.selected_database, state.selected_table,
                                 (primary_key, "\"" + primary_key_value + "\""))

    delete_result = await run_in_executor(delete)
    if delete_result == True:
        del result_rows[result_index]
        state.result = (result_headers, result_rows)
        await _show_result(settings, result_headers, result_rows)


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

    state.selected_table = None
    state.result = None
    await _show_result(settings, table_info[0], table_info[1:])


async def _show_table_content(settings: Settings, table: str) -> None:

    def get_table_content():
        sql_client = SqlClientFactory.create(state.selected_connection)
        query = "SELECT *" if state.filter_column is None else "SELECT " + state.filter_column
        query = query + " FROM " + table
        if state.filter_condition is not None:
            query = query + " WHERE " + state.filter_condition
        query = query + " LIMIT " + str(settings.results_limit)
        return sql_client.run_query(state.selected_database, query)

    table_content = await run_in_executor(get_table_content)
    if table_content is None:
        # Error
        state.filter_condition = None
        state.filter_column = None
        return
    if len(table_content) == 0:
        log.info("[vim-database] No record found for table " + table)
        return

    state.selected_table = table
    state.result = (table_content[0], table_content[1:])
    await _show_result(settings, table_content[0], table_content[1:])


async def _select_connection(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION or len(state.connections) == 0:
        return

    connection_index = await async_call(_get_connection_index)
    if connection_index is None:
        return

    state.selected_connection = state.connections[connection_index]
    state.selected_database = state.selected_connection.database

    # Update connections table
    await show_connections(settings)


async def _select_database(settings: Settings) -> None:
    if state.mode != Mode.DATABASE or len(state.databases) == 0:
        return

    database_index = await async_call(_get_database_index)
    if database_index is None:
        return

    state.selected_database = state.databases[database_index]

    # Update databases table
    await show_databases(settings)


def _get_database_index() -> Optional[int]:
    row = get_current_database_window_row()
    database_size = len(state.databases)
    # Minus 4 for header of the table
    database_index = row - 4
    if database_index < 0 or database_index >= database_size:
        return None

    return database_index


def _get_connection_index() -> Optional[int]:
    row = get_current_database_window_row()
    connections_size = len(state.connections)
    # Minus 4 for header of the table
    connection_index = row - 4
    if connection_index < 0 or connection_index >= connections_size:
        return None

    return connection_index


def _get_result_index() -> Optional[int]:
    row = get_current_database_window_row()
    _, result_rows = state.result
    result_size = len(result_rows)

    # Minus 4 for header of the table
    result_index = row - 4
    if result_index < 0 or result_index >= result_size:
        return None

    return result_index


def _get_result_row_and_column() -> Optional[Tuple[int, int]]:
    row, column = get_current_database_window_cursor()
    _, result_rows = state.result
    result_size = len(result_rows)
    # Minus 4 for header of the table
    result_row = row - 4
    if result_row < 0 or result_row >= result_size:
        return None
    line = get_current_database_window_line()
    if line is None:
        return None
    if line[column] == '|':
        return None
    result_column = 0
    for i in range(column):
        if line[i] == '|':
            result_column += 1

    return (result_row, result_column - 1)


def _get_table_index() -> Optional[int]:
    row = get_current_database_window_row()
    table_size = len(state.tables)
    # Minus 4 for header of the table
    table_index = row - 4
    if table_index < 0 or table_index >= table_size:
        return None

    return table_index


async def _table_filter(settings: Settings) -> None:

    def get_filter_pattern() -> Optional[str]:
        pattern = state.filter_pattern if state.filter_pattern is not None else ""
        return get_input("New filter: ", pattern)

    filter_pattern = await async_call(get_filter_pattern)
    if filter_pattern:
        state.filter_pattern = filter_pattern
        await show_tables(settings)


async def _result_filter(settings: Settings) -> None:

    def get_filter_condition() -> Optional[str]:
        condition = state.filter_condition if state.filter_condition is not None else ""
        return get_input("New condition: ", condition)

    filter_condition = await async_call(get_filter_condition)
    filter_condition = filter_condition if filter_condition is None else filter_condition.strip()
    if filter_condition:
        state.filter_condition = filter_condition
        await _show_table_content(settings, state.selected_table)


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
    connection_headers = ["Name", "Type", "Host", "Port", "Username", "Password", "Database"]

    state.mode = Mode.CONNECTION

    def get_connection_datas():
        connections = get_connections()
        state.connections = list(connections)
        if state.selected_connection is None:
            state.selected_connection = get_default_connection()
        connection_datas = []
        for connection in state.connections:
            connection_datas.append([
                connection.name + " (*)" if state.selected_connection.name == connection.name else connection.name,
                connection.connection_type.to_string(), "" if connection.host is None else connection.host,
                "" if connection.port is None else connection.port,
                "" if connection.username is None else connection.username,
                "" if connection.password is None else connection.password, connection.database
            ])
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

    def get_tables():
        sql_client = SqlClientFactory.create(state.selected_connection)
        state.tables = list(
            filter(lambda table: state.filter_pattern is None or re.search(state.filter_pattern, table),
                   sql_client.get_tables(state.selected_database)))

        return list(map(lambda table: [table], state.tables))

    tables = await run_in_executor(get_tables)
    await async_call(partial(render, window, ascii_table(table_headers, tables)))


async def quit(settings: Settings) -> None:
    await async_call(close_database_window)


async def new(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION:
        return

    await new_connection(settings)


async def copy(settings: Settings) -> None:
    if state.mode != Mode.RESULT:
        return

    result_index = await async_call(_get_result_index)
    if result_index is None:
        return
    result_headers, result_rows = state.result

    def get_primary_key() -> Optional[str]:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.get_primary_key(state.selected_database, state.selected_table)

    primary_key = await run_in_executor(get_primary_key)
    if primary_key is None:
        log.info("[vim-database] No primary key found for table " + state.selected_table)
        return

    primary_key_index = -1
    header_index = 0
    for header in result_headers:
        if header == primary_key:
            primary_key_index = header_index
            break
        header_index = header_index + 1

    if primary_key_index == -1:
        log.info("[vim-database] No primary key found in result columns")
        return

    primary_key_value = result_rows[result_index][primary_key_index]

    ans = await async_call(partial(confirm, "Copy row: " + primary_key + " = " + primary_key_value))
    if ans == False:
        return

    new_primary_key_value = await async_call(partial(get_input, "New primary key value: "))

    if not new_primary_key_value:
        return

    def copy_row() -> bool:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.copy(state.selected_database, state.selected_table, (primary_key, primary_key_value),
                               new_primary_key_value)

    copy_result = await run_in_executor(copy_row)
    if copy_result == True:
        new_row = result_rows[result_index]
        new_row[primary_key_index] = new_primary_key_value
        result_rows.append(new_row)
        state.result = (result_headers, result_rows)
        await _show_result(settings, result_headers, result_rows)


async def edit(settings: Settings) -> None:
    if state.mode != Mode.RESULT or state.selected_table is None:
        return

    result_index = await async_call(_get_result_row_and_column)
    if result_index is None:
        return
    result_headers, result_rows = state.result
    row, column = result_index
    edit_column = result_headers[column]
    edit_value = result_rows[row][column]

    new_value = await async_call(partial(get_input, "Edit column " + edit_column + ": ", edit_value))
    if new_value and new_value != edit_value:

        def get_primary_key() -> Optional[str]:
            sql_client = SqlClientFactory.create(state.selected_connection)
            return sql_client.get_primary_key(state.selected_database, state.selected_table)

        primary_key = await run_in_executor(get_primary_key)
        if primary_key is None:
            log.info("[vim-database] No primary key found for table " + state.selected_table)
            return

        primary_key_index = -1
        header_index = 0
        for header in result_headers:
            if header == primary_key:
                primary_key_index = header_index
                break
            header_index = header_index + 1

        if primary_key_index == -1:
            log.info("[vim-database] No primary key found in result columns")
            return

        primary_key_value = result_rows[row][primary_key_index]

        ans = await async_call(
            partial(
                confirm, "UPDATE " + state.selected_table + " SET " + edit_column + " = " + new_value + " WHERE " +
                primary_key + " = " + primary_key_value))
        if ans == False:
            return

        def update() -> bool:
            sql_client = SqlClientFactory.create(state.selected_connection)
            return sql_client.update(state.selected_database, state.selected_table,
                                     (edit_column, "\"" + new_value + "\""),
                                     (primary_key, "\"" + primary_key_value + "\""))

        update_result = await run_in_executor(update)
        if update_result == True:
            result_rows[row][column] = new_value
            state.result = (result_headers, result_rows)
            await _show_result(settings, result_headers, result_rows)


async def info(settings: Settings) -> None:
    if state.mode == Mode.TABLE and len(state.tables) != 0:
        await _describe_table(settings)


async def delete(settings: Settings) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await _delete_connection(settings)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await _delete_table(settings)
    elif state.mode == Mode.RESULT and state.selected_table is not None:
        await _delete_result(settings)


async def select(settings: Settings) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await _select_connection(settings)
    elif state.mode == Mode.DATABASE and len(state.databases) != 0:
        await _select_database(settings)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        state.filter_condition = None
        state.filter_column = None
        table_index = await async_call(_get_table_index)
        if table_index is None:
            return
        table = state.tables[table_index]

        await _show_table_content(settings, table)


async def new_filter(settings: Settings) -> None:
    if state.mode == Mode.TABLE:
        await _table_filter(settings)
    elif state.mode == Mode.RESULT and state.selected_table is not None:
        await _result_filter(settings)


async def filter_column(settings: Settings) -> None:
    if state.mode != Mode.RESULT or state.selected_table is None:
        return

    def get_filter_column() -> Optional[str]:
        filter_column = state.filter_column if state.filter_column is not None else ""
        return get_input("New filter column: ", filter_column)

    filter_column = await async_call(get_filter_column)
    filter_column = filter_column if filter_column is None else filter_column.strip()
    if filter_column:
        state.filter_column = filter_column
        await _show_table_content(settings, state.selected_table)


async def clear_filter_column(settings: Settings) -> None:
    if state.mode != Mode.RESULT or state.selected_table is None:
        return

    if state.filter_column is not None:
        state.filter_column = None
        await _show_table_content(settings, state.selected_table)


async def clear_filter(settings: Settings) -> None:
    if state.mode == Mode.TABLE and state.filter_pattern is not None:
        state.filter_pattern = None
        await show_tables(settings)
    elif state.mode == Mode.RESULT and state.selected_table is not None and state.filter_condition is not None:
        state.filter_condition = None
        await _show_table_content(settings, state.selected_table)


async def show_query(settings: Settings) -> None:
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

    await async_call(partial(open_query_window, settings))


async def quit_query(settings: Settings) -> None:
    await async_call(close_query_window)


async def run_query(settings: Settings) -> None:
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

    query = await async_call(get_query)
    if query is None:
        return

    def run_sql_query():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.run_query(state.selected_database, query)

    query_result = await run_in_executor(run_sql_query)
    if query_result is None:
        return

    if len(query_result) < 2:
        log.info("[vim-database] Query executed successfully")
        return
    state.selected_table = None
    state.result = None
    await async_call(close_query_window)
    await _show_result(settings, query_result[0], query_result[1:])
