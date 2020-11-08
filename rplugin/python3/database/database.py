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
from .sql_language_server_config import switch_database_connection as lsp_switch_database_connection
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
    resize,
)
from .query_window import (
    open_query_window,
    close_query_window,
    get_query,
    is_query_window_opened,
)
from .nvim import (
    call_function,
    async_call,
    confirm,
    get_input,
    set_cursor,
    render,
)


class Mode(Enum):
    CONNECTION = 1
    DATABASE = 2
    TABLE = 3
    INFO_RESULT = 4
    TABLE_CONTENT_RESULT = 5
    QUERY_RESULT = 6


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
    order: Optional[Tuple[str, str]]


state: State = State(mode=Mode.CONNECTION,
                     connections=list(),
                     selected_connection=None,
                     databases=list(),
                     selected_database=None,
                     tables=list(),
                     selected_table=None,
                     result=None,
                     filter_pattern=None,
                     filter_column=None,
                     filter_condition=None,
                     order=None)


async def _show_result(settings: Settings, headers: list, rows: list) -> None:
    window = await async_call(partial(open_database_window, settings))

    await async_call(partial(render, window, ascii_table(headers, rows)))
    await async_call(partial(set_cursor, window, (4, 0)))


def _get_table_datas_from_state() -> Tuple[list, list, int]:
    table_datas = []
    selected_index = 0
    for index, table in enumerate(state.tables):
        table_datas.append([table])
        if table == state.selected_table:
            selected_index = index

    return (["Table"], table_datas, selected_index)


def _get_database_datas_from_state() -> Tuple[list, list, int]:
    database_datas = []
    selected_index = 0
    for index, database in enumerate(state.databases):
        if state.selected_database == database:
            database_datas.append([database + " (*)"])
            selected_index = index
        else:
            database_datas.append([database])

    return (["Database"], database_datas, selected_index)


def _get_connection_datas_from_state() -> Tuple[list, list, int]:
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

    return (["Name", "Type", "Host", "Port", "Username", "Password", "Database"], connection_datas, selected_index)


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
                                 (primary_key, "\'" + primary_key_value + "\'"))

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


async def _show_table_info(settings: Settings, table: str) -> None:

    def get_table_info():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.describe_table(state.selected_database, table)

    table_info = await run_in_executor(get_table_info)
    if table_info is None:
        return

    state.mode = Mode.INFO_RESULT
    await _show_result(settings, table_info[0], table_info[1:])


async def _describe_table(settings: Settings) -> None:
    table_index = await async_call(_get_table_index)
    if table_index is None:
        return

    table = state.tables[table_index]
    state.selected_table = table
    await _show_table_info(settings, table)


async def show_table_content(settings: Settings, table: str) -> None:
    if state.selected_connection is None:
        log.info("[vim-database] No connection found")
        return

    if state.selected_database is None:
        log.info("[vim-database] No database found")
        return

    await _show_table_content(settings, table)


async def _show_table_content(settings: Settings, table: str) -> None:

    def get_table_content():
        sql_client = SqlClientFactory.create(state.selected_connection)
        query = "SELECT *" if state.filter_column is None else "SELECT " + state.filter_column
        query = query + " FROM " + table
        if state.filter_condition is not None:
            query = query + " WHERE " + state.filter_condition
        if state.order is not None:
            order_column, order_orientation = state.order
            query = query + " ORDER BY " + order_column + " " + order_orientation
        query = query + " LIMIT " + str(settings.results_limit)
        return sql_client.run_query(state.selected_database, query)

    table_content = await run_in_executor(get_table_content)
    if table_content is None:
        # Error
        state.filter_condition = None
        state.filter_column = None
        state.order = None
        return

    table_empty = len(table_content) == 0
    headers = [table] if table_empty else table_content[0]
    rows = [] if table_empty else table_content[1:]
    state.selected_table = table
    state.result = (headers, rows)
    state.mode = Mode.TABLE_CONTENT_RESULT
    await _show_result(settings, headers, rows)


async def _select_connection(settings: Settings) -> None:
    if state.mode != Mode.CONNECTION or len(state.connections) == 0:
        return

    connection_index = await async_call(_get_connection_index)
    if connection_index is None:
        return

    state.selected_connection = state.connections[connection_index]
    state.selected_database = state.selected_connection.database

    # Update connections table
    window = await async_call(partial(open_database_window, settings))
    connection_headers, connection_rows, selected_index = _get_connection_datas_from_state()
    await async_call(partial(render, window, ascii_table(connection_headers, connection_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def _select_database(settings: Settings) -> None:
    if state.mode != Mode.DATABASE or len(state.databases) == 0:
        return

    database_index = await async_call(_get_database_index)
    if database_index is None:
        return

    state.selected_database = state.databases[database_index]

    # Update databases table
    window = await async_call(partial(open_database_window, settings))
    database_headers, database_rows, selected_index = _get_database_datas_from_state()
    await async_call(partial(render, window, ascii_table(database_headers, database_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))

    await show_databases(settings)


async def _select_table(settings: Settings) -> None:
    state.filter_condition = None
    state.filter_column = None
    state.order = None
    table_index = await async_call(_get_table_index)
    if table_index is None:
        return
    table = state.tables[table_index]

    await _show_table_content(settings, table)


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


async def toggle(settings: Settings) -> None:
    is_window_open = await async_call(is_database_window_open)
    if is_window_open:
        await async_call(close_database_window)
        return

    if state.mode == Mode.DATABASE:
        await show_databases(settings)
    elif state.mode == Mode.TABLE:
        await show_tables(settings)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _show_table_content(settings, state.selected_table)
    elif state.mode == Mode.INFO_RESULT:
        await _show_table_info(settings, state.selected_table)
    else:
        # Fallback
        await show_connections(settings)


async def show_connections(settings: Settings) -> None:
    window = await async_call(partial(open_database_window, settings))

    state.mode = Mode.CONNECTION

    def _get_connections() -> list:
        return list(get_connections())

    state.connections = await run_in_executor(_get_connections)
    if state.selected_connection is None:
        state.selected_connection = get_default_connection()

    connection_headers, connection_rows, selected_index = _get_connection_datas_from_state()
    await async_call(partial(render, window, ascii_table(connection_headers, connection_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


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

    def _get_databases():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.get_databases()

    state.databases = await run_in_executor(_get_databases)

    database_headers, database_rows, selected_index = _get_database_datas_from_state()
    await async_call(partial(render, window, ascii_table(database_headers, database_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def list_tables_fzf(settings: Settings) -> None:
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

    def _get_tables():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return list(
            filter(lambda table: state.filter_pattern is None or re.search(state.filter_pattern, table),
                   sql_client.get_tables(state.selected_database)))

    tables = await run_in_executor(_get_tables)

    await async_call(partial(call_function, "VimDatabaseSelectTables", tables))


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

    def _get_tables():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return list(
            filter(lambda table: state.filter_pattern is None or re.search(state.filter_pattern, table),
                   sql_client.get_tables(state.selected_database)))

    state.tables = await run_in_executor(_get_tables)
    table_headers, table_rows, selected_index = _get_table_datas_from_state()
    await async_call(partial(render, window, ascii_table(table_headers, table_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def quit(settings: Settings) -> None:
    await async_call(close_database_window)


async def new(settings: Settings) -> None:
    if state.mode == Mode.CONNECTION:
        await new_connection(settings)


async def show_insert_query(settings: Settings) -> None:
    if state.mode == Mode.TABLE_CONTENT_RESULT:

        def get_template_insert_query():
            sql_client = SqlClientFactory.create(state.selected_connection)
            return sql_client.get_template_insert_query(state.selected_database, state.selected_table)

        insert_query = await run_in_executor(get_template_insert_query)
        if insert_query is None:
            return

        query_window = await async_call(partial(open_query_window, settings))
        await async_call(partial(render, query_window, insert_query))
    elif state.mode == Mode.TABLE:
        create_table_query = ["CREATE TABLE table_name (", "\t", ")"]
        query_window = await async_call(partial(open_query_window, settings))
        await async_call(partial(render, query_window, create_table_query))


async def show_update_query(settings: Settings) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    if state.filter_column is not None:
        log.info("[vim-database] Can not show update query in filter column mode")
        return
    result_headers, result_rows = state.result
    if len(result_headers) <= 1:
        return

    result_index = await async_call(_get_result_index)
    if result_index is None:
        return
    result_row = result_rows[result_index]

    def get_primary_key() -> Optional[str]:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.get_primary_key(state.selected_database, state.selected_table)

    primary_key = await run_in_executor(get_primary_key)
    if primary_key is None:
        log.info("[vim-database] No primary key found for table " + state.selected_table)
        return

    primary_key_index = -1
    for header_index, header in enumerate(result_headers):
        if header == primary_key:
            primary_key_index = header_index
            break

    update_query = ["UPDATE " + state.selected_table + " SET "]
    num_columns = len(result_headers)
    for i in range(num_columns):
        column = result_headers[i]
        column_value = result_row[i]
        if column != primary_key:
            update_query.append("\t" + column + " = \'" + column_value + "\',")
            first_update_column = True
    update_query[-1] = update_query[-1][:-1]
    update_query.append("WHERE " + primary_key + " = \'" + result_row[primary_key_index] + "\'")
    query_window = await async_call(partial(open_query_window, settings))
    await async_call(partial(render, query_window, update_query))


async def show_copy_query(settings: Settings) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    if state.filter_column is not None:
        log.info("[vim-database] Can not show copy query in filter column mode")
        return

    result_headers, result_rows = state.result
    if len(result_headers) <= 1:
        return

    result_index = await async_call(_get_result_index)
    if result_index is None:
        return
    result_row = result_rows[result_index]

    insert_query = ["INSERT INTO " + state.selected_table + " ("]
    num_columns = len(result_headers)
    for i in range(num_columns):
        column_name = result_headers[i]
        insert_query.append("\t" + column_name)
        if i != num_columns - 1:
            insert_query[-1] += ","

    insert_query.append(") VALUES (")
    for i in range(num_columns):
        column_value = result_row[i]
        insert_query.append("\t" + (column_value if column_value == 'NULL' else ("\'" + column_value + "\'")))
        if i != num_columns - 1:
            insert_query[-1] += ","
    insert_query.append(")")

    query_window = await async_call(partial(open_query_window, settings))
    await async_call(partial(render, query_window, insert_query))


async def copy(settings: Settings) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    if state.filter_column is not None:
        log.info("[vim-database] Can not copy row in filter column mode")
        return

    result_index = await async_call(_get_result_index)
    if result_index is None:
        return
    result_headers, result_rows = state.result
    copy_row = result_rows[result_index][:]

    ans = await async_call(partial(confirm, "Do you want to copy this row?"))
    if ans == False:
        return

    header_map = dict()
    for header_index, header in enumerate(result_headers):
        header_map[header] = header_index

    def get_unique_columns() -> Optional[str]:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.get_unique_columns(state.selected_database, state.selected_table)

    unique_column_names = await run_in_executor(get_unique_columns)
    if len(unique_column_names) == 0:
        log.info("[vim-database] No unique column found")
        return

    unique_columns = []
    new_unique_column_values = []
    for unique_column in unique_column_names:
        new_unique_column_value = await async_call(partial(get_input, "New unique value " + unique_column + ": "))
        if new_unique_column_value:
            unique_columns.append((unique_column, copy_row[header_map[unique_column]]))
            new_unique_column_values.append(new_unique_column_value)
            copy_row[header_map[unique_column]] = new_unique_column_value
        else:
            return

    def _copy_row() -> bool:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.copy(state.selected_database, state.selected_table, unique_columns, new_unique_column_values)

    copy_result = await run_in_executor(_copy_row)
    if copy_result == True:
        result_rows.append(copy_row)
        state.result = (result_headers, result_rows)
        await _show_result(settings, result_headers, result_rows)


async def edit(settings: Settings) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
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
                                     (edit_column, "\'" + new_value + "\'"),
                                     (primary_key, "\'" + primary_key_value + "\'"))

        update_result = await run_in_executor(update)
        if update_result == True:
            result_rows[row][column] = new_value
            state.result = (result_headers, result_rows)
            await _show_result(settings, result_headers, result_rows)


async def info(settings: Settings) -> None:
    if state.mode == Mode.TABLE and len(state.tables) != 0:
        await _describe_table(settings)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _show_table_info(settings, state.selected_table)


async def delete(settings: Settings) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await _delete_connection(settings)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await _delete_table(settings)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _delete_result(settings)


async def select(settings: Settings) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await _select_connection(settings)
    elif state.mode == Mode.DATABASE and len(state.databases) != 0:
        await _select_database(settings)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await _select_table(settings)
    elif state.mode == Mode.INFO_RESULT:
        await _show_table_content(settings, state.selected_table)


async def new_filter(settings: Settings) -> None:
    if state.mode == Mode.TABLE:
        await _table_filter(settings)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _result_filter(settings)


async def filter_column(settings: Settings) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    def get_filter_column() -> Optional[str]:
        filter_column = state.filter_column if state.filter_column is not None else ""
        return get_input("New filter column: ", filter_column)

    filter_column = await async_call(get_filter_column)
    filter_column = filter_column if filter_column is None else filter_column.strip()
    if filter_column:
        state.filter_column = filter_column
        await _show_table_content(settings, state.selected_table)


async def sort(settings: Settings, orientation: str) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    result_index = await async_call(_get_result_row_and_column)
    if result_index is None:
        return

    result_headers, result_rows = state.result
    _, column = result_index
    order_column = result_headers[column]
    state.order = (order_column, orientation)

    await _show_table_content(settings, state.selected_table)


async def clear_filter_column(settings: Settings) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    if state.filter_column is not None:
        state.filter_column = None
        await _show_table_content(settings, state.selected_table)


async def clear_filter(settings: Settings) -> None:
    if state.mode == Mode.TABLE and state.filter_pattern is not None:
        state.filter_pattern = None
        await show_tables(settings)
    elif state.mode == Mode.TABLE_CONTENT_RESULT and state.filter_condition is not None:
        state.filter_condition = None
        await _show_table_content(settings, state.selected_table)


async def refresh(settings: Settings) -> None:
    if state.mode == Mode.DATABASE and len(state.databases) != 0:
        await show_databases(settings)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await show_tables(settings)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _show_table_content(settings, state.selected_table)
    elif state.mode == Mode.INFO_RESULT:
        await _show_table_info(settings, state.selected_table)


async def toggle_query(settings: Settings) -> None:
    is_opened = await async_call(is_query_window_opened)
    if is_opened:
        await quit_query(settings)
    else:
        await show_query(settings)


async def lsp_config(settings: Settings) -> None:
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

    await run_in_executor(partial(lsp_switch_database_connection, state.selected_connection, state.selected_database))


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
        await async_call(close_query_window)
        headers = []
        if query.lower().startswith("select "):
            headers = ["Empty"]
        else:
            headers = ["Query executed successfully"]

        await async_call(close_query_window)
        await _show_result(settings, headers, [])
        return
    state.selected_table = None
    state.result = None
    state.mode = Mode.QUERY_RESULT
    await async_call(close_query_window)
    await _show_result(settings, query_result[0], query_result[1:])


async def resize_database_window(settings: Settings, direction: int) -> None:
    await async_call(partial(resize, direction))
