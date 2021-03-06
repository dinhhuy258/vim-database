import re
from functools import partial
from typing import Optional, Tuple

from .transitions.connection_ops import show_connections, new_connection, delete_connection, select_connection
from .concurrents.executors import run_in_executor
from .utils.log import log
from .configs.config import UserConfig
from .transitions.lsp_config import switch_database_connection as lsp_switch_database_connection
from .sql_clients.sql_client_factory import SqlClientFactory
from .states.state import Mode, State
from .utils.ascii_table import ascii_table
from .utils.nvim import (
    call_function,
    async_call,
    confirm,
    get_input,
    set_cursor,
    render,
)
from .views.database_window import (
    open_database_window,
    close_database_window,
    is_database_window_open,
    get_current_database_window_row,
    get_current_database_window_line,
    get_current_database_window_cursor,
    resize,
)
from .views.query_window import (
    open_query_window,
    close_query_window,
    get_query,
    is_query_window_opened,
)


async def _show_result(configs: UserConfig, headers: list, rows: list) -> None:
    window = await async_call(partial(open_database_window, configs))

    await async_call(partial(render, window, ascii_table(headers, rows)))
    await async_call(partial(set_cursor, window, (4, 0)))


def _get_table_datas_from_state(state: State) -> Tuple[list, list, int]:
    table_datas = []
    selected_index = 0
    for index, table in enumerate(state.tables):
        table_datas.append([table])
        if table == state.selected_table:
            selected_index = index

    return ["Table"], table_datas, selected_index


def _get_database_datas_from_state(state: State) -> Tuple[list, list, int]:
    database_datas = []
    selected_index = 0
    for index, database in enumerate(state.databases):
        if state.selected_database == database:
            database_datas.append([database + " (*)"])
            selected_index = index
        else:
            database_datas.append([database])

    return ["Database"], database_datas, selected_index


async def _delete_result(configs: UserConfig, state: State) -> None:
    result_index = await async_call(partial(_get_result_index, state))
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
    if not ans:
        return

    def delete() -> bool:
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.delete(state.selected_database, state.selected_table,
                                 (primary_key, "\'" + primary_key_value + "\'"))

    delete_result = await run_in_executor(delete)
    if delete_result:
        del result_rows[result_index]
        state.result = (result_headers, result_rows)
        await _show_result(configs, result_headers, result_rows)


async def _delete_table(configs: UserConfig, state: State) -> None:
    table_index = await async_call(partial(_get_table_index, state))
    if table_index is None:
        return

    table = state.tables[table_index]
    ans = await async_call(partial(confirm, "Do you want to delete table " + table + "?"))
    if not ans:
        return

    def delete_table():
        sql_client = SqlClientFactory.create(state.selected_connection)
        sql_client.delete_table(state.selected_database, table)

    await run_in_executor(delete_table)

    # Update tables
    await show_tables(configs, state)


async def _show_table_info(configs: UserConfig, state: State, table: str) -> None:

    def get_table_info():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.describe_table(state.selected_database, table)

    table_info = await run_in_executor(get_table_info)
    if table_info is None:
        return

    state.mode = Mode.INFO_RESULT
    await _show_result(configs, table_info[0], table_info[1:])


async def _describe_table(configs: UserConfig, state: State) -> None:
    table_index = await async_call(partial(_get_table_index, state))
    if table_index is None:
        return

    table = state.tables[table_index]
    state.selected_table = table
    await _show_table_info(configs, state, table)


async def show_table_content(configs: UserConfig, state: State, table: str) -> None:
    if state.selected_connection is None:
        log.info("[vim-database] No connection found")
        return

    if state.selected_database is None:
        log.info("[vim-database] No database found")
        return

    await _show_table_content(configs, state, table)


async def _show_table_content(configs: UserConfig, state: State, table: str) -> None:

    def get_table_content():
        sql_client = SqlClientFactory.create(state.selected_connection)
        query = "SELECT *" if state.filter_column is None else "SELECT " + state.filter_column
        query = query + " FROM " + table
        if state.filter_condition is not None:
            query = query + " WHERE " + state.filter_condition
        if state.order is not None:
            order_column, order_orientation = state.order
            query = query + " ORDER BY " + order_column + " " + order_orientation
        query = query + " LIMIT " + str(configs.results_limit)
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
    await _show_result(configs, headers, rows)


async def _select_database(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.DATABASE or len(state.databases) == 0:
        return

    database_index = await async_call(_get_database_index)
    if database_index is None:
        return

    state.selected_database = state.databases[database_index]

    # Update databases table
    window = await async_call(partial(open_database_window, configs))
    database_headers, database_rows, selected_index = _get_database_datas_from_state(state)
    await async_call(partial(render, window, ascii_table(database_headers, database_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))

    await show_databases(configs, state)


async def _select_table(configs: UserConfig, state: State) -> None:
    state.filter_condition = None
    state.filter_column = None
    state.order = None
    table_index = await async_call(partial(_get_table_index, state))
    if table_index is None:
        return
    table = state.tables[table_index]

    await _show_table_content(configs, state, table)


def _get_database_index(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    database_size = len(state.databases)
    # Minus 4 for header of the table
    database_index = row - 4
    if database_index < 0 or database_index >= database_size:
        return None

    return database_index


def _get_result_index(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    _, result_rows = state.result
    result_size = len(result_rows)

    # Minus 4 for header of the table
    result_index = row - 4
    if result_index < 0 or result_index >= result_size:
        return None

    return result_index


def _get_result_row_and_column(state: State) -> Optional[Tuple[int, int]]:
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

    return result_row, result_column - 1


def _get_table_index(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    table_size = len(state.tables)
    # Minus 4 for header of the table
    table_index = row - 4
    if table_index < 0 or table_index >= table_size:
        return None

    return table_index


async def _table_filter(configs: UserConfig, state: State) -> None:

    def get_filter_pattern() -> Optional[str]:
        pattern = state.filter_pattern if state.filter_pattern is not None else ""
        return get_input("New filter: ", pattern)

    filter_pattern = await async_call(get_filter_pattern)
    if filter_pattern:
        state.filter_pattern = filter_pattern
        await show_tables(configs, state)


async def _result_filter(configs: UserConfig, state: State) -> None:

    def get_filter_condition() -> Optional[str]:
        condition = state.filter_condition if state.filter_condition is not None else ""
        return get_input("New condition: ", condition)

    filter_condition = await async_call(get_filter_condition)
    filter_condition = filter_condition if filter_condition is None else filter_condition.strip()
    if filter_condition:
        state.filter_condition = filter_condition
        await _show_table_content(configs, state, state.selected_table)


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
        await _show_table_content(configs, state, state.selected_table)
    elif state.mode == Mode.INFO_RESULT:
        await _show_table_info(configs, state, state.selected_table)
    else:
        # Fallback
        await show_connections(configs, state)


async def show_databases(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    state.mode = Mode.DATABASE
    window = await async_call(partial(open_database_window, configs))

    def _get_databases():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.get_databases()

    state.databases = await run_in_executor(_get_databases)

    database_headers, database_rows, selected_index = _get_database_datas_from_state(state)
    await async_call(partial(render, window, ascii_table(database_headers, database_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def list_tables_fzf(_: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    def _get_tables():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return sql_client.get_tables(state.selected_database)

    tables = await run_in_executor(_get_tables)

    await async_call(partial(call_function, "VimDatabaseSelectTables", tables))


async def show_tables(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    state.mode = Mode.TABLE
    window = await async_call(partial(open_database_window, configs))

    def _get_tables():
        sql_client = SqlClientFactory.create(state.selected_connection)
        return list(
            filter(lambda table: state.filter_pattern is None or re.search(state.filter_pattern, table),
                   sql_client.get_tables(state.selected_database)))

    state.tables = await run_in_executor(_get_tables)
    table_headers, table_rows, selected_index = _get_table_datas_from_state(state)
    await async_call(partial(render, window, ascii_table(table_headers, table_rows)))
    await async_call(partial(set_cursor, window, (selected_index + 4, 0)))


async def quit(_: UserConfig, __: State) -> None:
    await async_call(close_database_window)


async def new(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.CONNECTION:
        await new_connection(configs, state)


async def show_insert_query(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.TABLE_CONTENT_RESULT:

        def get_template_insert_query():
            sql_client = SqlClientFactory.create(state.selected_connection)
            return sql_client.get_template_insert_query(state.selected_database, state.selected_table)

        insert_query = await run_in_executor(get_template_insert_query)
        if insert_query is None:
            return

        query_window = await async_call(partial(open_query_window, configs))
        await async_call(partial(render, query_window, insert_query))
    elif state.mode == Mode.TABLE:
        create_table_query = ["CREATE TABLE table_name (", "\t", ")"]
        query_window = await async_call(partial(open_query_window, configs))
        await async_call(partial(render, query_window, create_table_query))


async def show_update_query(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    if state.filter_column is not None:
        log.info("[vim-database] Can not show update query in filter column mode")
        return
    result_headers, result_rows = state.result
    if len(result_headers) <= 1:
        return

    result_index = await async_call(partial(_get_result_index, state))
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
    update_query[-1] = update_query[-1][:-1]
    update_query.append("WHERE " + primary_key + " = \'" + result_row[primary_key_index] + "\'")
    query_window = await async_call(partial(open_query_window, configs))
    await async_call(partial(render, query_window, update_query))


async def show_copy_query(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    if state.filter_column is not None:
        log.info("[vim-database] Can not show copy query in filter column mode")
        return

    result_headers, result_rows = state.result
    if len(result_headers) <= 1:
        return

    result_index = await async_call(partial(_get_result_index, state))
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

    query_window = await async_call(partial(open_query_window, configs))
    await async_call(partial(render, query_window, insert_query))


async def copy(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    if state.filter_column is not None:
        log.info("[vim-database] Can not copy row in filter column mode")
        return

    result_index = await async_call(partial(_get_result_index, state))
    if result_index is None:
        return
    result_headers, result_rows = state.result
    copy_row = result_rows[result_index][:]

    ans = await async_call(partial(confirm, "Do you want to copy this row?"))
    if not ans:
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
    if copy_result:
        result_rows.append(copy_row)
        state.result = (result_headers, result_rows)
        await _show_result(configs, result_headers, result_rows)


async def edit(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    result_index = await async_call(partial(_get_result_row_and_column, state))
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
        if not ans:
            return

        def update() -> bool:
            sql_client = SqlClientFactory.create(state.selected_connection)
            return sql_client.update(state.selected_database, state.selected_table,
                                     (edit_column, "\'" + new_value + "\'"),
                                     (primary_key, "\'" + primary_key_value + "\'"))

        update_result = await run_in_executor(update)
        if update_result:
            result_rows[row][column] = new_value
            state.result = (result_headers, result_rows)
            await _show_result(configs, result_headers, result_rows)


async def info(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.TABLE and len(state.tables) != 0:
        await _describe_table(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _show_table_info(configs, state, state.selected_table)


async def delete(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await delete_connection(configs, state)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await _delete_table(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _delete_result(configs, state)


async def select(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.CONNECTION and len(state.connections) != 0:
        await select_connection(configs, state)
    elif state.mode == Mode.DATABASE and len(state.databases) != 0:
        await _select_database(configs, state)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await _select_table(configs, state)
    elif state.mode == Mode.INFO_RESULT:
        await _show_table_content(configs, state, state.selected_table)


async def new_filter(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.TABLE:
        await _table_filter(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _result_filter(configs, state)


async def filter_column(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    def get_filter_column() -> Optional[str]:
        filter_column = state.filter_column if state.filter_column is not None else ""
        return get_input("New filter column: ", filter_column)

    filter_column = await async_call(get_filter_column)
    filter_column = filter_column if filter_column is None else filter_column.strip()
    if filter_column:
        state.filter_column = filter_column
        await _show_table_content(configs, state, state.selected_table)


async def sort(configs: UserConfig, state: State, orientation: str) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    result_index = await async_call(partial(_get_result_row_and_column, state))
    if result_index is None:
        return

    result_headers, result_rows = state.result
    _, column = result_index
    order_column = result_headers[column]
    state.order = (order_column, orientation)

    await _show_table_content(configs, state, state.selected_table)


async def clear_filter_column(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    if state.filter_column is not None:
        state.filter_column = None
        await _show_table_content(configs, state, state.selected_table)


async def clear_filter(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.TABLE and state.filter_pattern is not None:
        state.filter_pattern = None
        await show_tables(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT and state.filter_condition is not None:
        state.filter_condition = None
        await _show_table_content(configs, state, state.selected_table)


async def refresh(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.DATABASE and len(state.databases) != 0:
        await show_databases(configs, state)
    elif state.mode == Mode.TABLE and len(state.tables) != 0:
        await show_tables(configs, state)
    elif state.mode == Mode.TABLE_CONTENT_RESULT:
        await _show_table_content(configs, state, state.selected_table)
    elif state.mode == Mode.INFO_RESULT:
        await _show_table_info(configs, state, state.selected_table)


async def toggle_query(configs: UserConfig, state: State) -> None:
    is_opened = await async_call(is_query_window_opened)
    if is_opened:
        await quit_query(configs, state)
    else:
        await show_query(configs, state)


async def lsp_config(_: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    await run_in_executor(partial(lsp_switch_database_connection, state.selected_connection, state.selected_database))


async def show_query(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    await async_call(partial(open_query_window, configs))


async def quit_query(_: UserConfig, __: State) -> None:
    await async_call(close_query_window)


async def run_query(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
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
        if query.lower().startswith("select "):
            headers = ["Empty"]
        else:
            headers = ["Query executed successfully"]

        await async_call(close_query_window)
        await _show_result(configs, headers, [])
        return
    state.selected_table = None
    state.result = None
    state.mode = Mode.QUERY_RESULT
    await async_call(close_query_window)
    await _show_result(configs, query_result[0], query_result[1:])


async def resize_database_window(_: UserConfig, direction: int) -> None:
    await async_call(partial(resize, direction))
