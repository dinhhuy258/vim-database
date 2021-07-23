import re
from functools import partial

from .shared.get_row_index import get_row_index
from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import Mode, State
from ..transitions.shared.show_result import show_result
from ..utils.log import log
from ..utils.nvim import (
    async_call,
    render,
)
from ..views.query_window import (
    open_query_window,
    close_query_window,
    get_query,
)


async def run_query(configs: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    query = await async_call(get_query)
    if query is None:
        return

    query_result = await run_in_executor(partial(state.sql_client.run_query, state.selected_database, query))
    if query_result is None:
        return

    await async_call(close_query_window)

    if len(query_result) < 2 and not query.lower().startswith("select "):
        log.info("[vim-database] Query executed successfully")
        return
    elif len(query_result) < 2:
        matches = re.search(r'(?<=from)(\s+\w+\b)', query, re.IGNORECASE)
        if matches is None:
            log.info("[vim-database] Can not be able to identify table name in select query")
            return
        table_name = matches.group(0).strip()
        query_result = [[table_name]]

    state.selected_table = None
    state.result = None
    state.mode = Mode.QUERY_RESULT

    await show_result(configs, query_result[0], query_result[1:])


async def show_insert_query(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.TABLE_CONTENT_RESULT:

        insert_query = await run_in_executor(partial(state.sql_client.get_template_insert_query,
                                                     state.selected_database,
                                                     state.selected_table))
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
    if state.filtered_columns is not None:
        log.info("[vim-database] Can not show update query in filter column mode")
        return
    result_headers, result_rows = state.result
    if len(result_headers) <= 1:
        return

    result_index = await async_call(partial(get_row_index, state))
    if result_index is None:
        return
    result_row = result_rows[result_index]

    primary_key = await run_in_executor(partial(state.sql_client.get_primary_key,
                                                state.selected_database,
                                                state.selected_table))
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
    if state.filtered_columns is not None:
        log.info("[vim-database] Can not show copy query in filter column mode")
        return

    result_headers, result_rows = state.result
    if len(result_headers) <= 1:
        return

    result_index = await async_call(partial(get_row_index, state))
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