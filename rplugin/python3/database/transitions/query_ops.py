import re
from functools import partial

from .data_ops import show_table_data
from .database_ops import show_databases
from .shared.get_current_row_idx import get_current_row_idx
from .shared.get_primary_key_value import get_primary_key_value
from .table_ops import (show_tables)
from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import Mode, State
from ..transitions.shared.show_ascii_table import show_ascii_table
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

        if state.mode == Mode.DATABASE and state.databases:
            await show_databases(configs, state)
        elif state.mode == Mode.TABLE and state.tables:
            await show_tables(configs, state)
        elif state.mode == Mode.QUERY and not state.user_query:
            await show_table_data(configs, state, state.selected_table)
        return
    elif len(query_result) < 2:
        matches = re.search(r'(?<=from)(\s+\w+\b)', query, re.IGNORECASE)
        if matches is None:
            log.info("[vim-database] Can not be able to identify table name in select query")
            return
        table_name = matches.group(0).strip()
        query_result = [[table_name]]

    state.selected_table = None
    state.table_data = None
    state.mode = Mode.QUERY
    state.user_query = True
    await show_ascii_table(configs, query_result[0], query_result[1:])


async def show_insert_query(configs: UserConfig, state: State) -> None:
    if state.mode == Mode.QUERY and not state.user_query:

        insert_query = await run_in_executor(
            partial(state.sql_client.get_template_insert_query, state.selected_database, state.selected_table))
        if insert_query is None:
            return

        query_window = await async_call(partial(open_query_window, configs))
        await async_call(partial(render, query_window, insert_query))
    elif state.mode == Mode.TABLE:
        create_table_query = ["CREATE TABLE table_name (", "\t", ")"]
        query_window = await async_call(partial(open_query_window, configs))
        await async_call(partial(render, query_window, create_table_query))


async def show_update_query(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.QUERY or state.user_query:
        return

    headers, rows = state.table_data
    row_idx = await async_call(partial(get_current_row_idx, state))
    if row_idx is None:
        return

    row = rows[row_idx]
    primary_key, primary_key_value = await get_primary_key_value(state, row_idx)
    if primary_key is None:
        return

    update_query = ["UPDATE " + state.selected_table + " SET "]
    num_columns = len(headers)
    for i in range(num_columns):
        column = headers[i]
        column_value = row[i]
        if column != primary_key:
            update_query.append("\t" + column + " = \'" + column_value + "\',")
    update_query[-1] = update_query[-1][:-1]
    update_query.append("WHERE " + primary_key + " = \'" + primary_key_value + "\'")
    query_window = await async_call(partial(open_query_window, configs))
    await async_call(partial(render, query_window, update_query))


async def show_copy_query(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.QUERY or state.user_query:
        return

    headers, rows = state.table_data
    row_idx = await async_call(partial(get_current_row_idx, state))
    if row_idx is None:
        return

    row = rows[row_idx]
    insert_query = ["INSERT INTO " + state.selected_table + " ("]
    num_columns = len(headers)
    for i in range(num_columns):
        column_name = headers[i]
        insert_query.append("\t" + column_name)
        if i != num_columns - 1:
            insert_query[-1] += ","

    insert_query.append(") VALUES (")
    for i in range(num_columns):
        column_value = row[i]
        insert_query.append("\t" + (column_value if column_value == 'NULL' else ("\'" + column_value + "\'")))
        if i != num_columns - 1:
            insert_query[-1] += ","
    insert_query.append(")")

    query_window = await async_call(partial(open_query_window, configs))
    await async_call(partial(render, query_window, insert_query))
