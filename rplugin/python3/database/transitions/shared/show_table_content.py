from functools import partial

from .show_result import show_result
from ...concurrents.executors import run_in_executor
from ...configs.config import UserConfig
from ...states.state import Mode, State
from ...utils.log import log


async def show_table_content(configs: UserConfig, state: State, table: str) -> None:
    if not state.selected_connection:
        log.info("[vim-database] No connection found")
        return

    query = "SELECT *" if state.filtered_columns is None else "SELECT " + state.filtered_columns
    query = query + " FROM " + table
    if state.query_conditions is not None:
        query = query + " WHERE " + state.query_conditions
    if state.order is not None:
        order_column, order_orientation = state.order
        query = query + " ORDER BY " + order_column + " " + order_orientation
    query = query + " LIMIT " + str(configs.results_limit)

    table_content = await run_in_executor(partial(state.sql_client.run_query, state.selected_database, query))
    if table_content is None:
        # Error
        state.query_conditions = None
        state.filtered_columns = None
        state.order = None
        return

    table_empty = len(table_content) == 0
    headers = [table] if table_empty else table_content[0]
    rows = [] if table_empty else table_content[1:]
    state.selected_table = table
    state.result = (headers, rows)
    state.mode = Mode.TABLE_CONTENT_RESULT
    await show_result(configs, headers, rows)
