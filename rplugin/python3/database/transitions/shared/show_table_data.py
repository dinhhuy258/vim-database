from functools import partial

from .show_ascii_table import show_ascii_table
from ...concurrents.executors import run_in_executor
from ...configs.config import UserConfig
from ...states.state import Mode, State
from ...utils.log import log


async def show_table_data(configs: UserConfig, state: State, table: str) -> None:
    if not state.selected_connection:
        log.info("[vim-database] No connection found")
        return

    query = "SELECT * FROM " + table
    if state.query_conditions is not None:
        query = query + " WHERE " + state.query_conditions
    if state.order is not None:
        ordering_column, order = state.order
        query = query + " ORDER BY " + ordering_column + " " + order
    query = query + " LIMIT " + str(configs.results_limit)

    table_content = await run_in_executor(partial(state.sql_client.run_query, state.selected_database, query))
    if table_content is None:
        # Error
        state.query_conditions = None
        return

    table_empty = len(table_content) == 0
    headers = [table] if table_empty else table_content[0]
    rows = [] if table_empty else table_content[1:]
    state.selected_table = table
    state.table_data = (headers, rows)
    state.mode = Mode.QUERY
    state.user_query = False

    if state.filtered_columns:
        filtered_idx = \
            set([header_idx for header_idx, header in enumerate(headers) if header in state.filtered_columns])
        headers = [header for header in headers if header in state.filtered_columns]
        rows = \
            list(map(lambda row: [column for column_idx, column in enumerate(row) if column_idx in filtered_idx], rows))

    await show_ascii_table(configs, headers, rows)
