from .show_result import show_result
from ...concurrents.executors import run_in_executor
from ...configs.config import UserConfig
from ...sql_clients.sql_client_factory import SqlClientFactory
from ...states.state import Mode, State
from ...utils.log import log


async def show_table_content(configs: UserConfig, state: State, table: str) -> None:
    if not state.selected_connection:
        log.info("[vim-database] No connection found")
        return

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
    await show_result(configs, headers, rows)
