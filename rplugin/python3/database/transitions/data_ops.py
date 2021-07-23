from functools import partial
from typing import Optional, Tuple

from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import Mode, State
from ..transitions.shared.get_current_row import get_current_row
from ..transitions.shared.show_result import show_result
from ..transitions.shared.show_table_content import show_table_content
from ..utils.log import log
from ..utils.nvim import (
    async_call,
    confirm,
    get_input,
)
from ..views.database_window import (
    get_current_database_window_line,
    get_current_database_window_cursor,
)


async def delete_row(configs: UserConfig, state: State) -> None:
    result_index = await async_call(partial(get_current_row, state))
    if result_index is None:
        return
    result_headers, result_rows = state.result

    primary_key, primary_key_value = await _get_primary_key_value(state, result_index)
    if primary_key is None:
        return

    ans = await async_call(
        partial(confirm, "DELETE FROM " + state.selected_table + " WHERE " + primary_key + " = " + primary_key_value))
    if not ans:
        return

    delete_success = await run_in_executor(state.sql_client.delete, state.selected_database, state.selected_table,
                                           (primary_key, "\'" + primary_key_value + "\'"))
    if delete_success:
        del result_rows[result_index]
        state.result = (result_headers, result_rows)
        await show_result(configs, result_headers, result_rows)


async def copy(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return
    if state.filtered_columns is not None:
        log.info("[vim-database] Can not copy row in filter column mode")
        return

    result_index = await async_call(partial(get_current_row, state))
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

    unique_column_names = await run_in_executor(partial(state.sql_client.get_unique_columns,
                                                        state.selected_database,
                                                        state.selected_table))
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

    copy_result = await run_in_executor(partial(state.sql_client.copy,
                                                state.selected_database,
                                                state.selected_table,
                                                unique_columns,
                                                new_unique_column_values))
    if copy_result:
        result_rows.append(copy_row)
        state.result = (result_headers, result_rows)
        await show_result(configs, result_headers, result_rows)


async def edit(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    edit_column, edit_value, row, column = await _get_current_cell_value(state)
    if edit_column is None:
        return

    new_value = await async_call(partial(get_input, "Edit column " + edit_column + ": ", edit_value))
    if new_value and new_value != edit_value:
        primary_key, primary_key_value = await _get_primary_key_value(state, row)
        if primary_key is None:
            return

        ans = await async_call(
            partial(
                confirm, "UPDATE " + state.selected_table + " SET " + edit_column + " = " + new_value + " WHERE " +
                         primary_key + " = " + primary_key_value))
        if not ans:
            return

        update_success = await run_in_executor(partial(state.sql_client.update,
                                                       state.selected_database,
                                                       state.selected_table,
                                                       (edit_column, "\'" + new_value + "\'"),
                                                       (primary_key, "\'" + primary_key_value + "\'")
                                                       ))
        if update_success:
            data_headers, data_rows = state.result
            data_rows[row][column] = new_value
            state.result = (data_headers, data_rows)
            await show_result(configs, data_headers, data_rows)


async def filter_columns(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    def get_filtered_columns() -> Optional[str]:
        columns = state.filtered_columns if state.filtered_columns is not None else ""
        return get_input("Filter columns: ", columns)

    filtered_columns = await async_call(get_filtered_columns)
    filtered_columns = filtered_columns if filtered_columns is None else filtered_columns.strip()
    if filtered_columns:
        state.filtered_columns = filtered_columns
        await show_table_content(configs, state, state.selected_table)


async def order(configs: UserConfig, state: State, orientation: str) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    order_column, _, _, _ = await _get_current_cell_value(state)
    if order_column is None:
        return

    state.order = (order_column, orientation)

    await show_table_content(configs, state, state.selected_table)


async def result_filter(configs: UserConfig, state: State) -> None:
    def get_filter_condition() -> Optional[str]:
        condition = state.query_conditions if state.query_conditions is not None else ""
        return get_input("New condition: ", condition)

    filter_condition = await async_call(get_filter_condition)
    filter_condition = filter_condition if filter_condition is None else filter_condition.strip()
    if filter_condition:
        state.query_conditions = filter_condition
        await show_table_content(configs, state, state.selected_table)


def _get_current_row_and_column(state: State) -> Tuple[Optional[int], Optional[int]]:
    row_cursor, column_cursor = get_current_database_window_cursor()

    _, data_rows = state.result
    result_size = len(data_rows)

    # Minus 4 for header of the table
    row = row_cursor - 4
    line = get_current_database_window_line()
    if row < 0 or row >= result_size or line is None or line[column_cursor] == '|':
        return None, None

    column = 0
    for i in range(column_cursor):
        if line[i] == '|':
            column += 1

    return row, column - 1


async def _get_current_cell_value(state: State) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int]]:
    row, column = await async_call(partial(_get_current_row_and_column, state))
    if row is None:
        return None, None, None, None

    data_headers, data_rows = state.result
    return data_headers[column], data_rows[row][column], row, column


async def _get_primary_key_value(state: State, row: int) -> Tuple[Optional[str], Optional[str]]:
    primary_key = await run_in_executor(partial(state.sql_client.get_primary_key,
                                                state.selected_database,
                                                state.selected_table))
    if primary_key is None:
        log.info("[vim-database] No primary key found for table " + state.selected_table)
        return None, None

    result_headers, result_rows = state.result

    for header_index, header in result_headers:
        if header == primary_key:
            return primary_key, result_rows[row][header_index]

    # Not reachable
    return None, None
