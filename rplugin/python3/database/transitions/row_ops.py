from functools import partial
from typing import Optional, Tuple

from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import Mode, State
from ..transitions.shared.get_row_index import get_row_index
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


async def delete_result(configs: UserConfig, state: State) -> None:
    result_index = await async_call(partial(get_row_index, state))
    if result_index is None:
        return
    result_headers, result_rows = state.result

    primary_key = await run_in_executor(partial(state.sql_client.get_primary_key, state.selected_database,
                                                state.selected_table))
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

    result_index = await async_call(partial(get_row_index, state))
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

    result_index = await async_call(partial(_get_result_row_and_column, state))
    if result_index is None:
        return
    result_headers, result_rows = state.result
    row, column = result_index
    edit_column = result_headers[column]
    edit_value = result_rows[row][column]

    new_value = await async_call(partial(get_input, "Edit column " + edit_column + ": ", edit_value))
    if new_value and new_value != edit_value:
        primary_key = await run_in_executor(partial(state.sql_client.get_primary_key,
                                                    state.selected_database,
                                                    state.selected_table))
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

        update_success = await run_in_executor(partial(state.sql_client.update,
                                                       state.selected_database,
                                                       state.selected_table,
                                                       (edit_column, "\'" + new_value + "\'"),
                                                       (primary_key, "\'" + primary_key_value + "\'")
                                                       ))
        if update_success:
            result_rows[row][column] = new_value
            state.result = (result_headers, result_rows)
            await show_result(configs, result_headers, result_rows)


async def filter_column(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.TABLE_CONTENT_RESULT:
        return

    def get_filter_column() -> Optional[str]:
        filter_column = state.filtered_columns if state.filtered_columns is not None else ""
        return get_input("New filter column: ", filter_column)

    filter_column = await async_call(get_filter_column)
    filter_column = filter_column if filter_column is None else filter_column.strip()
    if filter_column:
        state.filtered_columns = filter_column
        await show_table_content(configs, state, state.selected_table)


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

