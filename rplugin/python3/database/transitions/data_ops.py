from functools import partial
from typing import Optional, Tuple

from .shared.get_primary_key_value import get_primary_key_value
from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import Mode, State
from ..transitions.shared.get_current_row_idx import get_current_row_idx
from ..transitions.shared.show_ascii_table import show_ascii_table
from ..transitions.shared.show_table_data import show_table_data
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
    row_idx = await async_call(partial(get_current_row_idx, state))
    if row_idx is None:
        return
    headers, rows = state.table_data

    primary_key, primary_key_value = await get_primary_key_value(state, row_idx)
    if primary_key is None:
        return

    ans = await async_call(
        partial(confirm, "DELETE FROM " + state.selected_table + " WHERE " + primary_key + " = " + primary_key_value))
    if not ans:
        return

    delete_success = await run_in_executor(state.sql_client.delete, state.selected_database, state.selected_table,
                                           (primary_key, "\'" + primary_key_value + "\'"))
    if delete_success:
        del rows[row_idx]
        state.table_data = (headers, rows)
        await show_ascii_table(configs, headers, rows)


async def copy_row(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.QUERY or state.user_query:
        return

    row_idx = await async_call(partial(get_current_row_idx, state))
    if row_idx is None:
        return

    headers, rows = state.table_data
    row = rows[row_idx][:]

    ans = await async_call(partial(confirm, "Do you want to copy this row?"))
    if not ans:
        return

    header_map = dict()
    for header_idx, header in enumerate(headers):
        header_map[header] = header_idx

    unique_column_names = await run_in_executor(
        partial(state.sql_client.get_unique_columns, state.selected_database, state.selected_table))
    if len(unique_column_names) == 0:
        log.info("[vim-database] No unique column found")
        return

    unique_columns = []
    new_unique_column_values = []
    for unique_column in unique_column_names:
        new_unique_column_value = await async_call(partial(get_input, "New unique value " + unique_column + ": "))
        if new_unique_column_value:
            unique_columns.append((unique_column, row[header_map[unique_column]]))
            new_unique_column_values.append(new_unique_column_value)
            row[header_map[unique_column]] = new_unique_column_value
        else:
            return

    copy_result = await run_in_executor(
        partial(state.sql_client.copy, state.selected_database, state.selected_table, unique_columns,
                new_unique_column_values))
    if copy_result:
        rows.append(row)
        state.table_data = (headers, rows)
        await show_ascii_table(configs, headers, rows)


async def edit_row(configs: UserConfig, state: State) -> None:
    edit_column, edit_value, row_idx, column_idx = await _get_current_cell_value(state)
    if edit_column is None:
        return

    new_value = await async_call(partial(get_input, "Edit column " + edit_column + ": ", edit_value))
    if new_value and new_value != edit_value:
        primary_key, primary_key_value = await get_primary_key_value(state, row_idx)
        if primary_key is None:
            return

        ans = await async_call(
            partial(
                confirm, "UPDATE " + state.selected_table + " SET " + edit_column + " = " + new_value + " WHERE " +
                primary_key + " = " + primary_key_value))
        if not ans:
            return

        update_success = await run_in_executor(
            partial(state.sql_client.update, state.selected_database, state.selected_table,
                    (edit_column, "\'" + new_value + "\'"), (primary_key, "\'" + primary_key_value + "\'")))
        if update_success:
            data_headers, data_rows = state.table_data
            data_rows[row_idx][column_idx] = new_value
            state.table_data = (data_headers, data_rows)
            await show_ascii_table(configs, data_headers, data_rows)


async def filter_columns(configs: UserConfig, state: State) -> None:
    if state.mode != Mode.QUERY or state.user_query:
        return

    filtered_columns = await async_call(partial(get_input, "Filter columns: ", ", ".join(state.filtered_columns)))
    filtered_columns = filtered_columns if filtered_columns is None else filtered_columns.strip()
    if filtered_columns:
        state.filtered_columns.clear()
        columns = filtered_columns.split(",")
        for column in columns:
            state.filtered_columns.add(column.strip())

    await show_table_data(configs, state, state.selected_table)


async def order(configs: UserConfig, state: State, orientation: str) -> None:
    if state.mode != Mode.QUERY or state.user_query:
        return

    order_column, _, _, _ = await _get_current_cell_value(state)
    if order_column is None:
        return

    state.order = (order_column, orientation)

    await show_table_data(configs, state, state.selected_table)


async def row_filter(configs: UserConfig, state: State) -> None:

    def get_filter_condition() -> Optional[str]:
        condition = state.query_conditions if state.query_conditions is not None else ""
        return get_input("New query conditions: ", condition)

    filter_condition = await async_call(get_filter_condition)
    filter_condition = filter_condition if filter_condition is None else filter_condition.strip()
    if filter_condition:
        state.query_conditions = filter_condition
        await show_table_data(configs, state, state.selected_table)


async def next_page(configs: UserConfig, state: State) -> None:
    state.current_page += 1
    await show_table_data(configs, state, state.selected_table)

    if not state.table_data[1]:
        state.current_page -= 1
        await show_table_data(configs, state, state.selected_table)


async def previous_page(configs: UserConfig, state: State) -> None:
    if state.current_page <= 1:
        return

    state.current_page -= 1
    await show_table_data(configs, state, state.selected_table)


def _get_current_row_and_column(state: State) -> Tuple[Optional[int], Optional[int]]:
    row_cursor, column_cursor = get_current_database_window_cursor()

    _, rows = state.table_data
    row_size = len(rows)

    # Minus 4 for header of the table
    row_idx = row_cursor - 4
    line = get_current_database_window_line()
    if row_idx < 0 or row_idx >= row_size or line is None or line[column_cursor] == '|':
        return None, None

    column_idx = 0
    for i in range(column_cursor):
        if line[i] == '|':
            column_idx += 1

    return row_idx, column_idx - 1


async def _get_current_cell_value(state: State) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int]]:
    row, column = await async_call(partial(_get_current_row_and_column, state))
    if row is None:
        return None, None, None, None

    data_headers, data_rows = state.table_data
    return data_headers[column], data_rows[row][column], row, column
