from typing import Optional

from ...states.state import State
from ...views.database_window import get_current_database_window_row


def get_row_index(state: State) -> Optional[int]:
    row = get_current_database_window_row()
    _, result_rows = state.result
    result_size = len(result_rows)

    # Minus 4 for header of the table
    result_index = row - 4
    if result_index < 0 or result_index >= result_size:
        return None

    return result_index
