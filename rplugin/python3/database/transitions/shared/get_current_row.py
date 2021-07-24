from typing import Optional

from ...states.state import State
from ...views.database_window import get_current_database_window_row


def get_current_row(state: State) -> Optional[int]:
    _, rows = state.table_data
    # Minus 4 for header of the table
    row = get_current_database_window_row() - 4

    return None if row < 0 or row >= len(rows) else row
