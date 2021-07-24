from functools import partial
from typing import Tuple, Optional

from ...concurrents.executors import run_in_executor
from ...states.state import State
from ...utils.log import log


async def get_primary_key_value(state: State, row: int) -> Tuple[Optional[str], Optional[str]]:
    primary_key = await run_in_executor(
        partial(state.sql_client.get_primary_key, state.selected_database, state.selected_table))
    if primary_key is None:
        log.info("[vim-database] No primary key found for table " + state.selected_table)
        return None, None

    headers, rows = state.table_data

    for header_idx, header in enumerate(headers):
        if header == primary_key:
            return primary_key, rows[row][header_idx]

    # Not reachable
    return None, None
