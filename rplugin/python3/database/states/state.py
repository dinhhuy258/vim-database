from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple
from logging import log
from ..concurrents.executors import run_in_executor
from ..connection import (
    Connection,
    get_connections,
)


class Mode(Enum):
    CONNECTION = 1
    DATABASE = 2
    TABLE = 3
    INFO_RESULT = 4
    TABLE_CONTENT_RESULT = 5
    QUERY_RESULT = 6


@dataclass(frozen=False)
class State:
    mode: Mode
    connections: list
    selected_connection: Optional[Connection]
    databases: list
    selected_database: Optional[str]
    tables: list
    selected_table: Optional[str]
    result: Optional[Tuple[list, list]]
    filter_pattern: Optional[str]
    filter_column: Optional[str]
    filter_condition: Optional[str]
    order: Optional[Tuple[str, str]]

    def load_default_connection(self):
        if self.connections:
            self.selected_connection = self.connections[0]
            for connection in self.connections:
                if connection.name == "default":
                    self.selected_connection = connection
                    break
            self.selected_database = self.selected_connection.database


async def init_state() -> State:
    state = State(mode=Mode.CONNECTION,
                  connections=list(),
                  selected_connection=None,
                  databases=list(),
                  selected_database=None,
                  tables=list(),
                  selected_table=None,
                  result=None,
                  filter_pattern=None,
                  filter_column=None,
                  filter_condition=None,
                  order=None)

    def _get_connections() -> list:
        return list(get_connections())

    state.connections = await run_in_executor(_get_connections)
    state.load_default_connection()

    return state
