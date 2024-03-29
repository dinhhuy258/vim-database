from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from ..concurrents.executors import run_in_executor
from ..sql_clients.sql_client import SqlClient
from ..sql_clients.sql_client_factory import SqlClientFactory
from ..storages.connection import (
    Connection,
    get_connections,
)


class Mode(Enum):
    CONNECTION = 1
    DATABASE = 2
    TABLE = 3
    QUERY = 4
    TABLE_INFO = 5


@dataclass(frozen=False)
class State:
    mode: Mode
    connections: list
    selected_connection: Optional[Connection]
    sql_client: Optional[SqlClient]
    databases: list
    selected_database: Optional[str]
    tables: list
    selected_table: Optional[str]
    table_data: Optional[Tuple[list, list]]
    filtered_tables: Optional[str]
    filtered_columns: set[str]
    query_conditions: Optional[str]
    order: Optional[Tuple[str, str]]
    user_query: bool
    current_page: int

    def load_default_connection(self):
        if self.connections:
            self.selected_connection = self.connections[0]
            for connection in self.connections:
                if connection.name == "default":
                    self.selected_connection = connection
                    break
            self.selected_database = self.selected_connection.database
            self.sql_client = SqlClientFactory.create(self.selected_connection)


async def init_state() -> State:
    state = State(mode=Mode.CONNECTION,
                  connections=list(),
                  selected_connection=None,
                  databases=list(),
                  selected_database=None,
                  sql_client=None,
                  tables=list(),
                  selected_table=None,
                  table_data=None,
                  filtered_tables=None,
                  filtered_columns=set(),
                  query_conditions=None,
                  order=None,
                  user_query=False,
                  current_page=1)

    def _get_connections() -> list:
        return list(get_connections())

    state.connections = await run_in_executor(_get_connections)
    state.load_default_connection()

    return state
