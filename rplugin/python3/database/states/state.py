from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple
from logging import log
from ..concurrents.executors import run_in_executor
from ..connection import (
    Connection,
    get_default_connection,
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

    async def load_connection(self) -> bool:
        if self.selected_connection is None:
            self.selected_connection = await run_in_executor(get_default_connection)

        if self.selected_connection is None:
            log.info("[vim-database] No connection found")
            return False

        return True

    async def load_database(self) -> bool:
        if self.selected_database is None:
            self.selected_database = self.selected_connection.database

        if self.selected_database is None:
            log.info("[vim-database] No database found")
            return False

        return True


def init_state() -> State:
    return State(mode=Mode.CONNECTION,
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
