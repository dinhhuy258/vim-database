import os
import shelve
from dataclasses import dataclass
from enum import Enum
from hashlib import md5
from os import path
from typing import Optional, Iterator

_CONNECTION_TYPES = {1: "SQLite", 2: "MySQL", 3: "PostgreSQL"}


class ConnectionType(Enum):
    SQLITE = 1
    MYSQL = 2
    POSTGRESQL = 3

    def to_string(self) -> str:
        return _CONNECTION_TYPES[self.value]


@dataclass(frozen=True)
class Connection:
    name: str
    connection_type: ConnectionType
    host: Optional[str]
    port: Optional[str]
    username: Optional[str]
    password: Optional[str]
    database: Optional[str]


def store_connection(connection: Connection) -> None:
    with shelve.open(_get_connection_store_file_path()) as connection_store:
        connection_store[connection.name] = connection


def remove_connection(connection: Connection) -> None:
    with shelve.open(_get_connection_store_file_path()) as connection_store:
        del connection_store[connection.name]


def get_connections() -> Iterator[Connection]:
    with shelve.open(_get_connection_store_file_path()) as connection_store:
        for connection_name in connection_store:
            yield connection_store[connection_name]


def _get_connection_store_file_path() -> str:
    connection_store_file_name = md5(os.getcwd().encode('utf-8')).hexdigest()
    return path.join(path.expanduser("~"), ".vim-database", connection_store_file_name)
