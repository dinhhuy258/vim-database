import shelve
import os
from os import path
from hashlib import md5
from typing import Optional
from dataclasses import dataclass
from enum import Enum


class ConnectionType(Enum):
    SQLITE = 1


@dataclass(frozen=True)
class Connection:
    name: str
    connection_type: ConnectionType
    database: Optional[str]


def _get_connection_store_file_path() -> str:
    connection_store_file_name = md5(os.getcwd().encode('utf-8')).hexdigest()
    return path.join(path.expanduser("~"), ".vim-database", connection_store_file_name)


def store_connection(connection: Connection) -> None:
    with shelve.open(_get_connection_store_file_path()) as connection_store:
        connection_store[connection.name] = connection
