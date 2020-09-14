from typing import Optional
from functools import partial
from .settings import Settings
from .logging import log
from .connection import Connection, ConnectionType, store_connection, get_connections
from .utils import is_file_exists, run_in_executor
from .database_window import open_database_window, render
from .ascii_table import ascii_table
from .nvim import (
    async_call,
    confirm,
    get_input,
)


def _new_sqlite_connection() -> Optional[Connection]:
    name = get_input("Name: ")
    if not name:
        return None
    file = get_input("File: ")
    if not file:
        return None
    if not is_file_exists(file):
        log.info('[vim-database] File not found: ' + file)
        return None

    return Connection(name=name, connection_type=ConnectionType.SQLITE, database=file)


def _new_connection() -> Optional[Connection]:
    try:
        connection_type_value = get_input("Connection type (1: SQLite): ")
        if connection_type_value:
            connection_type = ConnectionType(int(connection_type_value))
            if connection_type is ConnectionType.SQLITE:
                return _new_sqlite_connection()
    except:
        log.info('[vim-database] Invalid connection type')
    return None


async def new_connection(settings: Settings) -> None:
    connection = await async_call(_new_connection)
    if connection is not None:
        await run_in_executor(partial(store_connection, connection))
        log.info('[vim-database] Connection saved')
    return None


async def show_connections(settings: Settings) -> None:
    window = await async_call(open_database_window)
    connections = await run_in_executor(get_connections)
    connection_datas = []
    connection_headers = ["Name", "Type", "Database"]
    for connection in connections:
        connection_datas.append([connection.name, connection.connection_type.to_string(), connection.database])
    await async_call(partial(render, window, ascii_table(connection_headers, connection_datas)))
