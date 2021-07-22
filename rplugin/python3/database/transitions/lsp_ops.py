import json
import os
from functools import partial
from hashlib import md5
from os import path
from typing import Any, Dict

from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import State
from ..storages.connection import Connection, ConnectionType
from ..utils.files import is_file_exists, create_folder_if_not_present
from ..utils.log import log

_LS_CONFIG_FILE_NAME = ".sqllsrc.json"


async def lsp_config(_: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    await run_in_executor(partial(switch_database_connection, state.selected_connection, state.selected_database))


def switch_database_connection(connection: Connection, database: str) -> None:
    current_project_path = os.getcwd()
    config_connection_name = md5(current_project_path.encode('utf-8')).hexdigest()
    config_folder = path.join(path.expanduser("~"), ".config/sql-language-server")
    config_path = path.join(config_folder, _LS_CONFIG_FILE_NAME)

    if not is_file_exists(config_path):
        create_folder_if_not_present(config_folder)
        config_file = open(config_path, "w")
        config_file.write(json.dumps({"connections": []}, indent=2))
        config_file.close()

    with open(config_path) as config_file:
        json_data = json.load(config_file)
        config_connections = json_data["connections"]

    current_config_connection = next((config_connection for config_connection in config_connections
                                      if config_connection["name"] == config_connection_name), None)

    if current_config_connection is not None and _is_connection_configured(connection, database,
                                                                           current_config_connection):
        log.info("[vim-database] Switch database connection successfully")
        return

    if connection.connection_type == ConnectionType.SQLITE:
        new_config_connection = {
            "name": config_connection_name,
            "adapter": "sqlite3",
            "filename": connection.database,
            "projectPaths": [current_project_path]
        }
    elif connection.connection_type == ConnectionType.MYSQL:
        new_config_connection = {
            "name": config_connection_name,
            "adapter": "mysql",
            "host": connection.host,
            "port": connection.port,
            "user": connection.username,
            "password": connection.password,
            "database": database,
            "projectPaths": [current_project_path]
        }
    elif connection.connection_type == ConnectionType.POSTGRESQL:
        new_config_connection = {
            "name": config_connection_name,
            "adapter": "postgres",
            "host": connection.host,
            "port": connection.port,
            "user": connection.username,
            "password": connection.password,
            "database": database,
            "projectPaths": [current_project_path]
        }
    else:
        log.info("[vim-database] Connection type is not supported")
        return

    for index, config_connection in enumerate(config_connections):
        if config_connection_name == config_connection["name"]:
            del config_connections[index]
            break
    config_connections.append(new_config_connection)

    config_file = open(config_path, "w")
    config_file.write(json.dumps({"connections": config_connections}, indent=2))
    config_file.close()
    log.info("[vim-database] Switch database connection successfully. Please restart sql-language-server")


def _is_connection_configured(connection: Connection, selected_database: str, config_connection: Dict[str,
                                                                                                      Any]) -> bool:
    if not config_connection["adapter"].lower().startswith(connection.connection_type.to_string().lower()):
        return False

    if config_connection["adapter"] == "sqlite3":
        return config_connection["filename"] == selected_database

    if not config_connection["database"] != selected_database:
        return False
    if not config_connection["host"] != connection.host:
        return False
    if not config_connection["port"] != connection.port:
        return False

    return True
