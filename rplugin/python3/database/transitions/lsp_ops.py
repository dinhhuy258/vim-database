from functools import partial
from os import path

import yaml

from ..concurrents.executors import run_in_executor
from ..configs.config import UserConfig
from ..states.state import State
from ..storages.connection import Connection, ConnectionType
from ..utils.files import is_file_exists, create_folder_if_not_present
from ..utils.log import log


class YamlDumper(yaml.Dumper):

    def increase_indent(self, flow=False, *args, **kwargs):
        return super().increase_indent(flow=flow, indentless=False)


async def lsp_config(_: UserConfig, state: State) -> None:
    if not state.connections:
        log.info("[vim-database] No connection found")
        return

    await run_in_executor(partial(_switch_database_connection, state.selected_connection, state.selected_database))


def _switch_database_connection(connection: Connection, database: str) -> None:
    config_connection_name = "vim-database"
    config_folder = path.join(path.expanduser("~"), ".config/sqls")
    config_path = path.join(config_folder, "config.yml")

    config_connections = []
    if not is_file_exists(config_path):
        create_folder_if_not_present(config_folder)
    else:
        with open(config_path) as config_file:
            data = yaml.load(config_file)
            config_connections = data["connections"]

    if connection.connection_type == ConnectionType.SQLITE:
        new_config_connection = {
            "alias": config_connection_name,
            "driver": "sqlite3",
            "dataSourceName": connection.database,
        }
    elif connection.connection_type == ConnectionType.MYSQL or connection.connection_type == ConnectionType.POSTGRESQL:
        new_config_connection = {
            "alias": config_connection_name,
            "driver": "mysql" if connection.connection_type == ConnectionType.MYSQL else "postgres",
            "host": connection.host,
            "port": connection.port,
            "user": connection.username,
            "passwd": connection.password,
            "dbName": database
        }
    else:
        log.info("[vim-database] Connection type is not supported")
        return

    for index, config_connection in enumerate(config_connections):
        if config_connection_name == config_connection["alias"]:
            del config_connections[index]
            break
    config_connections.append(new_config_connection)

    config_file = open(config_path, "w")
    config_file.write(
        yaml.dump({
            "lowercaseKeywords": False,
            "connections": config_connections
        }, Dumper=YamlDumper, sort_keys=False))

    config_file.close()

    log.info("[vim-database] Switch database connection successfully.")
