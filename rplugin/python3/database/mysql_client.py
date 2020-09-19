import re
from typing import Optional
from .sql_client import SqlClient
from .connection import Connection
from .utils import CommandResult, run_command
from .logging import log


class MySqlClient(SqlClient):

    def __init__(self, connection: Connection):
        SqlClient.__init__(self, connection)

    def _run_query(self, query: str, options: list = list()) -> CommandResult:
        return run_command([
            "mysql",
            "--unbuffered",
            "--batch",
            "--host=" + self.connection.host,
            "--port=" + self.connection.port,
            "--user=" + self.connection.username,
            "--password=" + self.connection.password,
            "-e",
            query,
        ] + options)

    def get_databases(self) -> list:
        result = self._run_query("SHOW DATABASES", ["--skip-column-names"])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return list()
        return result.data.splitlines()

    def get_tables(self, database: str) -> list:
        result = self._run_query("SHOW TABLES FROM " + database, ["--skip-column-names"])

        if result.error:
            log.info("[vim-databse] " + result.data)
            return list()
        return result.data.splitlines()

    def delete_table(self, database: str, table: str) -> None:
        result = self._run_query("DROP TABLE " + database + "." + table)
        if result.error:
            log.info("[vim-databse] " + result.data)

    def describe_table(self, database: str, table: str) -> Optional[list]:
        result = self._run_query("DESCRIBE " + database + "." + table)
        if result.error:
            log.info("[vim-databse] " + result.data)
            return None

        lines = result.data.splitlines()
        if len(lines) < 2:
            log.info("[vim-databse] No table information found")
            return None

        return list(map(lambda line: line.split("\t"), lines))

    def run_query(self, database: str, query: str) -> Optional[list]:
        result = self._run_query(query, ["--database=" + database])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return None

        lines = result.data.splitlines()
        return list(map(lambda line: line.split("\t"), lines))
