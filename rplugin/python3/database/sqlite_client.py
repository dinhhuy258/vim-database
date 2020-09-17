from typing import Optional
from .sql_client import SqlClient
from .connection import Connection
from .utils import CommandResult, run_command
from .logging import log


class SqliteClient(SqlClient):

    def __init__(self, connection: Connection):
        SqlClient.__init__(self, connection)

    def get_databases(self) -> list:
        result = run_command(["sqlite3", self.connection.database, ".database"])
        if result.error:
            return list()
        return list([result.data.split()[-1]])

    def get_tables(self, database: str) -> list:
        result = run_command(["sqlite3", database, ".table"])
        if result.error:
            return list()
        return result.data.split()

    def delete_table(self, database: str, table: str) -> None:
        delete_table_query = "DROP TABLE " + table
        result = run_command(["sqlite3", database, delete_table_query])
        if result.error:
            log.info("[vim-databse] " + result.data)

    def describe_table(self, database: str, table: str) -> Optional[list]:
        describe_table_query = "PRAGMA table_info(" + table + ")"
        result = run_command(["sqlite3", database, "--header", describe_table_query])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return None

        lines = result.data.splitlines()
        if len(lines) < 2:
            log.info("[vim-databse] No table information found")
            return None

        return list(map(lambda data: data.split("|"), lines))

    def run_query(self, database: str, query: str) -> Optional[list]:
        result = run_command(["sqlite3", database, "--header", query])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return None

        lines = result.data.splitlines()
        return list(map(lambda data: data.split("|"), lines))
