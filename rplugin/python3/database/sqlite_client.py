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
