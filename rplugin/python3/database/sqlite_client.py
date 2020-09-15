from .sql_client import SqlClient
from .connection import Connection
from .utils import CommandResult, run_command


class SqliteClient(SqlClient):

    def __init__(self, connection: Connection):
        SqlClient.__init__(self, connection)

    def get_databases(self) -> list:
        result = run_command(["sqlite3", self.connection.database, ".database"])
        if result.error:
            return list('erorr')
        return list([result.data.split()[-1]])
