from .sql_client import SqlClient
from .sqlite_client import SqliteClient
from .connection import Connection, ConnectionType


class SqlClientFactory(object):

    def create(connection: Connection) -> SqlClient:
        if connection.connection_type == ConnectionType.SQLITE:
            return SqliteClient(connection)
        assert 0, "Bad sql client creation: " + connection.connection_type.to_string()
