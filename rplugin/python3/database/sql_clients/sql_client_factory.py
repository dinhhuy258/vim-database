from .mysql_client import MySqlClient
from .psql_client import PostgreSqlClient
from .sql_client import SqlClient
from .sqlite_client import SqliteClient
from ..storages.connection import Connection, ConnectionType


class SqlClientFactory(object):

    @staticmethod
    def create(connection: Connection) -> SqlClient:
        if connection.connection_type == ConnectionType.SQLITE:
            return SqliteClient(connection)
        if connection.connection_type == ConnectionType.MYSQL:
            return MySqlClient(connection)
        if connection.connection_type == ConnectionType.POSTGRESQL:
            return PostgreSqlClient(connection)
        assert 0, "Bad sql client creation: " + connection.connection_type.to_string()
