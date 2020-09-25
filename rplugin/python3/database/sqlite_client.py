from typing import Optional, Tuple
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
            log.info("[vim-databse] " + result.data)
            return list()
        return list([result.data.split()[-1]])

    def get_tables(self, database: str) -> list:
        result = run_command(["sqlite3", database, ".table"])
        if result.error:
            log.info("[vim-databse] " + result.data)
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

    def update(self, database: str, table: str, update: Tuple[str, str], condition: Tuple[str, str]) -> bool:
        update_column, update_value = update
        update_query = "UPDATE " + table + " SET " + update_column + " = " + update_value

        condition_column, condition_value = condition
        update_query = update_query + " WHERE " + condition_column + " = " + condition_value

        result = run_command(["sqlite3", database, update_query])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return False

        return True

    def copy(self, database: str, table: str, unique_columns: list, new_unique_column_values: list) -> bool:
        log.info("[vim-databse] Not supported for sqlite")
        return False

    def delete(self, database: str, table: str, condition: Tuple[str, str]) -> bool:
        condition_column, condition_value = condition
        delete_query = "DELETE FROM " + table + " WHERE " + condition_column + " = " + condition_value

        result = run_command(["sqlite3", database, delete_query])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return False

        return True

    def get_primary_key(self, database: str, table: str) -> Optional[str]:
        table_info = self.describe_table(database, table)
        if table_info is None:
            return None
        headers = table_info[0]
        columns = table_info[1:]
        pk_index = -1
        name_index = -1

        index = 0
        for header in headers:
            if header == "pk":
                pk_index = index
            if header == "name":
                name_index = index
            index = index + 1

        if pk_index == -1 or name_index == -1:
            return None

        for column_info in columns:
            if column_info[pk_index] == "1":
                return column_info[name_index]

        return None

    def get_unique_columns(self, database: str, table: str) -> Optional[list]:
        return [self.get_primary_key(database, table)]

    def get_template_insert_query(self, database: str, table: str) -> Optional[list]:
        log.info("[vim-databse] Not supported for sqlite")
        return None
