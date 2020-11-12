from typing import Optional, Tuple
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
            "--connect-timeout=10",
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

    def update(self, database: str, table: str, update: Tuple[str, str], condition: Tuple[str, str]) -> bool:
        update_column, update_value = update
        update_query = "UPDATE " + table + " SET " + update_column + " = " + update_value

        condition_column, condition_value = condition
        update_query = update_query + " WHERE " + condition_column + " = " + condition_value

        result = self._run_query(update_query, ["--database=" + database])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return False

        return True

    def copy(self, database: str, table: str, unique_columns: list, new_unique_column_values: list) -> bool:
        num_unique_columns = len(unique_columns)
        if num_unique_columns != len(new_unique_column_values):
            log.info("[vim-databse] The lenght of unique columns must be equal to new unique column values")
            return False

        assign_query = ""
        condition_query = ""
        for index in range(num_unique_columns):
            unique_column, unique_column_value = unique_columns[index]
            new_unique_column_value = new_unique_column_values[index]
            if index != 0:
                assign_query += ", "
                condition_query += " AND "
            assign_query += unique_column + " = " + (new_unique_column_value if new_unique_column_value == 'NULL' else
                                                     ("\'" + new_unique_column_value + "\'"))
            if unique_column_value == 'NULL':
                condition_query += unique_column + " is NULL"
            else:
                condition_query += unique_column + " = + \'" + unique_column_value + "\'"

        create_temporary_query = "CREATE TEMPORARY TABLE tmptable_1 SELECT * FROM " + table + " WHERE " + condition_query + ";"
        update_primary_key_temporary_query = "UPDATE tmptable_1 SET " + assign_query + ";"
        insert_query = "INSERT INTO " + table + " SELECT * FROM tmptable_1;"
        delete_temporary_query = "DROP TEMPORARY TABLE IF EXISTS tmptable_1;"
        copy_query = create_temporary_query + update_primary_key_temporary_query + insert_query + delete_temporary_query

        result = self._run_query(copy_query, ["--database=" + database])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return False

        return True

    def delete(self, database: str, table: str, condition: Tuple[str, str]) -> bool:
        condition_column, condition_value = condition
        delete_query = "DELETE FROM " + table + " WHERE " + condition_column + " = " + condition_value

        result = self._run_query(delete_query, ["--database=" + database])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return False

        return True

    def get_primary_key(self, database: str, table: str) -> Optional[str]:
        get_primary_key_query = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_NAME = \'" + table + "\'  AND CONSTRAINT_NAME = 'PRIMARY' AND CONSTRAINT_SCHEMA=\'" + database + "\'"
        result = self._run_query(get_primary_key_query, ["--skip-column-names"])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return None

        return result.data

    def get_unique_columns(self, database: str, table: str) -> Optional[list]:
        get_unique_keys_query = "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = \'" + database + "\' AND TABLE_NAME = \'" + table + "\' AND NON_UNIQUE = 0"
        result = self._run_query(get_unique_keys_query, ["--skip-column-names"])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return None

        return result.data.splitlines()

    def get_template_insert_query(self, database: str, table: str) -> Optional[list]:
        get_columns_query = "SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_NAME = \'" + table + "\' AND TABLE_SCHEMA=\'" + database + "\'"
        result = self._run_query(get_columns_query, ["--skip-column-names"])
        if result.error:
            log.info("[vim-databse] " + result.data)
            return None

        lines = result.data.splitlines()
        columns = list(map(lambda line: line.split("\t"), lines))
        insert_query = []
        insert_query.append("INSERT INTO " + table + " (")
        columns_len = len(columns)
        for index, column in enumerate(columns):
            insert_query.append("\t" + column[0])
            if index != columns_len - 1:
                insert_query[-1] += ","
        insert_query.append(") VALUES (")

        for index, column in enumerate(columns):
            if column[1] != 'NULL' or (column[1] == 'NULL' and column[2].lower() == 'yes'):
                insert_query.append("\t" + (column[1] if column[1] == 'NULL' else ("\'" + column[1] + "\'")))
            else:
                insert_query.append("\t")
            if index != columns_len - 1:
                insert_query[-1] += ","
        insert_query.append(")")

        return insert_query
