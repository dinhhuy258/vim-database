import os
from typing import Optional, Tuple

from .sql_client import SqlClient, CommandResult
from ..storages.connection import Connection
from ..utils.log import log


class PostgreSqlClient(SqlClient):

    def __init__(self, connection: Connection):
        SqlClient.__init__(self, connection)

    def _run_query(self, query: str, options: list = []) -> CommandResult:
        return self.run_command([
            "psql",
            "--host=" + self.connection.host,
            "--port=" + self.connection.port,
            "--username=" + self.connection.username,
            "--pset=footer",
            "-c",
            query,
        ] + options, dict(os.environ, PGPASSWORD=self.connection.password, PGCONNECT_TIMEOUT="10"))

    def get_databases(self) -> list:
        result = self._run_query("SELECT datname FROM pg_database WHERE datistemplate = false", ["--tuples-only"])
        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return list()

        return list(map(lambda database: database.strip(), result.data.splitlines()))

    def get_tables(self, database: str) -> list:
        result = self._run_query(
            "SELECT tablename "
            "FROM pg_catalog.pg_tables "
            "WHERE schemaname != \'pg_catalog\' AND schemaname != \'information_schema\'",
            ["--tuples-only", "--dbname=" + database])

        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return list()

        return list(map(lambda table: table.strip(), result.data.splitlines()))

    def delete_table(self, database: str, table: str) -> None:
        result = self._run_query("DROP TABLE " + table, ["--tuples-only", "--dbname=" + database])
        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))

    def describe_table(self, database: str, table: str) -> Optional[list]:
        result = self._run_query(
            "SELECT column_name, column_default, is_nullable, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = \'" + table + "\'", ["--tuples-only", "--dbname=" + database])

        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return None

        lines = result.data.splitlines()
        if len(lines) == 0:
            log.info("[vim-database] No table information found")
            return None

        data = list(map(lambda line: [column.strip() for column in line.split("|")], lines))
        data.insert(0, ["column_name", "column_default", "is_nullable", "data_type"])

        return data

    def run_query(self, database: str, query: str) -> Optional[list]:
        result = self._run_query(query, ["--dbname=" + database])
        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return None
        lines = result.data.splitlines()

        data = list(map(lambda line: [column.strip() for column in line.split("|")], lines))
        if len(data) <= 1:
            return list()

        del data[1]
        return data

    def update(self, database: str, table: str, update: Tuple[str, str], condition: Tuple[str, str]) -> bool:
        update_column, update_value = update
        update_query = "UPDATE " + table + " SET " + update_column + " = " + update_value

        condition_column, condition_value = condition
        update_query = update_query + " WHERE " + condition_column + " = " + condition_value

        result = self._run_query(update_query, ["--dbname=" + database])
        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return False

        return True

    def copy(self, database: str, table: str, unique_columns: list, new_unique_column_values: list) -> bool:
        log.info("[vim-database] Not supported for psql")
        return False

    def delete(self, database: str, table: str, condition: Tuple[str, str]) -> bool:
        condition_column, condition_value = condition
        delete_query = "DELETE FROM " + table + " WHERE " + condition_column + " = " + condition_value

        result = self._run_query(delete_query, ["--tuples-only", "--dbname=" + database])
        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return False

        return True

    def get_primary_key(self, database: str, table: str) -> Optional[str]:
        get_primary_key_query = "SELECT a.attname " \
                                "FROM pg_index i " \
                                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " \
                                "WHERE i.indrelid = \'" + table + "\'::regclass AND i.indisprimary"
        result = self._run_query(get_primary_key_query, ["--tuples-only", "--dbname=" + database])
        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return None

        return result.data.strip()

    def get_unique_columns(self, database: str, table: str) -> Optional[list]:
        get_unique_keys_query = "SELECT a.attname " \
                                "FROM pg_index i " \
                                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " \
                                "WHERE i.indrelid = \'" + table + "\'::regclass AND i.indisunique"
        result = self._run_query(get_unique_keys_query, ["--tuples-only", "--dbname=" + database])
        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return None

        return list(map(lambda column: column.strip(), result.data.splitlines()))

    def get_template_insert_query(self, database: str, table: str) -> Optional[list]:
        result = self._run_query(
            "SELECT column_name, column_default, is_nullable FROM information_schema.columns WHERE table_name = \'" +
            table + "\'", ["--tuples-only", "--dbname=" + database])

        if result.error:
            log.info("[vim-database] " + ". ".join(result.data.splitlines()))
            return None

        lines = result.data.splitlines()
        if len(lines) == 0:
            log.info("[vim-database] No table information found")
            return None

        columns = list(map(lambda line: [col.strip() for col in line.split("|")], lines))

        name_index = 0
        default_value_index = 1
        is_nullable_index = 2
        insert_query = ["INSERT INTO " + table + " ("]
        columns_len = len(columns)
        for index, column in enumerate(columns):
            insert_query.append("\t" + column[name_index])
            if index != columns_len - 1:
                insert_query[-1] += ","
        insert_query.append(") VALUES (")

        for index, column in enumerate(columns):
            default_value = column[default_value_index]
            colon_index = default_value.find(':')
            if colon_index != -1:
                default_value = default_value[0:colon_index]

            if default_value != 'NULL' or (default_value == 'NULL' and column[is_nullable_index].lower() == 'yes'):
                insert_query.append("\t" + default_value)
            else:
                insert_query.append("\t")

            if index != columns_len - 1:
                insert_query[-1] += ","
        insert_query.append(")")

        return insert_query
