import abc
from typing import Optional, Tuple
from .connection import Connection


class SqlClient(metaclass=abc.ABCMeta):

    def __init__(self, connection: Connection):
        self.connection = connection

    @abc.abstractmethod
    def get_databases(self) -> list:
        pass

    @abc.abstractmethod
    def get_tables(self, database: str) -> list:
        pass

    @abc.abstractmethod
    def delete_table(self, database: str, table: str) -> None:
        pass

    @abc.abstractmethod
    def describe_table(self, database: str, table: str) -> Optional[list]:
        pass

    @abc.abstractmethod
    def run_query(self, database: str, query: str) -> Optional[list]:
        pass

    @abc.abstractmethod
    def copy(self, database: str, table: str, primary: Tuple[str, str], new_primary_key_value: str) -> bool:
        pass

    @abc.abstractmethod
    def update(self, database: str, table: str, update: Tuple[str, str], condition: Tuple[str, str]) -> bool:
        pass

    @abc.abstractmethod
    def delete(self, database: str, table: str, condition: Tuple[str, str]) -> bool:
        pass

    @abc.abstractmethod
    def get_primary_key(self, database: str, table: str) -> Optional[str]:
        pass

    @abc.abstractmethod
    def get_template_insert_query(self, database: str, table: str) -> Optional[list]:
        pass
