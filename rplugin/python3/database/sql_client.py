import abc
from typing import Optional
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
