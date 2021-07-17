import abc
import subprocess
from dataclasses import dataclass
from typing import Optional, Tuple
from ..connection import Connection


@dataclass(frozen=True)
class CommandResult:
    error: bool
    data: str


class SqlClient(metaclass=abc.ABCMeta):

    def __init__(self, connection: Connection):
        self.connection = connection

    def run_command(self, command: list, environment: dict = None) -> CommandResult:
        if environment is None:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        else:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment)
        if result.returncode == 0:
            return CommandResult(error=False, data=result.stdout.rstrip())

        return CommandResult(error=True, data=result.stderr.rstrip())

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
    def copy(self, database: str, table: str, unique_columns: list, new_unique_column_values: list) -> bool:
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
    def get_unique_columns(self, database: str, table: str) -> Optional[list]:
        pass

    @abc.abstractmethod
    def get_template_insert_query(self, database: str, table: str) -> Optional[list]:
        pass
