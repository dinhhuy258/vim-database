import os
import subprocess
from os import path
from dataclasses import dataclass
from asyncio import get_running_loop
from functools import partial
from typing import Any, Callable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class CommandResult:
    error: bool
    data: str


async def run_in_executor(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    loop = get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))


def is_file_exists(file_path: str) -> bool:
    return path.exists(file_path) and path.isfile(file_path)


def is_folder_exists(folder_path: str) -> bool:
    return path.exists(folder_path) and path.isdir(folder_path)


def create_folder_if_not_present(folder_path: str) -> None:
    if not is_folder_exists(folder_path):
        os.makedirs(folder_path)


def run_command(command: list, environment: dict = None) -> CommandResult:
    if environment is None:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    else:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment)
    if result.returncode == 0:
        return CommandResult(error=False, data=result.stdout.rstrip())

    return CommandResult(error=True, data=result.stderr.rstrip())


def string_compose(target: str, pos: str, source: str) -> str:
    if source == '' or pos < 0:
        return target

    result = target[0:pos]
    if len(result) < pos:
        result += (' ' * pos - len(result))
    result += source
    result += ' ' + target[pos + len(source) + 1:]

    return result
