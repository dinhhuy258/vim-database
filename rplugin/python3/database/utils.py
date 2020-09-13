import os
from os import path
from asyncio import get_running_loop
from functools import partial
from typing import Any, Callable, TypeVar

T = TypeVar("T")


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
