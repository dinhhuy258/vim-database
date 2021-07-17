import os
from os import path
from asyncio import get_running_loop
from functools import partial
from typing import Any, Callable, TypeVar

T = TypeVar("T")


async def run_in_executor(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    loop = get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))
