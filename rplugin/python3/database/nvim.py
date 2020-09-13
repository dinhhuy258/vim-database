from pynvim import Nvim
from asyncio import Future
from typing import (
    Any,
    Awaitable,
    Callable,
    TypeVar,
)

T = TypeVar("T")


def init_nvim(nvim: Nvim) -> None:
    global _nvim
    _nvim = nvim


def async_call(func: Callable[[], T]) -> Awaitable[T]:
    future: Future = Future()

    def run() -> None:
        try:
            ret = func()
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(ret)

    _nvim.async_call(run)
    return future


def confirm(question: str) -> bool:
    return _nvim.funcs.confirm(question, "&Yes\n&No", 2) == 1


def get_input(question: str) -> str:
    return _nvim.funcs.input(question, "")


def get_global_var(name: str, default_value: Any) -> Any:
    try:
        return _nvim.api.get_var(name)
    except:
        return default_value
