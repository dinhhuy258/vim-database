from queue import SimpleQueue
from threading import Thread
from concurrent.futures import Future
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class ExecutorService:

    def __init__(self) -> None:
        self.__thread = Thread(target=self._loop, daemon=True)
        self._queue: SimpleQueue = SimpleQueue()
        self.__thread.start()

    def _loop(self) -> None:
        while True:
            func = self._queue.get()
            func()

    def run_sync(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> Future:
        future = Future()

        def run() -> None:
            try:
                future.set_result(func(*args, **kwargs))
            except BaseException as e:
                future.set_exception(e)

        self._queue.put_nowait(run)

        return future
