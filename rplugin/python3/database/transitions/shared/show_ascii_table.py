from functools import partial

from ...configs.config import UserConfig
from ...utils.ascii_table import ascii_table
from ...utils.nvim import (
    async_call,
    set_cursor,
    render,
)
from ...views.database_window import (
    open_database_window,)


async def show_ascii_table(configs: UserConfig, headers: list, rows: list) -> None:
    window = await async_call(partial(open_database_window, configs))

    await async_call(partial(render, window, ascii_table(headers, rows)))
    await async_call(partial(set_cursor, window, (4, 0)))
