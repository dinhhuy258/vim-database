from pynvim.api.buffer import Buffer
from pynvim.api.window import Window
from typing import Tuple, Iterator, Sequence, Any
from .utils import string_compose
from .nvim import (
    call_atomic,
    get_option,
    create_buffer,
    open_window,
    set_window_option,
    find_windows_in_tab,
    get_buffer_in_window,
    get_buffer_option,
    close_window,
)
from .settings import Settings

_VIM_DATABASE_QUERY_FILE_TYPE = "VimDatabaseQuery"
_VIM_DATABASE_QUERY_TITLE = "[vim-database] Query"
_VIM_DATABASE_QUERY_BORDER_CHARS = ['─', '│', '─', '│', '┌', '┐', '┘', '└']


def _find_query_window_in_tab() -> Iterator[Window]:
    for window in find_windows_in_tab():
        buffer: Buffer = get_buffer_in_window(window)
        buffer_file_type = get_buffer_option(buffer, 'filetype')
        if buffer_file_type == _VIM_DATABASE_QUERY_FILE_TYPE:
            yield window


def _buf_set_lines(buffer: Buffer, lines: list, modifiable: bool) -> Iterator[Tuple[str, Sequence[Any]]]:
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", True)

    yield "nvim_buf_set_lines", (buffer, 0, -1, True, [line.rstrip('\n') for line in lines])
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", False)


def close_query_window() -> None:
    windows: Iterator[Window] = _find_query_window_in_tab()
    for window in windows:
        close_window(window, True)


def open_query_window(settings: Settings) -> None:
    query_windows = _find_query_window_in_tab()
    for window in query_windows:
        return

    buffer = create_buffer(
        settings.query_mappings, {
            "buftype": "nofile",
            "bufhidden": "hide",
            "swapfile": False,
            "buflisted": False,
            "modifiable": True,
            "filetype": _VIM_DATABASE_QUERY_FILE_TYPE,
            'syntax': 'sql',
        })
    height = int((get_option("lines") - 2) / 1.5)
    width = int(get_option("columns") / 1.5)
    row = int((get_option("lines") - height) / 2)
    col = int((get_option("columns") - width) / 2)
    window = open_window(buffer, True, {
        "relative": "editor",
        "width": width,
        "height": height,
        "col": col,
        "row": row,
        "anchor": "NW",
        "style": "minimal",
    })
    set_window_option(window, "winblend", 0)
    set_window_option(window, "winhl", "Normal:Normal,NormalNC:Normal")

    # Border
    top = _VIM_DATABASE_QUERY_BORDER_CHARS[4] + (_VIM_DATABASE_QUERY_BORDER_CHARS[0] *
                                                 width) + _VIM_DATABASE_QUERY_BORDER_CHARS[5]
    mid = _VIM_DATABASE_QUERY_BORDER_CHARS[3] + (' ' * width) + _VIM_DATABASE_QUERY_BORDER_CHARS[1]
    bot = _VIM_DATABASE_QUERY_BORDER_CHARS[7] + (_VIM_DATABASE_QUERY_BORDER_CHARS[2] *
                                                 width) + _VIM_DATABASE_QUERY_BORDER_CHARS[6]
    top = string_compose(top, 1, _VIM_DATABASE_QUERY_TITLE)

    border_lines = [top] + ([mid] * height) + [bot]
    border_buffer = create_buffer({}, {
        "buftype": "nofile",
        "bufhidden": "wipe",
        "synmaxcol": 3000,
        "swapfile": False,
        "buflisted": False,
        "modifiable": False,
        "filetype": _VIM_DATABASE_QUERY_FILE_TYPE,
    })

    window = open_window(
        border_buffer, False, {
            "relative": "editor",
            "width": width + 2,
            "height": height + 2,
            "col": max(col - 1, 0),
            "row": max(row - 1, 0),
            "anchor": "NW",
            "style": "minimal",
            "focusable": False,
        })
    set_window_option(window, "winhl", "Normal:Normal")
    set_window_option(window, "cursorcolumn", False)
    set_window_option(window, "colorcolumn", '')

    instruction = _buf_set_lines(border_buffer, border_lines, False)
    call_atomic(*instruction)
