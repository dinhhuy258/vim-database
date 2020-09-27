from pynvim.api.buffer import Buffer
from pynvim.api.window import Window
from typing import Tuple, Iterator, Sequence, Any, Optional
from .utils import string_compose
from .logging import log
from .nvim import (
    call_atomic,
    execute,
    get_option,
    create_buffer,
    set_buffer_var,
    get_buffer_var,
    open_window,
    get_window_info,
    set_window_option,
    find_windows_in_tab,
    get_buffer_in_window,
    get_buffer_option,
    close_window,
    get_buffer_content,
)
from .settings import Settings

_VIM_DATABASE_QUERY_FILE_TYPE = "VimDatabaseQuery"
_VIM_DATABASE_QUERY_TITLE = "[vim-database] Query"
_VIM_DATABASE_QUERY_BORDER_CHARS = ['─', '│', '─', '│', '┌', '┐', '┘', '└']
_query_buffer: Buffer = None


def _find_window_by_winid(winid: int) -> Optional[Window]:
    for window in find_windows_in_tab():
        if window.handle == winid:
            return window

    return None


def _find_query_window() -> Optional[Window]:
    for window in find_windows_in_tab():
        buffer: Buffer = get_buffer_in_window(window)
        buffer_file_type = get_buffer_option(buffer, "filetype")
        if buffer_file_type == _VIM_DATABASE_QUERY_FILE_TYPE:
            return window

    return None


def _find_query_buffer() -> Optional[Buffer]:
    query_window = _find_query_window()
    if query_window is not None:
        return query_window.buffer

    return None


def _buf_set_lines(buffer: Buffer, lines: list, modifiable: bool) -> Iterator[Tuple[str, Sequence[Any]]]:
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", True)

    yield "nvim_buf_set_lines", (buffer, 0, -1, True, [line.rstrip('\n') for line in lines])
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", False)


def close_query_window() -> None:
    query_window = _find_query_window()
    if query_window is not None:
        close_window(query_window, True)


def open_query_window(settings: Settings) -> Optional[Window]:
    query_window = _find_query_window()
    if query_window is not None:
        return query_window

    global _query_buffer
    if _query_buffer is None:
        _query_buffer = create_buffer(
            settings.query_mappings, {
                "buftype": "nofile",
                "bufhidden": "hide",
                "swapfile": False,
                "buflisted": False,
                "modifiable": True,
                "filetype": _VIM_DATABASE_QUERY_FILE_TYPE,
                "syntax": "sql",
            })

    border_winid = get_buffer_var(_query_buffer.handle, "border_winid", -1)
    if len(get_window_info(border_winid)) != 0:
        border_window = _find_window_by_winid(border_winid)
        if border_window is not None:
            close_window(border_window, True)

    height = int((get_option("lines") - 2) / 1.5)
    width = int(get_option("columns") / 1.5)
    row = int((get_option("lines") - height) / 2)
    col = int((get_option("columns") - width) / 2)
    window = open_window(_query_buffer, True, {
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
    })

    border_window = open_window(
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

    execute("autocmd BufHidden <buffer=" + str(_query_buffer.handle) + "> ++once call CloseVimDatabaseQueryBorder(" +
            str(_query_buffer.handle) + ")")
    execute("autocmd BufLeave <buffer=" + str(_query_buffer.handle) + "> ++once call CloseVimDatabaseQuery(" +
            str(_query_buffer.handle) + ")")
    set_buffer_var(_query_buffer.handle, "border_winid", border_window.handle)
    set_window_option(border_window, "winhl", "Normal:Normal")
    set_window_option(border_window, "cursorcolumn", False)
    set_window_option(border_window, "colorcolumn", "")

    instruction = _buf_set_lines(border_buffer, border_lines, False)
    call_atomic(*instruction)

    return window


def get_query() -> Optional[str]:
    buffer = _find_query_buffer()
    if buffer is None:
        return None
    buffer_content = get_buffer_content(buffer)
    buffer_content = list(map(lambda line: line.strip(), buffer_content))
    sql_query = ' '.join(buffer_content).strip()

    return None if len(sql_query) == 0 else sql_query


def is_query_window_opened() -> bool:
    query_window = _find_query_window()

    return query_window is not None
