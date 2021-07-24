from typing import Optional, Tuple

from pynvim.api.buffer import Buffer
from pynvim.api.window import Window

from ..configs.config import UserConfig
from ..utils.nvim import (
    find_windows_in_tab,
    get_buffer_option,
    create_buffer,
    create_window,
    close_window,
    set_buffer_in_window,
    get_buffer_in_window,
    get_current_cursor,
    get_lines,
    get_window_width,
    set_window_width,
    WindowLayout,
)

_VIM_DATABASE_FILE_TYPE = 'VimDatabase'


def open_database_window(settings: UserConfig) -> Window:
    window = _find_database_window_in_tab()
    if window is None:
        window = _open_database_window(settings)

    return window


def close_database_window() -> None:
    window = _find_database_window_in_tab()
    if window is not None:
        close_window(window, True)


def get_current_database_window_line() -> Optional[str]:
    window = _find_database_window_in_tab()
    if window is None:
        return None

    buffer: Buffer = get_buffer_in_window(window)
    row, _ = get_current_cursor(window)
    lines = get_lines(buffer, row - 1, row)
    return None if len(lines) == 0 else lines[0]


def get_current_database_window_cursor() -> Optional[Tuple[int, int]]:
    window = _find_database_window_in_tab()
    if window is None:
        return None

    return get_current_cursor(window)


def get_current_database_window_row() -> Optional[int]:
    window = _find_database_window_in_tab()
    if window is None:
        return None
    row, _ = get_current_cursor(window)
    return row


def is_database_window_open() -> bool:
    return _find_database_window_in_tab() is not None


def resize(direction: int) -> None:
    window = _find_database_window_in_tab()
    if window is None:
        return
    width = get_window_width(window)
    set_window_width(window, width + direction)


def _find_database_window_in_tab() -> Optional[Window]:
    for window in find_windows_in_tab():
        buffer: Buffer = get_buffer_in_window(window)
        buffer_file_type = get_buffer_option(buffer, 'filetype')
        if buffer_file_type == _VIM_DATABASE_FILE_TYPE:
            return window
    return None


def _get_window_layout(window_layout: str) -> WindowLayout:
    if window_layout == "left":
        return WindowLayout.LEFT
    if window_layout == "right":
        return WindowLayout.RIGHT
    if window_layout == "above":
        return WindowLayout.ABOVE
    if window_layout == "below":
        return WindowLayout.BELOW

    # Fallback layout
    return WindowLayout.LEFT


def _open_database_window(settings: UserConfig) -> Window:
    buffer = create_buffer(
        settings.mappings, {
            'buftype': 'nofile',
            'bufhidden': 'hide',
            'swapfile': False,
            'buflisted': False,
            'modifiable': False,
            'filetype': _VIM_DATABASE_FILE_TYPE,
        })
    window = create_window(settings.window_size, _get_window_layout(settings.window_layout), {
        'list': False,
        'number': False,
        'relativenumber': False,
        'wrap': False,
    })
    set_buffer_in_window(window, buffer)
    return window
