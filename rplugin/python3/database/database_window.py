from pynvim.api.buffer import Buffer
from pynvim.api.window import Window
from typing import Optional, Tuple, Iterator, Sequence, Any
from .settings import Settings
from .nvim import (
    call_atomic,
    async_call,
    find_windows_in_tab,
    get_buffer_in_window,
    get_buffer_option,
    create_buffer,
    create_window,
    set_buffer_in_window,
    get_buffer_in_window,
    WindowLayout,
)

_VIM_DATABASE_FILE_TYPE = 'VimDatabase'


def _find_database_window_in_tab() -> Optional[Window]:
    for window in find_windows_in_tab():
        buffer: Buffer = get_buffer_in_window(window)
        buffer_file_type = get_buffer_option(buffer, 'filetype')
        if buffer_file_type == _VIM_DATABASE_FILE_TYPE:
            return window
    return None


def _open_database_window() -> Window:
    buffer = create_buffer(
        dict(), {
            'buftype': 'nofile',
            'bufhidden': 'hide',
            'swapfile': False,
            'buflisted': False,
            'modifiable': False,
            'filetype': _VIM_DATABASE_FILE_TYPE,
        })
    window = create_window(100, WindowLayout.LEFT, {
        'list': False,
        'number': False,
        'relativenumber': False,
        'wrap': False,
    })
    set_buffer_in_window(window, buffer)
    return window


def _buf_set_lines(buffer: Buffer, lines: list, modifiable: bool) -> Iterator[Tuple[str, Sequence[Any]]]:
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", True)

    yield "nvim_buf_set_lines", (buffer, 0, -1, True, [line.rstrip('\n') for line in lines])
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", False)


def open_database_window() -> Window:
    window = _find_database_window_in_tab()
    if window is None:
        window = _open_database_window()

    return window


def render(window: Window, lines: list) -> None:
    buffer: Buffer = get_buffer_in_window(window)
    instruction = _buf_set_lines(buffer, lines, False)
    call_atomic(*instruction)
