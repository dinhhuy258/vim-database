from pynvim import Nvim
from pynvim.api.common import NvimError
from pynvim.api.window import Window
from pynvim.api.buffer import Buffer
from pynvim.api.tabpage import Tabpage
from enum import Enum
from asyncio import Future
from typing import (
    Any,
    Awaitable,
    Callable,
    TypeVar,
    Iterator,
    Tuple,
    Sequence,
    Dict,
    Sequence,
)

T = TypeVar("T")


class WindowLayout(Enum):
    LEFT = 1
    RIGHT = 2
    ABOVE = 3
    BELOW = 4


def _buf_set_lines(buffer: Buffer, lines: list) -> Iterator[Tuple[str, Sequence[Any]]]:
    modifiable = get_buffer_option(buffer, "modifiable")
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", True)

    yield "nvim_buf_set_lines", (buffer, 0, -1, True, [line.rstrip('\n') for line in lines])
    if not modifiable:
        yield "nvim_buf_set_option", (buffer, "modifiable", False)


def init_nvim(nvim: Nvim) -> None:
    global _nvim
    _nvim = nvim


def call_atomic(*instructions: Tuple[str, Sequence[Any]]) -> None:
    inst = tuple((f"{instruction}", args) for instruction, args in instructions)
    out, error = _nvim.api.call_atomic(inst)
    if error:
        raise NvimError(error)


def call_function(name, argument) -> Any:
    return _nvim.call(name, argument)


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


def execute(command: str) -> Any:
    return _nvim.funcs.execute(command)


def find_windows_in_tab() -> Iterator[Window]:

    def key_by(window: Window) -> Tuple[int, int]:
        row, col = _nvim.api.win_get_position(window)
        return (col, row)

    tab: Tabpage = _nvim.api.get_current_tabpage()
    windows: Sequence[Window] = _nvim.api.tabpage_list_wins(tab)

    for window in sorted(windows, key=key_by):
        if not _nvim.api.win_get_option(window, "previewwindow"):
            yield window


def create_buffer(keymaps: Dict[str, Sequence[str]] = dict(), options: Dict[str, Any] = dict()) -> Buffer:
    mapping_options = {"noremap": True, "silent": True, "nowait": True}
    buffer: Buffer = _nvim.api.create_buf(False, True)

    for function, mappings in keymaps.items():
        for mapping in mappings:
            _nvim.api.buf_set_keymap(buffer, "n", mapping, f"<cmd>call {function}(v:false)<cr>", mapping_options)

    for option_name, option_value in options.items():
        _nvim.api.buf_set_option(buffer, option_name, option_value)

    return buffer


def set_buffer_var(buffer_handle: int, var_name: str, var_value: Any) -> None:
    _nvim.funcs.setbufvar(buffer_handle, var_name, var_value)


def get_buffer_var(buffer_handle: int, var_name: str, default_value: Any) -> None:
    _nvim.funcs.getbufvar(buffer_handle, var_name, default_value)


def create_window(size: int, layout: WindowLayout, options: Dict[str, Any] = dict()) -> Window:
    split_right = _nvim.api.get_option("splitright")
    split_below = _nvim.api.get_option("splitbelow")

    windows: Sequence[Window] = tuple(window for window in find_windows_in_tab())

    focus_win = windows[0]

    _nvim.api.set_current_win(focus_win)
    if layout is WindowLayout.LEFT:
        _nvim.api.set_option("splitright", False)
        _nvim.command(f"{size}vsplit")
    elif layout is WindowLayout.RIGHT:
        _nvim.api.set_option("splitright", True)
        _nvim.command(f"{size}vsplit")
    elif layout is WindowLayout.BELOW:
        _nvim.api.set_option("splitbelow", True)
        _nvim.command(f"{size}split")
    else:
        # Above layout
        _nvim.api.set_option("splitbelow", False)
        _nvim.command(f"{size}split")

    _nvim.api.set_option("splitright", split_right)
    _nvim.api.set_option("splitbelow", split_below)

    window: Window = _nvim.api.get_current_win()
    for option_name, option_value in options.items():
        _nvim.api.win_set_option(window, option_name, option_value)

    return window


def get_window_info(winid: int) -> list():
    return _nvim.funcs.getwininfo(winid)


def open_window(buffer: Buffer, enter: bool, opts: Dict[str, Any]) -> Window:
    return _nvim.api.open_win(buffer, enter, opts)


def set_window_option(window: Window, option_name: str, option_value: Any) -> None:
    _nvim.api.win_set_option(window, option_name, option_value)


def get_current_cursor(window: Window) -> Tuple[int, int]:
    return _nvim.api.win_get_cursor(window)


def close_window(window: Window, force: bool) -> None:
    _nvim.api.win_close(window, force)


def set_buffer_in_window(window: Window, buffer: Buffer) -> None:
    _nvim.api.win_set_buf(window, buffer)


def get_buffer_in_window(window: Window) -> Buffer:
    return _nvim.api.win_get_buf(window)


def get_buffer_option(buffer: Buffer, option: str) -> Any:
    return _nvim.api.buf_get_option(buffer, option)


def get_buffer_content(buffer: Buffer) -> list:
    return get_lines(buffer, 0, get_line_count(buffer))


def get_lines(buffer: Buffer, start: int, end: int) -> list:
    return _nvim.api.buf_get_lines(buffer, start, end, False)


def get_line_count(buffer: Buffer) -> int:
    return _nvim.api.buf_line_count(buffer)


def confirm(question: str) -> bool:
    return _nvim.funcs.confirm(question, "&Yes\n&No", 2) == 1


def get_input(question: str, default: str = "") -> str:
    return _nvim.funcs.input(question, default)


def get_buffer_in_window(window: Window) -> Buffer:
    return _nvim.api.win_get_buf(window)


def get_global_var(name: str, default_value: Any) -> Any:
    try:
        return _nvim.api.get_var(name)
    except:
        return default_value


def get_option(name: str) -> Any:
    return _nvim.api.get_option(name)


def get_window_width(window: Window) -> int:
    return _nvim.api.win_get_width(window)


def set_window_width(window: Window, width: int) -> None:
    _nvim.api.win_set_width(window, width)


def set_cursor(window: Window, cursor: Tuple[int, int]) -> None:
    _nvim.api.win_set_cursor(window, cursor)


def render(window: Window, lines: list) -> None:
    buffer: Buffer = get_buffer_in_window(window)
    instruction = _buf_set_lines(buffer, lines)
    call_atomic(*instruction)
