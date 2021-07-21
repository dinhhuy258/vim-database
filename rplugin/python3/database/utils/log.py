from pynvim import Nvim
from logging import Formatter, Handler, LogRecord, ERROR, INFO, getLogger

_LOGGER_NAME = 'VimDatabase'

_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_LOG_FMT = """
--  {name} level:    {levelname}
time:     {asctime}
module:   {module}
line:     {lineno}
function: {funcName}
{message}
"""


def init_log(nvim: Nvim) -> None:

    class NvimHandler(Handler):

        def handle(self, log_record: LogRecord) -> None:
            message = self.format(log_record)
            if log_record.levelno >= ERROR:
                nvim.async_call(nvim.err_write, message)
            else:
                nvim.async_call(nvim.out_write, message)

    handler = NvimHandler()
    handler.setFormatter(Formatter(fmt=_LOG_FMT, datefmt=_DATE_FMT, style="{"))

    logger = getLogger(_LOGGER_NAME)
    logger.addHandler(handler)
    logger.setLevel(INFO)


log = getLogger(_LOGGER_NAME)
