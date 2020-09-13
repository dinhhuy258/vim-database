from .settings import Settings
from .logging import log


async def open_connection(settings: Settings) -> None:
    log.info("Open connection")
