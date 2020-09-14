from dataclasses import dataclass
from typing import Dict

_DEFAULT_DATABASE_MAPPINGS = {
    'quit': ['q'],
    'select_connection': ['s'],
}


@dataclass(frozen=True)
class Settings:
    results_limit: int
    mappings: Dict


async def load_settings() -> Settings:
    mappings = _DEFAULT_DATABASE_MAPPINGS
    mappings = {f"VimDatabase_{function}": mappings for function, mappings in mappings.items()}

    return Settings(results_limit=50, mappings=mappings)
