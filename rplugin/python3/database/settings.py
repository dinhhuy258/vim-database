from dataclasses import dataclass
from typing import Dict

_DEFAULT_DATABASE_MAPPINGS = {
    "show_connections": ["C"],
    "show_databases": ["D"],
    "show_tables": ["T"],
    "show_query": ["R"],
    "quit": ["q"],
    "delete": ["d"],
    "new": ["n"],
    "info": ["i"],
    "select": ["s"],
    "filter": ["f"],
    "clear_filter": ["F"],
}

_DEFAULT_DATABASE_QUERY_MAPPINGS = {
    "quit": ["q"],
    "run_query": ["r"],
}


@dataclass(frozen=True)
class Settings:
    results_limit: int
    mappings: Dict
    query_mappings: Dict


async def load_settings() -> Settings:
    mappings = _DEFAULT_DATABASE_MAPPINGS
    mappings = {f"VimDatabase_{function}": mappings for function, mappings in mappings.items()}

    query_mappings = _DEFAULT_DATABASE_QUERY_MAPPINGS
    query_mappings = {
        f"VimDatabaseQuery_{function}": query_mappings for function, query_mappings in query_mappings.items()
    }

    return Settings(results_limit=50, mappings=mappings, query_mappings=query_mappings)
