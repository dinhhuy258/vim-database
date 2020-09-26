from dataclasses import dataclass
from typing import Dict

_DEFAULT_DATABASE_MAPPINGS = {
    "show_connections": ["<c-c>"],
    "show_databases": ["<c-d>"],
    "show_tables": ["<c-t>"],
    "show_query": ["<c-r>"],
    "quit": ["q"],
    "delete": ["d"],
    "new": ["c"],
    "show_insert_query": ["C"],
    "copy": ["p"],
    "show_copy_query": ["P"],
    "edit": ["m"],
    "show_update_query": ["M"],
    "info": ["."],
    "select": ["s"],
    "filter": ["f"],
    "refresh": ["r"],
    "filter_column": ["a"],
    "clear_filter_column": ["A"],
    "clear_filter": ["F"],
    'bigger': ["=", "+"],
    'smaller': ["_", "-"],
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
