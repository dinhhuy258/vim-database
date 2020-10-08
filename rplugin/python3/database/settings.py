from dataclasses import dataclass
from typing import Dict
from functools import partial
from .nvim import get_global_var, async_call

_DEFAULT_DATABASE_MAPPINGS = {
    "show_connections": ["<Leader>c"],
    "show_databases": ["<Leader>d"],
    "show_tables": ["<Leader>t"],
    "show_query": ["<Leader>r"],
    "quit": ["q"],
    "delete": ["d"],
    "new": ["c"],
    "show_insert_query": ["C"],
    "copy": ["p"],
    "show_copy_query": ["P"],
    "edit": ["m"],
    "show_update_query": ["M"],
    "info": ["."],
    "select": ["x"],
    "sort": ["s"],
    "sort_reverse": ["S"],
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
    window_layout: str
    window_size: int
    mappings: Dict
    query_mappings: Dict


async def load_settings() -> Settings:
    mappings = await async_call(partial(get_global_var, "vim_database_mappings", _DEFAULT_DATABASE_MAPPINGS))
    mappings = {f"VimDatabase_{function}": mappings for function, mappings in mappings.items()}

    query_mappings = await async_call(
        partial(get_global_var, "vim_database_query_mappings", _DEFAULT_DATABASE_QUERY_MAPPINGS))
    query_mappings = {
        f"VimDatabaseQuery_{function}": query_mappings for function, query_mappings in query_mappings.items()
    }

    results_limit = await async_call(partial(get_global_var, "vim_database_results_limit", 50))
    window_layout = await async_call(partial(get_global_var, "vim_database_window_layout", "left"))
    window_size = await async_call(partial(get_global_var, "vim_database_window_size", 100))

    return Settings(results_limit=results_limit,
                    window_layout=window_layout,
                    window_size=window_size,
                    mappings=mappings,
                    query_mappings=query_mappings)
