from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    results_limit: int


async def load_settings() -> Settings:
    return Settings(results_limit=50)
