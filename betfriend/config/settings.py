from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Supabase PostgreSQL
    database_url: str

    # API-Football
    api_football_key: str
    api_football_base_url: str = "https://v3.football.api-sports.io"

    # Telegram
    telegram_token: str
    telegram_chat_id: str

    # API budget
    api_daily_limit: int = 100
    api_reserve: int = 20  # always keep 20 for critical pre-game calls

    # Leagues
    la_liga_id: int = 140
    la_liga2_id: int = 141
    season: int = 2025

    # Timezone
    timezone: str = "Europe/Madrid"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()  # type: ignore[call-arg]
