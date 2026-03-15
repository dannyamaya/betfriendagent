from __future__ import annotations

from datetime import date, datetime

import asyncpg
from loguru import logger

from betfriend.config.settings import settings


class Store:
    """Async PostgreSQL store backed by asyncpg."""

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        self._pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            ssl=ctx,
            statement_cache_size=0,  # required for pgbouncer / Supabase pooler
        )
        await self._create_tables()
        await self._seed_competitions()
        logger.info("DB store started")

    async def stop(self) -> None:
        if self._pool:
            await self._pool.close()
            logger.info("DB store stopped")

    @property
    def pool(self) -> asyncpg.Pool:
        assert self._pool is not None, "Store not started"
        return self._pool

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    async def _create_tables(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS competitions (
                    id          SERIAL PRIMARY KEY,
                    api_id      INT UNIQUE NOT NULL,
                    name        TEXT NOT NULL,
                    season      INT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS teams (
                    id          SERIAL PRIMARY KEY,
                    api_id      INT UNIQUE NOT NULL,
                    name        TEXT NOT NULL,
                    short_name  TEXT,
                    logo_url    TEXT,
                    competition_id INT REFERENCES competitions(id)
                );

                CREATE TABLE IF NOT EXISTS fixtures (
                    id              SERIAL PRIMARY KEY,
                    api_id          INT UNIQUE NOT NULL,
                    competition_id  INT REFERENCES competitions(id),
                    home_team_id    INT REFERENCES teams(id),
                    away_team_id    INT REFERENCES teams(id),
                    referee_id      INT,
                    matchday        INT,
                    kickoff         TIMESTAMPTZ NOT NULL,
                    status          TEXT DEFAULT 'scheduled',
                    home_score      INT,
                    away_score      INT,
                    home_yc         INT DEFAULT 0,
                    away_yc         INT DEFAULT 0,
                    home_rc         INT DEFAULT 0,
                    away_rc         INT DEFAULT 0,
                    processed       BOOLEAN DEFAULT FALSE,
                    api_fetched_at  TIMESTAMPTZ
                );

                CREATE TABLE IF NOT EXISTS api_request_log (
                    id              SERIAL PRIMARY KEY,
                    endpoint        TEXT NOT NULL,
                    params          JSONB,
                    response_code   INT,
                    requested_at    TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        logger.info("Tables created / verified")

    async def _seed_competitions(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO competitions (api_id, name, season)
                VALUES ($1, $2, $3)
                ON CONFLICT (api_id) DO NOTHING
            """, settings.la_liga_id, "La Liga", settings.season)
            await conn.execute("""
                INSERT INTO competitions (api_id, name, season)
                VALUES ($1, $2, $3)
                ON CONFLICT (api_id) DO NOTHING
            """, settings.la_liga2_id, "La Liga 2", settings.season)

    # ------------------------------------------------------------------
    # Competitions
    # ------------------------------------------------------------------

    async def get_competition_id(self, api_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM competitions WHERE api_id = $1", api_id
            )
            assert row is not None, f"Competition {api_id} not found"
            return row["id"]

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    async def upsert_team(
        self, api_id: int, name: str, short_name: str | None,
        logo_url: str | None, competition_id: int
    ) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO teams (api_id, name, short_name, logo_url, competition_id)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (api_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        short_name = EXCLUDED.short_name,
                        logo_url = EXCLUDED.logo_url,
                        competition_id = EXCLUDED.competition_id
                RETURNING id
            """, api_id, name, short_name, logo_url, competition_id)
            return row["id"]  # type: ignore[index]

    async def get_team_id(self, api_id: int) -> int | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM teams WHERE api_id = $1", api_id
            )
            return row["id"] if row else None

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    async def upsert_fixture(
        self, *, api_id: int, competition_id: int,
        home_team_id: int, away_team_id: int,
        matchday: int | None, kickoff: datetime, status: str,
        home_score: int | None = None, away_score: int | None = None,
        home_yc: int = 0, away_yc: int = 0,
        home_rc: int = 0, away_rc: int = 0,
    ) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO fixtures (
                    api_id, competition_id, home_team_id, away_team_id,
                    matchday, kickoff, status, home_score, away_score,
                    home_yc, away_yc, home_rc, away_rc, api_fetched_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, NOW())
                ON CONFLICT (api_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        home_score = EXCLUDED.home_score,
                        away_score = EXCLUDED.away_score,
                        home_yc = EXCLUDED.home_yc,
                        away_yc = EXCLUDED.away_yc,
                        home_rc = EXCLUDED.home_rc,
                        away_rc = EXCLUDED.away_rc,
                        api_fetched_at = NOW()
                RETURNING id
            """, api_id, competition_id, home_team_id, away_team_id,
                matchday, kickoff, status, home_score, away_score,
                home_yc, away_yc, home_rc, away_rc)
            return row["id"]  # type: ignore[index]

    async def get_fixtures_by_date(self, target_date: date) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT f.*,
                       ht.name AS home_team_name,
                       at.name AS away_team_name,
                       c.name  AS competition_name
                FROM fixtures f
                JOIN teams ht ON f.home_team_id = ht.id
                JOIN teams at ON f.away_team_id = at.id
                JOIN competitions c ON f.competition_id = c.id
                WHERE DATE(f.kickoff AT TIME ZONE 'Europe/Madrid') = $1
                ORDER BY f.kickoff
            """, target_date)

    async def get_unprocessed_upcoming(
        self, from_dt: datetime, to_dt: datetime
    ) -> list[asyncpg.Record]:
        """Get fixtures starting between from_dt and to_dt that haven't been processed."""
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT f.*,
                       ht.name AS home_team_name,
                       at.name AS away_team_name,
                       c.name  AS competition_name
                FROM fixtures f
                JOIN teams ht ON f.home_team_id = ht.id
                JOIN teams at ON f.away_team_id = at.id
                JOIN competitions c ON f.competition_id = c.id
                WHERE f.kickoff BETWEEN $1 AND $2
                  AND f.processed = FALSE
                  AND f.status = 'scheduled'
                ORDER BY f.kickoff
            """, from_dt, to_dt)

    async def mark_fixture_processed(self, fixture_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE fixtures SET processed = TRUE WHERE id = $1",
                fixture_id,
            )

    # ------------------------------------------------------------------
    # API Request Log
    # ------------------------------------------------------------------

    async def log_api_request(
        self, endpoint: str, params: dict | None, response_code: int
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO api_request_log (endpoint, params, response_code)
                VALUES ($1, $2::jsonb, $3)
            """, endpoint, __import__("json").dumps(params) if params else None,
                response_code)

    async def count_requests_today(self) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) AS cnt
                FROM api_request_log
                WHERE requested_at >= CURRENT_DATE
            """)
            return row["cnt"]  # type: ignore[index]
