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

                CREATE TABLE IF NOT EXISTS referees (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT UNIQUE NOT NULL,
                    total_yc    INT DEFAULT 0,
                    total_rc    INT DEFAULT 0,
                    games       INT DEFAULT 0,
                    yc_per_game REAL DEFAULT 0,
                    rc_per_game REAL DEFAULT 0,
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS players (
                    id          SERIAL PRIMARY KEY,
                    api_id      INT UNIQUE NOT NULL,
                    name        TEXT NOT NULL,
                    photo_url   TEXT,
                    team_id     INT REFERENCES teams(id),
                    position    TEXT
                );

                CREATE TABLE IF NOT EXISTS player_card_stats (
                    id              SERIAL PRIMARY KEY,
                    player_id       INT REFERENCES players(id) UNIQUE,
                    total_yc        INT DEFAULT 0,
                    total_rc        INT DEFAULT 0,
                    games_played    INT DEFAULT 0,
                    yc_per_game     REAL DEFAULT 0,
                    rc_per_game     REAL DEFAULT 0,
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS player_fixture_cards (
                    id              SERIAL PRIMARY KEY,
                    player_id       INT REFERENCES players(id),
                    fixture_id      INT REFERENCES fixtures(id),
                    yellow_cards    INT DEFAULT 0,
                    red_cards       INT DEFAULT 0,
                    minutes_played  INT DEFAULT 0,
                    UNIQUE(player_id, fixture_id)
                );

                CREATE TABLE IF NOT EXISTS team_season_stats (
                    id              SERIAL PRIMARY KEY,
                    team_id         INT REFERENCES teams(id) UNIQUE,
                    games_played    INT DEFAULT 0,
                    total_yc        INT DEFAULT 0,
                    total_rc        INT DEFAULT 0,
                    yc_per_game     REAL DEFAULT 0,
                    rc_per_game     REAL DEFAULT 0,
                    standing_pos    INT,
                    standing_pts    INT,
                    form            TEXT,
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS team_form (
                    id              SERIAL PRIMARY KEY,
                    team_id         INT REFERENCES teams(id),
                    fixture_id      INT REFERENCES fixtures(id),
                    yc              INT DEFAULT 0,
                    rc              INT DEFAULT 0,
                    is_home         BOOLEAN,
                    match_date      DATE,
                    UNIQUE(team_id, fixture_id)
                );

                CREATE TABLE IF NOT EXISTS head2head (
                    id              SERIAL PRIMARY KEY,
                    team_a_id       INT REFERENCES teams(id),
                    team_b_id       INT REFERENCES teams(id),
                    fixture_api_id  INT,
                    match_date      DATE,
                    team_a_yc       INT DEFAULT 0,
                    team_a_rc       INT DEFAULT 0,
                    team_b_yc       INT DEFAULT 0,
                    team_b_rc       INT DEFAULT 0,
                    team_a_score    INT,
                    team_b_score    INT,
                    UNIQUE(team_a_id, team_b_id, fixture_api_id)
                );

                CREATE TABLE IF NOT EXISTS fixture_lineups (
                    id              SERIAL PRIMARY KEY,
                    fixture_id      INT REFERENCES fixtures(id),
                    player_id       INT REFERENCES players(id),
                    team_id         INT REFERENCES teams(id),
                    is_starter      BOOLEAN DEFAULT TRUE,
                    position        TEXT,
                    grid_pos        TEXT,
                    UNIQUE(fixture_id, player_id)
                );

                CREATE TABLE IF NOT EXISTS predictions (
                    id              SERIAL PRIMARY KEY,
                    fixture_id      INT REFERENCES fixtures(id) UNIQUE,
                    predicted_yc    REAL,
                    predicted_home_yc REAL,
                    predicted_away_yc REAL,
                    predicted_rc    REAL,
                    rc_probability  TEXT,
                    confidence      TEXT,
                    actual_yc       INT,
                    actual_home_yc  INT,
                    actual_away_yc  INT,
                    actual_rc       INT,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
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
                WHERE DATE(f.kickoff AT TIME ZONE 'America/Bogota') = $1
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
    # Referees
    # ------------------------------------------------------------------

    async def upsert_referee(self, name: str) -> int:
        """Upsert referee with fuzzy matching on last name."""
        async with self.pool.acquire() as conn:
            # Exact match first
            row = await conn.fetchrow("SELECT id FROM referees WHERE name = $1", name)
            if row:
                return row["id"]

            # Fuzzy match: find by last name similarity
            # Extract last significant word (skip initials like "J.")
            words = [w for w in name.split() if len(w) > 2 and not w.endswith(".")]
            if words:
                last_word = words[-1]  # Usually the last name
                candidates = await conn.fetch(
                    "SELECT id, name FROM referees WHERE name ILIKE $1 AND games > 0",
                    f"%{last_word}%"
                )
                if len(candidates) == 1:
                    # Unique match on last name — use existing referee
                    return candidates[0]["id"]
                elif len(candidates) > 1:
                    # Multiple matches — try matching more words
                    for candidate in candidates:
                        c_words = [w for w in candidate["name"].split() if len(w) > 2 and not w.endswith(".")]
                        # Check if first significant word also matches
                        if len(words) >= 2 and len(c_words) >= 2:
                            if words[0].lower()[:3] == c_words[0].lower()[:3]:
                                return candidate["id"]

            # No match found — create new
            row = await conn.fetchrow("""
                INSERT INTO referees (name) VALUES ($1)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, name)
            return row["id"]  # type: ignore[index]

    async def get_referee_by_name(self, name: str) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM referees WHERE name = $1", name
            )

    async def assign_referee_to_fixture(self, fixture_id: int, referee_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE fixtures SET referee_id = $1 WHERE id = $2",
                referee_id, fixture_id
            )

    async def recompute_referee_stats(self) -> None:
        """Recompute all referee stats from fixture card data."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE referees r SET
                    total_yc = sub.total_yc,
                    total_rc = sub.total_rc,
                    games = sub.games,
                    yc_per_game = CASE WHEN sub.games > 0 THEN sub.total_yc::REAL / sub.games ELSE 0 END,
                    rc_per_game = CASE WHEN sub.games > 0 THEN sub.total_rc::REAL / sub.games ELSE 0 END,
                    updated_at = NOW()
                FROM (
                    SELECT f.referee_id,
                           COUNT(*) AS games,
                           COALESCE(SUM(f.home_yc + f.away_yc), 0) AS total_yc,
                           COALESCE(SUM(f.home_rc + f.away_rc), 0) AS total_rc
                    FROM fixtures f
                    WHERE f.referee_id IS NOT NULL
                      AND f.status = 'finished'
                    GROUP BY f.referee_id
                ) sub
                WHERE r.id = sub.referee_id
            """)

    async def get_referee_stats(self, referee_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM referees WHERE id = $1", referee_id)

    async def get_referee_yc_rank(self, referee_id: int) -> int | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT rank FROM (
                    SELECT id, RANK() OVER (ORDER BY yc_per_game DESC) AS rank
                    FROM referees WHERE games >= 3
                ) sub WHERE sub.id = $1
            """, referee_id)
            return row["rank"] if row else None

    async def get_referee_last_games(self, referee_id: int, n: int = 3) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT f.home_yc + f.away_yc AS total_yc,
                       f.home_rc + f.away_rc AS total_rc,
                       ht.name AS home_team, at.name AS away_team,
                       f.kickoff
                FROM fixtures f
                JOIN teams ht ON f.home_team_id = ht.id
                JOIN teams at ON f.away_team_id = at.id
                WHERE f.referee_id = $1 AND f.status = 'finished'
                ORDER BY f.kickoff DESC
                LIMIT $2
            """, referee_id, n)

    async def get_total_referees_with_games(self) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) AS cnt FROM referees WHERE games >= 3")
            return row["cnt"]  # type: ignore[index]

    # ------------------------------------------------------------------
    # Players
    # ------------------------------------------------------------------

    async def upsert_player(
        self, api_id: int, name: str, photo_url: str | None,
        team_id: int, position: str | None
    ) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO players (api_id, name, photo_url, team_id, position)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (api_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        photo_url = EXCLUDED.photo_url,
                        team_id = EXCLUDED.team_id,
                        position = EXCLUDED.position
                RETURNING id
            """, api_id, name, photo_url, team_id, position)
            return row["id"]  # type: ignore[index]

    async def get_player_id(self, api_id: int) -> int | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM players WHERE api_id = $1", api_id
            )
            return row["id"] if row else None

    # ------------------------------------------------------------------
    # Player Card Stats
    # ------------------------------------------------------------------

    async def upsert_player_card_stats(
        self, player_id: int, total_yc: int, total_rc: int, games_played: int
    ) -> None:
        yc_pg = total_yc / games_played if games_played > 0 else 0
        rc_pg = total_rc / games_played if games_played > 0 else 0
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO player_card_stats (player_id, total_yc, total_rc, games_played, yc_per_game, rc_per_game, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT (player_id) DO UPDATE
                    SET total_yc = EXCLUDED.total_yc,
                        total_rc = EXCLUDED.total_rc,
                        games_played = EXCLUDED.games_played,
                        yc_per_game = EXCLUDED.yc_per_game,
                        rc_per_game = EXCLUDED.rc_per_game,
                        updated_at = NOW()
            """, player_id, total_yc, total_rc, games_played, yc_pg, rc_pg)

    # ------------------------------------------------------------------
    # Player Fixture Cards
    # ------------------------------------------------------------------

    async def upsert_player_fixture_card(
        self, player_id: int, fixture_id: int,
        yellow_cards: int, red_cards: int, minutes_played: int
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO player_fixture_cards (player_id, fixture_id, yellow_cards, red_cards, minutes_played)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (player_id, fixture_id) DO UPDATE
                    SET yellow_cards = EXCLUDED.yellow_cards,
                        red_cards = EXCLUDED.red_cards,
                        minutes_played = EXCLUDED.minutes_played
            """, player_id, fixture_id, yellow_cards, red_cards, minutes_played)

    # ------------------------------------------------------------------
    # Team Season Stats
    # ------------------------------------------------------------------

    async def upsert_team_season_stats(
        self, team_id: int, games_played: int,
        total_yc: int, total_rc: int,
        standing_pos: int | None = None, standing_pts: int | None = None,
        form: str | None = None
    ) -> None:
        yc_pg = total_yc / games_played if games_played > 0 else 0
        rc_pg = total_rc / games_played if games_played > 0 else 0
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO team_season_stats (team_id, games_played, total_yc, total_rc, yc_per_game, rc_per_game, standing_pos, standing_pts, form, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
                ON CONFLICT (team_id) DO UPDATE
                    SET games_played = EXCLUDED.games_played,
                        total_yc = EXCLUDED.total_yc,
                        total_rc = EXCLUDED.total_rc,
                        yc_per_game = EXCLUDED.yc_per_game,
                        rc_per_game = EXCLUDED.rc_per_game,
                        standing_pos = EXCLUDED.standing_pos,
                        standing_pts = EXCLUDED.standing_pts,
                        form = EXCLUDED.form,
                        updated_at = NOW()
            """, team_id, games_played, total_yc, total_rc, yc_pg, rc_pg,
                standing_pos, standing_pts, form)

    # ------------------------------------------------------------------
    # Team Form (per-fixture card history)
    # ------------------------------------------------------------------

    async def upsert_team_form(
        self, team_id: int, fixture_id: int,
        yc: int, rc: int, is_home: bool, match_date: date
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO team_form (team_id, fixture_id, yc, rc, is_home, match_date)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (team_id, fixture_id) DO UPDATE
                    SET yc = EXCLUDED.yc,
                        rc = EXCLUDED.rc,
                        is_home = EXCLUDED.is_home,
                        match_date = EXCLUDED.match_date
            """, team_id, fixture_id, yc, rc, is_home, match_date)

    async def get_team_last_n_form(self, team_id: int, n: int = 5) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT yc, rc, is_home, match_date
                FROM team_form
                WHERE team_id = $1
                ORDER BY match_date DESC
                LIMIT $2
            """, team_id, n)

    async def get_fixture_id_by_api_id(self, api_id: int) -> int | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM fixtures WHERE api_id = $1", api_id
            )
            return row["id"] if row else None

    # ------------------------------------------------------------------
    # Head to Head
    # ------------------------------------------------------------------

    async def upsert_h2h(
        self, team_a_id: int, team_b_id: int, fixture_api_id: int,
        match_date: date, team_a_yc: int, team_a_rc: int,
        team_b_yc: int, team_b_rc: int,
        team_a_score: int | None, team_b_score: int | None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO head2head (team_a_id, team_b_id, fixture_api_id, match_date,
                    team_a_yc, team_a_rc, team_b_yc, team_b_rc, team_a_score, team_b_score)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (team_a_id, team_b_id, fixture_api_id) DO UPDATE
                    SET team_a_yc = EXCLUDED.team_a_yc, team_a_rc = EXCLUDED.team_a_rc,
                        team_b_yc = EXCLUDED.team_b_yc, team_b_rc = EXCLUDED.team_b_rc,
                        team_a_score = EXCLUDED.team_a_score, team_b_score = EXCLUDED.team_b_score
            """, team_a_id, team_b_id, fixture_api_id, match_date,
                team_a_yc, team_a_rc, team_b_yc, team_b_rc, team_a_score, team_b_score)

    async def get_h2h(self, team_a_id: int, team_b_id: int, limit: int = 5) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT h.*, ta.name AS team_a_name, tb.name AS team_b_name
                FROM head2head h
                JOIN teams ta ON ta.id = h.team_a_id
                JOIN teams tb ON tb.id = h.team_b_id
                WHERE (h.team_a_id = $1 AND h.team_b_id = $2)
                   OR (h.team_a_id = $2 AND h.team_b_id = $1)
                ORDER BY h.match_date DESC
                LIMIT $3
            """, team_a_id, team_b_id, limit)

    # ------------------------------------------------------------------
    # Fixture Lineups
    # ------------------------------------------------------------------

    async def upsert_lineup_player(
        self, fixture_id: int, player_id: int, team_id: int,
        is_starter: bool, position: str | None, grid_pos: str | None
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO fixture_lineups (fixture_id, player_id, team_id, is_starter, position, grid_pos)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (fixture_id, player_id) DO UPDATE
                    SET is_starter = EXCLUDED.is_starter,
                        position = EXCLUDED.position,
                        grid_pos = EXCLUDED.grid_pos
            """, fixture_id, player_id, team_id, is_starter, position, grid_pos)

    async def get_fixture_lineup(self, fixture_id: int, team_id: int) -> list[asyncpg.Record]:
        """Get lineup for a team in a fixture, with card stats."""
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT fl.is_starter, fl.position, fl.grid_pos,
                       p.name AS player_name, p.id AS player_id,
                       COALESCE(pcs.total_yc, 0) AS total_yc,
                       COALESCE(pcs.total_rc, 0) AS total_rc,
                       COALESCE(pcs.games_played, 0) AS games_played,
                       COALESCE(pcs.yc_per_game, 0) AS yc_per_game,
                       COALESCE(pcs.rc_per_game, 0) AS rc_per_game
                FROM fixture_lineups fl
                JOIN players p ON p.id = fl.player_id
                LEFT JOIN player_card_stats pcs ON pcs.player_id = p.id
                WHERE fl.fixture_id = $1 AND fl.team_id = $2
                ORDER BY fl.is_starter DESC, pcs.total_yc DESC NULLS LAST
            """, fixture_id, team_id)

    async def has_lineup(self, fixture_id: int) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) AS cnt FROM fixture_lineups WHERE fixture_id = $1",
                fixture_id
            )
            return row["cnt"] > 0  # type: ignore[index]

    # ------------------------------------------------------------------
    # Pre-game analysis queries
    # ------------------------------------------------------------------

    async def get_team_stats(self, team_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT tss.*, t.name AS team_name
                FROM team_season_stats tss
                JOIN teams t ON t.id = tss.team_id
                WHERE tss.team_id = $1
            """, team_id)

    async def get_team_yc_rank(self, team_id: int) -> int | None:
        """Get team's YC ranking within its competition (1 = most cards)."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT rank FROM (
                    SELECT tss.team_id,
                           RANK() OVER (ORDER BY tss.total_yc DESC) AS rank
                    FROM team_season_stats tss
                    JOIN teams t ON t.id = tss.team_id
                    WHERE t.competition_id = (SELECT competition_id FROM teams WHERE id = $1)
                ) sub
                WHERE sub.team_id = $1
            """, team_id)
            return row["rank"] if row else None

    async def get_team_rc_rank(self, team_id: int) -> int | None:
        """Get team's RC ranking within its competition (1 = most cards)."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT rank FROM (
                    SELECT tss.team_id,
                           RANK() OVER (ORDER BY tss.total_rc DESC) AS rank
                    FROM team_season_stats tss
                    JOIN teams t ON t.id = tss.team_id
                    WHERE t.competition_id = (SELECT competition_id FROM teams WHERE id = $1)
                ) sub
                WHERE sub.team_id = $1
            """, team_id)
            return row["rank"] if row else None

    async def get_top_card_players(self, team_id: int, limit: int = 5) -> list[asyncpg.Record]:
        """Get top card-getting players for a team."""
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT p.name, pcs.total_yc, pcs.total_rc, pcs.games_played,
                       pcs.yc_per_game, pcs.rc_per_game
                FROM player_card_stats pcs
                JOIN players p ON p.id = pcs.player_id
                WHERE p.team_id = $1
                  AND pcs.games_played > 0
                ORDER BY pcs.total_yc DESC
                LIMIT $2
            """, team_id, limit)

    async def get_player_league_yc_rank(self, player_id: int) -> int | None:
        """Get player's YC ranking across the entire competition."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT rank FROM (
                    SELECT pcs.player_id,
                           RANK() OVER (ORDER BY pcs.total_yc DESC) AS rank
                    FROM player_card_stats pcs
                    JOIN players p ON p.id = pcs.player_id
                    WHERE p.team_id IN (
                        SELECT id FROM teams WHERE competition_id = (
                            SELECT competition_id FROM teams WHERE id = (
                                SELECT team_id FROM players WHERE id = $1
                            )
                        )
                    )
                    AND pcs.games_played > 0
                ) sub
                WHERE sub.player_id = $1
            """, player_id)
            return row["rank"] if row else None

    # ------------------------------------------------------------------
    # Predictions
    # ------------------------------------------------------------------

    async def save_prediction(
        self, fixture_id: int, predicted_yc: float, predicted_home_yc: float,
        predicted_away_yc: float, predicted_rc: float,
        rc_probability: str, confidence: str,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO predictions (fixture_id, predicted_yc, predicted_home_yc,
                    predicted_away_yc, predicted_rc, rc_probability, confidence)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (fixture_id) DO UPDATE
                    SET predicted_yc = EXCLUDED.predicted_yc,
                        predicted_home_yc = EXCLUDED.predicted_home_yc,
                        predicted_away_yc = EXCLUDED.predicted_away_yc,
                        predicted_rc = EXCLUDED.predicted_rc,
                        rc_probability = EXCLUDED.rc_probability,
                        confidence = EXCLUDED.confidence
            """, fixture_id, predicted_yc, predicted_home_yc,
                predicted_away_yc, predicted_rc, rc_probability, confidence)

    async def update_prediction_actuals(
        self, fixture_id: int, actual_yc: int, actual_home_yc: int,
        actual_away_yc: int, actual_rc: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE predictions SET actual_yc=$1, actual_home_yc=$2,
                    actual_away_yc=$3, actual_rc=$4
                WHERE fixture_id=$5
            """, actual_yc, actual_home_yc, actual_away_yc, actual_rc, fixture_id)

    async def get_accuracy_stats(self) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       AVG(ABS(predicted_yc - actual_yc)) AS avg_yc_error,
                       AVG(ABS(predicted_home_yc - actual_home_yc)) AS avg_home_error,
                       AVG(ABS(predicted_away_yc - actual_away_yc)) AS avg_away_error
                FROM predictions
                WHERE actual_yc IS NOT NULL
            """)

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
