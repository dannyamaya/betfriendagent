from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import httpx
from loguru import logger

from betfriend.api.budget import BudgetTracker
from betfriend.config.settings import settings
from betfriend.db.store import Store


class APIFootballClient:
    """HTTP client for API-Football v3."""

    def __init__(self, store: Store, budget: BudgetTracker) -> None:
        self._store = store
        self._budget = budget
        self._http = httpx.AsyncClient(
            base_url=settings.api_football_base_url,
            headers={"x-apisports-key": settings.api_football_key},
            timeout=30.0,
        )

    async def close(self) -> None:
        await self._http.aclose()

    # ------------------------------------------------------------------
    # Raw request with budget tracking
    # ------------------------------------------------------------------

    async def _request(
        self, endpoint: str, params: dict[str, Any], priority: str = "normal"
    ) -> dict[str, Any] | None:
        if not await self._budget.can_request(priority):
            return None

        resp = await self._http.get(endpoint, params=params)
        await self._store.log_api_request(endpoint, params, resp.status_code)

        if resp.status_code != 200:
            logger.error(f"API error {resp.status_code}: {endpoint} {params}")
            return None

        data = resp.json()
        errors = data.get("errors")
        if errors:
            logger.error(f"API errors: {errors}")
            return None

        return data

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    async def get_fixtures_by_date(
        self, league_id: int, target_date: date
    ) -> list[dict[str, Any]]:
        data = await self._request(
            "/fixtures",
            {"league": league_id, "season": settings.season, "date": target_date.isoformat()},
        )
        if not data:
            return []
        return data.get("response", [])

    async def get_fixture_by_id(self, fixture_id: int) -> dict[str, Any] | None:
        data = await self._request("/fixtures", {"id": fixture_id})
        if not data or not data.get("response"):
            return None
        return data["response"][0]

    # ------------------------------------------------------------------
    # Standings
    # ------------------------------------------------------------------

    async def get_standings(self, league_id: int) -> list[dict[str, Any]]:
        data = await self._request(
            "/standings",
            {"league": league_id, "season": settings.season},
        )
        if not data or not data.get("response"):
            return []
        return data["response"][0].get("league", {}).get("standings", [[]])[0]

    # ------------------------------------------------------------------
    # Lineups
    # ------------------------------------------------------------------

    async def get_lineups(self, fixture_api_id: int) -> list[dict[str, Any]]:
        data = await self._request(
            "/fixtures/lineups",
            {"fixture": fixture_api_id},
            priority="critical",
        )
        if not data:
            return []
        return data.get("response", [])

    # ------------------------------------------------------------------
    # Head to Head
    # ------------------------------------------------------------------

    async def get_head2head(
        self, team_a_api_id: int, team_b_api_id: int, last: int = 5
    ) -> list[dict[str, Any]]:
        h2h = f"{team_a_api_id}-{team_b_api_id}"
        data = await self._request(
            "/fixtures/headtohead",
            {"h2h": h2h, "last": last},
        )
        if not data:
            return []
        return data.get("response", [])

    # ------------------------------------------------------------------
    # Fixture events (cards, goals, subs)
    # ------------------------------------------------------------------

    async def get_fixture_events(self, fixture_api_id: int) -> list[dict[str, Any]]:
        data = await self._request(
            "/fixtures/events",
            {"fixture": fixture_api_id},
        )
        if not data:
            return []
        return data.get("response", [])


# ------------------------------------------------------------------
# Helpers for parsing API responses into DB-friendly data
# ------------------------------------------------------------------

def parse_fixture(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract relevant fields from an API-Football fixture response."""
    fixture = raw["fixture"]
    league = raw["league"]
    teams = raw["teams"]
    goals = raw.get("goals", {})

    kickoff_str = fixture["date"]
    kickoff = datetime.fromisoformat(kickoff_str)
    if kickoff.tzinfo is None:
        kickoff = kickoff.replace(tzinfo=timezone.utc)

    status_map = {
        "NS": "scheduled",
        "1H": "live", "HT": "live", "2H": "live", "ET": "live",
        "P": "live", "BT": "live", "SUSP": "suspended",
        "INT": "suspended", "FT": "finished", "AET": "finished",
        "PEN": "finished", "PST": "postponed", "CANC": "cancelled",
        "ABD": "abandoned", "AWD": "awarded", "WO": "walkover",
        "LIVE": "live",
    }
    raw_status = fixture.get("status", {}).get("short", "NS")
    status = status_map.get(raw_status, "scheduled")

    return {
        "api_id": fixture["id"],
        "league_api_id": league["id"],
        "home_team_api_id": teams["home"]["id"],
        "home_team_name": teams["home"]["name"],
        "home_team_logo": teams["home"].get("logo"),
        "away_team_api_id": teams["away"]["id"],
        "away_team_name": teams["away"]["name"],
        "away_team_logo": teams["away"].get("logo"),
        "matchday": league.get("round"),
        "kickoff": kickoff,
        "status": status,
        "home_score": goals.get("home"),
        "away_score": goals.get("away"),
        "referee": fixture.get("referee"),
    }
