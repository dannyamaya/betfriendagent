"""Daily fixtures job — runs at 6 AM CET via GitHub Actions.

Fetches today's fixtures for La Liga and La Liga 2 from API-Football,
upserts teams and fixtures into the DB, and sends a Telegram summary.
"""
from __future__ import annotations

import asyncio
from datetime import date

from loguru import logger

from betfriend.api.client import APIFootballClient, parse_fixture
from betfriend.api.budget import BudgetTracker
from betfriend.config.settings import settings
from betfriend.db.store import Store
from betfriend.notifications.formatter import format_fixture_list
from betfriend.notifications.telegram import TelegramNotifier


async def run() -> None:
    store = Store()
    await store.start()

    budget = BudgetTracker(store)
    api = APIFootballClient(store, budget)
    telegram = TelegramNotifier()

    try:
        today = date.today()
        logger.info(f"Fetching fixtures for {today}")

        all_fixtures = []
        for league_id in (settings.la_liga_id, settings.la_liga2_id):
            raw_fixtures = await api.get_fixtures_by_date(league_id, today)
            logger.info(
                f"League {league_id}: {len(raw_fixtures)} fixtures found"
            )

            comp_id = await store.get_competition_id(league_id)

            for raw in raw_fixtures:
                parsed = parse_fixture(raw)

                # Upsert home team
                home_team_id = await store.upsert_team(
                    api_id=parsed["home_team_api_id"],
                    name=parsed["home_team_name"],
                    short_name=None,
                    logo_url=parsed["home_team_logo"],
                    competition_id=comp_id,
                )
                # Upsert away team
                away_team_id = await store.upsert_team(
                    api_id=parsed["away_team_api_id"],
                    name=parsed["away_team_name"],
                    short_name=None,
                    logo_url=parsed["away_team_logo"],
                    competition_id=comp_id,
                )

                # Parse matchday number from round string like "Regular Season - 30"
                matchday = None
                if parsed["matchday"]:
                    parts = str(parsed["matchday"]).split(" - ")
                    if len(parts) == 2 and parts[1].isdigit():
                        matchday = int(parts[1])

                await store.upsert_fixture(
                    api_id=parsed["api_id"],
                    competition_id=comp_id,
                    home_team_id=home_team_id,
                    away_team_id=away_team_id,
                    matchday=matchday,
                    kickoff=parsed["kickoff"],
                    status=parsed["status"],
                    home_score=parsed["home_score"],
                    away_score=parsed["away_score"],
                )

        # Send Telegram summary
        db_fixtures = await store.get_fixtures_by_date(today)
        if db_fixtures:
            msg = format_fixture_list(db_fixtures)
            await telegram.send(msg)
            logger.info(f"Sent fixture summary: {len(db_fixtures)} games")
        else:
            logger.info("No fixtures today, no message sent")

        remaining = await budget.requests_remaining()
        logger.info(f"API budget remaining: {remaining}/{settings.api_daily_limit}")

    finally:
        await api.close()
        await store.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
