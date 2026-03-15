"""Daily fixtures job — runs at 6 AM CET via GitHub Actions.

Fetches today's fixtures for La Liga and La Liga 2 from API-Football,
upserts teams and fixtures into the DB, tries to assign referees from
API data and RFEF PDFs, and sends a Telegram summary.
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
from betfriend.scrapers.rfef_pdf import fetch_referee_designations, match_referee_to_fixture


async def _assign_referee(store: Store, fixture_db_id: int, referee_name: str | None) -> None:
    """Assign referee to fixture if we have a name."""
    if not referee_name:
        return
    # Clean up API referee string like "J. González Fuertes"
    name = referee_name.strip()
    if "," in name:
        name = name.split(",")[0].strip()
    referee_id = await store.upsert_referee(name)
    await store.assign_referee_to_fixture(fixture_db_id, referee_id)


async def run() -> None:
    store = Store()
    await store.start()

    budget = BudgetTracker(store)
    api = APIFootballClient(store, budget)
    telegram = TelegramNotifier()

    try:
        today = date.today()
        logger.info(f"Fetching fixtures for {today}")

        matchdays_to_scrape: set[tuple[int, int]] = set()  # (league_id, matchday)

        for league_id in (settings.la_liga_id, settings.la_liga2_id):
            raw_fixtures = await api.get_fixtures_by_date(league_id, today)
            logger.info(
                f"League {league_id}: {len(raw_fixtures)} fixtures found"
            )

            comp_id = await store.get_competition_id(league_id)

            for raw in raw_fixtures:
                parsed = parse_fixture(raw)

                home_team_id = await store.upsert_team(
                    api_id=parsed["home_team_api_id"],
                    name=parsed["home_team_name"],
                    short_name=None,
                    logo_url=parsed["home_team_logo"],
                    competition_id=comp_id,
                )
                away_team_id = await store.upsert_team(
                    api_id=parsed["away_team_api_id"],
                    name=parsed["away_team_name"],
                    short_name=None,
                    logo_url=parsed["away_team_logo"],
                    competition_id=comp_id,
                )

                matchday = None
                if parsed["matchday"]:
                    parts = str(parsed["matchday"]).split(" - ")
                    if len(parts) == 2 and parts[1].isdigit():
                        matchday = int(parts[1])

                fixture_db_id = await store.upsert_fixture(
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

                # Try to assign referee from API data
                await _assign_referee(store, fixture_db_id, parsed.get("referee"))

                # Track matchday for RFEF PDF scraping
                if matchday:
                    matchdays_to_scrape.add((league_id, matchday))

        # Try RFEF PDFs for referee assignments on fixtures missing referee
        rfef_warnings: list[str] = []
        for league_id, matchday in matchdays_to_scrape:
            league_name = "La Liga" if league_id == settings.la_liga_id else "La Liga 2"
            logger.info(f"Trying RFEF PDF for {league_name}, matchday {matchday}")
            designations = await fetch_referee_designations(league_id, matchday)
            if designations:
                logger.info(f"  Found {len(designations)} referee designations")
                db_fixtures = await store.get_fixtures_by_date(today)
                for f in db_fixtures:
                    if f["referee_id"] is not None:
                        continue
                    ref_name = match_referee_to_fixture(
                        designations, f["home_team_name"], f["away_team_name"]
                    )
                    if ref_name:
                        fixture_db_id = f["id"]
                        await _assign_referee(store, fixture_db_id, ref_name)
                        logger.info(f"  Assigned referee {ref_name} to {f['home_team_name']} vs {f['away_team_name']}")
                    else:
                        rfef_warnings.append(
                            f"No pude asignar arbitro para {f['home_team_name']} vs {f['away_team_name']} ({league_name})"
                        )
            else:
                rfef_warnings.append(f"No se pudo descargar PDF de RFEF para {league_name} jornada {matchday}")

        # Alert if RFEF scraping had issues
        if rfef_warnings:
            warning_msg = (
                "<b>⚠ BetFriend - RFEF PDF Alert</b>\n\n"
                + "\n".join(f"• {w}" for w in rfef_warnings)
                + "\n\n<i>Los arbitros se asignaran desde la API si estan disponibles</i>"
            )
            await telegram.send(warning_msg)
            logger.warning(f"RFEF PDF issues: {rfef_warnings}")

        # Recompute referee stats
        await store.recompute_referee_stats()

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
