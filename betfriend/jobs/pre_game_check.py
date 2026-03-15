"""Pre-game check job — runs every 10 min via GitHub Actions (11:00-23:00 CET).

Checks for fixtures starting in the next 10-20 minutes that haven't been
processed yet. Fetches lineups from the API, then sends a full pre-game
analysis message to Telegram.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from loguru import logger

from betfriend.api.budget import BudgetTracker
from betfriend.api.client import APIFootballClient
from betfriend.config.settings import settings
from betfriend.db.store import Store
from betfriend.notifications.formatter import format_pre_game
from betfriend.notifications.telegram import TelegramNotifier


async def _fetch_and_store_lineup(
    api: APIFootballClient, store: Store,
    fixture_api_id: int, fixture_db_id: int,
) -> bool:
    """Fetch lineup from API and store in DB. Returns True if lineup found."""
    lineups = await api.get_lineups(fixture_api_id)
    if not lineups:
        return False

    for team_lineup in lineups:
        team_api_id = team_lineup["team"]["id"]
        team_db_id = await store.get_team_id(team_api_id)
        if not team_db_id:
            continue

        # Starters
        for player in team_lineup.get("startXI", []):
            p = player.get("player", {})
            if not p.get("id"):
                continue
            player_id = await store.upsert_player(
                api_id=p["id"],
                name=p.get("name", "Unknown"),
                photo_url=None,
                team_id=team_db_id,
                position=p.get("pos"),
            )
            await store.upsert_lineup_player(
                fixture_id=fixture_db_id,
                player_id=player_id,
                team_id=team_db_id,
                is_starter=True,
                position=p.get("pos"),
                grid_pos=p.get("grid"),
            )

        # Substitutes
        for player in team_lineup.get("substitutes", []):
            p = player.get("player", {})
            if not p.get("id"):
                continue
            player_id = await store.upsert_player(
                api_id=p["id"],
                name=p.get("name", "Unknown"),
                photo_url=None,
                team_id=team_db_id,
                position=p.get("pos"),
            )
            await store.upsert_lineup_player(
                fixture_id=fixture_db_id,
                player_id=player_id,
                team_id=team_db_id,
                is_starter=False,
                position=p.get("pos"),
                grid_pos=None,
            )

    return True


async def run() -> None:
    store = Store()
    await store.start()

    budget = BudgetTracker(store)
    api = APIFootballClient(store, budget)
    telegram = TelegramNotifier()

    try:
        now = datetime.now(timezone.utc)
        window_start = now + timedelta(minutes=10)
        window_end = now + timedelta(minutes=20)

        upcoming = await store.get_unprocessed_upcoming(window_start, window_end)

        if not upcoming:
            logger.info("No upcoming fixtures in the next 10-20 min")
            return

        logger.info(f"Found {len(upcoming)} upcoming fixtures to process")

        for fixture in upcoming:
            home_id = fixture["home_team_id"]
            away_id = fixture["away_team_id"]
            fixture_db_id = fixture["id"]

            logger.info(
                f"Processing: {fixture['home_team_name']} vs "
                f"{fixture['away_team_name']} at {fixture['kickoff']}"
            )

            # Fetch lineup from API (1 API call per fixture, critical priority)
            has_lineup = await store.has_lineup(fixture_db_id)
            if not has_lineup:
                has_lineup = await _fetch_and_store_lineup(
                    api, store, fixture["api_id"], fixture_db_id
                )
                if has_lineup:
                    logger.info("  Lineup fetched and stored")
                else:
                    logger.warning("  Lineup not available yet")

            # Get lineup data from DB
            home_lineup = await store.get_fixture_lineup(fixture_db_id, home_id) if has_lineup else []
            away_lineup = await store.get_fixture_lineup(fixture_db_id, away_id) if has_lineup else []

            # Fetch all analysis data from DB (0 API calls)
            home_stats = await store.get_team_stats(home_id)
            away_stats = await store.get_team_stats(away_id)
            home_yc_rank = await store.get_team_yc_rank(home_id)
            away_yc_rank = await store.get_team_yc_rank(away_id)
            home_rc_rank = await store.get_team_rc_rank(home_id)
            away_rc_rank = await store.get_team_rc_rank(away_id)
            home_form = await store.get_team_last_n_form(home_id, 5)
            away_form = await store.get_team_last_n_form(away_id, 5)
            home_top_players = await store.get_top_card_players(home_id, 5)
            away_top_players = await store.get_top_card_players(away_id, 5)

            # Referee data
            referee_stats = None
            referee_yc_rank = None
            referee_total_refs = None
            referee_last_games = None
            if fixture["referee_id"]:
                referee_stats = await store.get_referee_stats(fixture["referee_id"])
                referee_yc_rank = await store.get_referee_yc_rank(fixture["referee_id"])
                referee_total_refs = await store.get_total_referees_with_games()
                referee_last_games = await store.get_referee_last_games(fixture["referee_id"], 3)

            msg = format_pre_game(
                fixture,
                home_stats=home_stats,
                away_stats=away_stats,
                home_yc_rank=home_yc_rank,
                away_yc_rank=away_yc_rank,
                home_rc_rank=home_rc_rank,
                away_rc_rank=away_rc_rank,
                home_form=home_form,
                away_form=away_form,
                home_top_players=home_top_players,
                away_top_players=away_top_players,
                referee_stats=referee_stats,
                referee_yc_rank=referee_yc_rank,
                referee_total_refs=referee_total_refs,
                referee_last_games=referee_last_games,
                home_lineup=home_lineup,
                away_lineup=away_lineup,
            )
            await telegram.send(msg)

            await store.mark_fixture_processed(fixture_db_id)
            logger.info(f"Fixture {fixture_db_id} processed and marked")

        remaining = await budget.requests_remaining()
        logger.info(f"API budget remaining: {remaining}/{settings.api_daily_limit}")

    finally:
        await api.close()
        await store.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
