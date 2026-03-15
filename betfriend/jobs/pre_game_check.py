"""Pre-game check job — runs every 10 min via GitHub Actions (11:00-23:00 CET).

Checks for fixtures starting in the next 10-20 minutes that haven't been
processed yet. If found, sends a pre-game analysis message to Telegram
with team stats, card rankings, form, and top card players.
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

            logger.info(
                f"Processing: {fixture['home_team_name']} vs "
                f"{fixture['away_team_name']} at {fixture['kickoff']}"
            )

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
            )
            await telegram.send(msg)

            await store.mark_fixture_processed(fixture["id"])
            logger.info(f"Fixture {fixture['id']} processed and marked")

        remaining = await budget.requests_remaining()
        logger.info(f"API budget remaining: {remaining}/{settings.api_daily_limit}")

    finally:
        await api.close()
        await store.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
