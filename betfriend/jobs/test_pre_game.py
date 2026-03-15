"""Test script — simulates pre-game analysis for a specific team's next fixture."""
from __future__ import annotations

import asyncio
from datetime import date

from loguru import logger

from betfriend.api.budget import BudgetTracker
from betfriend.api.client import APIFootballClient, parse_fixture
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
        # Find tomorrow's fixtures
        tomorrow = date.today() + __import__("datetime").timedelta(days=1)
        logger.info(f"Looking for fixtures on {tomorrow}")

        # First make sure we have tomorrow's fixtures
        for league_id in (settings.la_liga_id, settings.la_liga2_id):
            raw_fixtures = await api.get_fixtures_by_date(league_id, tomorrow)
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

                # Assign referee from API
                ref_name = parsed.get("referee")
                referee_id = None
                if ref_name:
                    ref_name = ref_name.strip()
                    if "," in ref_name:
                        ref_name = ref_name.split(",")[0].strip()
                    referee_id = await store.upsert_referee(ref_name)

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
                if referee_id:
                    await store.assign_referee_to_fixture(fixture_db_id, referee_id)

        await store.recompute_referee_stats()

        # Find Barcelona fixture
        db_fixtures = await store.get_fixtures_by_date(tomorrow)
        barca_fixture = None
        for f in db_fixtures:
            if "barce" in f["home_team_name"].lower() or "barce" in f["away_team_name"].lower():
                barca_fixture = f
                break

        if not barca_fixture:
            logger.warning("No Barcelona fixture found for tomorrow")
            # Just pick the first La Liga fixture
            for f in db_fixtures:
                if f["competition_name"] == "La Liga":
                    barca_fixture = f
                    break

        if not barca_fixture:
            logger.error("No fixtures found for tomorrow")
            return

        fixture = barca_fixture
        home_id = fixture["home_team_id"]
        away_id = fixture["away_team_id"]
        fixture_db_id = fixture["id"]

        logger.info(f"Testing: {fixture['home_team_name']} vs {fixture['away_team_name']}")

        # Try to fetch lineup (might not be available yet)
        from betfriend.jobs.pre_game_check import _fetch_and_store_lineup
        has_lineup = await store.has_lineup(fixture_db_id)
        if not has_lineup:
            has_lineup = await _fetch_and_store_lineup(api, store, fixture["api_id"], fixture_db_id)
            logger.info(f"Lineup available: {has_lineup}")

        home_lineup = await store.get_fixture_lineup(fixture_db_id, home_id) if has_lineup else []
        away_lineup = await store.get_fixture_lineup(fixture_db_id, away_id) if has_lineup else []

        # Fetch all analysis data
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

        # H2H
        h2h_records = await store.get_h2h(home_id, away_id, 5)
        if not h2h_records:
            async with store.pool.acquire() as conn:
                home_row = await conn.fetchrow("SELECT api_id FROM teams WHERE id=$1", home_id)
                away_row = await conn.fetchrow("SELECT api_id FROM teams WHERE id=$1", away_id)
            if home_row and away_row:
                raw_h2h = await api.get_head2head(home_row["api_id"], away_row["api_id"], 5)
                for raw in raw_h2h:
                    parsed = parse_fixture(raw)
                    if parsed["status"] != "finished":
                        continue
                    h_team_id = await store.get_team_id(parsed["home_team_api_id"])
                    a_team_id = await store.get_team_id(parsed["away_team_api_id"])
                    if h_team_id and a_team_id:
                        fid = await store.get_fixture_id_by_api_id(parsed["api_id"])
                        h_yc = h_rc = a_yc = a_rc = 0
                        if fid:
                            async with store.pool.acquire() as conn:
                                frow = await conn.fetchrow(
                                    "SELECT home_yc, away_yc, home_rc, away_rc FROM fixtures WHERE id=$1", fid
                                )
                            if frow:
                                h_yc, a_yc = frow["home_yc"], frow["away_yc"]
                                h_rc, a_rc = frow["home_rc"], frow["away_rc"]
                        await store.upsert_h2h(
                            team_a_id=h_team_id, team_b_id=a_team_id,
                            fixture_api_id=parsed["api_id"],
                            match_date=parsed["kickoff"].date() if hasattr(parsed["kickoff"], "date") else parsed["kickoff"],
                            team_a_yc=h_yc, team_a_rc=h_rc,
                            team_b_yc=a_yc, team_b_rc=a_rc,
                            team_a_score=parsed["home_score"],
                            team_b_score=parsed["away_score"],
                        )
                h2h_records = await store.get_h2h(home_id, away_id, 5)

        # Referee
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
            h2h_records=h2h_records,
        )

        await telegram.send(msg)
        logger.info("Test pre-game message sent!")

    finally:
        await api.close()
        await store.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
