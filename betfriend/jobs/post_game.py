"""Post-game job — runs at 2 AM CET via GitHub Actions.

Fetches events for yesterday's finished games, updates player card stats,
team season stats, and team form. Keeps the DB current after each matchday.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger

from betfriend.api.client import APIFootballClient, parse_fixture
from betfriend.api.budget import BudgetTracker
from betfriend.config.settings import settings
from betfriend.db.store import Store
from betfriend.notifications.telegram import TelegramNotifier


async def run() -> None:
    store = Store()
    await store.start()

    budget = BudgetTracker(store)
    api = APIFootballClient(store, budget)
    telegram = TelegramNotifier()

    try:
        yesterday = datetime.now(ZoneInfo("America/Bogota")).date() - timedelta(days=1)
        logger.info(f"Post-game update for {yesterday}")

        fixtures_updated = 0
        player_records = 0

        for league_id in (settings.la_liga_id, settings.la_liga2_id):
            comp_id = await store.get_competition_id(league_id)
            league_name = "La Liga" if league_id == settings.la_liga_id else "La Liga 2"

            # Fetch yesterday's fixtures from API to get final scores
            raw_fixtures = await api.get_fixtures_by_date(league_id, yesterday)
            logger.info(f"  {league_name}: {len(raw_fixtures)} fixtures yesterday")

            for raw in raw_fixtures:
                parsed = parse_fixture(raw)
                if parsed["status"] != "finished":
                    continue

                fixture_db_id = await store.get_fixture_id_by_api_id(parsed["api_id"])
                if not fixture_db_id:
                    logger.warning(f"  Fixture {parsed['api_id']} not in DB, skipping")
                    continue

                # Check if already processed (has player_fixture_cards)
                async with store.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT COUNT(*) AS cnt FROM player_fixture_cards WHERE fixture_id = $1",
                        fixture_db_id
                    )
                if row["cnt"] > 0:
                    logger.info(f"  Fixture {parsed['api_id']} already has player data, skipping")
                    continue

                # Fetch player stats for this fixture
                players_data = await api.get_fixture_players(parsed["api_id"])
                if not players_data:
                    logger.warning(f"  No player data for fixture {parsed['api_id']}")
                    continue

                home_team_id = await store.get_team_id(parsed["home_team_api_id"])
                away_team_id = await store.get_team_id(parsed["away_team_api_id"])
                if not home_team_id or not away_team_id:
                    continue

                home_yc = 0
                home_rc = 0
                away_yc = 0
                away_rc = 0

                for team_data in players_data:
                    team_api_id = team_data["team"]["id"]
                    team_db_id = await store.get_team_id(team_api_id)
                    if not team_db_id:
                        continue

                    is_home = team_db_id == home_team_id

                    for player_entry in team_data.get("players", []):
                        p = player_entry["player"]
                        stats_list = player_entry.get("statistics", [])
                        if not stats_list:
                            continue
                        stats = stats_list[0]

                        player_id = await store.upsert_player(
                            api_id=p["id"],
                            name=p["name"],
                            photo_url=p.get("photo"),
                            team_id=team_db_id,
                            position=stats.get("games", {}).get("position"),
                        )

                        yc = stats.get("cards", {}).get("yellow") or 0
                        rc = stats.get("cards", {}).get("red") or 0
                        minutes = stats.get("games", {}).get("minutes") or 0

                        if minutes > 0:
                            await store.upsert_player_fixture_card(
                                player_id=player_id,
                                fixture_id=fixture_db_id,
                                yellow_cards=yc,
                                red_cards=rc,
                                minutes_played=minutes,
                            )
                            player_records += 1

                        if is_home:
                            home_yc += yc
                            home_rc += rc
                        else:
                            away_yc += yc
                            away_rc += rc

                # Update fixture card totals and status
                async with store.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE fixtures
                        SET home_yc=$1, away_yc=$2, home_rc=$3, away_rc=$4,
                            status='finished', home_score=$5, away_score=$6
                        WHERE id=$7
                    """, home_yc, away_yc, home_rc, away_rc,
                        parsed["home_score"], parsed["away_score"], fixture_db_id)

                # Update team form
                for team_id, yc, rc, is_home in [
                    (home_team_id, home_yc, home_rc, True),
                    (away_team_id, away_yc, away_rc, False),
                ]:
                    await store.upsert_team_form(
                        team_id=team_id,
                        fixture_id=fixture_db_id,
                        yc=yc, rc=rc,
                        is_home=is_home,
                        match_date=yesterday,
                    )

                fixtures_updated += 1
                logger.info(f"  Updated: {parsed['home_team_name']} {parsed['home_score']}-{parsed['away_score']} {parsed['away_team_name']} (YC: {home_yc+away_yc}, RC: {home_rc+away_rc})")

        # Recompute player card stats for all players with new data
        if fixtures_updated > 0:
            logger.info("Recomputing player card stats...")
            async with store.pool.acquire() as conn:
                players = await conn.fetch("""
                    SELECT p.id AS player_id,
                           COALESCE(SUM(pfc.yellow_cards), 0) AS total_yc,
                           COALESCE(SUM(pfc.red_cards), 0) AS total_rc,
                           COUNT(pfc.id) AS games_played
                    FROM players p
                    JOIN player_fixture_cards pfc ON pfc.player_id = p.id
                    GROUP BY p.id
                """)

            for row in players:
                await store.upsert_player_card_stats(
                    player_id=row["player_id"],
                    total_yc=row["total_yc"],
                    total_rc=row["total_rc"],
                    games_played=row["games_played"],
                )

            # Recompute team season stats
            logger.info("Recomputing team season stats...")
            async with store.pool.acquire() as conn:
                teams = await conn.fetch("""
                    SELECT t.id AS team_id, t.api_id,
                           COUNT(DISTINCT tf.fixture_id) AS games,
                           COALESCE(SUM(tf.yc), 0) AS total_yc,
                           COALESCE(SUM(tf.rc), 0) AS total_rc
                    FROM teams t
                    JOIN team_form tf ON tf.team_id = t.id
                    GROUP BY t.id, t.api_id
                """)

            # Fetch standings for both leagues
            standings_map: dict[int, dict] = {}
            for league_id in (settings.la_liga_id, settings.la_liga2_id):
                standings = await api.get_standings(league_id)
                for entry in standings:
                    t = entry.get("team", {})
                    standings_map[t.get("id")] = {
                        "pos": entry.get("rank"),
                        "pts": entry.get("points"),
                        "form": entry.get("form"),
                    }

            for row in teams:
                standing = standings_map.get(row["api_id"], {})
                await store.upsert_team_season_stats(
                    team_id=row["team_id"],
                    games_played=row["games"],
                    total_yc=row["total_yc"],
                    total_rc=row["total_rc"],
                    standing_pos=standing.get("pos"),
                    standing_pts=standing.get("pts"),
                    form=standing.get("form"),
                )

        # Backfill referees using get_all_fixtures (2 API calls total)
        logger.info("Backfilling referees from API fixture data...")
        refs_assigned = 0

        # Get all fixtures missing referee_id
        async with store.pool.acquire() as conn:
            missing_ids = {
                row["api_id"]
                for row in await conn.fetch(
                    "SELECT api_id FROM fixtures WHERE referee_id IS NULL AND status = 'finished'"
                )
            }

        if missing_ids:
            logger.info(f"  {len(missing_ids)} fixtures missing referee")
            for league_id in (settings.la_liga_id, settings.la_liga2_id):
                # 1 API call per league — gets ALL fixtures with referee names
                all_raw = await api.get_all_fixtures(league_id, bypass_budget=True)
                for raw in all_raw:
                    parsed = parse_fixture(raw)
                    if parsed["api_id"] not in missing_ids:
                        continue
                    ref_name = parsed.get("referee")
                    if not ref_name:
                        continue
                    ref_name = ref_name.strip()
                    if "," in ref_name:
                        ref_name = ref_name.split(",")[0].strip()

                    fixture_db_id = await store.get_fixture_id_by_api_id(parsed["api_id"])
                    if fixture_db_id:
                        ref_id = await store.upsert_referee(ref_name)
                        await store.assign_referee_to_fixture(fixture_db_id, ref_id)
                        refs_assigned += 1

            if refs_assigned > 0:
                await store.recompute_referee_stats()
                logger.info(f"Assigned {refs_assigned} referees")

        remaining = await budget.requests_remaining()
        summary = (
            f"<b>BetFriend - Post-Game Update</b>\n\n"
            f"Date: {yesterday}\n"
            f"Fixtures updated: {fixtures_updated}\n"
            f"Player records added: {player_records}\n"
            f"Referees assigned: {refs_assigned}\n"
            f"API requests remaining: {remaining}"
        )
        await telegram.send(summary)
        logger.info(f"Post-game done: {fixtures_updated} fixtures, {player_records} player records")

    finally:
        await api.close()
        await store.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
