"""Bootstrap job — one-shot DB population (run with paid API tier).

Fetches all season fixtures, events, player cards, and team stats
for La Liga and La Liga 2. Designed to run once and fill the DB
with historical data.

Trigger manually: gh workflow run bootstrap.yml
"""
from __future__ import annotations

import asyncio
from datetime import date, timezone

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
        total_fixtures = 0
        total_events = 0
        total_players = 0

        for league_id in (settings.la_liga_id, settings.la_liga2_id):
            comp_id = await store.get_competition_id(league_id)
            league_name = "La Liga" if league_id == settings.la_liga_id else "La Liga 2"
            logger.info(f"=== Bootstrapping {league_name} (api_id={league_id}) ===")

            # ----------------------------------------------------------
            # Step 1: Fetch ALL fixtures for the season (1 request)
            # ----------------------------------------------------------
            raw_fixtures = await api.get_all_fixtures(league_id)
            logger.info(f"  Fixtures: {len(raw_fixtures)} total")

            fixture_map: dict[int, dict] = {}  # api_id -> {db_id, home_team_id, away_team_id, status, parsed}

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

                db_id = await store.upsert_fixture(
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

                fixture_map[parsed["api_id"]] = {
                    "db_id": db_id,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "status": parsed["status"],
                    "kickoff": parsed["kickoff"],
                }

            total_fixtures += len(raw_fixtures)

            # ----------------------------------------------------------
            # Step 2: Fetch player stats for finished fixtures
            # ----------------------------------------------------------
            finished = [
                (api_id, info) for api_id, info in fixture_map.items()
                if info["status"] == "finished"
            ]
            logger.info(f"  Finished fixtures to process: {len(finished)}")

            # Track card totals per team for this league
            team_cards: dict[int, dict] = {}  # team_id -> {yc, rc, games}

            for i, (fixture_api_id, info) in enumerate(finished):
                if i % 50 == 0:
                    remaining = await budget.requests_remaining()
                    logger.info(f"  Processing fixture {i+1}/{len(finished)} (API budget: {remaining})")

                # Get player-level stats for this fixture
                players_data = await api.get_fixture_players(fixture_api_id)
                if not players_data:
                    continue

                fixture_db_id = info["db_id"]
                match_date = info["kickoff"].date() if hasattr(info["kickoff"], "date") else info["kickoff"]

                home_yc = 0
                home_rc = 0
                away_yc = 0
                away_rc = 0

                for team_data in players_data:
                    team_api_id = team_data["team"]["id"]
                    team_db_id = await store.get_team_id(team_api_id)
                    if not team_db_id:
                        continue

                    is_home = team_db_id == info["home_team_id"]

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
                            total_players += 1

                        if is_home:
                            home_yc += yc
                            home_rc += rc
                        else:
                            away_yc += yc
                            away_rc += rc

                # Update fixture card totals
                async with store.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE fixtures SET home_yc=$1, away_yc=$2, home_rc=$3, away_rc=$4
                        WHERE id=$5
                    """, home_yc, away_yc, home_rc, away_rc, fixture_db_id)

                # Track team form
                for team_id, yc, rc, is_home in [
                    (info["home_team_id"], home_yc, home_rc, True),
                    (info["away_team_id"], away_yc, away_rc, False),
                ]:
                    await store.upsert_team_form(
                        team_id=team_id,
                        fixture_id=fixture_db_id,
                        yc=yc, rc=rc,
                        is_home=is_home,
                        match_date=match_date,
                    )

                    if team_id not in team_cards:
                        team_cards[team_id] = {"yc": 0, "rc": 0, "games": 0}
                    team_cards[team_id]["yc"] += yc
                    team_cards[team_id]["rc"] += rc
                    team_cards[team_id]["games"] += 1

                total_events += 1

                # Small delay to be nice to the API
                await asyncio.sleep(0.1)

            # ----------------------------------------------------------
            # Step 3: Compute player card stats from fixture data
            # ----------------------------------------------------------
            logger.info("  Computing player card stats...")
            async with store.pool.acquire() as conn:
                players = await conn.fetch("""
                    SELECT p.id AS player_id,
                           COALESCE(SUM(pfc.yellow_cards), 0) AS total_yc,
                           COALESCE(SUM(pfc.red_cards), 0) AS total_rc,
                           COUNT(pfc.id) AS games_played
                    FROM players p
                    LEFT JOIN player_fixture_cards pfc ON pfc.player_id = p.id
                    WHERE p.team_id IN (SELECT id FROM teams WHERE competition_id = $1)
                    GROUP BY p.id
                """, comp_id)

            for row in players:
                await store.upsert_player_card_stats(
                    player_id=row["player_id"],
                    total_yc=row["total_yc"],
                    total_rc=row["total_rc"],
                    games_played=row["games_played"],
                )

            # ----------------------------------------------------------
            # Step 4: Fetch standings and team stats
            # ----------------------------------------------------------
            logger.info("  Fetching standings...")
            standings = await api.get_standings(league_id)
            standings_map: dict[int, dict] = {}
            for entry in standings:
                t = entry.get("team", {})
                standings_map[t.get("id")] = {
                    "pos": entry.get("rank"),
                    "pts": entry.get("points"),
                    "form": entry.get("form"),
                }

            # Save team season stats
            for team_id, card_data in team_cards.items():
                # Look up team api_id to get standings
                async with store.pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT api_id FROM teams WHERE id = $1", team_id)
                team_api_id = row["api_id"] if row else None

                standing = standings_map.get(team_api_id, {})
                await store.upsert_team_season_stats(
                    team_id=team_id,
                    games_played=card_data["games"],
                    total_yc=card_data["yc"],
                    total_rc=card_data["rc"],
                    standing_pos=standing.get("pos"),
                    standing_pts=standing.get("pts"),
                    form=standing.get("form"),
                )

        # ----------------------------------------------------------
        # Done — send summary to Telegram
        # ----------------------------------------------------------
        remaining = await budget.requests_remaining()
        summary = (
            f"<b>BetFriend - Bootstrap Complete</b>\n\n"
            f"Fixtures loaded: {total_fixtures}\n"
            f"Fixtures with events: {total_events}\n"
            f"Player-fixture records: {total_players}\n"
            f"API requests remaining today: {remaining}"
        )
        await telegram.send(summary)
        logger.info(f"Bootstrap complete! {total_fixtures} fixtures, {total_events} events, {total_players} player records")

    finally:
        await api.close()
        await store.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
