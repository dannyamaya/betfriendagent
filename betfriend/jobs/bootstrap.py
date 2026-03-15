"""Bootstrap job — one-shot DB population (run with paid API tier).

Fetches all season fixtures, events, player cards, and team stats
for La Liga and La Liga 2. Resumable: skips fixtures already processed.

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

CONCURRENCY = 5  # parallel API calls


async def process_fixture(
    fixture_api_id: int,
    info: dict,
    api: APIFootballClient,
    store: Store,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    """Fetch player stats for one fixture and save to DB. Returns card totals."""
    async with semaphore:
        players_data = await api.get_fixture_players(fixture_api_id)
        if not players_data:
            return None

        fixture_db_id = info["db_id"]
        match_date = info["kickoff"].date() if hasattr(info["kickoff"], "date") else info["kickoff"]

        home_yc = 0
        home_rc = 0
        away_yc = 0
        away_rc = 0
        player_count = 0

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
                    player_count += 1

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

        return {
            "home_team_id": info["home_team_id"],
            "away_team_id": info["away_team_id"],
            "home_yc": home_yc, "home_rc": home_rc,
            "away_yc": away_yc, "away_rc": away_rc,
            "player_count": player_count,
        }


async def run() -> None:
    store = Store()
    await store.start()

    budget = BudgetTracker(store)
    api = APIFootballClient(store, budget)
    telegram = TelegramNotifier()
    semaphore = asyncio.Semaphore(CONCURRENCY)

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

            fixture_map: dict[int, dict] = {}

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
            # Step 2: Fetch player stats — skip already processed
            # ----------------------------------------------------------
            finished = [
                (api_id, info) for api_id, info in fixture_map.items()
                if info["status"] == "finished"
            ]

            # Check which fixtures already have player_fixture_cards data
            to_process = []
            for api_id, info in finished:
                async with store.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT COUNT(*) AS cnt FROM player_fixture_cards WHERE fixture_id = $1",
                        info["db_id"]
                    )
                if row["cnt"] == 0:
                    to_process.append((api_id, info))

            logger.info(f"  Finished: {len(finished)}, already done: {len(finished) - len(to_process)}, to process: {len(to_process)}")

            # Process in batches of CONCURRENCY
            team_cards: dict[int, dict] = {}

            for batch_start in range(0, len(to_process), CONCURRENCY):
                batch = to_process[batch_start:batch_start + CONCURRENCY]
                remaining = await budget.requests_remaining()
                logger.info(f"  Batch {batch_start+1}-{batch_start+len(batch)}/{len(to_process)} (API budget: {remaining})")

                tasks = [
                    process_fixture(api_id, info, api, store, semaphore)
                    for api_id, info in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"  Error processing fixture: {result}")
                        continue
                    if result is None:
                        continue

                    total_events += 1
                    total_players += result["player_count"]

                    for team_id, yc, rc in [
                        (result["home_team_id"], result["home_yc"], result["home_rc"]),
                        (result["away_team_id"], result["away_yc"], result["away_rc"]),
                    ]:
                        if team_id not in team_cards:
                            team_cards[team_id] = {"yc": 0, "rc": 0, "games": 0}
                        team_cards[team_id]["yc"] += yc
                        team_cards[team_id]["rc"] += rc
                        team_cards[team_id]["games"] += 1

            # Also count already-processed fixtures for team_cards
            for api_id, info in finished:
                if (api_id, info) in to_process:
                    continue
                async with store.pool.acquire() as conn:
                    row = await conn.fetchrow("""
                        SELECT home_yc, away_yc, home_rc, away_rc FROM fixtures WHERE id = $1
                    """, info["db_id"])
                if row:
                    for team_id, yc, rc in [
                        (info["home_team_id"], row["home_yc"], row["home_rc"]),
                        (info["away_team_id"], row["away_yc"], row["away_rc"]),
                    ]:
                        if team_id not in team_cards:
                            team_cards[team_id] = {"yc": 0, "rc": 0, "games": 0}
                        team_cards[team_id]["yc"] += yc
                        team_cards[team_id]["rc"] += rc
                        team_cards[team_id]["games"] += 1

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
            # Step 4: Fetch standings
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

            for team_id, card_data in team_cards.items():
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

            logger.info(f"  {league_name} done!")

        # ----------------------------------------------------------
        # Done
        # ----------------------------------------------------------
        remaining = await budget.requests_remaining()
        summary = (
            f"<b>BetFriend - Bootstrap Complete</b>\n\n"
            f"Fixtures loaded: {total_fixtures}\n"
            f"Fixtures with events processed: {total_events}\n"
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
