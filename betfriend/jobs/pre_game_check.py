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
from betfriend.notifications.image import generate_pre_game_image
from betfriend.notifications.telegram import TelegramNotifier
from betfriend.analysis.cards import predict_cards
from betfriend.scrapers.coaches import get_coach_aggressiveness, get_coach_by_team
from betfriend.scrapers.news import get_match_news_context


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
        window_start = now
        window_end = now + timedelta(minutes=35)

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

            # Player league rankings
            home_player_ranks: dict[str, int] = {}
            away_player_ranks: dict[str, int] = {}
            for players, ranks in [(home_top_players, home_player_ranks), (away_top_players, away_player_ranks)]:
                if not players:
                    continue
                for p in players[:5]:
                    # Get player_id from name
                    async with store.pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT pcs.player_id FROM player_card_stats pcs JOIN players pl ON pl.id=pcs.player_id WHERE pl.name=$1 LIMIT 1",
                            p["name"]
                        )
                    if row:
                        rank = await store.get_player_league_yc_rank(row["player_id"])
                        if rank:
                            ranks[p["name"]] = rank

            # H2H data — fetch from API if not in DB, then read from DB
            h2h_records = await store.get_h2h(home_id, away_id, 5)
            if not h2h_records:
                # Try to fetch from API and store (1 API call)
                home_api_id = home_stats["team_name"] if home_stats else None
                away_api_id = away_stats["team_name"] if away_stats else None
                # Get api_ids for H2H call
                async with store.pool.acquire() as conn:
                    home_row = await conn.fetchrow("SELECT api_id FROM teams WHERE id=$1", home_id)
                    away_row = await conn.fetchrow("SELECT api_id FROM teams WHERE id=$1", away_id)
                if home_row and away_row:
                    from betfriend.api.client import parse_fixture
                    raw_h2h = await api.get_head2head(home_row["api_id"], away_row["api_id"], 5)
                    for raw in raw_h2h:
                        parsed = parse_fixture(raw)
                        if parsed["status"] != "finished":
                            continue
                        h_team_id = await store.get_team_id(parsed["home_team_api_id"])
                        a_team_id = await store.get_team_id(parsed["away_team_api_id"])
                        if h_team_id and a_team_id:
                            # Get card data from fixture if in our DB
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

            # Coach aggressiveness — look up by team name
            home_coach_data = None
            away_coach_data = None
            home_coach_name = get_coach_by_team(fixture["home_team_name"])
            if home_coach_name:
                score, desc = get_coach_aggressiveness(home_coach_name)
                home_coach_data = (home_coach_name, score, desc)
            away_coach_name = get_coach_by_team(fixture["away_team_name"])
            if away_coach_name:
                score, desc = get_coach_aggressiveness(away_coach_name)
                away_coach_data = (away_coach_name, score, desc)

            # News context
            news_context = await get_match_news_context(
                fixture["home_team_name"], fixture["away_team_name"]
            )

            # Run prediction algorithm
            prediction = predict_cards(
                home_stats=dict(home_stats) if home_stats else None,
                away_stats=dict(away_stats) if away_stats else None,
                home_form=[dict(r) for r in home_form] if home_form else None,
                away_form=[dict(r) for r in away_form] if away_form else None,
                referee=dict(referee_stats) if referee_stats else None,
                referee_last=[dict(r) for r in referee_last_games] if referee_last_games else None,
                h2h=[dict(r) for r in h2h_records] if h2h_records else None,
                home_coach_score=home_coach_data[1] if home_coach_data else None,
                away_coach_score=away_coach_data[1] if away_coach_data else None,
                home_lineup=[dict(r) for r in home_lineup] if home_lineup else None,
                away_lineup=[dict(r) for r in away_lineup] if away_lineup else None,
                news_context=news_context,
            )

            # Save prediction to DB
            await store.save_prediction(
                fixture_id=fixture_db_id,
                predicted_yc=prediction.predicted_total_yc,
                predicted_home_yc=prediction.predicted_home_yc,
                predicted_away_yc=prediction.predicted_away_yc,
                predicted_rc=prediction.predicted_total_rc,
                rc_probability=prediction.rc_probability,
                confidence=prediction.confidence,
            )

            # Convert DB records to dicts for image generator
            def _to_dict(rec):
                return dict(rec) if rec else None
            def _to_dicts(recs):
                return [dict(r) for r in recs] if recs else None

            from zoneinfo import ZoneInfo
            tz = ZoneInfo(settings.timezone)
            kickoff_dt = fixture["kickoff"].astimezone(tz)

            img_buf = generate_pre_game_image(
                home=fixture["home_team_name"],
                away=fixture["away_team_name"],
                competition=fixture["competition_name"],
                kickoff_str=kickoff_dt.strftime("%d/%m/%Y %H:%M"),
                home_pos=home_stats["standing_pos"] if home_stats else None,
                away_pos=away_stats["standing_pos"] if away_stats else None,
                home_stats=_to_dict(home_stats),
                away_stats=_to_dict(away_stats),
                home_yc_rank=home_yc_rank,
                away_yc_rank=away_yc_rank,
                home_rc_rank=home_rc_rank,
                away_rc_rank=away_rc_rank,
                home_form=_to_dicts(home_form),
                away_form=_to_dicts(away_form),
                home_top_players=_to_dicts(home_top_players),
                away_top_players=_to_dicts(away_top_players),
                home_player_ranks=home_player_ranks,
                away_player_ranks=away_player_ranks,
                h2h=_to_dicts(h2h_records),
                home_lineup=_to_dicts(home_lineup) if home_lineup else None,
                away_lineup=_to_dicts(away_lineup) if away_lineup else None,
                referee=_to_dict(referee_stats),
                referee_rank=referee_yc_rank,
                referee_total=referee_total_refs,
                referee_last=_to_dicts(referee_last_games),
                home_coach=home_coach_data,
                away_coach=away_coach_data,
                news_context=news_context or None,
                prediction={
                    "predicted_total_yc": prediction.predicted_total_yc,
                    "predicted_home_yc": prediction.predicted_home_yc,
                    "predicted_away_yc": prediction.predicted_away_yc,
                    "rc_probability": prediction.rc_probability,
                    "confidence": prediction.confidence,
                },
            )
            await telegram.send_photo(img_buf)

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
                home_player_ranks=home_player_ranks,
                away_player_ranks=away_player_ranks,
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
