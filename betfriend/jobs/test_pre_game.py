"""Test script — simulates pre-game analysis for a specific team's next fixture."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger

from betfriend.api.budget import BudgetTracker
from betfriend.api.client import APIFootballClient, parse_fixture
from betfriend.config.settings import settings
from betfriend.db.store import Store
from betfriend.notifications.image import generate_pre_game_image
from betfriend.notifications.telegram import TelegramNotifier
from betfriend.analysis.cards import predict_cards
from betfriend.scrapers.coaches import get_coach_aggressiveness, get_coach_by_team
from betfriend.scrapers.news import get_match_news_context


async def run() -> None:
    store = Store()
    await store.start()

    budget = BudgetTracker(store)
    api = APIFootballClient(store, budget)
    telegram = TelegramNotifier()

    try:
        # Find tomorrow's fixtures
        tomorrow = datetime.now(ZoneInfo("America/Bogota")).date() + timedelta(days=1)
        logger.info(f"Looking for fixtures on {tomorrow}")

        # Make sure we have today's and tomorrow's fixtures
        today_bogota = datetime.now(ZoneInfo("America/Bogota")).date()
        for target_date in (today_bogota, tomorrow):
          for league_id in (settings.la_liga_id, settings.la_liga2_id):
            raw_fixtures = await api.get_fixtures_by_date(league_id, target_date)
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
                logger.info(f"  Fixture {parsed['home_team_name']} vs {parsed['away_team_name']}: referee='{ref_name}'")
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

        # Find Barcelona fixture — try today first, then tomorrow
        today = datetime.now(ZoneInfo("America/Bogota")).date()
        db_fixtures = await store.get_fixtures_by_date(today)
        if not db_fixtures:
            db_fixtures = await store.get_fixtures_by_date(tomorrow)
        if not db_fixtures:
            logger.error("No fixtures found")
            return

        barca_fixture = None
        # Priority 1: Barcelona
        for f in db_fixtures:
            if "barce" in f["home_team_name"].lower() or "barce" in f["away_team_name"].lower():
                barca_fixture = f
                break
        # Priority 2: Any La Liga game
        if not barca_fixture:
            for f in db_fixtures:
                if f["competition_name"] == "La Liga":
                    barca_fixture = f
                    break
        # Priority 3: First fixture
        if not barca_fixture:
            barca_fixture = db_fixtures[0]

        fixture = barca_fixture
        home_id = fixture["home_team_id"]
        away_id = fixture["away_team_id"]
        fixture_db_id = fixture["id"]

        logger.info(f"Testing: {fixture['home_team_name']} vs {fixture['away_team_name']} (referee_id={fixture['referee_id']}, matchday={fixture['matchday']})")

        # If no referee or referee has 0 games (bad match), try RFEF PDF
        needs_referee = fixture["referee_id"] is None
        if fixture["referee_id"] is not None:
            ref_check = await store.get_referee_stats(fixture["referee_id"])
            if ref_check and ref_check.get("games", 0) == 0:
                needs_referee = True
                logger.info(f"  Referee id={fixture['referee_id']} has 0 games, re-doing lookup")
        if needs_referee and fixture["matchday"]:
            from betfriend.scrapers.rfef_pdf import fetch_referee_designations, match_referee_to_fixture
            # Determine league_id from competition
            async with store.pool.acquire() as conn:
                comp_row = await conn.fetchrow("SELECT api_id FROM competitions WHERE id=$1", fixture["competition_id"])
            league_api_id = comp_row["api_id"] if comp_row else None
            if league_api_id:
                logger.info(f"  Trying RFEF PDF for league {league_api_id}, jornada {fixture['matchday']}")
                designations = await fetch_referee_designations(league_api_id, fixture["matchday"])
                logger.info(f"  RFEF designations found: {len(designations)} — {designations}")
                if designations:
                    ref_name = match_referee_to_fixture(
                        designations, fixture["home_team_name"], fixture["away_team_name"]
                    )
                    if ref_name:
                        ref_id = await store.upsert_referee(ref_name)
                        await store.assign_referee_to_fixture(fixture_db_id, ref_id)
                        await store.recompute_referee_stats()
                        # Re-fetch fixture to get referee_id
                        db_fixtures = await store.get_fixtures_by_date(today)
                        for f in db_fixtures:
                            if f["id"] == fixture_db_id:
                                fixture = f
                                break
                        logger.info(f"  Assigned referee '{ref_name}' (id={ref_id})")
                    else:
                        logger.warning(f"  Could not match referee for {fixture['home_team_name']} vs {fixture['away_team_name']}")
                else:
                    logger.warning("  No RFEF PDF found for this matchday")

        # Try to fetch lineup (might not be available yet)
        from betfriend.jobs.pre_game_check import _fetch_and_store_lineup
        has_lineup = await store.has_lineup(fixture_db_id)
        if not has_lineup:
            has_lineup = await _fetch_and_store_lineup(api, store, fixture["api_id"], fixture_db_id)
            logger.info(f"Lineup available: {has_lineup}")

        home_lineup = await store.get_fixture_lineup(fixture_db_id, home_id) if has_lineup else []
        away_lineup = await store.get_fixture_lineup(fixture_db_id, away_id) if has_lineup else []

        # Fetch all analysis data (0 API calls — all from DB)
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
                        # If no card data from DB, fetch events from API
                        if h_yc == 0 and a_yc == 0:
                            events = await api.get_fixture_events(parsed["api_id"])
                            for ev in events:
                                if ev.get("type") == "Card":
                                    tid = ev.get("team", {}).get("id")
                                    detail = ev.get("detail", "")
                                    if "Yellow" in detail:
                                        if tid == parsed["home_team_api_id"]:
                                            h_yc += 1
                                        else:
                                            a_yc += 1
                                    elif "Red" in detail:
                                        if tid == parsed["home_team_api_id"]:
                                            h_rc += 1
                                        else:
                                            a_rc += 1
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

        # Player league rankings
        home_player_ranks: dict[str, int] = {}
        away_player_ranks: dict[str, int] = {}
        for players, ranks in [(home_top_players, home_player_ranks), (away_top_players, away_player_ranks)]:
            if not players:
                continue
            for p in players[:5]:
                async with store.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT pcs.player_id FROM player_card_stats pcs JOIN players pl ON pl.id=pcs.player_id WHERE pl.name=$1 LIMIT 1",
                        p["name"]
                    )
                if row:
                    rank = await store.get_player_league_yc_rank(row["player_id"])
                    if rank:
                        ranks[p["name"]] = rank

        # Referee
        referee_stats = None
        referee_yc_rank = None
        referee_total_refs = None
        referee_last_games = None
        if fixture["referee_id"]:
            referee_stats = await store.get_referee_stats(fixture["referee_id"])
            referee_yc_rank = await store.get_referee_yc_rank(fixture["referee_id"])
            referee_total_refs = await store.get_total_referees_with_games()
            referee_last_games = await store.get_referee_last_games(fixture["referee_id"], 5)

        # Convert DB records to dicts for image generator
        def _to_dict(rec):
            return dict(rec) if rec else None
        def _to_dicts(recs):
            return [dict(r) for r in recs] if recs else None

        tz = ZoneInfo(settings.timezone)
        kickoff_dt = fixture["kickoff"].astimezone(tz)

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
        news_context, news_urls = await get_match_news_context(
            fixture["home_team_name"], fixture["away_team_name"]
        )

        # Run prediction
        prediction = predict_cards(
            home_stats=_to_dict(home_stats),
            away_stats=_to_dict(away_stats),
            home_form=_to_dicts(home_form),
            away_form=_to_dicts(away_form),
            referee=_to_dict(referee_stats),
            referee_last=_to_dicts(referee_last_games),
            h2h=_to_dicts(h2h_records),
            home_coach_score=home_coach_data[1] if home_coach_data else None,
            away_coach_score=away_coach_data[1] if away_coach_data else None,
            home_lineup=_to_dicts(home_lineup) if home_lineup else None,
            away_lineup=_to_dicts(away_lineup) if away_lineup else None,
            news_context=news_context,
        )
        logger.info(f"Prediction: {prediction.summary}")

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
        # Send news article links as separate text message
        if news_urls:
            links_msg = "📰 <b>Fuentes:</b>\n" + "\n".join(
                f"• {url}" for url in news_urls
            )
            await telegram.send(links_msg)
        logger.info("Test pre-game image sent!")

    finally:
        await api.close()
        await store.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
