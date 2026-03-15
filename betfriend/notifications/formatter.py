from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import asyncpg

from betfriend.config.settings import settings


def format_fixture_list(fixtures: list[asyncpg.Record]) -> str:
    """Format a list of fixtures into a simple Telegram message (HTML)."""
    tz = ZoneInfo(settings.timezone)

    if not fixtures:
        return "<b>BetFriend</b>\n\nNo hay partidos hoy."

    lines = ["<b>BetFriend - Partidos de hoy</b>\n"]

    current_comp = None
    for f in fixtures:
        comp = f["competition_name"]
        if comp != current_comp:
            lines.append(f"\n<b>{comp}</b>")
            current_comp = comp

        kickoff: datetime = f["kickoff"].astimezone(tz)
        time_str = kickoff.strftime("%H:%M")

        lines.append(
            f"  {f['home_team_name']} vs {f['away_team_name']} - {time_str}"
        )

    return "\n".join(lines)


def format_pre_game(fixture: asyncpg.Record) -> str:
    """Format a pre-game analysis message (Phase 1: basic info only)."""
    tz = ZoneInfo(settings.timezone)
    kickoff: datetime = fixture["kickoff"].astimezone(tz)
    date_str = kickoff.strftime("%d/%m/%Y %H:%M")

    lines = [
        f"<b>BetFriend - Analisis Pre-Partido</b>",
        f"",
        f"<b>{fixture['home_team_name']} vs {fixture['away_team_name']}</b>",
        f"{fixture['competition_name']} - {date_str}",
        f"",
        f"<i>Mas datos disponibles pronto...</i>",
    ]
    return "\n".join(lines)
