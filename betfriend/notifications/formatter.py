from __future__ import annotations

from datetime import datetime
from statistics import stdev
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


def _ordinal(n: int) -> str:
    """Return Spanish ordinal: 1->1ro, 2->2do, etc."""
    if n == 1:
        return "1ro"
    if n == 2:
        return "2do"
    if n == 3:
        return "3ro"
    return f"{n}to"


def _form_delta(form_records: list[asyncpg.Record]) -> tuple[float, float, str]:
    """Compute avg YC, delta (stdev), and form string from last N games."""
    if not form_records:
        return 0.0, 0.0, "-"
    ycs = [r["yc"] for r in form_records]
    avg = sum(ycs) / len(ycs)
    delta = stdev(ycs) if len(ycs) > 1 else 0.0
    form_str = " | ".join(f"{r['yc']}YC" for r in form_records)
    return avg, delta, form_str


def format_pre_game(
    fixture: asyncpg.Record,
    home_stats: asyncpg.Record | None = None,
    away_stats: asyncpg.Record | None = None,
    home_yc_rank: int | None = None,
    away_yc_rank: int | None = None,
    home_rc_rank: int | None = None,
    away_rc_rank: int | None = None,
    home_form: list[asyncpg.Record] | None = None,
    away_form: list[asyncpg.Record] | None = None,
    home_top_players: list[asyncpg.Record] | None = None,
    away_top_players: list[asyncpg.Record] | None = None,
    referee_stats: asyncpg.Record | None = None,
    referee_yc_rank: int | None = None,
    referee_total_refs: int | None = None,
    referee_last_games: list[asyncpg.Record] | None = None,
    home_lineup: list | None = None,
    away_lineup: list | None = None,
) -> str:
    """Format a pre-game analysis message with card stats."""
    tz = ZoneInfo(settings.timezone)
    kickoff: datetime = fixture["kickoff"].astimezone(tz)
    date_str = kickoff.strftime("%d/%m/%Y %H:%M")

    home = fixture["home_team_name"]
    away = fixture["away_team_name"]

    lines = [
        f"<b>BetFriend - Analisis Pre-Partido</b>",
        f"",
        f"<b>{home} vs {away}</b>",
        f"{fixture['competition_name']} - {date_str}",
    ]

    # --- Team stats section ---
    if home_stats and away_stats:
        lines.append("")
        lines.append("<b>Posicion en la tabla</b>")

        for name, stats in [(home, home_stats), (away, away_stats)]:
            pos = stats["standing_pos"]
            pts = stats["standing_pts"]
            pos_str = f"{_ordinal(pos)} ({pts} pts)" if pos else "N/A"
            lines.append(f"  {name}: {pos_str}")

        lines.append("")
        lines.append("<b>Tarjetas temporada</b>")

        for name, stats, yc_rank, rc_rank in [
            (home, home_stats, home_yc_rank, home_rc_rank),
            (away, away_stats, away_yc_rank, away_rc_rank),
        ]:
            yc_rank_str = f" ({_ordinal(yc_rank)} mas amarillas)" if yc_rank else ""
            rc_rank_str = f" ({_ordinal(rc_rank)} mas rojas)" if rc_rank else ""
            lines.append(
                f"  {name}: {stats['total_yc']} YC{yc_rank_str} "
                f"/ {stats['total_rc']} RC{rc_rank_str} "
                f"({stats['games_played']}J, avg {stats['yc_per_game']:.1f} YC/J)"
            )

    # --- Last 5 form ---
    if home_form is not None and away_form is not None:
        lines.append("")
        lines.append("<b>Ultimos 5 partidos (tarjetas)</b>")

        for name, form in [(home, home_form), (away, away_form)]:
            avg, delta, form_str = _form_delta(form)
            consistency = "consistente" if delta < 1.0 else "variable"
            lines.append(f"  {name}: {form_str}")
            lines.append(f"    avg {avg:.1f} YC/J | delta {delta:.1f} ({consistency})")

    # --- Top card players ---
    if home_top_players or away_top_players:
        lines.append("")
        lines.append("<b>Jugadores con mas tarjetas</b>")

        for name, players in [(home, home_top_players), (away, away_top_players)]:
            if not players:
                continue
            lines.append(f"  <b>{name}</b>")
            for p in players[:5]:
                lines.append(
                    f"    {p['name']}: {p['total_yc']} YC / {p['total_rc']} RC "
                    f"({p['games_played']}J, avg {p['yc_per_game']:.2f}/J)"
                )

    # --- Lineup with card risk ---
    if home_lineup or away_lineup:
        lines.append("")
        lines.append("<b>Alineacion - Riesgo de tarjetas</b>")

        for name, lineup in [(home, home_lineup), (away, away_lineup)]:
            if not lineup:
                continue
            starters = [p for p in lineup if p["is_starter"]]
            subs = [p for p in lineup if not p["is_starter"]]

            lines.append(f"  <b>{name} (titulares)</b>")
            for p in starters:
                risk = ""
                if p["games_played"] >= 3:
                    if p["yc_per_game"] >= 0.5:
                        risk = " ⚠️ALTA"
                    elif p["yc_per_game"] >= 0.3:
                        risk = " ⚡media"
                lines.append(
                    f"    {p['player_name']} ({p['position'] or '?'}): "
                    f"{p['total_yc']}YC/{p['total_rc']}RC "
                    f"({p['games_played']}J, {p['yc_per_game']:.2f}/J){risk}"
                )

            # Show top subs with cards
            risky_subs = [s for s in subs if s["total_yc"] > 0][:3]
            if risky_subs:
                lines.append(f"  <i>Suplentes destacados:</i>")
                for p in risky_subs:
                    lines.append(
                        f"    {p['player_name']}: {p['total_yc']}YC/{p['total_rc']}RC"
                    )

    # --- Referee section ---
    if referee_stats and referee_stats["games"] > 0:
        lines.append("")
        lines.append("<b>Arbitro</b>")
        lines.append(f"  {referee_stats['name']}")

        rank_str = ""
        if referee_yc_rank and referee_total_refs:
            rank_str = f" ({_ordinal(referee_yc_rank)} de {referee_total_refs} arbitros)"

        lines.append(
            f"  {referee_stats['total_yc']} YC / {referee_stats['total_rc']} RC "
            f"en {referee_stats['games']} partidos{rank_str}"
        )
        lines.append(
            f"  avg {referee_stats['yc_per_game']:.1f} YC/J | "
            f"{referee_stats['rc_per_game']:.2f} RC/J"
        )

        if referee_last_games:
            # Compute referee delta
            ref_ycs = [g["total_yc"] for g in referee_last_games]
            ref_avg = sum(ref_ycs) / len(ref_ycs) if ref_ycs else 0
            ref_delta = stdev(ref_ycs) if len(ref_ycs) > 1 else 0
            consistency = "consistente" if ref_delta < 1.5 else "variable"

            lines.append(f"  Ultimos {len(referee_last_games)} partidos:")
            for g in referee_last_games:
                lines.append(
                    f"    {g['home_team']} vs {g['away_team']}: "
                    f"{g['total_yc']} YC / {g['total_rc']} RC"
                )
            lines.append(f"  delta {ref_delta:.1f} ({consistency})")
    else:
        lines.append("")
        lines.append("<i>Arbitro: sin datos disponibles</i>")

    return "\n".join(lines)
