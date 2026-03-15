from __future__ import annotations

from datetime import datetime
from statistics import stdev
from zoneinfo import ZoneInfo

import asyncpg

from betfriend.config.settings import settings


def format_fixture_list(fixtures: list[asyncpg.Record]) -> str:
    tz = ZoneInfo(settings.timezone)
    if not fixtures:
        return "⚽ <b>BetFriend</b>\n\nNo hay partidos hoy."

    lines = ["⚽ <b>BetFriend - Partidos de hoy</b>\n"]
    current_comp = None
    for f in fixtures:
        comp = f["competition_name"]
        if comp != current_comp:
            lines.append(f"\n🏆 <b>{comp}</b>")
            current_comp = comp
        kickoff: datetime = f["kickoff"].astimezone(tz)
        lines.append(f"  ⏰ {f['home_team_name']} vs {f['away_team_name']} - {kickoff.strftime('%H:%M')}")
    return "\n".join(lines)


def _ord(n: int) -> str:
    if n == 1: return "1ro"
    if n == 2: return "2do"
    if n == 3: return "3ro"
    return f"{n}to"


def _team_block(
    name: str,
    stats: asyncpg.Record | None,
    yc_rank: int | None,
    rc_rank: int | None,
    form: list[asyncpg.Record] | None,
    top_players: list | None,
    player_ranks: dict | None,
) -> list[str]:
    """Build a compact team block with cards, form, and top 3 players."""
    lines = [f"  <b>{name}</b>"]
    player_ranks = player_ranks or {}

    if stats:
        yc_r = f" ({_ord(yc_rank)})" if yc_rank else ""
        rc_r = f" ({_ord(rc_rank)})" if rc_rank else ""
        lines.append(f"    🟨 {stats['total_yc']}{yc_r} — {stats['yc_per_game']:.1f}/J")
        lines.append(f"    🟥 {stats['total_rc']}{rc_r} — {stats['rc_per_game']:.2f}/J")

    if form:
        ycs = [r["yc"] for r in form]
        rcs = [r["rc"] for r in form]
        form_str = " | ".join(f"{r['yc']}🟨" + (f" {r['rc']}🟥" if r["rc"] > 0 else "") for r in form)
        avg_yc = sum(ycs) / len(ycs)
        avg_rc = sum(rcs) / len(rcs)
        delta = stdev(ycs) if len(ycs) > 1 else 0.0
        if delta <= 1.0:
            con = "✅ consistente"
        elif delta <= 2.0:
            con = "⚡ moderado"
        else:
            con = "🔴 variable"
        lines.append(f"    Ultimos {len(form)}")
        lines.append(f"     {form_str}")
        lines.append(f"    AVG {avg_yc:.1f} 🟨  {avg_rc:.1f} 🟥 por juego")
        lines.append(f"    delta {delta:.1f} ({con})")

    if top_players:
        for p in top_players[:3]:
            rank = player_ranks.get(p["name"])
            rank_str = f" — {_ord(rank)} en la liga" if rank else ""
            lines.append(
                f"    🟨 {p['total_yc']} / 🟥 {p['total_rc']} — "
                f"{p['name']} ({p['games_played']}J, {p['yc_per_game']:.2f}/J)"
                f"{rank_str}"
            )

    return lines


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
    h2h_records: list | None = None,
    home_player_ranks: dict | None = None,
    away_player_ranks: dict | None = None,
) -> str:
    tz = ZoneInfo(settings.timezone)
    kickoff: datetime = fixture["kickoff"].astimezone(tz)

    home = fixture["home_team_name"]
    away = fixture["away_team_name"]

    # Header with standings inline
    home_pos = f" {_ord(home_stats['standing_pos'])}" if home_stats and home_stats["standing_pos"] else ""
    away_pos = f" {_ord(away_stats['standing_pos'])}" if away_stats and away_stats["standing_pos"] else ""

    lines = [
        "⚽ <b>BetFriend - Analisis Pre-Partido</b>",
        "",
        f"🏟 <b>{home}{home_pos} vs {away}{away_pos}</b>",
        f"🏆 {fixture['competition_name']} — {kickoff.strftime('%d/%m/%Y %H:%M')}",
    ]

    # --- Team card blocks ---
    lines.append("")
    lines.append("🟨🟥 <b>Tarjetas temporada</b>")

    lines.extend(_team_block(home, home_stats, home_yc_rank, home_rc_rank, home_form, home_top_players, home_player_ranks))
    lines.append("")
    lines.extend(_team_block(away, away_stats, away_yc_rank, away_rc_rank, away_form, away_top_players, away_player_ranks))

    # --- H2H ---
    if h2h_records:
        lines.append("")
        lines.append(f"🔄 <b>Ultimos {len(h2h_records)} enfrentamientos</b>")
        total_yc = 0
        total_rc = 0
        for h in h2h_records:
            tyc = h["team_a_yc"] + h["team_b_yc"]
            trc = h["team_a_rc"] + h["team_b_rc"]
            total_yc += tyc
            total_rc += trc
            score = f"{h['team_a_score']}-{h['team_b_score']}" if h["team_a_score"] is not None else "?"
            lines.append(f"  {h['team_a_name']} {score} {h['team_b_name']}: {tyc}🟨 {trc}🟥")
        if len(h2h_records) > 1:
            lines.append(f"  AVG {total_yc/len(h2h_records):.1f} 🟨 / {total_rc/len(h2h_records):.1f} 🟥")

    # --- Lineup ---
    if home_lineup or away_lineup:
        lines.append("")
        lines.append("📋 <b>Alineacion</b>")
        for name, lineup, ranks in [
            (home, home_lineup, home_player_ranks or {}),
            (away, away_lineup, away_player_ranks or {}),
        ]:
            if not lineup:
                continue
            starters = [p for p in lineup if p["is_starter"]]
            lines.append(f"  <b>{name}</b>")
            for p in starters:
                risk = ""
                if p["games_played"] >= 3:
                    if p["yc_per_game"] >= 0.5:
                        risk = " ⚠️ALTA"
                    elif p["yc_per_game"] >= 0.3:
                        risk = " ⚡"
                rank = ranks.get(p["player_name"])
                rank_str = f" [{_ord(rank)}]" if rank else ""
                lines.append(
                    f"    {p['player_name']} ({p['position'] or '?'}): "
                    f"{p['total_yc']}🟨 {p['total_rc']}🟥 "
                    f"({p['yc_per_game']:.2f}/J){rank_str}{risk}"
                )

    # --- Referee ---
    if referee_stats and referee_stats["games"] > 0:
        lines.append("")
        rank_str = f" ({_ord(referee_yc_rank)} de {referee_total_refs})" if referee_yc_rank and referee_total_refs else ""
        lines.append(f"👨‍⚖️ <b>{referee_stats['name']}</b>{rank_str}")
        lines.append(f"  🟨 {referee_stats['total_yc']} / 🟥 {referee_stats['total_rc']} en {referee_stats['games']}J — {referee_stats['yc_per_game']:.1f}/J")

        if referee_last_games:
            ref_ycs = [g["total_yc"] for g in referee_last_games]
            ref_delta = stdev(ref_ycs) if len(ref_ycs) > 1 else 0.0
            con = "✅" if ref_delta <= 1.5 else "🔴"
            lines.append(f"  Ultimos {len(referee_last_games)}:")
            for g in referee_last_games:
                lines.append(f"    {g['home_team']} vs {g['away_team']}: {g['total_yc']}🟨 {g['total_rc']}🟥")
            lines.append(f"  delta {ref_delta:.1f} {con}")
    else:
        lines.append("")
        lines.append("👨‍⚖️ <i>Arbitro: sin datos</i>")

    return "\n".join(lines)
