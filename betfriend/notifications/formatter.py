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
        return "⚽ <b>BetFriend</b>\n\nNo hay partidos hoy."

    lines = ["⚽ <b>BetFriend - Partidos de hoy</b>\n"]

    current_comp = None
    for f in fixtures:
        comp = f["competition_name"]
        if comp != current_comp:
            lines.append(f"\n🏆 <b>{comp}</b>")
            current_comp = comp

        kickoff: datetime = f["kickoff"].astimezone(tz)
        time_str = kickoff.strftime("%H:%M")

        lines.append(
            f"  ⏰ {f['home_team_name']} vs {f['away_team_name']} - {time_str}"
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


def _form_delta(form_records: list[asyncpg.Record]) -> tuple[float, float, float, float, str]:
    """Compute avg YC, avg RC, delta YC, delta RC, and form string."""
    if not form_records:
        return 0.0, 0.0, 0.0, 0.0, "-"
    ycs = [r["yc"] for r in form_records]
    rcs = [r["rc"] for r in form_records]
    avg_yc = sum(ycs) / len(ycs)
    avg_rc = sum(rcs) / len(rcs)
    delta_yc = stdev(ycs) if len(ycs) > 1 else 0.0
    delta_rc = stdev(rcs) if len(rcs) > 1 else 0.0
    form_str = " | ".join(f"{r['yc']}YC {r['rc']}RC" for r in form_records)
    return avg_yc, avg_rc, delta_yc, delta_rc, form_str


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
    """Format a pre-game analysis message with card stats."""
    tz = ZoneInfo(settings.timezone)
    kickoff: datetime = fixture["kickoff"].astimezone(tz)
    date_str = kickoff.strftime("%d/%m/%Y %H:%M")

    home = fixture["home_team_name"]
    away = fixture["away_team_name"]
    home_player_ranks = home_player_ranks or {}
    away_player_ranks = away_player_ranks or {}

    lines = [
        f"⚽ <b>BetFriend - Analisis Pre-Partido</b>",
        f"",
        f"🏟 <b>{home} vs {away}</b>",
        f"🏆 {fixture['competition_name']} — {date_str}",
    ]

    # --- Team stats section ---
    if home_stats and away_stats:
        lines.append("")
        lines.append("📊 <b>Posicion en la tabla</b>")

        for name, stats in [(home, home_stats), (away, away_stats)]:
            pos = stats["standing_pos"]
            pts = stats["standing_pts"]
            pos_str = f"{_ordinal(pos)} ({pts} pts)" if pos else "N/A"
            lines.append(f"  {name}: {pos_str}")

        lines.append("")
        lines.append("🟨🟥 <b>Tarjetas temporada</b>")

        for name, stats, yc_rank, rc_rank in [
            (home, home_stats, home_yc_rank, home_rc_rank),
            (away, away_stats, away_yc_rank, away_rc_rank),
        ]:
            yc_rank_str = f" ({_ordinal(yc_rank)} mas YC)" if yc_rank else ""
            rc_rank_str = f" ({_ordinal(rc_rank)} mas RC)" if rc_rank else ""
            lines.append(f"  <b>{name}</b>")
            lines.append(
                f"    🟨 {stats['total_yc']} amarillas{yc_rank_str} — avg {stats['yc_per_game']:.1f}/J"
            )
            lines.append(
                f"    🟥 {stats['total_rc']} rojas{rc_rank_str} — avg {stats['rc_per_game']:.2f}/J"
            )
            lines.append(f"    ({stats['games_played']} partidos jugados)")

    # --- Last 5 form ---
    if home_form is not None and away_form is not None:
        lines.append("")
        lines.append("📈 <b>Ultimos 5 partidos (tarjetas)</b>")

        for name, form in [(home, home_form), (away, away_form)]:
            avg_yc, avg_rc, delta_yc, delta_rc, form_str = _form_delta(form)
            if delta_yc <= 1.0:
                consistency = "✅ consistente"
            elif delta_yc <= 2.0:
                consistency = "⚡ moderado"
            else:
                consistency = "🔴 variable"
            lines.append(f"  <b>{name}</b>: {form_str}")
            lines.append(
                f"    avg {avg_yc:.1f} YC/J | {avg_rc:.1f} RC/J | delta {delta_yc:.1f} ({consistency})"
            )

    # --- Top card players ---
    if home_top_players or away_top_players:
        lines.append("")
        lines.append("👤 <b>Jugadores con mas tarjetas</b>")

        for name, players, ranks in [
            (home, home_top_players, home_player_ranks),
            (away, away_top_players, away_player_ranks),
        ]:
            if not players:
                continue
            lines.append(f"  <b>{name}</b>")
            for p in players[:5]:
                rank = ranks.get(p["name"])
                rank_str = f" — {_ordinal(rank)} en la liga" if rank else ""
                lines.append(
                    f"    🟨 {p['total_yc']} / 🟥 {p['total_rc']} — "
                    f"{p['name']} ({p['games_played']}J, {p['yc_per_game']:.2f}/J)"
                    f"{rank_str}"
                )

    # --- Head to Head ---
    if h2h_records:
        lines.append("")
        lines.append(f"🔄 <b>Ultimos {len(h2h_records)} enfrentamientos</b>")
        total_h2h_yc = 0
        total_h2h_rc = 0
        for h in h2h_records:
            total_yc = h["team_a_yc"] + h["team_b_yc"]
            total_rc = h["team_a_rc"] + h["team_b_rc"]
            total_h2h_yc += total_yc
            total_h2h_rc += total_rc
            score = f"{h['team_a_score']}-{h['team_b_score']}" if h["team_a_score"] is not None else "?"
            lines.append(
                f"  {h['team_a_name']} {score} {h['team_b_name']}: "
                f"🟨{total_yc} 🟥{total_rc}"
            )
        if len(h2h_records) > 1:
            avg_yc = total_h2h_yc / len(h2h_records)
            avg_rc = total_h2h_rc / len(h2h_records)
            lines.append(f"  avg {avg_yc:.1f} YC / {avg_rc:.1f} RC por partido")

    # --- Lineup with card risk ---
    if home_lineup or away_lineup:
        lines.append("")
        lines.append("📋 <b>Alineacion — Riesgo de tarjetas</b>")

        for name, lineup, ranks in [
            (home, home_lineup, home_player_ranks),
            (away, away_lineup, away_player_ranks),
        ]:
            if not lineup:
                continue
            starters = [p for p in lineup if p["is_starter"]]
            subs = [p for p in lineup if not p["is_starter"]]

            lines.append(f"  <b>{name} (titulares)</b>")
            for p in starters:
                risk = ""
                if p["games_played"] >= 3:
                    if p["yc_per_game"] >= 0.5:
                        risk = " ⚠️ ALTA"
                    elif p["yc_per_game"] >= 0.3:
                        risk = " ⚡ media"
                rank = ranks.get(p["player_name"])
                rank_str = f" [{_ordinal(rank)} liga]" if rank else ""
                lines.append(
                    f"    {p['player_name']} ({p['position'] or '?'}): "
                    f"🟨{p['total_yc']} 🟥{p['total_rc']} "
                    f"({p['games_played']}J, {p['yc_per_game']:.2f}/J)"
                    f"{rank_str}{risk}"
                )

            risky_subs = [s for s in subs if s["total_yc"] > 0][:3]
            if risky_subs:
                lines.append(f"  <i>Suplentes destacados:</i>")
                for p in risky_subs:
                    lines.append(
                        f"    {p['player_name']}: 🟨{p['total_yc']} 🟥{p['total_rc']}"
                    )

    # --- Referee section ---
    if referee_stats and referee_stats["games"] > 0:
        lines.append("")
        lines.append(f"👨‍⚖️ <b>Arbitro: {referee_stats['name']}</b>")

        rank_str = ""
        if referee_yc_rank and referee_total_refs:
            rank_str = f" ({_ordinal(referee_yc_rank)} de {referee_total_refs} arbitros)"

        lines.append(
            f"  🟨 {referee_stats['total_yc']} YC / 🟥 {referee_stats['total_rc']} RC "
            f"en {referee_stats['games']} partidos{rank_str}"
        )
        lines.append(
            f"  avg {referee_stats['yc_per_game']:.1f} YC/J | "
            f"{referee_stats['rc_per_game']:.2f} RC/J"
        )

        if referee_last_games:
            ref_ycs = [g["total_yc"] for g in referee_last_games]
            ref_delta = stdev(ref_ycs) if len(ref_ycs) > 1 else 0.0
            if ref_delta <= 1.5:
                consistency = "✅ consistente"
            else:
                consistency = "🔴 variable"

            lines.append(f"  Ultimos {len(referee_last_games)} partidos:")
            for g in referee_last_games:
                lines.append(
                    f"    {g['home_team']} vs {g['away_team']}: "
                    f"🟨{g['total_yc']} 🟥{g['total_rc']}"
                )
            lines.append(f"  delta {ref_delta:.1f} ({consistency})")
    else:
        lines.append("")
        lines.append("👨‍⚖️ <i>Arbitro: sin datos disponibles</i>")

    return "\n".join(lines)
