"""Generate pre-game analysis as a PNG image using Pillow."""
from __future__ import annotations

from io import BytesIO
from statistics import stdev

from PIL import Image, ImageDraw, ImageFont

# ── Colors ──
BG = (24, 26, 32)
CARD_BG = (32, 36, 44)
WHITE = (230, 230, 230)
GRAY = (140, 145, 155)
YELLOW = (255, 200, 50)
RED = (220, 50, 50)
GREEN = (80, 200, 120)
ORANGE = (255, 160, 50)
ACCENT = (100, 140, 255)
DIVIDER = (55, 60, 70)

# ── Fonts (system fallback) ──
def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    names = [
        "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf",
        "arial.ttf", "Arial.ttf",
    ]
    for name in names:
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()

TITLE = _font(22, bold=True)
HEADER = _font(16, bold=True)
BODY = _font(14)
BODY_B = _font(14, bold=True)
SMALL = _font(12)


def _ord(n: int) -> str:
    if n == 1: return "1ro"
    if n == 2: return "2do"
    if n == 3: return "3ro"
    return f"{n}to"


def generate_pre_game_image(
    home: str,
    away: str,
    competition: str,
    kickoff_str: str,
    home_pos: int | None = None,
    away_pos: int | None = None,
    home_stats: dict | None = None,
    away_stats: dict | None = None,
    home_yc_rank: int | None = None,
    away_yc_rank: int | None = None,
    home_rc_rank: int | None = None,
    away_rc_rank: int | None = None,
    home_form: list[dict] | None = None,
    away_form: list[dict] | None = None,
    home_top_players: list[dict] | None = None,
    away_top_players: list[dict] | None = None,
    home_player_ranks: dict | None = None,
    away_player_ranks: dict | None = None,
    h2h: list[dict] | None = None,
    home_lineup: list[dict] | None = None,
    away_lineup: list[dict] | None = None,
    referee: dict | None = None,
    referee_rank: int | None = None,
    referee_total: int | None = None,
    referee_last: list[dict] | None = None,
    home_coach: tuple[str, int, str] | None = None,
    away_coach: tuple[str, int, str] | None = None,
    news_context: str | None = None,
    prediction: dict | None = None,
) -> BytesIO:
    """Generate a pre-game card image and return as BytesIO PNG."""
    W = 700
    home_player_ranks = home_player_ranks or {}
    away_player_ranks = away_player_ranks or {}

    # Pre-calculate height
    y = 0
    y += 100  # header
    y += _calc_team_height(home_stats, home_form, home_top_players)
    y += 10
    y += _calc_team_height(away_stats, away_form, away_top_players)
    if h2h:
        y += 30 + len(h2h) * 22 + 30
    if home_lineup or away_lineup:
        for lineup in [home_lineup, away_lineup]:
            if lineup:
                starters = [p for p in lineup if p.get("is_starter")]
                y += 30 + len(starters) * 20 + 10
    if home_coach or away_coach:
        y += 30 + 25 * (bool(home_coach) + bool(away_coach))
    if referee:
        y += 80
        if referee_last:
            y += len(referee_last) * 20 + 25
    if news_context:
        y += 70
    if prediction:
        y += 120
    y += 40  # footer padding

    img = Image.new("RGB", (W, y), BG)
    d = ImageDraw.Draw(img)
    cy = 0

    # ── Header ──
    d.rectangle([0, 0, W, 90], fill=CARD_BG)
    home_pos_str = f" ({_ord(home_pos)})" if home_pos else ""
    away_pos_str = f" ({_ord(away_pos)})" if away_pos else ""
    title = f"{home}{home_pos_str}  vs  {away}{away_pos_str}"
    d.text((20, 15), title, fill=WHITE, font=TITLE)
    d.text((20, 48), f"BetFriend  |  {competition}  —  {kickoff_str}", fill=GRAY, font=BODY)
    d.text((20, 68), "BetFriend", fill=ACCENT, font=SMALL)
    cy = 95

    # ── Team blocks ──
    for name, stats, yc_rank, rc_rank, form, players, p_ranks in [
        (home, home_stats, home_yc_rank, home_rc_rank, home_form, home_top_players, home_player_ranks),
        (away, away_stats, away_yc_rank, away_rc_rank, away_form, away_top_players, away_player_ranks),
    ]:
        cy = _draw_team_block(d, cy, W, name, stats, yc_rank, rc_rank, form, players, p_ranks)
        cy += 10

    # ── H2H ──
    if h2h:
        d.line([(20, cy), (W - 20, cy)], fill=DIVIDER, width=1)
        cy += 8
        d.text((20, cy), "H2H  Ultimos enfrentamientos", fill=WHITE, font=HEADER)
        cy += 25
        total_yc = 0
        total_rc = 0
        for h in h2h:
            a_yc = h.get("team_a_yc", 0)
            a_rc = h.get("team_a_rc", 0)
            b_yc = h.get("team_b_yc", 0)
            b_rc = h.get("team_b_rc", 0)
            total_yc += a_yc + b_yc
            total_rc += a_rc + b_rc
            score = f"{h.get('team_a_score', '?')}-{h.get('team_b_score', '?')}"
            # Date
            match_date = h.get("match_date")
            date_str = ""
            if match_date:
                if hasattr(match_date, "strftime"):
                    date_str = match_date.strftime("%d/%m/%y") + "  "
                else:
                    date_str = str(match_date)[:10] + "  "
            team_a = h.get('team_a_name', '?')
            team_b = h.get('team_b_name', '?')
            d.text((30, cy), f"{date_str}{team_a} {score} {team_b}", fill=GRAY, font=SMALL)
            # Cards per team right-aligned
            rx = W - 30
            # Team B cards
            d.text((rx - d.textlength(str(b_rc), font=SMALL), cy), str(b_rc), fill=GRAY, font=SMALL)
            rx -= d.textlength(str(b_rc), font=SMALL) + 3
            d.rectangle([rx - 8, cy + 2, rx, cy + 12], fill=RED)
            rx -= 12
            d.text((rx - d.textlength(str(b_yc), font=SMALL), cy), str(b_yc), fill=GRAY, font=SMALL)
            rx -= d.textlength(str(b_yc), font=SMALL) + 3
            d.rectangle([rx - 8, cy + 2, rx, cy + 12], fill=YELLOW)
            rx -= 18
            # Separator
            d.text((rx - 4, cy), "|", fill=DIVIDER, font=SMALL)
            rx -= 14
            # Team A cards
            d.text((rx - d.textlength(str(a_rc), font=SMALL), cy), str(a_rc), fill=GRAY, font=SMALL)
            rx -= d.textlength(str(a_rc), font=SMALL) + 3
            d.rectangle([rx - 8, cy + 2, rx, cy + 12], fill=RED)
            rx -= 12
            d.text((rx - d.textlength(str(a_yc), font=SMALL), cy), str(a_yc), fill=GRAY, font=SMALL)
            rx -= d.textlength(str(a_yc), font=SMALL) + 3
            d.rectangle([rx - 8, cy + 2, rx, cy + 12], fill=YELLOW)
            cy += 20
        if len(h2h) > 1:
            avg_yc = total_yc / len(h2h)
            avg_rc = total_rc / len(h2h)
            d.text((30, cy), "AVG", fill=WHITE, font=BODY_B)
            ax = 70
            d.rectangle([ax, cy + 2, ax + 10, cy + 14], fill=YELLOW)
            d.text((ax + 14, cy), f"{avg_yc:.1f}", fill=WHITE, font=BODY_B)
            ax += 50
            d.rectangle([ax, cy + 2, ax + 10, cy + 14], fill=RED)
            d.text((ax + 14, cy), f"{avg_rc:.1f}", fill=WHITE, font=BODY_B)
            cy += 22

    # ── Lineup ──
    if home_lineup or away_lineup:
        d.line([(20, cy), (W - 20, cy)], fill=DIVIDER, width=1)
        cy += 8
        d.text((20, cy), "ALINEACION", fill=WHITE, font=HEADER)
        cy += 25
        for name, lineup, ranks in [
            (home, home_lineup, home_player_ranks),
            (away, away_lineup, away_player_ranks),
        ]:
            if not lineup:
                continue
            d.text((30, cy), name, fill=ACCENT, font=BODY_B)
            cy += 20
            starters = [p for p in lineup if p.get("is_starter")]
            for p in starters:
                pname = p.get("player_name", "?")
                pos = p.get("position", "?")
                yc = p.get("total_yc", 0)
                rc = p.get("total_rc", 0)
                ypg = p.get("yc_per_game", 0)
                gp = p.get("games_played", 0)

                # Risk color
                color = GRAY
                risk = ""
                if gp >= 3:
                    if ypg >= 0.5:
                        color = RED
                        risk = " ALTA"
                    elif ypg >= 0.3:
                        color = ORANGE
                        risk = " !!"

                text = f"{pname} ({pos})"
                d.text((40, cy), text, fill=color, font=SMALL)
                stats_text = f"{yc}  {rc}  {ypg:.2f}/J{risk}"
                sw = d.textlength(stats_text, font=SMALL)
                d.text((W - 30 - sw, cy), stats_text, fill=color, font=SMALL)
                cy += 18
            cy += 8

    # ── Coaches ──
    if home_coach or away_coach:
        d.line([(20, cy), (W - 20, cy)], fill=DIVIDER, width=1)
        cy += 8
        d.text((20, cy), "ENTRENADORES", fill=WHITE, font=HEADER)
        cy += 22
        for team_name, coach_info in [(home, home_coach), (away, away_coach)]:
            if not coach_info:
                continue
            c_name, c_score, c_desc = coach_info
            # Color based on score
            if c_score >= 7:
                c_color = RED
            elif c_score >= 5:
                c_color = ORANGE
            else:
                c_color = GREEN
            bar_w = int(c_score * 12)
            d.rectangle([30, cy + 3, 30 + bar_w, cy + 13], fill=c_color)
            d.text((30 + bar_w + 6, cy), f"{c_score}/10", fill=c_color, font=SMALL)
            coach_text = f"{team_name}: {c_name} — {c_desc}"
            d.text((130, cy), coach_text, fill=GRAY, font=SMALL)
            cy += 20

    # ── Referee ──
    if referee:
        d.line([(20, cy), (W - 20, cy)], fill=DIVIDER, width=1)
        cy += 8
        ref_name = referee.get("name", "?")
        rank_str = f"  ({_ord(referee_rank)} de {referee_total})" if referee_rank and referee_total else ""
        d.text((20, cy), f"ARBITRO  {ref_name}{rank_str}", fill=WHITE, font=HEADER)
        cy += 22
        ref_yc = referee.get("total_yc", 0)
        ref_rc = referee.get("total_rc", 0)
        ref_games = referee.get("games", 0)
        ref_ypg = referee.get("yc_per_game", 0)
        d.text((30, cy), "Temporada:", fill=GRAY, font=BODY)
        sx = 120
        d.rectangle([sx, cy + 2, sx + 10, cy + 14], fill=YELLOW)
        d.text((sx + 14, cy), f"{ref_yc}", fill=GRAY, font=BODY)
        sx += 14 + d.textlength(str(ref_yc), font=BODY) + 8
        d.rectangle([sx, cy + 2, sx + 10, cy + 14], fill=RED)
        d.text((sx + 14, cy), f"{ref_rc}", fill=GRAY, font=BODY)
        sx += 14 + d.textlength(str(ref_rc), font=BODY) + 8
        d.text((sx, cy), f"en {ref_games}J — {ref_ypg:.1f}/J", fill=GRAY, font=BODY)
        cy += 20

        if referee_last:
            d.text((30, cy), f"Ultimos {len(referee_last)} partidos:", fill=WHITE, font=SMALL)
            cy += 16
            ref_ycs = [g.get("total_yc", 0) for g in referee_last]
            ref_rcs = [g.get("total_rc", 0) for g in referee_last]
            ref_delta = stdev(ref_ycs) if len(ref_ycs) > 1 else 0.0
            for g in referee_last:
                text = f"{g.get('home_team', '?')} vs {g.get('away_team', '?')}"
                d.text((40, cy), text, fill=GRAY, font=SMALL)
                rx = W - 30
                rc_val = str(g.get('total_rc', 0))
                rx -= d.textlength(rc_val, font=SMALL)
                d.text((rx, cy), rc_val, fill=GRAY, font=SMALL)
                rx -= 13
                d.rectangle([rx, cy + 2, rx + 10, cy + 12], fill=RED)
                rx -= 10
                yc_val = str(g.get('total_yc', 0))
                rx -= d.textlength(yc_val, font=SMALL)
                d.text((rx, cy), yc_val, fill=GRAY, font=SMALL)
                rx -= 13
                d.rectangle([rx, cy + 2, rx + 10, cy + 12], fill=YELLOW)
                cy += 18
            avg_last = sum(ref_ycs) / len(ref_ycs)
            avg_rc_last = sum(ref_rcs) / len(ref_rcs)
            d.text((30, cy), f"AVG ult.{len(referee_last)}:", fill=WHITE, font=BODY_B)
            ax = 30 + d.textlength(f"AVG ult.{len(referee_last)}: ", font=BODY_B)
            d.rectangle([ax, cy + 2, ax + 10, cy + 14], fill=YELLOW)
            d.text((ax + 14, cy), f"{avg_last:.1f}", fill=WHITE, font=BODY_B)
            ax += 14 + d.textlength(f"{avg_last:.1f}", font=BODY_B) + 8
            d.rectangle([ax, cy + 2, ax + 10, cy + 14], fill=RED)
            d.text((ax + 14, cy), f"{avg_rc_last:.1f}", fill=WHITE, font=BODY_B)
            con_color = GREEN if ref_delta <= 1.5 else RED
            delta_text = f"delta {ref_delta:.1f}"
            dw = d.textlength(delta_text, font=BODY_B)
            d.text((W - 30 - dw, cy), delta_text, fill=con_color, font=BODY_B)
            cy += 22
    elif not referee:
        d.line([(20, cy), (W - 20, cy)], fill=DIVIDER, width=1)
        cy += 8
        d.text((20, cy), "ARBITRO: sin datos", fill=GRAY, font=BODY)
        cy += 22

    # ── News context ──
    if news_context:
        d.line([(20, cy), (W - 20, cy)], fill=DIVIDER, width=1)
        cy += 8
        d.text((20, cy), "CONTEXTO", fill=WHITE, font=HEADER)
        cy += 22
        # Word-wrap the news text
        words = news_context.split()
        line = ""
        for word in words:
            test = f"{line} {word}".strip()
            if d.textlength(test, font=SMALL) > W - 60:
                d.text((30, cy), line, fill=GRAY, font=SMALL)
                cy += 16
                line = word
            else:
                line = test
        if line:
            d.text((30, cy), line, fill=GRAY, font=SMALL)
            cy += 16

    # ── BetFriend Pronostic ──
    if prediction:
        d.line([(20, cy), (W - 20, cy)], fill=DIVIDER, width=1)
        cy += 8
        d.text((20, cy), "BETFRIEND PRONOSTIC", fill=ACCENT, font=HEADER)
        cy += 25

        pred_total = prediction.get("predicted_total_yc", 0)
        pred_home = prediction.get("predicted_home_yc", 0)
        pred_away = prediction.get("predicted_away_yc", 0)
        rc_prob = prediction.get("rc_probability", "baja")
        confidence = prediction.get("confidence", "baja")

        # Total cards bar
        bar_x = 30
        bar_w = int(min(pred_total, 10) * 40)
        bar_color = GREEN if pred_total < 4 else ORANGE if pred_total < 6 else RED
        d.rectangle([bar_x, cy, bar_x + bar_w, cy + 18], fill=bar_color)
        d.rectangle([bar_x + bar_w + 8, cy + 3, bar_x + bar_w + 18, cy + 15], fill=YELLOW)
        d.text((bar_x + bar_w + 22, cy + 2), f"{pred_total} total", fill=WHITE, font=BODY_B)
        cy += 25

        # Per-team breakdown
        d.text((30, cy), f"{home}: >{pred_home}", fill=YELLOW, font=BODY)
        away_text = f"{away}: >{pred_away}"
        aw = d.textlength(away_text, font=BODY)
        d.text((W - 30 - aw, cy), away_text, fill=YELLOW, font=BODY)
        cy += 22

        # Red card probability
        rc_colors = {"muy alta": RED, "alta": ORANGE, "media": YELLOW, "baja": GREEN}
        rc_color = rc_colors.get(rc_prob, GRAY)
        d.text((30, cy), f"Roja: {rc_prob}", fill=rc_color, font=BODY_B)

        # Confidence
        conf_text = f"Confianza: {confidence}"
        cw = d.textlength(conf_text, font=SMALL)
        d.text((W - 30 - cw, cy), conf_text, fill=GRAY, font=SMALL)
        cy += 22

    # Crop to actual height
    img = img.crop((0, 0, W, cy + 15))

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


def _calc_team_height(stats, form, players) -> int:
    h = 25  # team name
    if stats:
        h += 40  # YC + RC lines
    if form:
        h += 60  # form section
    if players:
        h += min(len(players), 3) * 20 + 5
    return h


def _draw_team_block(
    d: ImageDraw.ImageDraw, y: int, W: int,
    name: str, stats: dict | None,
    yc_rank: int | None, rc_rank: int | None,
    form: list[dict] | None,
    players: list[dict] | None,
    player_ranks: dict,
) -> int:
    d.line([(20, y), (W - 20, y)], fill=DIVIDER, width=1)
    y += 5
    d.text((20, y), name, fill=WHITE, font=HEADER)
    y += 22

    if stats:
        # YC line
        yc_r = f" ({_ord(yc_rank)})" if yc_rank else ""
        d.rectangle([30, y + 2, 42, y + 14], fill=YELLOW)
        d.text((48, y), f"{stats.get('total_yc', 0)}{yc_r}  —  {stats.get('yc_per_game', 0):.1f}/J", fill=WHITE, font=BODY)
        y += 18

        # RC line
        rc_r = f" ({_ord(rc_rank)})" if rc_rank else ""
        d.rectangle([30, y + 2, 42, y + 14], fill=RED)
        d.text((48, y), f"{stats.get('total_rc', 0)}{rc_r}  —  {stats.get('rc_per_game', 0):.2f}/J", fill=WHITE, font=BODY)
        y += 22

    if form:
        ycs = [r.get("yc", 0) for r in form]
        rcs = [r.get("rc", 0) for r in form]
        avg_yc = sum(ycs) / len(ycs) if ycs else 0
        avg_rc = sum(rcs) / len(rcs) if rcs else 0
        delta = stdev(ycs) if len(ycs) > 1 else 0.0

        # Form boxes
        x = 30
        for r in form:
            yc_val = r.get("yc", 0)
            rc_val = r.get("rc", 0)
            box_w = 55
            d.rectangle([x, y, x + box_w, y + 20], fill=CARD_BG, outline=DIVIDER)
            d.text((x + 4, y + 3), f"{yc_val}Y", fill=YELLOW, font=SMALL)
            if rc_val > 0:
                d.text((x + 28, y + 3), f"{rc_val}R", fill=RED, font=SMALL)
            x += box_w + 4
        y += 25

        # AVG and delta
        # AVG with colored squares
        d.text((30, y), "AVG", fill=GRAY, font=SMALL)
        ax = 55
        d.rectangle([ax, y + 2, ax + 8, y + 12], fill=YELLOW)
        d.text((ax + 11, y), f"{avg_yc:.1f}", fill=GRAY, font=SMALL)
        ax += 11 + d.textlength(f"{avg_yc:.1f}", font=SMALL) + 6
        d.rectangle([ax, y + 2, ax + 8, y + 12], fill=RED)
        d.text((ax + 11, y), f"{avg_rc:.1f}", fill=GRAY, font=SMALL)
        if delta <= 1.0:
            d_color, d_label = GREEN, "consistente"
        elif delta <= 2.0:
            d_color, d_label = ORANGE, "moderado"
        else:
            d_color, d_label = RED, "variable"
        delta_text = f"δ {delta:.1f} {d_label}"
        dw = d.textlength(delta_text, font=SMALL)
        d.text((W - 30 - dw, y), delta_text, fill=d_color, font=SMALL)
        y += 18

    if players:
        for p in (players or [])[:3]:
            pname = p.get("name", "?")
            yc = p.get("total_yc", 0)
            rc = p.get("total_rc", 0)
            gp = p.get("games_played", 0)
            ypg = p.get("yc_per_game", 0)
            rank = player_ranks.get(pname)
            rank_str = f" — {_ord(rank)}" if rank else ""

            # YC/RC badges
            d.rectangle([30, y + 2, 38, y + 12], fill=YELLOW)
            d.text((41, y), str(yc), fill=WHITE, font=SMALL)
            d.rectangle([60, y + 2, 68, y + 12], fill=RED)
            d.text((71, y), str(rc), fill=WHITE, font=SMALL)

            player_text = f"{pname} ({gp}J, {ypg:.2f}/J){rank_str}"
            d.text((92, y), player_text, fill=GRAY, font=SMALL)
            y += 18
        y += 5

    return y
