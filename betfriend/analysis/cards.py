"""BetFriend Pronostic — weighted card prediction algorithm.

Combines multiple signals to predict total cards in a match.
Each signal has a weight and a confidence based on its delta (stdev).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from statistics import stdev


@dataclass
class CardPrediction:
    """Result of the card prediction algorithm."""
    predicted_total_yc: float
    predicted_home_yc: float
    predicted_away_yc: float
    predicted_total_rc: float
    rc_probability: str  # "muy alta", "alta", "media", "baja"
    confidence: str  # "alta", "media", "baja"
    signals: list[dict] = field(default_factory=list)
    summary: str = ""


def predict_cards(
    home_stats: dict | None = None,
    away_stats: dict | None = None,
    home_form: list[dict] | None = None,
    away_form: list[dict] | None = None,
    referee: dict | None = None,
    referee_last: list[dict] | None = None,
    h2h: list[dict] | None = None,
    home_coach_score: int | None = None,
    away_coach_score: int | None = None,
    home_lineup: list[dict] | None = None,
    away_lineup: list[dict] | None = None,
    news_context: str | None = None,
) -> CardPrediction:
    """Run the weighted prediction algorithm."""
    signals: list[dict] = []
    total_weight = 0.0
    weighted_yc = 0.0

    # ── Signal 1: Team season avg (25%) ──
    if home_stats and away_stats:
        h_ypg = home_stats.get("yc_per_game", 0)
        a_ypg = away_stats.get("yc_per_game", 0)
        combined = h_ypg + a_ypg
        w = 0.25
        signals.append({"name": "Team season avg", "value": combined, "weight": w})
        weighted_yc += combined * w
        total_weight += w

    # ── Signal 2: Team last-5 form (20%) ──
    if home_form and away_form:
        h_ycs = [r.get("yc", 0) for r in home_form]
        a_ycs = [r.get("yc", 0) for r in away_form]
        h_avg = sum(h_ycs) / len(h_ycs) if h_ycs else 0
        a_avg = sum(a_ycs) / len(a_ycs) if a_ycs else 0
        combined = h_avg + a_avg

        # Higher weight if both teams are consistent (low delta)
        h_delta = stdev(h_ycs) if len(h_ycs) > 1 else 0
        a_delta = stdev(a_ycs) if len(a_ycs) > 1 else 0
        avg_delta = (h_delta + a_delta) / 2
        w = 0.20 * (1.2 if avg_delta < 1.0 else 0.8 if avg_delta > 2.0 else 1.0)

        signals.append({"name": "Last-5 form", "value": combined, "weight": w, "delta": avg_delta})
        weighted_yc += combined * w
        total_weight += w

    # ── Signal 3: Referee avg (20%) ──
    if referee and referee.get("games", 0) >= 3:
        ref_ypg = referee.get("yc_per_game", 0)
        ref_games = referee.get("games", 0)

        # Referee delta — weight higher if consistent
        if referee_last:
            ref_ycs = [g.get("total_yc", 0) for g in referee_last]
            ref_delta = stdev(ref_ycs) if len(ref_ycs) > 1 else 0
        else:
            ref_delta = 0

        w = 0.20 * (1.2 if ref_delta < 1.5 else 0.8 if ref_delta > 3.0 else 1.0)
        signals.append({"name": "Referee avg", "value": ref_ypg, "weight": w, "delta": ref_delta})
        weighted_yc += ref_ypg * w
        total_weight += w

    # ── Signal 4: H2H avg (10%) ──
    if h2h:
        h2h_ycs = [h.get("team_a_yc", 0) + h.get("team_b_yc", 0) for h in h2h]
        h2h_avg = sum(h2h_ycs) / len(h2h_ycs)
        w = 0.10
        signals.append({"name": "H2H avg", "value": h2h_avg, "weight": w})
        weighted_yc += h2h_avg * w
        total_weight += w

    # ── Signal 5: Coach aggressiveness (10%) ──
    if home_coach_score is not None and away_coach_score is not None:
        # Convert 0-10 scores to expected card contribution
        # Higher coach score = more cards for their team
        coach_factor = (home_coach_score + away_coach_score) / 10.0  # 0-2 range
        # Use as multiplier on average — a combined 14/20 coach score ≈ 1.4x
        w = 0.10
        base = weighted_yc / total_weight if total_weight > 0 else 4.0
        coach_adjusted = base * coach_factor
        signals.append({"name": "Coach factor", "value": coach_adjusted, "weight": w,
                        "home_score": home_coach_score, "away_score": away_coach_score})
        weighted_yc += coach_adjusted * w
        total_weight += w

    # ── Signal 6: Lineup composition (10%) ──
    if home_lineup and away_lineup:
        h_starters = [p for p in home_lineup if p.get("is_starter")]
        a_starters = [p for p in away_lineup if p.get("is_starter")]

        h_sum = sum(p.get("yc_per_game", 0) for p in h_starters)
        a_sum = sum(p.get("yc_per_game", 0) for p in a_starters)
        lineup_total = h_sum + a_sum

        w = 0.10
        signals.append({"name": "Lineup card rates", "value": lineup_total, "weight": w})
        weighted_yc += lineup_total * w
        total_weight += w

    # ── Signal 7: News bias (5%) ──
    if news_context:
        # Simple keyword boost
        hot_words = ["caliente", "derbi", "rivalry", "heated", "tension", "revenge",
                     "revancha", "clasico", "pelea", "europeo", "descenso", "relegation"]
        heat = sum(1 for word in hot_words if word in news_context.lower())
        news_boost = min(heat * 0.5, 2.0)  # max +2 cards
        w = 0.05
        if news_boost > 0:
            base = weighted_yc / total_weight if total_weight > 0 else 4.0
            news_value = base + news_boost
            signals.append({"name": "News heat", "value": news_value, "weight": w, "heat": heat})
            weighted_yc += news_value * w
            total_weight += w

    # ── Compute final prediction ──
    if total_weight > 0:
        predicted_total = weighted_yc / total_weight
    else:
        predicted_total = 4.0  # league average fallback

    # Split home/away based on team averages
    if home_stats and away_stats:
        h_ratio = home_stats.get("yc_per_game", 2) / max(
            home_stats.get("yc_per_game", 2) + away_stats.get("yc_per_game", 2), 0.1
        )
    else:
        h_ratio = 0.5

    predicted_home = predicted_total * h_ratio
    predicted_away = predicted_total * (1 - h_ratio)

    # ── Red card probability ──
    rc_signals = []
    if home_stats:
        rc_signals.append(home_stats.get("rc_per_game", 0))
    if away_stats:
        rc_signals.append(away_stats.get("rc_per_game", 0))
    if referee and referee.get("games", 0) >= 3:
        rc_signals.append(referee.get("rc_per_game", 0))

    avg_rc_rate = sum(rc_signals) / len(rc_signals) if rc_signals else 0
    # Rough probability: sum of per-game rates
    rc_prob = sum(rc_signals)
    if rc_prob >= 0.5:
        rc_label = "muy alta"
    elif rc_prob >= 0.3:
        rc_label = "alta"
    elif rc_prob >= 0.15:
        rc_label = "media"
    else:
        rc_label = "baja"

    predicted_rc = rc_prob

    # ── Confidence ──
    if total_weight >= 0.8:
        confidence = "alta"
    elif total_weight >= 0.5:
        confidence = "media"
    else:
        confidence = "baja"

    # ── Summary ──
    summary = (
        f"> {predicted_total:.1f} tarjetas en el partido, "
        f">{predicted_home:.1f} {home_stats.get('team_name', 'local') if home_stats else 'local'} "
        f">{predicted_away:.1f} {away_stats.get('team_name', 'visitante') if away_stats else 'visitante'}, "
        f"Tarjeta roja probabilidad {rc_label}"
    )

    return CardPrediction(
        predicted_total_yc=round(predicted_total, 1),
        predicted_home_yc=round(predicted_home, 1),
        predicted_away_yc=round(predicted_away, 1),
        predicted_total_rc=round(predicted_rc, 2),
        rc_probability=rc_label,
        confidence=confidence,
        signals=signals,
        summary=summary,
    )
