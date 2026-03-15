"""Coach aggressiveness ratings for La Liga and La Liga 2 (2025-26 season).

Static ranking that estimates how likely a coach's team is to receive cards.
Scale: 0 (very disciplined) to 10 (extremely aggressive / card-prone).
"""
from __future__ import annotations

# Coach name -> aggressiveness score (0-10)
COACH_RATINGS: dict[str, int] = {
    # La Liga 2025-26
    "Diego Simeone": 9,
    "Gennaro Gattuso": 8,
    "Ernesto Valverde": 5,
    "Carlo Ancelotti": 4,
    "Hansi Flick": 5,
    "Imanol Alguacil": 7,
    "Quique Setien": 4,
    "Marcelino Garcia Toral": 7,
    "Diego Martinez": 6,
    "Manuel Pellegrini": 5,
    "Jose Luis Mendilibar": 8,
    "Eduardo Coudet": 7,
    "Luis Garcia Plaza": 6,
    "Jagoba Arrasate": 6,
    "Eusebio Sacristan": 5,
    "Mauricio Pochettino": 6,
    "Michel": 6,
    "Pepe Bordalas": 9,
    "Pacheta": 7,
    "Andoni Iraola": 6,
    "Sergio Gonzalez": 5,
    "Alvaro Cervera": 8,
    "Luis Miguel Ramis": 6,
    "Francisco Rodriguez": 6,
    # La Liga 2
    "Jose Ramon Sandoval": 7,
    "Paco Jemez": 7,
    "Asier Garitano": 7,
    "Abelardo Fernandez": 7,
    "Gaizka Garitano": 6,
    "Quique Flores": 5,
    "Diego Alonso": 6,
    "Oltra": 6,
}


_DESCRIPTIONS = {
    range(0, 3): "Muy disciplinado, pocas tarjetas",
    range(3, 5): "Moderado, equipo controlado",
    range(5, 7): "Normal, nivel medio de tarjetas",
    range(7, 9): "Agresivo, equipos con muchas tarjetas",
    range(9, 11): "Muy agresivo, equipos tarjeteros",
}


def get_coach_aggressiveness(coach_name: str) -> tuple[int, str]:
    """Return (score, description) for a coach.

    Performs a fuzzy match on last name if exact match fails.
    Returns (5, "Sin datos del entrenador") if unknown.
    """
    # Exact match
    if coach_name in COACH_RATINGS:
        score = COACH_RATINGS[coach_name]
        return score, _desc(score)

    # Try matching by last name
    query_lower = coach_name.lower()
    for name, score in COACH_RATINGS.items():
        # Match on last token of stored name
        last = name.split()[-1].lower()
        if last in query_lower or query_lower in name.lower():
            return score, _desc(score)

    return 5, "Sin datos del entrenador"


def _desc(score: int) -> str:
    for rng, desc in _DESCRIPTIONS.items():
        if score in rng:
            return desc
    return "Sin clasificacion"
