"""RFEF referee designation PDF scraper.

Downloads and parses the official RFEF PDF that lists referee assignments
for each matchday. This is free (no API cost) and more reliable than
API-Football for referee data.

URL pattern:
https://rfef.es/sites/default/files/designaciones_{division}_-_temp_{season}_-_jornada_{N}_{day}.pdf
"""
from __future__ import annotations

import re
from io import BytesIO

import httpx
import pdfplumber
from loguru import logger


DIVISION_MAP = {
    140: "1a_division_masculina",  # La Liga
    141: "2a_division_masculina",  # La Liga 2
}

DAYS = ["viernes", "sabado", "domingo", "lunes"]

SEASON_STR = "2025-26"


def _build_urls(league_id: int, matchday: int) -> list[str]:
    """Build possible PDF URLs for a given matchday."""
    division = DIVISION_MAP.get(league_id)
    if not division:
        return []
    return [
        f"https://rfef.es/sites/default/files/designaciones_{division}"
        f"_-_temp_{SEASON_STR}_-_jornada_{matchday}_{day}.pdf"
        for day in DAYS
    ]


async def fetch_referee_designations(
    league_id: int, matchday: int
) -> dict[str, str]:
    """Try to download and parse RFEF PDF for referee designations.

    Returns a dict mapping a simplified match key to referee name.
    Example: {"betis-celta": "González Fuertes"}
    """
    urls = _build_urls(league_id, matchday)
    if not urls:
        return {}

    designations: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as http:
        for url in urls:
            try:
                resp = await http.get(url)
                if resp.status_code != 200:
                    continue

                logger.info(f"RFEF PDF downloaded: {url}")
                parsed = _parse_pdf(resp.content)
                designations.update(parsed)

            except Exception as e:
                logger.warning(f"Failed to fetch RFEF PDF {url}: {e}")
                continue

    return designations


def _parse_pdf(content: bytes) -> dict[str, str]:
    """Parse referee designations from PDF content.

    The PDF typically has a table structure with columns like:
    PARTIDO | ARBITRO | ... other officials
    """
    designations: dict[str, str] = {}

    try:
        with pdfplumber.open(BytesIO(content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    designations.update(_parse_table(table))

                # Fallback: try text extraction if no tables found
                if not tables:
                    text = page.extract_text()
                    if text:
                        designations.update(_parse_text(text))
    except Exception as e:
        logger.error(f"Failed to parse RFEF PDF: {e}")

    return designations


def _parse_table(table: list[list[str | None]]) -> dict[str, str]:
    """Extract match->referee mappings from a PDF table."""
    result: dict[str, str] = {}

    if not table or len(table) < 2:
        return result

    # Find column indices for match and referee
    header = [str(cell).lower().strip() if cell else "" for cell in table[0]]

    match_col = None
    ref_col = None
    for i, h in enumerate(header):
        if "partido" in h or "match" in h or "encuentro" in h:
            match_col = i
        if "árbitro" in h or "arbitro" in h or "referee" in h:
            ref_col = i

    if match_col is None or ref_col is None:
        # Try first two columns as fallback
        if len(header) >= 2:
            match_col = 0
            ref_col = 1
        else:
            return result

    for row in table[1:]:
        if not row or len(row) <= max(match_col, ref_col):
            continue
        match_str = str(row[match_col] or "").strip()
        ref_str = str(row[ref_col] or "").strip()

        if match_str and ref_str and len(ref_str) > 2:
            key = _normalize_match_key(match_str)
            if key:
                result[key] = ref_str

    return result


def _parse_text(text: str) -> dict[str, str]:
    """Fallback: try to extract referee from plain text."""
    result: dict[str, str] = {}
    # Look for patterns like "Team A - Team B" followed by referee info
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if " - " in line and i + 1 < len(lines):
            match_key = _normalize_match_key(line)
            # Next line might have the referee
            next_line = lines[i + 1].strip()
            if next_line and not " - " in next_line:
                result[match_key] = next_line
    return result


def _normalize_match_key(match_str: str) -> str:
    """Normalize a match string into a comparable key.

    'Real Betis Balompié - RC Celta de Vigo' -> 'betis-celta'
    """
    # Remove common prefixes/suffixes
    s = match_str.lower()
    s = re.sub(r'[^a-záéíóúñü\s-]', '', s)

    # Split by " - " or " vs "
    parts = re.split(r'\s*[-–vs]+\s*', s)
    if len(parts) < 2:
        return s.strip()

    # Take the most distinctive word from each team name
    def key_word(name: str) -> str:
        words = name.split()
        skip = {"real", "club", "deportivo", "cf", "rc", "sd", "ud", "cd",
                "atletico", "sporting", "racing", "fc", "de", "la", "del"}
        for w in words:
            if w not in skip and len(w) > 2:
                return w
        return words[-1] if words else ""

    return f"{key_word(parts[0])}-{key_word(parts[1])}"


def match_referee_to_fixture(
    designations: dict[str, str],
    home_team: str,
    away_team: str,
) -> str | None:
    """Try to find the referee for a specific fixture in the designations."""
    fixture_key = _normalize_match_key(f"{home_team} - {away_team}")

    # Direct match
    if fixture_key in designations:
        return designations[fixture_key]

    # Fuzzy: check if any key contains both team key words
    home_word = fixture_key.split("-")[0] if "-" in fixture_key else ""
    away_word = fixture_key.split("-")[1] if "-" in fixture_key else ""

    for key, ref in designations.items():
        if home_word and away_word and home_word in key and away_word in key:
            return ref

    return None
