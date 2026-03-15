"""RFEF referee designation PDF scraper.

Downloads and parses the official RFEF PDF that lists referee assignments.
Source: https://rfef.es/es/noticias/arbitros/designaciones

The page lists links to PDF files with referee designations for each matchday.
"""
from __future__ import annotations

import re
from io import BytesIO

import httpx
import pdfplumber
from bs4 import BeautifulSoup
from loguru import logger


RFEF_DESIGNATIONS_URL = "https://rfef.es/es/noticias/arbitros/designaciones"

DIVISION_KEYWORDS = {
    140: ["primera", "1a division", "laliga", "la liga ea sports", "primera division"],
    141: ["segunda", "2a division", "laliga hypermotion", "segunda division"],
}

SEASON_STR = "2025-26"


async def _discover_pdf_urls(league_id: int, matchday: int | None = None) -> list[str]:
    """Scrape the RFEF designations page to find relevant PDF URLs."""
    urls: list[str] = []
    keywords = DIVISION_KEYWORDS.get(league_id, [])
    if not keywords:
        return urls

    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as http:
            resp = await http.get(RFEF_DESIGNATIONS_URL, headers={
                "User-Agent": "Mozilla/5.0 (compatible; BetFriend/1.0)"
            })
            if resp.status_code != 200:
                logger.warning(f"RFEF page returned {resp.status_code}")
                return urls

            soup = BeautifulSoup(resp.text, "lxml")

            # Find all links that point to PDF files
            for link in soup.find_all("a", href=True):
                href = link["href"]
                text = (link.get_text() + " " + href).lower()

                if not href.endswith(".pdf") and "/files/" not in href:
                    continue

                # Check if it matches our division
                if not any(kw in text for kw in keywords):
                    continue

                # Check matchday if specified
                if matchday:
                    jornada_match = re.search(r'jornada[_\s-]*(\d+)', text)
                    if jornada_match:
                        found_md = int(jornada_match.group(1))
                        if found_md != matchday:
                            continue

                # Build full URL
                if href.startswith("/"):
                    href = f"https://rfef.es{href}"
                elif not href.startswith("http"):
                    href = f"https://rfef.es/{href}"

                urls.append(href)
                logger.info(f"  Found RFEF PDF: {href}")

    except Exception as e:
        logger.warning(f"Failed to scrape RFEF designations page: {e}")

    # Also try direct URL patterns as fallback
    if not urls and matchday:
        division_map = {
            140: "1a_division_masculina",
            141: "2a_division_masculina",
        }
        division = division_map.get(league_id)
        if division:
            for day in ["viernes", "sabado", "domingo", "lunes"]:
                urls.append(
                    f"https://rfef.es/sites/default/files/designaciones_{division}"
                    f"_-_temp_{SEASON_STR}_-_jornada_{matchday}_{day}.pdf"
                )

    return urls


async def fetch_referee_designations(
    league_id: int, matchday: int | None = None
) -> dict[str, str]:
    """Download and parse RFEF PDFs for referee designations.

    Returns a dict mapping normalized match key to referee name.
    """
    urls = await _discover_pdf_urls(league_id, matchday)
    if not urls:
        logger.info(f"  No RFEF PDF URLs found for league {league_id}, matchday {matchday}")
        return {}

    designations: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as http:
        for url in urls:
            try:
                resp = await http.get(url, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; BetFriend/1.0)"
                })
                if resp.status_code != 200:
                    continue

                logger.info(f"  RFEF PDF downloaded: {url}")
                parsed = _parse_pdf(resp.content)
                designations.update(parsed)

            except Exception as e:
                logger.warning(f"  Failed to fetch RFEF PDF {url}: {e}")
                continue

    return designations


def _parse_pdf(content: bytes) -> dict[str, str]:
    """Parse referee designations from PDF content."""
    designations: dict[str, str] = {}

    try:
        with pdfplumber.open(BytesIO(content)) as pdf:
            for page in pdf.pages:
                # Try table extraction first
                tables = page.extract_tables()
                for table in tables:
                    designations.update(_parse_table(table))

                # Fallback: text extraction
                if not designations:
                    text = page.extract_text()
                    if text:
                        designations.update(_parse_text(text))
    except Exception as e:
        logger.error(f"  Failed to parse RFEF PDF: {e}")

    return designations


def _parse_table(table: list[list[str | None]]) -> dict[str, str]:
    """Extract match->referee mappings from a PDF table."""
    result: dict[str, str] = {}

    if not table or len(table) < 2:
        return result

    header = [str(cell).lower().strip() if cell else "" for cell in table[0]]

    match_col = None
    ref_col = None
    for i, h in enumerate(header):
        if any(kw in h for kw in ["partido", "match", "encuentro", "enfrentamiento"]):
            match_col = i
        if any(kw in h for kw in ["árbitro", "arbitro", "referee", "principal"]):
            ref_col = i

    if match_col is None or ref_col is None:
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
                logger.debug(f"    PDF table: '{match_str}' -> '{ref_str}' (key={key})")

    return result


def _parse_text(text: str) -> dict[str, str]:
    """Fallback: try to extract referee from plain text."""
    result: dict[str, str] = {}
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if " - " in line and i + 1 < len(lines):
            match_key = _normalize_match_key(line)
            next_line = lines[i + 1].strip()
            if next_line and " - " not in next_line:
                result[match_key] = next_line
    return result


def _normalize_match_key(match_str: str) -> str:
    """Normalize a match string into a comparable key."""
    s = match_str.lower()
    s = re.sub(r'[^a-záéíóúñü\s-]', '', s)

    parts = re.split(r'\s*[-–vs]+\s*', s)
    if len(parts) < 2:
        return s.strip()

    def key_word(name: str) -> str:
        words = name.split()
        skip = {"real", "club", "deportivo", "cf", "rc", "sd", "ud", "cd",
                "atletico", "sporting", "racing", "fc", "de", "la", "del",
                "balompie", "futbol", "sociedad"}
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
    """Try to find the referee for a specific fixture."""
    fixture_key = _normalize_match_key(f"{home_team} - {away_team}")
    logger.info(f"    Matching fixture key '{fixture_key}' against {list(designations.keys())}")

    # Direct match
    if fixture_key in designations:
        return designations[fixture_key]

    # Fuzzy: check if key words appear in any designation key
    home_word = fixture_key.split("-")[0] if "-" in fixture_key else ""
    away_word = fixture_key.split("-")[1] if "-" in fixture_key else ""

    for key, ref in designations.items():
        if home_word and away_word and home_word in key and away_word in key:
            return ref

    # Even more fuzzy: partial match on first 3+ chars
    for key, ref in designations.items():
        key_parts = key.split("-")
        if len(key_parts) >= 2:
            if (home_word[:3] in key_parts[0] or key_parts[0][:3] in home_word) and \
               (away_word[:3] in key_parts[1] or key_parts[1][:3] in away_word):
                return ref

    return None
