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
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            })
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "lxml")
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    text = (link.get_text() + " " + href).lower()

                    if not href.endswith(".pdf") and "/files/" not in href:
                        continue
                    if not any(kw in text for kw in keywords):
                        continue
                    if matchday:
                        jornada_match = re.search(r'jornada[_\s-]*(\d+)', text)
                        if jornada_match and int(jornada_match.group(1)) != matchday:
                            continue

                    if href.startswith("/"):
                        href = f"https://rfef.es{href}"
                    elif not href.startswith("http"):
                        href = f"https://rfef.es/{href}"
                    urls.append(href)
                    logger.info(f"  Found RFEF PDF from page: {href}")
            else:
                logger.warning(f"RFEF page returned {resp.status_code}, using direct URL fallback")

    except Exception as e:
        logger.warning(f"Failed to scrape RFEF designations page: {e}")

    # Always try direct URL patterns (these are predictable)
    if matchday:
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
    logger.info(f"  RFEF: {len(urls)} URLs to try for league {league_id}, matchday {matchday}")
    if not urls:
        return {}

    designations: dict[str, str] = {}

    _headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "*/*",
    }
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as http:
        for url in urls:
            try:
                resp = await http.get(url, headers=_headers)
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
    """Parse referee designations from PDF content.

    RFEF PDF format:
    - Each match has a table row: [date, home_team, away_team, time]
    - Below each table: "Árbitro:Name 4º Árbitro:..."
    - We use text extraction to pair matches with their referees.
    """
    designations: dict[str, str] = {}

    try:
        with pdfplumber.open(BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                # Also get tables to know the match teams
                tables = page.extract_tables()
                matches_in_order = []
                for table in tables:
                    if table and len(table) >= 1:
                        row = table[0]
                        if len(row) >= 3:
                            home = str(row[1] or "").strip()
                            away = str(row[2] or "").strip()
                            if home and away:
                                matches_in_order.append((home, away))

                # Parse text to find "Árbitro:" lines
                lines = text.split("\n")
                referees_in_order = []
                for line in lines:
                    if "rbitro:" in line and "4" not in line.split("rbitro:")[0][-2:]:
                        # Line like "Árbitro:Juan Martínez 4º Árbitro:..."
                        match = re.search(r'[ÁáA]rbitro:\s*(.+?)(?:\s+4[ºo°]|\s+$)', line)
                        if match:
                            ref_name = match.group(1).strip()
                            if ref_name and len(ref_name) > 2:
                                referees_in_order.append(ref_name)

                # Match them up by order
                for i, (home, away) in enumerate(matches_in_order):
                    if i < len(referees_in_order):
                        key = _normalize_match_key(f"{home} - {away}")
                        designations[key] = referees_in_order[i]
                        logger.info(f"    {home} vs {away} -> {referees_in_order[i]} (key={key})")

    except Exception as e:
        logger.error(f"  Failed to parse RFEF PDF: {e}")

    return designations


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
                "balompie", "futbol"}
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
