"""News bias scraper — searches for card-relevant betting analysis articles.

Searches Google for match analysis, extracts card-related context from
top results using httpx + BeautifulSoup.
"""
from __future__ import annotations

import re

import httpx
from bs4 import BeautifulSoup
from loguru import logger

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
}

# Keywords that signal card-relevant content
_CARD_KEYWORDS = [
    "tarjeta", "tarjetas", "amarilla", "roja", "expuls",
    "sancion", "amonesta", "falta", "agresiv", "rival",
    "derbi", "clasico", "tension", "caliente", "polemic",
    "europeo", "descenso", "pelea", "lider", "arbitro",
    "suspension", "acumulacion", "apercib",
]

_MAX_RESULTS = 3
_MAX_TEXT_PER_PAGE = 3000  # chars to extract per page
_TIMEOUT = 10.0


async def get_match_news_context(home_team: str, away_team: str) -> str:
    """Search for card-relevant news about an upcoming match.

    Returns a brief summary string with card-focused context.
    Returns empty string if search fails or nothing relevant found.
    """
    try:
        urls = await _search_google(home_team, away_team)
        if not urls:
            return ""

        snippets: list[str] = []
        for url in urls[:_MAX_RESULTS]:
            text = await _fetch_page_text(url)
            if text:
                relevant = _extract_card_relevant(text)
                if relevant:
                    snippets.append(relevant)

        if not snippets:
            return ""

        # Combine and truncate
        combined = " | ".join(snippets)
        if len(combined) > 500:
            combined = combined[:497] + "..."
        return combined

    except Exception as e:
        logger.warning(f"News scraper failed for {home_team} vs {away_team}: {e}")
        return ""


async def _search_google(home_team: str, away_team: str) -> list[str]:
    """Search for match analysis URLs. Tries DuckDuckGo first (no CAPTCHA), then Google."""
    query = f"{home_team} vs {away_team} analisis apuestas tarjetas"

    # Try DuckDuckGo HTML (no CAPTCHA issues from CI)
    urls = await _search_ddg(query)
    if urls:
        logger.info(f"  News: DuckDuckGo returned {len(urls)} URLs")
        return urls

    # Fallback to Google
    search_url = "https://www.google.com/search"
    params = {"q": query, "hl": "es", "num": "5"}
    try:
        async with httpx.AsyncClient(
            headers=_HEADERS, follow_redirects=True, timeout=_TIMEOUT
        ) as client:
            resp = await client.get(search_url, params=params)
            resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Google search failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    urls = []
    for a_tag in soup.select("a[href]"):
        href = a_tag.get("href", "")
        if not isinstance(href, str):
            continue
        if href.startswith("/url?q="):
            real_url = href.split("/url?q=")[1].split("&")[0]
            if real_url.startswith("http") and "google." not in real_url:
                urls.append(real_url)
        elif href.startswith("http") and "google." not in href:
            urls.append(href)
        if len(urls) >= _MAX_RESULTS:
            break
    logger.info(f"  News: Google returned {len(urls)} URLs")
    return urls


async def _search_ddg(query: str) -> list[str]:
    """Search DuckDuckGo HTML for result URLs."""
    search_url = "https://html.duckduckgo.com/html/"
    try:
        async with httpx.AsyncClient(
            headers=_HEADERS, follow_redirects=True, timeout=_TIMEOUT
        ) as client:
            resp = await client.post(search_url, data={"q": query})
            resp.raise_for_status()
    except Exception as e:
        logger.debug(f"DuckDuckGo search failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    urls: list[str] = []
    for a_tag in soup.select("a.result__a"):
        href = a_tag.get("href", "")
        if isinstance(href, str) and href.startswith("http"):
            urls.append(href)
            if len(urls) >= _MAX_RESULTS:
                break
    return urls


async def _fetch_page_text(url: str) -> str:
    """Fetch a URL and extract visible text content."""
    try:
        async with httpx.AsyncClient(
            headers=_HEADERS, follow_redirects=True, timeout=_TIMEOUT
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    # Remove scripts, styles, nav, footer
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()

    text = soup.get_text(separator=" ", strip=True)

    # Clean up whitespace
    text = re.sub(r"\s+", " ", text)

    return text[:_MAX_TEXT_PER_PAGE]


def _extract_card_relevant(text: str) -> str:
    """Extract sentences that mention card-relevant keywords."""
    # Split into sentences
    sentences = re.split(r"[.!?]+", text)
    relevant: list[str] = []

    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 15 or len(sentence) > 300:
            continue
        lower = sentence.lower()
        if any(kw in lower for kw in _CARD_KEYWORDS):
            relevant.append(sentence)
            if len(relevant) >= 3:
                break

    return ". ".join(relevant) if relevant else ""
