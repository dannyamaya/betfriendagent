from __future__ import annotations

from loguru import logger

from betfriend.config.settings import settings
from betfriend.db.store import Store


class BudgetTracker:
    """Tracks API-Football requests against the daily limit."""

    def __init__(self, store: Store) -> None:
        self._store = store

    async def can_request(self, priority: str = "normal") -> bool:
        """Check if we can make a request.

        'critical' bypasses the reserve (for pre-game lineup calls).
        'normal' respects the reserve buffer.
        """
        used = await self._store.count_requests_today()
        limit = settings.api_daily_limit

        if priority == "critical":
            allowed = used < limit
        else:
            allowed = used < (limit - settings.api_reserve)

        if not allowed:
            logger.warning(
                f"API budget exhausted: {used}/{limit} used "
                f"(priority={priority}, reserve={settings.api_reserve})"
            )
        return allowed

    async def requests_remaining(self) -> int:
        used = await self._store.count_requests_today()
        return max(0, settings.api_daily_limit - used)
