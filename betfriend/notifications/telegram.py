from __future__ import annotations

from loguru import logger
from telegram import Bot
from telegram.constants import ParseMode

from betfriend.config.settings import settings


class TelegramNotifier:
    """Sends messages to a Telegram chat."""

    def __init__(self) -> None:
        self._bot = Bot(token=settings.telegram_token)
        self._chat_id = settings.telegram_chat_id

    async def send(self, text: str) -> None:
        """Send a message using MarkdownV2 parse mode."""
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            logger.info(f"Telegram message sent ({len(text)} chars)")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            raise
