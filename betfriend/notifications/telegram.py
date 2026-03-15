from __future__ import annotations

from io import BytesIO

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

    async def send_photo(self, photo: BytesIO, caption: str | None = None) -> None:
        try:
            await self._bot.send_photo(
                chat_id=self._chat_id,
                photo=photo,
                caption=caption,
                parse_mode=ParseMode.HTML if caption else None,
            )
            logger.info("Telegram photo sent")
        except Exception as e:
            logger.error(f"Failed to send Telegram photo: {e}")
            raise
