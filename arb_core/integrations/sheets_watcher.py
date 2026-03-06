"""
Google Sheets watcher for arb_core.

Polls sheet periodically and syncs pairs to store.
Sends Telegram notifications for new/re-enabled pairs.
"""

import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Set

from ..core.config import Config
from ..core.logging import get_logger
from ..core.models import PairStatus
from ..core.store import PairStore

from .sheets import ParsedRow, SheetsClient, SheetsError, SheetsSyncResult

if TYPE_CHECKING:
    from ..ui.telegram_bot import TelegramBot

logger = get_logger(__name__)


class SheetsWatcher:
    """
    Watches Google Sheets for changes and syncs to PairStore.

    Features:
    - Polls at configured interval
    - Does not downgrade user progress (PM_SELECTED/READY/ACTIVE)
    - Sends Telegram notifications for new pairs
    - Errors in watcher do not crash Telegram
    """

    # Statuses that should not be downgraded
    PROTECTED_STATUSES = {
        PairStatus.PM_SELECTED,
        PairStatus.READY,
        PairStatus.ACTIVE,
    }

    def __init__(
        self,
        config: Config,
        store: PairStore,
        telegram_bot: Optional["TelegramBot"] = None,
    ):
        self.config = config
        self.store = store
        self.telegram_bot = telegram_bot
        self.sheets_client = SheetsClient(config.sheets)

        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Track notified pairs to avoid duplicate notifications
        self._notified_pairs: Set[str] = set()
        self._last_sync_time: Optional[datetime] = None

    def start(self) -> bool:
        """Start the watcher thread."""
        if not self.config.sheets.enabled:
            logger.info("Sheets watcher disabled in config")
            return False

        if self._running:
            logger.warning("Sheets watcher already running")
            return False

        # Validate sheets config
        errors = self.config.sheets.validate()
        if errors:
            for error in errors:
                logger.error(f"Sheets config error: {error}")
            return False

        self._running = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

        logger.info("Sheets watcher started")
        print("Sheets watcher started")
        return True

    def stop(self) -> None:
        """Stop the watcher thread."""
        logger.info("Stopping sheets watcher")
        self._running = False

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

        logger.info("Sheets watcher stopped")

    def _watch_loop(self) -> None:
        """Main watch loop."""
        poll_interval = self.config.sheets.poll_interval_sec
        consecutive_errors = 0
        max_consecutive_errors = 10

        # Initial sync
        self._do_sync()

        while self._running:
            try:
                time.sleep(poll_interval)
                if not self._running:
                    break

                self._do_sync()
                consecutive_errors = 0

            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    f"Sheets watcher error ({consecutive_errors}/{max_consecutive_errors}): {e}"
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.critical(
                        "Too many consecutive sheets errors, continuing with backoff"
                    )
                    # Don't stop, just slow down
                    time.sleep(min(consecutive_errors * 10, 300))

    def _do_sync(self) -> None:
        """Perform a single sync from sheets to store."""
        try:
            result = self.sheets_client.fetch_and_parse()
            self._process_sync_result(result)
            self._last_sync_time = datetime.utcnow()

            logger.info(
                f"Sheets sync: ok={result.ok_count}, errors={result.error_count}, "
                f"disabled={result.disabled_count}"
            )

        except SheetsError as e:
            logger.error(f"Sheets fetch error: {e}")
        except Exception as e:
            logger.error(f"Unexpected sheets sync error: {e}")

    def _process_sync_result(self, result: SheetsSyncResult) -> None:
        """Process sync result and update store."""
        new_pairs = []
        reenabled_pairs = []

        for parsed_row in result.parsed_rows:
            try:
                action = self._process_row(parsed_row)
                if action == "new":
                    new_pairs.append(parsed_row)
                elif action == "reenabled":
                    reenabled_pairs.append(parsed_row)
            except Exception as e:
                logger.error(f"Error processing row {parsed_row.row_index}: {e}")

        # Send notifications for new pairs
        for parsed_row in new_pairs:
            self._notify_new_pair(parsed_row)

        # Send notifications for re-enabled pairs
        for parsed_row in reenabled_pairs:
            self._notify_reenabled_pair(parsed_row)

    def _process_row(self, parsed_row: ParsedRow) -> Optional[str]:
        """
        Process a single parsed row.

        Returns:
            "new" if new pair created
            "reenabled" if pair was re-enabled
            None otherwise
        """
        # Handle ERROR status
        if parsed_row.status == PairStatus.ERROR:
            # Try to find existing pair by URLs to mark as error
            # For error rows, pair_id might be empty, try to compute it
            if parsed_row.polymarket_url and parsed_row.opinion_url:
                from ..core.models import compute_pair_id

                try:
                    pair_id = compute_pair_id(
                        parsed_row.polymarket_url, parsed_row.opinion_url
                    )
                    existing = self.store.get_pair(pair_id)
                    if existing:
                        self.store.mark_error(pair_id, parsed_row.error_message or "Invalid row")
                except Exception:
                    pass
            return None

        pair_id = parsed_row.pair_id
        existing = self.store.get_pair(pair_id)

        # Handle DISABLED status
        if parsed_row.status == PairStatus.DISABLED:
            if existing and existing.status != PairStatus.DISABLED:
                self.store.mark_disabled(pair_id)
            elif not existing:
                # Create as disabled
                self.store.upsert_pair(
                    pair_id=pair_id,
                    pm_url=parsed_row.polymarket_url,
                    op_url=parsed_row.opinion_url,
                    status=PairStatus.DISABLED,
                    max_position=parsed_row.max_position,
                    min_profit_percent=parsed_row.min_profit_percent,
                )
            return None

        # Handle DISCOVERED status (enabled row)
        if not existing:
            # New pair - create it
            self.store.upsert_pair(
                pair_id=pair_id,
                pm_url=parsed_row.polymarket_url,
                op_url=parsed_row.opinion_url,
                status=PairStatus.DISCOVERED,
                max_position=parsed_row.max_position,
                min_profit_percent=parsed_row.min_profit_percent,
            )
            return "new"

        # Existing pair - check if we should update
        if existing.status == PairStatus.DISABLED:
            # Re-enabling a disabled pair
            self.store.reset_selection(pair_id)
            # Update settings
            self.store.upsert_pair(
                pair_id=pair_id,
                pm_url=parsed_row.polymarket_url,
                op_url=parsed_row.opinion_url,
                max_position=parsed_row.max_position,
                min_profit_percent=parsed_row.min_profit_percent,
            )
            return "reenabled"

        if existing.status in self.PROTECTED_STATUSES:
            # Do not downgrade user progress
            # Just update settings (max_position, min_profit_percent)
            self.store.upsert_pair(
                pair_id=pair_id,
                pm_url=parsed_row.polymarket_url,
                op_url=parsed_row.opinion_url,
                max_position=parsed_row.max_position,
                min_profit_percent=parsed_row.min_profit_percent,
            )
            return None

        # Update settings for DISCOVERED pairs
        self.store.upsert_pair(
            pair_id=pair_id,
            pm_url=parsed_row.polymarket_url,
            op_url=parsed_row.opinion_url,
            max_position=parsed_row.max_position,
            min_profit_percent=parsed_row.min_profit_percent,
        )
        return None

    def _notify_new_pair(self, parsed_row: ParsedRow) -> None:
        """Send Telegram notification for new pair."""
        if not self.telegram_bot:
            return

        pair_id = parsed_row.pair_id

        # Deduplicate notifications
        if pair_id in self._notified_pairs:
            return
        self._notified_pairs.add(pair_id)

        # Send to all admin chats
        for admin_id in self.config.telegram_admin_ids:
            self._send_new_pair_notification(admin_id, parsed_row)

    def _notify_reenabled_pair(self, parsed_row: ParsedRow) -> None:
        """Send Telegram notification for re-enabled pair."""
        if not self.telegram_bot:
            return

        pair_id = parsed_row.pair_id

        # Remove from notified set so we can notify again
        self._notified_pairs.discard(pair_id)

        # Send to all admin chats
        for admin_id in self.config.telegram_admin_ids:
            self._send_new_pair_notification(admin_id, parsed_row, reenabled=True)

    def _send_new_pair_notification(
        self, chat_id: int, parsed_row: ParsedRow, reenabled: bool = False
    ) -> None:
        """Send notification message with Open button."""
        if not self.telegram_bot:
            return

        emoji = "🔄" if reenabled else "🆕"
        action = "Пара включена повторно" if reenabled else "Найдена новая пара"

        # Extract slugs for display
        pm_slug = self._extract_slug(parsed_row.polymarket_url)
        op_slug = self._extract_slug(parsed_row.opinion_url)

        text = (
            f"{emoji} *{action}*\n\n"
            f"📈 PM: {pm_slug}\n"
            f"🎯 OP: {op_slug}\n\n"
            f"Нажми чтобы выбрать исход на Polymarket."
        )

        # Use shortened pair_id (first 16 chars) for callback_data to stay within 64-byte limit
        short_pid = parsed_row.pair_id[:16]
        keyboard = [
            [
                {
                    "text": "📋 Открыть",
                    "callback_data": f"open:{short_pid}",
                }
            ]
        ]

        try:
            self.telegram_bot.send_message(
                chat_id,
                text,
                reply_markup={"inline_keyboard": keyboard},
            )
        except Exception as e:
            logger.error(f"Failed to send notification to {chat_id}: {e}")

    def _extract_slug(self, url: str) -> str:
        """Extract readable slug from URL."""
        if not url:
            return "N/A"
        # Remove protocol
        url = url.replace("https://", "").replace("http://", "").rstrip("/")
        # Take last path segment
        parts = url.split("/")
        slug = parts[-1] if parts else url
        # Truncate
        if len(slug) > 30:
            slug = slug[:27] + "..."
        return slug

    def sync_now(self) -> SheetsSyncResult:
        """
        Perform an immediate sync (for testing or manual trigger).

        Returns the sync result.
        """
        result = self.sheets_client.fetch_and_parse()
        self._process_sync_result(result)
        return result
