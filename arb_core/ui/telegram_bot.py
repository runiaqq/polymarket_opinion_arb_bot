"""
Telegram bot with polling and conflict handling.
"""

import sys
import threading
import time
from typing import TYPE_CHECKING, Optional

import requests

from ..core.config import Config
from ..core.logging import get_logger
from ..core.models import PairStatus
from ..core.store import InvalidTransitionError, PairNotFoundError, PairStore
from ..integrations.resolvers.opinion_local import OpinionDependencyError, OpinionLocalResolver, OpinionResolverError
from ..integrations.resolvers.polymarket import PolymarketResolver, PolymarketResolverError

from .telegram_ui import (
    build_pair_keyboard,
    build_pairs_list_keyboard,
    format_error_pm_first,
    format_pair_card,
    format_pair_compact,
    format_ready_card,
    format_simulation_result,
    format_start_message,
    format_trade_result,
)

if TYPE_CHECKING:
    from ..integrations.sheets_watcher import SheetsWatcher
    from ..runners.runner import CoveredArbRunner

logger = get_logger(__name__)


class TelegramConflictError(Exception):
    """Raised when another polling instance is detected (409 conflict)."""

    pass


class TelegramBot:
    """
    Telegram bot using long polling.

    Features:
    - Single instance enforcement
    - 409 conflict detection with fail-fast
    - deleteWebhook on startup
    """

    _instance_lock = threading.Lock()
    _instance_running = False

    def __init__(self, config: Config, store: PairStore):
        self.config = config
        self.store = store
        self.token = config.telegram_token
        self.admin_ids = set(config.telegram_admin_ids)
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.offset = 0
        self._running = False
        self._poll_thread: Optional[threading.Thread] = None

        # Runner reference (set externally after creation)
        self._runner: Optional["CoveredArbRunner"] = None
        
        # Sheets watcher reference (set externally after creation)
        self._sheets_watcher: Optional["SheetsWatcher"] = None

    def set_runner(self, runner: "CoveredArbRunner") -> None:
        """Set the runner reference for simulation/trading."""
        self._runner = runner

    def set_sheets_watcher(self, watcher: "SheetsWatcher") -> None:
        """Set the sheets watcher reference for manual refresh."""
        self._sheets_watcher = watcher

    def _api_call(
        self,
        method: str,
        data: Optional[dict] = None,
        timeout: int = 60,
    ) -> dict:
        """Make Telegram API call."""
        url = f"{self.base_url}/{method}"
        try:
            if data:
                response = requests.post(url, json=data, timeout=timeout)
            else:
                response = requests.get(url, timeout=timeout)

            result = response.json()

            # Check for 409 conflict
            if response.status_code == 409:
                error_desc = result.get("description", "Unknown conflict")
                raise TelegramConflictError(
                    f"Telegram 409 Conflict: {error_desc}. "
                    "Another instance is already polling. "
                    "Stop the other instance or wait for it to terminate."
                )

            if not result.get("ok"):
                error_code = result.get("error_code", "unknown")
                error_desc = result.get("description", "Unknown error")
                # Don't log expected/harmless errors as ERROR level
                if "message is not modified" in error_desc or "query is too old" in error_desc:
                    logger.debug(f"Telegram API (expected): {method} -> {error_code}: {error_desc}")
                else:
                    logger.error(f"Telegram API error: {method} -> {error_code}: {error_desc}")
                return {"ok": False, "error_code": error_code, "description": error_desc}

            return result

        except requests.exceptions.Timeout:
            logger.warning(f"Telegram API timeout: {method}")
            return {"ok": False, "error_code": "timeout", "description": "Request timeout"}
        except TelegramConflictError:
            raise
        except Exception as e:
            logger.error(f"Telegram API exception: {method} -> {e}")
            return {"ok": False, "error_code": "exception", "description": str(e)}

    def delete_webhook(self) -> bool:
        """Delete any existing webhook and drop pending updates."""
        logger.info("Deleting webhook and pending updates")
        result = self._api_call(
            "deleteWebhook", {"drop_pending_updates": True}
        )
        return result.get("ok", False)

    def get_me(self) -> Optional[dict]:
        """Validate token and get bot info."""
        result = self._api_call("getMe")
        if result.get("ok"):
            return result.get("result")
        return None

    def send_message(
        self,
        chat_id: int,
        text: str,
        parse_mode: str = "Markdown",
        reply_markup: Optional[dict] = None,
    ) -> Optional[dict]:
        """Send a text message."""
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        if reply_markup:
            data["reply_markup"] = reply_markup

        result = self._api_call("sendMessage", data)
        if result.get("ok"):
            return result.get("result")
        return None

    def edit_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: str = "Markdown",
        reply_markup: Optional[dict] = None,
    ) -> Optional[dict]:
        """Edit an existing message."""
        data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        if reply_markup:
            data["reply_markup"] = reply_markup

        result = self._api_call("editMessageText", data)
        if result.get("ok"):
            return result.get("result")
        # Silently ignore "message is not modified" error - this is normal
        if "message is not modified" in result.get("description", ""):
            return None
        return None

    def answer_callback_query(
        self,
        callback_query_id: str,
        text: Optional[str] = None,
        show_alert: bool = False,
    ) -> bool:
        """Answer a callback query."""
        data = {"callback_query_id": callback_query_id}
        if text:
            data["text"] = text
            data["show_alert"] = show_alert

        result = self._api_call("answerCallbackQuery", data)
        # Silently ignore "query is too old" error - this is normal when user clicks slowly
        if "query is too old" in result.get("description", ""):
            return True
        return result.get("ok", False)

    def delete_message(self, chat_id: int, message_id: int) -> bool:
        """Delete a message."""
        result = self._api_call(
            "deleteMessage", {"chat_id": chat_id, "message_id": message_id}
        )
        return result.get("ok", False)

    def _get_updates(self, timeout: int = 30) -> list[dict]:
        """Get updates using long polling."""
        result = self._api_call(
            "getUpdates",
            {"offset": self.offset, "timeout": timeout},
            timeout=timeout + 10,
        )
        if result.get("ok"):
            return result.get("result", [])
        return []

    def _is_admin(self, user_id: int) -> bool:
        """Check if user is an admin."""
        # If no admin IDs configured, allow all (for testing)
        if not self.admin_ids:
            return True
        return user_id in self.admin_ids

    def _handle_command(self, message: dict) -> None:
        """Handle incoming command message."""
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        text = message.get("text", "")
        
        logger.info(f"Received command: {text} from user {user_id}")

        if not self._is_admin(user_id):
            logger.warning(f"Unauthorized access attempt from user {user_id}")
            self.send_message(chat_id, "⛔ Доступ запрещён. Вы не администратор.")
            return

        # Get the command (first word)
        cmd = text.split()[0] if text else ""
        
        # Route commands - use exact match to avoid conflicts
        if cmd == "/start_trading":
            self._cmd_start_trading(chat_id)
        elif cmd == "/stop_trading":
            self._cmd_stop_trading(chat_id)
        elif cmd == "/start":
            self._cmd_start(chat_id)
        elif cmd == "/pairs":
            self._cmd_pairs_show(chat_id)
        elif cmd == "/refresh":
            self._cmd_refresh(chat_id)
        elif cmd == "/pnl":
            self._cmd_pnl(chat_id)
        elif cmd == "/trades":
            self._cmd_trades(chat_id)
        elif cmd == "/balance":
            self._cmd_balance(chat_id)
        elif cmd == "/sell_all":
            self._cmd_sell_all(chat_id)
        elif cmd == "/status":
            self._cmd_status(chat_id)
        elif cmd == "/help":
            self._cmd_help(chat_id)
        elif cmd == "/reset_menu":
            self._cmd_reset_menu(chat_id)
        elif cmd == "/accounts":
            self._cmd_accounts(chat_id)
        else:
            self._cmd_help(chat_id)

    def _cmd_start(self, chat_id: int) -> None:
        """Handle /start command."""
        counts = self.store.count_by_status()
        text = format_start_message(counts, mode="polling")
        self.send_message(chat_id, text)

    def _cmd_pairs_show(self, chat_id: int) -> None:
        """Handle /pairs_show command."""
        pairs = self.store.list_pairs()
        if not pairs:
            self.send_message(chat_id, "📭 Пары не найдены.")
            return

        text = "📋 *Список пар:*\n\n"
        for pair in pairs[:20]:
            text += format_pair_compact(pair) + "\n"

        if len(pairs) > 20:
            text += f"\n... и ещё {len(pairs) - 20}"

        keyboard = build_pairs_list_keyboard(pairs)
        self.send_message(
            chat_id, text, reply_markup={"inline_keyboard": keyboard}
        )

    def _cmd_pnl(self, chat_id: int) -> None:
        """Handle /pnl command - show PnL summary."""
        try:
            summary = self.store.get_trade_summary()
            
            lines = [
                "📊 *СВОДКА PnL*",
                "",
                f"✅ Завершено (hedged): {summary.get('completed_count', 0)}",
                f"⏳ В процессе (pending): {summary.get('pending_count', 0)}",
                f"❌ Ошибок (failed): {summary.get('failed_count', 0)}",
                "",
                f"💰 *Итого PnL: ${summary.get('total_pnl', 0):.4f}*",
                f"💵 Комиссии: ${summary.get('total_fees', 0):.4f}",
                f"📈 Чистый PnL: ${summary.get('total_pnl', 0) - summary.get('total_fees', 0):.4f}",
            ]
            
            if summary.get('completed_count', 0) > 0:
                lines.append(f"\n📈 Средний PnL/сделку: ${summary.get('avg_pnl', 0):.4f}")
            
            self.send_message(chat_id, "\n".join(lines))
        except Exception as e:
            logger.error(f"PnL command error: {e}")
            self.send_message(chat_id, f"❌ Ошибка: `{str(e)[:100]}`")

    def _cmd_trades(self, chat_id: int) -> None:
        """Handle /trades command - show recent trades."""
        try:
            # Get recent trades
            trades = self.store.get_recent_trades(limit=10)
            
            if not trades:
                self.send_message(chat_id, "📭 Нет сделок в истории.")
                return
            
            lines = ["📋 *Последние сделки:*", ""]
            
            for trade in trades:
                status_emoji = {
                    "pending": "⏳",
                    "entry_filled": "🔄",
                    "hedged": "✅",
                    "failed": "❌",
                    "cancelled": "🚫",
                }.get(trade.status.value, "❓")
                
                pnl_str = f"${trade.pnl:.4f}" if trade.pnl is not None else "—"
                
                lines.append(
                    f"{status_emoji} `{trade.trade_id[:8]}` | "
                    f"{trade.entry_exchange}/{trade.hedge_exchange or '—'} | "
                    f"PnL: {pnl_str}"
                )
            
            self.send_message(chat_id, "\n".join(lines))
        except Exception as e:
            logger.error(f"Trades command error: {e}")
            self.send_message(chat_id, f"❌ Ошибка: `{str(e)[:100]}`")

    def _cmd_help(self, chat_id: int) -> None:
        """Handle /help command - show available commands."""
        lines = [
            "*Команды бота:*",
            "",
            "*Управление:*",
            "/status - Текущий статус",
            "/start\\_trading - Включить торговлю",
            "/stop\\_trading - Остановить торговлю",
            "",
            "*Информация:*",
            "/pairs - Список пар",
            "/balance - Балансы на биржах",
            "/accounts - Статус аккаунтов",
            "/pnl - Сводка PnL",
            "/trades - Последние сделки",
            "",
            "*Действия:*",
            "/refresh - Синхронизация с Sheets",
            "/sell\\_all - Продать ВСЕ позиции",
            "/help - Эта справка",
        ]
        self.send_message(chat_id, "\n".join(lines))

    def _cmd_reset_menu(self, chat_id: int) -> None:
        """Handle /reset_menu command - force refresh the commands menu."""
        self._setup_commands_menu()
        self.send_message(chat_id, "✅ Меню команд обновлено. Перезапустите Telegram чтобы увидеть изменения.")

    def _cmd_status(self, chat_id: int) -> None:
        """Handle /status command - show current bot status."""
        try:
            if not hasattr(self, '_runner') or not self._runner:
                self.send_message(chat_id, "❌ Runner не инициализирован")
                return

            trading_enabled = self._runner.is_trading_enabled()
            is_running = self._runner.is_running()
            active_orders = self._runner.get_active_orders_count()
            pairs_count = len(self.store.list_pairs())

            status_emoji = "🟢" if trading_enabled else "🔴"
            trading_status = "ВКЛЮЧЕНА" if trading_enabled else "ВЫКЛЮЧЕНА"

            lines = [
                "*Статус бота:*",
                "",
                f"{status_emoji} Торговля: *{trading_status}*",
                f"🔄 Runner: {'работает' if is_running else 'остановлен'}",
                f"📊 Пар в мониторинге: {pairs_count}",
                f"📝 Активных ордеров: {active_orders}",
                "",
                "Используй /start\\_trading или /stop\\_trading",
            ]
            self.send_message(chat_id, "\n".join(lines))
        except Exception as e:
            logger.error(f"Status command error: {e}")
            self.send_message(chat_id, f"❌ Ошибка: `{str(e)[:100]}`")

    def _cmd_start_trading(self, chat_id: int) -> None:
        """Handle /start_trading command - enable trading."""
        logger.info("Processing /start_trading command")
        try:
            if not hasattr(self, '_runner') or not self._runner:
                logger.warning("Runner not set!")
                self.send_message(chat_id, "❌ Runner не инициализирован")
                return

            if self._runner.is_trading_enabled():
                self.send_message(chat_id, "⚠️ Торговля уже включена")
                return

            self._runner.enable_trading()
            logger.info("Trading ENABLED via Telegram command")
            pairs_count = len(self.store.list_pairs())
            
            lines = [
                "🟢 *ТОРГОВЛЯ ВКЛЮЧЕНА*",
                "",
                f"Пар в мониторинге: {pairs_count}",
                "Бот начнёт выставлять ордера на прибыльных парах.",
                "",
                "Для остановки: /stop\\_trading",
            ]
            self.send_message(chat_id, "\n".join(lines))
        except Exception as e:
            logger.error(f"Start trading error: {e}")
            self.send_message(chat_id, f"❌ Ошибка: `{str(e)[:100]}`")

    def _cmd_stop_trading(self, chat_id: int) -> None:
        """Handle /stop_trading command - disable trading."""
        try:
            if not hasattr(self, '_runner') or not self._runner:
                self.send_message(chat_id, "❌ Runner не инициализирован")
                return

            if not self._runner.is_trading_enabled():
                self.send_message(chat_id, "⚠️ Торговля уже выключена")
                return

            self._runner.disable_trading()
            
            lines = [
                "🔴 *ТОРГОВЛЯ ОСТАНОВЛЕНА*",
                "",
                "Бот продолжает мониторить пары, но не выставляет ордера.",
                "Активные ордера НЕ отменяются автоматически.",
                "",
                "Для запуска: /start\\_trading",
            ]
            self.send_message(chat_id, "\n".join(lines))
        except Exception as e:
            logger.error(f"Stop trading error: {e}")
            self.send_message(chat_id, f"❌ Ошибка: `{str(e)[:100]}`")

    def _cmd_balance(self, chat_id: int) -> None:
        """Handle /balance command - show balances on both exchanges."""
        self.send_message(chat_id, "Проверяю балансы...")
        
        try:
            if not hasattr(self, '_runner') or not self._runner:
                self.send_message(chat_id, "Runner не инициализирован")
                return
            
            pm_balance = self._runner.clients.pm_client.get_balance()
            op_balance = self._runner.clients.op_client.get_balance()
            
            lines = [
                "*Балансы:*",
                "",
                f"*Polymarket:* ${pm_balance.available:.2f}",
                f"*Opinion:* ${op_balance.available:.2f}",
                "",
                f"*Всего:* ${pm_balance.available + op_balance.available:.2f}",
            ]
            self.send_message(chat_id, "\n".join(lines))
        except Exception as e:
            logger.error(f"Balance command error: {e}")
            self.send_message(chat_id, f"Ошибка: `{str(e)[:100]}`")

    def _cmd_accounts(self, chat_id: int) -> None:
        """Handle /accounts command - show account pool status."""
        try:
            # Try to load accounts from config
            import json
            try:
                with open("config/accounts.json") as f:
                    config_data = json.load(f)
            except FileNotFoundError:
                self.send_message(chat_id, "❌ Файл config/accounts.json не найден")
                return
            except json.JSONDecodeError as e:
                self.send_message(chat_id, f"❌ Ошибка парсинга accounts.json: {e}")
                return
            
            accounts_data = config_data.get("accounts", [])
            
            # Group by account_id
            account_ids = set()
            pm_count = 0
            op_count = 0
            proxy_count = 0
            
            for entry in accounts_data:
                account_ids.add(entry.get("account_id", "unknown"))
                exchange = entry.get("exchange", "").lower()
                if exchange == "polymarket":
                    pm_count += 1
                elif exchange == "opinion":
                    op_count += 1
                if entry.get("proxy"):
                    proxy_count += 1
            
            lines = [
                "*📊 Статус аккаунтов:*",
                "",
                f"👥 Всего аккаунтов: *{len(account_ids)}*",
                f"🔵 Polymarket: {pm_count}",
                f"🟢 Opinion: {op_count}",
                f"🌐 С прокси: {proxy_count}",
                "",
            ]
            
            # Show current active account
            if hasattr(self, '_runner') and self._runner:
                try:
                    pm_bal = self._runner.clients.pm_client.get_balance().available
                    op_bal = self._runner.clients.op_client.get_balance().available
                    lines.extend([
                        "*Активный аккаунт:*",
                        f"  PM: ${pm_bal:.2f}",
                        f"  OP: ${op_bal:.2f}",
                    ])
                except Exception:
                    lines.append("_Баланс недоступен_")
            
            self.send_message(chat_id, "\n".join(lines))
            
        except Exception as e:
            logger.error(f"Accounts command error: {e}")
            self.send_message(chat_id, f"❌ Ошибка: `{str(e)[:100]}`")

    def _cmd_sell_all(self, chat_id: int) -> None:
        """Handle /sell_all command - emergency sell all positions on both exchanges."""
        self.send_message(chat_id, "⏳ Сканирую позиции...")
        
        try:
            from web3 import Web3
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.constants import POLYGON
            import json
            
            results = []
            
            # Load Polymarket config
            try:
                with open("config/accounts.json") as f:
                    config = json.load(f)
            except FileNotFoundError:
                self.send_message(chat_id, "❌ Файл config/accounts.json не найден")
                return
            except json.JSONDecodeError as e:
                self.send_message(chat_id, f"❌ Ошибка парсинга accounts.json: {e}")
                return
            
            poly_acc = None
            for acc in config.get("accounts", []):
                if acc.get("exchange") == "Polymarket":
                    poly_acc = acc
                    break
            
            if not poly_acc:
                self.send_message(chat_id, "❌ Polymarket аккаунт не найден в accounts.json")
                return
            
            private_key = poly_acc.get("private_key")
            wallet = poly_acc.get("wallet_address")
            
            if not private_key:
                self.send_message(chat_id, "❌ private_key не найден в аккаунте Polymarket")
                return
            
            if not wallet:
                # Derive wallet from private key
                w3_temp = Web3()
                wallet = w3_temp.eth.account.from_key(private_key).address
                logger.info(f"Derived wallet address: {wallet}")
            
            results.append(f"Кошелек: {wallet[:10]}...{wallet[-6:]}")
            
            # Initialize CLOB client
            try:
                client = ClobClient(
                    host="https://clob.polymarket.com",
                    key=private_key,
                    chain_id=POLYGON,
                )
                client.set_api_creds(client.derive_api_key())
                results.append("✅ CLOB клиент подключен")
            except Exception as e:
                self.send_message(chat_id, f"❌ Ошибка CLOB клиента: {str(e)[:100]}")
                return
            
            # Connect to Polygon for balance check
            w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
            CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
            ERC1155_ABI = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}]
            ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=ERC1155_ABI)
            
            # Get all pairs from database
            pairs = self.store.list_pairs()
            results.append(f"Пар в базе: {len(pairs)}")
            
            if not pairs:
                self.send_message(chat_id, "Нет пар в базе данных. Позиции не найдены.")
                return
            
            sold_count = 0
            positions_found = 0
            
            for pair in pairs:
                if not pair.pm_token:
                    continue
                
                try:
                    balance = ctf.functions.balanceOf(
                        Web3.to_checksum_address(wallet),
                        int(pair.pm_token)
                    ).call()
                    
                    if balance > 0:
                        positions_found += 1
                        shares = balance / 1e6
                        
                        # Sell at low price to ensure fill
                        sell_price = 0.01
                        
                        try:
                            order_args = OrderArgs(
                                price=sell_price,
                                size=shares,
                                side="SELL",
                                token_id=pair.pm_token,
                            )
                            
                            signed_order = client.create_order(order_args)
                            result = client.post_order(signed_order, OrderType.GTC)
                            
                            status = result.get('status', 'unknown') if isinstance(result, dict) else str(result)
                            results.append(f"✅ PM: {shares:.2f} шейров @ ${sell_price} -> {status}")
                            sold_count += 1
                        except Exception as e:
                            results.append(f"❌ PM sell error: {str(e)[:50]}")
                        
                except Exception as e:
                    logger.warning(f"Balance check error for {pair.pair_id[:8]}: {e}")
            
            # Cancel all active orders in runner
            if hasattr(self, '_runner') and self._runner:
                active_orders = getattr(self._runner, '_active_orders', {})
                if active_orders:
                    results.append(f"Активных ордеров в раннере: {len(active_orders)}")
                    # Note: actual cancellation would need exchange client access
            
            # Build response
            lines = [
                "*РЕЗУЛЬТАТ СКАНИРОВАНИЯ:*",
                "",
            ]
            lines.extend(results)
            lines.append("")
            lines.append(f"Найдено позиций: {positions_found}")
            lines.append(f"Выставлено на продажу: {sold_count}")
            
            if positions_found == 0:
                lines.append("")
                lines.append("ℹ️ Позиций на Polymarket не найдено")
            
            self.send_message(chat_id, "\n".join(lines))
            
        except Exception as e:
            logger.error(f"Sell all error: {e}")
            import traceback
            traceback.print_exc()
            self.send_message(chat_id, f"❌ Ошибка: `{str(e)[:200]}`")

    def _cmd_refresh(self, chat_id: int) -> None:
        """Handle /refresh command - sync with Google Sheets and remove stale pairs."""
        self.send_message(chat_id, "Синхронизация с Google Sheets...")
        
        try:
            # Get current pairs from store
            old_pairs = {p.pair_id: p for p in self.store.list_pairs()}
            old_count = len(old_pairs)
            
            # Sync from sheets
            if self._sheets_watcher:
                result = self._sheets_watcher.sync_now()
                
                # Get new list of pair IDs from sheets
                sheet_pair_ids = set()
                for row in result.parsed_rows:
                    from ..core.models import compute_pair_id
                    pair_id = compute_pair_id(row.polymarket_url, row.opinion_url)
                    sheet_pair_ids.add(pair_id)
                
                # Find pairs to delete (not in sheet anymore)
                pairs_to_delete = []
                for pair_id, pair in old_pairs.items():
                    if pair_id not in sheet_pair_ids:
                        pairs_to_delete.append(pair)
                
                # Delete stale pairs
                deleted_count = 0
                for pair in pairs_to_delete:
                    if self.store.delete_pair(pair.pair_id):
                        deleted_count += 1
                        logger.info(f"Deleted stale pair: {pair.pair_id[:12]}")
                
                # Get updated counts
                new_pairs = self.store.list_pairs()
                new_count = len(new_pairs)
                
                lines = [
                    "✅ *Синхронизация завершена!*",
                    "",
                    f"📊 В таблице: {len(sheet_pair_ids)} пар",
                    f"📋 Было в боте: {old_count}",
                    f"🗑️ Удалено: {deleted_count}",
                    f"📋 Сейчас в боте: {new_count}",
                    "",
                    f"✅ Успешно: {result.ok_count}",
                    f"❌ Ошибок: {result.error_count}",
                    f"⏸️ Отключено: {result.disabled_count}",
                ]
                
                self.send_message(chat_id, "\n".join(lines))
            else:
                self.send_message(chat_id, "⚠️ Sheets watcher не инициализирован")
                
        except Exception as e:
            logger.error(f"Refresh error: {e}")
            error_msg = str(e)[:200].replace("_", "\\_").replace("*", "\\*")
            self.send_message(chat_id, f"❌ Ошибка: `{error_msg}`")

    def _handle_callback(self, callback_query: dict) -> None:
        """Handle callback query from inline keyboard."""
        callback_id = callback_query["id"]
        user_id = callback_query["from"]["id"]
        chat_id = callback_query["message"]["chat"]["id"]
        message_id = callback_query["message"]["message_id"]
        data = callback_query.get("data", "")

        if not self._is_admin(user_id):
            self.answer_callback_query(callback_id, "⛔ Доступ запрещён", show_alert=True)
            return

        try:
            self._process_callback(chat_id, message_id, callback_id, data)
        except Exception as e:
            logger.error(f"Callback error: {e}")
            self.answer_callback_query(callback_id, f"Error: {str(e)[:50]}", show_alert=True)

    def _process_callback(
        self, chat_id: int, message_id: int, callback_id: str, data: str
    ) -> None:
        """Process callback data."""
        if data == "close":
            self.delete_message(chat_id, message_id)
            self.answer_callback_query(callback_id)
            return

        if ":" not in data:
            self.answer_callback_query(callback_id, "Неверный callback")
            return

        action, pair_id_prefix = data.split(":", 1)

        # Resolve pair by prefix (callback_data is limited to 64 bytes)
        pair = self.store.get_pair_by_prefix(pair_id_prefix)
        if not pair and action != "open":
            # For open, try prefix lookup in _show_pair_card
            self.answer_callback_query(callback_id, "Пара не найдена", show_alert=True)
            return

        # Get full pair_id for handlers
        pair_id = pair.pair_id if pair else pair_id_prefix

        # Handle open action
        if action == "open":
            self._show_pair_card(chat_id, message_id, callback_id, pair_id_prefix)
            return

        if action == "pm_yes":
            self._handle_pm_selection(chat_id, message_id, callback_id, pair_id, "YES")
        elif action == "pm_no":
            self._handle_pm_selection(chat_id, message_id, callback_id, pair_id, "NO")
        elif action == "op_yes":
            self._handle_op_selection(chat_id, message_id, callback_id, pair_id, "YES")
        elif action == "op_no":
            self._handle_op_selection(chat_id, message_id, callback_id, pair_id, "NO")
        elif action == "reset":
            self._handle_reset(chat_id, message_id, callback_id, pair_id)
        elif action == "trade":
            self._handle_trade(chat_id, message_id, callback_id, pair_id)
        elif action in ("deactivate", "deact"):
            self._handle_deactivate(chat_id, message_id, callback_id, pair_id)
        elif action in ("simulate", "sim"):
            self._handle_simulate(chat_id, message_id, callback_id, pair_id)
        elif action == "pnl":
            self._handle_pnl(chat_id, message_id, callback_id, pair_id)
        else:
            self.answer_callback_query(callback_id, f"Неизвестное действие: {action}")

    def _show_pair_card(
        self, chat_id: int, message_id: int, callback_id: str, pair_id_prefix: str
    ) -> None:
        """Show pair card with details."""
        # Try exact match first, then prefix
        pair = self.store.get_pair(pair_id_prefix)
        if not pair:
            pair = self.store.get_pair_by_prefix(pair_id_prefix)
        if not pair:
            self.answer_callback_query(callback_id, "Пара не найдена", show_alert=True)
            return

        text = format_pair_card(pair)
        keyboard = build_pair_keyboard(pair)

        self.edit_message(
            chat_id, message_id, text, reply_markup={"inline_keyboard": keyboard}
        )
        self.answer_callback_query(callback_id)

    def _handle_pm_selection(
        self, chat_id: int, message_id: int, callback_id: str, pair_id: str, side: str
    ) -> None:
        """Handle Polymarket side selection with token resolution."""
        pair = self.store.get_pair(pair_id)
        if not pair:
            self.answer_callback_query(callback_id, "Пара не найдена", show_alert=True)
            return

        # Resolve Polymarket tokens
        self.answer_callback_query(callback_id, "Получаю токены Polymarket...")

        try:
            resolver = PolymarketResolver()
            tokens = resolver.resolve(pair.polymarket_url)
            token_id = tokens.get(side)

            if not token_id:
                self.answer_callback_query(
                    callback_id, f"Токен не найден для {side}", show_alert=True
                )
                return

            # Update store with side and token
            pair = self.store.set_pm_selection(pair_id, side, token=token_id)

            # Suggest opposite side for Opinion
            opposite = "NO" if side == "YES" else "YES"

            # Send confirmation with next step instruction
            self._refresh_pair_card(chat_id, message_id, pair)

            # Send follow-up message
            self.send_message(
                chat_id,
                f"✅ *Polymarket выбран: {side}*\n\n"
                f"Токен: `{token_id[:16]}...`\n\n"
                f"Теперь выбери исход на Opinion.\n"
                f"_(Рекомендуется: {opposite} для арбитража)_",
            )

        except PolymarketResolverError as e:
            logger.error(f"Polymarket resolver error: {e}")
            self.send_message(
                chat_id,
                f"❌ *Ошибка Polymarket:*\n{str(e)[:200]}",
            )
        except Exception as e:
            logger.error(f"PM selection error: {e}")
            self.answer_callback_query(callback_id, str(e)[:50], show_alert=True)

    def _handle_op_selection(
        self, chat_id: int, message_id: int, callback_id: str, pair_id: str, side: str
    ) -> None:
        """Handle Opinion side selection with token resolution."""
        pair = self.store.get_pair(pair_id)
        if not pair:
            self.answer_callback_query(callback_id, "Пара не найдена", show_alert=True)
            return

        # Check that PM is selected first
        if pair.status == PairStatus.DISCOVERED:
            self.answer_callback_query(
                callback_id, format_error_pm_first(), show_alert=True
            )
            return

        # Resolve Opinion tokens
        self.answer_callback_query(callback_id, "Получаю токены Opinion...")

        try:
            resolver = OpinionLocalResolver()
            tokens = resolver.resolve(pair.opinion_url)
            token_id = tokens.get(side)
            question_id = tokens.get("question_id")

            if not token_id:
                self.answer_callback_query(
                    callback_id, f"Токен не найден для {side}", show_alert=True
                )
                return

            # Update store with side, token, and question_id -> transitions to READY
            pair = self.store.set_op_selection(
                pair_id, side, token=token_id, question_id=question_id
            )

            # Show READY card with full details
            text = format_ready_card(pair)
            keyboard = build_pair_keyboard(pair)

            self.edit_message(
                chat_id, message_id, text, reply_markup={"inline_keyboard": keyboard}
            )

            # Confirmation message
            self.send_message(
                chat_id,
                f"✅ *Пара готова к торговле!*\n\n"
                f"PM: {pair.pm_side} | OP: {pair.op_side}\n\n"
                f"Нажми *Торговать* чтобы активировать.",
            )

        except OpinionDependencyError as e:
            logger.error(f"Opinion dependency error: {e}")
            self.send_message(
                chat_id,
                f"❌ *Отсутствует зависимость Opinion:*\n"
                f"`{e.dependency}`\n\n"
                f"Установи: `{e.install_hint}`" if e.install_hint else "",
            )
        except OpinionResolverError as e:
            logger.error(f"Opinion resolver error: {e}")
            self.send_message(
                chat_id,
                f"❌ *Ошибка Opinion:*\n{str(e)[:200]}",
            )
        except InvalidTransitionError as e:
            self.answer_callback_query(callback_id, format_error_pm_first(), show_alert=True)
        except Exception as e:
            logger.error(f"OP selection error: {e}")
            self.answer_callback_query(callback_id, str(e)[:50], show_alert=True)

    def _handle_reset(
        self, chat_id: int, message_id: int, callback_id: str, pair_id: str
    ) -> None:
        """Handle reset action."""
        try:
            pair = self.store.reset_selection(pair_id)
            self.answer_callback_query(callback_id, "Сброшено в НАЙДЕНА")
            self._refresh_pair_card(chat_id, message_id, pair)
        except Exception as e:
            self.answer_callback_query(callback_id, str(e)[:50], show_alert=True)

    def _handle_trade(
        self, chat_id: int, message_id: int, callback_id: str, pair_id: str
    ) -> None:
        """Handle trade activation."""
        try:
            pair = self.store.activate(pair_id)
            self.answer_callback_query(callback_id, "Торговля активирована! 🚀")
            self._refresh_pair_card(chat_id, message_id, pair)
        except InvalidTransitionError as e:
            self.answer_callback_query(callback_id, str(e)[:50], show_alert=True)
        except Exception as e:
            self.answer_callback_query(callback_id, str(e)[:50], show_alert=True)

    def _handle_deactivate(
        self, chat_id: int, message_id: int, callback_id: str, pair_id: str
    ) -> None:
        """Handle deactivation."""
        try:
            pair = self.store.deactivate(pair_id)
            self.answer_callback_query(callback_id, "Остановлено")
            self._refresh_pair_card(chat_id, message_id, pair)
        except Exception as e:
            self.answer_callback_query(callback_id, str(e)[:50], show_alert=True)

    def _handle_simulate(
        self, chat_id: int, message_id: int, callback_id: str, pair_id: str
    ) -> None:
        """Handle simulation request."""
        pair = self.store.get_pair(pair_id)
        if not pair:
            self.answer_callback_query(callback_id, "Пара не найдена", show_alert=True)
            return

        if not self._runner:
            self.answer_callback_query(
                callback_id, "Runner не инициализирован", show_alert=True
            )
            return

        if not pair.pm_token or not pair.op_token:
            self.answer_callback_query(
                callback_id, "Сначала выбери исходы на обеих платформах", show_alert=True
            )
            return

        self.answer_callback_query(callback_id, "Выполняю симуляцию...")

        try:
            simulation = self._runner.simulate_pair(pair)
            text = format_simulation_result(pair, simulation)
            self.send_message(chat_id, text)

        except Exception as e:
            logger.error(f"Simulation error for {pair_id[:12]}: {e}")
            # Escape error message for Markdown safety
            error_msg = str(e)[:200].replace("_", "\\_").replace("*", "\\*")
            self.send_message(
                chat_id,
                f"❌ *Ошибка симуляции:*\n`{error_msg}`",
            )

    def _handle_pnl(
        self, chat_id: int, message_id: int, callback_id: str, pair_id: str
    ) -> None:
        """Handle PnL request - show last simulation result."""
        pair = self.store.get_pair(pair_id)
        if not pair:
            self.answer_callback_query(callback_id, "Пара не найдена", show_alert=True)
            return

        if not self._runner:
            self.answer_callback_query(
                callback_id, "Runner не инициализирован", show_alert=True
            )
            return

        # Get last simulation
        simulation = self._runner.get_last_simulation(pair_id)

        if not simulation:
            self.answer_callback_query(
                callback_id, "Нет данных. Сначала выполни Симуляцию.", show_alert=True
            )
            return

        self.answer_callback_query(callback_id)

        # Format and send PnL info
        lines = [
            "💰 *Последняя симуляция PnL*",
            "",
            f"🆔 `{pair_id[:12]}...`",
            "",
        ]

        if simulation.is_tradeable:
            lines.extend([
                f"📊 Размер: *{simulation.size_result.size:.2f}*",
                f"💵 Инвестиция: *${simulation.total_investment:.2f}*",
                f"💰 Ожид. прибыль: *${simulation.expected_profit:.2f}*",
                f"📈 Ожид. %: *{simulation.expected_profit_pct:.2f}%*",
                "",
                f"PM цена: {simulation.quote.pm_ask:.4f}",
                f"OP цена: {simulation.quote.op_ask:.4f}",
                f"Итого: {simulation.quote.total_cost:.4f}",
            ])
        else:
            lines.extend([
                f"⚠️ Не торгуется: _{simulation.skip_reason}_",
                "",
                f"PM цена: {simulation.quote.pm_ask:.4f}",
                f"OP цена: {simulation.quote.op_ask:.4f}",
                f"Итого: {simulation.quote.total_cost:.4f}",
            ])

        self.send_message(chat_id, "\n".join(lines))

    def _refresh_pair_card(self, chat_id: int, message_id: int, pair) -> None:
        """Refresh pair card after state change."""
        text = format_pair_card(pair)
        keyboard = build_pair_keyboard(pair)
        self.edit_message(
            chat_id, message_id, text, reply_markup={"inline_keyboard": keyboard}
        )

    def _process_update(self, update: dict) -> None:
        """Process a single update."""
        if "message" in update:
            message = update["message"]
            if "text" in message and message["text"].startswith("/"):
                self._handle_command(message)

        elif "callback_query" in update:
            self._handle_callback(update["callback_query"])

    def _poll_loop(self) -> None:
        """Main polling loop."""
        logger.info("Starting poll loop")
        consecutive_errors = 0
        max_consecutive_errors = 5

        while self._running:
            try:
                updates = self._get_updates(timeout=self.config.polling_timeout)
                consecutive_errors = 0  # Reset on success

                for update in updates:
                    self.offset = update["update_id"] + 1
                    try:
                        self._process_update(update)
                    except Exception as e:
                        logger.error(f"Error processing update: {e}")

            except TelegramConflictError as e:
                logger.critical(f"FATAL: {e}")
                logger.critical(
                    "ACTION REQUIRED: Stop the other polling instance before restarting."
                )
                self._running = False
                # Exit with non-zero code
                sys.exit(1)

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Poll error ({consecutive_errors}/{max_consecutive_errors}): {e}")

                if consecutive_errors >= max_consecutive_errors:
                    logger.critical("Too many consecutive errors, stopping")
                    self._running = False
                else:
                    time.sleep(min(consecutive_errors * 2, 30))  # Backoff

        logger.info("Poll loop stopped")

    def _setup_commands_menu(self) -> None:
        """Set up the bot commands menu in Telegram."""
        commands = [
            {"command": "status", "description": "Статус бота и торговли"},
            {"command": "start_trading", "description": "Включить торговлю"},
            {"command": "stop_trading", "description": "Остановить торговлю"},
            {"command": "balance", "description": "Балансы PM и OP"},
            {"command": "accounts", "description": "Статус аккаунтов"},
            {"command": "pairs", "description": "Список пар"},
            {"command": "pnl", "description": "Сводка PnL"},
            {"command": "trades", "description": "Последние сделки"},
            {"command": "refresh", "description": "Синхронизация с Sheets"},
            {"command": "sell_all", "description": "Продать ВСЕ позиции"},
            {"command": "help", "description": "Справка по командам"},
        ]
        
        # Clear and set commands for all scopes
        scopes_to_clear = [
            {},
            {"scope": {"type": "default"}},
            {"scope": {"type": "all_private_chats"}},
            {"scope": {"type": "all_group_chats"}},
            {"scope": {"type": "all_chat_administrators"}},
        ]
        
        # Also clear for each admin chat specifically
        for admin_id in self.admin_ids:
            scopes_to_clear.append({"scope": {"type": "chat", "chat_id": admin_id}})
        
        for scope in scopes_to_clear:
            self._api_call("deleteMyCommands", scope)
        
        # Set commands for default scope
        self._api_call("setMyCommands", {"commands": commands})
        
        # Also set for each admin's private chat
        for admin_id in self.admin_ids:
            self._api_call("setMyCommands", {
                "commands": commands,
                "scope": {"type": "chat", "chat_id": admin_id}
            })

    def start(self) -> bool:
        """
        Start the bot.

        Returns True if started successfully, False otherwise.
        """
        # Ensure single instance
        with self._instance_lock:
            if TelegramBot._instance_running:
                logger.error("Another bot instance is already running in this process")
                return False
            TelegramBot._instance_running = True

        try:
            # Delete webhook first
            if not self.delete_webhook():
                logger.warning("Failed to delete webhook, continuing anyway")

            # Validate token
            me = self.get_me()
            if not me:
                logger.error("Failed to validate bot token")
                with self._instance_lock:
                    TelegramBot._instance_running = False
                return False

            logger.info(f"Bot validated: @{me.get('username', 'unknown')}")
            
            # Set up commands menu
            self._setup_commands_menu()

            # Start polling
            self._running = True
            self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
            self._poll_thread.start()

            logger.info("Telegram started")
            print("Telegram started")
            return True

        except TelegramConflictError as e:
            logger.critical(f"FATAL: {e}")
            with self._instance_lock:
                TelegramBot._instance_running = False
            sys.exit(1)

        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            with self._instance_lock:
                TelegramBot._instance_running = False
            return False

    def stop(self) -> None:
        """Stop the bot."""
        logger.info("Stopping bot")
        self._running = False

        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5)

        with self._instance_lock:
            TelegramBot._instance_running = False

        logger.info("Bot stopped")

    def run_forever(self) -> None:
        """Run the bot until interrupted."""
        if not self.start():
            logger.error("Failed to start bot")
            return

        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self.stop()

    def notify_trade_placed(self, trade_info: dict) -> None:
        """Send notification when orders are placed."""
        for admin_id in self.admin_ids:
            try:
                lines = [
                    "🔔 *ОРДЕРА РАЗМЕЩЕНЫ*",
                    "",
                    f"📊 Пара: `{trade_info.get('pair_id', 'unknown')[:12]}...`",
                    "",
                    f"*Polymarket:*",
                    f"  Размер: {trade_info.get('pm_size', 0):.2f} контрактов",
                    f"  Цена: ${trade_info.get('pm_price', 0):.4f}",
                    f"  Стоимость: ${trade_info.get('pm_cost', 0):.2f}",
                    "",
                    f"*Opinion:*",
                    f"  Размер: {trade_info.get('op_size', 0):.2f} контрактов",
                    f"  Цена: ${trade_info.get('op_price', 0):.4f}",
                    f"  Стоимость: ${trade_info.get('op_cost', 0):.2f}",
                    "",
                    f"💰 Итого инвестиция: ${trade_info.get('total_investment', 0):.2f}",
                    f"📈 Ожидаемый профит: ${trade_info.get('expected_profit', 0):.2f} ({trade_info.get('expected_profit_pct', 0):.2f}%)",
                ]
                self.send_message(admin_id, "\n".join(lines))
            except Exception as e:
                logger.error(f"Failed to send trade notification: {e}")

    def notify_trade_complete(self, trade) -> None:
        """Send notification when a trade is completed (hedged)."""
        for admin_id in self.admin_ids:
            try:
                status_emoji = "✅" if trade.status.value == "hedged" else "⚠️"

                # Calculate costs for breakdown
                entry_cost = trade.entry_size * trade.entry_price if trade.entry_price else 0
                hedge_cost = (trade.hedge_size or 0) * (trade.hedge_price or 0)
                total_cost = entry_cost + hedge_cost
                payout = min(trade.entry_size, trade.hedge_size or 0) * 1.0

                lines = [
                    f"{status_emoji} *СДЕЛКА ЗАВЕРШЕНА*",
                    "",
                    f"🆔 `{trade.trade_id[:12]}...`",
                    "",
                    "*ВХОД:*",
                    f"  {trade.entry_exchange}: {trade.entry_size:.2f} @ ${trade.entry_price:.4f}",
                    f"  Стоимость: ${entry_cost:.2f}",
                ]

                if trade.hedge_exchange:
                    lines.extend([
                        "",
                        "*ХЕДЖ:*",
                        f"  {trade.hedge_exchange}: {trade.hedge_size:.2f} @ ${trade.hedge_price:.4f}",
                        f"  Стоимость: ${hedge_cost:.2f}",
                    ])
                
                lines.extend([
                    "",
                    "*ИТОГ:*",
                    f"  Инвестиция: ${total_cost:.2f}",
                    f"  Выплата: ${payout:.2f}",
                    f"  Комиссии: ${trade.fees_total:.4f}" if trade.fees_total else "  Комиссии: $0",
                ])
                
                if trade.pnl is not None:
                    pnl_emoji = "📈" if trade.pnl >= 0 else "📉"
                    pnl_percent = trade.pnl_percent or 0
                    lines.extend([
                        "",
                        f"*{pnl_emoji} PnL: ${trade.pnl:.4f} ({pnl_percent:.2f}%)*",
                    ])
                
                if trade.error_message:
                    lines.append(f"\n⚠️ Ошибка: _{trade.error_message}_")
                
                self.send_message(admin_id, "\n".join([l for l in lines if l]))
            except Exception as e:
                logger.error(f"Failed to send trade complete notification: {e}")

    def notify_pnl_summary(self) -> None:
        """Send daily PnL summary."""
        for admin_id in self.admin_ids:
            try:
                summary = self.store.get_trade_summary()
                
                lines = [
                    "📊 *СВОДКА PnL*",
                    "",
                    f"✅ Завершено: {summary.get('completed_count', 0)}",
                    f"⏳ В процессе: {summary.get('pending_count', 0)}",
                    f"❌ Ошибок: {summary.get('failed_count', 0)}",
                    "",
                    f"💰 *Итого PnL: ${summary.get('total_pnl', 0):.4f}*",
                    f"💵 Комиссии: ${summary.get('total_fees', 0):.4f}",
                    "",
                    f"📈 Средний PnL/сделку: ${summary.get('avg_pnl', 0):.4f}",
                ]
                
                self.send_message(admin_id, "\n".join(lines))
            except Exception as e:
                logger.error(f"Failed to send PnL summary: {e}")

    def notify_unhedged_position(self, trade_info: dict) -> None:
        """Send CRITICAL alert when hedge fails and position is unhedged."""
        for admin_id in self.admin_ids:
            try:
                lines = [
                    "🚨🚨🚨 *КРИТИЧЕСКАЯ ОШИБКА* 🚨🚨🚨",
                    "",
                    "*НЕЗАХЕДЖИРОВАННАЯ ПОЗИЦИЯ!*",
                    "",
                    f"📊 Trade: `{trade_info.get('trade_id', 'unknown')[:12]}...`",
                    f"📍 Биржа: {trade_info.get('exchange', 'unknown')}",
                    f"📈 Размер: {trade_info.get('size', 0):.2f} контрактов",
                    f"💵 Цена: ${trade_info.get('price', 0):.4f}",
                    f"💰 Стоимость: ${trade_info.get('cost', 0):.2f}",
                    "",
                    f"❌ Причина: {trade_info.get('error', 'unknown')}",
                    "",
                    "*ТРЕБУЕТСЯ РУЧНОЕ ВМЕШАТЕЛЬСТВО!*",
                    "",
                    "Варианты:",
                    "1. Захеджируй вручную на другой бирже",
                    "2. Закрой позицию с убытком",
                ]
                self.send_message(admin_id, "\n".join(lines))
            except Exception as e:
                logger.error(f"Failed to send unhedged alert: {e}")
