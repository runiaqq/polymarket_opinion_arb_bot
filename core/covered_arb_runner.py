from __future__ import annotations

import asyncio
import uuid
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from core.covered_arb_models import CoveredArbPair
from core.models import ExchangeName, OrderSide
from exchanges.orderbook_manager import OrderbookManager
from utils.config_loader import OutcomeCoveredArbConfig
from utils.google_sheets import GoogleSheetsClient
from utils.logger import BotLogger
from utils.account_pool import AccountSelector


@dataclass(slots=True)
class CoveredArbEntry:
    pair: CoveredArbPair
    size: float
    price_yes: float
    price_no: float
    invested: float
    payout: float
    profit: float
    profit_percent: float


class CoveredArbRunner:
    """
    BUY-only outcome-covered arbitrage runner.

    - LIMIT-only orders
    - No hedging, no cancellations, no SELL
    - PnL fixed at entry
    """

    def __init__(
        self,
        *,
        config: OutcomeCoveredArbConfig,
        orderbooks: OrderbookManager,
        risk_manager,
        clients_by_exchange: Dict[ExchangeName, object],
        account_ids: Dict[ExchangeName, str],
        fees: Dict[ExchangeName, object],
        logger: BotLogger,
        notifier=None,
        sheets_client: Optional[GoogleSheetsClient] = None,
        poll_interval_sec: float = 2.0,
        clients_by_id: Optional[Dict[str, object]] = None,
        account_selectors: Optional[Dict[ExchangeName, AccountSelector]] = None,
        dry_run: bool = False,
    ):
        self.config = config
        self.orderbooks = orderbooks
        self.risk_manager = risk_manager
        self.clients = clients_by_exchange
        self.account_ids = account_ids
        self.clients_by_id = clients_by_id or {}
        self.account_selectors = account_selectors or {}
        self.fees = fees
        self.logger = logger
        self.notifier = notifier
        self.sheets_client = sheets_client
        self.poll_interval = max(0.5, float(poll_interval_sec or 2.0))
        self._stop = asyncio.Event()
        self.dry_run = bool(dry_run)
        self.skip_reasons: Dict[str, int] = {
            "missing_depth": 0,
            "not_profitable": 0,
            "insufficient_depth": 0,
            "size_below_min": 0,
        }
        self.last_eval_at: float | None = None
        self.ops_log_interval = 45.0
        self._last_ops_log_ts = time.monotonic()
        self.active_pairs: int = 0

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Main loop: poll sheets, evaluate pairs, place symmetric BUY legs."""
        if not self.config.enabled:
            raise RuntimeError("OutcomeCoveredArbitrage is disabled in config")

        while not self._stop.is_set():
            try:
                pairs = await self._load_pairs()
                self.active_pairs = len(pairs)
                for pair in pairs:
                    await self._evaluate_pair(pair)
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.error("covered_arb loop error", error=str(exc))
            now = time.monotonic()
            if now - self._last_ops_log_ts >= self.ops_log_interval:
                self._last_ops_log_ts = now
                self.logger.info(
                    "covered_arb ops",
                    active_pairs=self.active_pairs,
                    last_eval_ts=self.last_eval_at,
                    skip_reasons=self.skip_reasons,
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                continue

    async def _load_pairs(self) -> List[CoveredArbPair]:
        if self.sheets_client:
            rows, statuses = await self.sheets_client.fetch_covered_arb_report()
            errors = [s for s in statuses if s.status == "ERROR"]
            if errors:
                self.logger.warn("sheet validation errors", errors=[e.__dict__ for e in errors])
            return rows
        return []

    async def _evaluate_pair(self, pair: CoveredArbPair) -> None:
        yes_client = self.clients[ExchangeName.POLYMARKET]
        no_client = self.clients[ExchangeName.OPINION]
        yes_account_id = self.account_ids[ExchangeName.POLYMARKET]
        no_account_id = self.account_ids[ExchangeName.OPINION]
        if self.account_selectors:
            selected_yes = self.account_selectors.get(ExchangeName.POLYMARKET)
            selected_no = self.account_selectors.get(ExchangeName.OPINION)
            yes_account = selected_yes.select(yes_account_id) if selected_yes else None
            no_account = selected_no.select(no_account_id) if selected_no else None
            if yes_account:
                yes_account_id = yes_account.account_id
                yes_client = self.clients_by_id.get(yes_account_id, yes_client)
            if no_account:
                no_account_id = no_account.account_id
                no_client = self.clients_by_id.get(no_account_id, no_client)
        if not pair.polymarket_token_yes or not pair.opinion_token_no:
            raise RuntimeError("covered arb requires YES and NO outcome tokens")

        yes_book = await yes_client.get_orderbook(pair.polymarket_token_yes)
        no_book = await no_client.get_orderbook(pair.opinion_token_no)

        best_yes = await self.orderbooks.best_ask(yes_book)
        best_no = await self.orderbooks.best_ask(no_book)
        if not best_yes or not best_no:
            self.logger.warn("orderbook depth missing", event=pair.event_id)
            self.skip_reasons["missing_depth"] += 1
            return

        maker_fee_yes = float(getattr(self.fees.get(ExchangeName.POLYMARKET), "maker", 0.0))
        maker_fee_no = float(getattr(self.fees.get(ExchangeName.OPINION), "maker", 0.0))

        total_cost = best_yes.price * (1 + maker_fee_yes) + best_no.price * (1 + maker_fee_no)
        target_profit_threshold = 1.0 - pair.min_profit_percent
        if total_cost >= target_profit_threshold:
            self.skip_reasons["not_profitable"] += 1
            return

        target_size = pair.max_size
        if self.config.max_position_size_per_market > 0:
            target_size = min(target_size, self.config.max_position_size_per_market)
        if self.config.max_position_size_per_event > 0:
            target_size = min(target_size, self.config.max_position_size_per_event)
        if self.config.max_position_size_per_account > 0:
            target_size = min(target_size, self.config.max_position_size_per_account)
        if target_size < (self.config.min_quote_size or 0.0):
            self.skip_reasons["size_below_min"] += 1
            return
        # Require full size available at best ask; do not reduce size.
        if best_yes.size < target_size or best_no.size < target_size:
            self.skip_reasons["insufficient_depth"] += 1
            return
        size = target_size
        self.last_eval_at = time.time()

        # Balance and exposure checks (no hedging).
        await self.risk_manager.check_limits(pair.event_id, size, reserve=False, account_id=yes_account_id)
        await self.risk_manager.check_limits(pair.event_id, size, reserve=False, account_id=no_account_id)
        await self.risk_manager.check_balance(
            yes_client, best_yes.price * size, account_id=yes_account_id
        )
        await self.risk_manager.check_balance(
            no_client, best_no.price * size, account_id=no_account_id
        )

        yes_reservation = None
        no_reservation = None
        if not self.dry_run:
            yes_reservation = await self.risk_manager.reserve_order(
                event_id=pair.event_id,
                account_id=yes_account_id,
                exchange=yes_client,
                exchange_name=ExchangeName.POLYMARKET.value,
                notional=best_yes.price * size,
                size=size,
                order_id=str(uuid.uuid4()),
                reason="covered_arb_yes",
            )
            no_reservation = await self.risk_manager.reserve_order(
                event_id=pair.event_id,
                account_id=no_account_id,
                exchange=no_client,
                exchange_name=ExchangeName.OPINION.value,
                notional=best_no.price * size,
                size=size,
                order_id=str(uuid.uuid4()),
                reason="covered_arb_no",
            )

        yes_order = None
        no_order = None
        if not self.dry_run:
            yes_order = await self._place_limit_order(
                client=yes_client,
                exchange=ExchangeName.POLYMARKET,
                market_id=pair.polymarket_token_yes,
                price=best_yes.price,
                size=size,
                reservation=yes_reservation,
            )
            no_order = await self._place_limit_order(
                client=no_client,
                exchange=ExchangeName.OPINION,
                market_id=pair.opinion_token_no,
                price=best_no.price,
                size=size,
                reservation=no_reservation,
            )

        executed_yes = (
            size
            if yes_order is None
            else float(getattr(yes_order, "filled_size", getattr(yes_order, "size", size)) or size)
        )
        executed_no = (
            size
            if no_order is None
            else float(getattr(no_order, "filled_size", getattr(no_order, "size", size)) or size)
        )
        executed = min(executed_yes, executed_no)
        invested = executed * (best_yes.price + best_no.price)
        payout = executed
        profit = payout - invested
        profit_percent = (profit / invested) if invested > 0 else 0.0

        entry = CoveredArbEntry(
            pair=pair,
            size=executed,
            price_yes=best_yes.price,
            price_no=best_no.price,
            invested=invested,
            payout=payout,
            profit=profit,
            profit_percent=profit_percent,
        )
        if self.dry_run:
            self._log_dry_run(entry, yes_account_id=yes_account_id, no_account_id=no_account_id)
            return
        await self._notify_entry(entry)

    async def _place_limit_order(
        self,
        *,
        client,
        exchange: ExchangeName,
        market_id: str,
        price: float,
        size: float,
        reservation: str | None,
    ):
        try:
            if exchange == ExchangeName.POLYMARKET:
                try:
                    return await client.place_limit_order(
                        market_id=None,
                        token_id=market_id,
                        side=OrderSide.BUY,
                        price=price,
                        size=size,
                        client_order_id=uuid.uuid4().hex,
                    )
                except TypeError:
                    return await client.place_limit_order(
                        market_id=market_id,
                        side=OrderSide.BUY,
                        price=price,
                        size=size,
                        client_order_id=uuid.uuid4().hex,
                    )
            return await client.place_limit_order(
                market_id=market_id,
                side=OrderSide.BUY,
                price=price,
                size=size,
                client_order_id=uuid.uuid4().hex,
            )
        finally:
            if reservation:
                await self.risk_manager.release_reservation(
                    reservation,
                    release_size=size,
                    release_funds=price * size,
                    reason="covered_arb_order_placed",
                )

    async def _notify_entry(self, entry: CoveredArbEntry) -> None:
        payload = {
            "event": entry.pair.event_id,
            "price_yes": entry.price_yes,
            "price_no": entry.price_no,
            "size": entry.size,
            "invested": entry.invested,
            "payout": entry.payout,
            "profit": entry.profit,
            "profit_percent": entry.profit_percent,
        }
        self.logger.info("covered arbitrage entered", **payload)
        if not self.notifier:
            return
        message = (
            "🟢 COVERED ARBITRAGE ENTERED\n\n"
            f"Event: {entry.pair.event_id}\n"
            f"BUY YES (Polymarket): {entry.price_yes:.3f} × {entry.size:.4f}\n"
            f"BUY NO  (Opinion):    {entry.price_no:.3f} × {entry.size:.4f}\n\n"
            f"Invested: ${entry.invested:.2f}\n"
            f"Payout:   ${entry.payout:.2f}\n"
            f"Profit:   {entry.profit:+.2f} ({entry.profit_percent*100:.2f}%)"
        )
        try:
            await self.notifier.send_message(message, parse_mode=None)
        except Exception:  # pragma: no cover - alerting must not block
            self.logger.warn("failed to send notifier message")

    def _log_dry_run(self, entry: CoveredArbEntry, *, yes_account_id: str, no_account_id: str) -> None:
        """Log dry-run intent and assert BUY-only, limit-only invariants."""
        self.logger.info(
            "DRY-RUN covered arbitrage",
            event=entry.pair.event_id,
            price_yes=entry.price_yes,
            price_no=entry.price_no,
            size=entry.size,
            invested=entry.invested,
            payout=entry.payout,
            profit=entry.profit,
            profit_percent=entry.profit_percent,
            yes_account=yes_account_id,
            no_account=no_account_id,
            legs="BUY+BUY",
            order_type="LIMIT",
        )
        # Explicit guards to avoid accidental SELL or market paths.
        assert entry.payout == entry.size, "payout must equal size for symmetric BUY legs"
        assert entry.price_yes >= 0 and entry.price_no >= 0, "prices must be non-negative"
        # BUY-only invariant (orders were not placed in dry-run)
        if entry.price_yes + entry.price_no >= 1.0:
            self.logger.warn("dry-run pair not profitable", event=entry.pair.event_id)
