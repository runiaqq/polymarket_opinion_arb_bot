from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Optional, Tuple

from core.exceptions import HedgingError, MarketHedgeInvariantError, RiskCheckError
from core.polymarket_resolver import PolymarketOutcomeResolver
from core.models import (
    DoubleLimitState,
    ExchangeName,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)
from exchanges.polymarket_api import PolymarketInvalidOrderPayload
from core.order_fsm import OrderFSMEvent, OrderFSMState, OrderStateMachine
from core.hedger import HedgeExecutionStatus, HedgeLegRequest
from core.market_hedge_invariant import HedgeInvariantContext
from core.market_mapper import MarketMapper
from models.validators import validate_order, validate_fill
from utils.log_hooks import LogHooks
from utils.logger import BotLogger
from utils.config_loader import MarketPairConfig

CANCEL_RETRY_ATTEMPTS = 3
CANCEL_BACKOFF_BASE = 0.5
CANCEL_FAILURE_ALERT_THRESHOLD = 3
from utils.config_loader import MarketPairConfig


class OrderManager:
    """Coordinates order placement and lifecycle tracking."""

    def __init__(
        self,
        exchanges: Dict[ExchangeName, object],
        database,
        position_tracker,
        hedger,
        risk_manager,
        hedge_invariant=None,
        logger: BotLogger | None = None,
        dry_run: bool = False,
        event_id: str | None = None,
        pair: MarketPairConfig | None = None,
        market_map: Dict[ExchangeName, str] | None = None,
        mapper: Optional[MarketMapper] = None,
        double_limit_enabled: bool = False,
        cancel_after_ms: Optional[int] = None,
        account_map: Dict[ExchangeName, str] | None = None,
        polymarket_resolver: PolymarketOutcomeResolver | None = None,
    ):
        self.exchanges = exchanges
        self.db = database
        self.position_tracker = position_tracker
        self.hedger = hedger
        self.risk_manager = risk_manager
        self.hedge_invariant = hedge_invariant
        self.logger = logger or BotLogger(__name__)
        self.dry_run = dry_run
        self._locks = {name: asyncio.Lock() for name in exchanges}
        self.double_limit_enabled = double_limit_enabled
        self.primary = None
        self.secondary = None
        self.event_id = event_id
        self.pair_cfg = pair
        self.market_map = market_map or {}
        self.mapper = mapper
        self._fill_lock = asyncio.Lock()
        self._processed_fills: set[str] = set()
        self._shutdown = asyncio.Event()
        self._fsms: Dict[str, OrderStateMachine] = {}
        self._order_sizes: Dict[str, float] = {}
        self._order_costs: Dict[str, float] = {}
        self._fill_progress: Dict[str, float] = {}
        self._order_exchanges: Dict[str, ExchangeName] = {}
        self.log_hooks = LogHooks()
        self._double_limit_locks: Dict[str, asyncio.Lock] = {}
        self._double_limit_records: Dict[str, Dict[str, object]] = {}
        self._double_limit_index: Dict[str, str] = {}
        self._cancel_tasks: Dict[str, asyncio.Task] = {}
        self._cancel_after_ms = cancel_after_ms
        self.cancel_retry_attempts = CANCEL_RETRY_ATTEMPTS
        self._cancel_backoff_base = CANCEL_BACKOFF_BASE
        self._cancel_failure_count = 0
        self._cancel_alert_threshold = CANCEL_FAILURE_ALERT_THRESHOLD
        self.account_map = account_map or {}
        self._order_reservations: Dict[str, str] = {}
        self._reservation_meta: Dict[str, Dict[str, float]] = {}
        self.polymarket_resolver = polymarket_resolver

    def set_routing(self, primary: ExchangeName, secondary: ExchangeName) -> None:
        self.primary = primary
        self.secondary = secondary

    async def place_primary_limit(
        self,
        exchange_name: ExchangeName,
        market_id: str,
        side: OrderSide,
        price: float,
        size: float,
        client_order_id: str | None = None,
    ) -> Order | None:
        exchange = self.exchanges[exchange_name]
        account_id = self.account_map.get(exchange_name)
        if exchange_name == ExchangeName.POLYMARKET and side == OrderSide.SELL:
            await self._assert_polymarket_sell_allowed(size)
        async with self._locks[exchange_name]:
            client_order_id = client_order_id or str(uuid.uuid4())
            reservation_id = None
            notional = price * size
            if not account_id:
                self.logger.warn("account mapping missing", exchange=exchange_name.value)
            elif hasattr(self.risk_manager, "reserve_order"):
                try:
                    reservation_id = await self.risk_manager.reserve_order(
                        event_id=self.event_id or market_id,
                        account_id=account_id or "unknown",
                        exchange=exchange,
                        exchange_name=exchange_name.value,
                        notional=notional,
                        size=size,
                        order_id=client_order_id,
                        reason="entry_order",
                    )
                except Exception:
                    # propagate but ensure reservation does not stay if partially created
                    raise
            if self.dry_run:
                order = self._build_dry_order(
                    exchange_name, market_id, side, price, size, client_order_id
                )
            else:
                try:
                    if exchange_name == ExchangeName.POLYMARKET:
                        try:
                            order = await exchange.place_limit_order(
                                market_id=None,
                                token_id=market_id,
                                side=side,
                                price=price,
                                size=size,
                                client_order_id=client_order_id,
                            )
                        except TypeError:
                            order = await exchange.place_limit_order(
                                market_id=market_id,
                                side=side,
                                price=price,
                                size=size,
                                client_order_id=client_order_id,
                            )
                    else:
                        order = await exchange.place_limit_order(
                            market_id=market_id,
                            side=side,
                            price=price,
                            size=size,
                            client_order_id=client_order_id,
                        )
                except PolymarketInvalidOrderPayload:
                    handled = await self._handle_polymarket_invalid_payload(market_id)
                    if handled:
                        try:
                            try:
                                order = await exchange.place_limit_order(
                                    market_id=None,
                                    token_id=handled,
                                    side=side,
                                    price=price,
                                    size=size,
                                    client_order_id=client_order_id,
                                )
                            except TypeError:
                                order = await exchange.place_limit_order(
                                    market_id=handled,
                                    side=side,
                                    price=price,
                                    size=size,
                                    client_order_id=client_order_id,
                                )
                            market_id = handled
                        except Exception:
                            if reservation_id:
                                with suppress(Exception):
                                    await self.risk_manager.release_reservation(
                                        reservation_id,
                                        reason="place_failed",
                                    )
                            raise
                    else:
                        if reservation_id:
                            with suppress(Exception):
                                await self.risk_manager.release_reservation(
                                    reservation_id,
                                    reason="place_failed",
                                )
                        raise
                except Exception:
                    if reservation_id:
                        with suppress(Exception):
                            await self.risk_manager.release_reservation(
                                reservation_id,
                                reason="place_failed",
                            )
                    raise
            validate_order(order)
            await self.db.save_order(order)
            order_key = order.order_id or order.client_order_id
            self._order_sizes[order_key] = order.size
            self._order_costs[order_key] = notional
            self._fill_progress.setdefault(order_key, 0.0)
            self._order_exchanges[order_key] = exchange_name
            if reservation_id:
                self._order_reservations[order_key] = reservation_id
                if order.order_id:
                    self._order_reservations.setdefault(order.order_id, reservation_id)
                if order.client_order_id:
                    self._order_reservations.setdefault(order.client_order_id, reservation_id)
                self._reservation_meta[reservation_id] = {
                    "size": size,
                    "funds": notional,
                    "remaining_size": size,
                    "remaining_funds": notional,
                }
            fsm = OrderStateMachine(order_key, self.db, logger=self.logger)
            self._fsms[order_key] = fsm
            await fsm.transition(
                OrderFSMEvent.PLACE,
                payload=order,
                event_id=f"place-{order_key}",
            )
            await self.log_hooks.emit(
                "order_state",
                {
                    "order_id": order_key,
                    "state": fsm.current_state.value,
                    "market_id": market_id,
                    "exchange": exchange_name.value,
                    "size": size,
                    "price": price,
                    "side": side.value,
                    "event_id": self.event_id,
                },
            )
            self.logger.info(
                "limit order placed",
                order_id=order_key,
                market_id=market_id,
                exchange=exchange_name.value,
            )
            await self._schedule_cancel(order_key, exchange_name)
            return order

    async def _handle_polymarket_invalid_payload(self, market_id: str) -> str | None:
        if not self.polymarket_resolver or not self.event_id:
            return None
        try:
            self.polymarket_resolver.invalidate(self.event_id)
            refreshed = await self.polymarket_resolver.refresh(self.event_id)
        except Exception as exc:
            self.logger.warn(
                "polymarket token refresh failed",
                event_id=self.event_id,
                error=str(exc),
            )
            return None
        if not refreshed or not refreshed.token_id:
            return None
        new_token = refreshed.token_id
        if self.primary == ExchangeName.POLYMARKET:
            self.market_map[self.primary] = new_token
        if self.secondary == ExchangeName.POLYMARKET:
            self.market_map[self.secondary] = new_token
        if self.pair_cfg:
            if self.primary == ExchangeName.POLYMARKET:
                self.pair_cfg.primary_market_id = new_token
            if self.secondary == ExchangeName.POLYMARKET:
                self.pair_cfg.secondary_market_id = new_token
            try:
                self.pair_cfg.polymarket_token_id = new_token  # type: ignore[attr-defined]
            except Exception:
                pass
        self.logger.warn(
            "polymarket token refreshed after invalid payload",
            event_id=self.event_id,
            previous_token=market_id,
            new_token=new_token,
        )
        return new_token

    async def place_double_limit(
        self,
        account: str | None,
        pair: MarketPairConfig | None,
        price_a: float,
        size_a: float,
        price_b: float,
        size_b: float,
        side_a: OrderSide = OrderSide.BUY,
        side_b: OrderSide = OrderSide.BUY,
    ) -> Tuple[str, str]:
        if not self.double_limit_enabled:
            raise RuntimeError("double limit placement attempted while disabled")
        if not self.primary or not self.secondary:
            raise RuntimeError("exchange routing not configured for double limit placement")
        if (self.primary == ExchangeName.POLYMARKET and side_a == OrderSide.SELL) or (
            self.secondary == ExchangeName.POLYMARKET and side_b == OrderSide.SELL
        ):
            raise RiskCheckError("polymarket SELL orders are not allowed for double limit placement")
        primary_market = self._resolve_pair_market(pair, self.primary)
        secondary_market = self._resolve_pair_market(pair, self.secondary)
        if not primary_market or not secondary_market:
            raise ValueError("missing market identifiers for double limit order")
        suffix = uuid.uuid4().hex
        primary_client_id = self._build_client_order_id(self.primary, suffix)
        secondary_client_id = self._build_client_order_id(self.secondary, suffix)
        primary_order = await self.place_primary_limit(
            self.primary,
            primary_market,
            side_a,
            price_a,
            size_a,
            client_order_id=primary_client_id,
        )
        if primary_order is None:
            raise RuntimeError("primary exchange did not return order for double limit placement")
        try:
            secondary_order = await self.place_primary_limit(
                self.secondary,
                secondary_market,
                side_b,
                price_b,
                size_b,
                client_order_id=secondary_client_id,
            )
        except Exception as exc:
            await self._attempt_cancel(self.primary, primary_order)
            raise
        if secondary_order is None:
            await self._attempt_cancel(self.primary, primary_order)
            raise RuntimeError("secondary exchange did not return order for double limit placement")

        record_id = uuid.uuid4().hex
        primary_ref = primary_order.order_id or primary_order.client_order_id
        secondary_ref = secondary_order.order_id or secondary_order.client_order_id
        pair_key = self._derive_pair_key(pair)
        await self.db.save_double_limit_pair(
            record_id=record_id,
            pair_key=pair_key,
            primary_order_ref=primary_ref,
            secondary_order_ref=secondary_ref,
            primary_exchange=self.primary.value,
            secondary_exchange=self.secondary.value,
            primary_client_order_id=primary_order.client_order_id,
            secondary_client_order_id=secondary_order.client_order_id,
        )
        self._double_limit_locks.setdefault(record_id, asyncio.Lock())
        self._cache_double_limit_record(
            {
                "id": record_id,
                "pair_key": pair_key,
                "order_a_ref": primary_ref,
                "order_b_ref": secondary_ref,
                "order_a_exchange": self.primary.value,
                "order_b_exchange": self.secondary.value,
                "client_order_id_a": primary_order.client_order_id,
                "client_order_id_b": secondary_order.client_order_id,
                "state": DoubleLimitState.ACTIVE.value,
            }
        )
        await self._promote_order_to_double_limit(primary_ref)
        await self._promote_order_to_double_limit(secondary_ref)
        await self.log_hooks.emit(
            "double_limit_placed",
            {
                "record_id": record_id,
                "pair_key": pair_key,
                "account": account,
                "primary_order_id": primary_ref,
                "secondary_order_id": secondary_ref,
                "event_id": self.event_id,
            },
        )
        self.logger.info(
            "double limit orders placed",
            record_id=record_id,
            account=account,
            pair_key=pair_key,
        )
        return primary_order.client_order_id, secondary_order.client_order_id

    async def track_fills(self, exchange_name: ExchangeName) -> None:
        raise RuntimeError("track_fills is handled by Reconciler.")

    async def poll_fills(self, exchange_name: ExchangeName, interval: float) -> None:
        raise RuntimeError("poll_fills is handled by Reconciler.")

    async def cancel_limit(self, exchange_name: ExchangeName, order_id: str) -> bool:
        exchange = self.exchanges[exchange_name]
        async with self._locks[exchange_name]:
            if self.dry_run:
                await self._after_cancel(order_id)
                return True
            fsm = self._fsms.get(order_id)
            if fsm:
                await fsm.transition(
                    OrderFSMEvent.CANCEL_REQUEST,
                    event_id=f"cancel-req-{order_id}",
                )
            await exchange.cancel_order(order_id)
            if fsm:
                await fsm.transition(
                    OrderFSMEvent.CANCEL_ACK,
                    event_id=f"cancel-ack-{order_id}",
                )
            else:
                await self.db.update_order_status(order_id, OrderStatus.CANCELED)
            await self._after_cancel(order_id)
            self.logger.info(
                "limit order cancelled",
                exchange=exchange_name.value,
                order_id=order_id,
            )
            return True

    async def handle_fill(self, exchange_name: ExchangeName, fill: Fill) -> Optional[str]:
        validate_fill(fill)
        if not await self._mark_fill_processed(fill):
            return None
        double_record: Optional[Dict[str, object]] = None
        double_record_id: Optional[str] = None
        if self.double_limit_enabled:
            double_record = await self._get_double_limit_record(fill.order_id)
            if double_record and double_record.get("id"):
                double_record_id = str(double_record["id"])
        event_id = (
            (double_record.get("pair_key") if double_record else None)
            or self.event_id
            or fill.market_id
        )
        await self.db.update_order_fill(fill.order_id, Decimal(str(fill.size)), fill)
        fsm = self._get_or_create_fsm(fill.order_id)
        progress = self._fill_progress.get(fill.order_id, 0.0) + fill.size
        target = self._order_sizes.get(fill.order_id)
        is_full = target is not None and progress >= target - 1e-9
        event = OrderFSMEvent.FILL_FULL if is_full else OrderFSMEvent.FILL_PARTIAL
        self._fill_progress[fill.order_id] = target if is_full and target is not None else progress
        await self.log_hooks.emit(
            "fill_consumed",
            {
                "order_id": fill.order_id,
                "exchange": exchange_name.value,
                "market_id": fill.market_id,
                "size": fill.size,
                "price": fill.price,
                "target": target,
                "progress": progress,
                "is_full": is_full,
                "side": fill.side.value,
                "event_id": event_id,
            },
        )
        await fsm.transition(
            event,
            payload=fill,
            event_id=f"fill-{fill.order_id}-{fill.timestamp.isoformat()}",
        )
        await self.position_tracker.add_fill(event_id, fill.size, fill.price, fill.side)
        await self._record_sequence_event(
            fill.order_id,
            "fill",
            {
                "exchange": exchange_name.value,
                "market_id": fill.market_id,
                "size": fill.size,
                "price": fill.price,
                "is_full": is_full,
            },
        )

        counter_order_id: Optional[str] = None
        counter_exchange: Optional[ExchangeName] = None
        if self.double_limit_enabled:
            counter = await self._prepare_double_limit_cancel(exchange_name, fill)
            if counter:
                counter_order_id, counter_exchange, double_record_id = counter
            if double_record_id and not double_record:
                double_record = self._double_limit_records.get(double_record_id)
            if double_record and double_record.get("pair_key"):
                event_id = double_record.get("pair_key") or event_id

        cancel_summary = {
            "attempted": bool(counter_order_id and counter_exchange),
            "order_id": counter_order_id,
            "exchange": counter_exchange.value if counter_exchange else None,
            "double_limit_id": double_record_id,
        }
        if counter_order_id and counter_exchange:
            success, attempts, error = await self._cancel_with_retry(
                fill.order_id,
                counter_exchange,
                counter_order_id,
            )
            cancel_summary.update(
                {
                    "success": success,
                    "attempts": attempts,
                    "error": error,
                    "double_limit_id": double_record_id,
                }
            )
        else:
            cancel_summary["skipped"] = True
        await self._record_sequence_event(fill.order_id, "cancel_result", cancel_summary)

        hedge_exchange_name = self.secondary if exchange_name == self.primary else self.primary
        if hedge_exchange_name is None:
            self.logger.warn("hedge exchange not configured")
            return
        hedge_side = self._determine_hedge_side(fill.side, hedge_exchange_name)
        if hedge_exchange_name == ExchangeName.POLYMARKET and hedge_side == OrderSide.SELL:
            raise MarketHedgeInvariantError("polymarket hedge cannot be SELL")
        hedge_exchange = self.exchanges[hedge_exchange_name]
        hedge_market_id = self._resolve_market_id(exchange_name, hedge_exchange_name, fill.market_id)
        hedge_payload = {
            "hedge_exchange": hedge_exchange_name.value,
            "market_id": hedge_market_id,
            "size": fill.size,
            "side": hedge_side.value,
            "double_limit_id": double_record_id,
            "pair_key": double_record.get("pair_key") if double_record else None,
            "order_id": fill.order_id,
            "event_id": event_id,
        }
        self.logger.info(
            "hedge triggered",
            source_exchange=exchange_name.value,
            order_id=fill.order_id,
            fill_id=getattr(fill, "fill_id", None),
            market_id=fill.market_id,
            hedge_exchange=hedge_exchange_name.value,
            hedge_market_id=hedge_market_id,
            size=fill.size,
            side=hedge_side.value,
        )
        context = HedgeInvariantContext(
            event_id=event_id,
            fill_id=str(getattr(fill, "fill_id", None) or fill.order_id),
            fill_size=fill.size,
            hedge_side=hedge_side,
            source_exchange=exchange_name,
            hedge_exchange=hedge_exchange_name,
            reference_price=fill.price,
        )

        async def _hedge_call():
            return await self.hedger.hedge(
                legs=[
                    HedgeLegRequest(
                        client=hedge_exchange,
                        exchange=hedge_exchange_name,
                        market_id=hedge_market_id,
                        account_id=self.account_map.get(hedge_exchange_name),
                    )
                ],
                event_id=event_id,
                side=hedge_side,
                size=fill.size,
                reference_price=fill.price,
                entry_order_id=fill.order_id,
                entry_exchange=exchange_name,
            )

        try:
            result, hedge_attempts, residual_unhedged = await self._execute_hedge(context, _hedge_call)
            hedged_size = getattr(result, "executed_size", fill.size if result else 0.0)
            status_obj = getattr(result, "status", HedgeExecutionStatus.HEDGE_OK)
            status_value = (
                status_obj.value if isinstance(status_obj, HedgeExecutionStatus) else str(status_obj)
            )
            hedge_payload.update(
                {
                    "status": status_value,
                    "legs": len(result or []),
                    "hedged_size": hedged_size,
                    "requested_size": fill.size,
                    "unhedged_size": residual_unhedged,
                    "attempts": hedge_attempts,
                    "avg_price": getattr(result, "weighted_price", None),
                    "pnl_estimate": getattr(result, "pnl_estimate", None),
                    "pnl_realized": getattr(result, "realized_pnl", None),
                    "message": getattr(result, "message", None),
                }
            )
            hedge_avg_price = hedge_payload.get("avg_price")
            if hedge_avg_price is not None:
                realized_spread = (
                    hedge_avg_price - fill.price if hedge_side == OrderSide.SELL else fill.price - hedge_avg_price
                )
                hedge_payload["realized_spread_per_unit"] = realized_spread
                hedge_payload["realized_spread_total"] = realized_spread * fill.size
            if status_obj == HedgeExecutionStatus.HEDGE_REDUCED:
                hedge_payload["note"] = "hedge_reduced"
            if status_obj != HedgeExecutionStatus.HEDGE_FAILED:
                await self._release_reservation_for_order(
                    fill.order_id,
                    release_size=hedged_size,
                    reason="hedged",
                )
            await self.log_hooks.emit("hedge_requested", hedge_payload)
        except MarketHedgeInvariantError as exc:
            status_value = HedgeExecutionStatus.HEDGE_FAILED.value
            self.logger.error("hedging failed invariant", error=str(exc))
            hedge_payload.update(
                {
                    "status": status_value,
                    "error": str(exc),
                    "attempts": getattr(exc, "attempts", None),
                    "unhedged_size": getattr(exc, "residual_unhedged", fill.size),
                }
            )
            await self._record_sequence_event(fill.order_id, "hedge", hedge_payload)
            await self.log_hooks.emit("hedge_requested", hedge_payload)
            raise
        except Exception as exc:
            status_value = HedgeExecutionStatus.HEDGE_FAILED.value
            self.logger.error("hedging failed", error=str(exc))
            hedge_payload.update(
                {
                    "status": status_value,
                    "error": str(exc),
                }
            )
            try:
                await self._send_alert(
                    f"[{status_value}] Hedge failed for {fill.order_id} on {hedge_exchange_name.value}: {exc}"
                )
            except Exception:
                # alerting should never block downstream handling
                pass
            with suppress(Exception):
                await self.cancel_all_open_orders()
            await self.log_hooks.emit("hedge_requested", hedge_payload)
        await self._record_sequence_event(fill.order_id, "hedge", hedge_payload)
        if hedge_payload.get("status") != HedgeExecutionStatus.HEDGE_FAILED.value:
            self.logger.info(
                "market hedge executed",
                event_id=event_id,
                entry_exchange=exchange_name.value,
                hedge_exchange=hedge_exchange_name.value,
                entry_side=fill.side.value,
                hedge_side=hedge_side.value,
                entry_price=fill.price,
                hedge_price=hedge_payload.get("avg_price"),
                realized_spread=hedge_payload.get("realized_spread_per_unit"),
                size=fill.size,
            )
        if is_full:
            if double_record_id:
                await self._complete_double_limit(double_record_id)
            await self._clear_cancel_task(fill.order_id)
        return self._fill_key(fill)

    @staticmethod
    def normalize_fill(exchange_name: ExchangeName, message) -> Fill | None:
        if not isinstance(message, dict):
            return None
        data = message.get("data", message)
        if not isinstance(data, dict):
            return None
        order_id = str(data.get("order_id") or data.get("id"))
        if not order_id:
            return None
        price = float(data.get("price") or data.get("fill_price") or 0.0)
        size = float(
            data.get("size")
            or data.get("filled_size")
            or data.get("fill_size")
            or data.get("matchedAmount")
            or 0.0
        )
        side_raw = str(data.get("side", "BUY")).upper()
        side = OrderSide.BUY if side_raw == "BUY" else OrderSide.SELL
        timestamp = data.get("timestamp") or data.get("filled_at") or datetime.now(tz=timezone.utc)
        if isinstance(timestamp, (int, float)):
            ts_dt = datetime.fromtimestamp(
                timestamp / 1000 if timestamp > 10**12 else timestamp,
                tz=timezone.utc,
            )
        elif isinstance(timestamp, str):
            ts_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        else:
            ts_dt = datetime.now(tz=timezone.utc)
        fill_id_raw = (
            data.get("fill_id")
            or data.get("id")
            or data.get("trade_id")
            or data.get("transaction_hash")
            or data.get("tx_hash")
            or data.get("hash")
        )
        fill_id = str(fill_id_raw) if fill_id_raw is not None else None
        return Fill(
            order_id=order_id,
            market_id=str(data.get("market_id") or data.get("token_id")),
            exchange=exchange_name,
            side=side,
            price=price,
            size=size,
            fee=float(data.get("fee", 0.0)),
            timestamp=ts_dt,
            fill_id=fill_id,
        )

    async def _mark_fill_processed(self, fill: Fill) -> bool:
        ts_part = fill.timestamp.isoformat() if fill.timestamp else ""
        key_root = getattr(fill, "fill_id", None) or f"{fill.order_id}:{fill.size}"
        key = f"{key_root}:{ts_part}"
        async with self._fill_lock:
            if key in self._processed_fills:
                return False
            self._processed_fills.add(key)
            if len(self._processed_fills) > 10000:
                self._processed_fills.clear()
            return True

    def stop(self) -> None:
        self._shutdown.set()

    def _get_or_create_fsm(
        self,
        order_id: str,
        default_state: OrderFSMState = OrderFSMState.PLACED,
    ) -> OrderStateMachine:
        if order_id not in self._fsms:
            self._fsms[order_id] = OrderStateMachine(
                order_id,
                self.db,
                initial_state=default_state,
                logger=self.logger,
            )
        return self._fsms[order_id]
    def _build_dry_order(
        self,
        exchange_name: ExchangeName,
        market_id: str,
        side: OrderSide,
        price: float,
        size: float,
        client_order_id: str,
    ) -> Order:
        return Order(
            order_id=f"dry-{client_order_id}",
            client_order_id=client_order_id,
            market_id=market_id,
            exchange=exchange_name,
            side=side,
            order_type=OrderType.LIMIT,
            price=price,
            size=size,
            filled_size=0.0,
            status=OrderStatus.PENDING,
            created_at=datetime.now(tz=timezone.utc),
        )

    def _resolve_market_id(
        self,
        source_exchange: ExchangeName,
        target_exchange: ExchangeName,
        source_market_id: str,
    ) -> str:
        mapped: Optional[str] = None
        if self.mapper:
            if source_exchange == ExchangeName.POLYMARKET and target_exchange == ExchangeName.OPINION:
                mapped = self.mapper.find_opinion_for_polymarket(source_market_id)
            elif source_exchange == ExchangeName.OPINION and target_exchange == ExchangeName.POLYMARKET:
                mapped = self.mapper.find_polymarket_for_opinion(source_market_id)
        return mapped or self.market_map.get(target_exchange, source_market_id)

    def _determine_hedge_side(self, fill_side: OrderSide, hedge_exchange: ExchangeName) -> OrderSide:
        """
        Market-hedge rule: Polymarket hedge leg is always BUY.
        Opinion hedge leg is the directional opposite of the filled side.
        """
        if hedge_exchange == ExchangeName.POLYMARKET:
            return OrderSide.BUY
        return OrderSide.SELL if fill_side == OrderSide.BUY else OrderSide.BUY

    def _resolve_pair_market(
        self,
        pair: MarketPairConfig | None,
        exchange: ExchangeName,
    ) -> str | None:
        if pair:
            if exchange == self.primary:
                candidate = getattr(pair, "primary_market_id", None)
            elif exchange == self.secondary:
                candidate = getattr(pair, "secondary_market_id", None)
            else:
                candidate = None
            if candidate:
                return candidate
        return self.market_map.get(exchange)

    def _build_client_order_id(self, exchange: ExchangeName, suffix: str) -> str:
        prefix = exchange.value.lower()
        return f"dl-{prefix}-{suffix}"

    async def cancel_all_open_orders(self) -> None:
        cancellable_states = {
            OrderFSMState.PLACED,
            OrderFSMState.DOUBLE_LIMIT,
            OrderFSMState.PARTIALLY_FILLED,
        }
        tasks = []
        for order_id, exchange in list(self._order_exchanges.items()):
            fsm = self._fsms.get(order_id)
            if fsm and fsm.current_state in cancellable_states:
                tasks.append(self.cancel_limit(exchange, order_id))
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self.logger.warn("cancel_all_open_orders encountered error", error=str(result))
        await self._cancel_all_timers()

    async def _attempt_cancel(self, exchange_name: ExchangeName, order: Order | None) -> None:
        if not order:
            return
        order_id = order.order_id or order.client_order_id
        if not order_id:
            return
        try:
            await self.cancel_limit(exchange_name, order_id)
        except Exception as exc:
            self.logger.warn(
                "cleanup cancel failed",
                exchange=exchange_name.value,
                order_id=order_id,
                error=str(exc),
            )

    async def cancel_all_orders(self) -> None:
        pending = list(self._order_exchanges.items())
        for order_id, exchange in pending:
            try:
                await self.cancel_limit(exchange, order_id)
            except Exception as exc:
                self.logger.warn(
                    "cancel_all_orders failed",
                    exchange=exchange.value,
                    order_id=order_id,
                    error=str(exc),
                )

    async def _promote_order_to_double_limit(self, order_key: str | None) -> None:
        if not order_key:
            return
        fsm = self._fsms.get(order_key)
        if not fsm:
            return
        await fsm.transition(
            OrderFSMEvent.DOUBLE_LINKED,
            event_id=f"double-link-{order_key}",
        )

    def _derive_pair_key(self, pair: MarketPairConfig | None) -> str:
        if pair and getattr(pair, "event_id", None):
            return pair.event_id
        if self.event_id:
            return self.event_id
        primary_market = self.market_map.get(self.primary, "")
        secondary_market = self.market_map.get(self.secondary, "")
        return f"{primary_market}:{secondary_market}"

    def _ensure_double_limit_lock(self, record_id: str) -> asyncio.Lock:
        lock = self._double_limit_locks.get(record_id)
        if lock is None:
            lock = asyncio.Lock()
            self._double_limit_locks[record_id] = lock
        return lock

    def _cache_double_limit_record(self, record: Dict[str, object]) -> None:
        record_id = str(record.get("id") or "")
        if not record_id:
            return
        normalized = dict(record)
        self._double_limit_records[record_id] = normalized
        for key in [
            normalized.get("order_a_ref"),
            normalized.get("order_b_ref"),
            normalized.get("client_order_id_a"),
            normalized.get("client_order_id_b"),
        ]:
            if key:
                self._double_limit_index[str(key)] = record_id

    async def _get_double_limit_record(self, order_ref: str) -> Optional[Dict[str, object]]:
        record_id = self._double_limit_index.get(order_ref)
        if record_id:
            cached = self._double_limit_records.get(record_id)
            if cached:
                return cached
        record = await self.db.get_double_limit_by_order(order_ref)
        if record:
            self._cache_double_limit_record(record)
        return record

    def _update_cached_double_limit(self, record_id: str, **updates: object) -> None:
        if not record_id:
            return
        cached = self._double_limit_records.get(record_id)
        if cached is None:
            cached = {"id": record_id}
            self._double_limit_records[record_id] = cached
        for key, value in updates.items():
            if value is not None:
                cached[key] = value

    async def _complete_double_limit(self, record_id: str) -> None:
        if not record_id:
            return
        lock = self._ensure_double_limit_lock(record_id)
        async with lock:
            cached = self._double_limit_records.get(record_id)
            if cached and cached.get("state") == DoubleLimitState.COMPLETED.value:
                return
            await self.db.update_double_limit_state(record_id, DoubleLimitState.COMPLETED)
            self._update_cached_double_limit(record_id, state=DoubleLimitState.COMPLETED.value)

    def _counterparty_from_record(
        self,
        record: Dict[str, object],
        order_ref: str,
    ) -> Tuple[Optional[str], Optional[ExchangeName]]:
        if record.get("order_a_ref") == order_ref:
            exchange_value = record.get("order_b_exchange")
            try:
                exchange = ExchangeName(str(exchange_value))
            except ValueError:
                exchange = None
            other_ref = record.get("order_b_ref")
            return (str(other_ref) if other_ref is not None else None), exchange
        if record.get("order_b_ref") == order_ref:
            exchange_value = record.get("order_a_exchange")
            try:
                exchange = ExchangeName(str(exchange_value))
            except ValueError:
                exchange = None
            other_ref = record.get("order_a_ref")
            return (str(other_ref) if other_ref is not None else None), exchange
        return None, None

    async def _prepare_double_limit_cancel(
        self,
        exchange_name: ExchangeName,
        fill: Fill,
    ) -> Optional[Tuple[str, ExchangeName, str]]:
        record = await self._get_double_limit_record(fill.order_id)
        if not record or not record.get("id"):
            return None
        record_id = str(record["id"])
        lock = self._ensure_double_limit_lock(record_id)
        async with lock:
            latest = await self.db.get_double_limit_by_order(fill.order_id)
            if latest:
                self._cache_double_limit_record(latest)
            record_view = latest or record
            if record_view.get("state") != DoubleLimitState.ACTIVE.value:
                return None
            counter_order_id, counter_exchange = self._counterparty_from_record(record_view, fill.order_id)
            if not counter_order_id or not counter_exchange:
                return None
            self.logger.debug(
                "double limit trigger",
                record_id=record_view["id"],
                fill_exchange=exchange_name.value,
                trigger_order=fill.order_id,
            )
            await self.db.update_double_limit_state(
                record_id,
                DoubleLimitState.TRIGGERED,
                triggered_order_id=fill.order_id,
                cancelled_order_id=counter_order_id,
            )
            self._update_cached_double_limit(
                record_id,
                state=DoubleLimitState.TRIGGERED.value,
                triggered_order_id=fill.order_id,
                cancelled_order_id=counter_order_id,
            )
            return counter_order_id, counter_exchange, record_id

    async def _cancel_with_retry(
        self,
        source_order_id: str,
        exchange_name: ExchangeName,
        cancel_order_id: str,
    ) -> Tuple[bool, int, Optional[str]]:
        attempts = 0
        delay = self._cancel_backoff_base
        last_error: Optional[str] = None
        while attempts < self.cancel_retry_attempts:
            attempts += 1
            try:
                await self.log_hooks.emit(
                    "cancel_attempt",
                    {
                        "order_id": cancel_order_id,
                        "source_order_id": source_order_id,
                        "exchange": exchange_name.value,
                        "attempt": attempts,
                    },
                )
                success = await self.cancel_limit(exchange_name, cancel_order_id)
                if success:
                    return True, attempts, None
            except Exception as exc:
                last_error = str(exc)
                self.logger.warn(
                    "cancel attempt failed",
                    exchange=exchange_name.value,
                    order_id=cancel_order_id,
                    attempt=attempts,
                    error=last_error,
                )
            if attempts < self.cancel_retry_attempts:
                await asyncio.sleep(delay)
                delay *= 2
        await self._record_cancel_failure_incident(cancel_order_id, exchange_name, last_error)
        return False, attempts, last_error

    async def _record_cancel_failure_incident(
        self,
        order_id: str,
        exchange_name: ExchangeName,
        error: Optional[str],
    ) -> None:
        self._cancel_failure_count += 1
        await self.log_hooks.emit(
            "metric",
            {
                "name": "cancel_failures",
                "value": self._cancel_failure_count,
            },
        )
        if hasattr(self.db, "record_incident"):
            await self.db.record_incident(
                "WARNING",
                "cancel_failure",
                {
                    "order_id": order_id,
                    "exchange": exchange_name.value,
                    "error": error or "unknown",
                    "attempts": self.cancel_retry_attempts,
                },
            )
        if self._cancel_failure_count >= self._cancel_alert_threshold:
            await self._notify_cancel_threshold()

    async def _notify_cancel_threshold(self) -> None:
        message = (
            f"Cancel failures exceeded threshold ({self._cancel_alert_threshold}). "
            "Investigate exchange reliability."
        )
        await self._send_alert(message)
        self._cancel_failure_count = 0

    async def _send_alert(self, message: str) -> None:
        notifier = getattr(self.hedger, "notifier", None)
        if not notifier:
            return
        try:
            await notifier.send_message(message)
        except Exception as exc:
            self.logger.warn("alert send failed", error=str(exc))

    async def _record_sequence_event(self, order_id: str, stage: str, payload: Dict[str, object]) -> None:
        log_method = getattr(self.db, "log_order_event", None)
        if not log_method:
            return
        try:
            await log_method(order_id, stage, payload)
        except Exception as exc:
            self.logger.debug(
                "sequence log failed",
                order_id=order_id,
                stage=stage,
                error=str(exc),
            )

    def _fill_key(self, fill: Fill) -> str:
        return f"{fill.order_id}:{fill.timestamp.isoformat()}:{fill.size}"

    async def _assert_polymarket_sell_allowed(self, size: float) -> None:
        """
        Prevent naked sells on Polymarket. Only allow if there is an existing
        positive position large enough to cover the sell size.
        """
        if not self.event_id:
            raise RiskCheckError("polymarket sell blocked: missing event_id context")
        net_position = await self.position_tracker.get_net_position(self.event_id)
        if net_position + 1e-9 < size:
            raise RiskCheckError(
                f"polymarket sell blocked: insufficient position ({net_position:.6f}) for size {size:.6f}"
            )

    async def _execute_hedge(
        self,
        context: HedgeInvariantContext,
        hedge_callable: Callable[[], Awaitable[object]],
    ) -> tuple[object | None, int, float]:
        if self.hedge_invariant:
            return await self.hedge_invariant.enforce(
                context=context,
                hedge_callable=hedge_callable,  # type: ignore[arg-type]
                cancel_open_orders=self.cancel_all_open_orders,
            )
        result = await hedge_callable()
        hedged_size = float(getattr(result, "executed_size", 0.0) or 0.0)
        residual_unhedged = max(context.fill_size - hedged_size, 0.0)
        return result, 1, residual_unhedged

    async def _schedule_cancel(self, order_id: str, exchange: ExchangeName) -> None:
        if not self._cancel_after_ms or self.dry_run:
            return
        # avoid duplicate timers
        await self._clear_cancel_task(order_id)

        async def _wait_and_cancel():
            try:
                await asyncio.sleep(self._cancel_after_ms / 1000)
                fsm = self._fsms.get(order_id)
                if fsm and fsm.current_state in {
                    OrderFSMState.FILLED,
                    OrderFSMState.CANCELLED,
                    OrderFSMState.FAILED,
                }:
                    return
                await self._record_sequence_event(
                    order_id,
                    "cancel_timeout",
                    {"reason": "cancel_unfilled_after_ms", "ms": self._cancel_after_ms},
                )
                await self._send_alert(
                    f"Auto-cancel triggered for order {order_id} after {self._cancel_after_ms}ms"
                )
                await self.cancel_limit(exchange, order_id)
            except asyncio.CancelledError:
                return

        task = asyncio.create_task(_wait_and_cancel())
        self._cancel_tasks[order_id] = task

    async def _clear_cancel_task(self, order_id: str) -> None:
        task = self._cancel_tasks.pop(order_id, None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _cancel_all_timers(self) -> None:
        tasks = list(self._cancel_tasks.values())
        self._cancel_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _release_reservation_for_order(
        self,
        order_id: str,
        *,
        release_size: float | None = None,
        reason: str,
    ) -> None:
        reservation_id = self._order_reservations.get(order_id)
        if not reservation_id:
            return
        meta = self._reservation_meta.get(reservation_id)
        if not meta:
            return
        remaining_size = meta.get("remaining_size", meta.get("size", 0.0))
        remaining_funds = meta.get("remaining_funds", meta.get("funds", 0.0))
        if remaining_size <= 0 and remaining_funds <= 0:
            return
        if release_size is None:
            size_to_release = remaining_size
        else:
            size_to_release = min(release_size, remaining_size)
        if size_to_release <= 0:
            return
        per_unit = (meta.get("funds", 0.0) / meta.get("size", size_to_release)) if meta.get("size", 0.0) > 0 else 0.0
        release_funds = min(remaining_funds, per_unit * size_to_release)
        meta["remaining_size"] = max(0.0, remaining_size - size_to_release)
        meta["remaining_funds"] = max(0.0, remaining_funds - release_funds)
        await self.risk_manager.release_reservation(
            reservation_id,
            release_size=size_to_release,
            release_funds=release_funds,
            reason=reason,
        )
        if meta["remaining_size"] <= 1e-9:
            self._order_reservations.pop(order_id, None)

    async def _after_cancel(self, order_id: str) -> None:
        await self._clear_cancel_task(order_id)
        await self._release_reservation_for_order(order_id, reason="cancelled")

    def _remaining_unfilled(self, order_id: str) -> float:
        size = self._order_sizes.get(order_id, 0.0)
        filled = self._fill_progress.get(order_id, 0.0)
        remaining = max(0.0, size - filled)
        return remaining