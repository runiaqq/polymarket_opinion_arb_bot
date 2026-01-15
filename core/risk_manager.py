from __future__ import annotations

import asyncio
import uuid
from typing import Dict, Tuple

from core.exceptions import RiskCheckError
from utils.config_loader import OutcomeCoveredArbConfig
from utils.logger import BotLogger


class RiskManager:
    """Performs pre-trade risk validation."""

    def __init__(
        self,
        config: OutcomeCoveredArbConfig,
        logger: BotLogger | None = None,
        database=None,
        default_asset: str = "USDC",
    ):
        self.config = config
        self.logger = logger or BotLogger(__name__)
        self.db = database
        self.default_asset = default_asset
        self._event_limits: Dict[Tuple[str, str], float] = {}
        self._fund_reservations: Dict[Tuple[str, str], float] = {}
        self._reservations: Dict[str, dict] = {}
        self._blocked_events: dict[str, str] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._loaded = False
        self.ops_panel = None
        self._exposure_alerts: Dict[Tuple[str, str], float] = {}

    async def restore_reservations(self) -> None:
        """Load active reservations from the database on startup."""
        if not self.db:
            self.logger.warn("no database attached; persistence disabled")
            self._loaded = True
            return
        rows = await self.db.fetch_active_reservations()
        self._event_limits.clear()
        self._fund_reservations.clear()
        self._reservations.clear()
        for row in rows:
            self._register_reservation_cache(row)
        self._loaded = True
        if rows:
            self.logger.info(
                "active exposure reservations restored",
                count=len(rows),
            )

    def _register_reservation_cache(self, row: dict) -> None:
        reservation_id = str(row["id"])
        event_id = str(row["event_id"])
        account_id = str(row["account_id"])
        asset = str(row.get("asset") or self.default_asset)
        remaining_size = float(row.get("remaining_size", 0.0) or 0.0)
        remaining_funds = float(row.get("remaining_funds", 0.0) or 0.0)
        self._reservations[reservation_id] = {
            **row,
            "remaining_size": remaining_size,
            "remaining_funds": remaining_funds,
            "asset": asset,
        }
        event_key = (event_id, account_id)
        fund_key = (account_id, asset)
        self._event_limits[event_key] = self._event_limits.get(event_key, 0.0) + remaining_size
        self._event_limits[event_id] = self._event_limits.get(event_id, 0.0) + remaining_size
        self._fund_reservations[fund_key] = self._fund_reservations.get(fund_key, 0.0) + remaining_funds

    def _account_lock(self, account_id: str) -> asyncio.Lock:
        lock = self._locks.get(account_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[account_id] = lock
        return lock

    async def _ensure_loaded(self) -> None:
        if not self._loaded:
            await self.restore_reservations()

    async def check_balance(
        self,
        exchange,
        required: float,
        asset: str = "USDC",
        account_id: str | None = None,
    ) -> None:
        await self._ensure_loaded()
        getter = getattr(exchange, "get_balances", None)
        if getter is None:
            raise RiskCheckError("balance check unavailable for exchange client")
        balances = await getter()
        if balances is None:
            raise RiskCheckError("balance response missing")
        available = float(balances.get(asset, 0))
        reserved = self._fund_reservations.get((account_id or "", asset), 0.0)
        effective_available = available - reserved
        if available < required:
            self.logger.warn(
                "balance check failed",
                required=required,
                available=available,
                asset=asset,
                exchange=exchange.__class__.__name__,
            )
            raise RiskCheckError("insufficient balance")
        if effective_available < required:
            self.logger.warn(
                "balance reserved exceeds headroom",
                required=required,
                reserved=reserved,
                available=available,
                asset=asset,
                exchange=exchange.__class__.__name__,
            )
            raise RiskCheckError("balance reserved")

    async def check_limits(
        self,
        event_id: str,
        size: float,
        reserve: bool = True,
        account_id: str | None = None,
    ) -> None:
        """Validate position limits; optionally reserve exposure."""
        await self._ensure_loaded()
        if event_id in self._blocked_events:
            reason = self._blocked_events[event_id]
            self.logger.error("event blocked", event_id=event_id, reason=reason)
            raise RiskCheckError(f"event blocked: {reason}")
        self._validate_limits(event_id, account_id, size)
        await self._maybe_warn_exposure(event_id, account_id or "", size)
        if reserve:
            key = (event_id, account_id or "")
            self._event_limits[key] = self._event_limits.get(key, 0.0) + size
        if account_id is None:
            self._event_limits[event_id] = self._event_limits.get(event_id, 0.0) + size

    def _validate_limits(self, event_id: str, account_id: str | None, size: float) -> None:
        current = self._event_limits.get((event_id, account_id or ""), 0.0)
        if size > self.config.max_position_size_per_market:
            raise RiskCheckError("size exceeds per-market limit")
        if current + size > self.config.max_position_size_per_event:
            raise RiskCheckError("size exceeds per-event limit")

    async def reserve_order(
        self,
        *,
        event_id: str,
        account_id: str,
        exchange,
        exchange_name: str | None = None,
        notional: float,
        size: float,
        asset: str | None = None,
        order_id: str | None = None,
        reason: str | None = None,
    ) -> str:
        """Atomically reserve exposure and funds for an order."""
        if not self.db:
            raise RiskCheckError("persistence unavailable; cannot reserve exposure")
        asset = asset or self.default_asset
        await self._ensure_loaded()
        if event_id in self._blocked_events:
            reason = self._blocked_events[event_id]
            raise RiskCheckError(f"event blocked: {reason}")
        lock = self._account_lock(account_id)
        async with lock:
            self._validate_limits(event_id, account_id, size)
            await self._validate_balance_with_reservations(exchange, notional, asset, account_id)
            reservation_id = uuid.uuid4().hex
            exchange_label = exchange_name or getattr(exchange, "name", None) or exchange.__class__.__name__
            await self.db.create_exposure_reservation(
                reservation_id=reservation_id,
                event_id=event_id,
                account_id=account_id,
                exchange=str(getattr(exchange_label, "value", exchange_label)),
                asset=asset,
                reserved_size=size,
                reserved_funds=notional,
                order_id=order_id,
                reason=reason,
            )
            self._register_reservation_cache(
                {
                    "id": reservation_id,
                    "event_id": event_id,
                    "account_id": account_id,
                    "exchange": str(getattr(exchange_label, "value", exchange_label)),
                    "asset": asset,
                    "reserved_size": size,
                    "reserved_funds": notional,
                    "remaining_size": size,
                    "remaining_funds": notional,
                    "status": "ACTIVE",
                    "reason": reason,
                    "order_id": order_id,
                    "created_at": None,
                    "released_at": None,
                }
            )
            return reservation_id

    async def release_reservation(
        self,
        reservation_id: str,
        *,
        release_size: float | None = None,
        release_funds: float | None = None,
        reason: str | None = None,
    ) -> float:
        """Release a reservation partially or fully; returns remaining size."""
        await self._ensure_loaded()
        reservation = self._reservations.get(reservation_id)
        if reservation is None and self.db:
            reservation = await self.db.get_reservation(reservation_id)
            if reservation:
                self._register_reservation_cache(reservation)
        if reservation is None:
            return 0.0
        if reservation.get("status") != "ACTIVE":
            return float(reservation.get("remaining_size", 0.0) or 0.0)

        account_id = str(reservation["account_id"])
        asset = str(reservation.get("asset") or self.default_asset)
        lock = self._account_lock(account_id)
        async with lock:
            remaining_size = float(reservation.get("remaining_size", 0.0) or 0.0)
            remaining_funds = float(reservation.get("remaining_funds", 0.0) or 0.0)
            size_delta = remaining_size if release_size is None else min(release_size, remaining_size)
            fund_delta = remaining_funds if release_funds is None else min(release_funds, remaining_funds)
            new_remaining_size = max(0.0, remaining_size - size_delta)
            new_remaining_funds = max(0.0, remaining_funds - fund_delta)
            status = "RELEASED" if new_remaining_size <= 1e-9 else "ACTIVE"
            if self.db:
                await self.db.release_exposure_reservation(
                    reservation_id,
                    new_remaining_size,
                    new_remaining_funds,
                    status,
                    reason=reason,
                )

            # Update caches
            reservation["remaining_size"] = new_remaining_size
            reservation["remaining_funds"] = new_remaining_funds
            reservation["status"] = status
            event_key = (reservation["event_id"], account_id)
            fund_key = (account_id, asset)
            self._event_limits[reservation["event_id"]] = max(
                0.0, self._event_limits.get(reservation["event_id"], 0.0) - size_delta
            )
            self._event_limits[event_key] = max(
                0.0, self._event_limits.get(event_key, 0.0) - size_delta
            )
            self._fund_reservations[fund_key] = max(
                0.0, self._fund_reservations.get(fund_key, 0.0) - fund_delta
            )
            if status != "ACTIVE":
                self._reservations[reservation_id] = reservation
            return new_remaining_size

    async def _maybe_warn_exposure(self, event_id: str, account_id: str, incoming: float) -> None:
        """Send exposure warning via ops panel when nearing per-event limits."""
        if not self.ops_panel:
            return
        limit = float(self.config.max_position_size_per_event or 0.0)
        if limit <= 0:
            return
        current = self._event_limits.get((event_id, account_id), 0.0)
        projected = current + incoming
        remaining = limit - projected
        tolerance = float(getattr(self.config, "exposure_tolerance", 0.0) or 0.0)
        ratio = projected / limit
        last = self._exposure_alerts.get((event_id, account_id), 0.0)
        thresholds = [0.7, 0.85, 0.95, 1.0]
        if tolerance > 0 and limit > 0:
            thresholds.append(max(0.0, (limit - tolerance) / limit))
        thresholds = sorted(set(thresholds))
        hit = next((thr for thr in thresholds if ratio >= thr and thr > last), None)
        if hit is None:
            return
        self._exposure_alerts[(event_id, account_id)] = ratio
        try:
            await self.ops_panel.exposure_warning(
                event_id=event_id,
                account_id=account_id,
                used=projected,
                limit=limit,
                remaining=remaining,
            )
        except Exception:
            self.logger.warn("exposure warning send failed", event_id=event_id, account_id=account_id)

    async def check_slippage(self, slippage: float, max_slippage: float) -> None:
        if slippage > max_slippage:
            raise RiskCheckError("slippage exceeds threshold")

    async def decrement(self, event_id: str, size: float, account_id: str | None = None) -> None:
        """Legacy hook to reduce in-memory exposure when reservation id is unavailable."""
        if size <= 0:
            return
        await self._ensure_loaded()
        key = (event_id, account_id or "")
        current = self._event_limits.get(key, 0.0)
        new_value = max(0.0, current - size)
        self._event_limits[key] = new_value
        if account_id is None:
            current_event = self._event_limits.get(event_id, current)
            self._event_limits[event_id] = max(0.0, current_event - size)
        self.logger.debug("exposure decremented", event_id=event_id, size=size, remaining=new_value)

    async def block_event(self, event_id: str, reason: str = "hedge_invariant_breach") -> None:
        """Prevent further trading for an event/pair."""
        self._blocked_events[event_id] = reason
        self.logger.error("trading blocked", event_id=event_id, reason=reason)

    def is_blocked(self, event_id: str) -> bool:
        return event_id in self._blocked_events

    def current_reserved_size(self, event_id: str, account_id: str) -> float:
        return self._event_limits.get((event_id, account_id), 0.0)

    def current_reserved_funds(self, account_id: str, asset: str | None = None) -> float:
        asset = asset or self.default_asset
        return self._fund_reservations.get((account_id, asset), 0.0)

    async def _validate_balance_with_reservations(
        self,
        exchange,
        required: float,
        asset: str,
        account_id: str,
    ) -> None:
        getter = getattr(exchange, "get_balances", None)
        if getter is None:
            raise RiskCheckError("balance check unavailable for exchange client")
        balances = await getter()
        if balances is None:
            raise RiskCheckError("balance response missing")
        available = float(balances.get(asset, 0))
        reserved = self._fund_reservations.get((account_id, asset), 0.0)
        effective = available - reserved
        if effective < required:
            raise RiskCheckError("insufficient balance after reservations")
