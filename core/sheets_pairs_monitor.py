from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.models import ContractType, ExchangeName, StrategyDirection
from core.pairs_status import (
    OrderbookView,
    PairRuntimeStatus,
    PairStatusComputer,
    PairStatusSnapshot,
    ValidationResult,
    hash_row,
)
from core.manual_event import ManualEventError
from core.manual_match.errors import ManualMatchError
from core.manual_match.models import OpinionMarket
from core.opm_pair_ui import build_opinion_side_prompt, build_opinion_topic_prompt
from core.opm_pair import OpmPairService
from utils.config_loader import MarketPairConfig, SheetsMonitorConfig, Settings
from utils.google_sheets import GoogleSheetsClient, SheetPairSpec, SheetRowStatus, SheetSyncReport, _normalize_header, _fingerprint
from utils.logger import BotLogger


@dataclass(slots=True)
class PairState:
    pair_id: str
    row: int | None
    row_hash: str | None
    fingerprint: str | None
    last_seen: str
    status: str
    enabled: bool
    reason: str | None = None
    missed_polls: int = 0
    last_notified_hash: str | None = None
    sheet_message: str | None = None
    validation_reason: str | None = None
    orderbooks: Dict[str, dict] = field(default_factory=dict)
    selected_side: str | None = None
    selected_token_id: str | None = None
    pm_token_id: str | None = None
    child_page: int = 0
    freshly_created: bool = False
    finalized: bool = False
    child_market_id: str | None = None
    child_market_label: str | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None


@dataclass(slots=True)
class SheetSelection:
    """Persisted user choices for a sheet row (Opinion child + side)."""

    row: int
    row_hash: str
    op_input: str
    pm_input: str
    choice_id: str
    topic_id: str | None = None
    topic_title: str | None = None
    child_count: int = 0
    selected_child: int | None = None
    selected_side: str | None = None
    selected_token_id: str | None = None
    pm_token_id: str | None = None
    event_id: str | None = None
    created_at: str = ""
    child_page: int = 0
    opinion_orderbook_ok: bool = False
    polymarket_orderbook_ok: bool = False
    finalized: bool = False
    child_market_id: str | None = None
    child_market_label: str | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None


@dataclass(slots=True)
class SyncSummary:
    correlation_id: str
    started_at: str
    duration_sec: float
    rows_total: int
    rows_enabled: int
    ok: int
    skipped: int
    errors: int
    new: int
    updated: int
    disabled: int
    status_changes: int
    message: str | None = None
    reason: str | None = None


@dataclass(slots=True)
class RowResolution:
    spec: SheetPairSpec | None
    status: SheetRowStatus

class SheetsPairsMonitor:
    """
    Polls Google Sheets, diffs against previous state, updates running pairs, and emits notifications.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        config: SheetsMonitorConfig,
        sheet_client: GoogleSheetsClient,
        pair_controller,
        pair_store,
        notifier,
        logger: BotLogger | None = None,
        registry_service=None,
        opm_pair_service: OpmPairService | None = None,
        state_path: str | Path = Path("data") / "sheets_pairs_state.json",
        verifier=None,
    ):
        self.settings = settings
        self.config = config
        self.sheet_client = sheet_client
        self.pair_controller = pair_controller
        self.pair_store = pair_store
        self.notifier = notifier
        self.registry_service = registry_service
        self.logger = logger or BotLogger(__name__)
        self.state_path = Path(state_path)
        self._status_computer = PairStatusComputer(verifier or getattr(registry_service, "verifier", None), logger=self.logger)
        self.opm_pair_service = opm_pair_service
        self.callback_router = None
        self._state, self._choices = self._load_state()
        self._choice_index: Dict[str, str] = {choice.choice_id: row_hash for row_hash, choice in self._choices.items()}
        self._lock = asyncio.Lock()
        self._last_summary: Optional[SyncSummary] = None
        self._last_sheet_specs: Dict[str, SheetPairSpec] = {}
        self._sheet_pairs_bootstrapped = False
        self._last_issue_fingerprint: Optional[str] = None
        self._dynamic_chat_id: Optional[str] = None

    @property
    def bootstrapped(self) -> bool:
        return self._sheet_pairs_bootstrapped

    def set_callback_router(self, router: Any) -> None:
        """Attach callback router for short Telegram callbacks."""
        self.callback_router = router

    def _cb(self, action: str, payload: dict | None = None, fallback: str | None = None) -> str:
        """Safe callback_data builder that keeps data under Telegram limits."""
        if self.callback_router:
            try:
                data = self.callback_router.bind(action, payload or {})
                self.logger.debug("callback mapped", action=action, payload_keys=list((payload or {}).keys()))
                return data
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warn("callback mapping failed", action=action, error=str(exc))
        return fallback or action

    async def _send_final_card(self, choice: SheetSelection) -> None:
        """Send full pair card after selection is finalized."""
        if not self.notifier.enabled:
            return
        chat_id = self._target_chat()
        if not chat_id:
            return
        pair_id = None
        for state in self._state.values():
            if state.row_hash == choice.row_hash:
                pair_id = state.pair_id
                break
        if not pair_id and choice.event_id and choice.event_id in self._state:
            pair_id = choice.event_id
        if not pair_id:
            return
        state = self.get_pair(pair_id)
        if not state:
            return
        text, markup = await self.build_state_card(state)
        await self.notifier.send_message(text, chat_id=chat_id, parse_mode="HTML", reply_markup=markup)

    def _load_state(self) -> tuple[Dict[str, PairState], Dict[str, SheetSelection]]:
        if not self.state_path.exists():
            return {}, {}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}, {}
        pairs: Dict[str, PairState] = {}
        for pair_id, row in (payload.get("pairs") or {}).items():
            try:
                pairs[pair_id] = PairState(
                    pair_id=pair_id,
                    row=row.get("row"),
                    row_hash=row.get("row_hash"),
                    fingerprint=row.get("fingerprint"),
                    last_seen=row.get("last_seen"),
                    status=row.get("status"),
                    enabled=bool(row.get("enabled", False)),
                    reason=row.get("reason"),
                    missed_polls=int(row.get("missed_polls", 0)),
                    last_notified_hash=row.get("last_notified_hash"),
                    sheet_message=row.get("sheet_message"),
                    validation_reason=row.get("validation_reason"),
                    orderbooks=row.get("orderbooks") or {},
                    selected_side=row.get("selected_side"),
                    selected_token_id=row.get("selected_token_id"),
                    pm_token_id=row.get("pm_token_id"),
                    child_page=int(row.get("child_page", 0)),
                    freshly_created=bool(row.get("freshly_created", False)),
                    finalized=bool(row.get("finalized", False)),
                    child_market_id=row.get("child_market_id"),
                    child_market_label=row.get("child_market_label"),
                    yes_token_id=row.get("yes_token_id"),
                    no_token_id=row.get("no_token_id"),
                )
            except Exception:
                continue

        choices: Dict[str, SheetSelection] = {}
        for row_hash, raw in (payload.get("choices") or {}).items():
            try:
                choice = SheetSelection(
                    row=int(raw.get("row", 0)),
                    row_hash=row_hash,
                    op_input=raw.get("op_input", ""),
                    pm_input=raw.get("pm_input", ""),
                    choice_id=raw.get("choice_id") or hash_row({"row": raw.get("row"), "row_hash": row_hash})[:12],
                    topic_id=raw.get("topic_id"),
                    topic_title=raw.get("topic_title"),
                    child_count=int(raw.get("child_count", 0)),
                    selected_child=raw.get("selected_child"),
                    selected_side=raw.get("selected_side"),
                    selected_token_id=raw.get("selected_token_id"),
                    pm_token_id=raw.get("pm_token_id"),
                    created_at=raw.get("created_at") or datetime.now(tz=timezone.utc).isoformat(),
                    child_page=int(raw.get("child_page", 0)),
                    opinion_orderbook_ok=bool(raw.get("opinion_orderbook_ok", False)),
                    polymarket_orderbook_ok=bool(raw.get("polymarket_orderbook_ok", False)),
                    finalized=bool(raw.get("finalized", False)),
                    child_market_id=raw.get("child_market_id"),
                    child_market_label=raw.get("child_market_label"),
                    yes_token_id=raw.get("yes_token_id"),
                    no_token_id=raw.get("no_token_id"),
                )
                choices[row_hash] = choice
            except Exception:
                continue

        return pairs, choices

    def _save_state(self) -> None:
        payload = {
            "pairs": {pid: asdict(state) for pid, state in self._state.items()},
            "last_summary": asdict(self._last_summary) if self._last_summary else None,
            "choices": {row_hash: asdict(choice) for row_hash, choice in self._choices.items()},
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def _store_choice(self, choice: SheetSelection) -> SheetSelection:
        choice.finalized = self._choice_is_finalized(choice)
        self._choices[choice.row_hash] = choice
        self._choice_index[choice.choice_id] = choice.row_hash
        return choice

    def _prune_choices(self, used_hashes: set[str]) -> None:
        for row_hash, choice in list(self._choices.items()):
            if row_hash in used_hashes:
                continue
            self._choices.pop(row_hash, None)
            if choice:
                self._choice_index.pop(choice.choice_id, None)

    def _ensure_choice(
        self,
        *,
        row: int,
        row_hash: str,
        op_input: str,
        pm_input: str,
        topic_id: str | None = None,
        topic_title: str | None = None,
        child_count: int = 0,
        pm_token_id: str | None = None,
        child_market_id: str | None = None,
        child_market_label: str | None = None,
        yes_token_id: str | None = None,
        no_token_id: str | None = None,
    ) -> SheetSelection:
        existing = self._choices.get(row_hash)
        if existing:
            existing.op_input = op_input
            existing.pm_input = pm_input
            existing.topic_id = topic_id or existing.topic_id
            existing.topic_title = topic_title or existing.topic_title
            existing.child_count = child_count or existing.child_count
            existing.pm_token_id = pm_token_id or existing.pm_token_id
            existing.child_market_id = child_market_id or existing.child_market_id
            existing.child_market_label = child_market_label or existing.child_market_label
            existing.yes_token_id = yes_token_id or existing.yes_token_id
            existing.no_token_id = no_token_id or existing.no_token_id
            return self._store_choice(existing)
        choice_id = hash_row({"row": row, "row_hash": row_hash, "op": op_input, "pm": pm_input})[:12]
        created_at = datetime.now(tz=timezone.utc).isoformat()
        choice = SheetSelection(
            row=row,
            row_hash=row_hash,
            op_input=op_input,
            pm_input=pm_input,
            choice_id=choice_id,
            topic_id=topic_id,
            topic_title=topic_title,
            child_count=child_count,
            pm_token_id=pm_token_id,
            child_market_id=child_market_id,
            child_market_label=child_market_label,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
                    child_page=0,
            created_at=created_at,
        )
        return self._store_choice(choice)

    @staticmethod
    def _choice_is_finalized(choice: SheetSelection) -> bool:
        """Final state: tokens chosen, side chosen, both orderbooks seen."""
        return bool(
            choice.selected_token_id
            and choice.pm_token_id
            and choice.selected_side
            and (choice.yes_token_id or choice.no_token_id)
            and choice.opinion_orderbook_ok
            and choice.polymarket_orderbook_ok
        )

    @staticmethod
    def _orderbook_ok(ob) -> bool:
        return bool(
            ob
            and getattr(ob, "status", None) == 200
            and (getattr(ob, "bid", None) is not None or getattr(ob, "ask", None) is not None)
        )

    @staticmethod
    def _is_topic_link(op_input: str) -> bool:
        low = str(op_input or "").lower()
        return "topicid=" in low or "type=multi" in low

    def _mark_orderbook_flags(
        self,
        choice: SheetSelection,
        *,
        opinion_market,
        selection_token: str | None,
        polymarket_result,
    ) -> None:
        if opinion_market and selection_token:
            ob = (opinion_market.orderbooks or {}).get(selection_token)
            if self._orderbook_ok(ob):
                choice.opinion_orderbook_ok = True
        if polymarket_result:
            pm_ob = getattr(polymarket_result, "orderbook", None)
            if self._orderbook_ok(pm_ob):
                choice.polymarket_orderbook_ok = True
        self._store_choice(choice)

    def _target_chat(self) -> str | None:
        return self.config.notify_chat_id or self._dynamic_chat_id or getattr(self.notifier, "chat_id", None)

    @staticmethod
    def _fmt_price(value: float | None) -> str:
        if value is None:
            return "—"
        try:
            return f"{float(value):.4f}"
        except (TypeError, ValueError):
            return "—"

    def _fmt_book(self, book) -> str:
        if not book or book.status != 200:
            return "(нет стакана)"
        bid = self._fmt_price(book.bid)
        ask = self._fmt_price(book.ask)
        if bid == "—" and ask == "—":
            return "(стакан пуст)"
        return f"(bid {bid} / ask {ask})"

    @staticmethod
    def _match_child_market(op_result, child_market_id: str | None, fallback_idx: int | None) -> tuple[int | None, object | None]:
        children = list(getattr(op_result, "children", []) or [])
        if child_market_id:
            for idx, child in enumerate(children):
                cand = getattr(child, "market_id", None) or getattr(child, "topic_id", None)
                if cand and str(cand) == str(child_market_id):
                    return idx, child
        if fallback_idx is not None and 0 <= fallback_idx < len(children):
            return fallback_idx, children[fallback_idx]
        return None, None

    async def _prompt_topic_choice(self, choice: SheetSelection, result) -> None:
        if not self.notifier.enabled:
            return
        chat_id = self._target_chat()
        if not chat_id:
            return
        children = list(result.children or [])
        text, markup = build_opinion_topic_prompt(
            result.topic_title or "",
            children,
            child_callback=lambda idx: self._cb(
                "op:child",
                {
                    "choice_id": choice.choice_id,
                    "child_index": idx,
                    "child_market_id": getattr(children[idx], "market_id", None),
                    "child_market_label": getattr(children[idx], "title", None),
                    "yes_token_id": getattr(children[idx], "yes_token_id", None),
                    "no_token_id": getattr(children[idx], "no_token_id", None),
                },
                fallback=f"sheets:selectchild:{choice.choice_id}:{idx}",
            ),
            cancel_callback=self._cb("op:cancel", {"choice_id": choice.choice_id}, fallback="sheets:cancel"),
        )
        await self.notifier.send_message(text, chat_id=chat_id, reply_markup=markup)

    async def _prompt_side_choice(self, choice: SheetSelection, market: OpinionMarket) -> None:
        if not self.notifier.enabled:
            return
        chat_id = self._target_chat()
        if not chat_id:
            return
        text, markup = build_opinion_side_prompt(
            market,
            yes_callback=self._cb(
                "op:side",
                {"choice_id": choice.choice_id, "side": "YES", "child_market_id": getattr(market, "market_id", None)},
                fallback=f"sheets:selectside:{choice.choice_id}:YES",
            ),
            no_callback=self._cb(
                "op:side",
                {"choice_id": choice.choice_id, "side": "NO", "child_market_id": getattr(market, "market_id", None)},
                fallback=f"sheets:selectside:{choice.choice_id}:NO",
            ),
            cancel_callback=self._cb("op:cancel", {"choice_id": choice.choice_id}, fallback="sheets:cancel"),
        )
        await self.notifier.send_message(text, chat_id=chat_id, reply_markup=markup)
    async def handle_selection_callback(
        self,
        *,
        chat_id: str,
        action: str,
        choice_id: str = "",
        value: str | None = None,
        payload: dict | None = None,
    ) -> bool:
        payload = payload or {}
        choice_id = payload.get("choice_id") or choice_id
        value = payload.get("value") or value
        self.logger.debug(
            "selection callback",
            action=action,
            choice_id=choice_id,
            payload_keys=list(payload.keys()),
        )
        if action in {"op:cancel"}:
            await self.notifier.send_message("Выбор отменён. Используйте /pairs_refresh для повторения.", chat_id=chat_id)
            return True
        if not self.opm_pair_service:
            await self.notifier.send_message("⚠️ OPM resolver недоступен.", chat_id=chat_id)
            return True
        row_hash = self._choice_index.get(choice_id)
        if not row_hash:
            await self.notifier.send_message("⚠️ Выбор не найден или устарел. Запустите /pairs_refresh.", chat_id=chat_id)
            return True
        choice = self._choices.get(row_hash)
        if not choice:
            await self.notifier.send_message("⚠️ Состояние выбора потеряно. Запустите /pairs_refresh.", chat_id=chat_id)
            return True
        if choice.finalized:
            await self.notifier.send_message("✅ Пара уже финализирована, повторный выбор не нужен.", chat_id=chat_id)
            return True
        try:
            # lightweight resolve (без стаканов) для отображения
            op_result = await self.opm_pair_service.resolve_opinion(
                str(choice.op_input),
                fetch_orderbooks=False,
                strict_topic=self._is_topic_link(choice.op_input),
            )
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warn("opinion resolve failed during callback", error=str(exc))
            await self.notifier.send_message("⚠️ Не удалось обновить Opinion. Попробуйте позже.", chat_id=chat_id)
            return True

        is_child_action = action in {"selectchild", "op:child"}
        is_page_action = action in {"childpage", "op:page"}
        is_side_action = action in {"selectside", "op:side"}

        if is_child_action:
            try:
                fallback_idx = int(payload.get("child_index")) if payload.get("child_index") is not None else None
            except (TypeError, ValueError):
                fallback_idx = None
            if fallback_idx is None:
                try:
                    fallback_idx = int(value) if value is not None else None
                except (TypeError, ValueError):
                    fallback_idx = None
            target_market_id = payload.get("child_market_id") or value
            matched_idx, child_market = self._match_child_market(op_result, target_market_id, fallback_idx)
            if not op_result.topic or matched_idx is None or child_market is None:
                await self.notifier.send_message("⚠️ Рынок не найден, выберите из списка.", chat_id=chat_id)
                return True
            choice.selected_child = matched_idx
            choice.child_market_id = getattr(child_market, "market_id", None) or str(target_market_id)
            choice.child_market_label = (
                payload.get("child_market_label")
                or getattr(child_market, "title", None)
                or str(choice.child_market_id)
            )
            self.logger.debug(
                "child selected",
                child_market_id=choice.child_market_id,
                child_market_label=choice.child_market_label,
            )
            self._store_choice(choice)
            try:
                # повторный resolve со стаканами для выбранного child
                op_full = await self.opm_pair_service.resolve_opinion(
                    str(choice.op_input),
                    fetch_orderbooks=True,
                    strict_topic=self._is_topic_link(choice.op_input),
                )
                market = None
                if op_full.topic:
                    matched_idx, market = self._match_child_market(op_full, choice.child_market_id, matched_idx)
                    if market is None and matched_idx is not None:
                        market = self.opm_pair_service.pick_opinion_market(op_full, matched_idx)
                else:
                    market = op_full.market
                if not market:
                    raise ManualEventError("Opinion рынок не найден.")
                choice.child_market_id = getattr(market, "market_id", None) or choice.child_market_id
                choice.child_market_label = choice.child_market_label or getattr(market, "title", None)
                choice.yes_token_id = getattr(market, "yes_token_id", None)
                choice.no_token_id = getattr(market, "no_token_id", None)
                self._store_choice(choice)
            except ManualEventError as exc:
                await self.notifier.send_message(f"⚠️ {exc}", chat_id=chat_id)
                return True
            except Exception as exc:
                self.logger.warn("sheet row child pick failed", error=str(exc))
                await self.notifier.send_message("⚠️ Не удалось выбрать рынок Opinion, попробуйте снова.", chat_id=chat_id)
                return True
            await self._prompt_side_choice(choice, market)
            return True

        if is_page_action:
            try:
                page = max(0, int(value if value is not None else payload.get("page", 0)))
            except (TypeError, ValueError):
                page = 0
            choice.child_page = page
            self._store_choice(choice)
            await self.notifier.send_message("Страница обновлена. Нажмите /pairs_show <pair_id> ещё раз.", chat_id=chat_id)
            return True

        if is_side_action:
            side = (payload.get("side") or value or "").upper()
            if side not in {"YES", "NO"}:
                await self.notifier.send_message("⚠️ Сторона должна быть YES или NO.", chat_id=chat_id)
                return True
            market: OpinionMarket | None = None
            # Всегда резолвим со стаканами перед выбором стороны
            try:
                op_full = await self.opm_pair_service.resolve_opinion(
                    str(choice.op_input),
                    fetch_orderbooks=True,
                    strict_topic=self._is_topic_link(choice.op_input),
                )
            except Exception as exc:
                self.logger.warn("opinion resolve failed before side select", error=str(exc))
                await self.notifier.send_message("⚠️ Opinion недоступен, попробуйте позже.", chat_id=chat_id)
                return True
            if op_full.topic:
                matched_idx, market = self._match_child_market(op_full, choice.child_market_id, choice.selected_child)
                if market is None and choice.selected_child is not None:
                    try:
                        market = self.opm_pair_service.pick_opinion_market(op_full, int(choice.selected_child))
                    except ManualEventError as exc:
                        await self.notifier.send_message(f"⚠️ {exc}", chat_id=chat_id)
                        return True
                if market is None:
                    await self.notifier.send_message("⚠️ Сначала выберите рынок в теме.", chat_id=chat_id)
                    return True
                choice.child_market_id = getattr(market, "market_id", None) or choice.child_market_id
                choice.child_market_label = choice.child_market_label or getattr(market, "title", None)
                choice.yes_token_id = getattr(market, "yes_token_id", None)
                choice.no_token_id = getattr(market, "no_token_id", None)
                self._store_choice(choice)
            else:
                market = op_full.market
                if market:
                    choice.child_market_id = getattr(market, "market_id", None) or choice.child_market_id
                    choice.child_market_label = choice.child_market_label or getattr(market, "title", None)
            if not market:
                await self.notifier.send_message("⚠️ Opinion рынок не найден.", chat_id=chat_id)
                return True
            try:
                selection = self.opm_pair_service.select_side(market, side)
            except ManualEventError as exc:
                await self.notifier.send_message(f"⚠️ {exc}", chat_id=chat_id)
                return True
            choice.selected_side = selection.side
            choice.selected_token_id = selection.selected_token
            choice.yes_token_id = getattr(market, "yes_token_id", None)
            choice.no_token_id = getattr(market, "no_token_id", None)
            choice.child_market_id = getattr(market, "market_id", None)
            choice.child_market_label = choice.child_market_label or getattr(market, "title", None)
            self.logger.debug(
                "side selected",
                child_market_id=choice.child_market_id,
                child_market_label=choice.child_market_label,
                side=choice.selected_side,
            )
            self._store_choice(choice)
            await self.notifier.send_message("✅ Выбор сохранён. Запускаю sync...", chat_id=chat_id)
            # Выполним синхронно, чтобы пользователь сразу увидел результат выбора
            await self.run_once(reason=f"manual-select:{choice.row}", force_notify=True)
            await self._send_final_card(choice)
            return True

        return False

    async def run_once(self, *, reason: str, force_notify: bool = False) -> SyncSummary:
        async with self._lock:
            started_at = datetime.now(tz=timezone.utc).isoformat()
            correlation_id = uuid.uuid4().hex[:8]
            started_ts = time.monotonic()
            self.logger.info("sheets sync started", correlation_id=correlation_id, reason=reason)
            try:
                rows = await self.sheet_client.fetch_rows()
                report = await self._resolve_rows(rows)
            except Exception as exc:
                duration = time.monotonic() - started_ts
                summary = SyncSummary(
                    correlation_id=correlation_id,
                    started_at=started_at,
                    duration_sec=duration,
                    rows_total=0,
                    rows_enabled=0,
                    ok=0,
                    skipped=0,
                    errors=1,
                    new=0,
                    updated=0,
                    disabled=0,
                    status_changes=0,
                    message=f"google sheets fetch failed: {exc}",
                    reason=reason,
                )
                self._last_summary = summary
                self.logger.warn("sheets sync failed", correlation_id=correlation_id, error=str(exc))
                self._save_state()
                return summary

            normalized_rows = self._normalize_rows(rows)
            effective_report = await self._apply_to_runtime(report, reason=reason)
            snapshots = await self._build_snapshots(effective_report, normalized_rows)
            summary = self._diff_and_notify(
                snapshots,
                effective_report,
                normalized_rows,
                correlation_id=correlation_id,
                started_at=started_at,
                started_ts=started_ts,
                force_notify=force_notify,
                reason=reason,
            )
            return summary

    async def _apply_to_runtime(self, report: SheetSyncReport, *, reason: str) -> SheetSyncReport:
        statuses = list(report.statuses)
        specs: Dict[str, SheetPairSpec] = dict(report.specs)
        row_index = {status.event_id: status.row for status in report.statuses if status.event_id}

        if self.registry_service:
            try:
                await self.registry_service.prune_sheet_rows(
                    active_rows={row for row in row_index.values() if row is not None},
                    active_event_ids=set(row_index.keys()),
                )
            except Exception as exc:
                self.logger.warn("registry prune failed", error=str(exc))

        if self.registry_service and specs:
            verified_specs: Dict[str, SheetPairSpec] = {}
            for pair_id, spec in specs.items():
                cfg = spec.pair_cfg
                entry = {
                    "event_id": cfg.event_id,
                    "primary_exchange": (cfg.primary_exchange or self.settings.exchanges.primary).value,
                    "secondary_exchange": (cfg.secondary_exchange or self.settings.exchanges.secondary).value,
                    "primary_market_id": cfg.primary_market_id,
                    "secondary_market_id": cfg.secondary_market_id,
                    "contract_type": cfg.contract_type.value,
                    "strategy_direction": cfg.strategy_direction.value if cfg.strategy_direction else "AUTO",
                    "dry_run": self.settings.dry_run,
                }
                meta = {"sheet_row": row_index.get(pair_id)}
                result = await self.registry_service.register_entry(entry, source="sheet", meta=meta)
                if result.ok:
                    verified_specs[pair_id] = spec
                else:
                    statuses.append(
                        SheetRowStatus(
                            row=row_index.get(pair_id, 0),
                            event_id=pair_id,
                            status="ERROR",
                            message=result.reason or "verification failed",
                        )
                    )
            specs = verified_specs

        effective_report = SheetSyncReport(specs=specs, statuses=statuses)
        if effective_report.statuses:
            await self._notify_sheet_issues(effective_report, reason=reason)

        await self.pair_controller.sync_sheet_pairs(specs)
        await self.pair_store.update_pairs([spec.pair_cfg for spec in specs.values()])
        self._last_sheet_specs = specs
        self._sheet_pairs_bootstrapped = True
        if specs:
            self.logger.info("sheet sync applied", pairs=len(specs), reason=reason)
        else:
            self.logger.info("sheet sync applied with 0 pairs; cleared runtime state", reason=reason)

        return effective_report

    async def _notify_sheet_issues(self, report: SheetSyncReport, *, reason: str) -> None:
        if not self.notifier.enabled:
            return
        non_ok = [item for item in report.statuses if item.status != "OK"]
        if not non_ok:
            return
        lines = [
            f"⚠️ Sheets sync issues ({reason})",
            f"OK: {report.count('OK')} | SKIPPED: {report.count('SKIPPED')} | ERROR: {report.count('ERROR')}",
        ]
        for item in non_ok[:5]:
            label = item.event_id or "n/a"
            lines.append(f"row {item.row} [{label}]: {item.message}")
        if len(non_ok) > 5:
            lines.append(f"...and {len(non_ok) - 5} more rows")
        message = "\n".join(lines)
        if message == self._last_issue_fingerprint:
            return
        self._last_issue_fingerprint = message
        await self.notifier.send_message(message)

    async def _build_snapshots(
        self,
        report: SheetSyncReport,
        normalized_rows: Dict[int, dict],
    ) -> Dict[str, PairStatusSnapshot]:
        runtime = await self.pair_controller.snapshot()
        running_ids = {pair["pair_id"] for pair in runtime.get("pairs", [])}

        snapshots: Dict[str, PairStatusSnapshot] = {}
        for status in report.statuses:
            row_meta = normalized_rows.get(status.row, {})
            row_hash = row_meta.get("hash")
            row_enabled = row_meta.get("enabled", True)
            event_id = status.event_id or row_meta.get("event_id") or f"row-{status.row}"
            # If this row_hash already exists in state, reuse its pair_id to keep continuity.
            if row_hash:
                for state_pair in self._state.values():
                    if state_pair.row_hash == row_hash:
                        event_id = state_pair.pair_id
                        break
            spec = report.specs.get(event_id)
            if spec is None and report.specs and len(report.specs) == 1:
                # Single-spec fallback: align snapshot with the only resolved spec.
                only_key = next(iter(report.specs))
                spec = report.specs[only_key]
                event_id = only_key
            fingerprint = spec.fingerprint if spec else row_meta.get("fingerprint")
            enabled = row_enabled and status.status != "SKIPPED"
            choice = self._choices.get(row_hash) if row_hash else None
            prev_state = self._state.get(event_id)
            validation: ValidationResult
            if spec and enabled:
                validation = await self._status_computer.validate(spec.pair_cfg)
            else:
                validation = ValidationResult(
                    ok=False,
                    reason=status.message,
                    orderbooks={},
                )
            runtime_active = event_id in running_ids
            pair_status = self._status_computer.compute_status(
                enabled=enabled,
                validation_ok=validation.ok,
                runtime_active=runtime_active,
                stale=False,
            )
            if enabled and validation.ok:
                pair_status = PairRuntimeStatus.ACTIVE if runtime_active else PairRuntimeStatus.READY
            reason = validation.reason or status.message
            finalized_flag = choice.finalized if choice else (prev_state.finalized if prev_state else False)
            snapshots[event_id] = PairStatusSnapshot(
                pair_id=event_id,
                status=pair_status,
                enabled=enabled,
                validation=validation,
                runtime_active=runtime_active,
                stale=False,
                reason=reason,
                row=status.row,
                fingerprint=fingerprint,
                row_hash=row_hash,
                sheet_message=status.message,
                selected_side=choice.selected_side if choice else None,
                selected_token_id=choice.selected_token_id if choice else None,
                pm_token_id=choice.pm_token_id if choice else None,
                finalized=finalized_flag,
                child_market_id=choice.child_market_id if choice else None,
                child_market_label=choice.child_market_label if choice else (prev_state.child_market_label if prev_state else None),
                yes_token_id=choice.yes_token_id if choice else None,
                no_token_id=choice.no_token_id if choice else None,
            )

        return snapshots

    def _diff_and_notify(
        self,
        snapshots: Dict[str, PairStatusSnapshot],
        report: SheetSyncReport,
        normalized_rows: Dict[int, dict],
        *,
        correlation_id: str,
        started_at: str,
        started_ts: float,
        force_notify: bool,
        reason: str,
    ) -> SyncSummary:
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        rows_total = max(0, len(normalized_rows))
        rows_enabled = sum(1 for snap in snapshots.values() if snap.enabled)
        new = updated = disabled = status_changes = 0

        seen_ids = set()
        for pair_id, snapshot in snapshots.items():
            seen_ids.add(pair_id)
            prev = self._state.get(pair_id)
            missed_polls = 0
            change_type = None

            if not prev:
                change_type = "NEW" if snapshot.enabled else None
                new += 1 if change_type else 0
            else:
                if snapshot.row_hash and prev.row_hash and snapshot.row_hash != prev.row_hash and snapshot.enabled:
                    change_type = "UPDATED"
                    updated += 1
                elif prev.enabled and not snapshot.enabled:
                    change_type = "DISABLED"
                    disabled += 1
                elif prev.status != snapshot.status and snapshot.status in {
                    PairRuntimeStatus.ACTIVE,
                    PairRuntimeStatus.READY,
                    PairRuntimeStatus.ERROR,
                }:
                    change_type = "STATUS_CHANGE"
                    status_changes += 1
                missed_polls = 0

            state = PairState(
                pair_id=pair_id,
                row=snapshot.row,
                row_hash=snapshot.row_hash,
                fingerprint=snapshot.fingerprint,
                last_seen=now_iso,
                status=snapshot.status.value,
                enabled=snapshot.enabled,
                reason=snapshot.reason,
                missed_polls=missed_polls,
                last_notified_hash=prev.last_notified_hash if prev else None,
                sheet_message=snapshot.sheet_message,
                validation_reason=snapshot.validation.reason,
                orderbooks={
                    label: {
                        "exchange": ob.exchange.value,
                        "market_id": ob.market_id,
                        "bid": ob.bid,
                        "ask": ob.ask,
                        "status": ob.status,
                        "source": getattr(ob, "source", None),
                    }
                    for label, ob in snapshot.validation.orderbooks.items()
                },
                selected_side=snapshot.selected_side,
                selected_token_id=snapshot.selected_token_id,
                pm_token_id=snapshot.pm_token_id,
                child_page=prev.child_page if prev else 0,
                freshly_created=False if not snapshot.enabled else (prev.freshly_created if prev else True),
                finalized=snapshot.finalized or (prev.finalized if prev else False),
                child_market_id=getattr(snapshot, "child_market_id", None) or (prev.child_market_id if prev else None),
                child_market_label=getattr(snapshot, "child_market_label", None) or (prev.child_market_label if prev else None),
                yes_token_id=getattr(snapshot, "yes_token_id", None) or (prev.yes_token_id if prev else None),
                no_token_id=getattr(snapshot, "no_token_id", None) or (prev.no_token_id if prev else None),
            )
            notify_hash = self._build_notify_hash(snapshot, change_type)
            should_notify = change_type and self.notifier.enabled and (
                force_notify or notify_hash != state.last_notified_hash or self.config.notify_on_startup
            )
            if should_notify:
                text, markup = self._format_pair_card(snapshot, change_type=change_type)
                target_chat = self.config.notify_chat_id or self._dynamic_chat_id or self.notifier.chat_id
                try:
                    asyncio.create_task(
                        self.notifier.send_message(text, chat_id=target_chat, parse_mode="HTML", reply_markup=markup)
                    )
                    state.last_notified_hash = notify_hash
                except Exception:
                    pass
            self._state[pair_id] = state

        # handle missing pairs by pruning them immediately
        removed_pairs: list[str] = []
        for pair_id, prev in list(self._state.items()):
            if pair_id in seen_ids:
                continue
            removed_pairs.append(pair_id)
            row_hash = prev.row_hash
            if row_hash and row_hash in self._choices:
                self._choices.pop(row_hash, None)
            if row_hash:
                for token, stored_hash in list(self._choice_index.items()):
                    if stored_hash == row_hash:
                        self._choice_index.pop(token, None)
            self._state.pop(pair_id, None)
        if removed_pairs:
            self.logger.info("sheet sync pruned missing pairs", removed=len(removed_pairs), pairs=removed_pairs)

        duration = time.monotonic() - started_ts
        summary = SyncSummary(
            correlation_id=correlation_id,
            started_at=started_at,
            duration_sec=duration,
            rows_total=rows_total,
            rows_enabled=rows_enabled,
            ok=report.count("OK"),
            skipped=report.count("SKIPPED"),
            errors=report.count("ERROR"),
            new=new,
            updated=updated,
            disabled=disabled,
            status_changes=status_changes,
            reason=reason,
        )
        self._last_summary = summary
        self.logger.info(
            "sheets sync completed",
            correlation_id=correlation_id,
            duration_sec=round(duration, 3),
            rows_total=rows_total,
            rows_enabled=rows_enabled,
            ok=summary.ok,
            skipped=summary.skipped,
            errors=summary.errors,
            new=new,
            updated=updated,
            disabled=disabled,
            status_changes=status_changes,
        )
        self._save_state()
        return summary

    @staticmethod
    def _normalize_rows(rows: List[List[str]]) -> Dict[int, dict]:
        if not rows:
            return {}
        headers = [_normalize_header(cell) for cell in rows[0]]
        normalized: Dict[int, dict] = {}
        for row_idx, row_values in enumerate(rows[1:], start=2):
            row_dict = {headers[idx]: row_values[idx] for idx in range(min(len(headers), len(row_values)))}
            hash_payload = {
                "event": row_dict.get("event_id") or row_dict.get("pair_id") or "",
                "primary": row_dict.get("polymarket") or row_dict.get("primary_market_id") or row_dict.get("marketa_url"),
                "secondary": row_dict.get("opinion") or row_dict.get("secondary_market_id") or row_dict.get("marketb_url"),
            }
            row_hash = hash_row(hash_payload)
            enabled_raw = str(row_dict.get("enabled", "")).strip().lower()
            enabled = enabled_raw not in {"0", "false", "no", "off", "disabled"}
            event_id = row_dict.get("event_id") or row_dict.get("pair_id") or ""
            if not event_id:
                primary = row_dict.get("polymarket") or row_dict.get("primary_market_id")
                secondary = row_dict.get("opinion") or row_dict.get("secondary_market_id")
                if primary and secondary:
                    event_id = f"{primary}:{secondary}"
            normalized[row_idx] = {
                "data": row_dict,
                "hash": row_hash,
                "enabled": enabled,
                "event_id": event_id,
            }
        return normalized

    def _build_notify_hash(self, snapshot: PairStatusSnapshot, change_type: str | None) -> str | None:
        if not change_type:
            return None
        payload = {
            "pair": snapshot.pair_id,
            "change": change_type,
            "status": snapshot.status.value,
            "row_hash": snapshot.row_hash,
            "reason": snapshot.reason,
        }
        return hash_row(payload)

    async def build_state_card(self, state) -> tuple[str, dict | None]:
        """Format a sheet pair card for Telegram with optional selection buttons."""
        from telegram.formatters import PairCardFormatter
        
        # Use human-readable formatter
        # For now, we use child_market_label as the Opinion label
        # Polymarket title would need to be fetched separately if needed
        text, markup = PairCardFormatter.build_pair_card(
            state,
            pm_title=None,  # Can be enhanced later to fetch from Polymarket API
            op_label=state.child_market_label,
            show_technical=False,  # Technical details only in /details view
        )
        
        # Override keyboard to use our callback router
        if markup and "inline_keyboard" in markup:
            keyboard = []
            for row in markup["inline_keyboard"]:
                new_row = []
                for btn in row:
                    # Extract action from callback_data if it's a simple string
                    callback_data = btn.get("callback_data", "")
                    if isinstance(callback_data, str) and callback_data.startswith("pair:"):
                        # Parse and rebind with our router
                        parts = callback_data.split(":", 2)
                        if len(parts) >= 3:
                            action = f"pair:{parts[1]}"
                            payload = {"pair_id": parts[2]}
                            callback_data = self._cb(action, payload, fallback=callback_data)
                        btn["callback_data"] = callback_data
                    new_row.append(btn)
                keyboard.append(new_row)
            markup["inline_keyboard"] = keyboard

        # Handle selection UI for ERROR status
        choice = self._choices.get(state.row_hash) if state.row_hash else None
        if choice and state.status == PairRuntimeStatus.ERROR.value:
            # Try to present selection UI (do not exceed TG length)
            try:
                op_result = await self.opm_pair_service.resolve_opinion(
                    choice.op_input,
                    fetch_orderbooks=False,
                    strict_topic=self._is_topic_link(choice.op_input),
                )
            except Exception:
                op_result = None
            if op_result and getattr(op_result, "topic", False):
                if choice.selected_child is None:
                    # show child list buttons with paging (default page 0)
                    page_size = 5
                    page = max(0, int(choice.child_page or 0))
                    total = len(op_result.children)
                    max_pages = max(1, (total + page_size - 1) // page_size)
                    if page >= max_pages:
                        page = max_pages - 1
                        choice.child_page = page
                        self._store_choice(choice)
                    start = page * page_size
                    end = min(total, start + page_size)
                    lines = [text, "", "📚 Нужно выбрать рынок Opinion:"]
                    for idx, child in enumerate(op_result.children[start:end], start=start):
                        lines.append(f"{idx + 1}) {child.title}")
                    lines.append(f"Страница {page + 1}/{max_pages}")
                    text = "\n".join(lines)
                    child_keyboard = [
                        [
                            {
                                "text": f"Выбрать #{idx + 1}",
                                "callback_data": self._cb(
                                    "op:child",
                                    {
                                        "choice_id": choice.choice_id,
                                        "child_index": idx,
                                        "child_market_id": getattr(op_result.children[idx], "market_id", None),
                                        "child_market_label": getattr(op_result.children[idx], "title", None),
                                        "yes_token_id": getattr(op_result.children[idx], "yes_token_id", None),
                                        "no_token_id": getattr(op_result.children[idx], "no_token_id", None),
                                    },
                                    fallback=f"sheets:selectchild:{choice.choice_id}:{idx}",
                                ),
                            }
                        ]
                        for idx in range(start, end)
                    ]
                    nav_row: list[dict] = []
                    if start > 0:
                        nav_row.append(
                            {
                                "text": "⬅️",
                                "callback_data": self._cb(
                                    "op:page",
                                    {"choice_id": choice.choice_id, "page": max(0, page - 1)},
                                    fallback=f"sheets:childpage:{choice.choice_id}:{max(0, page - 1)}",
                                ),
                            }
                        )
                    if end < total:
                        nav_row.append(
                            {
                                "text": "➡️",
                                "callback_data": self._cb(
                                    "op:page",
                                    {"choice_id": choice.choice_id, "page": page + 1},
                                    fallback=f"sheets:childpage:{choice.choice_id}:{page + 1}",
                                ),
                            }
                        )
                    if nav_row:
                        child_keyboard.append(nav_row)
                    child_keyboard.append(
                        [
                            {
                                "text": "❌ Отменить",
                                "callback_data": self._cb("op:cancel", {"choice_id": choice.choice_id}, fallback="sheets:cancel"),
                            }
                        ]
                    )
                    markup = {"inline_keyboard": child_keyboard}
                elif choice.selected_side is None:
                    # show side buttons
                    lines = [text, "", "Выберите сторону YES/NO для выбранного рынка."]
                    text = "\n".join(lines)
                    markup = {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "🟢 YES",
                                    "callback_data": self._cb(
                                        "op:side",
                                        {"choice_id": choice.choice_id, "side": "YES", "child_market_id": choice.child_market_id},
                                        fallback=f"sheets:selectside:{choice.choice_id}:YES",
                                    ),
                                },
                                {
                                    "text": "🔴 NO",
                                    "callback_data": self._cb(
                                        "op:side",
                                        {"choice_id": choice.choice_id, "side": "NO", "child_market_id": choice.child_market_id},
                                        fallback=f"sheets:selectside:{choice.choice_id}:NO",
                                    ),
                                },
                            ],
                            [
                                {
                                    "text": "❌ Отменить",
                                    "callback_data": self._cb("op:cancel", {"choice_id": choice.choice_id}, fallback="sheets:cancel"),
                                }
                            ],
                        ]
                    }
        
        return text, markup

    def _parse_size_limit(self, row: dict) -> tuple[float | None, str | None]:
        raw = row.get("size_limit") or row.get("max_position") or row.get("max_position_size_per_market")
        if raw in (None, "", "None"):
            return None, None
        try:
            return float(raw), None
        except (TypeError, ValueError):
            return None, "max_position must be numeric"

    def _build_spec_from_choice(self, row: dict, choice: SheetSelection, size_limit: float | None) -> SheetPairSpec:
        selected_token = choice.selected_token_id
        pm_token = choice.pm_token_id
        if not selected_token or not pm_token:
            raise RuntimeError("choice missing token ids")
        event_id = choice.event_id or row.get("event_id") or row.get("pair_id") or f"{selected_token}:{pm_token}"
        if not choice.event_id:
            choice.event_id = event_id
            self._store_choice(choice)
        pair_id = row.get("pair_id") or event_id
        pair_cfg = MarketPairConfig(
            event_id=event_id,
            primary_market_id=pm_token,
            secondary_market_id=selected_token,
            primary_account_id=row.get("primary_account_id"),
            secondary_account_id=row.get("secondary_account_id"),
            pair_id=pair_id,
            strategy=row.get("strategy"),
            max_position_size_per_market=size_limit,
            primary_exchange=ExchangeName.POLYMARKET,
            secondary_exchange=ExchangeName.OPINION,
            contract_type=ContractType.BINARY,
            strategy_direction=StrategyDirection.AUTO,
        )
        fingerprint = self.opm_pair_service.polymarket_service.build_fingerprint(selected_token, pm_token)
        self._store_choice(choice)
        return SheetPairSpec(pair_cfg=pair_cfg, size_limit=size_limit, fingerprint=fingerprint)

    async def _resolve_rows(self, rows: List[List[str]]) -> SheetSyncReport:
        """
        Resolve sheet rows using the same manual resolvers as /add_pair (OpinionResolver + ManualEventService).
        """
        statuses: List[SheetRowStatus] = []
        specs: Dict[str, SheetPairSpec] = {}
        if not rows:
            return SheetSyncReport(specs=specs, statuses=statuses)

        headers = [_normalize_header(cell) for cell in rows[0]]
        used_hashes: set[str] = set()
        for row_idx, row_values in enumerate(rows[1:], start=2):
            row = {headers[idx]: row_values[idx] for idx in range(min(len(headers), len(row_values)))}
            pm_input = row.get("polymarket") or row.get("primary_market_id") or row.get("marketa_url")
            op_input = row.get("opinion") or row.get("secondary_market_id") or row.get("marketb_url")
            hash_payload = {"event": row.get("event_id") or row.get("pair_id") or "", "primary": pm_input, "secondary": op_input}
            row_hash = hash_row(hash_payload)
            used_hashes.add(row_hash)
            event_hint = row.get("event_id") or row.get("pair_id") or (f"{pm_input}:{op_input}" if pm_input and op_input else f"row-{row_idx}")
            enabled_raw = str(row.get("enabled", "")).strip().lower()
            if enabled_raw in {"0", "false", "no", "off", "disabled"}:
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="SKIPPED", message="row disabled"))
                continue
            if not pm_input or not op_input:
                statuses.append(
                    SheetRowStatus(
                        row=row_idx,
                        event_id=event_hint,
                        status="ERROR",
                        message="polymarket/opinion links missing",
                    )
                )
                continue
            if not self.opm_pair_service:
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message="opm resolver unavailable"))
                continue
            size_limit, size_error = self._parse_size_limit(row)
            if size_error:
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message=size_error))
                continue
            choice = self._ensure_choice(
                row=row_idx,
                row_hash=row_hash,
                op_input=str(op_input),
                pm_input=str(pm_input),
            )
            prev_state = None
            for st in self._state.values():
                if st.row_hash == row_hash or st.pair_id == event_hint or (st.fingerprint and st.fingerprint == row.get("fingerprint")):
                    prev_state = st
                    break
            if prev_state and (not choice.selected_side or not choice.selected_token_id or not choice.pm_token_id):
                if prev_state.selected_side:
                    choice.selected_side = prev_state.selected_side
                if prev_state.selected_token_id:
                    choice.selected_token_id = prev_state.selected_token_id
                if prev_state.pm_token_id:
                    choice.pm_token_id = prev_state.pm_token_id
                if prev_state.child_market_id:
                    choice.child_market_id = prev_state.child_market_id
                if prev_state.child_market_label:
                    choice.child_market_label = prev_state.child_market_label
                if prev_state.yes_token_id:
                    choice.yes_token_id = prev_state.yes_token_id
                if prev_state.no_token_id:
                    choice.no_token_id = prev_state.no_token_id
                if prev_state.finalized:
                    choice.opinion_orderbook_ok = True
                    choice.polymarket_orderbook_ok = True
                    choice.finalized = True
                self._store_choice(choice)
            if self._choice_is_finalized(choice):
                spec = self._build_spec_from_choice(row, self._store_choice(choice), size_limit)
                specs[spec.pair_cfg.event_id] = spec
                statuses.append(
                    SheetRowStatus(
                        row=row_idx,
                        event_id=spec.pair_cfg.event_id,
                        status="OK",
                        message="finalized; skipped resolve",
                    )
                )
                continue
            if (
                choice.selected_side
                and choice.selected_token_id
                and choice.pm_token_id
                and choice.opinion_orderbook_ok
                and choice.polymarket_orderbook_ok
            ):
                spec = self._build_spec_from_choice(row, choice, size_limit)
                specs[spec.pair_cfg.event_id] = spec
                statuses.append(
                    SheetRowStatus(
                        row=row_idx,
                        event_id=spec.pair_cfg.event_id,
                        status="OK",
                        message="resolved via manual resolver",
                    )
                )
                continue
            need_orderbooks = True
            try:
                op_result = await self.opm_pair_service.resolve_opinion(
                    str(op_input),
                    fetch_orderbooks=need_orderbooks,
                    strict_topic=self._is_topic_link(op_input),
                )
            except (ManualEventError, ManualMatchError) as exc:
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message=str(exc)))
                continue
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.info("sheet row resolve failed", row=row_idx, error=str(exc))
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message="resolve failed"))
                continue
            choice.topic_id = op_result.topic_id
            choice.topic_title = op_result.topic_title
            choice.child_count = len(op_result.children) if getattr(op_result, "topic", False) else 0
            self._store_choice(choice)
            market: OpinionMarket | None = None
            if op_result.topic:
                if not op_result.children:
                    statuses.append(
                        SheetRowStatus(
                            row=row_idx,
                            event_id=event_hint,
                            status="ERROR",
                            message="Opinion тема без дочерних рынков",
                        )
                    )
                    continue
                if choice.selected_child is None:
                    await self._prompt_topic_choice(choice, op_result)
                    statuses.append(
                        SheetRowStatus(
                            row=row_idx,
                            event_id=event_hint,
                            status="ERROR",
                            message="требуется выбрать рынок Opinion",
                        )
                    )
                    continue
                try:
                    market = self.opm_pair_service.pick_opinion_market(op_result, int(choice.selected_child))
                except ManualEventError as exc:
                    statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message=str(exc)))
                    continue
                except Exception as exc:
                    self.logger.info("sheet row child pick failed", row=row_idx, error=str(exc))
                    statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message="resolve failed"))
                    continue
            else:
                market = op_result.market
                if not market:
                    statuses.append(
                        SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message="Opinion рынок не найден")
                    )
                    continue
            choice.child_market_id = getattr(market, "market_id", None)
            choice.yes_token_id = getattr(market, "yes_token_id", None)
            choice.no_token_id = getattr(market, "no_token_id", None)
            self._store_choice(choice)
            if not choice.selected_side:
                await self._prompt_side_choice(choice, market)
                statuses.append(
                    SheetRowStatus(
                        row=row_idx,
                        event_id=event_hint,
                        status="ERROR",
                        message="требуется выбор стороны YES/NO",
                    )
                )
                continue
            try:
                selection = self.opm_pair_service.select_side(market, choice.selected_side)
            except ManualEventError as exc:
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message=str(exc)))
                continue
            self._mark_orderbook_flags(
                choice,
                opinion_market=selection.market,
                selection_token=selection.selected_token,
                polymarket_result=None,
            )
            try:
                pm_resolved = await self.opm_pair_service.resolve_polymarket(
                    str(pm_input), known_token_id=choice.pm_token_id
                )
            except TypeError:
                pm_resolved = await self.opm_pair_service.resolve_polymarket(str(pm_input))
            except ManualEventError as exc:
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message=str(exc)))
                continue
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.info("polymarket resolve failed", row=row_idx, error=str(exc))
                statuses.append(SheetRowStatus(row=row_idx, event_id=event_hint, status="ERROR", message="resolve failed"))
                continue
            choice.selected_side = selection.side
            choice.selected_token_id = selection.selected_token
            choice.pm_token_id = pm_resolved.token_id
            self._mark_orderbook_flags(
                choice,
                opinion_market=selection.market,
                selection_token=selection.selected_token,
                polymarket_result=pm_resolved,
            )
            spec = self._build_spec_from_choice(row, choice, size_limit)
            specs[spec.pair_cfg.event_id] = spec
            statuses.append(
                SheetRowStatus(
                    row=row_idx,
                    event_id=spec.pair_cfg.event_id,
                    status="OK",
                    message="resolved via manual resolver",
                )
            )
        self._prune_choices(used_hashes)
        return SheetSyncReport(specs=specs, statuses=statuses)
    def _format_pair_card(self, snapshot: PairStatusSnapshot, *, change_type: str | None):
        """Format pair card for notifications - uses human-readable formatter."""
        from telegram.formatters import PairCardFormatter
        
        # Convert snapshot to state-like format for formatter
        # The formatter will handle the display
        # For notifications, we don't show technical details
        text, markup = PairCardFormatter.build_pair_card(
            snapshot,
            pm_title=None,  # Can be enhanced later
            op_label=snapshot.child_market_label,
            show_technical=False,
        )
        
        # Add change type prefix if provided
        if change_type:
            title_prefix = {
                "NEW": "🆕 New Pair",
                "UPDATED": "✏️ Updated Pair",
                "DISABLED": "🚫 Pair Disabled",
                "STATUS_CHANGE": "ℹ️ Status Change",
            }.get(change_type, "ℹ️ Pair Update")
            text = f"{title_prefix}\n\n{text}"
        
        # Rebind callbacks with our router
        if markup and "inline_keyboard" in markup:
            keyboard = []
            for row in markup["inline_keyboard"]:
                new_row = []
                for btn in row:
                    callback_data = btn.get("callback_data", "")
                    if isinstance(callback_data, str) and callback_data.startswith("pair:"):
                        parts = callback_data.split(":", 2)
                        if len(parts) >= 3:
                            action = f"pair:{parts[1]}"
                            payload = {"pair_id": parts[2]}
                            callback_data = self._cb(action, payload, fallback=callback_data)
                        btn["callback_data"] = callback_data
                    new_row.append(btn)
                keyboard.append(new_row)
            markup["inline_keyboard"] = keyboard
        
        return text, markup

    async def remove_pair(self, pair_id: str, *, reason: str | None = None, stop_runtime: bool = True) -> bool:
        """
        Explicitly remove a pair from the in-memory sheet snapshot and stop runtime if requested.
        Used by admin UX (/pairs_remove).
        """
        removed_state = None
        async with self._lock:
            state = self._state.pop(pair_id, None)
            if state and state.row_hash:
                self._choices.pop(state.row_hash, None)
                for token, stored_hash in list(self._choice_index.items()):
                    if stored_hash == state.row_hash:
                        self._choice_index.pop(token, None)
            if state and pair_id in self._last_sheet_specs:
                self._last_sheet_specs.pop(pair_id, None)
            removed_state = state
        if stop_runtime:
            try:
                await self.pair_controller.stop_pair(pair_id, reason=reason or "removed_by_command")
            except Exception as exc:
                self.logger.warn("failed to stop pair on remove", pair_id=pair_id, error=str(exc))
        try:
            current_pairs = await self.pair_store.list_pairs()
            filtered = [pair for pair in current_pairs if (pair.pair_id or pair.event_id) != pair_id]
            if len(filtered) != len(current_pairs):
                await self.pair_store.update_pairs(filtered)
        except Exception:
            pass
        self._save_state()
        return bool(removed_state)

    def snapshot_pairs(self) -> List[PairState]:
        return list(self._state.values())

    def last_summary(self) -> Optional[SyncSummary]:
        return self._last_summary

    def get_pair(self, pair_id: str) -> Optional[PairState]:
        return self._state.get(pair_id)

    def set_notify_chat(self, chat_id: str) -> None:
        """Allow runtime selection of chat when config/chat_id is not preset."""
        if chat_id:
            self._dynamic_chat_id = str(chat_id)


