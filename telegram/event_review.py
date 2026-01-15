from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from utils.logger import BotLogger


class _EventStore:
    """Lightweight in-memory store with optional SQLite persistence."""

    def __init__(self, db_path: str | Path | None = None):
        self._db_path = Path(db_path) if db_path else None
        self._events: Dict[str, Dict[str, object]] = {}
        if self._db_path:
            self._init_db()
            self._load()

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS event_reviews (event_id TEXT PRIMARY KEY, status TEXT, payload TEXT)"
            )

    def _load(self) -> None:
        if not self._db_path:
            return
        with sqlite3.connect(self._db_path) as conn:
            for row in conn.execute("SELECT event_id, status, payload FROM event_reviews"):
                event_id, status, payload = row
                try:
                    parsed = json.loads(payload) if payload else {}
                except Exception:
                    parsed = {}
                self._events[event_id] = {"status": status, "payload": parsed}

    def record(self, event_id: str, payload: object, status: str = "pending") -> None:
        self._events[event_id] = {"status": status, "payload": payload}
        if self._db_path:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO event_reviews(event_id, status, payload) VALUES (?, ?, ?)",
                    (event_id, status, json.dumps(payload, ensure_ascii=False)),
                )

    def set_status(self, event_id: str, status: str) -> None:
        if event_id not in self._events:
            self.record(event_id, payload={}, status=status)
            return
        payload = self._events[event_id].get("payload", {})
        self.record(event_id, payload, status=status)

    def summary(self) -> Dict[str, int]:
        counts = {"approved": 0, "rejected": 0, "pending": 0}
        for meta in self._events.values():
            status = meta.get("status", "pending")
            if status in counts:
                counts[status] += 1
        return counts

    def unverified(self) -> List[Dict[str, object]]:
        return [
            {"event_id": eid, "payload": meta.get("payload")}
            for eid, meta in self._events.items()
            if meta.get("status") == "pending"
        ]


class EventReviewHandler:
    """
    Minimal, real event review handler:
    - Stores review state in memory with optional SQLite persistence.
    - Provides approve/reject/summary/unverified helpers.
    - Never blocks notifier delivery.
    """

    def __init__(
        self,
        registry,
        approvals,
        notifier,
        logger: Optional[BotLogger] = None,
        db_path: str | Path | None = None,
    ):
        self.registry = registry
        self.approvals = approvals
        self.notifier = notifier
        self.logger = logger or BotLogger(__name__)
        self.store = _EventStore(db_path)

    async def send_strong_matches(self, chat_id: str) -> None:
        matches = getattr(self.registry, "strong_matches", None) or getattr(self.registry, "matches", [])
        if not matches:
            return
        for match in matches:
            match_id = self._match_id(match)
            self.store.record(match_id, payload=self._serialize(match), status="pending")
            msg = "Сильное совпадение\n" + self._format_match(match)
            await self._safe_notify(
                msg,
                chat_id=chat_id,
                reply_markup={"inline_keyboard": [["approve", "reject"]]},
            )

    async def handle_callback(self, chat_id: str, data: str) -> None:
        if data.startswith("event:approve:"):
            match_id = data.split(":", 2)[-1]
            await self.approve(match_id, chat_id=chat_id)
        elif data.startswith("event:reject:"):
            match_id = data.split(":", 2)[-1]
            await self.reject(match_id, chat_id=chat_id)

    async def approve(self, event_id: str, chat_id: str | None = None) -> None:
        match = self._find_match(event_id)
        op_id, pm_id = self._parse_match_id(event_id)
        title = getattr(match, "title", "") if match else event_id
        self.approvals.mark_approved(event_id, opinion_event_id=op_id, polymarket_event_id=pm_id, title=title)
        self.store.set_status(event_id, "approved")
        await self._safe_notify(f"Матч {event_id} подтверждено", chat_id=chat_id)

    async def reject(self, event_id: str, chat_id: str | None = None) -> None:
        match = self._find_match(event_id)
        op_id, pm_id = self._parse_match_id(event_id)
        title = getattr(match, "title", "") if match else event_id
        self.approvals.mark_rejected(event_id, opinion_event_id=op_id, polymarket_event_id=pm_id, title=title)
        self.store.set_status(event_id, "rejected")
        await self._safe_notify(f"Матч {event_id} отклонен", chat_id=chat_id)

    async def send_raw_events(self, chat_id: str, source: str) -> None:
        await self._safe_notify(f"сырые события ({source})", chat_id=chat_id)

    async def send_unverified_matches(self, chat_id: str) -> None:
        unverified = getattr(self.registry, "unverified_matches", []) or []
        for uv in unverified:
            match_id = self._match_id(uv.match)
            self.store.record(match_id, payload=self._serialize(uv.match), status="pending")
            msg = "Непроверенное совпадение\n" + self._format_match(uv.match)
            await self._safe_notify(
                msg,
                chat_id=chat_id,
                reply_markup={"inline_keyboard": [["approve", "reject"]]},
            )

    async def send_summary(self, chat_id: str) -> None:
        counts = self.store.summary()
        if sum(counts.values()) == 0:
            await self._safe_notify("Кандидатов нет", chat_id=chat_id)
            return
        lines = [
            "📋 Итоги ревью",
            f"✅ Подтверждено: {counts['approved']}",
            f"❌ Отклонено: {counts['rejected']}",
            f"🟡 В ожидании: {counts['pending']}",
        ]
        await self._safe_notify("\n".join(lines), chat_id=chat_id)

    def summary(self) -> Dict[str, int]:
        return self.store.summary()

    def unverified(self) -> List[Dict[str, object]]:
        return self.store.unverified()

    async def _safe_notify(self, message: str, chat_id: str | None = None, **kwargs) -> None:
        if not self.notifier:
            return
        try:
            await self.notifier.send_message(
                message,
                chat_id=chat_id,
                parse_mode=None,
                disable_web_page_preview=True,
                **kwargs,
            )
        except Exception:
            self.logger.warn("event review notification failed")

    def _match_id(self, match) -> str:
        if getattr(self.registry, "match_id", None):
            try:
                return self.registry.match_id(match)  # type: ignore[attr-defined]
            except Exception:
                pass
        return getattr(match, "id", None) or getattr(match, "match_id", None) or str(match)

    def _find_match(self, match_id: str):
        matches = getattr(self.registry, "matches", []) or []
        for m in matches:
            if getattr(self.registry, "match_id", None):
                try:
                    if self.registry.match_id(m) == match_id:  # type: ignore[attr-defined]
                        return m
                except Exception:
                    continue
        return None

    def _parse_match_id(self, match_id: str) -> tuple[str, str]:
        if "::" in match_id:
            a, b = match_id.split("::", 1)
            return a, b
        return match_id, match_id

    def _format_match(self, match) -> str:
        try:
            payload = match.__dict__
            return json.dumps(payload, ensure_ascii=False)
        except Exception:
            return str(match)

    def _serialize(self, obj) -> object:
        try:
            return json.loads(json.dumps(obj, default=lambda o: getattr(o, "__dict__", str(o))))
        except Exception:
            return str(obj)
