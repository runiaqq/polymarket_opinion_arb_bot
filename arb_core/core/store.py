"""
SQLite store for pair management.

Thread-safe implementation with WAL mode for concurrent access.
"""

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

from .logging import get_logger
from .models import Pair, PairStatus, Trade, TradeStatus, compute_pair_id, generate_trade_id

logger = get_logger(__name__)


class StoreError(Exception):
    """Base exception for store errors."""

    pass


class InvalidTransitionError(StoreError):
    """Raised when an invalid status transition is attempted."""

    pass


class PairNotFoundError(StoreError):
    """Raised when a pair is not found."""

    pass


class PairStore:
    """
    SQLite-based pair storage with status transitions.
    
    Thread-safe implementation using:
    - Global lock for write operations
    - WAL mode for better concurrent reads
    - Foreign key enforcement
    """

    # Valid status transitions
    VALID_TRANSITIONS = {
        PairStatus.DISCOVERED: {PairStatus.PM_SELECTED, PairStatus.DISABLED, PairStatus.ERROR},
        PairStatus.PM_SELECTED: {
            PairStatus.READY,
            PairStatus.DISCOVERED,
            PairStatus.DISABLED,
            PairStatus.ERROR,
        },
        PairStatus.READY: {
            PairStatus.ACTIVE,
            PairStatus.DISCOVERED,
            PairStatus.DISABLED,
            PairStatus.ERROR,
        },
        PairStatus.ACTIVE: {
            PairStatus.READY,
            PairStatus.DISCOVERED,
            PairStatus.DISABLED,
            PairStatus.ERROR,
        },
        PairStatus.DISABLED: {PairStatus.DISCOVERED, PairStatus.ERROR},
        PairStatus.ERROR: {PairStatus.DISCOVERED, PairStatus.DISABLED},
    }

    def __init__(self, db_path: str):
        """Initialize store with database path."""
        self.db_path = db_path
        self._lock = threading.RLock()  # Reentrant lock for nested calls
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pairs (
                    pair_id TEXT PRIMARY KEY,
                    polymarket_url TEXT NOT NULL,
                    opinion_url TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'DISCOVERED',
                    pm_side TEXT,
                    op_side TEXT,
                    pm_token TEXT,
                    op_token TEXT,
                    op_question_id TEXT,
                    max_position REAL DEFAULT 15.0,
                    min_profit_percent REAL DEFAULT 0.0,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pairs_status ON pairs(status)"
            )

            # Migration: add op_question_id column if missing
            cursor = conn.execute("PRAGMA table_info(pairs)")
            columns = [row[1] for row in cursor.fetchall()]
            if "op_question_id" not in columns:
                conn.execute("ALTER TABLE pairs ADD COLUMN op_question_id TEXT")
                logger.info("Migration: added op_question_id column to pairs table")

            # Trade journal table for Market-Hedge Mode
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    pair_id TEXT NOT NULL,
                    account_id TEXT,
                    
                    -- Entry leg (the limit order that was filled)
                    entry_exchange TEXT NOT NULL,
                    entry_order_id TEXT,
                    entry_side TEXT NOT NULL,
                    entry_size REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    entry_filled_at TEXT,
                    
                    -- Hedge leg (the market order placed after fill)
                    hedge_exchange TEXT,
                    hedge_order_id TEXT,
                    hedge_side TEXT,
                    hedge_size REAL,
                    hedge_price REAL,
                    hedge_filled_at TEXT,
                    hedge_slippage REAL,
                    
                    -- Status and result
                    status TEXT NOT NULL DEFAULT 'pending',
                    pnl REAL,
                    pnl_percent REAL,
                    fees_total REAL DEFAULT 0,
                    error_message TEXT,
                    
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    
                    FOREIGN KEY (pair_id) REFERENCES pairs(pair_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_pair_id ON trades(pair_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)"
            )

            conn.commit()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """
        Context manager for database connections.
        
        Enables WAL mode for better concurrency and foreign key constraints.
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL")
        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    def upsert_pair(
        self,
        pair_id: str,
        pm_url: str,
        op_url: str,
        status: PairStatus = PairStatus.DISCOVERED,
        max_position: float = 15.0,
        min_profit_percent: float = 0.0,
    ) -> Pair:
        """
        Insert or update a pair.

        For existing pairs, only updates URLs and position settings,
        preserving status and selections.
        
        Thread-safe: uses lock for write operations.
        """
        now = datetime.utcnow().isoformat()

        with self._lock:  # Thread safety
            with self._connection() as conn:
                # Use INSERT OR REPLACE pattern for atomic upsert
                existing = conn.execute(
                    "SELECT * FROM pairs WHERE pair_id = ?", (pair_id,)
                ).fetchone()

                if existing:
                    # Update existing pair - preserve status and selections
                    conn.execute(
                        """
                        UPDATE pairs SET
                            polymarket_url = ?,
                            opinion_url = ?,
                            max_position = ?,
                            min_profit_percent = ?,
                            updated_at = ?
                        WHERE pair_id = ?
                        """,
                        (pm_url, op_url, max_position, min_profit_percent, now, pair_id),
                    )
                else:
                    # Insert new pair
                    conn.execute(
                        """
                        INSERT INTO pairs (
                            pair_id, polymarket_url, opinion_url, status,
                            max_position, min_profit_percent, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            pair_id,
                            pm_url,
                            op_url,
                            status.value,
                            max_position,
                            min_profit_percent,
                            now,
                            now,
                        ),
                    )

                conn.commit()

                # Fetch and return the pair
                row = conn.execute(
                    "SELECT * FROM pairs WHERE pair_id = ?", (pair_id,)
                ).fetchone()
                return Pair.from_row(dict(row))

    def get_pair(self, pair_id: str) -> Optional[Pair]:
        """Get a pair by ID."""
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM pairs WHERE pair_id = ?", (pair_id,)
            ).fetchone()
            if row:
                return Pair.from_row(dict(row))
            return None

    def get_pair_by_prefix(self, prefix: str) -> Optional[Pair]:
        """
        Get a pair by ID prefix.

        Used for shortened callback_data in Telegram buttons.
        Returns the first matching pair if prefix is unique enough.
        """
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM pairs WHERE pair_id LIKE ? LIMIT 2",
                (prefix + "%",)
            ).fetchall()

            if len(rows) == 1:
                return Pair.from_row(dict(rows[0]))
            elif len(rows) > 1:
                logger.warning(f"Multiple pairs match prefix {prefix}, returning first")
                return Pair.from_row(dict(rows[0]))
            return None

    def list_pairs(self, statuses: Optional[list[PairStatus]] = None) -> list[Pair]:
        """List pairs, optionally filtered by status."""
        with self._connection() as conn:
            if statuses:
                placeholders = ",".join("?" * len(statuses))
                status_values = [s.value for s in statuses]
                rows = conn.execute(
                    f"SELECT * FROM pairs WHERE status IN ({placeholders}) ORDER BY updated_at DESC",
                    status_values,
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM pairs ORDER BY updated_at DESC"
                ).fetchall()

            return [Pair.from_row(dict(row)) for row in rows]

    def count_by_status(self) -> dict[PairStatus, int]:
        """Get count of pairs grouped by status."""
        counts = {status: 0 for status in PairStatus}
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM pairs GROUP BY status"
            ).fetchall()
            for row in rows:
                try:
                    status = PairStatus(row["status"])
                    counts[status] = row["cnt"]
                except ValueError:
                    pass  # Unknown status, skip
        return counts

    def _validate_transition(
        self, current: PairStatus, target: PairStatus
    ) -> None:
        """Validate that a status transition is allowed."""
        if current == target:
            return  # No change is always OK

        valid_targets = self.VALID_TRANSITIONS.get(current, set())
        if target not in valid_targets:
            raise InvalidTransitionError(
                f"Cannot transition from {current.value} to {target.value}"
            )

    def _update_status(
        self,
        pair_id: str,
        new_status: PairStatus,
        updates: Optional[dict] = None,
    ) -> Pair:
        """
        Internal method to update pair status with optional field updates.
        
        Thread-safe: uses lock for atomic read-validate-write.
        """
        now = datetime.utcnow().isoformat()

        with self._lock:  # Thread safety - atomic read-validate-write
            with self._connection() as conn:
                row = conn.execute(
                    "SELECT * FROM pairs WHERE pair_id = ?", (pair_id,)
                ).fetchone()

                if not row:
                    raise PairNotFoundError(f"Pair not found: {pair_id}")

                pair = Pair.from_row(dict(row))
                self._validate_transition(pair.status, new_status)

                # Build update query
                set_clauses = ["status = ?", "updated_at = ?"]
                params = [new_status.value, now]

                if updates:
                    for key, value in updates.items():
                        set_clauses.append(f"{key} = ?")
                        params.append(value)

                params.append(pair_id)

                conn.execute(
                    f"UPDATE pairs SET {', '.join(set_clauses)} WHERE pair_id = ?",
                    params,
                )
                conn.commit()

                # Return updated pair
                row = conn.execute(
                    "SELECT * FROM pairs WHERE pair_id = ?", (pair_id,)
                ).fetchone()
                return Pair.from_row(dict(row))

    def set_pm_selection(
        self, pair_id: str, side: str, token: Optional[str] = None
    ) -> Pair:
        """
        Set Polymarket side selection.

        Transitions to PM_SELECTED unless already READY/ACTIVE.
        """
        if side not in ("YES", "NO"):
            raise StoreError(f"Invalid side: {side}. Must be YES or NO")

        pair = self.get_pair(pair_id)
        if not pair:
            raise PairNotFoundError(f"Pair not found: {pair_id}")

        updates = {"pm_side": side}
        if token:
            updates["pm_token"] = token

        # Determine target status
        if pair.status in (PairStatus.READY, PairStatus.ACTIVE):
            # Keep current status, just update selection
            target_status = pair.status
        else:
            target_status = PairStatus.PM_SELECTED

        return self._update_status(pair_id, target_status, updates)

    def set_op_selection(
        self,
        pair_id: str,
        side: str,
        token: Optional[str] = None,
        question_id: Optional[str] = None,
    ) -> Pair:
        """
        Set Opinion side selection.

        Transitions to READY if PM is already selected.
        Requires PM to be selected first.

        Args:
            pair_id: The pair ID
            side: "YES" or "NO"
            token: The Opinion token ID (yesPos or noPos)
            question_id: The Opinion questionId for orderbook fetching
        """
        if side not in ("YES", "NO"):
            raise StoreError(f"Invalid side: {side}. Must be YES or NO")

        pair = self.get_pair(pair_id)
        if not pair:
            raise PairNotFoundError(f"Pair not found: {pair_id}")

        # Check PM is selected
        if pair.status == PairStatus.DISCOVERED:
            raise InvalidTransitionError(
                "Cannot set Opinion selection before Polymarket selection"
            )

        updates = {"op_side": side}
        if token:
            updates["op_token"] = token
        if question_id:
            updates["op_question_id"] = question_id

        # Determine target status
        if pair.status == PairStatus.ACTIVE:
            # Keep active, just update selection
            target_status = PairStatus.ACTIVE
        elif pair.pm_side:
            # PM is selected, transition to READY
            target_status = PairStatus.READY
        else:
            # Should not happen, but handle gracefully
            target_status = pair.status

        return self._update_status(pair_id, target_status, updates)

    def activate(self, pair_id: str) -> Pair:
        """
        Activate a pair for trading.

        Only allowed from READY status.
        """
        pair = self.get_pair(pair_id)
        if not pair:
            raise PairNotFoundError(f"Pair not found: {pair_id}")

        if pair.status != PairStatus.READY:
            raise InvalidTransitionError(
                f"Can only activate from READY status, current: {pair.status.value}"
            )

        return self._update_status(pair_id, PairStatus.ACTIVE)

    def deactivate(self, pair_id: str) -> Pair:
        """
        Deactivate a pair (ACTIVE -> READY).
        """
        pair = self.get_pair(pair_id)
        if not pair:
            raise PairNotFoundError(f"Pair not found: {pair_id}")

        if pair.status != PairStatus.ACTIVE:
            raise InvalidTransitionError(
                f"Can only deactivate from ACTIVE status, current: {pair.status.value}"
            )

        return self._update_status(pair_id, PairStatus.READY)

    def reset_selection(self, pair_id: str) -> Pair:
        """
        Reset pair to DISCOVERED status, clearing all selections and tokens.
        """
        pair = self.get_pair(pair_id)
        if not pair:
            raise PairNotFoundError(f"Pair not found: {pair_id}")

        # If active, must deactivate first (but we allow reset from any status for flexibility)
        updates = {
            "pm_side": None,
            "op_side": None,
            "pm_token": None,
            "op_token": None,
            "op_question_id": None,
            "error_message": None,
        }

        return self._update_status(pair_id, PairStatus.DISCOVERED, updates)

    def mark_disabled(self, pair_id: str) -> Pair:
        """Mark a pair as disabled."""
        return self._update_status(pair_id, PairStatus.DISABLED)

    def mark_error(self, pair_id: str, message: str) -> Pair:
        """Mark a pair as error with message."""
        return self._update_status(
            pair_id, PairStatus.ERROR, {"error_message": message}
        )

    def delete_pair(self, pair_id: str) -> bool:
        """
        Delete a pair from the database.
        
        Returns True if deleted, False if not found.
        Thread-safe: uses lock for write operations.
        """
        with self._lock:  # Thread safety
            with self._connection() as conn:
                cursor = conn.execute(
                    "DELETE FROM pairs WHERE pair_id = ?",
                    (pair_id,),
                )
                conn.commit()
                return cursor.rowcount > 0

    # ==================== Trade Journal Methods ====================

    def create_trade(
        self,
        pair_id: str,
        entry_exchange: str,
        entry_side: str,
        entry_size: float,
        entry_price: float,
        entry_order_id: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> Trade:
        """
        Create a new trade entry (when limit order is placed).
        
        Returns the created Trade object.
        Thread-safe: uses lock for write operations.
        """
        trade_id = generate_trade_id()
        now = datetime.utcnow().isoformat()
        
        with self._lock:  # Thread safety
            with self._connection() as conn:
                conn.execute(
                    """
                    INSERT INTO trades (
                        trade_id, pair_id, account_id,
                        entry_exchange, entry_order_id, entry_side, entry_size, entry_price,
                        status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade_id, pair_id, account_id,
                        entry_exchange, entry_order_id, entry_side, entry_size, entry_price,
                        TradeStatus.PENDING.value, now, now,
                    ),
                )
                conn.commit()
        
        return Trade(
            trade_id=trade_id,
            pair_id=pair_id,
            account_id=account_id,
            entry_exchange=entry_exchange,
            entry_order_id=entry_order_id,
            entry_side=entry_side,
            entry_size=entry_size,
            entry_price=entry_price,
            status=TradeStatus.PENDING,
            created_at=datetime.fromisoformat(now),
            updated_at=datetime.fromisoformat(now),
        )

    def update_trade_entry_filled(
        self,
        trade_id: str,
        filled_size: float,
        filled_price: float,
    ) -> Optional[Trade]:
        """Update trade when entry leg is filled. Thread-safe."""
        now = datetime.utcnow().isoformat()
        
        with self._lock:  # Thread safety
            with self._connection() as conn:
                conn.execute(
                    """
                    UPDATE trades SET
                        entry_size = ?,
                        entry_price = ?,
                        entry_filled_at = ?,
                        status = ?,
                        updated_at = ?
                    WHERE trade_id = ?
                    """,
                    (filled_size, filled_price, now, TradeStatus.ENTRY_FILLED.value, now, trade_id),
                )
                conn.commit()
        
        return self.get_trade(trade_id)

    def update_trade_hedged(
        self,
        trade_id: str,
        hedge_exchange: str,
        hedge_order_id: str,
        hedge_side: str,
        hedge_size: float,
        hedge_price: float,
        hedge_slippage: float = 0.0,
        pm_fee_rate: float = 0.0,
        op_fee_rate: float = 0.0,
        fees_override: Optional[float] = None,  # Pass pre-calculated fees
    ) -> Optional[Trade]:
        """Update trade when hedge leg is filled. Thread-safe."""
        now = datetime.utcnow().isoformat()

        with self._lock:  # Thread safety - lock for entire operation
            # Calculate PnL with proper fee calculation
            trade = self.get_trade(trade_id)
            if trade:
                # For covered arb: payout = 1.0 per share
                # Investment = entry_size * entry_price + hedge_size * hedge_price
                # Payout = min(entry_size, hedge_size) * 1.0
                
                entry_cost = trade.entry_size * trade.entry_price
                hedge_cost = hedge_size * hedge_price
                investment = entry_cost + hedge_cost
                
                if fees_override is not None:
                    # Use pre-calculated fees (e.g., with Opinion minimum $0.50)
                    fees_total = fees_override
                else:
                    # Calculate fees based on rates
                    if trade.entry_exchange == "PM":
                        entry_fee = entry_cost * pm_fee_rate
                        hedge_fee = hedge_cost * op_fee_rate
                    else:
                        entry_fee = entry_cost * op_fee_rate
                        hedge_fee = hedge_cost * pm_fee_rate
                    fees_total = entry_fee + hedge_fee
                
                # Payout is 1.0 per share for covered position
                payout = min(trade.entry_size, hedge_size) * 1.0
                pnl = payout - investment - fees_total
                pnl_percent = (pnl / investment * 100) if investment > 0 else 0
            else:
                pnl = 0
                pnl_percent = 0
                fees_total = 0
            
            with self._connection() as conn:
                conn.execute(
                    """
                    UPDATE trades SET
                        hedge_exchange = ?,
                        hedge_order_id = ?,
                        hedge_side = ?,
                        hedge_size = ?,
                        hedge_price = ?,
                        hedge_filled_at = ?,
                        hedge_slippage = ?,
                        status = ?,
                        pnl = ?,
                        pnl_percent = ?,
                        fees_total = ?,
                        updated_at = ?
                    WHERE trade_id = ?
                    """,
                    (
                        hedge_exchange, hedge_order_id, hedge_side, hedge_size, hedge_price,
                        now, hedge_slippage, TradeStatus.HEDGED.value, pnl, pnl_percent, 
                        fees_total, now, trade_id,
                    ),
                )
                conn.commit()

            return self.get_trade(trade_id)

    def update_trade_failed(self, trade_id: str, error_message: str) -> Optional[Trade]:
        """Mark a trade as failed. Thread-safe."""
        now = datetime.utcnow().isoformat()
        
        with self._lock:  # Thread safety
            with self._connection() as conn:
                conn.execute(
                    """
                    UPDATE trades SET
                        status = ?,
                        error_message = ?,
                        updated_at = ?
                    WHERE trade_id = ?
                    """,
                    (TradeStatus.FAILED.value, error_message, now, trade_id),
                )
                conn.commit()
        
        return self.get_trade(trade_id)

    def get_trade(self, trade_id: str) -> Optional[Trade]:
        """Get a trade by ID."""
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM trades WHERE trade_id = ?", (trade_id,)
            ).fetchone()
            
            if row:
                return Trade.from_row(dict(row))
        
        return None

    def get_pending_trades(self, pair_id: Optional[str] = None) -> list[Trade]:
        """Get all pending trades (entry placed, waiting for fill)."""
        with self._connection() as conn:
            if pair_id:
                rows = conn.execute(
                    "SELECT * FROM trades WHERE status = ? AND pair_id = ?",
                    (TradeStatus.PENDING.value, pair_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM trades WHERE status = ?",
                    (TradeStatus.PENDING.value,),
                ).fetchall()
            
            return [Trade.from_row(dict(row)) for row in rows]

    def get_trades_for_pair(self, pair_id: str, limit: int = 50) -> list[Trade]:
        """Get recent trades for a pair."""
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE pair_id = ? ORDER BY created_at DESC LIMIT ?",
                (pair_id, limit),
            ).fetchall()
            
            return [Trade.from_row(dict(row)) for row in rows]

    def get_trade_summary(self) -> dict:
        """Get summary of all trades."""
        with self._connection() as conn:
            # Get counts by status
            rows = conn.execute(
                """
                SELECT 
                    status,
                    COUNT(*) as count,
                    COALESCE(SUM(pnl), 0) as total_pnl,
                    COALESCE(SUM(fees_total), 0) as total_fees
                FROM trades
                GROUP BY status
                """
            ).fetchall()
            
            completed_count = 0
            pending_count = 0
            failed_count = 0
            total_pnl = 0.0
            total_fees = 0.0
            
            for row in rows:
                status = row["status"]
                count = row["count"]
                pnl = row["total_pnl"] or 0
                fees = row["total_fees"] or 0
                
                if status == "hedged":
                    completed_count = count
                    total_pnl = pnl
                    total_fees = fees
                elif status == "pending" or status == "entry_filled":
                    pending_count += count
                elif status == "failed":
                    failed_count = count
            
            avg_pnl = total_pnl / completed_count if completed_count > 0 else 0
            
            return {
                "completed_count": completed_count,
                "pending_count": pending_count,
                "failed_count": failed_count,
                "total_pnl": total_pnl,
                "total_fees": total_fees,
                "avg_pnl": avg_pnl,
            }

    def get_recent_trades(self, limit: int = 10) -> list:
        """Get most recent trades."""
        from .models import Trade
        
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            
            return [Trade.from_row(dict(row)) for row in rows]

    def get_unfinished_trades(self) -> list[Trade]:
        """
        Get all unfinished trades (pending or entry_filled).
        
        Used for crash recovery to rebuild active orders state.
        """
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM trades 
                WHERE status IN (?, ?)
                ORDER BY created_at ASC
                """,
                (TradeStatus.PENDING.value, TradeStatus.ENTRY_FILLED.value),
            ).fetchall()
            
            return [Trade.from_row(dict(row)) for row in rows]

    def mark_trade_cancelled(self, trade_id: str, reason: str = "Cancelled on restart") -> Optional[Trade]:
        """
        Mark a trade as cancelled (used during crash recovery).
        Thread-safe.
        """
        now = datetime.utcnow().isoformat()
        
        with self._lock:
            with self._connection() as conn:
                conn.execute(
                    """
                    UPDATE trades SET
                        status = ?,
                        error_message = ?,
                        updated_at = ?
                    WHERE trade_id = ?
                    """,
                    (TradeStatus.FAILED.value, reason, now, trade_id),
                )
                conn.commit()
        
        return self.get_trade(trade_id)
