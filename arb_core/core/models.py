"""
Data models and enums for arb_core.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
import hashlib


class PairStatus(Enum):
    """Status of a trading pair in the system."""

    DISCOVERED = "DISCOVERED"  # Pair exists from Sheets but no selections
    PM_SELECTED = "PM_SELECTED"  # User selected Polymarket side, waiting for Opinion
    READY = "READY"  # Both sides selected + tokens stored
    ACTIVE = "ACTIVE"  # Trading enabled
    DISABLED = "DISABLED"  # Sheet row disabled
    ERROR = "ERROR"  # Pair invalid; keep record


class TradeStatus(Enum):
    """Status of a trade in Market-Hedge Mode."""

    PENDING = "pending"  # Entry limit order placed, waiting for fill
    ENTRY_FILLED = "entry_filled"  # Entry filled, hedge in progress
    HEDGED = "hedged"  # Both legs filled, trade complete
    PARTIAL = "partial"  # Partial fill, partial hedge
    FAILED = "failed"  # Hedge failed
    CANCELLED = "cancelled"  # Trade cancelled


@dataclass
class Pair:
    """Represents a trading pair between Polymarket and Opinion."""

    pair_id: str
    polymarket_url: str
    opinion_url: str
    status: PairStatus
    pm_side: Optional[str] = None  # YES or NO
    op_side: Optional[str] = None  # YES or NO
    pm_token: Optional[str] = None
    op_token: Optional[str] = None
    op_question_id: Optional[str] = None  # Opinion questionId for orderbook
    max_position: float = 15.0
    min_profit_percent: float = 0.0
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "pair_id": self.pair_id,
            "polymarket_url": self.polymarket_url,
            "opinion_url": self.opinion_url,
            "status": self.status.value,
            "pm_side": self.pm_side,
            "op_side": self.op_side,
            "pm_token": self.pm_token,
            "op_token": self.op_token,
            "op_question_id": self.op_question_id,
            "max_position": self.max_position,
            "min_profit_percent": self.min_profit_percent,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_row(cls, row: dict) -> "Pair":
        """Create Pair from database row."""
        return cls(
            pair_id=row["pair_id"],
            polymarket_url=row["polymarket_url"],
            opinion_url=row["opinion_url"],
            status=PairStatus(row["status"]),
            pm_side=row.get("pm_side"),
            op_side=row.get("op_side"),
            pm_token=row.get("pm_token"),
            op_token=row.get("op_token"),
            op_question_id=row.get("op_question_id"),
            max_position=row.get("max_position", 15.0),
            min_profit_percent=row.get("min_profit_percent", 0.0),
            error_message=row.get("error_message"),
            created_at=datetime.fromisoformat(row["created_at"])
            if row.get("created_at")
            else None,
            updated_at=datetime.fromisoformat(row["updated_at"])
            if row.get("updated_at")
            else None,
        )


@dataclass
class Trade:
    """Represents a trade in Market-Hedge Mode (entry + hedge)."""

    trade_id: str
    pair_id: str
    account_id: Optional[str] = None

    # Entry leg (limit order)
    entry_exchange: str = ""  # "PM" or "OP"
    entry_order_id: Optional[str] = None
    entry_side: str = ""  # "BUY" or "SELL"
    entry_size: float = 0.0
    entry_price: float = 0.0
    entry_filled_at: Optional[datetime] = None

    # Hedge leg (market order)
    hedge_exchange: Optional[str] = None
    hedge_order_id: Optional[str] = None
    hedge_side: Optional[str] = None
    hedge_size: Optional[float] = None
    hedge_price: Optional[float] = None
    hedge_filled_at: Optional[datetime] = None
    hedge_slippage: Optional[float] = None

    # Result
    status: TradeStatus = TradeStatus.PENDING
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    fees_total: float = 0.0
    error_message: Optional[str] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: dict) -> "Trade":
        """Create Trade from database row."""
        return cls(
            trade_id=row["trade_id"],
            pair_id=row["pair_id"],
            account_id=row.get("account_id"),
            entry_exchange=row.get("entry_exchange", ""),
            entry_order_id=row.get("entry_order_id"),
            entry_side=row.get("entry_side", ""),
            entry_size=row.get("entry_size", 0.0),
            entry_price=row.get("entry_price", 0.0),
            entry_filled_at=datetime.fromisoformat(row["entry_filled_at"])
            if row.get("entry_filled_at")
            else None,
            hedge_exchange=row.get("hedge_exchange"),
            hedge_order_id=row.get("hedge_order_id"),
            hedge_side=row.get("hedge_side"),
            hedge_size=row.get("hedge_size"),
            hedge_price=row.get("hedge_price"),
            hedge_filled_at=datetime.fromisoformat(row["hedge_filled_at"])
            if row.get("hedge_filled_at")
            else None,
            hedge_slippage=row.get("hedge_slippage"),
            status=TradeStatus(row.get("status", "pending")),
            pnl=row.get("pnl"),
            pnl_percent=row.get("pnl_percent"),
            fees_total=row.get("fees_total", 0.0),
            error_message=row.get("error_message"),
            created_at=datetime.fromisoformat(row["created_at"])
            if row.get("created_at")
            else None,
            updated_at=datetime.fromisoformat(row["updated_at"])
            if row.get("updated_at")
            else None,
        )


def compute_pair_id(pm_url: str, op_url: str) -> str:
    """
    Compute deterministic pair_id from URLs.

    pair_id = sha256(lower(pm_url.strip()) + "|" + lower(op_url.strip()))
    """
    normalized = f"{pm_url.strip().lower()}|{op_url.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def generate_trade_id() -> str:
    """Generate a unique trade ID."""
    import uuid
    return str(uuid.uuid4())
