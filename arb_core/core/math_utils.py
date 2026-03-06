"""
Covered Arbitrage Math Utilities.

Covered position: BUY YES on Platform A + BUY NO on Platform B.
Payout at resolution = 1 per share (one leg wins, other loses).
Profit locked if total entry cost < 1.
"""

from dataclasses import dataclass
from typing import Optional

from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class CoveredArbQuote:
    """Quote for a covered arbitrage opportunity."""

    # Prices (best ask)
    pm_ask: float
    op_ask: float

    # Fees (maker fees, configurable)
    pm_fee: float = 0.0
    op_fee: float = 0.0

    # Depth available at best ask
    pm_depth: float = 0.0
    op_depth: float = 0.0

    # Balance constraints
    pm_balance: float = float("inf")
    op_balance: float = float("inf")

    # Market constraints
    pm_min_size: float = 0.0
    op_min_size: float = 0.0

    @property
    def cost_per_share(self) -> float:
        """Total cost per share (both legs)."""
        return self.pm_ask + self.op_ask

    @property
    def fees_per_share(self) -> float:
        """Total fees per share."""
        return self.pm_fee + self.op_fee

    @property
    def total_cost(self) -> float:
        """Total cost including fees."""
        return self.cost_per_share + self.fees_per_share

    @property
    def payout(self) -> float:
        """Guaranteed payout at resolution."""
        return 1.0

    @property
    def profit_per_share(self) -> float:
        """Expected profit per share."""
        return self.payout - self.total_cost

    @property
    def profit_percent(self) -> float:
        """Profit as percentage of investment."""
        if self.total_cost <= 0:
            return 0.0
        return (self.profit_per_share / self.total_cost) * 100

    def is_profitable(self, min_profit_percent: float = 0.0) -> bool:
        """
        Check if opportunity is profitable.

        Entry condition: total_cost <= (1 - min_profit_percent/100)
        Equivalent to: profit_per_share >= min_profit_percent/100
        """
        min_profit = min_profit_percent / 100.0
        return self.total_cost <= (1.0 - min_profit)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "pm_ask": self.pm_ask,
            "op_ask": self.op_ask,
            "pm_fee": self.pm_fee,
            "op_fee": self.op_fee,
            "cost_per_share": self.cost_per_share,
            "fees_per_share": self.fees_per_share,
            "total_cost": self.total_cost,
            "payout": self.payout,
            "profit_per_share": self.profit_per_share,
            "profit_percent": self.profit_percent,
            "pm_depth": self.pm_depth,
            "op_depth": self.op_depth,
        }


@dataclass
class SizeResult:
    """Result of size computation."""

    size: float
    skip_reason: Optional[str] = None

    # Limiting factors for debugging
    limited_by_max_position: bool = False
    limited_by_pm_depth: bool = False
    limited_by_op_depth: bool = False
    limited_by_pm_balance: bool = False
    limited_by_op_balance: bool = False
    limited_by_min_size: bool = False

    @property
    def is_valid(self) -> bool:
        """Check if size is valid for trading."""
        return self.size > 0 and self.skip_reason is None


def compute_entry_size(
    max_position: float,
    pm_depth: float,
    op_depth: float,
    pm_balance: float,
    op_balance: float,
    pm_ask: float,
    op_ask: float,
    pm_min_size: float = 0.0,
    op_min_size: float = 0.0,
) -> SizeResult:
    """
    Compute symmetric entry size for covered arbitrage.

    Size is the minimum of:
    - max_position (user setting)
    - available depth at best ask (PM)
    - available depth at best ask (OP)
    - balance-constrained size (PM): balance / ask_price
    - balance-constrained size (OP): balance / ask_price

    Args:
        max_position: Maximum position size allowed
        pm_depth: Available depth at best ask on Polymarket
        op_depth: Available depth at best ask on Opinion
        pm_balance: Available balance for PM
        op_balance: Available balance for OP
        pm_ask: Best ask price on PM
        op_ask: Best ask price on OP
        pm_min_size: Minimum order size on PM
        op_min_size: Minimum order size on OP

    Returns:
        SizeResult with computed size or skip reason
    """
    result = SizeResult(size=0.0)

    # Validate inputs
    if pm_ask <= 0 or op_ask <= 0:
        result.skip_reason = "invalid_prices"
        return result

    if pm_depth <= 0:
        result.skip_reason = "missing_depth_pm"
        return result

    if op_depth <= 0:
        result.skip_reason = "missing_depth_op"
        return result

    # Calculate balance-constrained sizes
    pm_balance_size = pm_balance / pm_ask if pm_ask > 0 else 0
    op_balance_size = op_balance / op_ask if op_ask > 0 else 0

    # Find minimum (symmetric size)
    candidates = [
        (max_position, "max_position"),
        (pm_depth, "pm_depth"),
        (op_depth, "op_depth"),
        (pm_balance_size, "pm_balance"),
        (op_balance_size, "op_balance"),
    ]

    # Find minimum and track limiting factor
    min_size = float("inf")
    limiting_factor = None

    for size, factor in candidates:
        if size < min_size:
            min_size = size
            limiting_factor = factor

    result.size = min_size

    # Track limiting factor
    if limiting_factor == "max_position":
        result.limited_by_max_position = True
    elif limiting_factor == "pm_depth":
        result.limited_by_pm_depth = True
    elif limiting_factor == "op_depth":
        result.limited_by_op_depth = True
    elif limiting_factor == "pm_balance":
        result.limited_by_pm_balance = True
    elif limiting_factor == "op_balance":
        result.limited_by_op_balance = True

    # Check minimum size constraints
    min_required = max(pm_min_size, op_min_size)
    if result.size < min_required:
        result.skip_reason = "size_below_min"
        result.limited_by_min_size = True
        return result

    # Floor to reasonable precision
    result.size = float(int(result.size * 100) / 100)  # 2 decimal places

    if result.size <= 0:
        result.skip_reason = "size_zero"

    return result


@dataclass
class SimulationResult:
    """Result of a covered arbitrage simulation."""

    # Quote info
    quote: CoveredArbQuote

    # Size info
    size_result: SizeResult

    # Trade info
    is_profitable: bool
    min_profit_percent: float

    # Computed values
    total_investment: float = 0.0
    expected_profit: float = 0.0
    expected_profit_pct: float = 0.0

    # Skip reason if not tradeable
    skip_reason: Optional[str] = None

    def __post_init__(self):
        """Compute derived values."""
        if self.size_result.is_valid and self.is_profitable:
            self.total_investment = self.size_result.size * self.quote.total_cost
            self.expected_profit = self.size_result.size * self.quote.profit_per_share
            if self.total_investment > 0:
                self.expected_profit_pct = (
                    self.expected_profit / self.total_investment
                ) * 100

    @property
    def is_tradeable(self) -> bool:
        """Check if this simulation result is tradeable."""
        return (
            self.is_profitable
            and self.size_result.is_valid
            and self.skip_reason is None
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "quote": self.quote.to_dict(),
            "size": self.size_result.size,
            "is_profitable": self.is_profitable,
            "is_tradeable": self.is_tradeable,
            "min_profit_percent": self.min_profit_percent,
            "total_investment": self.total_investment,
            "expected_profit": self.expected_profit,
            "expected_profit_pct": self.expected_profit_pct,
            "skip_reason": self.skip_reason or self.size_result.skip_reason,
        }


def simulate_covered_arb(
    quote: CoveredArbQuote,
    max_position: float,
    min_profit_percent: float = 0.0,
    pm_balance: float = float("inf"),
    op_balance: float = float("inf"),
    pm_min_size: float = 0.0,
    op_min_size: float = 0.0,
) -> SimulationResult:
    """
    Simulate a covered arbitrage trade.

    Args:
        quote: Quote with prices and depths
        max_position: Maximum position size
        min_profit_percent: Minimum required profit percentage
        pm_balance: Available balance for PM trades
        op_balance: Available balance for OP trades
        pm_min_size: Minimum order size on PM
        op_min_size: Minimum order size on OP

    Returns:
        SimulationResult with all computed values
    """
    # Check profitability first
    is_profitable = quote.is_profitable(min_profit_percent)

    if not is_profitable:
        return SimulationResult(
            quote=quote,
            size_result=SizeResult(size=0.0, skip_reason="not_profitable"),
            is_profitable=False,
            min_profit_percent=min_profit_percent,
            skip_reason="not_profitable",
        )

    # Compute size
    size_result = compute_entry_size(
        max_position=max_position,
        pm_depth=quote.pm_depth,
        op_depth=quote.op_depth,
        pm_balance=pm_balance,
        op_balance=op_balance,
        pm_ask=quote.pm_ask,
        op_ask=quote.op_ask,
        pm_min_size=pm_min_size,
        op_min_size=op_min_size,
    )

    return SimulationResult(
        quote=quote,
        size_result=size_result,
        is_profitable=is_profitable,
        min_profit_percent=min_profit_percent,
        skip_reason=size_result.skip_reason,
    )
