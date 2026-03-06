"""
Market-Hedge Mode Runner.

Implements the trading logic:
1. Place LIMIT orders on BOTH exchanges simultaneously
2. Monitor for fills (polling-based)
3. When one fills → cancel the other + place MARKET hedge
4. Log trades to journal

Key principle: Never have an unhedged position.

Safety features:
- Per-trade locks to prevent race conditions
- Crash recovery on startup
- Retry logic for hedge failures
- Alerts for unhedged positions
"""

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from ..core.logging import get_logger
from ..core.models import Pair, PairStatus, Trade, TradeStatus
from ..core.store import PairStore
from ..exchanges.exchange_clients import (
    ExchangeClients,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
)
from ..market_data.orderbook import OrderbookManager, PairOrderbooks

logger = get_logger(__name__)


# Maximum retry attempts for hedge placement
MAX_HEDGE_RETRIES = 3
# Delay between retry attempts (exponential backoff base)
RETRY_BASE_DELAY = 1.0


@dataclass
class MarketHedgeConfig:
    """Configuration for Market-Hedge Mode."""

    # Trading parameters
    hedge_ratio: float = 1.0  # 1:1 hedge
    max_slippage_market_hedge: float = 0.005  # 0.5 cents max slippage
    min_spread_for_entry: float = 0.002  # 0.2 cents minimum spread for entry

    # Position limits
    max_position_size_per_market: float = 50000.0
    max_position_size_per_event: float = 200000.0

    # Timing
    cancel_unfilled_after_sec: float = 60.0  # Cancel limit orders after 60s
    poll_interval_sec: float = 1.0  # Check for fills every 1s

    # Fees (for PnL calculation) - Updated based on real exchange docs:
    # Polymarket: 0% maker, 0% taker (most markets)
    # Opinion: 0% maker, 0-2% taker (+ $0.50 min fee per trade)
    # For limit orders (maker-maker): both are 0%
    pm_maker_fee: float = 0.0   # Polymarket maker fee (limit orders)
    pm_taker_fee: float = 0.0   # Polymarket taker fee (market orders)
    op_maker_fee: float = 0.0   # Opinion maker fee (limit orders) - FREE!
    op_taker_fee: float = 0.02  # Opinion taker fee (market orders) - up to 2%
    op_min_fee: float = 0.50    # Opinion minimum fee per trade ($0.50)
    
    # Minimum NET profit requirement (after Opinion's $0.50 fee is subtracted)
    min_net_profit: float = 0.10  # Require at least $0.10 net profit after fees

    # Safety
    allow_partial_fill_hedge: bool = True
    dry_run: bool = True


@dataclass
class ActiveOrder:
    """Represents an active limit order being monitored."""

    trade_id: str
    pair_id: str
    exchange: str  # "PM" or "OP"
    order_id: str
    side: OrderSide
    size: float
    price: float
    token_id: str
    topic_id: Optional[str] = None
    placed_at: float = field(default_factory=time.time)

    # For the opposite leg
    opposite_exchange: str = ""
    opposite_order_id: Optional[str] = None
    opposite_token_id: str = ""
    opposite_topic_id: Optional[str] = None
    opposite_price: float = 0.0  # Price of the opposite limit order


@dataclass
class HedgeResult:
    """Result of a hedge attempt."""

    success: bool
    entry_exchange: str
    entry_filled_size: float
    entry_filled_price: float
    hedge_exchange: str
    hedge_order_id: Optional[str] = None
    hedge_size: float = 0.0
    hedge_price: float = 0.0
    hedge_slippage: float = 0.0
    error: Optional[str] = None


class MarketHedgeRunner:
    """
    Runner for Market-Hedge Mode.

    Places dual limit orders and hedges with market orders when fills occur.
    
    Safety features:
    - Per-trade locks to prevent race conditions on simultaneous fills
    - Crash recovery on startup
    - Retry logic for hedge failures
    - Tracking of unhedged positions for manual intervention
    """

    def __init__(
        self,
        store: PairStore,
        clients: ExchangeClients,
        orderbook_manager: Optional[OrderbookManager] = None,
        config: Optional[MarketHedgeConfig] = None,
        on_trade_complete: Optional[Callable[[Trade], None]] = None,
        on_hedge_executed: Optional[Callable[[HedgeResult], None]] = None,
        on_unhedged_position: Optional[Callable[[dict], None]] = None,  # Alert callback
    ):
        self.store = store
        self.clients = clients
        self.orderbook_manager = orderbook_manager or OrderbookManager()
        self.config = config or MarketHedgeConfig()

        # Callbacks
        self.on_trade_complete = on_trade_complete
        self.on_hedge_executed = on_hedge_executed
        self.on_unhedged_position = on_unhedged_position  # Called when hedge fails

        # State
        self._running = False
        self._trading_enabled = False  # Trading disabled by default - must be enabled via /start_trading
        self._thread: Optional[threading.Thread] = None
        self._active_orders: dict[str, ActiveOrder] = {}  # order_id -> ActiveOrder
        self._order_lock = threading.Lock()
        
        # Per-trade locks to prevent race conditions on simultaneous fills
        self._trade_locks: dict[str, threading.Lock] = {}  # trade_id -> Lock
        self._trade_locks_lock = threading.Lock()  # Lock for accessing _trade_locks
        
        # Tracking of unhedged positions requiring manual intervention
        self._unhedged_positions: list[dict] = []
        
        # Cooldown tracking for failed pairs (pair_id -> timestamp when can retry)
        self._pair_cooldowns: dict[str, float] = {}
        self._cooldown_duration = 60.0  # 60 seconds cooldown after failure
        
    def _get_trade_lock(self, trade_id: str) -> threading.Lock:
        """Get or create a lock for a specific trade."""
        with self._trade_locks_lock:
            if trade_id not in self._trade_locks:
                self._trade_locks[trade_id] = threading.Lock()
            return self._trade_locks[trade_id]
    
    def _cleanup_trade_lock(self, trade_id: str) -> None:
        """Remove a trade lock when no longer needed."""
        with self._trade_locks_lock:
            self._trade_locks.pop(trade_id, None)
    
    def recover_pending_trades(self) -> int:
        """
        Recover pending trades from database on startup.
        
        For trades that were in-flight when the bot crashed:
        - Check order status on exchanges
        - Cancel open orders
        - Mark trades appropriately
        
        Returns number of trades recovered/cleaned up.
        """
        recovered = 0
        unfinished = self.store.get_unfinished_trades()
        
        if not unfinished:
            logger.info("No unfinished trades to recover")
            return 0
        
        logger.warning(f"Found {len(unfinished)} unfinished trades from previous session")
        
        for trade in unfinished:
            try:
                logger.info(f"Recovering trade {trade.trade_id[:12]}... status={trade.status.value}")
                
                if trade.status == TradeStatus.ENTRY_FILLED:
                    # Entry was filled but hedge wasn't completed - CRITICAL
                    logger.error(
                        f"CRITICAL: Trade {trade.trade_id[:12]} has ENTRY_FILLED but no hedge! "
                        f"Manual intervention required."
                    )
                    self._unhedged_positions.append({
                        "trade_id": trade.trade_id,
                        "exchange": trade.entry_exchange,
                        "size": trade.entry_size,
                        "price": trade.entry_price,
                        "cost": trade.entry_size * trade.entry_price,
                        "error": "Bot crashed after entry fill, before hedge",
                    })
                    # Alert via callback
                    if self.on_unhedged_position:
                        self.on_unhedged_position({
                            "trade_id": trade.trade_id,
                            "exchange": trade.entry_exchange,
                            "size": trade.entry_size,
                            "price": trade.entry_price,
                            "cost": trade.entry_size * trade.entry_price,
                            "error": "Bot crashed after entry fill, before hedge - MANUAL INTERVENTION REQUIRED",
                        })
                    
                elif trade.status == TradeStatus.PENDING:
                    # Try to cancel any open orders
                    if trade.entry_order_id:
                        try:
                            if trade.entry_exchange == "PM":
                                self.clients.pm_client.cancel_order(trade.entry_order_id)
                            else:
                                self.clients.op_client.cancel_order(trade.entry_order_id)
                            logger.info(f"Cancelled pending order {trade.entry_order_id}")
                        except Exception as e:
                            logger.warning(f"Could not cancel order {trade.entry_order_id}: {e}")
                    
                    # Mark as cancelled
                    self.store.mark_trade_cancelled(trade.trade_id, "Cancelled on bot restart")
                
                recovered += 1
                
            except Exception as e:
                logger.error(f"Error recovering trade {trade.trade_id[:12]}: {e}")
        
        if self._unhedged_positions:
            logger.error(f"CRITICAL: {len(self._unhedged_positions)} UNHEDGED POSITIONS require manual intervention!")
        
        return recovered

    def simulate_pair(self, pair: Pair) -> "SimulationResult":
        """
        Simulate a trade for a pair (for Telegram UI).
        
        Returns a SimulationResult with quotes, size, and profitability info.
        """
        from ..core.math_utils import CoveredArbQuote, SimulationResult, SizeResult
        
        # Validate pair has tokens
        if not pair.pm_token or not pair.op_token:
            quote = CoveredArbQuote(pm_ask=0, op_ask=0)
            return SimulationResult(
                quote=quote,
                size_result=SizeResult(size=0, skip_reason="missing_tokens"),
                is_profitable=False,
                min_profit_percent=pair.min_profit_percent,
                skip_reason="missing_tokens",
            )
        
        # Fetch orderbooks
        orderbooks = self.orderbook_manager.fetch_pair(
            pm_token=pair.pm_token,
            op_token=pair.op_token,
            op_question_id=pair.op_question_id,
            op_side=pair.op_side or "YES",
        )
        
        if not orderbooks.is_valid:
            quote = CoveredArbQuote(pm_ask=0, op_ask=0)
            return SimulationResult(
                quote=quote,
                size_result=SizeResult(size=0, skip_reason=orderbooks.error),
                is_profitable=False,
                min_profit_percent=pair.min_profit_percent,
                skip_reason=orderbooks.error or "invalid_orderbooks",
            )
        
        pm_ask = orderbooks.pm_orderbook.best_ask_price
        op_ask = orderbooks.op_orderbook.best_ask_price
        pm_depth = orderbooks.pm_orderbook.best_ask_size
        op_depth = orderbooks.op_orderbook.best_ask_size
        
        # Get balances
        pm_balance = self.clients.pm_client.get_balance().available
        op_balance = self.clients.op_client.get_balance().available
        
        pm_balance_size = pm_balance / pm_ask if pm_ask > 0 else 0
        op_balance_size = op_balance / op_ask if op_ask > 0 else 0
        
        # Calculate size
        size = min(
            pair.max_position,
            pm_depth,
            op_depth,
            pm_balance_size,
            op_balance_size,
            self.config.max_position_size_per_market,
        )
        
        # Build quote (fees are 0 for maker-maker mode)
        # Note: Opinion's $0.50 min fee is per-trade, not per-share - handled separately
        quote = CoveredArbQuote(
            pm_ask=pm_ask,
            op_ask=op_ask,
            pm_fee=0.0,  # Maker fee = 0%
            op_fee=0.0,  # Maker fee = 0%
            pm_depth=pm_depth,
            op_depth=op_depth,
        )
        
        # Check profitability
        is_profitable = self._is_profitable_entry(pm_ask, op_ask)
        
        # Build size result with limiting factor info
        size_result = SizeResult(size=size)
        if size == pair.max_position:
            size_result.limited_by_max_position = True
        elif size == pm_balance_size:
            size_result.limited_by_pm_balance = True
        elif size == op_balance_size:
            size_result.limited_by_op_balance = True
        elif size == pm_depth:
            size_result.limited_by_pm_depth = True
        elif size == op_depth:
            size_result.limited_by_op_depth = True
        
        # Check minimum dollar amounts
        pm_dollar = size * pm_ask
        op_dollar = size * op_ask
        skip_reason = None
        
        if pm_dollar < 1.0:
            skip_reason = f"PM order too small: ${pm_dollar:.2f} < $1.00"
            size_result.skip_reason = skip_reason
        elif op_dollar < 1.0:
            skip_reason = f"OP order too small: ${op_dollar:.2f} < $1.00"
            size_result.skip_reason = skip_reason
        elif not is_profitable:
            skip_reason = "not_profitable"
        
        return SimulationResult(
            quote=quote,
            size_result=size_result,
            is_profitable=is_profitable and skip_reason is None,
            min_profit_percent=pair.min_profit_percent,
            skip_reason=skip_reason,
        )

    def _calculate_spread(self, pm_price: float, op_price: float, use_maker_fees: bool = True) -> float:
        """
        Calculate net spread after fees.
        
        For covered arb with LIMIT orders (maker-maker):
        - Polymarket maker: 0%
        - Opinion maker: 0%
        - Total fees: 0%
        
        For market orders (taker), fees apply.
        """
        total_cost = pm_price + op_price
        
        if use_maker_fees:
            # Maker-maker: both limit orders = 0% fees on both exchanges
            fees = self.config.pm_maker_fee + self.config.op_maker_fee
        else:
            # Taker scenario (market orders)
            fees = self.config.pm_taker_fee + self.config.op_taker_fee
        
        return 1.0 - total_cost - fees

    def _is_profitable_entry(
        self, pm_ask: float, op_ask: float
    ) -> bool:
        """Check if entry is profitable given current prices."""
        spread = self._calculate_spread(pm_ask, op_ask)
        return spread >= self.config.min_spread_for_entry

    def place_dual_orders(self, pair: Pair) -> Optional[str]:
        """
        Place limit orders on BOTH exchanges simultaneously.

        Returns trade_id if orders placed, None otherwise.
        """
        # Check if trading is enabled
        if not self._trading_enabled:
            return None  # Silent skip - monitoring mode
        
        # Check cooldown
        if pair.pair_id in self._pair_cooldowns:
            if time.time() < self._pair_cooldowns[pair.pair_id]:
                logger.debug(f"Pair {pair.pair_id[:12]} in cooldown, skipping")
                return None
            else:
                # Cooldown expired, remove it
                del self._pair_cooldowns[pair.pair_id]
        
        # Validate pair
        if not pair.pm_token or not pair.op_token:
            logger.warning(f"Pair {pair.pair_id[:12]} missing tokens")
            return None

        # Fetch orderbooks
        orderbooks = self.orderbook_manager.fetch_pair(
            pm_token=pair.pm_token,
            op_token=pair.op_token,
            op_question_id=pair.op_question_id,
            op_side=pair.op_side or "YES",
        )

        if not orderbooks.is_valid:
            logger.warning(
                f"Pair {pair.pair_id[:12]} invalid orderbooks: {orderbooks.error}"
            )
            return None

        pm_ask = orderbooks.pm_orderbook.best_ask_price
        op_ask = orderbooks.op_orderbook.best_ask_price

        # Check profitability at best prices first
        spread = self._calculate_spread(pm_ask, op_ask)
        logger.info(
            f"Pair {pair.pair_id[:12]} prices: PM={pm_ask:.4f} OP={op_ask:.4f} "
            f"sum={pm_ask + op_ask:.4f} spread={spread:.4f}"
        )
        
        if not self._is_profitable_entry(pm_ask, op_ask):
            logger.info(
                f"Pair {pair.pair_id[:12]} not profitable: spread {spread:.4f} < {self.config.min_spread_for_entry}"
            )
            return None

        # Get AGGREGATED depth (up to 5% slippage) - this is what arb sites show
        MAX_SLIPPAGE_PCT = 0.05  # 5% max slippage for depth aggregation
        pm_depth, pm_avg_price = orderbooks.pm_orderbook.get_aggregated_ask_depth(MAX_SLIPPAGE_PCT)
        op_depth, op_avg_price = orderbooks.op_orderbook.get_aggregated_ask_depth(MAX_SLIPPAGE_PCT)
        
        # Log aggregated depths for debugging
        pm_best_depth = orderbooks.pm_orderbook.best_ask_size
        op_best_depth = orderbooks.op_orderbook.best_ask_size
        
        # Always log if aggregated > best (shows we're using more depth)
        if pm_depth > pm_best_depth * 1.01:
            logger.info(f"Pair {pair.pair_id[:12]} PM aggregated: {pm_depth:.2f} shares (best: {pm_best_depth:.2f}) @ avg ${pm_avg_price:.4f}")
        if op_depth > op_best_depth * 1.01:
            logger.info(f"Pair {pair.pair_id[:12]} OP aggregated: {op_depth:.2f} shares (best: {op_best_depth:.2f}) @ avg ${op_avg_price:.4f}")

        pm_balance = self.clients.pm_client.get_balance().available
        op_balance = self.clients.op_client.get_balance().available
        
        # Apply 1% safety margin to avoid floating point issues
        SAFETY_MARGIN = 0.99
        pm_safe_balance = pm_balance * SAFETY_MARGIN
        op_safe_balance = op_balance * SAFETY_MARGIN

        pm_balance_size = pm_safe_balance / pm_ask if pm_ask > 0 else 0
        op_balance_size = op_safe_balance / op_ask if op_ask > 0 else 0

        # Log all size factors for debugging
        logger.info(
            f"Pair {pair.pair_id[:12]} size factors: "
            f"max_pos={pair.max_position:.2f} pm_depth={pm_depth:.2f} op_depth={op_depth:.2f} "
            f"pm_bal_size={pm_balance_size:.2f} op_bal_size={op_balance_size:.2f}"
        )

        # Round down to 2 decimal places to avoid precision issues
        size = math.floor(min(
            pair.max_position,
            pm_depth,
            op_depth,
            pm_balance_size,
            op_balance_size,
            self.config.max_position_size_per_market,
        ) * 100) / 100

        # Check minimum order size (in shares, not dollars)
        pm_min = self.clients.pm_client.get_min_order_size()
        op_min = self.clients.op_client.get_min_order_size()
        min_size = max(pm_min, op_min)

        if size < min_size:
            logger.info(
                f"Pair {pair.pair_id[:12]} size too small: {size:.2f} shares < {min_size:.2f} min"
            )
            return None

        # Create trade record
        # For covered arb, we BUY on both sides (YES on PM, NO on OP or vice versa)
        pm_side = OrderSide.BUY
        op_side = OrderSide.BUY

        trade = self.store.create_trade(
            pair_id=pair.pair_id,
            entry_exchange="PM",  # PM is the entry side
            entry_side="BUY",
            entry_size=size,
            entry_price=pm_ask,  # PM price (will be updated on fill)
        )

        logger.info(
            f"Placing dual orders for {pair.pair_id[:12]}: "
            f"size={size:.2f} PM@{pm_ask:.4f} OP@{op_ask:.4f}"
        )

        # Extract topic_id for Opinion
        op_topic_id = None
        if pair.opinion_url:
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(pair.opinion_url)
            qs = parse_qs(parsed.query)
            if "topicId" in qs:
                op_topic_id = qs["topicId"][0]

        # Place PM limit order
        pm_order_req = OrderRequest(
            token_id=pair.pm_token,
            side=pm_side,
            size=size,
            price=pm_ask,
            order_type=OrderType.LIMIT,
        )

        # Place OP limit order
        op_order_req = OrderRequest(
            token_id=pair.op_token,
            side=op_side,
            size=size,
            price=op_ask,
            order_type=OrderType.LIMIT,
            topic_id=op_topic_id,
        )

        # === PRE-VALIDATION: Check dollar amounts meet minimums BEFORE placing any orders ===
        pm_dollar_amount = size * pm_ask
        op_dollar_amount = size * op_ask
        
        PM_MIN_DOLLAR = 1.0  # Polymarket requires minimum ~$1 order
        OP_MIN_DOLLAR = 5.0  # Opinion requires minimum $5 order (per docs)
        
        if pm_dollar_amount < PM_MIN_DOLLAR:
            error = f"PM order too small: ${pm_dollar_amount:.2f} < ${PM_MIN_DOLLAR}"
            logger.warning(error)
            self.store.update_trade_failed(trade.trade_id, error)
            return None
            
        if op_dollar_amount < OP_MIN_DOLLAR:
            error = f"OP order too small: ${op_dollar_amount:.2f} < ${OP_MIN_DOLLAR}"
            logger.warning(error)
            self.store.update_trade_failed(trade.trade_id, error)
            return None

        # === PROFITABILITY CHECK: Ensure profit exceeds Opinion's $0.50 minimum fee ===
        # Calculate spread per share
        spread_per_share = 1.0 - (pm_ask + op_ask)
        
        # Calculate MINIMUM profitable size (to cover $0.50 fee + $0.10 min profit)
        # Formula: spread_per_share * min_size >= fee + min_profit
        required_profit = self.config.op_min_fee + self.config.min_net_profit
        if spread_per_share > 0:
            min_profitable_size = required_profit / spread_per_share
        else:
            min_profitable_size = float('inf')
        
        # Check if current size meets minimum profitable size
        if size < min_profitable_size:
            error = (
                f"Size too small for profitable trade: {size:.1f} < {min_profitable_size:.1f} min "
                f"(spread=${spread_per_share:.4f}/share, need ${required_profit:.2f} to cover fees)"
            )
            logger.warning(error)
            self.store.update_trade_failed(trade.trade_id, error)
            return None
        
        # Also check expected profit (double-check)
        total_cost = pm_dollar_amount + op_dollar_amount
        expected_payout = size * 1.0  # $1 per share
        expected_gross_profit = expected_payout - total_cost
        expected_net_profit = expected_gross_profit - self.config.op_min_fee
        
        # Require minimum net profit (after $0.50 Opinion fee)
        if expected_net_profit < self.config.min_net_profit:
            error = (
                f"Trade unprofitable after fees: "
                f"gross=${expected_gross_profit:.2f} - fee=${self.config.op_min_fee} = net=${expected_net_profit:.2f} "
                f"< min ${self.config.min_net_profit}"
            )
            logger.warning(error)
            self.store.update_trade_failed(trade.trade_id, error)
            return None
        
        logger.info(
            f"Pre-validation passed: size={size:.1f} (min profitable: {min_profitable_size:.1f}), "
            f"PM=${pm_dollar_amount:.2f}, OP=${op_dollar_amount:.2f}, expected_net=${expected_net_profit:.2f}"
        )

        if self.config.dry_run:
            logger.info(f"[DRY-RUN] Would place PM order: {pm_order_req}")
            logger.info(f"[DRY-RUN] Would place OP order: {op_order_req}")
            return trade.trade_id

        # === CRITICAL: Place PM order FIRST, only place OP if PM succeeds ===
        # This prevents unhedged positions on Opinion
        pm_result = self.clients.pm_client.place_order(pm_order_req)
        
        if not pm_result.success:
            logger.error(f"PM order failed: {pm_result.error}")
            self.store.update_trade_failed(trade.trade_id, f"PM order failed: {pm_result.error}")
            # No OP order was placed yet, so nothing to cancel
            return None

        logger.info(f"PM order placed: {pm_result.order_id}")
        
        # Now place OP order
        op_result = self.clients.op_client.place_order(op_order_req)

        if not op_result.success:
            logger.error(f"OP order failed: {op_result.error}")
            # CRITICAL: Cancel PM order immediately!
            logger.warning(f"Cancelling PM order {pm_result.order_id} due to OP failure")
            try:
                cancelled = self.clients.pm_client.cancel_order(pm_result.order_id)
                if cancelled:
                    logger.info(f"PM order {pm_result.order_id} cancelled successfully")
                else:
                    logger.error(f"FAILED to cancel PM order {pm_result.order_id} - MANUAL INTERVENTION REQUIRED!")
            except Exception as e:
                logger.error(f"Exception cancelling PM order: {e} - MANUAL INTERVENTION REQUIRED!")
            
            # Set cooldown for this pair to avoid rapid retries
            self._pair_cooldowns[pair.pair_id] = time.time() + self._cooldown_duration
            logger.info(f"Pair {pair.pair_id[:12]} set to cooldown for {self._cooldown_duration}s")
            
            self.store.update_trade_failed(trade.trade_id, f"OP order failed: {op_result.error}")
            return None

        # Track active orders
        with self._order_lock:
            pm_active = ActiveOrder(
                trade_id=trade.trade_id,
                pair_id=pair.pair_id,
                exchange="PM",
                order_id=pm_result.order_id,
                side=pm_side,
                size=size,
                price=pm_ask,
                token_id=pair.pm_token,
                opposite_exchange="OP",
                opposite_order_id=op_result.order_id,
                opposite_token_id=pair.op_token,
                opposite_topic_id=op_topic_id,
                opposite_price=op_ask,  # Price of the OP limit order
            )
            op_active = ActiveOrder(
                trade_id=trade.trade_id,
                pair_id=pair.pair_id,
                exchange="OP",
                order_id=op_result.order_id,
                side=op_side,
                size=size,
                price=op_ask,
                token_id=pair.op_token,
                topic_id=op_topic_id,
                opposite_exchange="PM",
                opposite_order_id=pm_result.order_id,
                opposite_token_id=pair.pm_token,
                opposite_price=pm_ask,  # Price of the PM limit order
            )

            self._active_orders[pm_result.order_id] = pm_active
            self._active_orders[op_result.order_id] = op_active

        # Calculate expected PnL at order placement
        pm_cost = size * pm_ask
        op_cost = size * op_ask
        total_cost = pm_cost + op_cost
        expected_payout = size * 1.0
        expected_pnl = expected_payout - total_cost
        expected_pnl_pct = (expected_pnl / total_cost * 100) if total_cost > 0 else 0
        
        logger.info("=" * 50)
        logger.info("📤 DUAL ORDERS PLACED")
        logger.info("=" * 50)
        logger.info(f"  Pair: {pair.pair_id[:12]}")
        logger.info(f"  PM Order: {size:.2f} shares @ ${pm_ask:.4f} = ${pm_cost:.2f}")
        logger.info(f"  OP Order: {size:.2f} shares @ ${op_ask:.4f} = ${op_cost:.2f}")
        logger.info("-" * 50)
        logger.info(f"  Total Cost: ${total_cost:.2f}")
        logger.info(f"  Expected Payout: ${expected_payout:.2f}")
        logger.info(f"  Expected PnL: ${expected_pnl:.2f} ({expected_pnl_pct:.2f}%)")
        logger.info(f"  PM Order ID: {pm_result.order_id[:20]}...")
        logger.info(f"  OP Order ID: {op_result.order_id}")
        logger.info("=" * 50)

        return trade.trade_id

    def _check_for_fills(self) -> None:
        """Check all active orders for fills and execute hedges."""
        with self._order_lock:
            orders_to_check = list(self._active_orders.values())

        for active_order in orders_to_check:
            try:
                self._check_order_fill(active_order)
            except Exception as e:
                logger.error(f"Error checking order {active_order.order_id}: {e}")

    def _check_order_fill(self, active_order: ActiveOrder) -> None:
        """
        Check if an order is filled and execute hedge if so.
        
        Uses per-trade lock to prevent race conditions when both orders fill simultaneously.
        """
        # Acquire per-trade lock to prevent race condition on simultaneous fills
        trade_lock = self._get_trade_lock(active_order.trade_id)
        
        # Try to acquire lock - if another thread has it, skip this check
        if not trade_lock.acquire(blocking=False):
            logger.debug(f"Trade {active_order.trade_id[:12]} is being processed by another thread")
            return
        
        try:
            # Check if order is still active (might have been removed by other thread)
            with self._order_lock:
                if active_order.order_id not in self._active_orders:
                    return
            
            # Get order status
            if active_order.exchange == "PM":
                status = self.clients.pm_client.get_order_status(active_order.order_id)
            else:
                status = self.clients.op_client.get_order_status(active_order.order_id)

            if status.status in ("filled", "partially_filled"):
                filled_size = status.filled_size
                filled_price = status.filled_price or active_order.price

                if filled_size > 0:
                    logger.info(
                        f"Order {active_order.order_id} on {active_order.exchange} "
                        f"filled: {filled_size:.2f} @ {filled_price:.4f}"
                    )

                    # Execute hedge
                    self._execute_hedge(active_order, filled_size, filled_price)

                    # Remove from active orders
                    with self._order_lock:
                        self._active_orders.pop(active_order.order_id, None)
                        # Also remove the opposite order
                        if active_order.opposite_order_id:
                            self._active_orders.pop(active_order.opposite_order_id, None)
                    
                    # Cleanup trade lock
                    self._cleanup_trade_lock(active_order.trade_id)

            elif status.status == "cancelled":
                logger.info(f"Order {active_order.order_id} was cancelled")
                with self._order_lock:
                    self._active_orders.pop(active_order.order_id, None)

            # Check for timeout - re-verify status before cancelling
            elif time.time() - active_order.placed_at > self.config.cancel_unfilled_after_sec:
                # Re-check status to avoid cancelling a just-filled order
                if active_order.exchange == "PM":
                    final_status = self.clients.pm_client.get_order_status(active_order.order_id)
                else:
                    final_status = self.clients.op_client.get_order_status(active_order.order_id)
                
                if final_status.status in ("filled", "partially_filled"):
                    logger.info(f"Order {active_order.order_id} filled just before timeout cancel")
                    # Process the fill instead
                    filled_size = final_status.filled_size
                    filled_price = final_status.filled_price or active_order.price
                    if filled_size > 0:
                        self._execute_hedge(active_order, filled_size, filled_price)
                        with self._order_lock:
                            self._active_orders.pop(active_order.order_id, None)
                            if active_order.opposite_order_id:
                                self._active_orders.pop(active_order.opposite_order_id, None)
                        self._cleanup_trade_lock(active_order.trade_id)
                else:
                    logger.info(f"Order {active_order.order_id} timed out, cancelling")
                    self._cancel_order_pair(active_order)
        finally:
            trade_lock.release()

    def _execute_hedge(
        self, filled_order: ActiveOrder, filled_size: float, filled_price: float
    ) -> HedgeResult:
        """
        Execute hedge: cancel opposite limit order + place market order.

        This is triggered when a limit order is filled on one exchange.
        
        IMPORTANT: If the opposite LIMIT order was also filled, no market hedge is needed!
        """
        logger.info(
            f"Executing hedge for {filled_order.exchange} fill: "
            f"size={filled_size:.2f} price={filled_price:.4f}"
        )

        # Check if the opposite limit order was already filled
        if filled_order.opposite_order_id:
            if filled_order.opposite_exchange == "PM":
                op_status = self.clients.pm_client.get_order_status(filled_order.opposite_order_id)
            else:
                op_status = self.clients.op_client.get_order_status(filled_order.opposite_order_id)
            
            logger.info(f"Opposite order {filled_order.opposite_order_id} status: {op_status.status}")
            
            # If the opposite order was already filled, we have our covered position!
            if op_status.status == "filled" or (op_status.status == "unknown" and op_status.filled_size > 0):
                hedge_size = op_status.filled_size if op_status.filled_size > 0 else filled_size
                
                logger.info(
                    f"BOTH limit orders filled! Position is covered. "
                    f"Entry: {filled_order.exchange} {filled_size:.2f} @ {filled_price:.4f}, "
                    f"Hedge (limit): {filled_order.opposite_exchange} {hedge_size:.2f}"
                )
                
                result = HedgeResult(
                    success=True,
                    entry_exchange=filled_order.exchange,
                    entry_filled_size=filled_size,
                    entry_filled_price=filled_price,
                    hedge_exchange=filled_order.opposite_exchange,
                    hedge_order_id=filled_order.opposite_order_id,
                    hedge_size=hedge_size,
                    hedge_price=filled_order.opposite_price,  # Use the opposite limit order price
                    hedge_slippage=0.0,  # No slippage since both were limits
                )
                
                # Update trade record in database
                self.store.update_trade_entry_filled(
                    filled_order.trade_id, filled_size, filled_price
                )
                # Calculate PnL FIRST so we can pass fees to store
                entry_cost = filled_size * filled_price
                hedge_cost = hedge_size * filled_order.opposite_price  # Use opposite order's price
                total_investment = entry_cost + hedge_cost
                payout = filled_size * 1.0  # $1 per share guaranteed

                # Calculate fees (maker fees for both limit orders = 0% on both exchanges!)
                # But Opinion has $0.50 minimum fee per trade
                pm_fee = 0.0  # Polymarket maker = 0%
                op_fee = 0.0  # Opinion maker = 0%

                # Opinion minimum fee check (even for makers, there might be platform fees)
                # Conservative: assume $0.50 min fee on Opinion side
                if filled_order.exchange == "OP" or filled_order.opposite_exchange == "OP":
                    op_fee = max(op_fee, self.config.op_min_fee) if hasattr(self.config, 'op_min_fee') else 0.0

                total_fees = pm_fee + op_fee

                # Now update database with correct fees
                self.store.update_trade_hedged(
                    trade_id=filled_order.trade_id,
                    hedge_exchange=result.hedge_exchange,
                    hedge_order_id=result.hedge_order_id or "",
                    hedge_side="BUY",
                    hedge_size=result.hedge_size,
                    hedge_price=result.hedge_price,
                    hedge_slippage=result.hedge_slippage,
                    fees_override=total_fees,  # Pass calculated fees with Opinion minimum
                )
                
                gross_pnl = payout - total_investment
                net_pnl = gross_pnl - total_fees
                pnl_percent = (net_pnl / total_investment * 100) if total_investment > 0 else 0
                
                logger.info("=" * 50)
                logger.info("✅ TRADE COMPLETE - BOTH LIMIT ORDERS FILLED")
                logger.info("=" * 50)
                logger.info(f"  Entry ({filled_order.exchange}): {filled_size:.2f} shares @ ${filled_price:.4f} = ${entry_cost:.2f}")
                logger.info(f"  Hedge ({filled_order.opposite_exchange}): {hedge_size:.2f} shares @ ${filled_order.opposite_price:.4f} = ${hedge_cost:.2f}")
                logger.info("-" * 50)
                logger.info(f"  Total Investment: ${total_investment:.2f}")
                logger.info(f"  Guaranteed Payout: ${payout:.2f}")
                logger.info(f"  Fees: ${total_fees:.4f}")
                logger.info(f"  Gross PnL: ${gross_pnl:.2f}")
                logger.info(f"  Net PnL: ${net_pnl:.2f} ({pnl_percent:.2f}%)")
                logger.info("=" * 50)
                
                # Send Telegram notification!
                trade = self.store.get_trade(filled_order.trade_id)
                if trade and self.on_trade_complete:
                    self.on_trade_complete(trade)
                
                return result
            
            # Try to cancel the opposite order
            if filled_order.opposite_exchange == "PM":
                self.clients.pm_client.cancel_order(filled_order.opposite_order_id)
            else:
                self.clients.op_client.cancel_order(filled_order.opposite_order_id)

            logger.info(f"Cancelled opposite order {filled_order.opposite_order_id}")

        # Calculate hedge size
        hedge_size = filled_size * self.config.hedge_ratio

        # Get current best ask for the hedge
        if filled_order.opposite_exchange == "PM":
            # Hedge on Polymarket
            orderbook = self.orderbook_manager.pm_client.fetch(
                filled_order.opposite_token_id
            )
            hedge_client = self.clients.pm_client
        else:
            # Hedge on Opinion
            # Get question_id from the pair
            pair = self.store.get_pair(filled_order.pair_id)
            orderbook = self.orderbook_manager.op_client.fetch(
                token_id=filled_order.opposite_token_id,
                question_id=pair.op_question_id if pair else None,
                symbol_type=1 if pair and pair.op_side == "NO" else 0,
            )
            hedge_client = self.clients.op_client

        if not orderbook.asks:
            error = "No asks available for hedge"
            logger.error(error)
            self.store.update_trade_failed(filled_order.trade_id, error)
            return HedgeResult(
                success=False,
                entry_exchange=filled_order.exchange,
                entry_filled_size=filled_size,
                entry_filled_price=filled_price,
                hedge_exchange=filled_order.opposite_exchange,
                error=error,
            )

        hedge_price = orderbook.best_ask_price

        # Check slippage
        expected_price = filled_order.opposite_price  # We expected the opposite limit order price
        slippage = abs(hedge_price - expected_price)

        if slippage > self.config.max_slippage_market_hedge:
            error = f"Slippage too high: {slippage:.4f} > {self.config.max_slippage_market_hedge}"
            logger.warning(error)
            # Still try to hedge but log the warning
            if not self.config.allow_partial_fill_hedge:
                self.store.update_trade_failed(filled_order.trade_id, error)
                return HedgeResult(
                    success=False,
                    entry_exchange=filled_order.exchange,
                    entry_filled_size=filled_size,
                    entry_filled_price=filled_price,
                    hedge_exchange=filled_order.opposite_exchange,
                    hedge_slippage=slippage,
                    error=error,
                )

        # Check minimum dollar amount for hedge
        OP_MIN_DOLLAR = 5.50  # Opinion minimum $5 + buffer
        PM_MIN_DOLLAR = 1.10  # Polymarket minimum ~$1 + buffer
        
        hedge_dollar_amount = hedge_size * hedge_price
        
        if filled_order.opposite_exchange == "OP" and hedge_dollar_amount < OP_MIN_DOLLAR:
            error = f"Hedge too small for Opinion: ${hedge_dollar_amount:.2f} < ${OP_MIN_DOLLAR}"
            logger.error(error)
            logger.error(f"CRITICAL: Unhedged position on {filled_order.exchange}! Manual intervention required!")
            self.store.update_trade_failed(filled_order.trade_id, error)
            return HedgeResult(
                success=False,
                entry_exchange=filled_order.exchange,
                entry_filled_size=filled_size,
                entry_filled_price=filled_price,
                hedge_exchange=filled_order.opposite_exchange,
                error=error,
            )
        
        if filled_order.opposite_exchange == "PM" and hedge_dollar_amount < PM_MIN_DOLLAR:
            error = f"Hedge too small for Polymarket: ${hedge_dollar_amount:.2f} < ${PM_MIN_DOLLAR}"
            logger.error(error)
            logger.error(f"CRITICAL: Unhedged position on {filled_order.exchange}! Manual intervention required!")
            self.store.update_trade_failed(filled_order.trade_id, error)
            return HedgeResult(
                success=False,
                entry_exchange=filled_order.exchange,
                entry_filled_size=filled_size,
                entry_filled_price=filled_price,
                hedge_exchange=filled_order.opposite_exchange,
                error=error,
            )

        # Place MARKET order for hedge
        hedge_order_req = OrderRequest(
            token_id=filled_order.opposite_token_id,
            side=OrderSide.BUY,  # Covered arb always buys
            size=hedge_size,
            price=hedge_price,  # Best ask
            order_type=OrderType.MARKET,
            topic_id=filled_order.opposite_topic_id,
        )

        if self.config.dry_run:
            logger.info(f"[DRY-RUN] Would place hedge order: {hedge_order_req}")
            result = HedgeResult(
                success=True,
                entry_exchange=filled_order.exchange,
                entry_filled_size=filled_size,
                entry_filled_price=filled_price,
                hedge_exchange=filled_order.opposite_exchange,
                hedge_order_id="DRY-HEDGE",
                hedge_size=hedge_size,
                hedge_price=hedge_price,
                hedge_slippage=slippage,
            )
        else:
            hedge_result = hedge_client.place_order(hedge_order_req)

            if not hedge_result.success:
                # RETRY LOGIC: Try again with exponential backoff
                last_error = hedge_result.error
                for retry in range(MAX_HEDGE_RETRIES):
                    wait_time = RETRY_BASE_DELAY * (2 ** retry)
                    logger.warning(
                        f"Hedge failed (attempt {retry + 1}/{MAX_HEDGE_RETRIES + 1}), "
                        f"retrying in {wait_time}s: {last_error}"
                    )
                    time.sleep(wait_time)
                    
                    # Refresh orderbook and try again
                    if filled_order.opposite_exchange == "PM":
                        orderbook = self.orderbook_manager.pm_client.fetch(filled_order.opposite_token_id)
                    else:
                        pair = self.store.get_pair(filled_order.pair_id)
                        orderbook = self.orderbook_manager.op_client.fetch(
                            token_id=filled_order.opposite_token_id,
                            question_id=pair.op_question_id if pair else None,
                            symbol_type=1 if pair and pair.op_side == "NO" else 0,
                        )
                    
                    if orderbook.asks:
                        new_hedge_price = orderbook.best_ask_price
                        hedge_order_req.price = new_hedge_price
                        hedge_result = hedge_client.place_order(hedge_order_req)
                        
                        if hedge_result.success:
                            logger.info(f"Hedge succeeded on retry {retry + 1}")
                            break
                        last_error = hedge_result.error
                    else:
                        last_error = "No asks available"
                
                # If still failed after retries, this is CRITICAL
                if not hedge_result.success:
                    error = f"Hedge order failed after {MAX_HEDGE_RETRIES + 1} attempts: {last_error}"
                    logger.error(error)
                    logger.error(f"CRITICAL: UNHEDGED POSITION on {filled_order.exchange}!")
                    
                    # Track unhedged position
                    unhedged_info = {
                        "trade_id": filled_order.trade_id,
                        "exchange": filled_order.exchange,
                        "size": filled_size,
                        "price": filled_price,
                        "cost": filled_size * filled_price,
                        "error": error,
                    }
                    self._unhedged_positions.append(unhedged_info)
                    
                    # Alert via callback
                    if self.on_unhedged_position:
                        self.on_unhedged_position(unhedged_info)
                    
                    self.store.update_trade_failed(filled_order.trade_id, error)
                    return HedgeResult(
                        success=False,
                        entry_exchange=filled_order.exchange,
                        entry_filled_size=filled_size,
                        entry_filled_price=filled_price,
                        hedge_exchange=filled_order.opposite_exchange,
                        hedge_slippage=slippage,
                        error=error,
                    )

            result = HedgeResult(
                success=True,
                entry_exchange=filled_order.exchange,
                entry_filled_size=filled_size,
                entry_filled_price=filled_price,
                hedge_exchange=filled_order.opposite_exchange,
                hedge_order_id=hedge_result.order_id,
                hedge_size=hedge_result.filled_size or hedge_size,
                hedge_price=hedge_result.filled_price or hedge_price,
                hedge_slippage=slippage,
            )

        # Update trade record
        self.store.update_trade_entry_filled(
            filled_order.trade_id, filled_size, filled_price
        )

        # Calculate fees for market hedge scenario:
        # Entry: limit (maker) - 0% on both exchanges
        # Hedge: market (taker) - 0% on PM, 0-2% on OP + $0.50 minimum
        entry_cost = filled_size * filled_price
        hedge_cost = result.hedge_size * result.hedge_price
        
        if filled_order.exchange == "PM":
            # Entry on PM (maker), hedge on OP (taker)
            entry_fee = entry_cost * self.config.pm_maker_fee  # 0%
            hedge_fee = hedge_cost * self.config.op_taker_fee  # 2%
            # Apply Opinion minimum fee
            hedge_fee = max(hedge_fee, self.config.op_min_fee if hasattr(self.config, 'op_min_fee') else 0.0)
        else:
            # Entry on OP (maker), hedge on PM (taker)
            entry_fee = entry_cost * self.config.op_maker_fee  # 0%
            hedge_fee = hedge_cost * self.config.pm_taker_fee  # 0%
        
        total_fees = entry_fee + hedge_fee

        self.store.update_trade_hedged(
            trade_id=filled_order.trade_id,
            hedge_exchange=result.hedge_exchange,
            hedge_order_id=result.hedge_order_id or "",
            hedge_side="BUY",
            hedge_size=result.hedge_size,
            hedge_price=result.hedge_price,
            hedge_slippage=result.hedge_slippage,
            fees_override=total_fees,  # Pass calculated fees with Opinion minimum
        )

        logger.info(
            f"Hedge executed: {result.hedge_exchange} "
            f"size={result.hedge_size:.2f} @ {result.hedge_price:.4f} "
            f"slippage={result.hedge_slippage:.4f} fees=${total_fees:.4f}"
        )

        # Callback
        if self.on_hedge_executed:
            self.on_hedge_executed(result)

        # Trade complete callback
        trade = self.store.get_trade(filled_order.trade_id)
        if trade and self.on_trade_complete:
            self.on_trade_complete(trade)

        return result

    def _cancel_order_pair(self, active_order: ActiveOrder) -> None:
        """Cancel both orders in a pair."""
        # Cancel this order
        if active_order.exchange == "PM":
            self.clients.pm_client.cancel_order(active_order.order_id)
        else:
            self.clients.op_client.cancel_order(active_order.order_id)

        # Cancel opposite order
        if active_order.opposite_order_id:
            if active_order.opposite_exchange == "PM":
                self.clients.pm_client.cancel_order(active_order.opposite_order_id)
            else:
                self.clients.op_client.cancel_order(active_order.opposite_order_id)

        # Remove from tracking
        with self._order_lock:
            self._active_orders.pop(active_order.order_id, None)
            if active_order.opposite_order_id:
                self._active_orders.pop(active_order.opposite_order_id, None)

        # Update trade status
        self.store.update_trade_failed(active_order.trade_id, "Cancelled due to timeout")

    def run_once(self) -> int:
        """
        Run one iteration:
        1. Place new orders for profitable pairs
        2. Check existing orders for fills

        Returns number of trades initiated.
        """
        trades_initiated = 0

        # Check for fills first
        self._check_for_fills()

        # Get ACTIVE pairs
        active_pairs = self.store.list_pairs(statuses=[PairStatus.ACTIVE])

        for pair in active_pairs:
            # Skip if we already have active orders for this pair
            with self._order_lock:
                has_active = any(
                    o.pair_id == pair.pair_id for o in self._active_orders.values()
                )

            if has_active:
                continue

            # Try to place dual orders
            trade_id = self.place_dual_orders(pair)
            if trade_id:
                trades_initiated += 1

        return trades_initiated

    def _run_loop(self) -> None:
        """Main trading loop."""
        logger.info("Market-Hedge runner loop started")

        while self._running:
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Runner loop error: {e}")

            time.sleep(self.config.poll_interval_sec)

        logger.info("Market-Hedge runner loop stopped")

    def start(self) -> None:
        """Start the runner in a background thread."""
        if self._running:
            logger.warning("Runner already running")
            return

        # Crash recovery: handle any unfinished trades from previous session
        try:
            recovered = self.recover_pending_trades()
            if recovered > 0:
                logger.info(f"Crash recovery: processed {recovered} unfinished trades")
        except Exception as e:
            logger.error(f"Crash recovery failed: {e}")

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Market-Hedge runner started (trading DISABLED - use /start_trading to enable)")

    def stop(self) -> None:
        """Stop the runner."""
        logger.info("Stopping Market-Hedge runner")
        self._running = False

        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None

        # Cancel all active orders
        with self._order_lock:
            for order_id, active_order in list(self._active_orders.items()):
                try:
                    if active_order.exchange == "PM":
                        self.clients.pm_client.cancel_order(order_id)
                    else:
                        self.clients.op_client.cancel_order(order_id)
                except Exception as e:
                    logger.error(f"Error cancelling order {order_id}: {e}")

            self._active_orders.clear()

        logger.info("Market-Hedge runner stopped")

    def is_running(self) -> bool:
        """Check if runner is running."""
        return self._running

    def is_trading_enabled(self) -> bool:
        """Check if trading is enabled."""
        return self._trading_enabled

    def enable_trading(self) -> None:
        """Enable trading on all pairs."""
        self._trading_enabled = True
        logger.info("Trading ENABLED - bot will now execute trades")

    def disable_trading(self) -> None:
        """Disable trading on all pairs (monitoring only)."""
        self._trading_enabled = False
        logger.info("Trading DISABLED - bot is in monitoring mode only")

    def get_active_orders_count(self) -> int:
        """Get number of active orders being monitored."""
        with self._order_lock:
            return len(self._active_orders)

    def get_unhedged_positions(self) -> list[dict]:
        """Get list of unhedged positions requiring manual intervention."""
        return self._unhedged_positions.copy()

    def clear_unhedged_position(self, trade_id: str) -> bool:
        """Mark an unhedged position as resolved (after manual intervention)."""
        for i, pos in enumerate(self._unhedged_positions):
            if pos.get("trade_id") == trade_id:
                self._unhedged_positions.pop(i)
                logger.info(f"Unhedged position {trade_id[:12]} marked as resolved")
                return True
        return False

    def get_last_simulation(self, pair_id: str) -> Optional["SimulationResult"]:
        """
        Get last simulation for a pair.
        
        For MarketHedgeRunner, we run a fresh simulation since we don't cache.
        """
        from ..core.math_utils import SimulationResult
        
        pair = self.store.get_pair(pair_id)
        if not pair:
            return None
        
        try:
            return self.simulate_pair(pair)
        except Exception as e:
            logger.error(f"Error simulating pair {pair_id}: {e}")
            return None
