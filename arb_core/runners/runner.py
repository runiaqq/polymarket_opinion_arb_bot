"""
Covered Arbitrage Runner.

Executes covered arbitrage trades:
- BUY on Platform A (YES) + BUY on Platform B (NO)
- Guaranteed payout = 1 at resolution
- Profit locked if total entry cost < 1
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from ..core.logging import get_logger
from ..core.math_utils import CoveredArbQuote, SimulationResult, simulate_covered_arb
from ..core.models import Pair, PairStatus
from ..core.store import PairStore
from ..exchanges.exchange_clients import (
    ExchangeClients,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderType,
)
from ..market_data.orderbook import OrderbookManager, PairOrderbooks

logger = get_logger(__name__)


@dataclass
class TradeResult:
    """Result of a covered arbitrage trade."""

    pair_id: str
    success: bool
    simulation: Optional[SimulationResult] = None

    # Order results
    pm_order: Optional[OrderResult] = None
    op_order: Optional[OrderResult] = None

    # Error info
    error: Optional[str] = None
    skip_reason: Optional[str] = None

    # Metrics
    total_invested: float = 0.0
    expected_profit: float = 0.0
    expected_profit_pct: float = 0.0

    @property
    def is_traded(self) -> bool:
        """Check if trade was executed."""
        return (
            self.success
            and self.pm_order is not None
            and self.op_order is not None
            and self.pm_order.success
            and self.op_order.success
        )


@dataclass
class RunnerConfig:
    """Runner configuration."""

    # Trading settings
    dry_run: bool = True
    maker_fee_pm: float = 0.0
    maker_fee_op: float = 0.0

    # Limits
    default_max_position: float = 15.0
    default_min_profit_percent: float = 0.0

    # Loop settings
    poll_interval_sec: float = 5.0
    max_consecutive_errors: int = 5

    # Backoff settings
    backoff_base_sec: float = 5.0
    backoff_max_sec: float = 300.0

    # Balance simulation (dry-run)
    sim_pm_balance: float = 10000.0
    sim_op_balance: float = 10000.0


class CoveredArbRunner:
    """
    Main runner for covered arbitrage.

    Monitors ACTIVE pairs and executes trades when profitable.
    """

    def __init__(
        self,
        store: PairStore,
        clients: ExchangeClients,
        orderbook_manager: Optional[OrderbookManager] = None,
        config: Optional[RunnerConfig] = None,
        on_trade: Optional[Callable[[TradeResult], None]] = None,
        on_simulation: Optional[Callable[[str, SimulationResult], None]] = None,
    ):
        self.store = store
        self.clients = clients
        self.orderbook_manager = orderbook_manager or OrderbookManager()
        self.config = config or RunnerConfig()

        # Callbacks
        self.on_trade = on_trade
        self.on_simulation = on_simulation

        # State
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._pair_backoff: dict[str, float] = {}  # pair_id -> backoff_until
        self._pair_errors: dict[str, int] = {}  # pair_id -> consecutive_errors

        # Last simulation results for PnL display
        self._last_simulations: dict[str, SimulationResult] = {}

    def simulate_pair(self, pair: Pair) -> SimulationResult:
        """
        Run simulation for a pair.

        Args:
            pair: The pair to simulate

        Returns:
            SimulationResult with computed values
        """
        # Validate pair has tokens
        if not pair.pm_token or not pair.op_token:
            quote = CoveredArbQuote(pm_ask=0, op_ask=0)
            return SimulationResult(
                quote=quote,
                size_result=None,
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
                size_result=None,
                is_profitable=False,
                min_profit_percent=pair.min_profit_percent,
                skip_reason=orderbooks.error or "invalid_orderbooks",
            )

        # Build quote
        pm_ask = orderbooks.pm_orderbook.best_ask_price
        op_ask = orderbooks.op_orderbook.best_ask_price
        pm_depth = orderbooks.pm_orderbook.best_ask_size
        op_depth = orderbooks.op_orderbook.best_ask_size

        # Get balances
        pm_balance_result = self.clients.pm_client.get_balance()
        op_balance_result = self.clients.op_client.get_balance()
        pm_balance = pm_balance_result.available
        op_balance = op_balance_result.available
        
        # Log balance info for debugging
        logger.info(f"Simulation balances: PM=${pm_balance:.2f}, OP=${op_balance:.2f}")
        
        # Check if balances are zero (indicating fetch failure)
        if pm_balance <= 0 and op_balance <= 0:
            quote = CoveredArbQuote(pm_ask=0, op_ask=0)
            return SimulationResult(
                quote=quote,
                size_result=None,
                is_profitable=False,
                min_profit_percent=pair.min_profit_percent,
                skip_reason="balance_fetch_failed_both",
            )

        quote = CoveredArbQuote(
            pm_ask=pm_ask,
            op_ask=op_ask,
            pm_fee=self.config.maker_fee_pm,
            op_fee=self.config.maker_fee_op,
            pm_depth=pm_depth,
            op_depth=op_depth,
            pm_balance=pm_balance,
            op_balance=op_balance,
            pm_min_size=self.clients.pm_client.get_min_order_size(),
            op_min_size=self.clients.op_client.get_min_order_size(),
        )

        # Run simulation
        result = simulate_covered_arb(
            quote=quote,
            max_position=pair.max_position,
            min_profit_percent=pair.min_profit_percent,
            pm_balance=pm_balance,
            op_balance=op_balance,
            pm_min_size=quote.pm_min_size,
            op_min_size=quote.op_min_size,
        )

        # Store for PnL display
        self._last_simulations[pair.pair_id] = result

        # Callback
        if self.on_simulation:
            self.on_simulation(pair.pair_id, result)

        return result

    def execute_trade(self, pair: Pair, simulation: SimulationResult) -> TradeResult:
        """
        Execute a covered arbitrage trade.

        Args:
            pair: The pair to trade
            simulation: Pre-computed simulation result

        Returns:
            TradeResult with order outcomes
        """
        result = TradeResult(
            pair_id=pair.pair_id,
            success=False,
            simulation=simulation,
        )

        # Validate simulation is tradeable
        if not simulation.is_tradeable:
            result.skip_reason = simulation.skip_reason
            return result

        size = simulation.size_result.size
        pm_price = simulation.quote.pm_ask
        op_price = simulation.quote.op_ask

        # === CRITICAL: Verify balances on BOTH platforms BEFORE trading ===
        pm_cost = size * pm_price
        op_cost = size * op_price
        
        logger.info(f"Checking balances before trade: PM need ${pm_cost:.2f}, OP need ${op_cost:.2f}")
        
        try:
            pm_balance = self.clients.pm_client.get_balance()
            op_balance = self.clients.op_client.get_balance()
            
            logger.info(f"Balances: PM=${pm_balance.available:.2f}, OP=${op_balance.available:.2f}")
            
            # Check PM balance
            if pm_balance.available < pm_cost:
                result.skip_reason = f"PM balance insufficient: ${pm_balance.available:.2f} < ${pm_cost:.2f}"
                logger.warning(result.skip_reason)
                return result
            
            # Check OP balance - THIS IS THE CRITICAL CHECK THAT WAS MISSING
            if op_balance.available < op_cost:
                result.skip_reason = f"OP balance insufficient: ${op_balance.available:.2f} < ${op_cost:.2f}"
                logger.warning(result.skip_reason)
                return result
                
        except Exception as e:
            result.error = f"Balance check failed: {e}"
            logger.error(result.error)
            return result

        # === PRE-VALIDATION: Check dollar amounts meet minimums ===
        pm_dollar_amount = size * pm_price
        op_dollar_amount = size * op_price
        
        PM_MIN_DOLLAR = 1.0  # Polymarket requires minimum $1 order
        OP_MIN_DOLLAR = 1.0  # Opinion minimum
        
        if pm_dollar_amount < PM_MIN_DOLLAR:
            result.skip_reason = f"PM order too small: ${pm_dollar_amount:.2f} < ${PM_MIN_DOLLAR}"
            logger.warning(result.skip_reason)
            return result
            
        if op_dollar_amount < OP_MIN_DOLLAR:
            result.skip_reason = f"OP order too small: ${op_dollar_amount:.2f} < ${OP_MIN_DOLLAR}"
            logger.warning(result.skip_reason)
            return result

        logger.info(
            f"Executing trade for {pair.pair_id[:12]}... "
            f"size={size:.2f} PM@{pm_price:.4f} (${pm_dollar_amount:.2f}) OP@{op_price:.4f} (${op_dollar_amount:.2f})"
        )

        # Place PM order FIRST
        pm_order_req = OrderRequest(
            token_id=pair.pm_token,
            side=OrderSide.BUY,
            size=size,
            price=pm_price,
            order_type=OrderType.LIMIT,
        )

        result.pm_order = self.clients.pm_client.place_order(pm_order_req)

        if not result.pm_order.success:
            result.error = f"PM order failed: {result.pm_order.error}"
            logger.error(result.error)
            return result

        # Extract topic_id from opinion_url
        op_topic_id = None
        if pair.opinion_url:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(pair.opinion_url)
            qs = parse_qs(parsed.query)
            if "topicId" in qs:
                op_topic_id = qs["topicId"][0]

        # Place OP order
        op_order_req = OrderRequest(
            token_id=pair.op_token,
            side=OrderSide.BUY,
            size=size,
            price=op_price,
            order_type=OrderType.LIMIT,
            topic_id=op_topic_id,
        )

        result.op_order = self.clients.op_client.place_order(op_order_req)

        if not result.op_order.success:
            result.error = f"OP order failed: {result.op_order.error}"
            logger.error(result.error)
            
            # CRITICAL: Cancel PM order to avoid unhedged position!
            if result.pm_order and result.pm_order.order_id:
                logger.warning(f"Cancelling PM order {result.pm_order.order_id} due to OP failure")
                try:
                    cancelled = self.clients.pm_client.cancel_order(result.pm_order.order_id)
                    if cancelled:
                        logger.info(f"PM order {result.pm_order.order_id} cancelled successfully")
                    else:
                        logger.error(f"Failed to cancel PM order {result.pm_order.order_id} - MANUAL INTERVENTION REQUIRED!")
                        result.error += " | PM ORDER NOT CANCELLED - CHECK MANUALLY!"
                except Exception as cancel_error:
                    logger.error(f"Exception cancelling PM order: {cancel_error} - MANUAL INTERVENTION REQUIRED!")
                    result.error += f" | PM cancel failed: {cancel_error}"
            
            return result

        # Success
        result.success = True
        result.total_invested = simulation.total_investment
        result.expected_profit = simulation.expected_profit
        result.expected_profit_pct = simulation.expected_profit_pct

        logger.info(
            f"Trade executed: invested=${result.total_invested:.2f} "
            f"expected_profit=${result.expected_profit:.2f} ({result.expected_profit_pct:.2f}%)"
        )

        # Callback
        if self.on_trade:
            self.on_trade(result)

        return result

    def run_once(self) -> list[TradeResult]:
        """
        Run one iteration of the trading loop.

        Returns:
            List of trade results (one per ACTIVE pair)
        """
        results = []

        # Get ACTIVE pairs only
        active_pairs = self.store.list_pairs(statuses=[PairStatus.ACTIVE])

        for pair in active_pairs:
            # Check backoff
            if self._is_in_backoff(pair.pair_id):
                logger.debug(f"Pair {pair.pair_id[:12]} in backoff, skipping")
                continue

            try:
                # Simulate
                simulation = self.simulate_pair(pair)

                if not simulation.is_tradeable:
                    # Log at INFO level for visibility
                    skip_reason = simulation.skip_reason or "unknown"
                    if simulation.quote and simulation.quote.total_cost > 0:
                        logger.info(
                            f"Pair {pair.pair_id[:12]} skipped: {skip_reason} "
                            f"(cost={simulation.quote.total_cost:.4f}, profit={simulation.quote.profit_percent:.2f}%)"
                        )
                    else:
                        logger.info(f"Pair {pair.pair_id[:12]} skipped: {skip_reason}")
                    results.append(
                        TradeResult(
                            pair_id=pair.pair_id,
                            success=False,
                            simulation=simulation,
                            skip_reason=simulation.skip_reason,
                        )
                    )
                    self._clear_backoff(pair.pair_id)
                    continue

                # Execute trade
                logger.info(
                    f"Pair {pair.pair_id[:12]} PROFITABLE! "
                    f"cost={simulation.quote.total_cost:.4f}, profit={simulation.quote.profit_percent:.2f}%, "
                    f"size={simulation.size_result.size:.2f}"
                )
                trade_result = self.execute_trade(pair, simulation)
                results.append(trade_result)

                if trade_result.success:
                    self._clear_backoff(pair.pair_id)
                else:
                    self._increment_backoff(pair.pair_id)

            except Exception as e:
                logger.error(f"Error processing pair {pair.pair_id[:12]}: {e}")
                self._increment_backoff(pair.pair_id)
                results.append(
                    TradeResult(
                        pair_id=pair.pair_id,
                        success=False,
                        error=str(e),
                    )
                )

        return results

    def _is_in_backoff(self, pair_id: str) -> bool:
        """Check if pair is in backoff period."""
        backoff_until = self._pair_backoff.get(pair_id, 0)
        return time.time() < backoff_until

    def _increment_backoff(self, pair_id: str):
        """Increment backoff for a pair."""
        errors = self._pair_errors.get(pair_id, 0) + 1
        self._pair_errors[pair_id] = errors

        # Exponential backoff with cap
        backoff_sec = min(
            self.config.backoff_base_sec * (2 ** (errors - 1)),
            self.config.backoff_max_sec,
        )
        self._pair_backoff[pair_id] = time.time() + backoff_sec

        logger.warning(
            f"Pair {pair_id[:12]} backoff: {backoff_sec:.0f}s (errors: {errors})"
        )

    def _clear_backoff(self, pair_id: str):
        """Clear backoff for a pair."""
        self._pair_errors.pop(pair_id, None)
        self._pair_backoff.pop(pair_id, None)

    def get_last_simulation(self, pair_id: str) -> Optional[SimulationResult]:
        """Get last simulation result for a pair."""
        return self._last_simulations.get(pair_id)

    def _run_loop(self):
        """Main trading loop."""
        logger.info("Runner loop started")

        while self._running:
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Runner loop error: {e}")

            # Sleep
            time.sleep(self.config.poll_interval_sec)

        logger.info("Runner loop stopped")

    def start(self):
        """Start the runner in a background thread."""
        if self._running:
            logger.warning("Runner already running")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Runner started")

    def stop(self):
        """Stop the runner."""
        logger.info("Stopping runner")
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("Runner stopped")

    def is_running(self) -> bool:
        """Check if runner is running."""
        return self._running


def run_smoke_test(
    store: PairStore,
    clients: ExchangeClients,
    orderbook_manager: Optional[OrderbookManager] = None,
    max_size: float = 1.0,
) -> Optional[TradeResult]:
    """
    Run a smoke test with minimal size.

    Requires I_UNDERSTAND_LIVE_TRADING=YES environment variable.

    Args:
        store: Pair store
        clients: Exchange clients (should be live, not dry-run)
        orderbook_manager: Orderbook manager
        max_size: Maximum size for smoke test (default 1.0)

    Returns:
        TradeResult if trade executed, None if no profitable pair found
    """
    import os

    # Safety check
    if os.environ.get("I_UNDERSTAND_LIVE_TRADING") != "YES":
        logger.error(
            "Smoke test requires I_UNDERSTAND_LIVE_TRADING=YES environment variable"
        )
        return None

    if clients.is_dry_run:
        logger.error("Smoke test requires live clients, not dry-run")
        return None

    logger.warning("=== SMOKE TEST: LIVE TRADING ===")

    # Get first ACTIVE pair
    active_pairs = store.list_pairs(statuses=[PairStatus.ACTIVE])
    if not active_pairs:
        logger.info("No ACTIVE pairs for smoke test")
        return None

    pair = active_pairs[0]
    logger.info(f"Smoke test pair: {pair.pair_id[:12]}...")

    # Create runner with minimal config
    config = RunnerConfig(
        dry_run=False,
        default_max_position=max_size,
    )

    runner = CoveredArbRunner(
        store=store,
        clients=clients,
        orderbook_manager=orderbook_manager,
        config=config,
    )

    # Simulate first
    simulation = runner.simulate_pair(pair)

    if not simulation.is_tradeable:
        logger.info(f"Pair not tradeable for smoke test: {simulation.skip_reason}")
        return None

    # Override size for smoke test
    simulation.size_result.size = min(simulation.size_result.size, max_size)

    if simulation.size_result.size <= 0:
        logger.info("Smoke test size too small")
        return None

    # Execute
    logger.warning(
        f"Executing smoke test trade: size={simulation.size_result.size:.2f}"
    )
    result = runner.execute_trade(pair, simulation)

    if result.success:
        logger.info(
            f"Smoke test SUCCESS: PM order={result.pm_order.order_id}, "
            f"OP order={result.op_order.order_id}"
        )
    else:
        logger.error(f"Smoke test FAILED: {result.error or result.skip_reason}")

    return result
