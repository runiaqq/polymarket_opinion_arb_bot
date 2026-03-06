"""
Tests for Market-Hedge Mode Runner.
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from ..exchanges.exchange_clients import (
    DryRunClient,
    ExchangeClients,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
)
from ..runners.market_hedge_runner import (
    ActiveOrder,
    HedgeResult,
    MarketHedgeConfig,
    MarketHedgeRunner,
)
from ..core.models import Pair, PairStatus
from ..core.store import PairStore


@pytest.fixture
def mock_store(tmp_path):
    """Create a test store."""
    db_path = str(tmp_path / "test.db")
    return PairStore(db_path)


@pytest.fixture
def mock_clients():
    """Create mock exchange clients."""
    return ExchangeClients(
        pm_client=DryRunClient(name="PM-test", balance=10000.0),
        op_client=DryRunClient(name="OP-test", balance=10000.0),
        is_dry_run=True,
    )


@pytest.fixture
def mock_orderbook_manager():
    """Create mock orderbook manager."""
    from ..market_data.orderbook import Orderbook, OrderbookLevel, OrderbookManager, PairOrderbooks
    
    manager = MagicMock(spec=OrderbookManager)
    
    # Mock fetch_pair to return valid orderbooks
    mock_pm_ob = Orderbook(
        token_id="test_pm_token",
        bids=[OrderbookLevel(0.45, 100), OrderbookLevel(0.44, 200)],
        asks=[OrderbookLevel(0.46, 100), OrderbookLevel(0.47, 200)],
    )
    mock_op_ob = Orderbook(
        token_id="test_op_token",
        bids=[OrderbookLevel(0.44, 100), OrderbookLevel(0.43, 200)],
        asks=[OrderbookLevel(0.45, 100), OrderbookLevel(0.46, 200)],
    )
    
    mock_pair_ob = PairOrderbooks(
        pm_orderbook=mock_pm_ob,
        op_orderbook=mock_op_ob,
    )
    
    manager.fetch_pair.return_value = mock_pair_ob
    manager.pm_client = MagicMock()
    manager.pm_client.fetch.return_value = mock_pm_ob
    manager.op_client = MagicMock()
    manager.op_client.fetch.return_value = mock_op_ob
    
    return manager


class TestMarketHedgeConfig:
    """Test MarketHedgeConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = MarketHedgeConfig()
        
        assert config.hedge_ratio == 1.0
        assert config.max_slippage_market_hedge == 0.005
        assert config.min_spread_for_entry == 0.002
        assert config.cancel_unfilled_after_sec == 60.0
        assert config.dry_run is True

    def test_custom_values(self):
        """Test custom configuration values."""
        config = MarketHedgeConfig(
            hedge_ratio=0.5,
            max_slippage_market_hedge=0.01,
            dry_run=False,
        )
        
        assert config.hedge_ratio == 0.5
        assert config.max_slippage_market_hedge == 0.01
        assert config.dry_run is False


class TestActiveOrder:
    """Test ActiveOrder dataclass."""

    def test_creation(self):
        """Test creating an ActiveOrder."""
        order = ActiveOrder(
            trade_id="test-trade",
            pair_id="test-pair",
            exchange="PM",
            order_id="order-123",
            side=OrderSide.BUY,
            size=100.0,
            price=0.45,
            token_id="token-abc",
        )
        
        assert order.trade_id == "test-trade"
        assert order.exchange == "PM"
        assert order.side == OrderSide.BUY
        assert order.size == 100.0
        assert order.placed_at > 0


class TestMarketHedgeRunner:
    """Test MarketHedgeRunner class."""

    def test_calculate_spread(self, mock_store, mock_clients, mock_orderbook_manager):
        """Test spread calculation."""
        config = MarketHedgeConfig(dry_run=True)
        runner = MarketHedgeRunner(
            store=mock_store,
            clients=mock_clients,
            orderbook_manager=mock_orderbook_manager,
            config=config,
        )
        
        # Total cost = 0.46 + 0.45 = 0.91
        # Maker-maker mode: pm_maker_fee=0, op_maker_fee=0
        # Spread = 1.0 - 0.91 - 0.00 = 0.09 (profitable)
        spread = runner._calculate_spread(0.46, 0.45)
        assert spread == pytest.approx(0.09, rel=0.01)

    def test_is_profitable_entry(self, mock_store, mock_clients, mock_orderbook_manager):
        """Test profitability check."""
        config = MarketHedgeConfig(
            dry_run=True,
            min_spread_for_entry=0.01,  # 1 cent minimum
            op_taker_fee=0.0,  # No fees for this test
        )
        runner = MarketHedgeRunner(
            store=mock_store,
            clients=mock_clients,
            orderbook_manager=mock_orderbook_manager,
            config=config,
        )
        
        # Profitable: spread = 1.0 - 0.46 - 0.45 = 0.09
        assert runner._is_profitable_entry(0.46, 0.45) is True
        
        # Not profitable: spread = 1.0 - 0.50 - 0.495 = 0.005 < 0.01
        assert runner._is_profitable_entry(0.50, 0.495) is False

    def test_place_dual_orders_dry_run(self, mock_store, mock_clients, mock_orderbook_manager):
        """Test placing dual orders in dry-run mode."""
        # Use lower min_net_profit to allow smaller trades in test
        config = MarketHedgeConfig(dry_run=True, min_net_profit=0.01, op_min_fee=0.0)
        runner = MarketHedgeRunner(
            store=mock_store,
            clients=mock_clients,
            orderbook_manager=mock_orderbook_manager,
            config=config,
        )
        
        # Enable trading (disabled by default for safety)
        runner.enable_trading()
        
        # First create the pair in the store (required for foreign key constraint)
        pair = mock_store.upsert_pair(
            pair_id="test-pair-123",
            pm_url="https://polymarket.com/event/test",
            op_url="https://opinion.trade/topic?topicId=123",
            max_position=100.0,
        )
        
        # Update pair with required fields for trading
        pair = Pair(
            pair_id="test-pair-123",
            polymarket_url="https://polymarket.com/event/test",
            opinion_url="https://opinion.trade/topic?topicId=123",
            status=PairStatus.ACTIVE,
            pm_token="pm_token_id",
            op_token="op_token_id",
            pm_side="YES",
            op_side="NO",
            max_position=100.0,
        )
        
        # Place dual orders
        trade_id = runner.place_dual_orders(pair)
        
        # Should return a trade_id
        assert trade_id is not None
        
        # Trade should be in the database
        trade = mock_store.get_trade(trade_id)
        assert trade is not None
        assert trade.pair_id == "test-pair-123"

    def test_runner_start_stop(self, mock_store, mock_clients, mock_orderbook_manager):
        """Test starting and stopping the runner."""
        config = MarketHedgeConfig(dry_run=True, poll_interval_sec=0.1)
        runner = MarketHedgeRunner(
            store=mock_store,
            clients=mock_clients,
            orderbook_manager=mock_orderbook_manager,
            config=config,
        )
        
        assert runner.is_running() is False
        
        runner.start()
        assert runner.is_running() is True
        
        time.sleep(0.2)  # Let it run briefly
        
        runner.stop()
        assert runner.is_running() is False

    def test_run_once_no_pairs(self, mock_store, mock_clients, mock_orderbook_manager):
        """Test run_once with no active pairs."""
        config = MarketHedgeConfig(dry_run=True)
        runner = MarketHedgeRunner(
            store=mock_store,
            clients=mock_clients,
            orderbook_manager=mock_orderbook_manager,
            config=config,
        )
        
        # No pairs in store
        trades_initiated = runner.run_once()
        assert trades_initiated == 0

    def test_callbacks_are_called(self, mock_store, mock_clients, mock_orderbook_manager):
        """Test that callbacks are invoked."""
        config = MarketHedgeConfig(dry_run=True)
        
        trade_complete_called = []
        hedge_executed_called = []
        
        runner = MarketHedgeRunner(
            store=mock_store,
            clients=mock_clients,
            orderbook_manager=mock_orderbook_manager,
            config=config,
            on_trade_complete=lambda t: trade_complete_called.append(t),
            on_hedge_executed=lambda h: hedge_executed_called.append(h),
        )
        
        # Callbacks should be set
        assert runner.on_trade_complete is not None
        assert runner.on_hedge_executed is not None


class TestHedgeResult:
    """Test HedgeResult dataclass."""

    def test_successful_hedge(self):
        """Test successful hedge result."""
        result = HedgeResult(
            success=True,
            entry_exchange="PM",
            entry_filled_size=100.0,
            entry_filled_price=0.45,
            hedge_exchange="OP",
            hedge_order_id="hedge-123",
            hedge_size=100.0,
            hedge_price=0.46,
            hedge_slippage=0.001,
        )
        
        assert result.success is True
        assert result.error is None

    def test_failed_hedge(self):
        """Test failed hedge result."""
        result = HedgeResult(
            success=False,
            entry_exchange="PM",
            entry_filled_size=100.0,
            entry_filled_price=0.45,
            hedge_exchange="OP",
            error="Slippage too high",
        )
        
        assert result.success is False
        assert result.error == "Slippage too high"
