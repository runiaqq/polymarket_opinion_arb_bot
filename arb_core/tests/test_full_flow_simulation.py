"""
Full flow simulation test - demonstrates the complete Market-Hedge Mode logic.

This test creates a simulated pair and shows every step of the trading process.
"""

import pytest
from unittest.mock import MagicMock, patch
from decimal import Decimal

from ..exchanges.exchange_clients import (
    DryRunClient,
    ExchangeClients,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
    AccountBalance,
)
from ..runners.market_hedge_runner import (
    MarketHedgeConfig,
    MarketHedgeRunner,
)
from ..core.models import Pair, PairStatus
from ..market_data.orderbook import Orderbook, OrderbookLevel, OrderbookManager, PairOrderbooks
from ..core.store import PairStore


def create_mock_orderbook_manager(pm_ask: float, op_ask: float, depth: float = 1000):
    """Create a mock orderbook manager with specified prices."""
    manager = MagicMock(spec=OrderbookManager)
    
    pm_ob = Orderbook(
        token_id="pm_token_123",
        bids=[OrderbookLevel(pm_ask - 0.01, depth)],
        asks=[OrderbookLevel(pm_ask, depth)],
    )
    op_ob = Orderbook(
        token_id="op_token_456", 
        bids=[OrderbookLevel(op_ask - 0.01, depth)],
        asks=[OrderbookLevel(op_ask, depth)],
    )
    
    pair_ob = PairOrderbooks(pm_orderbook=pm_ob, op_orderbook=op_ob)
    manager.fetch_pair.return_value = pair_ob
    manager.pm_client = MagicMock()
    manager.pm_client.fetch.return_value = pm_ob
    manager.op_client = MagicMock()
    manager.op_client.fetch.return_value = op_ob
    
    return manager


class TestFullFlowSimulation:
    """Complete flow simulation tests."""

    def test_profitable_pair_full_flow(self, tmp_path):
        """
        Test a profitable pair through the complete flow.
        
        Scenario:
        - PM YES price: $0.45
        - OP NO price: $0.45
        - Total cost: $0.90 per share
        - Profit: $0.10 per share (10%)
        - max_position: 20 shares
        - Balances: $50 on each exchange
        """
        print("\n" + "="*60)
        print("TEST: Profitable pair - full cycle")
        print("="*60)
        
        # Setup
        db_path = str(tmp_path / "test.db")
        store = PairStore(db_path)
        
        # Create clients with $50 balance each
        pm_client = DryRunClient(name="PM", balance=50.0)
        op_client = DryRunClient(name="OP", balance=50.0)
        clients = ExchangeClients(pm_client=pm_client, op_client=op_client, is_dry_run=True)
        
        # Mock orderbook with profitable spread
        pm_ask = 0.45
        op_ask = 0.45
        orderbook_manager = create_mock_orderbook_manager(pm_ask, op_ask, depth=100)
        
        # Config - disable fees for clear math in test
        config = MarketHedgeConfig(
            dry_run=True,
            min_spread_for_entry=0.01,  # 1% minimum profit
            op_taker_fee=0.0,  # No fees for clear math
            op_min_fee=0.0,  # No minimum fee for test
            min_net_profit=0.01,  # Low threshold for test
        )
        
        # Runner
        runner = MarketHedgeRunner(
            store=store,
            clients=clients,
            orderbook_manager=orderbook_manager,
            config=config,
        )
        
        # Enable trading (disabled by default for safety)
        runner.enable_trading()
        
        # Create test pair in store first (required for foreign key constraint)
        store.upsert_pair(
            pair_id="test-pair-profitable",
            pm_url="https://polymarket.com/event/test-event",
            op_url="https://opinion.trade/topic?topicId=1234",
            max_position=20.0,
        )
        
        # Create pair object for runner
        pair = Pair(
            pair_id="test-pair-profitable",
            polymarket_url="https://polymarket.com/event/test-event",
            opinion_url="https://opinion.trade/topic?topicId=1234",
            status=PairStatus.ACTIVE,
            pm_token="pm_token_123",
            op_token="op_token_456",
            pm_side="YES",
            op_side="NO",
            max_position=20.0,
            min_profit_percent=0.0,
        )
        
        print(f"\n[INPUT DATA]")
        print(f"  PM YES price: ${pm_ask:.2f}")
        print(f"  OP NO price: ${op_ask:.2f}")
        print(f"  Sum: ${pm_ask + op_ask:.2f}")
        print(f"  Payout: $1.00")
        print(f"  Profit/share: ${1.0 - pm_ask - op_ask:.2f} ({(1.0 - pm_ask - op_ask)*100:.1f}%)")
        print(f"  max_position: {pair.max_position}")
        print(f"  PM balance: ${pm_client._balance:.2f}")
        print(f"  OP balance: ${op_client._balance:.2f}")
        
        # Run simulation
        print(f"\n[SIMULATION]")
        simulation = runner.simulate_pair(pair)
        
        print(f"  PM Ask: ${simulation.quote.pm_ask:.4f}")
        print(f"  OP Ask: ${simulation.quote.op_ask:.4f}")
        print(f"  Total cost: ${simulation.quote.total_cost:.4f}")
        print(f"  Profit %: {simulation.quote.profit_percent:.2f}%")
        print(f"  Size: {simulation.size_result.size:.2f} contracts")
        print(f"  Is profitable: {simulation.is_profitable}")
        print(f"  Is tradeable: {simulation.is_tradeable}")
        
        if simulation.size_result.limited_by_max_position:
            print(f"  Limit: max_position")
        if simulation.size_result.limited_by_pm_balance:
            print(f"  Limit: PM balance")
        if simulation.size_result.limited_by_op_balance:
            print(f"  Limit: OP balance")
        
        # Check simulation is correct
        assert simulation.is_profitable, "Should be profitable"
        assert simulation.size_result.size == 20.0, f"Size should be 20, got {simulation.size_result.size}"
        
        # Calculate expected costs
        pm_cost = simulation.size_result.size * pm_ask
        op_cost = simulation.size_result.size * op_ask
        total_cost = pm_cost + op_cost
        expected_payout = simulation.size_result.size * 1.0
        expected_profit = expected_payout - total_cost
        
        print(f"\n[TRADE CALCULATION]")
        print(f"  PM: {simulation.size_result.size:.0f} x ${pm_ask:.2f} = ${pm_cost:.2f}")
        print(f"  OP: {simulation.size_result.size:.0f} x ${op_ask:.2f} = ${op_cost:.2f}")
        print(f"  Total invested: ${total_cost:.2f}")
        print(f"  Guaranteed payout: ${expected_payout:.2f}")
        print(f"  Profit: ${expected_profit:.2f} ({expected_profit/total_cost*100:.1f}%)")
        
        # Place orders (dry-run)
        print(f"\n[PLACING ORDERS (dry-run)]")
        trade_id = runner.place_dual_orders(pair)
        
        assert trade_id is not None, "Trade should be created"
        
        # Verify trade in database
        trade = store.get_trade(trade_id)
        print(f"\n[DATABASE RECORD]")
        print(f"  Trade ID: {trade.trade_id[:12]}...")
        print(f"  Pair ID: {trade.pair_id}")
        print(f"  Entry size: {trade.entry_size}")
        print(f"  Status: {trade.status.value}")
        
        # Verify the trade was recorded correctly
        assert trade.entry_size == 20.0, f"Entry size should be 20, got {trade.entry_size}"
        assert trade.pair_id == "test-pair-profitable"
        
        print(f"\n[OK] TEST PASSED!")
        print("="*60)

    def test_unprofitable_pair_rejected(self, tmp_path):
        """
        Test that unprofitable pairs are rejected.
        
        Scenario:
        - PM YES price: $0.55
        - OP NO price: $0.50
        - Total cost: $1.05 per share (LOSS!)
        """
        print("\n" + "="*60)
        print("TEST: Unprofitable pair - should be rejected")
        print("="*60)
        
        db_path = str(tmp_path / "test.db")
        store = PairStore(db_path)
        
        pm_client = DryRunClient(name="PM", balance=50.0)
        op_client = DryRunClient(name="OP", balance=50.0)
        clients = ExchangeClients(pm_client=pm_client, op_client=op_client, is_dry_run=True)
        
        # Unprofitable prices: 0.55 + 0.50 = 1.05 > 1.00
        pm_ask = 0.55
        op_ask = 0.50
        orderbook_manager = create_mock_orderbook_manager(pm_ask, op_ask)
        
        config = MarketHedgeConfig(dry_run=True, min_spread_for_entry=0.01)
        runner = MarketHedgeRunner(
            store=store,
            clients=clients,
            orderbook_manager=orderbook_manager,
            config=config,
        )
        
        pair = Pair(
            pair_id="test-pair-unprofitable",
            polymarket_url="https://polymarket.com/event/test",
            opinion_url="https://opinion.trade/topic?topicId=1234",
            status=PairStatus.ACTIVE,
            pm_token="pm_token",
            op_token="op_token",
            pm_side="YES",
            op_side="NO",
            max_position=20.0,
        )
        
        print(f"\n[INPUT DATA]")
        print(f"  PM YES: ${pm_ask:.2f}")
        print(f"  OP NO: ${op_ask:.2f}")
        print(f"  Sum: ${pm_ask + op_ask:.2f}")
        print(f"  Payout: $1.00")
        print(f"  LOSS: ${pm_ask + op_ask - 1.0:.2f} per share!")
        
        # Try to place orders
        trade_id = runner.place_dual_orders(pair)
        
        print(f"\n[RESULT]")
        print(f"  Orders placed: {'Yes' if trade_id else 'No'}")
        
        assert trade_id is None, "Unprofitable pair should not trade"
        print(f"\n[OK] Unprofitable pair correctly rejected!")
        print("="*60)

    def test_insufficient_balance_rejected(self, tmp_path):
        """
        Test that trades are rejected when balance is insufficient.
        
        Scenario:
        - Profitable pair but only $5 on each exchange
        - Need $9 on each ($0.45 × 20 = $9)
        """
        print("\n" + "="*60)
        print("TEST: Insufficient balance - order size reduced")
        print("="*60)
        
        db_path = str(tmp_path / "test.db")
        store = PairStore(db_path)
        
        # Low balance
        pm_client = DryRunClient(name="PM", balance=5.0)
        op_client = DryRunClient(name="OP", balance=5.0)
        clients = ExchangeClients(pm_client=pm_client, op_client=op_client, is_dry_run=True)
        
        pm_ask = 0.45
        op_ask = 0.45
        orderbook_manager = create_mock_orderbook_manager(pm_ask, op_ask)
        
        config = MarketHedgeConfig(dry_run=True, min_spread_for_entry=0.01, op_taker_fee=0.0)
        runner = MarketHedgeRunner(
            store=store,
            clients=clients,
            orderbook_manager=orderbook_manager,
            config=config,
        )
        
        pair = Pair(
            pair_id="test-pair-low-balance",
            polymarket_url="https://polymarket.com/event/test",
            opinion_url="https://opinion.trade/topic?topicId=1234",
            status=PairStatus.ACTIVE,
            pm_token="pm_token",
            op_token="op_token",
            pm_side="YES",
            op_side="NO",
            max_position=20.0,  # Want 20 but can't afford
        )
        
        print(f"\n[INPUT DATA]")
        print(f"  PM balance: ${pm_client._balance:.2f}")
        print(f"  OP balance: ${op_client._balance:.2f}")
        print(f"  max_position: {pair.max_position}")
        print(f"  PM price: ${pm_ask:.2f}")
        print(f"  OP price: ${op_ask:.2f}")
        
        # Simulate
        simulation = runner.simulate_pair(pair)
        
        # Max affordable: $5 / $0.45 = 11.11 shares
        expected_size = min(5.0 / pm_ask, 5.0 / op_ask)
        
        print(f"\n[CALCULATION]")
        print(f"  Max by PM balance: ${pm_client._balance:.2f} / ${pm_ask:.2f} = {pm_client._balance/pm_ask:.2f}")
        print(f"  Max by OP balance: ${op_client._balance:.2f} / ${op_ask:.2f} = {op_client._balance/op_ask:.2f}")
        print(f"  Final size: {simulation.size_result.size:.2f}")
        
        assert simulation.size_result.size == pytest.approx(expected_size, rel=0.01)
        
        # Check dollar amounts
        pm_dollar = simulation.size_result.size * pm_ask
        op_dollar = simulation.size_result.size * op_ask
        
        print(f"\n[MINIMUM CHECK]")
        print(f"  PM order: ${pm_dollar:.2f} (min $1.00)")
        print(f"  OP order: ${op_dollar:.2f} (min $1.00)")
        
        if pm_dollar >= 1.0 and op_dollar >= 1.0:
            print(f"  [OK] Both orders pass minimum")
        else:
            print(f"  [WARN] One order below minimum")
        
        print("="*60)

    def test_minimum_order_size_check(self, tmp_path):
        """
        Test that orders below $1 minimum are rejected.
        
        Scenario:
        - Very low PM price ($0.05)
        - High OP price ($0.90)
        - Small balance leads to PM order < $1
        """
        print("\n" + "="*60)
        print("TEST: Minimum order size check ($1)")
        print("="*60)
        
        db_path = str(tmp_path / "test.db")
        store = PairStore(db_path)
        
        pm_client = DryRunClient(name="PM", balance=10.0)
        op_client = DryRunClient(name="OP", balance=10.0)
        clients = ExchangeClients(pm_client=pm_client, op_client=op_client, is_dry_run=True)
        
        # Asymmetric prices (like the real case)
        pm_ask = 0.05  # Very cheap
        op_ask = 0.90  # Expensive
        orderbook_manager = create_mock_orderbook_manager(pm_ask, op_ask)
        
        config = MarketHedgeConfig(dry_run=True, min_spread_for_entry=0.01, op_taker_fee=0.0)
        runner = MarketHedgeRunner(
            store=store,
            clients=clients,
            orderbook_manager=orderbook_manager,
            config=config,
        )
        
        pair = Pair(
            pair_id="test-pair-asymmetric",
            polymarket_url="https://polymarket.com/event/test",
            opinion_url="https://opinion.trade/topic?topicId=1234",
            status=PairStatus.ACTIVE,
            pm_token="pm_token",
            op_token="op_token",
            pm_side="YES",
            op_side="NO",
            max_position=30.0,
        )
        
        print(f"\n[INPUT DATA]")
        print(f"  PM YES: ${pm_ask:.2f} (5%)")
        print(f"  OP NO: ${op_ask:.2f} (90%)")
        print(f"  Sum: ${pm_ask + op_ask:.2f} (5% profit!)")
        print(f"  PM balance: ${pm_client._balance:.2f}")
        print(f"  OP balance: ${op_client._balance:.2f}")
        
        # Calculate
        op_balance_size = 10.0 / op_ask  # 11.11 shares max
        print(f"\n[CALCULATION]")
        print(f"  OP balance / price: ${op_client._balance:.2f} / ${op_ask:.2f} = {op_balance_size:.2f} shares")
        print(f"  PM cost: {op_balance_size:.2f} x ${pm_ask:.2f} = ${op_balance_size * pm_ask:.2f}")
        print(f"  OP cost: {op_balance_size:.2f} x ${op_ask:.2f} = ${op_balance_size * op_ask:.2f}")
        
        # Simulate
        simulation = runner.simulate_pair(pair)
        
        print(f"\n[CHECK]")
        print(f"  PM order: ${simulation.size_result.size * pm_ask:.2f}")
        
        if simulation.size_result.skip_reason:
            print(f"  Skip reason: {simulation.size_result.skip_reason}")
        
        pm_dollar = simulation.size_result.size * pm_ask
        assert pm_dollar < 1.0, "PM order should be below $1"
        assert "PM order too small" in (simulation.size_result.skip_reason or ""), \
            "Should have skip reason about PM being too small"
        
        # Try to place
        trade_id = runner.place_dual_orders(pair)
        assert trade_id is None, "Should not place orders when PM < $1"
        
        print(f"\n[OK] Order correctly rejected due to $1 minimum!")
        print("="*60)


def test_run_all_simulations(tmp_path):
    """Run all simulation tests with verbose output."""
    test = TestFullFlowSimulation()
    
    print("\n" + "="*70)
    print("      FULL LOGIC TEST: MARKET-HEDGE MODE")
    print("="*70)
    
    test.test_profitable_pair_full_flow(tmp_path)
    test.test_unprofitable_pair_rejected(tmp_path)
    test.test_insufficient_balance_rejected(tmp_path)
    test.test_minimum_order_size_check(tmp_path)
    
    print("\n" + "="*70)
    print("      ALL TESTS PASSED!")
    print("="*70 + "\n")
