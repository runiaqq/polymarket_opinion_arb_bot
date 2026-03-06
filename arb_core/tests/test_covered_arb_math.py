"""
Tests for covered arbitrage math and trading logic.
"""

import pytest

from arb_core.exchanges.exchange_clients import (
    DryRunClient,
    ExchangeClients,
    OrderRequest,
    OrderSide,
    OrderType,
    create_clients,
)
from arb_core.core.math_utils import (
    CoveredArbQuote,
    SimulationResult,
    SizeResult,
    compute_entry_size,
    simulate_covered_arb,
)
from arb_core.core.models import Pair, PairStatus, compute_pair_id
from arb_core.market_data.orderbook import Orderbook, OrderbookLevel, PairOrderbooks
from arb_core.runners.runner import CoveredArbRunner, RunnerConfig, TradeResult
from arb_core.core.store import PairStore


class TestCoveredArbQuote:
    """Tests for CoveredArbQuote calculations."""

    def test_cost_per_share(self):
        """Total cost is sum of both asks."""
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50)
        assert quote.cost_per_share == 0.95

    def test_fees_per_share(self):
        """Fees are sum of both platform fees."""
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50, pm_fee=0.01, op_fee=0.02)
        assert quote.fees_per_share == 0.03

    def test_total_cost_with_fees(self):
        """Total includes asks and fees."""
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50, pm_fee=0.01, op_fee=0.02)
        assert quote.total_cost == 0.98

    def test_payout_always_one(self):
        """Covered position always pays out 1."""
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50)
        assert quote.payout == 1.0

    def test_profit_per_share_positive(self):
        """Profit when total < 1."""
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50)
        assert quote.profit_per_share == pytest.approx(0.05)

    def test_profit_per_share_negative(self):
        """Loss when total > 1."""
        quote = CoveredArbQuote(pm_ask=0.55, op_ask=0.50)
        assert quote.profit_per_share == pytest.approx(-0.05)

    def test_profit_percent_calculation(self):
        """Profit percentage relative to investment."""
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50)  # cost=0.95, profit=0.05
        # profit_pct = (0.05 / 0.95) * 100 ≈ 5.26%
        assert quote.profit_percent == pytest.approx(5.263, rel=0.01)


class TestProfitabilityCondition:
    """Tests for entry condition: total <= (1 - min_profit_percent/100)."""

    def test_total_less_than_one_enters(self):
        """Entry when total < 1 (with min_profit=0)."""
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50)  # total=0.95
        assert quote.is_profitable(min_profit_percent=0.0) is True

    def test_total_equals_one_rejected_with_min_profit(self):
        """No entry when total == 1 and min_profit > 0."""
        quote = CoveredArbQuote(pm_ask=0.50, op_ask=0.50)  # total=1.0
        assert quote.is_profitable(min_profit_percent=1.0) is False

    def test_total_equals_one_accepted_with_zero_min_profit(self):
        """Entry allowed when total == 1 and min_profit == 0."""
        quote = CoveredArbQuote(pm_ask=0.50, op_ask=0.50)  # total=1.0
        assert quote.is_profitable(min_profit_percent=0.0) is True

    def test_total_greater_than_one_rejected(self):
        """No entry when total > 1."""
        quote = CoveredArbQuote(pm_ask=0.55, op_ask=0.50)  # total=1.05
        assert quote.is_profitable(min_profit_percent=0.0) is False

    def test_min_profit_threshold(self):
        """Entry requires meeting min_profit threshold."""
        # total=0.98, need profit >= 2%, so total <= 0.98 (just passes)
        quote = CoveredArbQuote(pm_ask=0.48, op_ask=0.50)  # total=0.98
        assert quote.is_profitable(min_profit_percent=2.0) is True

        # total=0.985, need profit >= 2%, so total <= 0.98 (fails)
        quote2 = CoveredArbQuote(pm_ask=0.485, op_ask=0.50)  # total=0.985
        assert quote2.is_profitable(min_profit_percent=2.0) is False

    def test_fee_inclusion_in_profitability(self):
        """Fees affect profitability check."""
        # cost=0.95, fees=0.03 -> total=0.98
        quote = CoveredArbQuote(pm_ask=0.45, op_ask=0.50, pm_fee=0.01, op_fee=0.02)
        assert quote.total_cost == 0.98
        assert quote.is_profitable(min_profit_percent=2.0) is True
        assert quote.is_profitable(min_profit_percent=3.0) is False


class TestSizeComputation:
    """Tests for symmetric size computation."""

    def test_size_respects_max_position(self):
        """Size limited by max_position."""
        result = compute_entry_size(
            max_position=10.0,
            pm_depth=100.0,
            op_depth=100.0,
            pm_balance=1000.0,
            op_balance=1000.0,
            pm_ask=0.50,
            op_ask=0.50,
        )
        assert result.size == 10.0
        assert result.limited_by_max_position is True

    def test_size_respects_pm_depth(self):
        """Size limited by PM depth."""
        result = compute_entry_size(
            max_position=100.0,
            pm_depth=5.0,
            op_depth=100.0,
            pm_balance=1000.0,
            op_balance=1000.0,
            pm_ask=0.50,
            op_ask=0.50,
        )
        assert result.size == 5.0
        assert result.limited_by_pm_depth is True

    def test_size_respects_op_depth(self):
        """Size limited by OP depth."""
        result = compute_entry_size(
            max_position=100.0,
            pm_depth=100.0,
            op_depth=3.0,
            pm_balance=1000.0,
            op_balance=1000.0,
            pm_ask=0.50,
            op_ask=0.50,
        )
        assert result.size == 3.0
        assert result.limited_by_op_depth is True

    def test_size_respects_pm_balance(self):
        """Size limited by PM balance."""
        # balance=5, ask=0.5 -> can buy 10 shares
        result = compute_entry_size(
            max_position=100.0,
            pm_depth=100.0,
            op_depth=100.0,
            pm_balance=5.0,
            op_balance=1000.0,
            pm_ask=0.50,
            op_ask=0.50,
        )
        assert result.size == 10.0
        assert result.limited_by_pm_balance is True

    def test_size_respects_op_balance(self):
        """Size limited by OP balance."""
        # balance=3, ask=0.5 -> can buy 6 shares
        result = compute_entry_size(
            max_position=100.0,
            pm_depth=100.0,
            op_depth=100.0,
            pm_balance=1000.0,
            op_balance=3.0,
            pm_ask=0.50,
            op_ask=0.50,
        )
        assert result.size == 6.0
        assert result.limited_by_op_balance is True

    def test_size_below_min_rejected(self):
        """Skip when computed size < min_order_size."""
        result = compute_entry_size(
            max_position=100.0,
            pm_depth=0.5,  # Very small depth
            op_depth=100.0,
            pm_balance=1000.0,
            op_balance=1000.0,
            pm_ask=0.50,
            op_ask=0.50,
            pm_min_size=1.0,  # Min size is 1
        )
        assert result.skip_reason == "size_below_min"
        assert result.is_valid is False

    def test_missing_pm_depth_skipped(self):
        """Skip when PM depth is zero."""
        result = compute_entry_size(
            max_position=100.0,
            pm_depth=0.0,
            op_depth=100.0,
            pm_balance=1000.0,
            op_balance=1000.0,
            pm_ask=0.50,
            op_ask=0.50,
        )
        assert result.skip_reason == "missing_depth_pm"
        assert result.is_valid is False

    def test_missing_op_depth_skipped(self):
        """Skip when OP depth is zero."""
        result = compute_entry_size(
            max_position=100.0,
            pm_depth=100.0,
            op_depth=0.0,
            pm_balance=1000.0,
            op_balance=1000.0,
            pm_ask=0.50,
            op_ask=0.50,
        )
        assert result.skip_reason == "missing_depth_op"
        assert result.is_valid is False


class TestSimulateCoveredArb:
    """Tests for full simulation."""

    def test_profitable_simulation(self):
        """Profitable simulation produces valid result."""
        quote = CoveredArbQuote(
            pm_ask=0.45,
            op_ask=0.50,
            pm_depth=20.0,
            op_depth=15.0,
        )
        result = simulate_covered_arb(
            quote=quote,
            max_position=10.0,
            min_profit_percent=0.0,
        )

        assert result.is_profitable is True
        assert result.is_tradeable is True
        assert result.size_result.size == 10.0  # Limited by max_position
        assert result.total_investment == pytest.approx(10.0 * 0.95)
        assert result.expected_profit == pytest.approx(10.0 * 0.05)

    def test_not_profitable_simulation(self):
        """Non-profitable simulation rejected."""
        quote = CoveredArbQuote(
            pm_ask=0.55,
            op_ask=0.50,  # total=1.05 > 1
            pm_depth=20.0,
            op_depth=15.0,
        )
        result = simulate_covered_arb(
            quote=quote,
            max_position=10.0,
            min_profit_percent=0.0,
        )

        assert result.is_profitable is False
        assert result.is_tradeable is False
        assert result.skip_reason == "not_profitable"


class TestDryRunClient:
    """Tests for dry-run client behavior."""

    def test_dry_run_never_places_real_order(self):
        """DryRunClient simulates orders without calling exchange."""
        client = DryRunClient(name="Test", balance=1000.0)

        order = OrderRequest(
            token_id="test_token",
            side=OrderSide.BUY,
            size=10.0,
            price=0.50,
            order_type=OrderType.LIMIT,
        )

        result = client.place_order(order)

        assert result.success is True
        assert result.is_simulated is True
        assert result.order_id.startswith("DRY-")
        assert len(client.get_orders()) == 1

    def test_dry_run_balance(self):
        """DryRunClient returns simulated balance."""
        client = DryRunClient(name="Test", balance=500.0)
        balance = client.get_balance()

        assert balance.available == 500.0
        assert balance.total == 500.0

    def test_create_clients_dry_run(self):
        """create_clients with dry_run=True creates DryRunClients."""
        clients = create_clients(dry_run=True)

        assert clients.is_dry_run is True
        assert isinstance(clients.pm_client, DryRunClient)
        assert isinstance(clients.op_client, DryRunClient)


class TestCoveredArbRunner:
    """Tests for the runner."""

    @pytest.fixture
    def store(self, tmp_path):
        """Create test store."""
        db_path = str(tmp_path / "test.db")
        return PairStore(db_path)

    @pytest.fixture
    def dry_run_clients(self):
        """Create dry-run clients."""
        return create_clients(dry_run=True, pm_balance=1000.0, op_balance=1000.0)

    def test_runner_dry_run_no_real_orders(self, store, dry_run_clients):
        """Runner in dry-run mode doesn't place real orders."""
        # Create a READY pair with tokens
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES", token="pm_token_123")
        store.set_op_selection(pair_id, "NO", token="op_token_456")
        store.activate(pair_id)

        # Create mock orderbook manager that returns fixed orderbooks
        class MockOrderbookManager:
            def fetch_pair(self, pm_token, op_token, op_question_id=None, op_side="YES"):
                pm_ob = Orderbook(
                    token_id=pm_token,
                    asks=[OrderbookLevel(price=0.45, size=100.0)],
                )
                op_ob = Orderbook(
                    token_id=op_token,
                    asks=[OrderbookLevel(price=0.50, size=100.0)],
                )
                return PairOrderbooks(pm_orderbook=pm_ob, op_orderbook=op_ob)

            def close(self):
                pass

        runner = CoveredArbRunner(
            store=store,
            clients=dry_run_clients,
            orderbook_manager=MockOrderbookManager(),
            config=RunnerConfig(dry_run=True),
        )

        # Run once
        results = runner.run_once()

        # Should have one result
        assert len(results) == 1
        result = results[0]

        # Trade should succeed (in dry-run)
        assert result.success is True
        assert result.pm_order is not None
        assert result.pm_order.is_simulated is True
        assert result.op_order is not None
        assert result.op_order.is_simulated is True

    def test_runner_skips_ready_pairs(self, store, dry_run_clients):
        """Runner only trades ACTIVE pairs, not READY."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES", token="pm_token_123")
        store.set_op_selection(pair_id, "NO", token="op_token_456")
        # Don't activate - leave as READY

        class MockOrderbookManager:
            def fetch_pair(self, pm_token, op_token, op_question_id=None, op_side="YES"):
                return PairOrderbooks(
                    pm_orderbook=Orderbook(token_id=pm_token),
                    op_orderbook=Orderbook(token_id=op_token),
                )

            def close(self):
                pass

        runner = CoveredArbRunner(
            store=store,
            clients=dry_run_clients,
            orderbook_manager=MockOrderbookManager(),
        )

        # Run once
        results = runner.run_once()

        # Should have no results (READY pairs are skipped)
        assert len(results) == 0


class TestIntegrationDryRun:
    """Integration tests with dry-run."""

    @pytest.fixture
    def store(self, tmp_path):
        """Create test store."""
        db_path = str(tmp_path / "test.db")
        return PairStore(db_path)

    def test_simulate_produces_message_payload(self, store):
        """Simulation for READY pair produces message content."""
        from arb_core.ui.telegram_ui import format_simulation_result

        # Create pair
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES", token="pm_token_123")
        store.set_op_selection(pair_id, "NO", token="op_token_456")
        pair = store.get_pair(pair_id)

        # Create simulation result
        quote = CoveredArbQuote(
            pm_ask=0.45,
            op_ask=0.50,
            pm_depth=100.0,
            op_depth=100.0,
        )
        simulation = simulate_covered_arb(
            quote=quote,
            max_position=pair.max_position,
            min_profit_percent=pair.min_profit_percent,
        )

        # Format message
        message = format_simulation_result(pair, simulation)

        # Verify message content
        assert "Результат симуляции" in message
        assert "0.45" in message  # PM ask
        assert "0.50" in message  # OP ask
        assert "Можно торговать" in message
