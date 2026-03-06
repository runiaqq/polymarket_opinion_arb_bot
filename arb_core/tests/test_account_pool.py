"""
Tests for Account Pool (multi-account support).
"""

import pytest
from unittest.mock import MagicMock, patch

from ..core.account_pool import (
    AccountPool,
    TradingAccount,
    load_accounts_from_config,
)


class TestTradingAccount:
    """Test TradingAccount dataclass."""

    def test_creation(self):
        """Test creating a TradingAccount."""
        account = TradingAccount(
            account_id="acc1",
            pm_private_key="0xabc123",
            pm_wallet_address="0xwallet",
            op_private_key="0xdef456",
            op_multi_sig_address="0xmultisig",
            proxy="http://proxy.example.com:8080",
        )
        
        assert account.account_id == "acc1"
        assert account.pm_private_key == "0xabc123"
        assert account.op_multi_sig_address == "0xmultisig"
        assert account.proxy == "http://proxy.example.com:8080"
        assert account.enabled is True
        assert account.in_use is False

    def test_to_dict(self):
        """Test converting account to dict."""
        account = TradingAccount(
            account_id="acc1",
            pm_wallet_address="0xwallet123",
            op_multi_sig_address="0xmultisig456",
            pm_balance=1000.0,
            op_balance=500.0,
        )
        
        d = account.to_dict()
        
        assert d["account_id"] == "acc1"
        assert d["pm_wallet_address"] == "0xwallet123"
        assert d["pm_balance"] == 1000.0
        assert d["op_balance"] == 500.0


class TestAccountPool:
    """Test AccountPool class."""

    def test_add_account(self):
        """Test adding accounts to pool."""
        pool = AccountPool()
        
        account = TradingAccount(account_id="acc1")
        pool.add_account(account)
        
        assert pool.get_account("acc1") is not None
        assert pool.get_account("nonexistent") is None

    def test_add_multiple_accounts(self):
        """Test adding multiple accounts."""
        pool = AccountPool()
        
        accounts = [
            TradingAccount(account_id="acc1"),
            TradingAccount(account_id="acc2"),
            TradingAccount(account_id="acc3"),
        ]
        pool.add_accounts(accounts)
        
        assert len(pool.list_accounts()) == 3

    def test_list_enabled_only(self):
        """Test listing only enabled accounts."""
        pool = AccountPool()
        
        accounts = [
            TradingAccount(account_id="acc1", enabled=True),
            TradingAccount(account_id="acc2", enabled=False),
            TradingAccount(account_id="acc3", enabled=True),
        ]
        pool.add_accounts(accounts)
        
        enabled = pool.list_accounts(enabled_only=True)
        all_accounts = pool.list_accounts(enabled_only=False)
        
        assert len(enabled) == 2
        assert len(all_accounts) == 3

    def test_acquire_release_account(self):
        """Test acquiring and releasing accounts."""
        pool = AccountPool()
        
        account = TradingAccount(
            account_id="acc1",
            pm_balance=1000.0,
            op_balance=500.0,
        )
        pool.add_account(account)
        
        # Acquire
        acquired = pool.acquire_account()
        assert acquired is not None
        assert acquired.account_id == "acc1"
        assert acquired.in_use is True
        
        # Cannot acquire again (only one available)
        second = pool.acquire_account()
        assert second is None
        
        # Release
        pool.release_account("acc1")
        account = pool.get_account("acc1")
        assert account.in_use is False
        
        # Can acquire again
        acquired_again = pool.acquire_account()
        assert acquired_again is not None

    def test_acquire_selects_highest_balance(self):
        """Test that acquire selects account with highest balance."""
        pool = AccountPool()
        
        accounts = [
            TradingAccount(account_id="low", pm_balance=100, op_balance=100),
            TradingAccount(account_id="high", pm_balance=1000, op_balance=1000),
            TradingAccount(account_id="mid", pm_balance=500, op_balance=500),
        ]
        pool.add_accounts(accounts)
        
        acquired = pool.acquire_account()
        assert acquired.account_id == "high"

    def test_release_with_error(self):
        """Test releasing account with error increments count."""
        pool = AccountPool(max_errors_before_disable=3)
        
        account = TradingAccount(account_id="acc1")
        pool.add_account(account)
        
        pool.acquire_account()
        pool.release_account("acc1", error="Connection failed")
        
        account = pool.get_account("acc1")
        assert account.error_count == 1
        assert account.last_error == "Connection failed"
        assert account.enabled is True  # Not disabled yet

    def test_disable_after_max_errors(self):
        """Test account is disabled after max errors."""
        pool = AccountPool(max_errors_before_disable=2)
        
        account = TradingAccount(account_id="acc1")
        pool.add_account(account)
        
        # First error
        pool.acquire_account()
        pool.release_account("acc1", error="Error 1")
        assert pool.get_account("acc1").enabled is True
        
        # Second error - should disable
        pool.acquire_account()
        pool.release_account("acc1", error="Error 2")
        assert pool.get_account("acc1").enabled is False

    def test_success_resets_error_count(self):
        """Test successful release resets error count."""
        pool = AccountPool()
        
        account = TradingAccount(account_id="acc1")
        pool.add_account(account)
        
        # Accumulate some errors
        pool.acquire_account()
        pool.release_account("acc1", error="Error 1")
        assert pool.get_account("acc1").error_count == 1
        
        # Success resets
        pool.acquire_account()
        pool.release_account("acc1")  # No error
        assert pool.get_account("acc1").error_count == 0
        assert pool.get_account("acc1").last_error is None

    def test_update_balances(self):
        """Test updating account balances."""
        pool = AccountPool()
        
        account = TradingAccount(account_id="acc1")
        pool.add_account(account)
        
        pool.update_balances("acc1", pm_balance=1500.0, op_balance=750.0)
        
        updated = pool.get_account("acc1")
        assert updated.pm_balance == 1500.0
        assert updated.op_balance == 750.0

    def test_pool_stats(self):
        """Test getting pool statistics."""
        pool = AccountPool()
        
        accounts = [
            TradingAccount(account_id="a1", enabled=True, pm_balance=100, op_balance=50),
            TradingAccount(account_id="a2", enabled=True, pm_balance=200, op_balance=100),
            TradingAccount(account_id="a3", enabled=False, pm_balance=50, op_balance=25),
        ]
        pool.add_accounts(accounts)
        
        # Acquire one
        pool.acquire_account()
        
        stats = pool.get_pool_stats()
        
        assert stats["total"] == 3
        assert stats["enabled"] == 2
        assert stats["in_use"] == 1
        assert stats["disabled"] == 1
        assert stats["available"] == 1
        assert stats["total_pm_balance"] == 350
        assert stats["total_op_balance"] == 175

    def test_get_clients_dry_run(self):
        """Test getting dry-run clients for account."""
        pool = AccountPool()
        
        account = TradingAccount(
            account_id="acc1",
            pm_private_key="0xabc",
            op_private_key="0xdef",
        )
        pool.add_account(account)
        
        clients = pool.get_clients("acc1", dry_run=True)
        
        assert clients is not None
        assert clients.is_dry_run is True


class TestLoadAccountsFromConfig:
    """Test loading accounts from config data."""

    def test_load_single_account(self):
        """Test loading a single account with both exchanges."""
        config_data = [
            {
                "account_id": "main",
                "exchange": "Polymarket",
                "private_key": "0xpm_key",
                "wallet_address": "0xpm_wallet",
                "proxy": "http://proxy1:8080",
            },
            {
                "account_id": "main",
                "exchange": "Opinion",
                "private_key": "0xop_key",
                "multi_sig_address": "0xop_multisig",
            },
        ]
        
        accounts = load_accounts_from_config(config_data)
        
        assert len(accounts) == 1
        account = accounts[0]
        assert account.account_id == "main"
        assert account.pm_private_key == "0xpm_key"
        assert account.op_private_key == "0xop_key"
        assert account.proxy == "http://proxy1:8080"

    def test_load_multiple_accounts(self):
        """Test loading multiple accounts."""
        config_data = [
            {"account_id": "acc1", "exchange": "Polymarket", "private_key": "0xpm1"},
            {"account_id": "acc1", "exchange": "Opinion", "private_key": "0xop1"},
            {"account_id": "acc2", "exchange": "Polymarket", "private_key": "0xpm2"},
            {"account_id": "acc2", "exchange": "Opinion", "private_key": "0xop2"},
        ]
        
        accounts = load_accounts_from_config(config_data)
        
        assert len(accounts) == 2
        account_ids = {a.account_id for a in accounts}
        assert account_ids == {"acc1", "acc2"}

    def test_load_with_unique_proxies(self):
        """Test that each account gets its unique proxy."""
        config_data = [
            {"account_id": "acc1", "exchange": "Polymarket", "private_key": "k1", "proxy": "http://p1:8080"},
            {"account_id": "acc1", "exchange": "Opinion", "private_key": "k1"},
            {"account_id": "acc2", "exchange": "Polymarket", "private_key": "k2", "proxy": "http://p2:8080"},
            {"account_id": "acc2", "exchange": "Opinion", "private_key": "k2"},
        ]
        
        accounts = load_accounts_from_config(config_data)
        
        proxies = {a.account_id: a.proxy for a in accounts}
        assert proxies["acc1"] == "http://p1:8080"
        assert proxies["acc2"] == "http://p2:8080"
