"""
Account Pool for managing multiple trading accounts.

Supports 100+ accounts with unique proxies per account.
"""

import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .logging import get_logger

from ..exchanges.exchange_clients import (
    DryRunClient,
    ExchangeClients,
    OpinionClient,
    PolymarketClient,
)

logger = get_logger(__name__)


@dataclass
class TradingAccount:
    """Represents a single trading account with credentials for both platforms."""

    account_id: str

    # Polymarket credentials
    pm_private_key: str = ""
    pm_wallet_address: str = ""
    pm_signature_type: int = 0
    pm_funder_address: str = ""

    # Opinion credentials
    op_private_key: str = ""
    op_multi_sig_address: str = ""
    op_api_key: str = ""

    # Shared proxy (unique per account)
    proxy: str = ""

    # Status
    enabled: bool = True
    in_use: bool = False
    error_count: int = 0
    last_error: Optional[str] = None

    # Balance cache
    pm_balance: float = 0.0
    op_balance: float = 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "account_id": self.account_id,
            "pm_wallet_address": self.pm_wallet_address,
            "op_multi_sig_address": self.op_multi_sig_address,
            "proxy": self.proxy[:30] + "..." if len(self.proxy) > 30 else self.proxy,
            "enabled": self.enabled,
            "in_use": self.in_use,
            "error_count": self.error_count,
            "pm_balance": self.pm_balance,
            "op_balance": self.op_balance,
        }


class AccountPool:
    """
    Pool of trading accounts for parallel operation.

    Thread-safe account management with:
    - Acquiring/releasing accounts
    - Round-robin or balance-based selection
    - Error tracking and automatic disabling
    """

    def __init__(self, max_errors_before_disable: int = 5):
        self._accounts: Dict[str, TradingAccount] = {}
        self._clients_cache: Dict[str, ExchangeClients] = {}
        self._lock = threading.Lock()
        self.max_errors_before_disable = max_errors_before_disable

    def add_account(self, account: TradingAccount) -> None:
        """Add an account to the pool."""
        with self._lock:
            self._accounts[account.account_id] = account
            logger.info(f"Added account to pool: {account.account_id}")

    def add_accounts(self, accounts: List[TradingAccount]) -> None:
        """Add multiple accounts to the pool."""
        for account in accounts:
            self.add_account(account)

    def get_account(self, account_id: str) -> Optional[TradingAccount]:
        """Get a specific account by ID."""
        with self._lock:
            return self._accounts.get(account_id)

    def list_accounts(self, enabled_only: bool = True) -> List[TradingAccount]:
        """List all accounts."""
        with self._lock:
            if enabled_only:
                return [a for a in self._accounts.values() if a.enabled]
            return list(self._accounts.values())

    def acquire_account(self) -> Optional[TradingAccount]:
        """
        Acquire an available account for trading.

        Returns the account with highest combined balance that is not in use.
        Marks the account as in_use.
        """
        with self._lock:
            available = [
                a for a in self._accounts.values()
                if a.enabled and not a.in_use
            ]

            if not available:
                return None

            # Sort by combined balance (highest first)
            available.sort(
                key=lambda a: a.pm_balance + a.op_balance,
                reverse=True,
            )

            account = available[0]
            account.in_use = True
            return account

    def release_account(self, account_id: str, error: Optional[str] = None) -> None:
        """
        Release an account after use.

        If error is provided, increment error count and potentially disable.
        """
        with self._lock:
            account = self._accounts.get(account_id)
            if not account:
                return

            account.in_use = False

            if error:
                account.error_count += 1
                account.last_error = error
                logger.warning(
                    f"Account {account_id} error ({account.error_count}): {error[:50]}"
                )

                if account.error_count >= self.max_errors_before_disable:
                    account.enabled = False
                    logger.error(
                        f"Account {account_id} disabled after {account.error_count} errors"
                    )
            else:
                # Reset error count on success
                account.error_count = 0
                account.last_error = None

    def update_balances(self, account_id: str, pm_balance: float, op_balance: float) -> None:
        """Update cached balances for an account."""
        with self._lock:
            account = self._accounts.get(account_id)
            if account:
                account.pm_balance = pm_balance
                account.op_balance = op_balance

    def get_clients(self, account_id: str, dry_run: bool = False) -> Optional[ExchangeClients]:
        """
        Get exchange clients for an account.

        Caches clients for reuse.
        """
        cache_key = f"{account_id}:{'dry' if dry_run else 'live'}"

        with self._lock:
            if cache_key in self._clients_cache:
                return self._clients_cache[cache_key]

            account = self._accounts.get(account_id)
            if not account:
                return None

        # Create clients outside lock
        if dry_run:
            clients = ExchangeClients(
                pm_client=DryRunClient(name=f"PM-{account_id}", balance=10000.0),
                op_client=DryRunClient(name=f"OP-{account_id}", balance=10000.0),
                is_dry_run=True,
            )
        else:
            pm_client = PolymarketClient(
                private_key=account.pm_private_key,
                wallet_address=account.pm_wallet_address,
                signature_type=account.pm_signature_type,
                funder_address=account.pm_funder_address,
                proxy=account.proxy,
            )

            op_client = OpinionClient(
                private_key=account.op_private_key,
                multi_sig_address=account.op_multi_sig_address,
                api_key=account.op_api_key,
                proxy=account.proxy,
            )

            clients = ExchangeClients(
                pm_client=pm_client,
                op_client=op_client,
                is_dry_run=False,
            )

        with self._lock:
            self._clients_cache[cache_key] = clients

        return clients

    def get_pool_stats(self) -> dict:
        """Get pool statistics."""
        with self._lock:
            total = len(self._accounts)
            enabled = sum(1 for a in self._accounts.values() if a.enabled)
            in_use = sum(1 for a in self._accounts.values() if a.in_use)
            disabled = sum(1 for a in self._accounts.values() if not a.enabled)
            total_pm_balance = sum(a.pm_balance for a in self._accounts.values())
            total_op_balance = sum(a.op_balance for a in self._accounts.values())

            return {
                "total": total,
                "enabled": enabled,
                "in_use": in_use,
                "disabled": disabled,
                "available": enabled - in_use,
                "total_pm_balance": total_pm_balance,
                "total_op_balance": total_op_balance,
            }

    def refresh_all_balances(self, dry_run: bool = False) -> None:
        """Refresh balances for all accounts."""
        accounts = self.list_accounts(enabled_only=True)

        for account in accounts:
            try:
                clients = self.get_clients(account.account_id, dry_run=dry_run)
                if clients:
                    pm_balance = clients.pm_client.get_balance().available
                    op_balance = clients.op_client.get_balance().available
                    self.update_balances(account.account_id, pm_balance, op_balance)
            except Exception as e:
                logger.error(f"Error refreshing balance for {account.account_id}: {e}")

    def close_all(self) -> None:
        """Close all cached clients."""
        with self._lock:
            for clients in self._clients_cache.values():
                try:
                    clients.close()
                except Exception as e:
                    logger.error(f"Error closing clients: {e}")
            self._clients_cache.clear()


def load_accounts_from_config(accounts_data: list) -> List[TradingAccount]:
    """
    Load TradingAccount objects from config data.

    Expected format (accounts.json):
    [
        {
            "account_id": "acc1",
            "exchange": "Polymarket",
            "private_key": "...",
            "wallet_address": "...",
            "proxy": "http://..."
        },
        {
            "account_id": "acc1",
            "exchange": "Opinion",
            "private_key": "...",
            "multi_sig_address": "...",
            "api_key": "..."
        },
        ...
    ]

    Returns combined TradingAccount objects.
    """
    # Group by account_id
    grouped: Dict[str, dict] = {}

    for entry in accounts_data:
        account_id = entry.get("account_id", "unknown")
        exchange = entry.get("exchange", "").lower()

        if account_id not in grouped:
            grouped[account_id] = {
                "account_id": account_id,
                "proxy": entry.get("proxy", ""),
            }

        if exchange == "polymarket":
            grouped[account_id].update({
                "pm_private_key": entry.get("private_key", ""),
                "pm_wallet_address": entry.get("wallet_address", ""),
                "pm_signature_type": int(entry.get("signature_type", 0)),
                "pm_funder_address": entry.get("funder_address", ""),
            })
            # Use proxy from PM if not set
            if not grouped[account_id].get("proxy"):
                grouped[account_id]["proxy"] = entry.get("proxy", "")

        elif exchange == "opinion":
            grouped[account_id].update({
                "op_private_key": entry.get("private_key", ""),
                "op_multi_sig_address": entry.get("multi_sig_address", ""),
                "op_api_key": entry.get("api_key", ""),
            })

    # Convert to TradingAccount objects
    accounts = []
    for data in grouped.values():
        accounts.append(TradingAccount(
            account_id=data.get("account_id", "unknown"),
            pm_private_key=data.get("pm_private_key", ""),
            pm_wallet_address=data.get("pm_wallet_address", ""),
            pm_signature_type=data.get("pm_signature_type", 0),
            pm_funder_address=data.get("pm_funder_address", ""),
            op_private_key=data.get("op_private_key", ""),
            op_multi_sig_address=data.get("op_multi_sig_address", ""),
            op_api_key=data.get("op_api_key", ""),
            proxy=data.get("proxy", ""),
        ))

    logger.info(f"Loaded {len(accounts)} trading accounts from config")
    return accounts
