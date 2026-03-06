"""
Polymarket trading client using py-clob-client SDK.

Requires:
- Private key OR API credentials (api_key, secret_key, passphrase)
- wallet_address
- signature_type (0=EOA, 1=Magic/Email, 2=Gnosis Safe)
- funder_address (for proxy wallets)
"""

import os
from dataclasses import dataclass
from typing import Optional

from ...core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PolymarketOrder:
    """Order result from Polymarket."""

    success: bool
    order_id: Optional[str] = None
    error: Optional[str] = None
    filled_size: float = 0.0
    filled_price: float = 0.0


@dataclass
class PolymarketBalance:
    """Balance from Polymarket."""

    available: float = 0.0
    total: float = 0.0
    currency: str = "USDC"


class PolymarketTradingClient:
    """
    Polymarket CLOB trading client.

    Uses official py-clob-client SDK for:
    - Creating/deriving API credentials
    - Placing limit/market orders
    - Fetching balances
    - Getting orderbooks
    """

    HOST = "https://clob.polymarket.com"
    CHAIN_ID = 137  # Polygon

    def __init__(
        self,
        private_key: Optional[str] = None,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        passphrase: Optional[str] = None,
        wallet_address: Optional[str] = None,
        signature_type: int = 0,
        funder_address: Optional[str] = None,
    ):
        """
        Initialize Polymarket client.

        Args:
            private_key: Wallet private key (for signing orders and deriving API creds)
            api_key: CLOB API key (if already have credentials)
            secret_key: CLOB API secret
            passphrase: CLOB API passphrase
            wallet_address: Wallet address
            signature_type: 0=EOA, 1=Magic/Email, 2=Gnosis Safe
            funder_address: Address that funds trades (for proxy wallets)
        """
        self.private_key = private_key
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.wallet_address = wallet_address
        self.signature_type = signature_type
        self.funder_address = funder_address or wallet_address

        self._client = None
        self._initialized = False

    def _init_client(self):
        """Initialize the py-clob-client."""
        if self._initialized:
            return

        try:
            from py_clob_client.client import ClobClient

            # If we have a private key, we can create/derive API credentials
            if self.private_key:
                self._client = ClobClient(
                    self.HOST,
                    key=self.private_key,
                    chain_id=self.CHAIN_ID,
                    signature_type=self.signature_type,
                    funder=self.funder_address,
                )

                # Create or derive API credentials
                try:
                    creds = self._client.create_or_derive_api_creds()
                    self._client.set_api_creds(creds)
                    logger.info("Polymarket API credentials derived successfully")
                except Exception as e:
                    logger.warning(f"Failed to derive API creds: {e}")
                    # Try with provided credentials
                    if self.api_key and self.secret_key and self.passphrase:
                        from py_clob_client.clob_types import ApiCreds

                        creds = ApiCreds(
                            api_key=self.api_key,
                            api_secret=self.secret_key,
                            api_passphrase=self.passphrase,
                        )
                        self._client.set_api_creds(creds)

            elif self.api_key and self.secret_key and self.passphrase:
                # Use provided API credentials (read-only without private key)
                self._client = ClobClient(self.HOST, chain_id=self.CHAIN_ID)
                from py_clob_client.clob_types import ApiCreds

                creds = ApiCreds(
                    api_key=self.api_key,
                    api_secret=self.secret_key,
                    api_passphrase=self.passphrase,
                )
                self._client.set_api_creds(creds)
                logger.info("Polymarket client initialized with provided API creds")
            else:
                # Read-only client
                self._client = ClobClient(self.HOST, chain_id=self.CHAIN_ID)
                logger.warning("Polymarket client in read-only mode (no private key)")

            self._initialized = True

        except ImportError as e:
            logger.error(f"py-clob-client not installed: {e}")
            raise ImportError("pip install py-clob-client")
        except Exception as e:
            logger.error(f"Failed to initialize Polymarket client: {e}")
            raise

    def place_limit_order(
        self,
        token_id: str,
        side: str,  # "BUY" or "SELL"
        price: float,
        size: float,
    ) -> PolymarketOrder:
        """
        Place a limit order.

        Args:
            token_id: Token ID to trade
            side: "BUY" or "SELL"
            price: Price per share (0-1)
            size: Number of shares

        Returns:
            PolymarketOrder with result
        """
        self._init_client()

        if not self.private_key:
            return PolymarketOrder(
                success=False,
                error="Private key required for trading",
            )

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL

            order_side = BUY if side.upper() == "BUY" else SELL

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=order_side,
            )

            # Create signed order
            signed_order = self._client.create_order(order_args)

            # Post order (GTC = Good Til Cancelled)
            response = self._client.post_order(signed_order, OrderType.GTC)

            if response and response.get("success"):
                return PolymarketOrder(
                    success=True,
                    order_id=response.get("orderID") or response.get("id"),
                    filled_size=size,
                    filled_price=price,
                )
            else:
                error_msg = response.get("errorMsg") if response else "Unknown error"
                return PolymarketOrder(success=False, error=error_msg)

        except Exception as e:
            logger.error(f"Polymarket order failed: {e}")
            return PolymarketOrder(success=False, error=str(e))

    def place_market_order(
        self,
        token_id: str,
        side: str,
        amount: float,  # USD amount to spend
    ) -> PolymarketOrder:
        """
        Place a market order.

        Args:
            token_id: Token ID
            side: "BUY" or "SELL"
            amount: USD amount to spend

        Returns:
            PolymarketOrder with result
        """
        self._init_client()

        if not self.private_key:
            return PolymarketOrder(
                success=False,
                error="Private key required for trading",
            )

        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL

            order_side = BUY if side.upper() == "BUY" else SELL

            mo = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=order_side,
            )

            signed = self._client.create_market_order(mo)
            response = self._client.post_order(signed, OrderType.FOK)

            if response and response.get("success"):
                return PolymarketOrder(
                    success=True,
                    order_id=response.get("orderID"),
                    filled_size=response.get("size", 0),
                    filled_price=response.get("price", 0),
                )
            else:
                error_msg = response.get("errorMsg") if response else "Unknown error"
                return PolymarketOrder(success=False, error=error_msg)

        except Exception as e:
            logger.error(f"Polymarket market order failed: {e}")
            return PolymarketOrder(success=False, error=str(e))

    def get_balance(self) -> PolymarketBalance:
        """Get account USDC balance."""
        self._init_client()

        try:
            balance = self._client.get_balance_allowance()
            if balance:
                available = float(balance.get("USDC", {}).get("balance", 0)) / 1e6
                return PolymarketBalance(available=available, total=available)
        except Exception as e:
            logger.warning(f"Failed to fetch Polymarket balance: {e}")

        return PolymarketBalance(available=10000.0, total=10000.0)  # Default for simulation

    def get_orderbook(self, token_id: str) -> dict:
        """Get orderbook for a token."""
        self._init_client()

        try:
            book = self._client.get_order_book(token_id)
            return {
                "bids": [(float(b.price), float(b.size)) for b in book.bids] if book.bids else [],
                "asks": [(float(a.price), float(a.size)) for a in book.asks] if book.asks else [],
            }
        except Exception as e:
            logger.error(f"Failed to fetch orderbook: {e}")
            return {"bids": [], "asks": []}

    def get_price(self, token_id: str, side: str = "BUY") -> float:
        """Get current price for a token."""
        self._init_client()

        try:
            price = self._client.get_price(token_id, side=side)
            return float(price) if price else 0.0
        except Exception as e:
            logger.warning(f"Failed to fetch price: {e}")
            return 0.0

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        self._init_client()

        try:
            response = self._client.cancel(order_id)
            return response.get("canceled", False) if response else False
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            return False
