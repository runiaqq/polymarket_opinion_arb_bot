"""
Exchange clients for order placement.

Supports both dry-run (simulation) and live trading.

Features:
- Rate limiting to prevent API throttling
- Retry logic for transient errors
- Thread-safe operations
"""

import hashlib
import hmac
import json
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import requests

from ..core.logging import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """
    Simple rate limiter using sliding window.
    
    Thread-safe implementation to prevent API throttling.
    """
    
    def __init__(self, max_requests: int, window_seconds: float):
        """
        Args:
            max_requests: Maximum requests allowed in window
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: deque = deque()
        self._lock = threading.Lock()
    
    def acquire(self) -> None:
        """
        Wait until a request can be made within rate limits.
        Blocks if rate limit would be exceeded.
        """
        with self._lock:
            now = time.time()
            
            # Remove timestamps outside the window
            while self._timestamps and self._timestamps[0] < now - self.window_seconds:
                self._timestamps.popleft()
            
            # If at limit, wait until oldest request expires
            if len(self._timestamps) >= self.max_requests:
                wait_time = self._timestamps[0] + self.window_seconds - now
                if wait_time > 0:
                    logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    # Clean up again after waiting
                    now = time.time()
                    while self._timestamps and self._timestamps[0] < now - self.window_seconds:
                        self._timestamps.popleft()
            
            # Record this request
            self._timestamps.append(time.time())


# Default rate limiters for each exchange
# Conservative limits to avoid 429 errors
PM_RATE_LIMITER = RateLimiter(max_requests=10, window_seconds=1.0)  # 10 req/sec
OP_RATE_LIMITER = RateLimiter(max_requests=5, window_seconds=1.0)   # 5 req/sec


def is_transient_error(error: Exception) -> bool:
    """Check if an error is transient and worth retrying."""
    error_str = str(error).lower()
    transient_indicators = [
        "timeout",
        "timed out",
        "connection",
        "network",
        "503",
        "502",
        "429",  # Rate limit
        "temporarily",
        "retry",
        "unavailable",
    ]
    return any(indicator in error_str for indicator in transient_indicators)


def retry_on_transient(
    func,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
):
    """
    Decorator/wrapper for retrying on transient errors.
    Uses exponential backoff.
    """
    def wrapper(*args, **kwargs):
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries and is_transient_error(e):
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"Transient error (attempt {attempt + 1}), retrying in {delay:.1f}s: {e}")
                    time.sleep(delay)
                else:
                    raise
        raise last_error
    return wrapper


class OrderSide(Enum):
    """Order side."""

    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    """Order type."""

    LIMIT = "LIMIT"
    MARKET = "MARKET"


@dataclass
class OrderRequest:
    """Order request."""

    token_id: str
    side: OrderSide
    size: float
    price: float
    order_type: OrderType = OrderType.LIMIT
    topic_id: Optional[str] = None  # Opinion topic ID for order placement


@dataclass
class OrderResult:
    """Order placement result."""

    success: bool
    order_id: Optional[str] = None
    error: Optional[str] = None
    filled_size: float = 0.0
    filled_price: float = 0.0
    raw_response: Optional[dict] = None

    # For dry-run
    is_simulated: bool = False


@dataclass
class AccountBalance:
    """Account balance."""

    available: float = 0.0
    total: float = 0.0
    currency: str = "USDC"


@dataclass
class OrderStatus:
    """Order status information."""
    
    order_id: str
    status: str  # "open", "filled", "partially_filled", "cancelled", "unknown"
    filled_size: float = 0.0
    remaining_size: float = 0.0
    filled_price: float = 0.0
    raw_response: Optional[dict] = None


class ExchangeClient(ABC):
    """Base class for exchange clients."""

    @abstractmethod
    def place_order(self, order: OrderRequest) -> OrderResult:
        """Place an order (limit or market)."""
        pass
    
    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order. Returns True if cancelled successfully."""
        pass
    
    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatus:
        """Get current status of an order."""
        pass

    @abstractmethod
    def get_balance(self) -> AccountBalance:
        """Get account balance."""
        pass

    @abstractmethod
    def get_min_order_size(self) -> float:
        """Get minimum order size."""
        pass


class DryRunClient(ExchangeClient):
    """
    Dry-run client that simulates orders without placing them.
    """

    def __init__(self, name: str = "DryRun", balance: float = 10000.0):
        self.name = name
        self._balance = balance
        self._orders: list[OrderRequest] = []
        self._order_counter = 0

    def place_order(self, order: OrderRequest) -> OrderResult:
        """Simulate order placement."""
        self._order_counter += 1
        order_id = f"DRY-{self.name}-{self._order_counter}"

        logger.info(
            f"[DRY-RUN] {self.name} order: "
            f"{order.side.value} {order.size:.4f} @ {order.price:.4f} "
            f"token={order.token_id[:16]}..."
        )

        self._orders.append(order)

        return OrderResult(
            success=True,
            order_id=order_id,
            filled_size=order.size,
            filled_price=order.price,
            is_simulated=True,
        )

    def cancel_order(self, order_id: str) -> bool:
        """Simulate order cancellation."""
        logger.info(f"[DRY-RUN] {self.name} cancel order: {order_id}")
        return True
    
    def get_order_status(self, order_id: str) -> OrderStatus:
        """Simulate order status - always filled for dry-run."""
        return OrderStatus(
            order_id=order_id,
            status="filled",
            filled_size=1.0,
            remaining_size=0.0,
        )

    def get_balance(self) -> AccountBalance:
        """Return simulated balance."""
        return AccountBalance(available=self._balance, total=self._balance)

    def get_min_order_size(self) -> float:
        """Return minimum order size."""
        return 1.0

    def get_orders(self) -> list[OrderRequest]:
        """Get all simulated orders."""
        return self._orders.copy()

    def clear_orders(self):
        """Clear order history."""
        self._orders.clear()


class PolymarketClient(ExchangeClient):
    """
    Polymarket CLOB trading client.

    Uses the py-clob-client SDK for order placement.
    """

    HOST = "https://clob.polymarket.com"
    CHAIN_ID = 137  # Polygon

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        wallet_address: Optional[str] = None,
        private_key: Optional[str] = None,
        signature_type: int = 0,
        funder_address: Optional[str] = None,
        proxy: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.wallet_address = wallet_address
        self.private_key = private_key
        self.signature_type = signature_type
        self.funder_address = funder_address or wallet_address
        self.proxy = proxy
        self.timeout = timeout
        self._client = None
        self._initialized = False

    def _init_client(self):
        """Initialize the py-clob-client."""
        if self._initialized:
            return True

        try:
            # Set proxy environment variables if configured
            if self.proxy:
                import os
                os.environ["HTTP_PROXY"] = self.proxy
                os.environ["HTTPS_PROXY"] = self.proxy
                logger.info(f"Polymarket proxy configured: {self.proxy[:30]}...")

            from py_clob_client.client import ClobClient

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
                    if self.api_key and self.api_secret and self.passphrase:
                        from py_clob_client.clob_types import ApiCreds
                        creds = ApiCreds(
                            api_key=self.api_key,
                            api_secret=self.api_secret,
                            api_passphrase=self.passphrase,
                        )
                        self._client.set_api_creds(creds)
            else:
                logger.error("Polymarket: private_key required for trading")
                return False

            self._initialized = True
            return True

        except ImportError as e:
            logger.error(f"py-clob-client not installed: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize Polymarket client: {e}")

    def place_order(self, order: OrderRequest) -> OrderResult:
        """Place order on Polymarket CLOB using py-clob-client SDK."""
        if not self._init_client():
            return OrderResult(success=False, error="Failed to initialize Polymarket client")

        # Rate limiting
        PM_RATE_LIMITER.acquire()

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType as PmOrderType
            from py_clob_client.order_builder.constants import BUY, SELL

            order_side = BUY if order.side == OrderSide.BUY else SELL

            order_args = OrderArgs(
                token_id=order.token_id,
                price=order.price,
                size=order.size,
                side=order_side,
            )

            # Create signed order
            signed_order = self._client.create_order(order_args)

            # Determine order type based on OrderRequest
            if order.order_type == OrderType.MARKET:
                # For market orders, use FOK (Fill or Kill) for immediate execution
                pm_order_type = PmOrderType.FOK
                logger.info(f"PM placing MARKET order (FOK): {order.side.value} {order.size} @ {order.price}")
            else:
                # For limit orders, use GTC (Good Til Cancelled)
                pm_order_type = PmOrderType.GTC
                logger.info(f"PM placing LIMIT order (GTC): {order.side.value} {order.size} @ {order.price}")
            
            response = self._client.post_order(signed_order, pm_order_type)

            if response and response.get("success"):
                return OrderResult(
                    success=True,
                    order_id=response.get("orderID") or response.get("id"),
                    filled_size=order.size,
                    filled_price=order.price,
                    raw_response=response,
                )
            else:
                error_msg = response.get("errorMsg", "Unknown error") if response else "No response"
                logger.error(f"Polymarket order failed: {error_msg}")
                return OrderResult(success=False, error=error_msg)

        except Exception as e:
            logger.error(f"Polymarket order exception: {e}")
            return OrderResult(success=False, error=str(e))

    def get_balance(self) -> AccountBalance:
        """Get account balance from Polymarket using web3."""
        # USDC.e on Polygon
        USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        # Multiple RPC endpoints for redundancy
        POLYGON_RPCS = [
            "https://polygon-rpc.com",
            "https://rpc-mainnet.matic.quiknode.pro",
            "https://polygon-mainnet.public.blastapi.io",
        ]
        
        # Rate limiting
        PM_RATE_LIMITER.acquire()
        
        try:
            from web3 import Web3
            from eth_account import Account
            
            # Get wallet address from private key
            if self.private_key:
                account = Account.from_key(self.private_key)
                wallet_address = account.address
            elif self.wallet_address:
                wallet_address = self.wallet_address
            else:
                logger.error("PM balance: no wallet address")
                return AccountBalance(available=0.0, total=0.0, currency="USDC")
            
            # Try multiple RPC endpoints for redundancy
            last_error = None
            for rpc_url in POLYGON_RPCS:
                try:
                    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
                    
                    # ERC20 ABI for balanceOf
                    erc20_abi = [{"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]
                    
                    usdc_contract = w3.eth.contract(
                        address=Web3.to_checksum_address(USDC_ADDRESS),
                        abi=erc20_abi
                    )
                    
                    balance_wei = usdc_contract.functions.balanceOf(
                        Web3.to_checksum_address(wallet_address)
                    ).call()
                    
                    # USDC.e has 6 decimals
                    available = balance_wei / 1e6
                    logger.info(f"PM balance: ${available:.2f}")
                    return AccountBalance(available=available, total=available, currency="USDC")
                except Exception as e:
                    last_error = e
                    logger.debug(f"RPC {rpc_url} failed: {e}")
                    continue
            
            # All RPCs failed
            logger.error(f"PM balance fetch failed on all RPCs: {last_error}")
            
        except Exception as e:
            logger.error(f"PM balance fetch failed: {e}")

        # CRITICAL: Return 0 balance on failure to prevent unhedged trades
        logger.warning("PM balance: unknown, returning 0 for safety")
        return AccountBalance(available=0.0, total=0.0, currency="USDC")

    def get_min_order_size(self) -> float:
        """Polymarket minimum order size in shares (dollar check happens separately)."""
        return 1.0  # 1 share minimum (dollar amount check enforces $1 min)

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order on Polymarket."""
        if not self._init_client():
            logger.error("PM cancel: client not initialized")
            return False
        
        # Rate limiting
        PM_RATE_LIMITER.acquire()
        
        try:
            response = self._client.cancel(order_id)
            if response and response.get("canceled"):
                logger.info(f"PM order cancelled: {order_id}")
                return True
            else:
                logger.warning(f"PM cancel response: {response}")
                return False
        except Exception as e:
            # Retry on transient errors
            if is_transient_error(e):
                logger.warning(f"PM cancel transient error, retrying: {e}")
                time.sleep(1)
                PM_RATE_LIMITER.acquire()
                try:
                    response = self._client.cancel(order_id)
                    if response and response.get("canceled"):
                        logger.info(f"PM order cancelled on retry: {order_id}")
                        return True
                except Exception as e2:
                    logger.error(f"PM cancel retry failed: {e2}")
            logger.error(f"PM cancel error: {e}")
            return False

    def get_order_status(self, order_id: str) -> OrderStatus:
        """Get order status from Polymarket."""
        if not self._init_client():
            return OrderStatus(order_id=order_id, status="unknown")
        
        # Rate limiting
        PM_RATE_LIMITER.acquire()
        
        try:
            order = self._client.get_order(order_id)
            if order:
                # Parse status from order response
                size_matched = float(order.get("size_matched", 0))
                original_size = float(order.get("original_size", 0))
                remaining = original_size - size_matched
                
                if order.get("status") == "MATCHED":
                    status = "filled"
                elif order.get("status") == "CANCELED":
                    status = "cancelled"
                elif size_matched > 0:
                    status = "partially_filled"
                else:
                    status = "open"
                
                return OrderStatus(
                    order_id=order_id,
                    status=status,
                    filled_size=size_matched,
                    remaining_size=remaining,
                    filled_price=float(order.get("price", 0)),
                    raw_response=order,
                )
        except Exception as e:
            logger.error(f"PM get_order_status error: {e}")
        
        return OrderStatus(order_id=order_id, status="unknown")

    def close(self):
        """Close the client."""
        self._client = None
        self._initialized = False


class OpinionClient(ExchangeClient):
    """
    Opinion exchange trading client.

    Uses EIP-712 signed orders (same approach as /opinion/modules/opinion.py).
    """

    API_BASE_URL = "https://proxy.opinion.trade:8443/api/bsc/api/v2"

    # EIP-712 Order structure
    ORDER_TYPED_DATA = {
        "primaryType": "Order",
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Order": [
                {"name": "salt", "type": "uint256"},
                {"name": "maker", "type": "address"},
                {"name": "signer", "type": "address"},
                {"name": "taker", "type": "address"},
                {"name": "tokenId", "type": "uint256"},
                {"name": "makerAmount", "type": "uint256"},
                {"name": "takerAmount", "type": "uint256"},
                {"name": "expiration", "type": "uint256"},
                {"name": "nonce", "type": "uint256"},
                {"name": "feeRateBps", "type": "uint256"},
                {"name": "side", "type": "uint8"},
                {"name": "signatureType", "type": "uint8"},
            ],
        },
        "domain": {
            "name": "OPINION CTF Exchange",
            "version": "1",
            "chainId": 56,
            "verifyingContract": "0x5f45344126d6488025b0b84a3a8189f2487a7246",
        },
        "message": {
            "taker": "0x0000000000000000000000000000000000000000",
            "expiration": "0",
            "nonce": "0",
            "feeRateBps": "0",
            "signatureType": "2",
        },
    }

    def __init__(
        self,
        api_key: str = "",
        private_key: Optional[str] = None,
        multi_sig_address: Optional[str] = None,
        proxy: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.api_key = api_key
        self.private_key = private_key
        self.multi_sig_address = (multi_sig_address or "").lower()
        self.proxy = proxy
        self.timeout = timeout
        self._session: Optional[requests.Session] = None
        self._account = None
        self._auth_token = None
        self._wallet_address = None

        if private_key:
            try:
                from eth_account import Account
                self._account = Account.from_key(private_key)
                self._wallet_address = self._account.address
            except Exception as e:
                logger.error(f"Failed to load Opinion account: {e}")

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(
                {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://app.opinion.trade",
                    "Referer": "https://app.opinion.trade/",
                    "x-device-kind": "web",
                }
            )
            # Configure proxy if set
            if self.proxy:
                self._session.proxies = {
                    "http": self.proxy,
                    "https": self.proxy,
                }
                logger.info(f"Opinion proxy configured: {self.proxy[:30]}...")
        return self._session

    def _login(self) -> bool:
        """Login to Opinion and get auth token."""
        if self._auth_token:
            return True

        if not self._account:
            logger.error("Opinion: private_key required for trading")
            return False

        try:
            from random import random
            from datetime import datetime, timezone
            from eth_account.messages import encode_defunct

            session = self._get_session()
            nonce = int(random() * 0xffffffffffff)
            date_now = datetime.now(timezone.utc)
            sign_message = f"""app.opinion.trade wants you to sign in with your Ethereum account:
{self._wallet_address}

Welcome to opinion.trade! By proceeding, you agree to our Privacy Policy and Terms of Use.

URI: https://app.opinion.trade
Version: 1
Chain ID: 56
Nonce: {nonce}
Issued At: {date_now.isoformat()[:-9] + 'Z'}"""

            message = encode_defunct(text=sign_message)
            signed = self._account.sign_message(message)
            signature = signed.signature.hex()

            url = f"{self.API_BASE_URL.replace('/v2', '/v1')}/user/token"
            payload = {
                "nonce": str(nonce),
                "timestamp": int(date_now.timestamp()),
                "siwe_message": sign_message,
                "sign": signature,
                "invite_code": "",
                "sources": "web",
                "sign_in_wallet_plugin": None,
            }

            response = session.post(url, json=payload, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                # Safely extract token - handle None result
                result = data.get("result")
                if result is not None and isinstance(result, dict):
                    token = result.get("token")
                    if token:
                        self._auth_token = token
                        session.headers["Authorization"] = f"Bearer {self._auth_token}"
                        logger.info("Opinion login successful")
                        
                        # Auto-fetch multi_sig_address if not set
                        if not self.multi_sig_address:
                            self._fetch_multi_sig_address()
                        
                        return True
                    else:
                        logger.error(f"Opinion login: no token in result: {result}")
                else:
                    # Log the actual response for debugging
                    logger.error(f"Opinion login: unexpected result format: {data}")
                return False

            logger.error(f"Opinion login failed: {response.status_code}, response: {response.text[:200]}")
            return False

        except Exception as e:
            logger.error(f"Opinion login error: {e}")
            return False

    def _fetch_multi_sig_address(self) -> None:
        """Fetch multi_sig_address from user profile."""
        try:
            session = self._get_session()
            url = f"{self.API_BASE_URL}/user/{self._wallet_address}/profile"
            response = session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                result = data.get("result", {})
                
                # Try to get multi_sig from profile
                multi_sig = result.get("multiSigAddress") or result.get("multi_sig_address")
                if multi_sig:
                    self.multi_sig_address = multi_sig.lower()
                    logger.info(f"Opinion multi_sig_address auto-detected: {self.multi_sig_address[:10]}...")
                else:
                    # Fallback to wallet address (works for most accounts)
                    self.multi_sig_address = self._wallet_address.lower()
                    logger.info(f"Opinion using wallet as multi_sig: {self.multi_sig_address[:10]}...")
        except Exception as e:
            logger.warning(f"Could not fetch multi_sig_address: {e}")

    def place_order(self, order: OrderRequest) -> OrderResult:
        """Place order on Opinion with EIP-712 signature."""
        if not self._login():
            return OrderResult(success=False, error="Opinion login failed")

        # Rate limiting
        OP_RATE_LIMITER.acquire()

        try:
            import copy
            from random import random
            from decimal import Decimal

            # Calculate amounts - MUST use Decimal to avoid floating point errors
            order_side = 0 if order.side == OrderSide.BUY else 1
            
            # Round price to 3 decimal places to match Opinion API requirements
            price_decimal = Decimal(str(round(order.price, 3)))
            size_decimal = Decimal(str(order.size))

            if order_side == 0:  # BUY
                maker_amount = size_decimal * price_decimal
                taker_amount = size_decimal
            else:  # SELL
                maker_amount = size_decimal
                taker_amount = size_decimal * price_decimal

            # Build typed data
            typed_data = copy.deepcopy(self.ORDER_TYPED_DATA)
            typed_data["message"].update({
                "salt": str(int(random() * int(time.time() * 1e3))),
                "maker": self.multi_sig_address,
                "signer": self._wallet_address,
                "tokenId": str(order.token_id),
                "makerAmount": str(int(maker_amount * Decimal("1e18"))),
                "takerAmount": str(int(taker_amount * Decimal("1e18"))),
                "side": str(order_side),
            })

            # Sign
            signed = self._account.sign_typed_data(full_message=typed_data)
            signature = signed.signature.hex()
            if not signature.startswith('0x'):
                signature = '0x' + signature

            # Submit order
            session = self._get_session()
            url = f"{self.API_BASE_URL}/order"

            # Build payload with topicId (must be int)
            topic_id_int = int(order.topic_id) if order.topic_id else 0
            
            payload = {
                **typed_data["message"],
                "topicId": topic_id_int,  # Required for Opinion (as int)
                "signature": signature,
                "sign": signature,
                "timestamp": int(time.time()),
                "safeRate": "0" if order.order_type == OrderType.MARKET else "0.05",
                "price": "0" if order.order_type == OrderType.MARKET else str(round(order.price, 3)),
                "tradingMethod": 1 if order.order_type == OrderType.MARKET else 2,  # 1=market, 2=limit
                "contractAddress": "",
                "orderExpTime": "0",
                "currencyAddress": "0x55d398326f99059fF775485246999027B3197955",
                "chainId": 56,
            }
            
            order_type_str = "MARKET" if order.order_type == OrderType.MARKET else "LIMIT"
            logger.info(f"OP placing {order_type_str} order: {order.side.value} {order.size} @ {order.price}")

            logger.info(f"Opinion order payload: topicId={topic_id_int}, price={order.price}, size={order.size}")
            response = session.post(url, json=payload, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                if data.get("result", {}).get("orderData"):
                    order_data = data["result"]["orderData"]
                    return OrderResult(
                        success=True,
                        order_id=order_data.get("orderId"),
                        filled_size=order.size,
                        filled_price=order.price,
                        raw_response=data,
                    )
                else:
                    error_msg = data.get("errmsg", "Unknown error")
                    logger.error(f"Opinion order failed: {error_msg}")
                    return OrderResult(success=False, error=error_msg)
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.error(f"Opinion order failed: {error_msg}")
                return OrderResult(success=False, error=error_msg)

        except Exception as e:
            logger.error(f"Opinion order exception: {e}")
            return OrderResult(success=False, error=str(e))

    def get_balance(self) -> AccountBalance:
        """Get account balance from Opinion."""
        # Try to get balance even without login first (some endpoints are public)
        balance = self._fetch_balance_direct()
        if balance is not None:
            return balance
        
        # If direct fetch failed, try with login
        if not self._login():
            logger.error("OP balance: login failed, returning 0")
            return AccountBalance(available=0.0, total=0.0, currency="USDT")

        # Try again after login
        balance = self._fetch_balance_direct()
        if balance is not None:
            return balance

        # CRITICAL: Return 0 balance on failure to prevent unhedged trades
        logger.warning("OP balance: unknown, returning 0 for safety")
        return AccountBalance(available=0.0, total=0.0, currency="USDT")

    def _fetch_balance_direct(self) -> Optional[AccountBalance]:
        """Fetch balance directly from profile API."""
        if not self._wallet_address:
            return None
        
        # Rate limiting
        OP_RATE_LIMITER.acquire()
            
        try:
            session = self._get_session()
            url = f"{self.API_BASE_URL}/user/{self._wallet_address}/profile"

            response = session.get(url, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                result = data.get("result")
                
                if result is None:
                    logger.warning(f"OP balance: result is None, response: {data}")
                    return None
                
                # Handle different response formats
                balance_list = result.get("balance", [])
                
                if isinstance(balance_list, list) and len(balance_list) > 0:
                    balance_data = balance_list[0]
                    if isinstance(balance_data, dict):
                        available = float(balance_data.get("balance", 0))
                    else:
                        available = float(balance_data) if balance_data else 0.0
                elif isinstance(balance_list, (int, float)):
                    available = float(balance_list)
                else:
                    # Try alternative field names
                    available = float(result.get("availableBalance", 0) or 
                                     result.get("available_balance", 0) or
                                     result.get("usdt_balance", 0) or 0)
                
                logger.info(f"OP balance: ${available:.2f}")
                return AccountBalance(available=available, total=available, currency="USDT")
            else:
                logger.error(f"OP balance fetch failed: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"OP balance error: {e}")
            return None

    def get_min_order_size(self) -> float:
        """Opinion minimum order size in shares (dollar check happens separately)."""
        # Opinion requires minimum $5 order according to official docs
        return 1.0  # 1 share minimum (dollar amount of $5 enforced in runner)

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order on Opinion."""
        if not self._login():
            logger.error("OP cancel: login failed")
            return False
        
        # Rate limiting
        OP_RATE_LIMITER.acquire()
        
        try:
            session = self._get_session()
            url = f"{self.API_BASE_URL.replace('/v2', '/v1')}/order/cancel/order"
            
            response = session.post(
                url,
                json={"trans_no": order_id, "chainId": 56},
                timeout=self.timeout,
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("errno") == 0 and data.get("result", {}).get("result"):
                    logger.info(f"OP order cancelled: {order_id}")
                    return True
                else:
                    logger.warning(f"OP cancel response: {data}")
                    return False
        except Exception as e:
            # Retry on transient errors
            if is_transient_error(e):
                logger.warning(f"OP cancel transient error, retrying: {e}")
                time.sleep(1)
                OP_RATE_LIMITER.acquire()
                try:
                    response = session.post(
                        url,
                        json={"trans_no": order_id, "chainId": 56},
                        timeout=self.timeout,
                    )
                    if response.status_code == 200:
                        data = response.json()
                        if data.get("errno") == 0 and data.get("result", {}).get("result"):
                            logger.info(f"OP order cancelled on retry: {order_id}")
                            return True
                except Exception as e2:
                    logger.error(f"OP cancel retry failed: {e2}")
            logger.error(f"OP cancel error: {e}")
        
        return False

    def get_order_status(self, order_id: str) -> OrderStatus:
        """Get order status from Opinion."""
        if not self._login():
            return OrderStatus(order_id=order_id, status="unknown")
        
        # Rate limiting
        OP_RATE_LIMITER.acquire()
        
        try:
            session = self._get_session()
            url = f"{self.API_BASE_URL}/order"
            params = {
                "page": 1,
                "limit": 100,
                "walletAddress": self._wallet_address,
                "queryType": 1,  # Open orders
            }
            
            response = session.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                orders = data.get("result", {}).get("list", []) or []
                
                for order in orders:
                    if order.get("transNo") == order_id or str(order.get("orderId")) == order_id:
                        # Parse status
                        filled = order.get("filled", "0/0")
                        parts = filled.split("/")
                        filled_amount = float(parts[0]) if parts[0] else 0
                        total_amount = float(parts[1]) if len(parts) > 1 and parts[1] else 0
                        
                        order_status = order.get("status", 0)
                        if order_status == 2:  # Filled
                            status = "filled"
                        elif order_status == 6:  # Cancelled
                            status = "cancelled"
                        elif filled_amount > 0:
                            status = "partially_filled"
                        else:
                            status = "open"
                        
                        return OrderStatus(
                            order_id=order_id,
                            status=status,
                            filled_size=filled_amount,
                            remaining_size=total_amount - filled_amount,
                            filled_price=float(order.get("price", 0)),
                            raw_response=order,
                        )
                
                # Order not in open orders - might be filled or cancelled
                return OrderStatus(order_id=order_id, status="filled")
                
        except Exception as e:
            logger.error(f"OP get_order_status error: {e}")
        
        return OrderStatus(order_id=order_id, status="unknown")

    def close(self):
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None


@dataclass
class ExchangeClients:
    """Container for exchange clients."""

    pm_client: ExchangeClient
    op_client: ExchangeClient
    is_dry_run: bool = False

    def close(self):
        """Close all clients."""
        if hasattr(self.pm_client, "close"):
            self.pm_client.close()
        if hasattr(self.op_client, "close"):
            self.op_client.close()


def create_clients(
    dry_run: bool = True,
    pm_api_key: str = "",
    pm_api_secret: str = "",
    pm_passphrase: str = "",
    pm_wallet: str = "",
    pm_private_key: str = "",
    pm_signature_type: int = 0,
    pm_funder: str = "",
    pm_proxy: str = "",
    op_api_key: str = "",
    op_private_key: str = "",
    op_multi_sig: str = "",
    op_proxy: str = "",
    pm_balance: float = 10000.0,
    op_balance: float = 10000.0,
) -> ExchangeClients:
    """
    Create exchange clients.

    Args:
        dry_run: If True, create dry-run clients
        pm_*: Polymarket credentials
        op_*: Opinion credentials
        pm_balance: Simulated PM balance (dry-run only)
        op_balance: Simulated OP balance (dry-run only)

    Returns:
        ExchangeClients container
    """
    if dry_run:
        return ExchangeClients(
            pm_client=DryRunClient(name="PM", balance=pm_balance),
            op_client=DryRunClient(name="OP", balance=op_balance),
            is_dry_run=True,
        )
    else:
        return ExchangeClients(
            pm_client=PolymarketClient(
                api_key=pm_api_key,
                api_secret=pm_api_secret,
                passphrase=pm_passphrase,
                wallet_address=pm_wallet,
                private_key=pm_private_key,
                signature_type=pm_signature_type,
                funder_address=pm_funder,
                proxy=pm_proxy,
            ),
            op_client=OpinionClient(
                api_key=op_api_key,
                private_key=op_private_key,
                multi_sig_address=op_multi_sig,
                proxy=op_proxy,
            ),
            is_dry_run=False,
        )
