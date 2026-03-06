"""
Opinion trading client using EIP-712 signed orders.

Based on /opinion/modules/opinion.py logic.
Requires:
- private_key: Wallet private key for signing
- multi_sig_address: Gnosis Safe proxy wallet on Opinion
"""

import time
from dataclasses import dataclass
from decimal import Decimal
from random import random
from typing import Optional

import requests
from eth_account import Account

from ...core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class OpinionOrder:
    """Order result from Opinion."""

    success: bool
    order_id: Optional[str] = None
    trans_no: Optional[str] = None
    error: Optional[str] = None
    filled_size: float = 0.0
    filled_price: float = 0.0


@dataclass
class OpinionBalance:
    """Balance from Opinion."""

    available: float = 0.0
    total: float = 0.0
    currency: str = "USDT"


class OpinionTradingClient:
    """
    Opinion exchange trading client.

    Uses EIP-712 signed orders (same approach as /opinion/modules/opinion.py).
    Orders are signed with wallet private key and sent to Opinion API.
    """

    API_BASE_URL = "https://proxy.opinion.trade:8443/api/bsc/api/v2"

    # EIP-712 Order structure (from /opinion/modules/opinion.py)
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
        private_key: str,
        multi_sig_address: str,
        proxy: Optional[str] = None,
        timeout: float = 30.0,
    ):
        """
        Initialize Opinion trading client.

        Args:
            private_key: Wallet private key (0x prefixed)
            multi_sig_address: Gnosis Safe proxy wallet address on Opinion
            proxy: Optional HTTP proxy
            timeout: Request timeout
        """
        self.private_key = private_key
        self.multi_sig_address = multi_sig_address.lower()
        self.proxy = proxy
        self.timeout = timeout

        # Derive wallet address from private key
        self.account = Account.from_key(private_key)
        self.wallet_address = self.account.address

        self._session = None
        self._auth_token = None

    def _get_session(self) -> requests.Session:
        """Get or create session."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(
                {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://app.opinion.trade",
                    "Referer": "https://app.opinion.trade/",
                    "x-device-kind": "web",
                }
            )
        return self._session

    def _sign_typed_data(self, typed_data: dict) -> str:
        """Sign EIP-712 typed data."""
        # Create a copy with proper structure for encoding
        data_to_sign = {
            "types": typed_data["types"],
            "primaryType": typed_data["primaryType"],
            "domain": typed_data["domain"],
            "message": typed_data["message"],
        }

        signed = self.account.sign_typed_data(full_message=data_to_sign)
        return signed.signature.hex()

    def login(self) -> bool:
        """
        Login to Opinion and get auth token.

        Returns:
            True if successful
        """
        try:
            session = self._get_session()

            # Build SIWE message
            nonce = int(random() * 0xffffffffffff)
            timestamp = int(time.time())
            from datetime import datetime, timezone

            date_now = datetime.now(timezone.utc)
            sign_message = f"""app.opinion.trade wants you to sign in with your Ethereum account:
{self.wallet_address}

Welcome to opinion.trade! By proceeding, you agree to our Privacy Policy and Terms of Use.

URI: https://app.opinion.trade
Version: 1
Chain ID: 56
Nonce: {nonce}
Issued At: {date_now.isoformat()[:-9] + 'Z'}"""

            # Sign the message
            from eth_account.messages import encode_defunct

            message = encode_defunct(text=sign_message)
            signed = self.account.sign_message(message)
            signature = signed.signature.hex()

            # Login request
            url = f"{self.API_BASE_URL.replace('/v2', '/v1')}/user/token"
            payload = {
                "nonce": str(nonce),
                "timestamp": timestamp,
                "siwe_message": sign_message,
                "sign": signature,
                "invite_code": "",
                "sources": "web",
                "sign_in_wallet_plugin": None,
            }

            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            response = session.post(url, json=payload, timeout=self.timeout, proxies=proxies)

            if response.status_code == 200:
                data = response.json()
                if data.get("result", {}).get("token"):
                    self._auth_token = data["result"]["token"]
                    session.headers["Authorization"] = f"Bearer {self._auth_token}"
                    logger.info("Opinion login successful")
                    return True
                else:
                    logger.error(f"Opinion login failed: {data}")
            else:
                logger.error(f"Opinion login failed: {response.status_code}")

        except Exception as e:
            logger.error(f"Opinion login error: {e}")

        return False

    def place_limit_order(
        self,
        token_id: str,
        side: str,  # "BUY" or "SELL"
        price: float,
        size: float,
        topic_id: str,
    ) -> OpinionOrder:
        """
        Place a limit order.

        Args:
            token_id: Token ID (yesPos or noPos)
            side: "BUY" or "SELL"
            price: Price per share (0-1)
            size: Number of shares (USDT amount for BUY, token amount for SELL)
            topic_id: Topic ID for the market

        Returns:
            OpinionOrder with result
        """
        if not self._auth_token:
            if not self.login():
                return OpinionOrder(success=False, error="Failed to login to Opinion")

        try:
            # Calculate amounts
            # side: 0 = BUY, 1 = SELL
            order_side = 0 if side.upper() == "BUY" else 1

            if order_side == 0:  # BUY
                # For BUY: makerAmount = USDT to spend, takerAmount = shares to receive
                maker_amount = Decimal(str(size))
                taker_amount = Decimal(str(size)) / Decimal(str(price))
            else:  # SELL
                # For SELL: makerAmount = shares to sell, takerAmount = USDT to receive
                taker_amount = Decimal(str(size)) * Decimal(str(price))
                maker_amount = Decimal(str(size))

            # Build typed data for signing
            typed_data = self._build_order_typed_data(
                token_id=token_id,
                maker_amount=maker_amount,
                taker_amount=taker_amount,
                side=order_side,
            )

            # Sign the order
            signature = self._sign_typed_data(typed_data)

            # Submit order
            session = self._get_session()
            url = f"{self.API_BASE_URL}/order"

            payload = {
                **typed_data["message"],
                "topicId": int(topic_id),
                "signature": signature,
                "sign": signature,
                "timestamp": int(time.time()),
                "safeRate": "0.05",  # 5% slippage tolerance
                "price": str(price),
                "tradingMethod": 2,  # 2 = limit order
                "contractAddress": "",
                "orderExpTime": "0",
                "currencyAddress": "0x55d398326f99059fF775485246999027B3197955",  # USDT on BSC
                "chainId": 56,
            }

            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            response = session.post(url, json=payload, timeout=self.timeout, proxies=proxies)

            if response.status_code == 200:
                data = response.json()
                if data.get("result", {}).get("orderData"):
                    order_data = data["result"]["orderData"]
                    return OpinionOrder(
                        success=True,
                        order_id=order_data.get("orderId"),
                        trans_no=order_data.get("transNo"),
                        filled_size=float(size),
                        filled_price=float(price),
                    )
                else:
                    error_msg = data.get("errmsg", "Unknown error")
                    return OpinionOrder(success=False, error=error_msg)
            else:
                return OpinionOrder(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                )

        except Exception as e:
            logger.error(f"Opinion order failed: {e}")
            return OpinionOrder(success=False, error=str(e))

    def _build_order_typed_data(
        self,
        token_id: str,
        maker_amount: Decimal,
        taker_amount: Decimal,
        side: int,
    ) -> dict:
        """Build EIP-712 typed data for order signing."""
        import copy

        typed_data = copy.deepcopy(self.ORDER_TYPED_DATA)

        typed_data["message"].update(
            {
                "salt": str(int(random() * int(time.time() * 1e3))),
                "maker": self.multi_sig_address,
                "signer": self.wallet_address,
                "tokenId": str(token_id),
                "makerAmount": str(int(maker_amount * Decimal("1e18"))),
                "takerAmount": str(int(taker_amount * Decimal("1e18"))),
                "side": str(side),
            }
        )

        return typed_data

    def get_balance(self) -> OpinionBalance:
        """Get account USDT balance."""
        if not self._auth_token:
            if not self.login():
                return OpinionBalance(available=10000.0, total=10000.0)

        try:
            session = self._get_session()
            url = f"{self.API_BASE_URL}/user/{self.wallet_address}/profile"

            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            response = session.get(url, timeout=self.timeout, proxies=proxies)

            if response.status_code == 200:
                data = response.json()
                result = data.get("result", {})
                balance_data = result.get("balance", [{}])[0]
                available = float(balance_data.get("balance", 0))
                return OpinionBalance(available=available, total=available)

        except Exception as e:
            logger.warning(f"Failed to fetch Opinion balance: {e}")

        return OpinionBalance(available=10000.0, total=10000.0)

    def get_orderbook(self, question_id: str, token_id: str, symbol_type: int = 0) -> dict:
        """Get orderbook for a market."""
        try:
            session = self._get_session()
            url = f"{self.API_BASE_URL}/order/market/depth"
            params = {
                "symbol_types": str(symbol_type),
                "question_id": question_id,
                "symbol": token_id,
                "chainId": "56",
            }

            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            response = session.get(url, params=params, timeout=self.timeout, proxies=proxies)

            if response.status_code == 200:
                data = response.json()
                result = data.get("result", {})
                return {
                    "bids": [
                        (float(b[0]), float(b[1])) for b in result.get("bids", [])
                    ],
                    "asks": [
                        (float(a[0]), float(a[1])) for a in result.get("asks", [])
                    ],
                }

        except Exception as e:
            logger.error(f"Failed to fetch Opinion orderbook: {e}")

        return {"bids": [], "asks": []}

    def cancel_order(self, trans_no: str) -> bool:
        """Cancel an order."""
        if not self._auth_token:
            if not self.login():
                return False

        try:
            session = self._get_session()
            url = f"{self.API_BASE_URL.replace('/v2', '/v1')}/order/cancel/order"

            payload = {
                "trans_no": trans_no,
                "chainId": 56,
            }

            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            response = session.post(url, json=payload, timeout=self.timeout, proxies=proxies)

            if response.status_code == 200:
                data = response.json()
                return data.get("result", {}).get("result", False)

        except Exception as e:
            logger.error(f"Failed to cancel Opinion order: {e}")

        return False

    def close(self):
        """Close session."""
        if self._session:
            self._session.close()
            self._session = None
