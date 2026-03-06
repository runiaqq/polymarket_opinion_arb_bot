"""
Orderbook fetching for Polymarket and Opinion exchanges.
"""

import time
from dataclasses import dataclass, field
from typing import Optional

import requests

from ..core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class OrderbookLevel:
    """Single level in orderbook."""

    price: float
    size: float


@dataclass
class Orderbook:
    """Orderbook with bids and asks."""

    token_id: str
    bids: list[OrderbookLevel] = field(default_factory=list)
    asks: list[OrderbookLevel] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
    error: Optional[str] = None

    @property
    def best_bid(self) -> Optional[OrderbookLevel]:
        """Get best bid (highest price)."""
        if not self.bids:
            return None
        return max(self.bids, key=lambda x: x.price)

    @property
    def best_ask(self) -> Optional[OrderbookLevel]:
        """Get best ask (lowest price)."""
        if not self.asks:
            return None
        return min(self.asks, key=lambda x: x.price)

    @property
    def best_ask_price(self) -> float:
        """Get best ask price or 0."""
        ask = self.best_ask
        return ask.price if ask else 0.0

    @property
    def best_ask_size(self) -> float:
        """Get best ask size (depth) or 0."""
        ask = self.best_ask
        return ask.size if ask else 0.0

    def get_aggregated_ask_depth(self, max_slippage_pct: float = 0.05) -> tuple[float, float]:
        """
        Get aggregated depth across multiple ask levels within slippage tolerance.
        
        Args:
            max_slippage_pct: Maximum price slippage from best ask (e.g., 0.05 = 5%)
        
        Returns:
            Tuple of (total_size, average_price)
        """
        if not self.asks:
            return 0.0, 0.0
        
        best_ask = self.best_ask
        if not best_ask:
            return 0.0, 0.0
        
        max_price = best_ask.price * (1 + max_slippage_pct)
        
        total_size = 0.0
        total_value = 0.0
        
        # Sort asks by price (ascending)
        sorted_asks = sorted(self.asks, key=lambda x: x.price)
        
        for ask in sorted_asks:
            if ask.price <= max_price:
                total_size += ask.size
                total_value += ask.size * ask.price
            else:
                break  # Stop when we exceed max slippage
        
        avg_price = total_value / total_size if total_size > 0 else 0.0
        return total_size, avg_price


class PolymarketOrderbook:
    """
    Polymarket CLOB orderbook client.

    Uses the CLOB API to fetch orderbooks.
    """

    CLOB_BASE_URL = "https://clob.polymarket.com"

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self._session: Optional[requests.Session] = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(
                {
                    "Accept": "application/json",
                    "User-Agent": "arb-core/1.0",
                }
            )
        return self._session

    def fetch(self, token_id: str) -> Orderbook:
        """
        Fetch orderbook for a token.

        Args:
            token_id: The CLOB token ID

        Returns:
            Orderbook with bids and asks
        """
        try:
            session = self._get_session()
            url = f"{self.CLOB_BASE_URL}/book"
            params = {"token_id": token_id}

            response = session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()

            # Parse bids and asks
            bids = []
            for bid in data.get("bids", []):
                try:
                    price = float(bid.get("price", 0))
                    size = float(bid.get("size", 0))
                    if price > 0 and size > 0:
                        bids.append(OrderbookLevel(price=price, size=size))
                except (ValueError, TypeError):
                    continue

            asks = []
            for ask in data.get("asks", []):
                try:
                    price = float(ask.get("price", 0))
                    size = float(ask.get("size", 0))
                    if price > 0 and size > 0:
                        asks.append(OrderbookLevel(price=price, size=size))
                except (ValueError, TypeError):
                    continue

            return Orderbook(token_id=token_id, bids=bids, asks=asks)

        except requests.exceptions.RequestException as e:
            # 404 is expected for delisted/resolved markets - log as debug, not error
            if "404" in str(e):
                logger.debug(f"Polymarket orderbook not found (market may be resolved): {token_id}")
            else:
                logger.warning(f"Polymarket orderbook fetch failed for {token_id}: {e}")
            return Orderbook(token_id=token_id, error=str(e))
        except Exception as e:
            logger.error(f"Polymarket orderbook parse error for {token_id}: {e}")
            return Orderbook(token_id=token_id, error=str(e))

    def close(self):
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None


class OpinionOrderbook:
    """
    Opinion exchange orderbook client.

    Uses the Opinion API to fetch orderbooks via /order/market/depth endpoint.
    Requires question_id and symbol (token_id) for fetching.
    """

    API_BASE_URL = "https://proxy.opinion.trade:8443/api/bsc/api/v2"

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self._session: Optional[requests.Session] = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(
                {
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://app.opinion.trade",
                    "Referer": "https://app.opinion.trade/",
                }
            )
        return self._session

    def fetch(
        self,
        token_id: str,
        question_id: Optional[str] = None,
        symbol_type: int = 0,
    ) -> Orderbook:
        """
        Fetch orderbook for a token (topic outcome).

        Args:
            token_id: The Opinion token ID (yesPos or noPos)
            question_id: The questionId from the topic (required for depth endpoint)
            symbol_type: 0 for YES, 1 for NO

        Returns:
            Orderbook with bids and asks
        """
        try:
            session = self._get_session()

            # Use the correct depth endpoint
            url = f"{self.API_BASE_URL}/order/market/depth"
            params = {
                "symbol_types": str(symbol_type),
                "question_id": question_id or "",
                "symbol": token_id,
                "chainId": "56",
            }

            response = session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()

            if data.get("errno") or data.get("errmsg"):
                error_msg = data.get("errmsg", "Unknown error")
                logger.error(f"Opinion API error for {token_id}: {error_msg}")
                return Orderbook(token_id=token_id, error=error_msg)

            # Parse result - format is [[price, size], ...]
            result = data.get("result", {})
            bids = []
            asks = []

            for bid_data in result.get("bids", []):
                try:
                    if isinstance(bid_data, (list, tuple)) and len(bid_data) >= 2:
                        price = float(bid_data[0])
                        size = float(bid_data[1])
                    else:
                        price = float(bid_data.get("price", bid_data.get("p", 0)))
                        size = float(bid_data.get("size", bid_data.get("amount", 0)))
                    if price > 0 and size > 0:
                        bids.append(OrderbookLevel(price=price, size=size))
                except (ValueError, TypeError, IndexError):
                    continue

            for ask_data in result.get("asks", []):
                try:
                    if isinstance(ask_data, (list, tuple)) and len(ask_data) >= 2:
                        price = float(ask_data[0])
                        size = float(ask_data[1])
                    else:
                        price = float(ask_data.get("price", ask_data.get("p", 0)))
                        size = float(ask_data.get("size", ask_data.get("amount", 0)))
                    if price > 0 and size > 0:
                        asks.append(OrderbookLevel(price=price, size=size))
                except (ValueError, TypeError, IndexError):
                    continue

            return Orderbook(token_id=token_id, bids=bids, asks=asks)

        except requests.exceptions.RequestException as e:
            logger.error(f"Opinion orderbook fetch failed for {token_id}: {e}")
            return Orderbook(token_id=token_id, error=str(e))
        except Exception as e:
            logger.error(f"Opinion orderbook parse error for {token_id}: {e}")
            return Orderbook(token_id=token_id, error=str(e))

    def fetch_by_topic(self, topic_id: str, side: str = "YES") -> Orderbook:
        """
        Fetch orderbook by topic ID - first gets topic metadata, then orderbook.

        Args:
            topic_id: The Opinion topic ID
            side: "YES" or "NO"

        Returns:
            Orderbook with bids and asks
        """
        try:
            session = self._get_session()

            # First, get topic metadata
            topic_url = f"{self.API_BASE_URL}/topic/{topic_id}"
            response = session.get(topic_url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            if data.get("errno") or data.get("errmsg"):
                error_msg = data.get("errmsg", "Topic not found")
                return Orderbook(token_id=topic_id, error=error_msg)

            topic_data = data.get("result", {}).get("data", {})
            question_id = topic_data.get("questionId", "")

            if side.upper() == "YES":
                token_id = topic_data.get("yesPos", "")
                symbol_type = 0
            else:
                token_id = topic_data.get("noPos", "")
                symbol_type = 1

            if not token_id or not question_id:
                return Orderbook(
                    token_id=topic_id,
                    error=f"Missing token or question ID for topic {topic_id}"
                )

            # Now fetch the orderbook
            return self.fetch(token_id, question_id, symbol_type)

        except Exception as e:
            logger.error(f"Opinion topic orderbook fetch failed for {topic_id}: {e}")
            return Orderbook(token_id=topic_id, error=str(e))

    def close(self):
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None


@dataclass
class PairOrderbooks:
    """Orderbooks for a pair (PM and OP)."""

    pm_orderbook: Orderbook
    op_orderbook: Orderbook

    @property
    def is_valid(self) -> bool:
        """Check if both orderbooks are valid."""
        return (
            self.pm_orderbook.error is None
            and self.op_orderbook.error is None
            and self.pm_orderbook.best_ask is not None
            and self.op_orderbook.best_ask is not None
        )

    @property
    def error(self) -> Optional[str]:
        """Get error if any."""
        if self.pm_orderbook.error:
            return f"PM: {self.pm_orderbook.error}"
        if self.op_orderbook.error:
            return f"OP: {self.op_orderbook.error}"
        if self.pm_orderbook.best_ask is None:
            return "PM: no asks"
        if self.op_orderbook.best_ask is None:
            return "OP: no asks"
        return None


class OrderbookManager:
    """
    Manages orderbook fetching for multiple exchanges.
    """

    def __init__(self, timeout: float = 10.0):
        self.pm_client = PolymarketOrderbook(timeout=timeout)
        self.op_client = OpinionOrderbook(timeout=timeout)

    def fetch_pair(
        self,
        pm_token: str,
        op_token: str,
        op_question_id: Optional[str] = None,
        op_side: str = "YES",
    ) -> PairOrderbooks:
        """
        Fetch orderbooks for a pair.

        Args:
            pm_token: Polymarket token ID
            op_token: Opinion token ID (yesPos or noPos)
            op_question_id: Opinion questionId for depth endpoint
            op_side: "YES" or "NO" - determines symbol_type (0 or 1)

        Returns:
            PairOrderbooks with both orderbooks
        """
        pm_ob = self.pm_client.fetch(pm_token)

        # Determine symbol_type based on side
        symbol_type = 0 if op_side == "YES" else 1

        op_ob = self.op_client.fetch(
            token_id=op_token,
            question_id=op_question_id,
            symbol_type=symbol_type,
        )
        return PairOrderbooks(pm_orderbook=pm_ob, op_orderbook=op_ob)

    def close(self):
        """Close all clients."""
        self.pm_client.close()
        self.op_client.close()
