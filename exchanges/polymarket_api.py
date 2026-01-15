from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from eth_utils import to_checksum_address

from core.exceptions import FatalExchangeError, RecoverableExchangeError
from core.models import (
    ExchangeName,
    Fill,
    Market,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
)
from core.polymarket_clob import PolymarketClobClient, PolymarketOrderbookUnavailable
from exchanges.base_client import BaseExchangeClient
from exchanges.orderbook_manager import OrderbookManager
from exchanges.rate_limiter import RateLimiter
from utils.logger import BotLogger
from utils.timeparse import parse_polymarket_timestamp


class PolymarketAuthError(FatalExchangeError):
    """Raised when Polymarket returns an auth/permission failure."""

    def __init__(self, *, status: int, message: str, payload: Any | None = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class PolymarketInvalidOrderPayload(FatalExchangeError):
    """Raised when Polymarket rejects an order payload (400)."""

    def __init__(self, message: str, payload: Any | None = None):
        super().__init__(message)
        self.payload = payload


def build_polymarket_headers(
    *,
    api_key: str,
    secret_key_b64: str,
    passphrase: str,
    wallet_address: str,
    method: str,
    path: str,
    body: str = "",
    params: Any | None = None,
    include_params_in_path: bool = False,
    timestamp: str | None = None,
    timestamp_in_ms: bool = False,
    signature_encoding: str = "base64url",
    header_style: str = "underscores",
    logger: BotLogger | None = None,
) -> Dict[str, str]:
    """
    Build Polymarket authentication headers.

    Signature spec: base64(HMAC_SHA256(secret_bytes, f"{ts}{method}{path}{body}"))
    where:
      - ts is seconds (default) or milliseconds when timestamp_in_ms=True
      - path includes query string starting with "/"
      - body is the serialized JSON string ("" for GET)
    """
    from urllib.parse import urlencode

    ts = (
        timestamp
        if timestamp is not None
        else (str(int(time.time() * 1000)) if timestamp_in_ms else str(int(time.time())))
    )
    request_path = path if path.startswith("/") else f"/{path}"
    if include_params_in_path and params:
        request_path = f"{request_path}?{urlencode(params, doseq=True)}"
    body_str = body or ""
    message = f"{ts}{method.upper()}{request_path}{body_str}"
    padded_secret = secret_key_b64 + "=" * (-len(secret_key_b64) % 4)
    try:
        decoded_secret = base64.urlsafe_b64decode(padded_secret)
    except Exception:
        try:
            decoded_secret = base64.b64decode(padded_secret)
        except Exception as exc:
            raise ValueError("Invalid Polymarket API secret; expected base64 string") from exc
    digest = hmac.new(decoded_secret, message.encode("utf-8"), hashlib.sha256).digest()
    if signature_encoding == "hex":
        signature = digest.hex()
    elif signature_encoding == "base64url":
        signature = base64.urlsafe_b64encode(digest).decode("utf-8")
    else:
        signature = base64.b64encode(digest).decode("utf-8")

    if logger:
        api_hint = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) >= 8 else "short"
        logger.debug(
            "polymarket auth headers built",
            method=method.upper(),
            path=request_path,
            timestamp=ts,
            timestamp_len=len(ts),
            timestamp_in_ms=timestamp_in_ms,
            body_len=len(body_str),
            api_key_hint=api_hint,
            signature_len=len(signature),
            header_style=header_style,
        )

    headers = {
        "POLY_API_KEY": api_key,
        "POLY_PASSPHRASE": passphrase,
        "POLY_TIMESTAMP": ts,
        "POLY_SIGNATURE": signature,
        "POLY_ADDRESS": wallet_address,
    }
    if header_style == "hyphen":
        headers = {
            "POLY-API-KEY": api_key,
            "POLY-PASSPHRASE": passphrase,
            "POLY-TIMESTAMP": ts,
            "POLY-SIGNATURE": signature,
            "POLY-ADDRESS": wallet_address,
        }
    elif header_style == "both":
        headers.update(
            {
                "POLY-API-KEY": api_key,
                "POLY-PASSPHRASE": passphrase,
                "POLY-TIMESTAMP": ts,
                "POLY-SIGNATURE": signature,
                "POLY-ADDRESS": wallet_address,
            }
        )
    return headers


class PolymarketAPI(BaseExchangeClient):
    """Async interface for interacting with Polymarket endpoints."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        secret: str,
        passphrase: str | None,
        wallet_address: str | None,
        rate_limit: RateLimiter,
        logger: BotLogger | None = None,
        proxy: str | None = None,
        signature_type: int | None = None,
        funder_address: str | None = None,
        builder_api_key: str | None = None,
        builder_secret: str | None = None,
        builder_passphrase: str | None = None,
        rest_url: str = "https://clob.polymarket.com",
        data_url: str = "https://gamma-api.polymarket.com",
        trades_url: str = "https://data-api.polymarket.com",
    ):
        super().__init__(rest_url, session, api_key, secret, rate_limit, logger, proxy)
        if not passphrase:
            raise ValueError("Polymarket passphrase is required")
        if not wallet_address:
            raise ValueError("Polymarket wallet_address is required")
        self.passphrase = passphrase
        try:
            self.wallet_address = to_checksum_address(wallet_address)
        except ValueError:
            # Allow non-checksummed or placeholder addresses in dry-run/test contexts.
            self.wallet_address = wallet_address
            self.logger.warn("proceeding with non-checksummed wallet address", wallet_address=wallet_address)
        self.data_url = data_url.rstrip("/")
        self.trades_url = trades_url.rstrip("/")
        self.orderbooks = OrderbookManager()
        # Polymarket user auth is based on API creds tied to the EOA (wallet_address).
        # signature_type determines whether the proxy wallet should be used as funder.
        self.signature_type = signature_type if signature_type is not None else 1
        self.funder_address = funder_address
        self._profile_checked: bool = False
        self.builder_api_key = builder_api_key
        self.builder_secret = builder_secret
        self.builder_passphrase = builder_passphrase
        self._clob = PolymarketClobClient(
            session=session,
            base_url=rest_url,
            logger=self.logger,
            proxy=proxy,
            rate_limiter=rate_limit,
        )
        self.last_orderbook_source: str | None = None
        self._last_fill_cursor: str | None = None

    async def fetch_markets(self) -> List[Market]:
        return await self.get_markets()

    async def fetch_market(self, market_id: str) -> Market:
        return await self.get_market(market_id)

    async def get_markets(self) -> List[Market]:
        markets: List[Market] = []
        offset = 0
        limit = 200
        while True:
            params = {"offset": offset, "limit": limit, "closed": "false", "archived": "false"}
            data = await self._request_data("GET", "/markets", params=params)
            try:
                records = self._coerce_markets_payload(data)
            except Exception as exc:
                self.logger.warn("polymarket markets payload invalid", error=str(exc))
                break
            markets.extend(records)
            if len(records) < limit:
                break
            offset += limit
        return [self._parse_market(entry) for entry in markets]

    async def get_market(self, market_id: str) -> Market:
        data = await self._request_data("GET", f"/markets/{market_id}")
        payload = data.get("market", data) if isinstance(data, dict) else data
        return self._parse_market(payload)

    async def get_orderbook(self, market_id: str) -> OrderBook:
        try:
            orderbook, meta = await self._clob.build_orderbook(market_id)
            self.last_orderbook_at = datetime.now(tz=timezone.utc)
            self.last_orderbook_error = None
            self.last_orderbook_source = meta.source
            return orderbook
        except Exception as exc:
            self.last_orderbook_error = str(exc)
            raise PolymarketOrderbookUnavailable(str(exc)) from exc

    async def place_limit_order(
        self,
        market_id: str,
        side: OrderSide | str,
        price: float,
        size: float,
        client_order_id: str | None = None,
        token_id: str | None = None,
    ) -> Order:
        if token_id and market_id:
            raise ValueError("Invalid order payload: token_id and market_id are mutually exclusive")
        if not token_id and not market_id:
            raise ValueError("token_id or market_id is required")
        await self._ensure_proxy_funder()
        side_value = side.value.lower() if isinstance(side, OrderSide) else str(side).lower()
        self.logger.info(
            "polymarket order placement request",
            endpoint=f"{self.base_url}/orders",
            market_id=market_id,
            token_id=token_id,
            side=side_value,
            price=price,
            size=size,
        )
        payload = {
            "side": side_value,
            "type": "limit",
            "price": price,
            "size": size,
            "client_order_id": client_order_id,
        }
        if token_id:
            payload["token_id"] = token_id
        else:
            payload["market_id"] = market_id
        payload.update(
            {
                "signature_type": self.signature_type,
                "funder": self.funder_address or self.wallet_address,
            }
        )
        self.logger.debug(
            "polymarket order payload prepared",
            keys=list(payload.keys()),
            token_id=token_id,
            market_id=market_id,
        )
        data = await self._request(
            "POST",
            "/orders",
            payload=payload,
            headers={"User-Agent": "market-hedge-bot/1.0", "Accept": "application/json"},
        )
        return self._parse_order(data)

    async def place_market_order(
        self,
        market_id: str,
        side: OrderSide | str,
        size: float,
        client_order_id: str | None = None,
        token_id: str | None = None,
    ) -> Order:
        if token_id and market_id:
            raise ValueError("Invalid order payload: token_id and market_id are mutually exclusive")
        if not token_id and not market_id:
            raise ValueError("token_id or market_id is required")
        await self._ensure_proxy_funder()
        side_value = side.value.lower() if isinstance(side, OrderSide) else str(side).lower()
        self.logger.info(
            "polymarket order placement request",
            endpoint=f"{self.base_url}/orders",
            market_id=market_id,
            token_id=token_id,
            side=side_value,
            size=size,
        )
        payload = {
            "market_id": market_id if not token_id else None,
            "token_id": token_id,
            "side": side_value,
            "type": "market",
            "size": size,
            "client_order_id": client_order_id,
        }
        payload.update(
            {
                "signature_type": self.signature_type,
                "funder": self.funder_address or self.wallet_address,
            }
        )
        data = await self._request(
            "POST",
            "/orders",
            payload=payload,
            headers={"User-Agent": "market-hedge-bot/1.0", "Accept": "application/json"},
        )
        return self._parse_order(data)

    async def cancel_order(
        self,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> bool:
        identifier = order_id or client_order_id
        if not identifier:
            raise ValueError("order_id or client_order_id is required")
        await self._request("DELETE", f"/orders/{identifier}")
        return True

    async def get_order_status(self, order_id: str) -> Order:
        data = await self._request("GET", f"/orders/{order_id}")
        return self._parse_order(data)

    async def get_recent_trades(self, market_id: str) -> List[Dict[str, Any]]:
        data = await self._request("GET", "/trades", params={"market_id": market_id})
        return data.get("trades", [])

    async def get_balances(self) -> Dict[str, float]:
        self.logger.info("polymarket balance check", endpoint=f"{self.base_url}/balance-allowance")
        await self._ensure_proxy_funder()
        data = await self._request(
            "GET",
            "/balance-allowance",
            params={"asset_type": "COLLATERAL", "signature_type": self.signature_type},
        )
        balance = float(data.get("balance", 0.0))
        allowance = float(data.get("allowance", 0.0))
        allowances_map = data.get("allowances") or {}
        return {
            "USDC": balance,
            "USDC_allowance": allowance,
            "allowances": allowances_map,
        }

    async def get_positions(self) -> List[Dict[str, Any]]:
        try:
            data = await self._request_data("GET", "/positions")
        except RuntimeError:
            # Gamma/Data API may not expose positions; fall back to empty
            return []
        if isinstance(data, dict):
            return data.get("positions", [])
        return data

    async def fetch_fills(self, since: Optional[float] = None) -> List[Fill]:
        return await self.fetch_user_trades(since=since)

    async def fetch_user_trades(self, since: Optional[float] = None) -> List[Fill]:
        """
        Poll authenticated trades endpoint with cursor + since filtering.
        Returns individual partial fills (no aggregation) and preserves order.
        """
        params: Dict[str, Any] = {}
        if since is not None:
            try:
                params["start_time"] = int(since * 1000)
            except Exception:
                self.logger.warn("invalid since timestamp for trades", since=since)
        if self._last_fill_cursor:
            params["cursor"] = self._last_fill_cursor

        try:
            data = await self._request("GET", "/fills", params=params)
        except Exception as exc:
            self.logger.warn("polymarket trades fetch failed", error=str(exc))
            return []

        entries, cursor = self._extract_fill_entries(data)
        fills: List[Fill] = []
        seen: set[Tuple[str | None, str]] = set()
        for entry in entries:
            try:
                fill = self._parse_fill(entry)
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warn("polymarket fill parse failed", error=str(exc))
                continue
            if since is not None and fill.timestamp and fill.timestamp.timestamp() <= since:
                continue
            key = (fill.fill_id or fill.order_id, fill.timestamp.isoformat() if fill.timestamp else "")
            if key in seen:
                continue
            seen.add(key)
            fills.append(fill)

        fills.sort(key=lambda f: f.timestamp or datetime.now(tz=timezone.utc))
        self._last_fill_cursor = cursor
        return fills

    async def listen_fills(self, handler):
        raise NotImplementedError("Polymarket client does not expose websocket fills")

    async def close(self) -> None:
        return None

    def _parse_market(self, payload: Dict[str, Any]) -> Market:
        token_ids_raw = payload.get("clobTokenIds") or payload.get("clob_token_ids") or []
        if isinstance(token_ids_raw, str):
            try:
                token_ids_raw = json.loads(token_ids_raw)
            except Exception:
                token_ids_raw = [token_ids_raw]
        token_ids = [str(t) for t in token_ids_raw if t]
        yes_token_id = payload.get("yesTokenId") or payload.get("yes_token_id")
        no_token_id = payload.get("noTokenId") or payload.get("no_token_id")
        if not yes_token_id and len(token_ids) >= 1:
            yes_token_id = token_ids[0]
        if not no_token_id and len(token_ids) >= 2:
            no_token_id = token_ids[1]
        closed = str(payload.get("closed", "")).lower() == "true"
        archived = str(payload.get("archived", "")).lower() == "true"
        tradable_flag = payload.get("acceptingOrders")
        if tradable_flag is None:
            tradable_flag = payload.get("enableOrderBook")
        if tradable_flag is None:
            tradable_flag = payload.get("accepting_orders")
        if tradable_flag is None:
            tradable_flag = payload.get("enable_order_book")
        tradable = str(tradable_flag).lower() != "false" and not closed and not archived
        return Market(
            market_id=str(payload.get("id") or payload.get("market_id")),
            name=payload.get("question") or payload.get("name", ""),
            exchange=ExchangeName.POLYMARKET,
            status="open" if tradable else "closed",
            extra={
                "category": payload.get("category", ""),
                "volume": str(payload.get("volume", "")),
                "token_ids": token_ids,
                "tradable": tradable,
                "yes_token_id": str(yes_token_id) if yes_token_id else None,
                "no_token_id": str(no_token_id) if no_token_id else None,
                "yesTokenId": str(yes_token_id) if yes_token_id else None,
                "noTokenId": str(no_token_id) if no_token_id else None,
            },
        )

    def _parse_order(self, payload: Dict[str, Any]) -> Order:
        created_at = parse_polymarket_timestamp(payload.get("created_at") or payload.get("timestamp") or time.time())
        created_dt = created_at or datetime.now(tz=timezone.utc)
        raw_status = str(payload.get("status", "OPEN")).upper()
        try:
            status = OrderStatus(raw_status)
        except ValueError:
            status = OrderStatus.OPEN
        raw_side = str(payload.get("side", "BUY")).upper()
        try:
            side = OrderSide(raw_side)
        except ValueError:
            side = OrderSide.BUY
        order_type = (
            OrderType.LIMIT if str(payload.get("type", "limit")).lower() == "limit" else OrderType.MARKET
        )
        return Order(
            order_id=str(payload.get("order_id") or payload.get("id")),
            client_order_id=str(payload.get("client_order_id") or payload.get("order_id")),
            market_id=str(payload.get("market_id")),
            exchange=ExchangeName.POLYMARKET,
            side=side,
            order_type=order_type,
            price=float(payload.get("price", 0)),
            size=float(payload.get("size", 0)),
            filled_size=float(payload.get("filled_size", payload.get("fillAmount", 0))),
            status=status,
            created_at=created_dt,
        )

    def _extract_fill_entries(self, payload: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Extract fill entries and cursor/continuation token from heterogeneous responses.
        Explicitly fails if no fill array is present to avoid silent degradation.
        """
        cursor = None
        data: Any = None

        if isinstance(payload, list):
            data = payload
        elif isinstance(payload, dict):
            cursor = (
                payload.get("cursor")
                or payload.get("next_cursor")
                or payload.get("nextCursor")
                or payload.get("next")
            )
            data = payload.get("fills") or payload.get("trades")
            if data is None and isinstance(payload.get("data"), dict):
                inner = payload.get("data") or {}
                data = inner.get("fills") or inner.get("trades") or inner.get("data") or data
                cursor = inner.get("cursor") or inner.get("next_cursor") or cursor
            if data is None and isinstance(payload.get("data"), list):
                data = payload.get("data")
            if data is None and isinstance(payload.get("result"), list):
                data = payload.get("result")
            if data is None and isinstance(payload.get("result"), dict):
                inner = payload.get("result")
                data = inner.get("fills") or inner.get("trades") or inner.get("data")
                cursor = inner.get("cursor") or inner.get("next_cursor") or cursor

        if not isinstance(data, list):
            raise RuntimeError("polymarket fills payload missing list of fills")
        return data, cursor

    def _parse_fill(self, payload: Dict[str, Any]) -> Fill:
        ts = (
            payload.get("timestamp")
            or payload.get("filled_at")
            or payload.get("created_at")
            or payload.get("createdAt")
            or time.time()
        )
        try:
            ts_dt = parse_polymarket_timestamp(ts, allow_none=False)
        except Exception:
            self.logger.warn("polymarket fill timestamp parse failed", raw_timestamp=ts)
            ts_dt = datetime.now(tz=timezone.utc)
        side_raw = str(payload.get("side") or payload.get("taker_side") or payload.get("maker_side") or "BUY").upper()
        side = OrderSide.BUY if side_raw == "BUY" else OrderSide.SELL
        fill_id_raw = (
            payload.get("id")
            or payload.get("fill_id")
            or payload.get("trade_id")
            or payload.get("transaction_hash")
            or payload.get("transactionHash")
            or payload.get("tx_hash")
            or payload.get("txid")
            or payload.get("hash")
            or payload.get("order_id")
            or payload.get("orderId")
        )
        order_ref = payload.get("order_id") or payload.get("orderId") or payload.get("id")
        market_ref = (
            payload.get("market_id")
            or payload.get("token_id")
            or payload.get("tokenId")
            or payload.get("asset")
            or payload.get("market")
        )
        if not order_ref:
            raise RuntimeError("polymarket fill missing order_id")
        if not market_ref:
            raise RuntimeError("polymarket fill missing market_id")
        fill_id = str(fill_id_raw) if fill_id_raw is not None else None
        price_val = float(payload.get("price") or payload.get("fill_price") or 0.0)
        size_val = float(
            payload.get("size")
            or payload.get("filled_size")
            or payload.get("amount")
            or payload.get("quantity")
            or 0.0
        )
        if price_val <= 0 or size_val <= 0:
            raise RuntimeError("polymarket fill missing price/size")
        return Fill(
            order_id=str(order_ref),
            market_id=str(market_ref),
            exchange=ExchangeName.POLYMARKET,
            side=side,
            price=price_val,
            size=size_val,
            fee=float(payload.get("fee") or payload.get("fee_amount") or 0.0),
            timestamp=ts_dt,
            fill_id=fill_id,
        )

    async def _request_data(
        self,
        method: str,
        path: str,
        params: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        url = f"{self.data_url}/{path.lstrip('/')}"
        await self.rate_limit.acquire()
        headers = {"User-Agent": "market-hedge-bot/1.0"}
        async with self.session.request(method, url, params=params, proxy=self.proxy, headers=headers) as response:
            if response.status != 200:
                raise RuntimeError(f"polymarket data error {response.status}")
            return await response.json()

    def _auth_headers(
        self,
        method: str,
        path: str,
        payload: Dict[str, Any] | None = None,
        serialized_body: str | None = None,
        params: Any | None = None,
    ) -> Dict[str, str]:
        body = serialized_body or ""
        return build_polymarket_headers(
            api_key=self.api_key,
            secret_key_b64=self.secret,
            passphrase=self.passphrase,
            wallet_address=self.wallet_address,
            method=method,
            path=path,
            body=body,
            params=params,
            include_params_in_path=False,
            timestamp=None,
            timestamp_in_ms=False,
            signature_encoding="base64url",
            logger=self.logger,
        )

    async def _ensure_proxy_funder(self) -> None:
        """
        Resolve proxy wallet (funder) for signature_type=1 accounts by calling
        the Polymarket profile endpoint once per client instance.
        """
        if self.signature_type != 1:
            return
        if self.funder_address:
            return
        if self._profile_checked:
            return
        self._profile_checked = True
        try:
            proxy_wallet = await self._fetch_proxy_wallet()
        except PolymarketAuthError:
            raise
        except Exception as exc:
            self.logger.warn("polymarket proxy wallet fetch failed", error=str(exc))
            return
        if proxy_wallet:
            try:
                self.funder_address = to_checksum_address(proxy_wallet)
            except ValueError:
                self.funder_address = proxy_wallet
            self.logger.info(
                "polymarket proxy wallet resolved",
                signature_type=self.signature_type,
                funder_address=self.funder_address,
            )

    async def _fetch_proxy_wallet(self) -> str | None:
        """
        Fetch proxy wallet from Polymarket profile endpoints (no hardcoding).
        """
        candidates = ["/profile", "/auth/profile", "/profile/me"]
        for path in candidates:
            try:
                data = await self._request("GET", path)
            except FatalExchangeError:
                continue
            proxy = self._extract_proxy_wallet(data)
            if proxy:
                return proxy
        return None

    @staticmethod
    def _extract_proxy_wallet(data: Any) -> str | None:
        if isinstance(data, dict):
            for key in (
                "proxy_wallet",
                "proxyWallet",
                "wallet",
                "walletAddress",
                "proxy_address",
                "proxyAddress",
                "funder",
                "funderAddress",
                "address",
            ):
                value = data.get(key)
                if value:
                    return str(value)
            for nested_key in ("profile", "result", "data"):
                nested = data.get(nested_key)
                proxy = PolymarketAPI._extract_proxy_wallet(nested)
                if proxy:
                    return proxy
        return None

    async def _parse_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        try:
            data = await response.json()
        except aiohttp.ContentTypeError:
            text = await response.text()
            raise FatalExchangeError(f"unexpected response: {text}")

        status = response.status
        path = response.url.path
        if 200 <= status < 300:
            return data

        error_msg = data.get("error") or data.get("message") or str(data)
        if status == 400 and "invalid order payload" in error_msg.lower():
            raise PolymarketInvalidOrderPayload(error_msg, payload=data)
        if status in (401, 403):
            self.logger.error(
                "polymarket unauthorized",
                status=status,
                path=path,
                payload=data,
            )
            raise PolymarketAuthError(status=status, message=error_msg, payload=data)

        if status in (429, 500, 502, 503, 504):
            raise RecoverableExchangeError(error_msg)

        self.logger.warn("polymarket request failed", status=status, path=path, payload=data)
        raise FatalExchangeError(f"{status}: {error_msg}")

    async def _fetch_public_trades(self, role: str, since: Optional[float]) -> List[Dict[str, Any]]:
        """
        Poll the public data-api trades endpoint (no auth required).
        Supports maker/taker filters via role.
        """
        limit = 200
        offset = 0
        collected: List[Dict[str, Any]] = []
        headers = {"User-Agent": "market-hedge-bot/1.0"}
        backoff = 1.0
        while True:
            params: Dict[str, Any] = {role: self.wallet_address, "limit": limit, "offset": offset}
            url = f"{self.trades_url}/trades"
            if self.rate_limit:
                await self.rate_limit.acquire()
            async with self.session.get(url, params=params, proxy=self.proxy, headers=headers, timeout=30) as resp:
                if resp.status == 429:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 10.0)
                    continue
                backoff = 1.0
                if resp.status != 200:
                    raise RuntimeError(f"polymarket trades error {resp.status}")
                payload = await resp.json()
            if not isinstance(payload, list):
                raise RuntimeError("polymarket trades payload not list")
            if not payload:
                break
            collected.extend(payload)
            if len(payload) < limit:
                break
            if since is not None:
                try:
                    min_ts = min(
                        (parse_polymarket_timestamp(item.get("timestamp")) or datetime.now(tz=timezone.utc))
                        for item in payload
                    )
                    if min_ts and min_ts.timestamp() <= since:
                        break
                except Exception:
                    pass
            offset += limit
        return collected

    def _coerce_markets_payload(self, data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("markets"), list):
                return data.get("markets", [])
            if isinstance(data.get("data"), list):
                return data.get("data", [])
            if isinstance(data.get("result"), list):
                return data.get("result", [])
        raise RuntimeError("polymarket markets payload malformed")

    async def run_auth_diagnostics(self, market_id_for_trade: str | None = None) -> None:
        """
        Lightweight startup probe to validate Polymarket auth/permissions without
        touching core trading logic. Uses only read-only balance checks.
        """
        await self._ensure_proxy_funder()
        self.logger.info(
            "polymarket auth diagnostics start",
            base_url=self.base_url,
            wallet_address=self.wallet_address,
            signature_type=self.signature_type,
            funder_address=self.funder_address or self.wallet_address,
        )
        balance_endpoint = f"{self.base_url}/balance-allowance"
        try:
            balances = await self.get_balances()
            self.logger.info(
                "READ permission OK",
                endpoint=balance_endpoint,
                balance=balances.get("USDC"),
                allowance=balances.get("USDC_allowance"),
            )
        except Exception as exc:
            self.logger.error("READ permission FAILED", endpoint=balance_endpoint, error=str(exc))
            raise
