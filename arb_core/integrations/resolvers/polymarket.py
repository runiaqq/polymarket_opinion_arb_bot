"""
Polymarket CLOB token resolver.

Extracts YES/NO token IDs from Polymarket market metadata.
"""

import re
import time
from typing import Optional
from urllib.parse import parse_qs, urlparse

import requests

from ...core.logging import get_logger

logger = get_logger(__name__)


class PolymarketResolverError(Exception):
    """Error resolving Polymarket tokens."""

    pass


class PolymarketResolver:
    """
    Resolves Polymarket market URLs to YES/NO token IDs.

    Uses the Polymarket CLOB API to fetch market metadata.
    """

    # CLOB API endpoints
    CLOB_BASE_URL = "https://clob.polymarket.com"
    GAMMA_API_URL = "https://gamma-api.polymarket.com"

    # Retry settings
    MAX_RETRIES = 3
    RETRY_BACKOFF = [1, 2, 5]  # seconds

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def resolve(self, polymarket_url: str) -> dict[str, str]:
        """
        Resolve Polymarket URL to token IDs.

        Args:
            polymarket_url: Full Polymarket market URL

        Returns:
            {"YES": token_id, "NO": token_id}

        Raises:
            PolymarketResolverError: If resolution fails
        """
        # Extract slug or condition_id from URL
        slug, condition_id, token_id = self._parse_url(polymarket_url)

        if token_id:
            # URL contains tid parameter - fetch market by token
            return self._resolve_by_token_id(token_id)

        if condition_id:
            # URL contains condition_id - fetch directly
            return self._resolve_by_condition_id(condition_id)

        if slug:
            # URL contains slug - need to look up condition_id first
            return self._resolve_by_slug(slug)

        raise PolymarketResolverError(
            f"Could not extract slug, condition_id, or token_id from URL: {polymarket_url}"
        )

    def _parse_url(self, url: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Parse Polymarket URL to extract identifiers.

        Returns (slug, condition_id, token_id)
        """
        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)

            # Check for tid (token_id) in query params
            token_id = query.get("tid", [None])[0]
            if not token_id:
                token_id = query.get("token_id", [None])[0]

            # Check for condition_id in query params
            condition_id = query.get("cid", [None])[0]
            if not condition_id:
                condition_id = query.get("condition_id", [None])[0]

            # Extract slug from path
            slug = None
            path_parts = [p for p in parsed.path.split("/") if p]
            for anchor in ("event", "market", "markets"):
                if anchor in path_parts:
                    idx = path_parts.index(anchor)
                    if idx + 1 < len(path_parts):
                        slug = path_parts[idx + 1]
                        break

            # Fallback: last path segment
            if not slug and path_parts:
                slug = path_parts[-1]
                # Clean up slug
                slug = slug.split("?")[0].split("#")[0]

            return slug, condition_id, token_id

        except Exception as e:
            logger.warning(f"Error parsing Polymarket URL: {e}")
            return None, None, None

    def _resolve_by_slug(self, slug: str) -> dict[str, str]:
        """Resolve by market slug using Gamma API."""
        logger.debug(f"Resolving Polymarket by slug: {slug}")

        # Try Gamma API for slug lookup
        url = f"{self.GAMMA_API_URL}/markets?slug={slug}"

        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(url, timeout=self.timeout)

                if response.status_code == 422:
                    # Retry with backoff
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(self.RETRY_BACKOFF[attempt])
                        continue
                    raise PolymarketResolverError(
                        f"Polymarket API returned 422 after {self.MAX_RETRIES} retries"
                    )

                if response.status_code != 200:
                    raise PolymarketResolverError(
                        f"Gamma API error ({response.status_code}): {response.text[:200]}"
                    )

                data = response.json()
                if not data:
                    # Try alternative endpoint
                    return self._resolve_by_slug_events(slug)

                # Gamma returns a list
                market = data[0] if isinstance(data, list) else data
                return self._extract_tokens_from_market(market)

            except requests.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_BACKOFF[attempt])
                    continue
                raise PolymarketResolverError(f"Network error: {e}") from e

        raise PolymarketResolverError("Failed to resolve by slug")

    def _resolve_by_slug_events(self, slug: str) -> dict[str, str]:
        """Try resolving by slug using events endpoint."""
        url = f"{self.GAMMA_API_URL}/events?slug={slug}"

        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code != 200:
                raise PolymarketResolverError(
                    f"Events API error ({response.status_code})"
                )

            data = response.json()
            if not data:
                raise PolymarketResolverError(f"No event found for slug: {slug}")

            event = data[0] if isinstance(data, list) else data
            markets = event.get("markets", [])

            if not markets:
                raise PolymarketResolverError(f"No markets found for event: {slug}")

            # Use first market
            market = markets[0]
            return self._extract_tokens_from_market(market)

        except requests.RequestException as e:
            raise PolymarketResolverError(f"Network error: {e}") from e

    def _resolve_by_condition_id(self, condition_id: str) -> dict[str, str]:
        """Resolve by condition_id using CLOB API."""
        logger.debug(f"Resolving Polymarket by condition_id: {condition_id}")

        url = f"{self.CLOB_BASE_URL}/markets/{condition_id}"

        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(url, timeout=self.timeout)

                if response.status_code == 422:
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(self.RETRY_BACKOFF[attempt])
                        continue
                    raise PolymarketResolverError(
                        f"CLOB API returned 422 after {self.MAX_RETRIES} retries"
                    )

                if response.status_code != 200:
                    raise PolymarketResolverError(
                        f"CLOB API error ({response.status_code})"
                    )

                market = response.json()
                return self._extract_tokens_from_clob_market(market)

            except requests.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_BACKOFF[attempt])
                    continue
                raise PolymarketResolverError(f"Network error: {e}") from e

        raise PolymarketResolverError("Failed to resolve by condition_id")

    def _resolve_by_token_id(self, token_id: str) -> dict[str, str]:
        """Resolve by token_id to find the complementary token."""
        logger.debug(f"Resolving Polymarket by token_id: {token_id}")

        # Query CLOB for token info
        url = f"{self.CLOB_BASE_URL}/markets"
        params = {"token_id": token_id}

        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=self.timeout)

                if response.status_code == 422:
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(self.RETRY_BACKOFF[attempt])
                        continue

                if response.status_code != 200:
                    # Fallback: try gamma API
                    return self._resolve_token_via_gamma(token_id)

                data = response.json()
                if not data:
                    return self._resolve_token_via_gamma(token_id)

                market = data[0] if isinstance(data, list) else data
                return self._extract_tokens_from_clob_market(market)

            except requests.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_BACKOFF[attempt])
                    continue
                raise PolymarketResolverError(f"Network error: {e}") from e

        raise PolymarketResolverError("Failed to resolve by token_id")

    def _resolve_token_via_gamma(self, token_id: str) -> dict[str, str]:
        """Fallback resolution via Gamma API."""
        url = f"{self.GAMMA_API_URL}/markets"
        params = {"clob_token_ids": token_id}

        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            if response.status_code != 200:
                raise PolymarketResolverError("Gamma fallback failed")

            data = response.json()
            if not data:
                raise PolymarketResolverError(f"Token not found: {token_id}")

            market = data[0] if isinstance(data, list) else data
            return self._extract_tokens_from_market(market)

        except requests.RequestException as e:
            raise PolymarketResolverError(f"Network error: {e}") from e

    def _extract_tokens_from_market(self, market: dict) -> dict[str, str]:
        """Extract YES/NO tokens from Gamma market response."""
        tokens = market.get("clobTokenIds") or market.get("clob_token_ids")

        if not tokens:
            # Try outcomes structure
            outcomes = market.get("outcomes", [])
            if len(outcomes) >= 2:
                # Check for token IDs in outcomes
                yes_token = None
                no_token = None
                for outcome in outcomes:
                    if isinstance(outcome, dict):
                        label = outcome.get("value", "").upper()
                        token = outcome.get("clob_token_id")
                        if label == "YES":
                            yes_token = token
                        elif label == "NO":
                            no_token = token

                if yes_token and no_token:
                    return {"YES": str(yes_token), "NO": str(no_token)}

            raise PolymarketResolverError("No token IDs found in market data")

        if isinstance(tokens, str):
            # Try JSON parse first (for ["id1", "id2"] format)
            import json
            try:
                parsed = json.loads(tokens)
                if isinstance(parsed, list):
                    tokens = parsed
            except json.JSONDecodeError:
                # Fallback to comma split
                tokens = tokens.split(",")

        if len(tokens) < 2:
            raise PolymarketResolverError(f"Expected 2 tokens, got {len(tokens)}")

        # First token is YES, second is NO (Polymarket convention)
        # Clean up any remaining quotes/brackets
        yes_token = str(tokens[0]).strip().strip('"').strip("'").strip("[").strip("]")
        no_token = str(tokens[1]).strip().strip('"').strip("'").strip("[").strip("]")
        return {"YES": yes_token, "NO": no_token}

    def _extract_tokens_from_clob_market(self, market: dict) -> dict[str, str]:
        """Extract YES/NO tokens from CLOB market response."""
        import json

        tokens = market.get("tokens", [])

        if not tokens:
            # Try clobTokenIds field
            clob_tokens = market.get("clobTokenIds")
            if clob_tokens:
                if isinstance(clob_tokens, str):
                    # Try JSON parse first
                    try:
                        parsed = json.loads(clob_tokens)
                        if isinstance(parsed, list):
                            clob_tokens = parsed
                    except json.JSONDecodeError:
                        clob_tokens = clob_tokens.split(",")
                # Clean tokens
                yes_t = str(clob_tokens[0]).strip().strip('"').strip("'").strip("[").strip("]")
                no_t = str(clob_tokens[1]).strip().strip('"').strip("'").strip("[").strip("]")
                return {"YES": yes_t, "NO": no_t}
            raise PolymarketResolverError("No tokens in CLOB market response")

        yes_token = None
        no_token = None

        for token in tokens:
            outcome = token.get("outcome", "").upper()
            token_id = token.get("token_id")

            if outcome == "YES":
                yes_token = token_id
            elif outcome == "NO":
                no_token = token_id

        if not yes_token or not no_token:
            # Fallback: assume first is YES, second is NO
            if len(tokens) >= 2:
                return {
                    "YES": str(tokens[0].get("token_id", "")),
                    "NO": str(tokens[1].get("token_id", "")),
                }
            raise PolymarketResolverError("Could not identify YES/NO tokens")

        return {"YES": str(yes_token), "NO": str(no_token)}
