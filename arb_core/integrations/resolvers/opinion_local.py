"""
Opinion token resolver using local /opinion software interface.

Resolves Opinion market URLs to YES/NO token IDs by querying
the Opinion API directly (same endpoints as opinion/modules/browser.py).
"""

import re
import time
from random import choices
from string import hexdigits
from typing import Optional
from urllib.parse import parse_qs, urlparse

from ...core.logging import get_logger

logger = get_logger(__name__)


class OpinionResolverError(Exception):
    """Error resolving Opinion tokens."""

    pass


class OpinionDependencyError(OpinionResolverError):
    """Missing dependency for Opinion resolver."""

    def __init__(self, dependency: str, install_hint: str = ""):
        self.dependency = dependency
        self.install_hint = install_hint
        message = f"Opinion resolver dependency missing: {dependency}"
        if install_hint:
            message += f". Install with: {install_hint}"
        super().__init__(message)


class OpinionLocalResolver:
    """
    Resolves Opinion market URLs to YES/NO token IDs.

    Uses the Opinion API directly (same approach as /opinion/modules/browser.py).
    Does NOT require authentication for token resolution.
    """

    # Opinion API base URL
    API_BASE_URL = "https://proxy.opinion.trade:8443/api/bsc/api/v2"

    # Retry settings
    MAX_RETRIES = 3
    RETRY_BACKOFF = [1, 2, 5]

    def __init__(self, proxy: Optional[str] = None, timeout: int = 30):
        """
        Initialize resolver.

        Args:
            proxy: Optional HTTP proxy (format: http://user:pass@host:port)
            timeout: Request timeout in seconds
        """
        self.proxy = proxy
        self.timeout = timeout
        self._session = None

    def _get_session(self):
        """Get or create aiohttp-like session using requests."""
        try:
            import requests
        except ImportError:
            raise OpinionDependencyError("requests", "pip install requests")

        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(
                {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://app.opinion.trade",
                    "Referer": "https://app.opinion.trade/",
                    "x-device-kind": "web",
                    "x-device-fingerprint": "".join(choices(hexdigits, k=32)).lower(),
                }
            )

        return self._session

    def resolve(self, opinion_url: str) -> dict[str, str]:
        """
        Resolve Opinion URL to token IDs.

        Args:
            opinion_url: Full Opinion market URL (e.g., https://app.opinion.trade/trade?topicId=123)

        Returns:
            {"YES": token_id, "NO": token_id}

        Raises:
            OpinionResolverError: If resolution fails
            OpinionDependencyError: If required dependency is missing
        """
        # Extract topicId from URL
        topic_id, is_multi = self._parse_url(opinion_url)

        if not topic_id:
            raise OpinionResolverError(
                f"Could not extract topicId from URL: {opinion_url}"
            )

        return self._resolve_topic(topic_id, is_multi)

    def _parse_url(self, url: str) -> tuple[Optional[str], bool]:
        """
        Parse Opinion URL to extract topicId and type.

        Returns (topic_id, is_multi)
        """
        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)

            # Get topicId
            topic_id = query.get("topicId", [None])[0]
            if not topic_id:
                topic_id = query.get("topic_id", [None])[0]

            # Check if multi-topic
            is_multi = query.get("type", [""])[0].lower() == "multi"

            # Fallback: try to find numeric ID in path
            if not topic_id:
                path_parts = parsed.path.split("/")
                for part in reversed(path_parts):
                    if part.isdigit():
                        topic_id = part
                        break

            return topic_id, is_multi

        except Exception as e:
            logger.warning(f"Error parsing Opinion URL: {e}")
            return None, False

    def _resolve_topic(self, topic_id: str, is_multi: bool = False) -> dict[str, str]:
        """Resolve topic to token IDs."""
        logger.debug(f"Resolving Opinion topic: {topic_id}, multi={is_multi}")

        try:
            import requests
        except ImportError:
            raise OpinionDependencyError("requests", "pip install requests")

        # Build API URL
        if is_multi:
            url = f"{self.API_BASE_URL}/topic/mutil/{topic_id}"
        else:
            url = f"{self.API_BASE_URL}/topic/{topic_id}"

        session = self._get_session()
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None

        for attempt in range(self.MAX_RETRIES):
            try:
                response = session.get(url, timeout=self.timeout, proxies=proxies)

                if response.status_code != 200:
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(self.RETRY_BACKOFF[attempt])
                        continue
                    raise OpinionResolverError(
                        f"Opinion API error ({response.status_code}): {response.text[:200]}"
                    )

                data = response.json()

                # Check for API errors
                if data.get("errmsg") or data.get("errno"):
                    raise OpinionResolverError(
                        f"Opinion API error: {data.get('errmsg', 'Unknown error')}"
                    )

                result = data.get("result", {}).get("data", {})
                if not result:
                    raise OpinionResolverError(f"No data found for topic {topic_id}")

                return self._extract_tokens(result, is_multi)

            except requests.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_BACKOFF[attempt])
                    continue
                raise OpinionResolverError(f"Network error: {e}") from e

        raise OpinionResolverError("Failed to resolve Opinion topic")

    def _extract_tokens(self, topic_data: dict, is_multi: bool = False) -> dict[str, str]:
        """
        Extract YES/NO tokens from topic data.

        Based on /opinion/modules/browser.py get_events() logic.
        """
        # For multi-topics, we need to handle childList
        if is_multi and topic_data.get("childList"):
            # Use first child for now
            # In future, could allow specifying which child via URL
            child = topic_data["childList"][0]
            return self._extract_tokens_from_event(child)

        return self._extract_tokens_from_event(topic_data)

    def _extract_tokens_from_event(self, event: dict) -> dict[str, str]:
        """Extract tokens from a single event/topic."""
        # Token IDs are in yesPos and noPos fields
        yes_token = event.get("yesPos")
        no_token = event.get("noPos")
        question_id = event.get("questionId")

        if not yes_token or not no_token:
            raise OpinionResolverError(
                f"Missing token IDs in event data. "
                f"yesPos={yes_token}, noPos={no_token}"
            )

        return {
            "YES": str(yes_token),
            "NO": str(no_token),
            "question_id": str(question_id) if question_id else None,
        }

    def resolve_with_labels(self, opinion_url: str) -> dict:
        """
        Resolve Opinion URL to token IDs with labels.

        Returns:
            {
                "YES": {"token": token_id, "label": label},
                "NO": {"token": token_id, "label": label}
            }
        """
        topic_id, is_multi = self._parse_url(opinion_url)

        if not topic_id:
            raise OpinionResolverError(
                f"Could not extract topicId from URL: {opinion_url}"
            )

        return self._resolve_topic_with_labels(topic_id, is_multi)

    def _resolve_topic_with_labels(
        self, topic_id: str, is_multi: bool = False
    ) -> dict:
        """Resolve topic to tokens with labels."""
        try:
            import requests
        except ImportError:
            raise OpinionDependencyError("requests", "pip install requests")

        if is_multi:
            url = f"{self.API_BASE_URL}/topic/mutil/{topic_id}"
        else:
            url = f"{self.API_BASE_URL}/topic/{topic_id}"

        session = self._get_session()
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None

        response = session.get(url, timeout=self.timeout, proxies=proxies)

        if response.status_code != 200:
            raise OpinionResolverError(
                f"Opinion API error ({response.status_code})"
            )

        data = response.json()
        if data.get("errmsg") or data.get("errno"):
            raise OpinionResolverError(
                f"Opinion API error: {data.get('errmsg', 'Unknown error')}"
            )

        result = data.get("result", {}).get("data", {})
        if not result:
            raise OpinionResolverError(f"No data found for topic {topic_id}")

        # Handle multi-topic
        if is_multi and result.get("childList"):
            event = result["childList"][0]
        else:
            event = result

        yes_token = event.get("yesPos")
        no_token = event.get("noPos")
        yes_label = event.get("yesLabel", "Yes")
        no_label = event.get("noLabel", "No")

        return {
            "YES": {"token": str(yes_token), "label": yes_label},
            "NO": {"token": str(no_token), "label": no_label},
        }

    def close(self):
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None
