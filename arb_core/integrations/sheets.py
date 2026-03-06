"""
Google Sheets client for arb_core.

Fetches rows from configured sheet and parses them into pairs.
"""

import json
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import requests

from ..core.config import SheetsConfig
from ..core.logging import get_logger
from ..core.models import PairStatus, compute_pair_id

logger = get_logger(__name__)


class SheetsError(Exception):
    """Base exception for sheets errors."""

    pass


@dataclass
class ParsedRow:
    """Parsed row from Google Sheet."""

    row_index: int
    enabled: bool
    polymarket_url: str
    opinion_url: str
    max_position: float
    min_profit_percent: float
    pair_id: str
    status: PairStatus
    error_message: Optional[str] = None


@dataclass
class SheetsSyncResult:
    """Result of syncing from Google Sheets."""

    parsed_rows: list[ParsedRow]
    ok_count: int
    error_count: int
    disabled_count: int


class SheetsClient:
    """
    Client for fetching and parsing Google Sheets data.

    Supports two authentication modes:
    - api_key: Uses Google API key (sheet must be public)
    - service_account: Uses service account credentials
    """

    # Expected domains for URL validation
    POLYMARKET_DOMAINS = {"polymarket.com", "www.polymarket.com"}
    OPINION_DOMAINS = {"app.opinion.trade", "opinion.trade"}

    def __init__(self, config: SheetsConfig):
        self.config = config
        self._token: Optional[str] = None

    def fetch_rows(self) -> list[list[str]]:
        """Fetch raw rows from Google Sheets."""
        if not self.config.sheet_id or not self.config.range:
            raise SheetsError("sheet_id and range are required")

        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/"
            f"{self.config.sheet_id}/values/{self.config.range}"
        )

        headers = {}
        params = {}

        if self.config.mode == "api_key":
            if not self.config.api_key:
                raise SheetsError("api_key required for api_key mode")
            params["key"] = self.config.api_key
        else:
            # service_account mode
            token = self._get_service_account_token()
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                raise SheetsError(
                    f"Google Sheets API error ({response.status_code}): {response.text}"
                )
            payload = response.json()
            return payload.get("values", [])
        except requests.RequestException as e:
            raise SheetsError(f"Network error fetching sheet: {e}") from e

    def _get_service_account_token(self) -> str:
        """Get OAuth token from service account credentials."""
        if self._token:
            return self._token

        try:
            from google.auth.transport.requests import Request
            from google.oauth2.service_account import Credentials
        except ImportError as exc:
            raise SheetsError(
                "google-auth library required for service_account mode. "
                "Install with: pip install google-auth"
            ) from exc

        try:
            with open(self.config.credentials_path, "r", encoding="utf-8") as f:
                info = json.load(f)
        except FileNotFoundError:
            raise SheetsError(
                f"Credentials file not found: {self.config.credentials_path}"
            )
        except json.JSONDecodeError as e:
            raise SheetsError(f"Invalid credentials JSON: {e}")

        creds = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        request = Request()
        creds.refresh(request)

        if not creds.token:
            raise SheetsError("Failed to obtain Google Sheets token")

        self._token = creds.token
        return self._token

    def parse_rows(self, rows: list[list[str]]) -> SheetsSyncResult:
        """
        Parse sheet rows into ParsedRow objects.

        Expected columns (order matters, headers optional):
        - Column 0: enabled (TRUE/FALSE or checkbox)
        - Column 1: polymarket_url
        - Column 2: opinion_url
        - Column 3: max_position (optional, default 15)
        - Column 4: min_profit_percent (optional, default 0.0)
        """
        parsed_rows = []
        ok_count = 0
        error_count = 0
        disabled_count = 0

        if not rows:
            return SheetsSyncResult(
                parsed_rows=parsed_rows,
                ok_count=ok_count,
                error_count=error_count,
                disabled_count=disabled_count,
            )

        # Detect if first row is header
        first_row = rows[0] if rows else []
        has_header = self._is_header_row(first_row)
        data_rows = rows[1:] if has_header else rows
        start_index = 2 if has_header else 1  # 1-based row numbers

        for i, row in enumerate(data_rows):
            row_index = start_index + i

            # Skip rows with less than 3 columns
            if len(row) < 3:
                continue

            parsed = self._parse_row(row, row_index)
            parsed_rows.append(parsed)

            if parsed.status == PairStatus.ERROR:
                error_count += 1
            elif parsed.status == PairStatus.DISABLED:
                disabled_count += 1
            else:
                ok_count += 1

        return SheetsSyncResult(
            parsed_rows=parsed_rows,
            ok_count=ok_count,
            error_count=error_count,
            disabled_count=disabled_count,
        )

    def _is_header_row(self, row: list[str]) -> bool:
        """Check if row looks like a header."""
        if not row:
            return False
        # Check for common header keywords
        header_keywords = {
            "enabled",
            "polymarket",
            "opinion",
            "url",
            "max_position",
            "max",
            "profit",
        }
        first_cell = row[0].strip().lower()
        return any(kw in first_cell for kw in header_keywords)

    def _parse_row(self, row: list[str], row_index: int) -> ParsedRow:
        """Parse a single row into ParsedRow."""
        # Extract columns
        enabled_raw = row[0].strip() if len(row) > 0 else ""
        pm_url_raw = row[1].strip() if len(row) > 1 else ""
        op_url_raw = row[2].strip() if len(row) > 2 else ""
        max_pos_raw = row[3].strip() if len(row) > 3 else ""
        min_profit_raw = row[4].strip() if len(row) > 4 else ""

        # Parse enabled (default to True if empty)
        enabled = self._parse_enabled(enabled_raw)

        # Parse max_position
        try:
            max_position = float(max_pos_raw) if max_pos_raw else 15.0
            if max_position <= 0:
                max_position = 15.0
        except ValueError:
            max_position = 15.0

        # Parse min_profit_percent
        try:
            min_profit_percent = float(min_profit_raw) if min_profit_raw else 0.0
        except ValueError:
            min_profit_percent = 0.0

        # Validate URLs
        pm_url, pm_error = self._validate_polymarket_url(pm_url_raw)
        op_url, op_error = self._validate_opinion_url(op_url_raw)

        # Determine status based on validation
        if pm_error:
            return ParsedRow(
                row_index=row_index,
                enabled=enabled,
                polymarket_url=pm_url_raw,
                opinion_url=op_url_raw,
                max_position=max_position,
                min_profit_percent=min_profit_percent,
                pair_id="",
                status=PairStatus.ERROR,
                error_message=pm_error,
            )

        if op_error:
            return ParsedRow(
                row_index=row_index,
                enabled=enabled,
                polymarket_url=pm_url,
                opinion_url=op_url_raw,
                max_position=max_position,
                min_profit_percent=min_profit_percent,
                pair_id="",
                status=PairStatus.ERROR,
                error_message=op_error,
            )

        # Compute pair_id
        pair_id = compute_pair_id(pm_url, op_url)

        # Determine status
        if not enabled:
            status = PairStatus.DISABLED
        else:
            status = PairStatus.DISCOVERED

        return ParsedRow(
            row_index=row_index,
            enabled=enabled,
            polymarket_url=pm_url,
            opinion_url=op_url,
            max_position=max_position,
            min_profit_percent=min_profit_percent,
            pair_id=pair_id,
            status=status,
            error_message=None,
        )

    def _parse_enabled(self, value: str) -> bool:
        """Parse enabled value from various formats."""
        if not value:
            return True  # Default to enabled if empty
        lower = value.lower()
        if lower in {"false", "0", "no", "off", "disabled"}:
            return False
        return True  # TRUE, 1, yes, on, or any other value = enabled

    def _validate_polymarket_url(self, url: str) -> tuple[str, Optional[str]]:
        """
        Validate Polymarket URL.

        Returns (normalized_url, error_message).
        """
        if not url:
            return "", "Polymarket URL is empty"

        # Extract URL if embedded in text
        extracted = self._extract_url(url)
        if not extracted:
            return url, "Polymarket URL is not a valid HTTP(S) URL"

        # Normalize URL
        if not extracted.startswith(("http://", "https://")):
            extracted = f"https://{extracted}"

        try:
            parsed = urlparse(extracted)
        except Exception:
            return url, "Polymarket URL is malformed"

        # Check scheme
        if parsed.scheme not in ("http", "https"):
            return url, "Polymarket URL must be HTTP or HTTPS"

        # Check domain
        domain = parsed.netloc.lower()
        if not any(domain == d or domain.endswith(f".{d}") for d in self.POLYMARKET_DOMAINS):
            return url, f"Polymarket URL domain must be polymarket.com, got: {domain}"

        return extracted, None

    def _validate_opinion_url(self, url: str) -> tuple[str, Optional[str]]:
        """
        Validate Opinion URL.

        Returns (normalized_url, error_message).
        """
        if not url:
            return "", "Opinion URL is empty"

        # Extract URL if embedded in text
        extracted = self._extract_url(url)
        if not extracted:
            return url, "Opinion URL is not a valid HTTP(S) URL"

        # Normalize URL
        if not extracted.startswith(("http://", "https://")):
            extracted = f"https://{extracted}"

        try:
            parsed = urlparse(extracted)
        except Exception:
            return url, "Opinion URL is malformed"

        # Check scheme
        if parsed.scheme not in ("http", "https"):
            return url, "Opinion URL must be HTTP or HTTPS"

        # Check domain
        domain = parsed.netloc.lower()
        if not any(domain == d or domain.endswith(f".{d}") for d in self.OPINION_DOMAINS):
            return url, f"Opinion URL domain must be app.opinion.trade, got: {domain}"

        return extracted, None

    def _extract_url(self, text: str) -> Optional[str]:
        """Extract first HTTP(S) URL from text."""
        # Try to find URL pattern
        url_pattern = r"https?://[^\s<>\"']+|[a-zA-Z0-9][-a-zA-Z0-9]*\.[a-zA-Z]{2,}[^\s<>\"']*"
        match = re.search(url_pattern, text)
        if match:
            url = match.group(0)
            # Clean trailing punctuation
            url = url.rstrip(".,;:!?)")
            return url
        return None

    def fetch_and_parse(self) -> SheetsSyncResult:
        """Fetch rows from sheet and parse them."""
        rows = self.fetch_rows()
        return self.parse_rows(rows)
