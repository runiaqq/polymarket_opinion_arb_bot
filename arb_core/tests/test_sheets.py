"""
Tests for Google Sheets parsing and sync logic.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from arb_core.core.config import Config, SheetsConfig, TelegramConfig
from arb_core.core.models import PairStatus, compute_pair_id
from arb_core.integrations.sheets import ParsedRow, SheetsClient, SheetsSyncResult
from arb_core.integrations.sheets_watcher import SheetsWatcher
from arb_core.core.store import PairStore


@pytest.fixture
def sheets_config():
    """Create a test sheets config."""
    return SheetsConfig(
        enabled=True,
        sheet_id="test_sheet_id",
        range="Sheet1!A1:E100",
        poll_interval_sec=30,
        mode="api_key",
        api_key="test_api_key",
    )


@pytest.fixture
def sheets_client(sheets_config):
    """Create a SheetsClient for testing."""
    return SheetsClient(sheets_config)


@pytest.fixture
def store():
    """Create a temporary store for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        yield PairStore(path)
    finally:
        os.unlink(path)


@pytest.fixture
def config(sheets_config):
    """Create a test config."""
    return Config(
        telegram=TelegramConfig(
            token="test_token",
            chat_id="12345",
            admin_ids=[12345],
        ),
        sheets=sheets_config,
        db_path=":memory:",
    )


class TestSheetsRowParsing:
    """Test parsing of sheet rows."""

    def test_parse_enabled_row_creates_discovered(self, sheets_client):
        """Enabled row should create DISCOVERED status."""
        rows = [
            ["enabled", "polymarket", "opinion", "max_position", "min_profit"],
            [
                "TRUE",
                "https://polymarket.com/event/test-event",
                "https://app.opinion.trade/market/test",
                "20",
                "0.5",
            ],
        ]

        result = sheets_client.parse_rows(rows)

        assert result.ok_count == 1
        assert result.error_count == 0
        assert len(result.parsed_rows) == 1

        row = result.parsed_rows[0]
        assert row.status == PairStatus.DISCOVERED
        assert row.enabled is True
        assert row.max_position == 20.0
        assert row.min_profit_percent == 0.5

    def test_parse_disabled_row_sets_disabled(self, sheets_client):
        """Disabled row should create DISABLED status."""
        rows = [
            ["enabled", "polymarket", "opinion"],
            [
                "FALSE",
                "https://polymarket.com/event/test",
                "https://app.opinion.trade/market/test",
            ],
        ]

        result = sheets_client.parse_rows(rows)

        assert result.disabled_count == 1
        assert len(result.parsed_rows) == 1

        row = result.parsed_rows[0]
        assert row.status == PairStatus.DISABLED
        assert row.enabled is False

    def test_parse_various_disabled_formats(self, sheets_client):
        """Various disabled formats should all be recognized."""
        test_cases = ["false", "FALSE", "0", "no", "off", "disabled"]

        for disabled_value in test_cases:
            rows = [
                [
                    disabled_value,
                    "https://polymarket.com/event/test",
                    "https://app.opinion.trade/market/test",
                ]
            ]
            result = sheets_client.parse_rows(rows)
            assert (
                result.parsed_rows[0].status == PairStatus.DISABLED
            ), f"Failed for '{disabled_value}'"

    def test_parse_invalid_polymarket_url_sets_error(self, sheets_client):
        """Invalid Polymarket URL should create ERROR status."""
        rows = [
            [
                "TRUE",
                "https://example.com/not-polymarket",
                "https://app.opinion.trade/market/test",
            ]
        ]

        result = sheets_client.parse_rows(rows)

        assert result.error_count == 1
        row = result.parsed_rows[0]
        assert row.status == PairStatus.ERROR
        assert "polymarket.com" in row.error_message.lower()

    def test_parse_invalid_opinion_url_sets_error(self, sheets_client):
        """Invalid Opinion URL should create ERROR status."""
        rows = [
            [
                "TRUE",
                "https://polymarket.com/event/test",
                "https://example.com/not-opinion",
            ]
        ]

        result = sheets_client.parse_rows(rows)

        assert result.error_count == 1
        row = result.parsed_rows[0]
        assert row.status == PairStatus.ERROR
        assert "opinion" in row.error_message.lower()

    def test_parse_empty_url_sets_error(self, sheets_client):
        """Empty URL should create ERROR status."""
        rows = [
            ["TRUE", "", "https://app.opinion.trade/market/test"],
        ]

        result = sheets_client.parse_rows(rows)

        assert result.error_count == 1
        row = result.parsed_rows[0]
        assert row.status == PairStatus.ERROR
        assert "empty" in row.error_message.lower()

    def test_parse_default_values(self, sheets_client):
        """Missing optional columns should use defaults."""
        rows = [
            [
                "TRUE",
                "https://polymarket.com/event/test",
                "https://app.opinion.trade/market/test",
            ]
        ]

        result = sheets_client.parse_rows(rows)

        row = result.parsed_rows[0]
        assert row.max_position == 15.0  # Default
        assert row.min_profit_percent == 0.0  # Default

    def test_parse_skips_short_rows(self, sheets_client):
        """Rows with less than 3 columns should be skipped."""
        rows = [
            ["TRUE", "https://polymarket.com/event/test"],  # Only 2 columns
        ]

        result = sheets_client.parse_rows(rows)

        assert len(result.parsed_rows) == 0

    def test_parse_pair_id_computed_correctly(self, sheets_client):
        """pair_id should be computed from URLs."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/market/test"
        expected_id = compute_pair_id(pm_url, op_url)

        rows = [["TRUE", pm_url, op_url]]
        result = sheets_client.parse_rows(rows)

        assert result.parsed_rows[0].pair_id == expected_id

    def test_parse_header_row_detected(self, sheets_client):
        """Header row should be skipped."""
        rows = [
            ["enabled", "polymarket", "opinion"],  # Header
            [
                "TRUE",
                "https://polymarket.com/event/test",
                "https://app.opinion.trade/market/test",
            ],
        ]

        result = sheets_client.parse_rows(rows)

        # Only the data row should be parsed
        assert len(result.parsed_rows) == 1
        assert result.parsed_rows[0].row_index == 2  # 1-based, after header


class TestSheetsSyncTransitions:
    """Test that sheet sync respects status transitions."""

    def test_new_pair_becomes_discovered(self, store, config):
        """New pair from sheet should become DISCOVERED."""
        watcher = SheetsWatcher(config, store)

        parsed_row = ParsedRow(
            row_index=2,
            enabled=True,
            polymarket_url="https://polymarket.com/event/test",
            opinion_url="https://app.opinion.trade/market/test",
            max_position=15.0,
            min_profit_percent=0.0,
            pair_id=compute_pair_id(
                "https://polymarket.com/event/test",
                "https://app.opinion.trade/market/test",
            ),
            status=PairStatus.DISCOVERED,
        )

        action = watcher._process_row(parsed_row)

        assert action == "new"
        pair = store.get_pair(parsed_row.pair_id)
        assert pair is not None
        assert pair.status == PairStatus.DISCOVERED

    def test_disabled_row_sets_disabled(self, store, config):
        """Disabled row should set status to DISABLED."""
        watcher = SheetsWatcher(config, store)

        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/market/test"
        pair_id = compute_pair_id(pm_url, op_url)

        # Create pair first
        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)

        parsed_row = ParsedRow(
            row_index=2,
            enabled=False,
            polymarket_url=pm_url,
            opinion_url=op_url,
            max_position=15.0,
            min_profit_percent=0.0,
            pair_id=pair_id,
            status=PairStatus.DISABLED,
        )

        watcher._process_row(parsed_row)

        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.DISABLED

    def test_does_not_downgrade_pm_selected(self, store, config):
        """Sheet should not downgrade PM_SELECTED to DISCOVERED."""
        watcher = SheetsWatcher(config, store)

        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/market/test"
        pair_id = compute_pair_id(pm_url, op_url)

        # Create pair and set PM selection
        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES")

        # Verify it's PM_SELECTED
        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.PM_SELECTED

        # Process sheet row (enabled)
        parsed_row = ParsedRow(
            row_index=2,
            enabled=True,
            polymarket_url=pm_url,
            opinion_url=op_url,
            max_position=15.0,
            min_profit_percent=0.0,
            pair_id=pair_id,
            status=PairStatus.DISCOVERED,
        )

        watcher._process_row(parsed_row)

        # Status should remain PM_SELECTED
        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.PM_SELECTED

    def test_does_not_downgrade_ready(self, store, config):
        """Sheet should not downgrade READY to DISCOVERED."""
        watcher = SheetsWatcher(config, store)

        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/market/test"
        pair_id = compute_pair_id(pm_url, op_url)

        # Create pair and set both selections
        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES")
        store.set_op_selection(pair_id, "NO")

        # Verify it's READY
        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.READY

        # Process sheet row
        parsed_row = ParsedRow(
            row_index=2,
            enabled=True,
            polymarket_url=pm_url,
            opinion_url=op_url,
            max_position=20.0,  # Update settings
            min_profit_percent=1.0,
            pair_id=pair_id,
            status=PairStatus.DISCOVERED,
        )

        watcher._process_row(parsed_row)

        # Status should remain READY, but settings updated
        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.READY
        assert pair.max_position == 20.0
        assert pair.min_profit_percent == 1.0

    def test_does_not_downgrade_active(self, store, config):
        """Sheet should not downgrade ACTIVE to DISCOVERED."""
        watcher = SheetsWatcher(config, store)

        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/market/test"
        pair_id = compute_pair_id(pm_url, op_url)

        # Create pair and activate it
        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES")
        store.set_op_selection(pair_id, "NO")
        store.activate(pair_id)

        # Verify it's ACTIVE
        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.ACTIVE

        # Process sheet row
        parsed_row = ParsedRow(
            row_index=2,
            enabled=True,
            polymarket_url=pm_url,
            opinion_url=op_url,
            max_position=15.0,
            min_profit_percent=0.0,
            pair_id=pair_id,
            status=PairStatus.DISCOVERED,
        )

        watcher._process_row(parsed_row)

        # Status should remain ACTIVE
        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.ACTIVE

    def test_reenabled_pair_returns_to_discovered(self, store, config):
        """Re-enabled pair should return to DISCOVERED."""
        watcher = SheetsWatcher(config, store)

        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/market/test"
        pair_id = compute_pair_id(pm_url, op_url)

        # Create as disabled
        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISABLED)

        # Re-enable
        parsed_row = ParsedRow(
            row_index=2,
            enabled=True,
            polymarket_url=pm_url,
            opinion_url=op_url,
            max_position=15.0,
            min_profit_percent=0.0,
            pair_id=pair_id,
            status=PairStatus.DISCOVERED,
        )

        action = watcher._process_row(parsed_row)

        assert action == "reenabled"
        pair = store.get_pair(pair_id)
        assert pair.status == PairStatus.DISCOVERED


class TestSheetsUrlValidation:
    """Test URL validation logic."""

    def test_valid_polymarket_urls(self, sheets_client):
        """Valid Polymarket URLs should pass validation."""
        valid_urls = [
            "https://polymarket.com/event/test",
            "https://www.polymarket.com/market/test",
            "https://polymarket.com/event/will-something-happen?tid=123",
            "polymarket.com/event/test",  # Without scheme
        ]

        for url in valid_urls:
            result = sheets_client._validate_polymarket_url(url)
            assert result[1] is None, f"Failed for {url}: {result[1]}"

    def test_invalid_polymarket_urls(self, sheets_client):
        """Invalid Polymarket URLs should fail validation."""
        invalid_urls = [
            "",
            "https://example.com/test",
            "https://poly-market.com/test",
            "not a url",
        ]

        for url in invalid_urls:
            result = sheets_client._validate_polymarket_url(url)
            assert result[1] is not None, f"Should have failed for {url}"

    def test_valid_opinion_urls(self, sheets_client):
        """Valid Opinion URLs should pass validation."""
        valid_urls = [
            "https://app.opinion.trade/market/test",
            "https://opinion.trade/market/test",
            "app.opinion.trade/market/123",
        ]

        for url in valid_urls:
            result = sheets_client._validate_opinion_url(url)
            assert result[1] is None, f"Failed for {url}: {result[1]}"

    def test_invalid_opinion_urls(self, sheets_client):
        """Invalid Opinion URLs should fail validation."""
        invalid_urls = [
            "",
            "https://example.com/test",
            "https://opinion-trade.com/test",
        ]

        for url in invalid_urls:
            result = sheets_client._validate_opinion_url(url)
            assert result[1] is not None, f"Should have failed for {url}"


class TestSheetsErrorHandling:
    """Test that errors don't crash the sync loop."""

    def test_invalid_url_does_not_crash(self, sheets_client):
        """Invalid URLs should create ERROR rows, not crash."""
        rows = [
            ["TRUE", "not-a-url", "also-not-a-url"],
            [
                "TRUE",
                "https://polymarket.com/event/valid",
                "https://app.opinion.trade/valid",
            ],
        ]

        # Should not raise
        result = sheets_client.parse_rows(rows)

        assert result.error_count == 1
        assert result.ok_count == 1
        assert len(result.parsed_rows) == 2

    def test_mixed_valid_invalid_rows(self, sheets_client):
        """Mix of valid and invalid rows should all be processed."""
        rows = [
            ["enabled", "polymarket", "opinion"],
            [
                "TRUE",
                "https://polymarket.com/event/valid1",
                "https://app.opinion.trade/m1",
            ],
            ["TRUE", "invalid-pm", "https://app.opinion.trade/m2"],
            [
                "FALSE",
                "https://polymarket.com/event/valid2",
                "https://app.opinion.trade/m3",
            ],
            [
                "TRUE",
                "https://polymarket.com/event/valid3",
                "https://app.opinion.trade/m4",
            ],
        ]

        result = sheets_client.parse_rows(rows)

        assert result.ok_count == 2  # valid1 and valid3
        assert result.error_count == 1  # invalid-pm
        assert result.disabled_count == 1  # valid2 (disabled)
        assert len(result.parsed_rows) == 4
