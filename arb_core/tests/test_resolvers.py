"""
Tests for Polymarket and Opinion resolvers.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from arb_core.core.config import Config, SheetsConfig
from arb_core.core.models import PairStatus, compute_pair_id
from arb_core.integrations.resolvers.opinion_local import (
    OpinionDependencyError,
    OpinionLocalResolver,
    OpinionResolverError,
)
from arb_core.integrations.resolvers.polymarket import PolymarketResolver, PolymarketResolverError
from arb_core.core.store import PairStore
from arb_core.ui.telegram_ui import build_pair_keyboard, format_ready_card


@pytest.fixture
def store():
    """Create a temporary store for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        yield PairStore(path)
    finally:
        os.unlink(path)


class TestPolymarketResolver:
    """Tests for Polymarket CLOB resolver."""

    def test_parse_url_with_slug(self):
        """Should extract slug from URL."""
        resolver = PolymarketResolver()

        slug, cid, tid = resolver._parse_url(
            "https://polymarket.com/event/will-something-happen"
        )

        assert slug == "will-something-happen"
        assert cid is None
        assert tid is None

    def test_parse_url_with_tid(self):
        """Should extract tid from query params."""
        resolver = PolymarketResolver()

        slug, cid, tid = resolver._parse_url(
            "https://polymarket.com/event/test?tid=12345678901234567890"
        )

        assert tid == "12345678901234567890"

    def test_parse_url_with_condition_id(self):
        """Should extract condition_id from query params."""
        resolver = PolymarketResolver()

        slug, cid, tid = resolver._parse_url(
            "https://polymarket.com/event/test?cid=0xabcdef"
        )

        assert cid == "0xabcdef"

    @patch("requests.get")
    def test_resolve_by_slug_success(self, mock_get):
        """Should resolve tokens from slug via Gamma API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "slug": "test-market",
                "clobTokenIds": "111111,222222",
            }
        ]
        mock_get.return_value = mock_response

        resolver = PolymarketResolver()
        result = resolver.resolve("https://polymarket.com/event/test-market")

        assert result == {"YES": "111111", "NO": "222222"}

    @patch("requests.get")
    def test_resolve_handles_422_with_retry(self, mock_get):
        """Should retry on 422 errors."""
        # First call returns 422, second succeeds
        mock_422 = MagicMock()
        mock_422.status_code = 422

        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.json.return_value = [{"clobTokenIds": "aaa,bbb"}]

        mock_get.side_effect = [mock_422, mock_success]

        resolver = PolymarketResolver()
        resolver.RETRY_BACKOFF = [0, 0, 0]  # Skip delays for test

        result = resolver.resolve("https://polymarket.com/event/test")

        assert result == {"YES": "aaa", "NO": "bbb"}
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_resolve_error_after_max_retries(self, mock_get):
        """Should raise error after max retries."""
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_get.return_value = mock_response

        resolver = PolymarketResolver()
        resolver.RETRY_BACKOFF = [0, 0, 0]

        with pytest.raises(PolymarketResolverError, match="422"):
            resolver.resolve("https://polymarket.com/event/test")


class TestOpinionLocalResolver:
    """Tests for Opinion local resolver."""

    def test_parse_url_extracts_topic_id(self):
        """Should extract topicId from URL."""
        resolver = OpinionLocalResolver()

        topic_id, is_multi = resolver._parse_url(
            "https://app.opinion.trade/trade?topicId=12345"
        )

        assert topic_id == "12345"
        assert is_multi is False

    def test_parse_url_detects_multi(self):
        """Should detect multi-topic URLs."""
        resolver = OpinionLocalResolver()

        topic_id, is_multi = resolver._parse_url(
            "https://app.opinion.trade/trade?topicId=12345&type=multi"
        )

        assert topic_id == "12345"
        assert is_multi is True

    @patch("requests.Session.get")
    def test_resolve_success(self, mock_get):
        """Should resolve tokens from Opinion API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "data": {
                    "yesPos": "yes_token_123",
                    "noPos": "no_token_456",
                    "yesLabel": "Yes",
                    "noLabel": "No",
                    "questionId": "question_789",
                }
            }
        }
        mock_get.return_value = mock_response

        resolver = OpinionLocalResolver()
        result = resolver.resolve("https://app.opinion.trade/trade?topicId=12345")

        assert result == {"YES": "yes_token_123", "NO": "no_token_456", "question_id": "question_789"}

    @patch("requests.Session.get")
    def test_resolve_with_child_list(self, mock_get):
        """Should handle multi-topic with childList."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "data": {
                    "title": "Parent Topic",
                    "childList": [
                        {
                            "title": "Child 1",
                            "yesPos": "child_yes_1",
                            "noPos": "child_no_1",
                            "questionId": "child_q_1",
                        }
                    ],
                }
            }
        }
        mock_get.return_value = mock_response

        resolver = OpinionLocalResolver()
        result = resolver.resolve(
            "https://app.opinion.trade/trade?topicId=12345&type=multi"
        )

        assert result == {"YES": "child_yes_1", "NO": "child_no_1", "question_id": "child_q_1"}

    @patch("requests.Session.get")
    def test_resolve_api_error(self, mock_get):
        """Should raise error on API failure."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "errno": 1001,
            "errmsg": "Topic not found",
        }
        mock_get.return_value = mock_response

        resolver = OpinionLocalResolver()

        with pytest.raises(OpinionResolverError, match="Topic not found"):
            resolver.resolve("https://app.opinion.trade/trade?topicId=99999")

    def test_missing_dependency_error(self):
        """Should create user-friendly error for missing dependency."""
        error = OpinionDependencyError("loguru", "pip install loguru")

        assert "loguru" in str(error)
        assert "pip install loguru" in str(error)
        assert error.dependency == "loguru"


class TestSequentialButtonsFlow:
    """Test that buttons are shown sequentially."""

    def test_discovered_shows_pm_buttons_only(self, store):
        """DISCOVERED status should show only PM buttons, not OP."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        pair = store.get_pair(pair_id)

        keyboard = build_pair_keyboard(pair)
        actions = _extract_actions(keyboard)

        # Should have PM buttons
        assert "pm_yes" in actions
        assert "pm_no" in actions

        # Should NOT have OP buttons
        assert "op_yes" not in actions
        assert "op_no" not in actions

    def test_pm_selected_shows_op_buttons_only(self, store):
        """PM_SELECTED status should show only OP buttons, not PM."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES", token="pm_token_abc")
        pair = store.get_pair(pair_id)

        keyboard = build_pair_keyboard(pair)
        actions = _extract_actions(keyboard)

        # Should NOT have PM buttons
        assert "pm_yes" not in actions
        assert "pm_no" not in actions

        # Should have OP buttons
        assert "op_yes" in actions
        assert "op_no" in actions

    def test_ready_shows_action_buttons(self, store):
        """READY status should show Trade, Simulate, PnL, Reset."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES", token="pm_token_abc")
        store.set_op_selection(pair_id, "NO", token="op_token_xyz")
        pair = store.get_pair(pair_id)

        keyboard = build_pair_keyboard(pair)
        actions = _extract_actions(keyboard)

        # Should NOT have selection buttons
        assert "pm_yes" not in actions
        assert "op_yes" not in actions

        # Should have action buttons (sim is shortened from simulate)
        assert "trade" in actions
        assert "sim" in actions
        assert "pnl" in actions
        assert "reset" in actions


class TestPmSelectionPersistsToken:
    """Test that PM selection persists token and status."""

    def test_pm_selection_stores_token(self, store):
        """PM selection should store the token ID."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES", token="my_token_12345")

        pair = store.get_pair(pair_id)

        assert pair.pm_side == "YES"
        assert pair.pm_token == "my_token_12345"
        assert pair.status == PairStatus.PM_SELECTED


class TestOpinionSelectionMakesReady:
    """Test that Opinion selection transitions to READY."""

    def test_op_selection_makes_ready(self, store):
        """Opinion selection should transition to READY and store token."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(pair_id, pm_url, op_url, PairStatus.DISCOVERED)
        store.set_pm_selection(pair_id, "YES", token="pm_token")
        store.set_op_selection(pair_id, "NO", token="op_token")

        pair = store.get_pair(pair_id)

        assert pair.op_side == "NO"
        assert pair.op_token == "op_token"
        assert pair.status == PairStatus.READY


class TestOpinionDependencyError:
    """Test that missing dependency returns user-facing error."""

    def test_dependency_error_is_user_friendly(self):
        """Error message should be clear and actionable."""
        error = OpinionDependencyError("loguru", "pip install loguru")

        message = str(error)

        assert "Opinion resolver dependency missing" in message
        assert "loguru" in message
        assert "pip install" in message

    def test_dependency_error_without_hint(self):
        """Error should work without install hint."""
        error = OpinionDependencyError("some_module")

        message = str(error)

        assert "some_module" in message
        assert "missing" in message.lower()


class TestReadyCardFormat:
    """Test READY card formatting."""

    def test_ready_card_contains_all_info(self, store):
        """READY card should contain all required information."""
        pm_url = "https://polymarket.com/event/test"
        op_url = "https://app.opinion.trade/trade?topicId=123"
        pair_id = compute_pair_id(pm_url, op_url)

        store.upsert_pair(
            pair_id, pm_url, op_url, PairStatus.DISCOVERED, max_position=25.0
        )
        store.set_pm_selection(pair_id, "YES", token="pm_token_abc123")
        store.set_op_selection(pair_id, "NO", token="op_token_xyz789")
        pair = store.get_pair(pair_id)

        card = format_ready_card(pair)

        # Should contain URLs
        assert pm_url in card
        assert op_url in card

        # Should contain selected sides
        assert "YES" in card
        assert "NO" in card

        # Should contain shortened tokens
        assert "pm_token_abc" in card  # First part of token
        assert "op_token_xyz" in card

        # Should contain settings
        assert "25.00" in card  # max_position
        # Status is now in Russian
        assert "Готова" in card or "готова" in card


def _extract_actions(keyboard: list[list[dict]]) -> set[str]:
    """Extract action prefixes from keyboard."""
    actions = set()
    for row in keyboard:
        for button in row:
            data = button.get("callback_data", "")
            if ":" in data:
                action = data.split(":")[0]
                actions.add(action)
            else:
                actions.add(data)
    return actions
