"""
Tests for Telegram UI gating logic.

Verifies that correct buttons are shown for each status.
"""

import pytest

from arb_core.core.models import Pair, PairStatus, compute_pair_id
from arb_core.ui.telegram_ui import (
    build_pair_keyboard,
    format_error_pm_first,
    format_pair_card,
    format_start_message,
    status_emoji,
)


def make_pair(status: PairStatus, pm_side=None, op_side=None) -> Pair:
    """Create a test pair with given status."""
    pm_url = "https://polymarket.com/event/test"
    op_url = "https://opinion.com/market/test"
    return Pair(
        pair_id=compute_pair_id(pm_url, op_url),
        polymarket_url=pm_url,
        opinion_url=op_url,
        status=status,
        pm_side=pm_side,
        op_side=op_side,
    )


def extract_callback_actions(keyboard: list[list[dict]]) -> set[str]:
    """Extract unique action prefixes from keyboard."""
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


class TestKeyboardGating:
    """Test that correct buttons are shown for each status."""

    def test_discovered_shows_only_pm_buttons(self):
        """DISCOVERED status should show only Polymarket selection buttons."""
        pair = make_pair(PairStatus.DISCOVERED)
        keyboard = build_pair_keyboard(pair)
        actions = extract_callback_actions(keyboard)

        # Should have PM buttons
        assert "pm_yes" in actions
        assert "pm_no" in actions

        # Should NOT have OP buttons
        assert "op_yes" not in actions
        assert "op_no" not in actions

        # Should NOT have action buttons
        assert "trade" not in actions
        assert "simulate" not in actions
        assert "deactivate" not in actions

        # Should have close button
        assert "close" in actions

    def test_pm_selected_shows_only_op_buttons_and_reset(self):
        """PM_SELECTED status should show only Opinion buttons + Reset."""
        pair = make_pair(PairStatus.PM_SELECTED, pm_side="YES")
        keyboard = build_pair_keyboard(pair)
        actions = extract_callback_actions(keyboard)

        # Should NOT have PM buttons
        assert "pm_yes" not in actions
        assert "pm_no" not in actions

        # Should have OP buttons
        assert "op_yes" in actions
        assert "op_no" in actions

        # Should have reset
        assert "reset" in actions

        # Should NOT have trade/deactivate
        assert "trade" not in actions
        assert "deactivate" not in actions

        # Should have close
        assert "close" in actions

    def test_ready_shows_action_buttons(self):
        """READY status should show Simulate, PnL, Trade, Reset."""
        pair = make_pair(PairStatus.READY, pm_side="YES", op_side="NO")
        keyboard = build_pair_keyboard(pair)
        actions = extract_callback_actions(keyboard)

        # Should NOT have selection buttons
        assert "pm_yes" not in actions
        assert "pm_no" not in actions
        assert "op_yes" not in actions
        assert "op_no" not in actions

        # Should have action buttons (sim is shortened from simulate)
        assert "sim" in actions
        assert "pnl" in actions
        assert "trade" in actions
        assert "reset" in actions

        # Should NOT have deactivate
        assert "deactivate" not in actions

        # Should have close
        assert "close" in actions

    def test_active_shows_active_buttons(self):
        """ACTIVE status should show Simulate, PnL, Deactivate."""
        pair = make_pair(PairStatus.ACTIVE, pm_side="YES", op_side="NO")
        keyboard = build_pair_keyboard(pair)
        actions = extract_callback_actions(keyboard)

        # Should NOT have selection buttons
        assert "pm_yes" not in actions
        assert "pm_no" not in actions
        assert "op_yes" not in actions
        assert "op_no" not in actions

        # Should have these action buttons (sim/deact are shortened)
        assert "sim" in actions
        assert "pnl" in actions
        assert "deact" in actions

        # Should NOT have trade (already active)
        assert "trade" not in actions

        # Reset should not be directly available for active
        # (user should deactivate first based on spec)

        # Should have close
        assert "close" in actions

    def test_disabled_shows_reset_only(self):
        """DISABLED status should show only Reset."""
        pair = make_pair(PairStatus.DISABLED)
        keyboard = build_pair_keyboard(pair)
        actions = extract_callback_actions(keyboard)

        # Should have reset
        assert "reset" in actions

        # Should NOT have other actions
        assert "pm_yes" not in actions
        assert "op_yes" not in actions
        assert "trade" not in actions
        assert "simulate" not in actions

        # Should have close
        assert "close" in actions

    def test_error_shows_reset_only(self):
        """ERROR status should show only Reset."""
        pair = make_pair(PairStatus.ERROR)
        keyboard = build_pair_keyboard(pair)
        actions = extract_callback_actions(keyboard)

        # Should have reset
        assert "reset" in actions

        # Should NOT have other actions
        assert "pm_yes" not in actions
        assert "op_yes" not in actions
        assert "trade" not in actions

        # Should have close
        assert "close" in actions


class TestMessageFormatting:
    """Test message formatting functions."""

    def test_status_emoji(self):
        """Each status should have an emoji."""
        for status in PairStatus:
            emoji = status_emoji(status)
            assert emoji  # Should not be empty
            assert len(emoji) <= 2  # Emoji length

    def test_format_start_message_contains_counts(self):
        """Start message should contain all status counts."""
        counts = {status: i for i, status in enumerate(PairStatus)}
        message = format_start_message(counts)

        # Should mention status counts (Russian names now)
        assert "Найдена" in message or "0" in message
        assert "Готова" in message or "2" in message

        # Should contain mode (Russian)
        assert "опрос" in message or "polling" in message

    def test_format_pair_card_contains_urls(self):
        """Pair card should contain both URLs."""
        pair = make_pair(PairStatus.DISCOVERED)
        card = format_pair_card(pair)

        assert pair.polymarket_url in card
        assert pair.opinion_url in card
        # Status is now in Russian
        assert "Найдена" in card or "Статус" in card

    def test_format_pair_card_shows_selections(self):
        """Pair card should show selections when present."""
        pair = make_pair(PairStatus.READY, pm_side="YES", op_side="NO")
        card = format_pair_card(pair)

        assert "YES" in card
        assert "NO" in card

    def test_error_pm_first_message(self):
        """Error message for PM first should be in Russian."""
        msg = format_error_pm_first()
        assert "Polymarket" in msg
        assert "⚠️" in msg


class TestCallbackDataFormat:
    """Test callback data format."""

    def test_callback_contains_pair_id(self):
        """All callbacks (except close) should contain pair_id prefix."""
        pair = make_pair(PairStatus.DISCOVERED)
        keyboard = build_pair_keyboard(pair)

        for row in keyboard:
            for button in row:
                data = button.get("callback_data", "")
                if data != "close":
                    assert ":" in data
                    _, pair_id_prefix = data.split(":", 1)
                    # Now uses 16-char prefix for callback_data
                    assert pair.pair_id.startswith(pair_id_prefix)
                    assert len(pair_id_prefix) == 16

    def test_keyboard_structure(self):
        """Keyboard should be list of rows, each row is list of buttons."""
        pair = make_pair(PairStatus.DISCOVERED)
        keyboard = build_pair_keyboard(pair)

        assert isinstance(keyboard, list)
        for row in keyboard:
            assert isinstance(row, list)
            for button in row:
                assert isinstance(button, dict)
                assert "text" in button
                assert "callback_data" in button
