"""
Tests for PairStore with status transitions.
"""

import os
import tempfile

import pytest

from arb_core.core.models import Pair, PairStatus, compute_pair_id
from arb_core.core.store import (
    InvalidTransitionError,
    PairNotFoundError,
    PairStore,
    StoreError,
)


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
def sample_pair_data():
    """Sample pair data for testing."""
    pm_url = "https://polymarket.com/event/test"
    op_url = "https://opinion.com/market/test"
    pair_id = compute_pair_id(pm_url, op_url)
    return {
        "pair_id": pair_id,
        "pm_url": pm_url,
        "op_url": op_url,
    }


class TestPairStoreBasics:
    """Test basic store operations."""

    def test_upsert_creates_new_pair(self, store, sample_pair_data):
        """Upsert should create a new pair if it doesn't exist."""
        pair = store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        assert pair.pair_id == sample_pair_data["pair_id"]
        assert pair.status == PairStatus.DISCOVERED
        assert pair.polymarket_url == sample_pair_data["pm_url"]
        assert pair.opinion_url == sample_pair_data["op_url"]
        assert pair.created_at is not None
        assert pair.updated_at is not None

    def test_upsert_updates_existing_pair(self, store, sample_pair_data):
        """Upsert should update URLs and settings but preserve status."""
        # Create pair
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        # Update pair
        new_max_position = 50.0
        pair = store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
            max_position=new_max_position,
        )

        assert pair.max_position == new_max_position
        assert pair.status == PairStatus.DISCOVERED  # Status preserved

    def test_get_pair_exists(self, store, sample_pair_data):
        """Get pair should return the pair if it exists."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        pair = store.get_pair(sample_pair_data["pair_id"])
        assert pair is not None
        assert pair.pair_id == sample_pair_data["pair_id"]

    def test_get_pair_not_found(self, store):
        """Get pair should return None if not found."""
        pair = store.get_pair("nonexistent")
        assert pair is None

    def test_list_pairs_all(self, store, sample_pair_data):
        """List pairs should return all pairs."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        pairs = store.list_pairs()
        assert len(pairs) == 1

    def test_list_pairs_by_status(self, store, sample_pair_data):
        """List pairs should filter by status."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        # Should find DISCOVERED
        pairs = store.list_pairs([PairStatus.DISCOVERED])
        assert len(pairs) == 1

        # Should not find ACTIVE
        pairs = store.list_pairs([PairStatus.ACTIVE])
        assert len(pairs) == 0

    def test_count_by_status(self, store, sample_pair_data):
        """Count by status should return correct counts."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        counts = store.count_by_status()
        assert counts[PairStatus.DISCOVERED] == 1
        assert counts[PairStatus.ACTIVE] == 0


class TestStatusTransitions:
    """Test valid and invalid status transitions."""

    def test_discovered_to_pm_selected(self, store, sample_pair_data):
        """DISCOVERED -> PM_SELECTED should work."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        pair = store.set_pm_selection(sample_pair_data["pair_id"], "YES")

        assert pair.status == PairStatus.PM_SELECTED
        assert pair.pm_side == "YES"

    def test_pm_selected_to_ready(self, store, sample_pair_data):
        """PM_SELECTED -> READY should work when setting Opinion."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        store.set_pm_selection(sample_pair_data["pair_id"], "YES")

        pair = store.set_op_selection(sample_pair_data["pair_id"], "NO")

        assert pair.status == PairStatus.READY
        assert pair.pm_side == "YES"
        assert pair.op_side == "NO"

    def test_ready_to_active(self, store, sample_pair_data):
        """READY -> ACTIVE should work."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        store.set_pm_selection(sample_pair_data["pair_id"], "YES")
        store.set_op_selection(sample_pair_data["pair_id"], "NO")

        pair = store.activate(sample_pair_data["pair_id"])

        assert pair.status == PairStatus.ACTIVE

    def test_full_flow_discovered_to_active(self, store, sample_pair_data):
        """Full transition: DISCOVERED -> PM_SELECTED -> READY -> ACTIVE."""
        # Start: DISCOVERED
        pair = store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        assert pair.status == PairStatus.DISCOVERED

        # Select PM: -> PM_SELECTED
        pair = store.set_pm_selection(sample_pair_data["pair_id"], "YES")
        assert pair.status == PairStatus.PM_SELECTED

        # Select OP: -> READY
        pair = store.set_op_selection(sample_pair_data["pair_id"], "NO")
        assert pair.status == PairStatus.READY

        # Activate: -> ACTIVE
        pair = store.activate(sample_pair_data["pair_id"])
        assert pair.status == PairStatus.ACTIVE

    def test_cannot_activate_from_discovered(self, store, sample_pair_data):
        """Cannot activate directly from DISCOVERED."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        with pytest.raises(InvalidTransitionError):
            store.activate(sample_pair_data["pair_id"])

    def test_cannot_activate_from_pm_selected(self, store, sample_pair_data):
        """Cannot activate from PM_SELECTED."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        store.set_pm_selection(sample_pair_data["pair_id"], "YES")

        with pytest.raises(InvalidTransitionError):
            store.activate(sample_pair_data["pair_id"])

    def test_cannot_set_opinion_before_polymarket(self, store, sample_pair_data):
        """Cannot set Opinion selection before Polymarket."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        with pytest.raises(InvalidTransitionError):
            store.set_op_selection(sample_pair_data["pair_id"], "YES")

    def test_reset_clears_selections(self, store, sample_pair_data):
        """Reset should clear all selections and go back to DISCOVERED."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        store.set_pm_selection(sample_pair_data["pair_id"], "YES", token="pm_token_123")
        store.set_op_selection(sample_pair_data["pair_id"], "NO", token="op_token_456")

        pair = store.reset_selection(sample_pair_data["pair_id"])

        assert pair.status == PairStatus.DISCOVERED
        assert pair.pm_side is None
        assert pair.op_side is None
        assert pair.pm_token is None
        assert pair.op_token is None

    def test_deactivate_from_active(self, store, sample_pair_data):
        """ACTIVE -> READY should work via deactivate."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        store.set_pm_selection(sample_pair_data["pair_id"], "YES")
        store.set_op_selection(sample_pair_data["pair_id"], "NO")
        store.activate(sample_pair_data["pair_id"])

        pair = store.deactivate(sample_pair_data["pair_id"])

        assert pair.status == PairStatus.READY

    def test_cannot_deactivate_from_ready(self, store, sample_pair_data):
        """Cannot deactivate from READY."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        store.set_pm_selection(sample_pair_data["pair_id"], "YES")
        store.set_op_selection(sample_pair_data["pair_id"], "NO")

        with pytest.raises(InvalidTransitionError):
            store.deactivate(sample_pair_data["pair_id"])

    def test_mark_disabled(self, store, sample_pair_data):
        """Should be able to disable from any status."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        pair = store.mark_disabled(sample_pair_data["pair_id"])
        assert pair.status == PairStatus.DISABLED

    def test_mark_error(self, store, sample_pair_data):
        """Should be able to mark error with message."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        error_msg = "Invalid market URL"
        pair = store.mark_error(sample_pair_data["pair_id"], error_msg)

        assert pair.status == PairStatus.ERROR
        assert pair.error_message == error_msg

    def test_invalid_side_raises_error(self, store, sample_pair_data):
        """Invalid side value should raise error."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        with pytest.raises(StoreError, match="Invalid side"):
            store.set_pm_selection(sample_pair_data["pair_id"], "MAYBE")

    def test_pair_not_found_raises_error(self, store):
        """Operations on non-existent pair should raise PairNotFoundError."""
        with pytest.raises(PairNotFoundError):
            store.set_pm_selection("nonexistent", "YES")

        with pytest.raises(PairNotFoundError):
            store.activate("nonexistent")


class TestTokenStorage:
    """Test token storage with selections."""

    def test_pm_token_stored(self, store, sample_pair_data):
        """PM token should be stored with selection."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )

        pair = store.set_pm_selection(
            sample_pair_data["pair_id"], "YES", token="pm_token_abc123"
        )

        assert pair.pm_token == "pm_token_abc123"

    def test_op_token_stored(self, store, sample_pair_data):
        """OP token should be stored with selection."""
        store.upsert_pair(
            sample_pair_data["pair_id"],
            sample_pair_data["pm_url"],
            sample_pair_data["op_url"],
        )
        store.set_pm_selection(sample_pair_data["pair_id"], "YES")

        pair = store.set_op_selection(
            sample_pair_data["pair_id"], "NO", token="op_token_xyz789"
        )

        assert pair.op_token == "op_token_xyz789"
