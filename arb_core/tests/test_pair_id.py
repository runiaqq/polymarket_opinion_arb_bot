"""
Tests for pair_id computation stability.
"""

import pytest

from arb_core.core.models import compute_pair_id


class TestPairIdStability:
    """Test pair_id is deterministic and case/whitespace insensitive."""

    def test_same_urls_produce_same_hash(self):
        """Same URLs should always produce the same pair_id."""
        pm_url = "https://polymarket.com/event/some-event"
        op_url = "https://opinion.com/market/some-market"

        id1 = compute_pair_id(pm_url, op_url)
        id2 = compute_pair_id(pm_url, op_url)

        assert id1 == id2
        assert len(id1) == 64  # SHA256 hex length

    def test_case_insensitive(self):
        """URLs should be normalized to lowercase."""
        pm_url_lower = "https://polymarket.com/event/test"
        pm_url_upper = "HTTPS://POLYMARKET.COM/EVENT/TEST"
        pm_url_mixed = "https://PolyMarket.COM/Event/TEST"

        op_url = "https://opinion.com/market/test"

        id1 = compute_pair_id(pm_url_lower, op_url)
        id2 = compute_pair_id(pm_url_upper, op_url)
        id3 = compute_pair_id(pm_url_mixed, op_url)

        assert id1 == id2 == id3

    def test_whitespace_insensitive(self):
        """Leading/trailing whitespace should be stripped."""
        pm_url = "https://polymarket.com/event/test"
        pm_url_spaces = "  https://polymarket.com/event/test  "
        pm_url_tabs = "\thttps://polymarket.com/event/test\t"

        op_url = "https://opinion.com/market/test"

        id1 = compute_pair_id(pm_url, op_url)
        id2 = compute_pair_id(pm_url_spaces, op_url)
        id3 = compute_pair_id(pm_url_tabs, op_url)

        assert id1 == id2 == id3

    def test_different_urls_produce_different_hashes(self):
        """Different URLs should produce different pair_ids."""
        pm_url1 = "https://polymarket.com/event/event1"
        pm_url2 = "https://polymarket.com/event/event2"
        op_url = "https://opinion.com/market/test"

        id1 = compute_pair_id(pm_url1, op_url)
        id2 = compute_pair_id(pm_url2, op_url)

        assert id1 != id2

    def test_url_order_matters(self):
        """Swapping PM and OP URLs should produce different IDs."""
        url1 = "https://polymarket.com/event/test"
        url2 = "https://opinion.com/market/test"

        id1 = compute_pair_id(url1, url2)
        id2 = compute_pair_id(url2, url1)

        assert id1 != id2

    def test_combined_normalization(self):
        """Test case + whitespace normalization together."""
        pm_url1 = "  HTTPS://POLYMARKET.COM/EVENT/TEST  "
        pm_url2 = "https://polymarket.com/event/test"
        op_url1 = "\tHTTPS://OPINION.COM/MARKET/TEST\t"
        op_url2 = "https://opinion.com/market/test"

        id1 = compute_pair_id(pm_url1, op_url1)
        id2 = compute_pair_id(pm_url2, op_url2)

        assert id1 == id2

    def test_hash_format(self):
        """Verify hash is valid hex string."""
        pair_id = compute_pair_id(
            "https://polymarket.com/test",
            "https://opinion.com/test"
        )

        # Should be 64 character hex string
        assert len(pair_id) == 64
        assert all(c in "0123456789abcdef" for c in pair_id)
