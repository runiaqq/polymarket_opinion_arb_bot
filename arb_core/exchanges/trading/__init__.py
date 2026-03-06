"""
Trading clients for arb_core.

Contains real trading implementations for:
- Polymarket: Uses py-clob-client SDK
- Opinion: Uses EIP-712 signed orders (same as /opinion folder)
"""

from .polymarket_client import PolymarketTradingClient
from .opinion_client import OpinionTradingClient

__all__ = ["PolymarketTradingClient", "OpinionTradingClient"]
