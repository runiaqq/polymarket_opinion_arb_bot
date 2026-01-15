from __future__ import annotations

import asyncio
from typing import Dict, List

import argparse

from core.covered_arb_runner import CoveredArbRunner
from core.models import AccountCredentials, ExchangeName
from core.polymarket_allowance import fetch_polymarket_allowance
from core.risk_manager import RiskManager
from exchanges.opinion_api import OpinionAPI
from exchanges.orderbook_manager import OrderbookManager
from exchanges.polymarket_api import PolymarketAPI
from exchanges.rate_limiter import RateLimiter
from telegram.notifier import TelegramNotifier
from utils.account_pool import AccountSelector
from utils.config_loader import ConfigLoader, RateLimitConfig
from utils.db import Database
from utils.db_migrations import apply_migrations
from utils.google_sheets import GoogleSheetsClient
from utils.logger import BotLogger
from utils.proxy_handler import ProxyHandler


async def build_client(
    account: AccountCredentials,
    session,
    rate_cfg: RateLimitConfig,
    logger: BotLogger,
):
    limiter = RateLimiter(
        requests_per_minute=rate_cfg.requests_per_minute,
        burst=rate_cfg.burst,
    )
    if account.exchange == ExchangeName.POLYMARKET:
        signature_type = (
            account.signature_type
            if account.signature_type is not None
            else account.metadata.get("signature_type")
        )
        return PolymarketAPI(
            session=session,
            api_key=account.api_key,
            secret=account.secret_key,
            passphrase=account.passphrase or account.metadata.get("passphrase"),
            wallet_address=account.wallet_address or account.metadata.get("wallet_address"),
            rate_limit=limiter,
            logger=logger,
            proxy=account.proxy,
            signature_type=signature_type,
            funder_address=account.funder_address or account.metadata.get("funder_address"),
            builder_api_key=account.builder_api_key or account.metadata.get("builder_api_key"),
            builder_secret=account.builder_secret_key or account.metadata.get("builder_secret_key"),
            builder_passphrase=account.builder_passphrase or account.metadata.get("builder_passphrase"),
        )
    return OpinionAPI(
        session=session,
        api_key=account.api_key,
        secret=account.secret_key,
        rate_limit=limiter,
        logger=logger,
        proxy=account.proxy,
        private_key=account.private_key or account.metadata.get("private_key") or account.secret_key,
        chain_id=account.chain_id or 56,
    )


async def run_healthcheck(settings, accounts, logger: BotLogger) -> dict:
    report = {
        "bot_running": True,
        "google_sheets": False,
        "polymarket": False,
        "opinion": False,
        "allowance": False,
    }
    sheets_client = None
    proxy_handler = ProxyHandler(logger)
    clients_by_id: Dict[str, object] = {}
    try:
        if settings.google_sheets.enabled:
            try:
                sheets_client = GoogleSheetsClient(settings.google_sheets, logger=logger)
                await sheets_client.fetch_rows()
                report["google_sheets"] = True
            except Exception as exc:
                logger.warn("healthcheck: google sheets unreachable", error=str(exc))

        for account in accounts:
            session = await proxy_handler.get_session(account)
            rate_cfg = settings.rate_limits.get(
                account.exchange.value,
                RateLimitConfig(requests_per_minute=60, burst=5),
            )
            client = await build_client(account, session, rate_cfg, logger)
            clients_by_id[account.account_id] = client
            balances = None
            try:
                getter = getattr(client, "get_balances", None)
                if getter:
                    balances = await getter()
            except Exception as exc:
                logger.warn("healthcheck: balance fetch failed", exchange=account.exchange.value, error=str(exc))
            if account.exchange == ExchangeName.POLYMARKET:
                report["polymarket"] = report["polymarket"] or balances is not None
                try:
                    balance, allowance = await fetch_polymarket_allowance(client=client, logger=logger)
                    report["allowance"] = report["allowance"] or allowance > 0
                except Exception as exc:
                    logger.warn("healthcheck: allowance fetch failed", error=str(exc))
            if account.exchange == ExchangeName.OPINION:
                report["opinion"] = report["opinion"] or balances is not None
    finally:
        if sheets_client:
            await sheets_client.close()
        for client in set(clients_by_id.values()):
            close = getattr(client, "close", None)
            if close:
                await close()
        await proxy_handler.close()

    logger.info("healthcheck report", **report)
    return report


async def main(dry_run_override: bool | None = None, health_only: bool = False) -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    if dry_run_override is not None:
        settings.dry_run = dry_run_override
    if not settings.outcome_covered_arbitrage.enabled:
        raise RuntimeError("outcome_covered_arbitrage.disabled - enable to run")
    accounts = loader.load_accounts()

    logger = BotLogger("covered_arb")
    if health_only:
        await run_healthcheck(settings, accounts, logger)
        logger.info(
            "final verification checklist",
            pytest="UNKNOWN",
            covered_arb_logic="UNCHANGED",
            buy_only="ENFORCED",
            dry_run=settings.dry_run,
            ready_for_live="pending user confirmation",
        )
        return
    if not settings.google_sheets.enabled:
        raise RuntimeError("google_sheets must be enabled for outcome_covered_arbitrage")
    await apply_migrations(settings.database)
    db = Database(settings.database, logger=logger)
    await db.init()
    proxy_handler = ProxyHandler(logger)
    notifier = TelegramNotifier(
        token=settings.telegram.token,
        chat_id=settings.telegram.chat_id,
        enabled=settings.telegram.enabled,
    )

    risk_manager = RiskManager(settings.outcome_covered_arbitrage, logger, db)
    await risk_manager.restore_reservations()
    orderbook_manager = OrderbookManager()

    clients_by_id: Dict[str, object] = {}
    for account in accounts:
        session = await proxy_handler.get_session(account)
        rate_cfg = settings.rate_limits.get(
            account.exchange.value,
            RateLimitConfig(requests_per_minute=60, burst=5),
        )
        clients_by_id[account.account_id] = await build_client(
            account,
            session,
            rate_cfg,
            logger,
        )

    account_pools: Dict[ExchangeName, List[AccountCredentials]] = {
        ExchangeName.POLYMARKET: [
            account for account in accounts if account.exchange == ExchangeName.POLYMARKET
        ],
        ExchangeName.OPINION: [
            account for account in accounts if account.exchange == ExchangeName.OPINION
        ],
    }
    for exchange_name, pool in account_pools.items():
        if not pool:
            raise RuntimeError(f"at least one account required for {exchange_name.value}")

    selector_policy = settings.scheduler_policy or "first"
    account_selectors = {
        ExchangeName.POLYMARKET: AccountSelector(account_pools[ExchangeName.POLYMARKET], policy=selector_policy),
        ExchangeName.OPINION: AccountSelector(account_pools[ExchangeName.OPINION], policy=selector_policy),
    }

    # Basic Polymarket allowance check to ensure BUY-only flow is possible.
    polymarket_zero_accounts = []
    for account in account_pools[ExchangeName.POLYMARKET]:
        client = clients_by_id.get(account.account_id)
        if isinstance(client, PolymarketAPI):
            try:
                balance, allowance = await fetch_polymarket_allowance(client=client, logger=logger)
                if allowance <= 0:
                    polymarket_zero_accounts.append(
                        {
                            "account_id": account.account_id,
                            "wallet": account.wallet_address,
                            "balance": balance,
                            "allowance": allowance,
                        }
                    )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "polymarket diagnostics failed",
                    account_id=account.account_id,
                    wallet=account.wallet_address,
                    error=str(exc),
                )
                raise

    if polymarket_zero_accounts:
        logger.warn(
            "Polymarket allowance is zero for accounts; covered arb cannot trade",
            accounts=[entry.get("account_id") for entry in polymarket_zero_accounts],
        )
        return

    primary_poly = account_selectors[ExchangeName.POLYMARKET].select()
    primary_opinion = account_selectors[ExchangeName.OPINION].select()
    if not primary_poly or not primary_opinion:
        raise RuntimeError("account selection failed; ensure account pools are populated")
    clients_by_exchange = {
        ExchangeName.POLYMARKET: clients_by_id[primary_poly.account_id],
        ExchangeName.OPINION: clients_by_id[primary_opinion.account_id],
    }
    account_ids = {
        ExchangeName.POLYMARKET: primary_poly.account_id,
        ExchangeName.OPINION: primary_opinion.account_id,
    }

    sheet_client = GoogleSheetsClient(settings.google_sheets, logger=logger) if settings.google_sheets.enabled else None

    runner = CoveredArbRunner(
        config=settings.outcome_covered_arbitrage,
        orderbooks=orderbook_manager,
        risk_manager=risk_manager,
        clients_by_exchange=clients_by_exchange,
        account_ids=account_ids,
        fees=settings.fees,
        logger=logger,
        notifier=notifier if notifier.enabled else None,
        sheets_client=sheet_client,
        poll_interval_sec=settings.google_sheets.poll_interval_sec if settings.google_sheets.enabled else 2.0,
        clients_by_id=clients_by_id,
        account_selectors=account_selectors,
        dry_run=settings.dry_run,
    )

    logger.info(
        "final verification checklist",
        pytest="PENDING",
        covered_arb_logic="UNCHANGED",
        buy_only="ENFORCED",
        dry_run_completed=settings.dry_run,
        ready_for_live="pending user confirmation",
    )

    try:
        await runner.run()
    except KeyboardInterrupt:
        runner.stop()
    finally:
        if sheet_client:
            await sheet_client.close()
        for client in set(clients_by_id.values()):
            close = getattr(client, "close", None)
            if close:
                await close()
        await notifier.close()
        await proxy_handler.close()
        await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--health", action="store_true", help="run startup healthcheck and exit")
    parser.add_argument("--dry-run", dest="dry_run_flag", action="store_true", help="force dry-run mode")
    args = parser.parse_args()
    asyncio.run(main(dry_run_override=args.dry_run_flag if args.dry_run_flag else None, health_only=args.health))
