"""
arb_core entrypoint.

Usage:
    python -m arb_core.main                       # Live trading with Telegram
    python -m arb_core.main --dry-run             # Dry-run mode (simulated orders)
    python -m arb_core.main --dry-run --no-telegram  # Console-only dry-run
    python -m arb_core.main --no-telegram         # Live trading without Telegram
    python -m arb_core.main --market-hedge        # Use Market-Hedge Mode
    python -m arb_core.main --health              # Health check only
    python -m arb_core.main --smoke-live          # Smoke test (REAL MONEY!)
"""

import argparse
import os
import sys
import time

from .core.config import Config
from .core.logging import get_logger
from .core.store import PairStore
from .exchanges.exchange_clients import create_clients
from .integrations.sheets_watcher import SheetsWatcher
from .market_data.orderbook import OrderbookManager
from .runners.market_hedge_runner import MarketHedgeRunner, MarketHedgeConfig
from .runners.runner import CoveredArbRunner, RunnerConfig, run_smoke_test
from .ui.telegram_bot import TelegramBot

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Arb Core - Covered Arbitrage Trading Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no real orders placed)",
    )

    parser.add_argument(
        "--smoke-live",
        action="store_true",
        help="Run live smoke test (requires I_UNDERSTAND_LIVE_TRADING=YES)",
    )

    parser.add_argument(
        "--no-runner",
        action="store_true",
        help="Disable the trading runner (Telegram only)",
    )

    parser.add_argument(
        "--market-hedge",
        action="store_true",
        help="Use Market-Hedge Mode (dual limit orders, market hedge on fill)",
    )

    parser.add_argument(
        "--health",
        action="store_true",
        help="Run health check and exit",
    )

    parser.add_argument(
        "--no-telegram",
        action="store_true",
        help="Run without Telegram bot (console-only mode)",
    )

    return parser.parse_args()


def run_health_check(config: Config) -> int:
    """Run health check and print results."""
    import sys
    import io

    # Force UTF-8 output on Windows
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    print("\n=== ARB_CORE HEALTH CHECK ===\n")

    health = config.health_check()
    all_ok = True

    # Telegram
    tg = health["telegram"]
    print("[Telegram]")
    if tg["token_set"]:
        print(f"   [OK] Token: set ({config.telegram.token[:15]}...)")
    else:
        print("   [FAIL] Token: NOT SET (check settings.yaml telegram.token)")
        all_ok = False

    if tg["admin_ids_set"]:
        print(f"   [OK] Admin IDs: {config.telegram.effective_admin_ids}")
    else:
        print("   [FAIL] Admin IDs: NOT SET (check settings.yaml telegram.chat_id)")
        all_ok = False

    # Sheets
    sh = health["sheets"]
    print("\n[Google Sheets]")
    if sh["enabled"]:
        print("   [OK] Enabled: true")
        if sh["sheet_id_set"]:
            print(f"   [OK] Sheet ID: {config.sheets.sheet_id[:20]}...")
        else:
            print("   [FAIL] Sheet ID: NOT SET")
            all_ok = False
        if sh["credentials_ok"]:
            print(f"   [OK] Credentials: OK (mode={config.sheets.mode})")
        else:
            print(f"   [WARN] Credentials: check {config.sheets.mode} settings")
    else:
        print("   [--] Disabled")

    # Polymarket
    pm = health["polymarket"]
    print("\n[Polymarket]")
    if pm["configured"]:
        print(f"   [OK] Account: {pm['account_id'] or 'configured'}")
        print(f"   [OK] API Key: {config.polymarket.api_key[:10]}...")
        if config.polymarket.wallet_address:
            print(f"   [OK] Wallet: {config.polymarket.wallet_address[:10]}...")
        else:
            print("   [--] Wallet: not set")
    else:
        print("   [FAIL] NOT CONFIGURED (check accounts.json)")
        all_ok = False

    # Opinion
    op = health["opinion"]
    print("\n[Opinion]")
    if op["configured"]:
        print(f"   [OK] Account: {op['account_id'] or 'configured'}")
        print(f"   [OK] API Key: {config.opinion.api_key[:10]}...")
        print(f"   [OK] Private Key: {'set' if config.opinion.private_key else 'not set'}")
    else:
        print("   [FAIL] NOT CONFIGURED (check accounts.json)")
        all_ok = False

    # Database
    db = health["database"]
    print("\n[Database]")
    print(f"   Path: {db['path']}")
    if db["dir_exists"]:
        print("   [OK] Directory exists")
    else:
        print("   [WARN] Directory will be created")

    # Summary
    print("\n" + "=" * 30)
    if all_ok:
        print("[OK] All checks passed!")
        return 0
    else:
        print("[FAIL] Some checks failed. Review configuration.")
        return 1


def main() -> int:
    """Main entrypoint."""
    args = parse_args()

    # Load configuration from existing files
    try:
        config = Config.load()
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        print(f"ERROR: Failed to load config: {e}")
        return 1

    # Health check mode
    if args.health:
        return run_health_check(config)

    mode = "dry-run" if args.dry_run else "live"
    logger.info(f"Starting arb_core (mode: {mode})")

    # Validate configuration (soft validation - warnings only)
    errors = config.validate()
    if errors:
        for error in errors:
            logger.warning(f"Config warning: {error}")
    
    # Check Telegram token (required unless --no-telegram)
    if not args.no_telegram and not config.telegram.token:
        logger.warning("No Telegram token configured. Use --no-telegram to run without Telegram.")
        args.no_telegram = True

    # Initialize store
    try:
        store = PairStore(config.db_path)
        logger.info(f"Store initialized: {config.db_path}")
    except Exception as e:
        logger.error(f"Failed to initialize store: {e}")
        return 1

    # Handle smoke-live mode
    if args.smoke_live:
        return run_smoke_live(config, store)

    # Create exchange clients using config credentials
    clients = create_clients(
        dry_run=args.dry_run or config.trading.dry_run,
        pm_api_key=config.polymarket.api_key,
        pm_api_secret=config.polymarket.secret_key,
        pm_passphrase=config.polymarket.passphrase,
        pm_wallet=config.polymarket.wallet_address,
        pm_private_key=config.polymarket.private_key,
        pm_signature_type=config.polymarket.signature_type,
        pm_funder=config.polymarket.funder_address,
        pm_proxy=config.polymarket.proxy,
        op_api_key=config.opinion.api_key,
        op_private_key=config.opinion.private_key,
        op_multi_sig=config.opinion.multi_sig_address,
        op_proxy=config.opinion.proxy,
    )

    # Create orderbook manager
    orderbook_manager = OrderbookManager()

    # Determine which runner to use
    use_market_hedge = args.market_hedge or config.trading.market_hedge_enabled

    if use_market_hedge:
        logger.info("Using Market-Hedge Mode")
        
        # Create Market-Hedge runner config
        mh_config = MarketHedgeConfig(
            dry_run=args.dry_run or config.trading.dry_run,
            hedge_ratio=config.trading.hedge_ratio,
            max_slippage_market_hedge=config.trading.max_slippage,
            min_spread_for_entry=config.trading.min_spread_for_entry,
            max_position_size_per_market=config.trading.max_position_size,
            cancel_unfilled_after_sec=config.trading.cancel_unfilled_after_sec,
            poll_interval_sec=config.trading.poll_interval_sec,
        )
        
        # Create Market-Hedge runner (callbacks will be set after bot is created)
        runner = MarketHedgeRunner(
            store=store,
            clients=clients,
            orderbook_manager=orderbook_manager,
            config=mh_config,
        )
        
        # Flag to set up callbacks later
        _needs_callbacks = True
    else:
        logger.info("Using Covered-Arb Mode")
        
        # Create covered arb runner config
        runner_config = RunnerConfig(
            dry_run=args.dry_run or config.trading.dry_run,
            default_max_position=config.trading.max_position_size,
            default_min_profit_percent=config.trading.min_profit_percent * 100,  # Convert to percent
        )

        # Create covered arb runner
        runner = CoveredArbRunner(
            store=store,
            clients=clients,
            orderbook_manager=orderbook_manager,
            config=runner_config,
        )

    # Initialize Telegram bot (optional)
    bot = None
    if not args.no_telegram:
        try:
            bot = TelegramBot(config, store)
            bot.set_runner(runner)
            
            # Set up trade notification callbacks
            if use_market_hedge:
                runner.on_trade_complete = bot.notify_trade_complete
                runner.on_unhedged_position = bot.notify_unhedged_position  # CRITICAL alerts
                logger.info("Trade notifications enabled (including unhedged position alerts)")
        except Exception as e:
            logger.warning(f"Failed to initialize Telegram bot: {e}")
            bot = None

    # Initialize Sheets watcher
    watcher = SheetsWatcher(config, store, telegram_bot=bot)
    if bot:
        bot.set_sheets_watcher(watcher)

    running = True

    try:
        # Start Telegram if available
        if bot:
            if not bot.start():
                logger.warning("Failed to start Telegram bot, continuing without it")
                bot = None

        # Start Sheets watcher
        if config.sheets.enabled:
            try:
                watcher.start()
            except Exception as e:
                logger.error(f"Failed to start sheets watcher: {e}")

        # Start runner (unless disabled)
        if not args.no_runner:
            try:
                runner.start()
                logger.info("Runner started")
                logger.info("TRADING DISABLED by default - send /start_trading in Telegram to enable")
            except Exception as e:
                logger.error(f"Failed to start runner: {e}")

        # Print status for console mode
        if not bot:
            print("\n" + "="*50)
            print("  ARB_CORE running in CONSOLE mode")
            print("  (No Telegram bot)")
            print("  Press Ctrl+C to stop")
            print("="*50 + "\n")

        # Run until interrupted
        while running:
            if bot and not bot._running:
                break
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        running = False
    except Exception as e:
        logger.error(f"Runtime error: {e}")
    finally:
        # Clean shutdown
        runner.stop()
        watcher.stop()
        if bot:
            bot.stop()
        clients.close()
        orderbook_manager.close()

    return 0


def run_smoke_live(config: Config, store: PairStore) -> int:
    """Run smoke-live test."""
    # Safety check
    if os.environ.get("I_UNDERSTAND_LIVE_TRADING") != "YES":
        logger.error(
            "Smoke-live requires I_UNDERSTAND_LIVE_TRADING=YES environment variable.\n"
            "This will execute a REAL trade with REAL money.\n"
            "Set the env var if you understand and accept this."
        )
        print("\n[FAIL] Smoke-live requires I_UNDERSTAND_LIVE_TRADING=YES")
        print("       This will execute a REAL trade with REAL money.")
        return 1

    logger.warning("=== SMOKE-LIVE: REAL TRADING MODE ===")

    # Validate exchange credentials
    if not config.polymarket.is_configured:
        logger.error("Polymarket account not configured in accounts.json")
        return 1
    if not config.opinion.is_configured:
        logger.error("Opinion account not configured in accounts.json")
        return 1

    # Create live clients
    clients = create_clients(
        dry_run=False,
        pm_api_key=config.polymarket.api_key,
        pm_api_secret=config.polymarket.secret_key,
        pm_passphrase=config.polymarket.passphrase,
        pm_wallet=config.polymarket.wallet_address,
        pm_private_key=config.polymarket.private_key,
        pm_signature_type=config.polymarket.signature_type,
        pm_funder=config.polymarket.funder_address,
        pm_proxy=config.polymarket.proxy,
        op_api_key=config.opinion.api_key,
        op_private_key=config.opinion.private_key,
        op_multi_sig=config.opinion.multi_sig_address,
        op_proxy=config.opinion.proxy,
    )

    if clients.is_dry_run:
        logger.error("Failed to create live clients")
        return 1

    orderbook_manager = OrderbookManager()

    try:
        result = run_smoke_test(
            store=store,
            clients=clients,
            orderbook_manager=orderbook_manager,
            max_size=1.0,  # Minimal size for smoke test
        )

        if result is None:
            logger.info("No tradeable pairs found for smoke test")
            print("\n[--] No tradeable ACTIVE pairs found for smoke test")
            return 0

        if result.success:
            logger.info("Smoke test completed successfully")
            print("\n[OK] Smoke test completed successfully!")
            print(f"     PM Order: {result.pm_order.order_id}")
            print(f"     OP Order: {result.op_order.order_id}")
            return 0
        else:
            logger.error(f"Smoke test failed: {result.error or result.skip_reason}")
            print(f"\n[FAIL] Smoke test failed: {result.error or result.skip_reason}")
            return 1

    finally:
        clients.close()
        orderbook_manager.close()


if __name__ == "__main__":
    sys.exit(main())
