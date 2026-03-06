"""
Configuration management for arb_core.

Reads from existing configuration files:
- config/settings.yaml - Telegram, Google Sheets, trading settings
- config/accounts.json - Exchange credentials (Polymarket, Opinion)

NO NEW CONFIG FILES - uses only existing sources of truth.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class SheetsConfig:
    """Google Sheets configuration from settings.yaml."""

    enabled: bool = False
    sheet_id: str = ""
    range: str = "Sheet1!A1:F100"
    poll_interval_sec: int = 30
    credentials_path: str = "config/google-service-account.json"
    mode: str = "service_account"  # service_account | api_key
    api_key: str = ""

    def validate(self) -> list[str]:
        """Validate sheets configuration."""
        if not self.enabled:
            return []
        errors = []
        if not self.sheet_id:
            errors.append("google_sheets.sheet_id is required when enabled")
        if self.mode == "api_key" and not self.api_key:
            errors.append("google_sheets.api_key required for api_key mode")
        if self.mode == "service_account":
            if not self.credentials_path:
                errors.append("google_sheets.credentials_path required for service_account mode")
            elif not Path(self.credentials_path).exists():
                logger.warning(
                    f"google_sheets.credentials_path not found: {self.credentials_path}"
                )
        return errors


@dataclass
class PolymarketAccount:
    """Polymarket account credentials from accounts.json."""

    account_id: str = ""
    api_key: str = ""
    secret_key: str = ""
    passphrase: str = ""
    wallet_address: str = ""
    private_key: str = ""
    signature_type: int = 0
    funder_address: str = ""
    proxy: str = ""

    def validate(self) -> list[str]:
        """Validate Polymarket credentials."""
        errors = []
        if not self.api_key:
            errors.append("Polymarket: api_key is missing in accounts.json")
        if not self.secret_key:
            errors.append("Polymarket: secret_key is missing in accounts.json")
        if not self.passphrase:
            errors.append("Polymarket: passphrase is missing in accounts.json")
        return errors

    @property
    def is_configured(self) -> bool:
        """Check if account has required credentials."""
        return bool(self.api_key and self.secret_key and self.passphrase)


@dataclass
class OpinionAccount:
    """Opinion account credentials from accounts.json."""

    account_id: str = ""
    api_key: str = ""
    secret_key: str = ""
    private_key: str = ""
    multi_sig_address: str = ""
    rpc_url: str = ""
    chain_id: int = 56
    ws_url: str = ""
    proxy: str = ""

    def validate(self) -> list[str]:
        """Validate Opinion credentials."""
        errors = []
        if not self.api_key:
            errors.append("Opinion: api_key is missing in accounts.json")
        if not self.private_key:
            logger.warning("Opinion: private_key is empty (may be required for trading)")
        return errors

    @property
    def is_configured(self) -> bool:
        """Check if account has required credentials."""
        return bool(self.api_key)


@dataclass
class TelegramConfig:
    """Telegram configuration from settings.yaml."""

    enabled: bool = True
    token: str = ""
    chat_id: str = ""
    admin_ids: list[int] = field(default_factory=list)
    heartbeat_enabled: bool = False
    heartbeat_interval_sec: int = 900

    def validate(self) -> list[str]:
        """Validate Telegram configuration."""
        errors = []
        if self.enabled:
            if not self.token:
                errors.append("telegram.token is missing in settings.yaml")
            if not self.admin_ids and not self.chat_id:
                errors.append("telegram.chat_id or admin_ids required in settings.yaml")
        return errors

    @property
    def effective_admin_ids(self) -> list[int]:
        """Get admin IDs, falling back to chat_id if needed."""
        if self.admin_ids:
            return self.admin_ids
        if self.chat_id:
            try:
                return [int(self.chat_id)]
            except ValueError:
                return []
        return []


@dataclass
class TradingConfig:
    """Trading-related settings from settings.yaml."""

    dry_run: bool = True
    min_profit_percent: float = 0.02
    max_position_size: float = 50000
    min_quote_size: float = 100

    # Market-Hedge Mode settings
    market_hedge_enabled: bool = False
    hedge_ratio: float = 1.0
    max_slippage: float = 0.005  # 0.5 cents
    min_spread_for_entry: float = 0.002  # 0.2 cents
    cancel_unfilled_after_sec: float = 60.0
    poll_interval_sec: float = 1.0


@dataclass
class Config:
    """
    Unified application configuration.

    Sources:
    - settings.yaml: Telegram, Sheets, trading settings
    - accounts.json: Exchange credentials
    - Environment variables: Overrides only
    """

    # Telegram
    telegram: TelegramConfig = field(default_factory=TelegramConfig)

    # Sheets
    sheets: SheetsConfig = field(default_factory=SheetsConfig)

    # Exchange accounts
    polymarket: PolymarketAccount = field(default_factory=PolymarketAccount)
    opinion: OpinionAccount = field(default_factory=OpinionAccount)

    # Trading
    trading: TradingConfig = field(default_factory=TradingConfig)

    # Database
    db_path: str = "data/arb_core.db"

    # Logging
    log_level: str = "INFO"

    # Polling
    polling_timeout: int = 30

    # --- Compatibility properties for existing code ---

    @property
    def telegram_token(self) -> str:
        """Compatibility: telegram token."""
        return self.telegram.token

    @property
    def telegram_admin_ids(self) -> list[int]:
        """Compatibility: admin IDs."""
        return self.telegram.effective_admin_ids

    @classmethod
    def load(
        cls,
        settings_path: Optional[str] = None,
        accounts_path: Optional[str] = None,
    ) -> "Config":
        """
        Load configuration from existing files.

        Args:
            settings_path: Path to settings.yaml (default: config/settings.yaml)
            accounts_path: Path to accounts.json (default: config/accounts.json)

        Priority: Environment variables > YAML/JSON files > defaults
        """
        # --- Load settings.yaml ---
        yaml_config = cls._load_settings_yaml(settings_path)

        # --- Load accounts.json ---
        accounts_data = cls._load_accounts_json(accounts_path)

        # --- Build Telegram config ---
        tg_yaml = yaml_config.get("telegram", {})
        telegram_config = cls._build_telegram_config(tg_yaml)

        # --- Build Sheets config ---
        sheets_yaml = yaml_config.get("google_sheets", {})
        sheets_config = cls._build_sheets_config(sheets_yaml)

        # --- Build exchange accounts ---
        polymarket_account = cls._find_account(accounts_data, "polymarket")
        opinion_account = cls._find_account(accounts_data, "opinion")

        # --- Build trading config ---
        trading_config = cls._build_trading_config(yaml_config)

        # --- Database path ---
        db_path = os.environ.get(
            "ARB_CORE_DB_PATH",
            yaml_config.get("db_path", "data/arb_core.db"),
        )
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        config = cls(
            telegram=telegram_config,
            sheets=sheets_config,
            polymarket=polymarket_account,
            opinion=opinion_account,
            trading=trading_config,
            db_path=db_path,
            log_level=os.environ.get("LOG_LEVEL", yaml_config.get("log_level", "INFO")),
            polling_timeout=int(
                os.environ.get("POLLING_TIMEOUT", yaml_config.get("polling_timeout", 30))
            ),
        )

        # Log configuration status
        cls._log_config_status(config)

        return config

    @staticmethod
    def _load_settings_yaml(settings_path: Optional[str]) -> dict:
        """Load settings.yaml from standard locations."""
        paths_to_try = []
        if settings_path:
            paths_to_try.append(settings_path)
        paths_to_try.extend([
            "config/settings.yaml",
            "settings.yaml",
        ])

        for path in paths_to_try:
            if Path(path).exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f) or {}
                        logger.info(f"Loaded settings from: {path}")
                        return data
                except Exception as e:
                    logger.error(f"Failed to load settings from {path}: {e}")

        logger.warning("settings.yaml not found - using defaults")
        return {}

    @staticmethod
    def _load_accounts_json(accounts_path: Optional[str]) -> dict:
        """Load accounts.json from standard locations."""
        paths_to_try = []
        if accounts_path:
            paths_to_try.append(accounts_path)
        paths_to_try.extend([
            "config/accounts.json",
            "accounts.json",
        ])

        for path in paths_to_try:
            if Path(path).exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        logger.info(f"Loaded accounts from: {path}")
                        return data
                except Exception as e:
                    logger.error(f"Failed to load accounts from {path}: {e}")

        logger.warning("accounts.json not found - exchange credentials unavailable")
        return {}

    @classmethod
    def _build_telegram_config(cls, tg_yaml: dict) -> TelegramConfig:
        """Build Telegram config from YAML and env."""
        # Token: env override > yaml
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        if not token:
            token = os.environ.get("TG_BOT_TOKEN")
        if not token:
            token = tg_yaml.get("token", tg_yaml.get("bot_token", ""))

        # Chat ID
        chat_id = str(tg_yaml.get("chat_id", ""))

        # Admin IDs: env override > yaml
        admin_ids = []
        admin_ids_str = os.environ.get("TELEGRAM_ADMIN_IDS", "")
        if admin_ids_str:
            admin_ids = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip()]
        else:
            yaml_admin_ids = tg_yaml.get("admin_ids", [])
            if isinstance(yaml_admin_ids, str):
                admin_ids = [int(x.strip()) for x in yaml_admin_ids.split(",") if x.strip()]
            elif isinstance(yaml_admin_ids, list):
                admin_ids = [int(x) for x in yaml_admin_ids if x]

        return TelegramConfig(
            enabled=tg_yaml.get("enabled", True),
            token=token,
            chat_id=chat_id,
            admin_ids=admin_ids,
            heartbeat_enabled=tg_yaml.get("heartbeat_enabled", False),
            heartbeat_interval_sec=tg_yaml.get("heartbeat_interval_sec", 900),
        )

    @classmethod
    def _build_sheets_config(cls, sheets_yaml: dict) -> SheetsConfig:
        """Build Sheets config from YAML and env."""
        return SheetsConfig(
            enabled=os.environ.get("SHEETS_ENABLED", "").lower() == "true"
            or sheets_yaml.get("enabled", False),
            sheet_id=os.environ.get("SHEETS_ID", sheets_yaml.get("sheet_id", "")),
            range=os.environ.get("SHEETS_RANGE", sheets_yaml.get("range", "Sheet1!A1:F100")),
            poll_interval_sec=int(
                os.environ.get("SHEETS_POLL_INTERVAL", sheets_yaml.get("poll_interval_sec", 30))
            ),
            credentials_path=os.environ.get(
                "SHEETS_CREDENTIALS_PATH",
                sheets_yaml.get("credentials_path", "config/google-service-account.json"),
            ),
            mode=os.environ.get("SHEETS_MODE", sheets_yaml.get("mode", "service_account")),
            api_key=os.environ.get("SHEETS_API_KEY", sheets_yaml.get("api_key", "")),
        )

    @classmethod
    def _find_account(cls, accounts_data: dict, exchange: str) -> "PolymarketAccount | OpinionAccount":
        """Find account by exchange name in accounts.json."""
        accounts = accounts_data.get("accounts", [])

        for account in accounts:
            acct_exchange = account.get("exchange", "").lower()
            if acct_exchange == exchange.lower():
                if exchange.lower() == "polymarket":
                    return PolymarketAccount(
                        account_id=account.get("account_id", ""),
                        api_key=account.get("api_key", ""),
                        secret_key=account.get("secret_key", ""),
                        passphrase=account.get("passphrase", ""),
                        wallet_address=account.get("wallet_address", ""),
                        private_key=account.get("private_key", ""),
                        signature_type=int(account.get("signature_type", 0)),
                        funder_address=account.get("funder_address", ""),
                        proxy=account.get("proxy", ""),
                    )
                elif exchange.lower() == "opinion":
                    return OpinionAccount(
                        account_id=account.get("account_id", ""),
                        api_key=account.get("api_key", ""),
                        secret_key=account.get("secret_key", ""),
                        private_key=account.get("private_key", ""),
                        multi_sig_address=account.get("multi_sig_address", ""),
                        rpc_url=account.get("rpc_url", ""),
                        chain_id=int(account.get("chain_id", 56)),
                        ws_url=account.get("ws_url", ""),
                        proxy=account.get("proxy", ""),
                    )

        # Return empty account if not found
        if exchange.lower() == "polymarket":
            logger.warning(f"Account not found in accounts.json: {exchange}")
            return PolymarketAccount()
        else:
            logger.warning(f"Account not found in accounts.json: {exchange}")
            return OpinionAccount()

    @classmethod
    def _build_trading_config(cls, yaml_config: dict) -> TradingConfig:
        """Build trading config from YAML."""
        # Try market_hedge_mode first, then outcome_covered_arbitrage
        mh_section = yaml_config.get("market_hedge_mode", {})
        trading_section = yaml_config.get("outcome_covered_arbitrage", {})

        # Merge sections (market_hedge_mode takes priority)
        merged = {**trading_section, **mh_section}

        return TradingConfig(
            dry_run=yaml_config.get("dry_run", True),
            min_profit_percent=float(merged.get("min_profit_percent", 0.02)),
            max_position_size=float(merged.get("max_position_size_per_market", 50000)),
            min_quote_size=float(merged.get("min_quote_size", 100)),
            # Market-Hedge Mode settings
            market_hedge_enabled=bool(mh_section.get("enabled", False)),
            hedge_ratio=float(mh_section.get("hedge_ratio", 1.0)),
            max_slippage=float(mh_section.get("max_slippage_market_hedge", 0.005)),
            min_spread_for_entry=float(mh_section.get("min_spread_for_entry", 0.002)),
            cancel_unfilled_after_sec=float(mh_section.get("cancel_unfilled_after_sec", 60.0)),
            poll_interval_sec=float(mh_section.get("poll_interval_sec", 1.0)),
        )

    @staticmethod
    def _log_config_status(config: "Config"):
        """Log configuration loading status."""
        # Telegram
        if config.telegram.token:
            token_preview = config.telegram.token[:15] + "..."
            logger.info(f"Telegram token: {token_preview}")
        else:
            logger.warning("Telegram token: NOT SET (check settings.yaml)")

        if config.telegram.effective_admin_ids:
            logger.info(f"Telegram admin IDs: {config.telegram.effective_admin_ids}")
        else:
            logger.warning("Telegram admin IDs: NOT SET (check settings.yaml)")

        # Sheets
        if config.sheets.enabled:
            logger.info(f"Google Sheets: enabled (mode={config.sheets.mode})")
            if config.sheets.sheet_id:
                logger.info(f"Sheet ID: {config.sheets.sheet_id[:20]}...")
        else:
            logger.info("Google Sheets: disabled")

        # Polymarket
        if config.polymarket.is_configured:
            logger.info(f"Polymarket account: {config.polymarket.account_id or 'configured'}")
        else:
            logger.warning("Polymarket account: NOT CONFIGURED (check accounts.json)")

        # Opinion
        if config.opinion.is_configured:
            logger.info(f"Opinion account: {config.opinion.account_id or 'configured'}")
        else:
            logger.warning("Opinion account: NOT CONFIGURED (check accounts.json)")

    def validate(self) -> list[str]:
        """Validate all configuration. Returns list of error messages."""
        errors = []

        # Telegram validation
        errors.extend(self.telegram.validate())

        # Sheets validation (only if enabled)
        errors.extend(self.sheets.validate())

        # Exchange validation (warnings already logged, but collect errors)
        # These are soft errors - don't fail startup, but log warnings
        pm_errors = self.polymarket.validate()
        op_errors = self.opinion.validate()

        for err in pm_errors:
            logger.warning(err)
        for err in op_errors:
            logger.warning(err)

        return errors

    def health_check(self) -> dict:
        """
        Perform health check on configuration.

        Returns dict with status for each component.
        """
        return {
            "telegram": {
                "token_set": bool(self.telegram.token),
                "admin_ids_set": bool(self.telegram.effective_admin_ids),
                "enabled": self.telegram.enabled,
            },
            "sheets": {
                "enabled": self.sheets.enabled,
                "sheet_id_set": bool(self.sheets.sheet_id),
                "credentials_ok": (
                    self.sheets.mode == "api_key" and bool(self.sheets.api_key)
                ) or (
                    self.sheets.mode == "service_account"
                    and Path(self.sheets.credentials_path).exists()
                ),
            },
            "polymarket": {
                "configured": self.polymarket.is_configured,
                "account_id": self.polymarket.account_id,
            },
            "opinion": {
                "configured": self.opinion.is_configured,
                "account_id": self.opinion.account_id,
            },
            "database": {
                "path": self.db_path,
                "dir_exists": Path(self.db_path).parent.exists(),
            },
        }
