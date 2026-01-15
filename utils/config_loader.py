from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from core.models import AccountCredentials, ContractType, ExchangeName, StrategyDirection


@dataclass(slots=True)
class OutcomeCoveredArbConfig:
    """
    Configuration for outcome-covered arbitrage (BUY-only, limit-only).

    We intentionally keep only the controls that matter for exposure and entry;
    legacy market-hedge fields are deliberately omitted.
    """

    enabled: bool = True
    min_profit_percent: float = 0.0
    max_position_size_per_market: float = 0.0
    max_position_size_per_event: float = 0.0
    max_position_size_per_account: float = 0.0
    min_quote_size: float = 0.0


# Legacy class kept only to avoid import errors in unused modules.
@dataclass(slots=True)
class MarketHedgeConfig:
    enabled: bool
    hedge_ratio: float
    max_slippage_market_hedge: float
    min_spread_for_entry: float
    max_position_size_per_market: float
    max_position_size_per_event: float
    cancel_unfilled_after_ms: int
    allow_partial_fill_hedge: bool
    hedge_strategy: str = "FULL"
    max_slippage_percent: float = 0.05
    min_quote_size: float = 0.0
    exposure_tolerance: float = 0.0
    ultra_safe: bool = False
    allowed_unhedged_threshold: float = 0.0
    hedge_retry_attempts: int = 2
    hedge_retry_backoff_sec: float = 0.5
    block_on_hedge_failure: bool = True
    cancel_on_hedge_failure: bool = True


@dataclass(slots=True)
class ExchangeRoutingConfig:
    primary: ExchangeName
    secondary: ExchangeName


@dataclass(slots=True)
class FeeConfig:
    maker: float = 0.0
    taker: float = 0.0


@dataclass(slots=True)
class DatabaseConfig:
    backend: str
    dsn: str


@dataclass(slots=True)
class TelegramConfig:
    enabled: bool
    token: Optional[str]
    chat_id: Optional[str]
    heartbeat_enabled: bool = False
    heartbeat_interval_sec: int = 900


@dataclass(slots=True)
class RateLimitConfig:
    requests_per_minute: int
    burst: int


@dataclass(slots=True)
class ExchangeConnectivity:
    use_websocket: bool
    poll_interval: float


@dataclass(slots=True)
class GoogleSheetsConfig:
    enabled: bool = False
    sheet_id: str | None = None
    range: str = "Sheet1!A1:F100"
    poll_interval_sec: int = 60
    credentials_path: str | None = None
    mode: str = "service_account"
    api_key: str | None = None


@dataclass(slots=True)
class SheetsMonitorConfig:
    notify_chat_id: str | None = None
    poll_interval_sec: int = 60
    stale_after_polls: int = 3
    notify_on_startup: bool = False
    max_list_items: int = 20


@dataclass(slots=True)
class WebhookConfig:
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8081
    admin_token: str = ""


@dataclass(slots=True)
class OpmXxlbFeedConfig:
    enabled: bool = False
    poll_interval_sec: int = 300
    max_items_per_fetch: int = 20
    base_url: str = "https://opm.xxlb.one/"
    timeout_sec: int = 20


@dataclass(slots=True)
class ExternalFeedsConfig:
    opm_xxlb: OpmXxlbFeedConfig = field(default_factory=OpmXxlbFeedConfig)


@dataclass(slots=True)
class DiscoveryLiquidity:
    polymarket: float = 0.0
    opinion: float = 0.0


@dataclass(slots=True)
class ClobDiscoveryConfig:
    enabled: bool = True
    max_events: int = 20
    min_liquidity: float = 0.0
    max_spread: float = 0.05
    page_size: int = 200
    concurrency: int = 10
    base_url: str = "https://clob.polymarket.com"
    max_pages: int | None = 1


@dataclass(slots=True)
class ArbScanFilterConfig:
    min_horizon_days: int = 7
    max_horizon_days: int = 730
    allow_crypto_daily: bool = False
    crypto_min_horizon_days: int = 14
    allow_unknown_end_time: bool = False
    short_horizon_hours: int = 24


@dataclass(slots=True)
class ManualMatchConfig:
    clob_index_path: str = "data/polymarket_clob_index.json"
    clob_ttl_sec: int = 600
    candidate_limit: int = 15
    page_size: int = 5
    refresh_on_start: bool = False


@dataclass(slots=True)
class EventDiscoveryConfig:
    enabled: bool = False
    keywords_allow: List[str] = field(default_factory=list)
    keywords_block: List[str] = field(default_factory=list)
    min_liquidity: DiscoveryLiquidity = field(default_factory=DiscoveryLiquidity)
    horizon_days_min: int = 0
    horizon_days_max: int = 365
    poll_interval_sec: int = 300


@dataclass(slots=True)
class MarketPairConfig:
    event_id: str
    primary_market_id: str
    secondary_market_id: str
    primary_account_id: str | None = None
    secondary_account_id: str | None = None
    pair_id: str | None = None
    strategy: str | None = None
    max_position_size_per_market: float | None = None
    primary_exchange: ExchangeName | None = None
    secondary_exchange: ExchangeName | None = None
    contract_type: ContractType = ContractType.BINARY
    strategy_direction: StrategyDirection = StrategyDirection.AUTO
    polymarket_url: str | None = None
    polymarket_slug: str | None = None
    polymarket_outcome: str | None = None
    polymarket_outcome_index: int | None = None
    polymarket_market_id: str | None = None
    polymarket_token_id: str | None = None


@dataclass(slots=True)
class OpinionWebsocketConfig:
    enabled: bool = False
    url: str = "wss://ws.opinion.trade"
    topics: List[str] = field(default_factory=list)
    fallback_to_rest: bool = True
    reconnect_max_delay_sec: float = 30.0
    ping_interval_sec: float = 20.0


@dataclass(slots=True)
class OpinionConfig:
    websocket: OpinionWebsocketConfig = field(default_factory=OpinionWebsocketConfig)


@dataclass(slots=True)
class Settings:
    outcome_covered_arbitrage: OutcomeCoveredArbConfig = field(default_factory=OutcomeCoveredArbConfig)
    double_limit_enabled: bool = False
    exchanges: ExchangeRoutingConfig = field(
        default_factory=lambda: ExchangeRoutingConfig(primary=ExchangeName.OPINION, secondary=ExchangeName.POLYMARKET)
    )
    fees: Dict[ExchangeName, FeeConfig] = field(default_factory=dict)
    google_sheets: GoogleSheetsConfig = field(default_factory=GoogleSheetsConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    scheduler_policy: str = "first"
    dry_run: bool = False
    telegram: TelegramConfig = field(default_factory=lambda: TelegramConfig(enabled=False, token=None, chat_id=None))
    database: DatabaseConfig = field(
        default_factory=lambda: DatabaseConfig(backend="sqlite", dsn="sqlite+aiosqlite:///./data/test.db")
    )
    rate_limits: Dict[str, RateLimitConfig] = field(default_factory=dict)
    market_pairs: List[MarketPairConfig] = field(default_factory=list)
    connectivity: Dict[ExchangeName, ExchangeConnectivity] = field(default_factory=dict)
    opinion: OpinionConfig = field(default_factory=OpinionConfig)
    event_discovery: EventDiscoveryConfig = field(default_factory=EventDiscoveryConfig)
    clob_discovery: ClobDiscoveryConfig = field(default_factory=ClobDiscoveryConfig)
    manual_match: ManualMatchConfig = field(default_factory=ManualMatchConfig)
    arb_scan_filters: ArbScanFilterConfig = field(default_factory=ArbScanFilterConfig)
    external_feeds: ExternalFeedsConfig = field(default_factory=ExternalFeedsConfig)
    sheets_monitor: SheetsMonitorConfig = field(default_factory=SheetsMonitorConfig)
    # Deprecated legacy field kept to avoid import errors in unused modules.
    market_hedge_mode: MarketHedgeConfig | None = None


class ConfigLoader:
    """Loads configuration files for the bot."""

    def __init__(self, base_path: Path | None = None):
        self.base_path = base_path or Path(__file__).resolve().parent.parent
        self._config_dir = self.base_path / "config"

    def _resolve_config_file(self, filename: str, fallbacks: list[str] | None = None) -> Path:
        candidates = [self._config_dir / filename]
        if fallbacks:
            candidates.extend(self._config_dir / name for name in fallbacks)
        for path in candidates:
            if path.exists():
                return path
        searched = ", ".join(str(path) for path in candidates)
        raise FileNotFoundError(f"missing config file; searched: {searched}")

    def load_settings(self) -> Settings:
        settings_path = self._resolve_config_file(
            "settings.yaml",
            ["settings.local.yaml", "settings.example.yaml", "settings.template.yaml"],
        )
        with settings_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        return self._parse_settings(raw)

    def load_accounts(self) -> List[AccountCredentials]:
        accounts_path = self._resolve_config_file(
            "accounts.json",
            ["accounts.local.json", "accounts.example.json", "accounts.template.json"],
        )
        data = json.loads(accounts_path.read_text(encoding="utf-8"))
        accounts: List[AccountCredentials] = []
        for entry in data.get("accounts", []):
            exchange = ExchangeName(entry["exchange"])
            metadata = dict(entry.get("metadata", {}))
            if entry.get("ws_url"):
                metadata.setdefault("ws_url", entry["ws_url"])
            if entry.get("rpc_url"):
                metadata.setdefault("rpc_url", entry["rpc_url"])
            if entry.get("private_key"):
                metadata.setdefault("private_key", entry["private_key"])
            if entry.get("multi_sig_address"):
                metadata.setdefault("multi_sig_address", entry["multi_sig_address"])
            if entry.get("multi_sig_addr"):
                metadata.setdefault("multi_sig_address", entry["multi_sig_addr"])
            if entry.get("builder_api_key"):
                metadata.setdefault("builder_api_key", entry["builder_api_key"])
            if entry.get("builder_secret_key"):
                metadata.setdefault("builder_secret_key", entry["builder_secret_key"])
            if entry.get("builder_passphrase"):
                metadata.setdefault("builder_passphrase", entry["builder_passphrase"])
            accounts.append(
                AccountCredentials(
                    account_id=entry["account_id"],
                    exchange=exchange,
                    api_key=entry["api_key"],
                    secret_key=entry.get("secret_key", ""),
                    passphrase=entry.get("passphrase"),
                    wallet_address=entry.get("wallet_address"),
                    private_key=entry.get("private_key") or entry.get("privateKey"),
                    multi_sig_address=entry.get("multi_sig_address") or entry.get("multi_sig_addr"),
                    rpc_url=entry.get("rpc_url"),
                    chain_id=int(entry["chain_id"]) if entry.get("chain_id") not in (None, "") else None,
                    proxy=entry.get("proxy"),
                    metadata=metadata,
                    weight=float(entry.get("weight", 1.0)),
                    tokens_per_sec=float(entry.get("tokens_per_sec", 5.0)),
                    burst=int(entry.get("burst", 10)),
                    signature_type=(
                        int(entry["signature_type"]) if entry.get("signature_type") not in (None, "") else None
                    ),
                    funder_address=entry.get("funder_address"),
                    builder_api_key=entry.get("builder_api_key"),
                    builder_secret_key=entry.get("builder_secret_key"),
                    builder_passphrase=entry.get("builder_passphrase"),
                )
            )
        return accounts

    def _parse_settings(self, raw: Dict[str, object]) -> Settings:
        arb_cfg = raw.get("outcome_covered_arbitrage", {})
        exchanges_cfg = raw.get("exchanges", {})
        telegram_cfg = raw.get("telegram", {})
        db_cfg = raw.get("database", {})
        rate_cfg = raw.get("rate_limits", {})
        connectivity_cfg = raw.get("connectivity", {})
        opinion_cfg = raw.get("opinion", {})
        ws_cfg = opinion_cfg.get("websocket", {})
        clob_cfg = raw.get("clob_discovery", {})
        manual_match_cfg = raw.get("manual_match", {})
        arb_scan_cfg = raw.get("arb_scan_filters", {})
        external_cfg = raw.get("external_feeds", {})
        opm_cfg = external_cfg.get("opm_xxlb", {})
        sheets_monitor_cfg = raw.get("sheets_monitor", {})

        outcome_covered_arbitrage = OutcomeCoveredArbConfig(
            enabled=bool(arb_cfg.get("enabled", True)),
            min_profit_percent=float(arb_cfg.get("min_profit_percent", 0.0)),
            max_position_size_per_market=float(arb_cfg.get("max_position_size_per_market", 0.0)),
            max_position_size_per_event=float(arb_cfg.get("max_position_size_per_event", 0.0)),
            max_position_size_per_account=float(arb_cfg.get("max_position_size_per_account", 0.0)),
            min_quote_size=float(arb_cfg.get("min_quote_size", 0.0)),
        )

        exchanges = ExchangeRoutingConfig(
            primary=ExchangeName(exchanges_cfg.get("primary", "Opinion")),
            secondary=ExchangeName(exchanges_cfg.get("secondary", "Polymarket")),
        )

        telegram = TelegramConfig(
            enabled=bool(telegram_cfg.get("enabled", False)),
            token=telegram_cfg.get("token"),
            chat_id=telegram_cfg.get("chat_id"),
            heartbeat_enabled=bool(telegram_cfg.get("heartbeat_enabled", False)),
            heartbeat_interval_sec=int(telegram_cfg.get("heartbeat_interval_sec", 900)),
        )

        database = DatabaseConfig(
            backend=db_cfg.get("backend", "sqlite"),
            dsn=db_cfg.get("dsn", "sqlite+aiosqlite:///./data/covered_arb.db"),
        )

        rate_limits: Dict[str, RateLimitConfig] = {}
        for name, cfg in rate_cfg.items():
            rate_limits[name] = RateLimitConfig(
                requests_per_minute=int(cfg.get("requests_per_minute", 60)),
                burst=int(cfg.get("burst", 5)),
            )

        pairs = []
        for item in raw.get("market_pairs", []):
            if {"event_id", "primary_market_id", "secondary_market_id"} - item.keys():
                continue
            primary_exchange = item.get("primary_exchange")
            secondary_exchange = item.get("secondary_exchange")
            try:
                primary_exchange_enum = ExchangeName(primary_exchange) if primary_exchange else None
            except ValueError:
                primary_exchange_enum = None
            try:
                secondary_exchange_enum = ExchangeName(secondary_exchange) if secondary_exchange else None
            except ValueError:
                secondary_exchange_enum = None
            pairs.append(
                MarketPairConfig(
                    event_id=item["event_id"],
                    primary_market_id=item["primary_market_id"],
                    secondary_market_id=item["secondary_market_id"],
                    primary_account_id=item.get("primary_account_id"),
                    secondary_account_id=item.get("secondary_account_id"),
                    primary_exchange=primary_exchange_enum,
                    secondary_exchange=secondary_exchange_enum,
                    contract_type=ContractType(
                        str(item.get("contract_type", ContractType.BINARY.value)).upper()
                    )
                    if item.get("contract_type")
                    else ContractType.BINARY,
                    strategy_direction=StrategyDirection(
                        str(item.get("strategy_direction", StrategyDirection.AUTO.value)).upper()
                    )
                    if item.get("strategy_direction")
                    else StrategyDirection.AUTO,
                    polymarket_url=item.get("polymarket_url"),
                    polymarket_slug=item.get("polymarket_slug"),
                    polymarket_outcome=item.get("polymarket_outcome"),
                    polymarket_outcome_index=(
                        int(item.get("polymarket_outcome_index"))
                        if str(item.get("polymarket_outcome_index", "")).strip().lstrip("-").isdigit()
                        else None
                    ),
                    polymarket_market_id=item.get("polymarket_market_id"),
                    polymarket_token_id=item.get("polymarket_token_id"),
                )
            )

        connectivity: Dict[ExchangeName, ExchangeConnectivity] = {}
        for name, cfg in connectivity_cfg.items():
            try:
                exchange_name = ExchangeName(name)
            except ValueError:
                continue
            connectivity[exchange_name] = ExchangeConnectivity(
                use_websocket=bool(cfg.get("use_websocket", True)),
                poll_interval=float(cfg.get("poll_interval", 5.0)),
            )

        fees: Dict[ExchangeName, FeeConfig] = {}
        for name, cfg in raw.get("fees", {}).items():
            try:
                exchange_name = ExchangeName(name)
            except ValueError:
                continue
            fees[exchange_name] = FeeConfig(
                maker=float(cfg.get("maker", 0.0)),
                taker=float(cfg.get("taker", 0.0)),
            )

        sheets_cfg = raw.get("google_sheets", {})
        google_sheets = GoogleSheetsConfig(
            enabled=bool(sheets_cfg.get("enabled", False)),
            sheet_id=sheets_cfg.get("sheet_id"),
            range=str(sheets_cfg.get("range", "Sheet1!A1:F100")),
            poll_interval_sec=int(sheets_cfg.get("poll_interval_sec", 60)),
            credentials_path=sheets_cfg.get("credentials_path"),
            mode=str(sheets_cfg.get("mode", "service_account")),
            api_key=sheets_cfg.get("api_key"),
        )

        sheets_monitor = SheetsMonitorConfig(
            notify_chat_id=sheets_monitor_cfg.get("notify_chat_id") or telegram_cfg.get("chat_id"),
            poll_interval_sec=int(
                sheets_monitor_cfg.get("poll_interval_sec", sheets_cfg.get("poll_interval_sec", 60))
            ),
            stale_after_polls=int(sheets_monitor_cfg.get("stale_after_polls", 3)),
            notify_on_startup=bool(sheets_monitor_cfg.get("notify_on_startup", False)),
            max_list_items=int(sheets_monitor_cfg.get("max_list_items", 20)),
        )

        webhook_cfg = raw.get("webhook", {})
        webhook = WebhookConfig(
            enabled=bool(webhook_cfg.get("enabled", False)),
            host=str(webhook_cfg.get("host", "0.0.0.0")),
            port=int(webhook_cfg.get("port", 8081)),
            admin_token=str(webhook_cfg.get("admin_token", "")),
        )

        opinion_ws = OpinionWebsocketConfig(
            enabled=bool(ws_cfg.get("enabled", False)),
            url=str(ws_cfg.get("url", OpinionWebsocketConfig.url)),
            topics=list(ws_cfg.get("topics", [])),
            fallback_to_rest=bool(ws_cfg.get("fallback_to_rest", True)),
            reconnect_max_delay_sec=float(ws_cfg.get("reconnect_max_delay_sec", 30)),
            ping_interval_sec=float(ws_cfg.get("ping_interval_sec", 20)),
        )

        opinion = OpinionConfig(websocket=opinion_ws)

        event_cfg = raw.get("event_discovery", {})
        min_liq_cfg = event_cfg.get("min_liquidity", {})
        event_discovery = EventDiscoveryConfig(
            enabled=bool(event_cfg.get("enabled", False)),
            keywords_allow=list(event_cfg.get("keywords_allow", [])),
            keywords_block=list(event_cfg.get("keywords_block", [])),
            min_liquidity=DiscoveryLiquidity(
                polymarket=float(min_liq_cfg.get("polymarket", 0.0)),
                opinion=float(min_liq_cfg.get("opinion", 0.0)),
            ),
            horizon_days_min=int(event_cfg.get("horizon_days_min", 0)),
            horizon_days_max=int(event_cfg.get("horizon_days_max", 365)),
            poll_interval_sec=int(event_cfg.get("poll_interval_sec", 300)),
        )

        clob_discovery = ClobDiscoveryConfig(
            enabled=bool(clob_cfg.get("enabled", True)),
            max_events=int(clob_cfg.get("max_events", 20)),
            min_liquidity=float(clob_cfg.get("min_liquidity", 0.0)),
            max_spread=float(clob_cfg.get("max_spread", 0.05)),
            page_size=int(clob_cfg.get("page_size", 200)),
            concurrency=int(clob_cfg.get("concurrency", 10)),
            base_url=str(clob_cfg.get("base_url", "https://clob.polymarket.com")),
            max_pages=clob_cfg.get("max_pages"),
        )

        arb_scan_filters = ArbScanFilterConfig(
            min_horizon_days=int(arb_scan_cfg.get("min_horizon_days", 7)),
            max_horizon_days=int(arb_scan_cfg.get("max_horizon_days", 730)),
            allow_crypto_daily=bool(arb_scan_cfg.get("allow_crypto_daily", False)),
            crypto_min_horizon_days=int(arb_scan_cfg.get("crypto_min_horizon_days", 14)),
            allow_unknown_end_time=bool(arb_scan_cfg.get("allow_unknown_end_time", False)),
            short_horizon_hours=int(arb_scan_cfg.get("short_horizon_hours", 24)),
        )

        external_feeds = ExternalFeedsConfig(
            opm_xxlb=OpmXxlbFeedConfig(
                enabled=bool(opm_cfg.get("enabled", False)),
                poll_interval_sec=int(opm_cfg.get("poll_interval_sec", 300)),
                max_items_per_fetch=int(opm_cfg.get("max_items_per_fetch", 20)),
                base_url=str(opm_cfg.get("base_url", OpmXxlbFeedConfig.base_url)),
                timeout_sec=int(opm_cfg.get("timeout_sec", 20)),
            )
        )

        manual_defaults = ManualMatchConfig()
        manual_match = ManualMatchConfig(
            clob_index_path=str(manual_match_cfg.get("clob_index_path", manual_defaults.clob_index_path)),
            clob_ttl_sec=int(manual_match_cfg.get("clob_ttl_sec", manual_defaults.clob_ttl_sec)),
            candidate_limit=int(manual_match_cfg.get("candidate_limit", manual_defaults.candidate_limit)),
            page_size=int(manual_match_cfg.get("page_size", manual_defaults.page_size)),
            refresh_on_start=bool(manual_match_cfg.get("refresh_on_start", manual_defaults.refresh_on_start)),
        )

        return Settings(
            outcome_covered_arbitrage=outcome_covered_arbitrage,
            double_limit_enabled=bool(raw.get("double_limit_enabled", True)),
            exchanges=exchanges,
            fees=fees,
            google_sheets=google_sheets,
            webhook=webhook,
            dry_run=bool(raw.get("dry_run", True)),
            telegram=telegram,
            database=database,
            rate_limits=rate_limits,
            market_pairs=pairs,
            connectivity=connectivity,
            scheduler_policy=str(raw.get("scheduler", {}).get("policy", "round_robin")).lower(),
            opinion=opinion,
            event_discovery=event_discovery,
            clob_discovery=clob_discovery,
            manual_match=manual_match,
            arb_scan_filters=arb_scan_filters,
            external_feeds=external_feeds,
            sheets_monitor=sheets_monitor,
            market_hedge_mode=None,
        )

