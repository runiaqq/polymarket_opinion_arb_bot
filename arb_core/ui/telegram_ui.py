"""
Telegram UI components: message templates and inline keyboards.

Весь интерфейс на русском языке.
"""

from typing import TYPE_CHECKING, Optional

from ..core.models import Pair, PairStatus

if TYPE_CHECKING:
    from .math_utils import SimulationResult
    from .runner import TradeResult


# Русские названия статусов
STATUS_NAMES_RU = {
    PairStatus.DISCOVERED: "Найдена",
    PairStatus.PM_SELECTED: "PM выбран",
    PairStatus.READY: "Готова",
    PairStatus.ACTIVE: "Активна",
    PairStatus.DISABLED: "Отключена",
    PairStatus.ERROR: "Ошибка",
}


def status_emoji(status: PairStatus) -> str:
    """Get emoji for status."""
    return {
        PairStatus.DISCOVERED: "🔍",
        PairStatus.PM_SELECTED: "⏳",
        PairStatus.READY: "✅",
        PairStatus.ACTIVE: "🚀",
        PairStatus.DISABLED: "🔒",
        PairStatus.ERROR: "❌",
    }.get(status, "❓")


def status_name_ru(status: PairStatus) -> str:
    """Get Russian name for status."""
    return STATUS_NAMES_RU.get(status, status.value)


def format_status_counts(counts: dict[PairStatus, int]) -> str:
    """Format status counts for /start command."""
    lines = ["📊 *Статистика пар:*", ""]
    for status in PairStatus:
        emoji = status_emoji(status)
        name = status_name_ru(status)
        count = counts.get(status, 0)
        lines.append(f"{emoji} {name}: {count}")
    return "\n".join(lines)


def format_pair_compact(pair: Pair) -> str:
    """Format pair for compact list view."""
    emoji = status_emoji(pair.status)
    short_id = pair.pair_id[:8]

    # Extract domain/slug from URLs for display
    pm_display = _extract_slug(pair.polymarket_url)
    op_display = _extract_slug(pair.opinion_url)

    return f"{emoji} `{short_id}` | {pm_display} ↔ {op_display}"


def format_pair_card(pair: Pair) -> str:
    """Format full pair card with details."""
    emoji = status_emoji(pair.status)
    status_ru = status_name_ru(pair.status)
    lines = [
        f"{emoji} *Пара: {pair.pair_id[:12]}...*",
        "",
        f"📈 *Polymarket:*",
        f"  {pair.polymarket_url}",
    ]

    if pair.pm_side:
        lines.append(f"  Исход: *{pair.pm_side}*")
    if pair.pm_token:
        lines.append(f"  Токен: `{pair.pm_token[:16]}...`")

    lines.extend(
        [
            "",
            f"🎯 *Opinion:*",
            f"  {pair.opinion_url}",
        ]
    )

    if pair.op_side:
        lines.append(f"  Исход: *{pair.op_side}*")
    if pair.op_token:
        lines.append(f"  Токен: `{pair.op_token[:16]}...`")

    lines.extend(
        [
            "",
            f"⚙️ *Настройки:*",
            f"  Макс. контрактов: {pair.max_position:.0f}",
            f"  _(при цене ~0.50 = ~${pair.max_position * 0.5:.0f} на каждой бирже)_",
            f"  Мин. профит: {pair.min_profit_percent:.2f}%",
            "",
            f"📍 Статус: *{status_ru}*",
        ]
    )

    if pair.error_message:
        lines.append(f"⚠️ Ошибка: {pair.error_message}")

    return "\n".join(lines)


def build_pair_keyboard(pair: Pair) -> list[list[dict]]:
    """
    Build inline keyboard for pair based on status.

    Returns list of button rows, each button is {"text": ..., "callback_data": ...}

    Note: Telegram callback_data is limited to 64 bytes.
    We use first 16 chars of pair_id (still unique enough for practical use).
    """
    # Shorten pair_id to 16 chars to fit in 64-byte callback_data limit
    pid = pair.pair_id[:16]
    buttons = []

    if pair.status == PairStatus.DISCOVERED:
        # Показываем ТОЛЬКО кнопки выбора Polymarket
        buttons.append(
            [
                {"text": "📈 PM: YES", "callback_data": f"pm_yes:{pid}"},
                {"text": "📉 PM: NO", "callback_data": f"pm_no:{pid}"},
            ]
        )

    elif pair.status == PairStatus.PM_SELECTED:
        # Показываем ТОЛЬКО кнопки выбора Opinion + Сброс
        buttons.append(
            [
                {"text": "🎯 OP: YES", "callback_data": f"op_yes:{pid}"},
                {"text": "🎯 OP: NO", "callback_data": f"op_no:{pid}"},
            ]
        )
        buttons.append(
            [{"text": "🔄 Сбросить", "callback_data": f"reset:{pid}"}]
        )

    elif pair.status == PairStatus.READY:
        # Кнопки действий
        buttons.append(
            [
                {"text": "📊 Симуляция", "callback_data": f"sim:{pid}"},
                {"text": "💰 PnL", "callback_data": f"pnl:{pid}"},
            ]
        )
        buttons.append(
            [
                {"text": "🚀 Торговать", "callback_data": f"trade:{pid}"},
                {"text": "🔄 Сбросить", "callback_data": f"reset:{pid}"},
            ]
        )

    elif pair.status == PairStatus.ACTIVE:
        # Кнопки для активной пары
        buttons.append(
            [
                {"text": "📊 Симуляция", "callback_data": f"sim:{pid}"},
                {"text": "💰 PnL", "callback_data": f"pnl:{pid}"},
            ]
        )
        buttons.append(
            [
                {"text": "⏹ Остановить", "callback_data": f"deact:{pid}"},
            ]
        )

    elif pair.status == PairStatus.DISABLED:
        # Кнопка включения
        buttons.append(
            [{"text": "🔄 Включить заново", "callback_data": f"reset:{pid}"}]
        )

    elif pair.status == PairStatus.ERROR:
        # Кнопка сброса
        buttons.append(
            [{"text": "🔄 Сбросить ошибку", "callback_data": f"reset:{pid}"}]
        )

    # Всегда добавляем кнопку закрытия
    buttons.append([{"text": "❌ Закрыть", "callback_data": "close"}])

    return buttons


def build_pairs_list_keyboard(pairs: list[Pair]) -> list[list[dict]]:
    """Build keyboard with Open button for each pair."""
    buttons = []
    for pair in pairs[:20]:  # Лимит 20 пар
        short_id = pair.pair_id[:8]
        pid = pair.pair_id[:16]  # Shortened ID for callback
        emoji = status_emoji(pair.status)
        buttons.append(
            [
                {
                    "text": f"{emoji} {short_id} - {_extract_slug(pair.polymarket_url)}",
                    "callback_data": f"open:{pid}",
                }
            ]
        )

    buttons.append([{"text": "❌ Закрыть", "callback_data": "close"}])
    return buttons


def format_start_message(counts: dict[PairStatus, int], mode: str = "polling") -> str:
    """Format /start welcome message."""
    total = sum(counts.values())
    mode_ru = "опрос" if mode == "polling" else mode
    lines = [
        "🤖 *Arb Core Bot*",
        "",
        f"Режим: `{mode_ru}`",
        f"Всего пар: {total}",
        "",
        format_status_counts(counts),
        "",
        "*Команды:*",
        "/start - Показать статус",
        "/pairs\\_show - Список пар",
    ]
    return "\n".join(lines)


def format_error_pm_first() -> str:
    """Message when user tries to select Opinion before Polymarket."""
    return "⚠️ Сначала выбери исход на Polymarket."


def format_ready_card(pair: Pair) -> str:
    """
    Format READY card with full details including tokens and settings.

    Shows:
    - Links to both markets
    - Selected sides
    - Token IDs (shortened)
    - Position settings
    - Action buttons (via keyboard)
    """
    status_ru = status_name_ru(pair.status)
    lines = [
        "✅ *Пара готова к торговле!*",
        "",
        f"🆔 `{pair.pair_id[:12]}...`",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        "📈 *Polymarket*",
        f"{pair.polymarket_url}",
        f"Исход: *{pair.pm_side}*",
    ]

    if pair.pm_token:
        lines.append(f"Токен: `{pair.pm_token[:20]}...`")

    lines.extend(
        [
            "",
            "🎯 *Opinion*",
            f"{pair.opinion_url}",
            f"Исход: *{pair.op_side}*",
        ]
    )

    if pair.op_token:
        lines.append(f"Токен: `{pair.op_token[:20]}...`")

    lines.extend(
        [
            "",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            "⚙️ *Настройки:*",
            f"  Макс. позиция: *${pair.max_position:.2f}*",
            f"  Мин. профит: *{pair.min_profit_percent:.2f}%*",
            "",
            f"📍 Статус: *{status_ru}*",
        ]
    )

    if pair.error_message:
        lines.append(f"⚠️ Ошибка: {pair.error_message}")

    return "\n".join(lines)


def _extract_slug(url: str) -> str:
    """Extract readable slug from URL."""
    if not url:
        return "N/A"
    # Remove protocol and trailing slash
    url = url.replace("https://", "").replace("http://", "").rstrip("/")
    # Take last path segment
    parts = url.split("/")
    slug = parts[-1] if parts else url
    # Truncate if too long
    if len(slug) > 25:
        slug = slug[:22] + "..."
    return slug


def format_simulation_result(pair: Pair, simulation: "SimulationResult") -> str:
    """
    Format simulation result for Telegram message.

    Shows:
    - Quote prices (PM ask, OP ask)
    - Total cost and payout
    - Expected profit (per share and %)
    - Computed size
    - Tradeable status
    """
    # Handle case when quote has no data (orderbook fetch failed)
    has_quote = simulation.quote and simulation.quote.pm_ask > 0

    lines = [
        "📊 *Результат симуляции*",
        "",
        f"🆔 `{pair.pair_id[:12]}...`",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    if has_quote:
        pm_ask = simulation.quote.pm_ask
        op_ask = simulation.quote.op_ask
        cost_per_share = simulation.quote.cost_per_share
        pm_depth = simulation.quote.pm_depth
        op_depth = simulation.quote.op_depth
        
        # Get sizes
        desired_size = pair.max_position  # From Google Sheet
        actual_size = simulation.size_result.size if simulation.size_result else 0
        
        # Spread calculation
        spread = 1.0 - cost_per_share
        spread_pct = (spread / cost_per_share * 100) if cost_per_share > 0 else 0
        is_spread_positive = spread > 0
        
        # PnL on DESIRED size (what user wants to trade)
        # Fee structure (from Opinion docs):
        # - PM maker: 0%, OP maker: 0%
        # - But OP has $0.50 minimum fee per trade (always applies)
        desired_investment = desired_size * cost_per_share
        desired_payout = desired_size * 1.0
        desired_gross = desired_payout - desired_investment
        
        # Opinion minimum fee: $0.50 (even for makers)
        op_min_fee = 0.50
        desired_net = desired_gross - op_min_fee
        desired_net_pct = (desired_net / desired_investment * 100) if desired_investment > 0 else 0
        is_profitable = desired_net > 0.10  # Require $0.10 min net profit
        
        lines.extend([
            "📈 *Котировки:*",
            f"  PM: ${pm_ask:.4f}",
            f"  OP: ${op_ask:.4f}",
            f"  Сумма: ${cost_per_share:.4f}",
            "",
        ])
        
        spread_emoji = "✅" if is_spread_positive else "❌"
        lines.append(f"{spread_emoji} *Спред: ${spread:.4f} ({spread_pct:.2f}%)*")
        lines.append("")
        
        # Show PnL on desired size
        profit_emoji = "✅" if is_profitable else "❌"
        lines.extend([
            f"📊 *PnL на {desired_size:.0f} шейров:*",
            f"  Инвестиция: ${desired_investment:.2f}",
            f"  Выплата: ${desired_payout:.2f}",
            f"  Валовая: ${desired_gross:.2f}",
            f"  OP мин. комиссия: -${op_min_fee:.2f}",
            f"  {profit_emoji} *Чистая: ${desired_net:.2f} ({desired_net_pct:.1f}%)*",
            "",
        ])
        
        # Calculate minimum profitable size
        # To cover $0.50 fee + $0.10 min profit with current spread
        required_profit = 0.60  # $0.50 fee + $0.10 min
        min_profitable_size = required_profit / spread if spread > 0 else float('inf')
        
        # Show liquidity and requirements
        pm_dollar_available = actual_size * pm_ask
        op_dollar_available = actual_size * op_ask
        
        lines.extend([
            "📦 *Размеры:*",
            f"  Желаемый: {desired_size:.0f} шейров",
            f"  Доступно: {actual_size:.1f} шейров",
            f"  Мин. прибыльный: {min_profitable_size:.1f} шейров",
            "",
            "📊 *Глубина стаканов:*",
            f"  PM: {pm_depth:.1f} шейров",
            f"  OP: {op_depth:.1f} шейров",
            "",
        ])

    # Check if tradeable
    actual_size_for_trade = simulation.size_result.size if simulation.size_result else 0
    can_trade = actual_size_for_trade >= 1.0 and simulation.skip_reason is None
    
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    if can_trade:
        # Calculate actual PnL on tradeable size
        actual_cost = actual_size_for_trade * simulation.quote.cost_per_share
        actual_gross = actual_size_for_trade * 1.0 - actual_cost
        actual_net = actual_gross - 0.50
        is_actually_profitable = actual_net > 0.10
        
        if is_actually_profitable:
            lines.append(f"✅ *Можно торговать {actual_size_for_trade:.1f} шейров!*")
            lines.append(f"   Чистая прибыль: ${actual_net:.2f}")
        else:
            lines.append(f"⚠️ *Размер {actual_size_for_trade:.1f} не прибылен*")
            lines.append(f"   Чистая: ${actual_net:.2f} (нужно > $0.10)")
    else:
        # Get skip reason
        skip_reason = simulation.skip_reason
        if not skip_reason and simulation.size_result:
            skip_reason = simulation.size_result.skip_reason
        skip_reason = skip_reason or "unknown"

        skip_reasons_ru = {
            "not_profitable": "Не прибыльно",
            "missing_tokens": "Токены не выбраны",
            "missing_depth_pm": "Нет глубины PM",
            "missing_depth_op": "Нет глубины OP",
            "insufficient_balance": "Недостаточно баланса",
            "size_below_min": "Размер ниже минимума",
            "invalid_orderbooks": "Ошибка стакана",
        }
        reason_ru = skip_reasons_ru.get(skip_reason, skip_reason)

        # Escape underscores in reason to avoid markdown issues
        reason_ru_escaped = reason_ru.replace("_", "\\_")

        lines.extend([
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            f"⚠️ *Не торгуется:* {reason_ru_escaped}",
        ])

    return "\n".join(lines)


def format_trade_result(pair: Pair, trade: "TradeResult") -> str:
    """
    Format trade execution result for Telegram message.
    """
    if trade.success:
        lines = [
            "🚀 *Сделка выполнена!*",
            "",
            f"🆔 `{pair.pair_id[:12]}...`",
            "",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            "📈 *Polymarket:*",
            f"  Order ID: `{trade.pm_order.order_id[:16]}...`" if trade.pm_order else "  Ошибка",
            f"  Размер: {trade.pm_order.filled_size:.2f}" if trade.pm_order else "",
            f"  Цена: {trade.pm_order.filled_price:.4f}" if trade.pm_order else "",
            "",
            "🎯 *Opinion:*",
            f"  Order ID: `{trade.op_order.order_id[:16]}...`" if trade.op_order else "  Ошибка",
            f"  Размер: {trade.op_order.filled_size:.2f}" if trade.op_order else "",
            f"  Цена: {trade.op_order.filled_price:.4f}" if trade.op_order else "",
            "",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            f"💵 Инвестировано: *${trade.total_invested:.2f}*",
            f"💰 Ожид. прибыль: *${trade.expected_profit:.2f}*",
            f"📈 Ожид. %: *{trade.expected_profit_pct:.2f}%*",
        ]
    else:
        error = trade.error or trade.skip_reason or "Неизвестная ошибка"
        lines = [
            "❌ *Ошибка сделки*",
            "",
            f"🆔 `{pair.pair_id[:12]}...`",
            "",
            f"Причина: _{error}_",
        ]

    return "\n".join(lines)
