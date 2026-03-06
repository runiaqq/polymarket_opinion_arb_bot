# Arb Core — Covered Arbitrage Bot

Arbitrage bot for prediction markets. Trades the same event on [Polymarket](https://polymarket.com) and [Opinion](https://opinion.trade), locking profit when YES + NO prices add up to less than $1.

---

## Core idea

Binary markets: YES + NO = $1 at resolution.  
If you buy YES at 0.45 and NO at 0.50, you pay $0.95 and receive $1.00. Profit = $0.05 per share.

The bot finds such mispricings across Polymarket and Opinion and places both legs so the outcome is locked in regardless of the result.

---

## How it works

1. **Pair setup** — You pick a market that exists on both platforms (same question/event) and link the YES/NO tokens.
2. **Spread check** — The bot compares orderbooks: if `PM_ask + OP_ask < 1 - fees - min_profit`, it enters.
3. **Trading** — Two modes:
   - **Covered-Arb** — Two limit orders at once; both must fill.
   - **Market-Hedge** — Two limits; when one fills, the other is cancelled and a market order closes the position on the other exchange.
4. **Resolution** — When the event resolves, you get $1 per share. No directional risk.

---

## Quick start

```bash
pip install -r requirements.txt
cp config/accounts.example.json config/accounts.json
cp config/settings.example.yaml config/settings.yaml
# edit both with your API keys, wallets, and Telegram token
python -m arb_core.main --health
python -m arb_core.main --dry-run
```

**Flags:** `--dry-run` (no real orders), `--no-telegram` (no bot), `--market-hedge` (use market-hedge mode).

---

## Requirements

- Python 3.10+
- Accounts on Polymarket (USDC, Polygon) and Opinion (USDT, BSC)
- Optional: Google Sheet for pair list, Telegram for control and alerts

---

## Project layout

```
arb_core/
├── core/          — config, models, store, math
├── market_data/   — orderbook fetch
├── exchanges/     — Polymarket & Opinion clients
├── runners/       — Covered-Arb and Market-Hedge logic
├── integrations/  — Sheets sync, token resolvers
├── ui/            — Telegram bot
└── tests/
```

See `docs/TRADING_SETUP.md` for account setup.

---

# Arb Core — Бот для покрытого арбитража

Бот для арбитражной торговли на рынках предсказаний. Торгует одним и тем же событием на [Polymarket](https://polymarket.com) и [Opinion](https://opinion.trade), фиксируя прибыль, когда сумма цен YES + NO меньше $1.

---

## Идея

Бинарные рынки: YES + NO = $1 при резолюции.  
Если купить YES по 0.45 и NO по 0.50, платим $0.95, получаем $1.00. Прибыль — $0.05 с акции.

Бот находит такие несоответствия между Polymarket и Opinion и выставляет обе ноги так, чтобы исход был зафиксирован при любом результате.

---

## Как устроен бот

1. **Настройка пары** — выбираешь рынок, который есть на обеих площадках (одинаковый вопрос), и связываешь токены YES/NO.
2. **Проверка спреда** — бот сравнивает стаканы: входит, если `PM_ask + OP_ask < 1 - комиссии - мин.прибыль`.
3. **Торговля** — два режима:
   - **Covered-Arb** — два лимитных ордера одновременно; оба должны исполниться.
   - **Market-Hedge** — два лимита; когда один исполняется, второй отменяется и рыночным ордером хеджируется позиция на другой бирже.
4. **Резолюция** — при разрешении события получаешь $1 за акцию. Направленного риска нет.

---

## Быстрый старт

```bash
pip install -r requirements.txt
cp config/accounts.example.json config/accounts.json
cp config/settings.example.yaml config/settings.yaml
# заполни API ключи, кошельки и Telegram
python -m arb_core.main --health
python -m arb_core.main --dry-run
```

**Флаги:** `--dry-run` (без реальных ордеров), `--no-telegram` (без бота), `--market-hedge` (режим market-hedge).

---

## Что нужно

- Python 3.10+
- Аккаунты на Polymarket (USDC, Polygon) и Opinion (USDT, BSC)
- Опционально: Google Таблица со списком пар, Telegram для управления и уведомлений

---

## Структура проекта

```
arb_core/
├── core/          — конфиг, модели, хранилище, расчёты
├── market_data/   — загрузка стаканов
├── exchanges/     — клиенты Polymarket и Opinion
├── runners/       — логика Covered-Arb и Market-Hedge
├── integrations/  — синхронизация Sheets, резолверы токенов
├── ui/            — Telegram-бот
└── tests/
```

Подробнее: `docs/TRADING_SETUP.md`.
