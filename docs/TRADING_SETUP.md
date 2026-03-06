# Настройка торговли в arb_core

## Обзор

Для арбитражной торговли нужны учетные данные на двух платформах:
- **Polymarket** - торговля через CLOB API (Polygon сеть)
- **Opinion** - торговля через EIP-712 подписанные ордера (BSC сеть)

---

## Polymarket (Polygon)

### Что нужно:

| Поле | Описание | Как получить |
|------|----------|--------------|
| `private_key` | Приватный ключ кошелька | Экспортировать из MetaMask/кошелька |
| `wallet_address` | Адрес кошелька | Адрес вашего кошелька (0x...) |
| `signature_type` | Тип подписи | 0=EOA (обычный кошелек), 1=Magic/Email, 2=Gnosis Safe |
| `funder_address` | Адрес с балансом | Только если используете прокси-кошелек |

### Опционально (автоматически получаются из private_key):
| Поле | Описание |
|------|----------|
| `api_key` | CLOB API ключ |
| `secret_key` | CLOB API секрет |
| `passphrase` | CLOB API пароль |

### Шаги настройки:

1. **Создать кошелек** (если нет):
   - MetaMask или любой другой Ethereum кошелек
   - Записать приватный ключ

2. **Зарегистрироваться на Polymarket**:
   - Зайти на https://polymarket.com
   - Подключить кошелек
   - Выполнить верификацию (если требуется)

3. **Пополнить баланс USDC**:
   - Перевести USDC на Polygon сеть
   - Адрес: ваш `wallet_address`

4. **Установить разрешения (Allowance)**:
   - При первой торговле система попросит подтвердить разрешения
   - Или вручную через https://polygonscan.com

### Проверка signature_type:

```
0 - Обычный кошелек (MetaMask EOA)
1 - Email/Magic Link вход на Polymarket
2 - Gnosis Safe или смарт-контракт кошелек
```

Если входили через email на polymarket.com → используйте `signature_type: 1`
Если через MetaMask напрямую → используйте `signature_type: 0`

---

## Opinion (BSC)

### Что нужно:

| Поле | Описание | Как получить |
|------|----------|--------------|
| `private_key` | Приватный ключ кошелька | Тот же что для подписи (0x...) |
| `multi_sig_address` | Адрес прокси-кошелька Opinion | Создается при регистрации на Opinion |

### Шаги настройки:

1. **Зарегистрироваться на Opinion**:
   - Зайти на https://app.opinion.trade
   - Подключить кошелек
   - **ВАЖНО**: При первом входе создается Gnosis Safe прокси-кошелек

2. **Получить multi_sig_address**:
   - После регистрации открыть профиль
   - Адрес прокси-кошелька виден в настройках
   - Или через API: `GET /api/v2/user/{wallet_address}/profile`
   - Поле `multiSignedWalletAddress.56`

3. **Пополнить баланс USDT**:
   - Перевести USDT (BEP-20) на адрес `multi_sig_address`
   - НЕ на обычный кошелек, а на прокси-кошелек Opinion!

4. **Включить торговлю**:
   - При первой торговле нужно подтвердить разрешения
   - Это происходит автоматически через интерфейс Opinion

---

## Пример config/accounts.json

```json
{
  "accounts": [
    {
      "account_id": "opinion_acc",
      "exchange": "Opinion",
      "private_key": "0xYOUR_PRIVATE_KEY_HERE",
      "multi_sig_address": "0xYOUR_OPINION_PROXY_WALLET",
      "chain_id": 56,
      "proxy": ""
    },
    {
      "account_id": "poly_acc",
      "exchange": "Polymarket",
      "private_key": "0xYOUR_PRIVATE_KEY_HERE",
      "wallet_address": "0xYOUR_WALLET_ADDRESS",
      "signature_type": 0,
      "funder_address": "",
      "api_key": "",
      "secret_key": "",
      "passphrase": "",
      "proxy": ""
    }
  ]
}
```

---

## Чеклист перед запуском

### Polymarket:
- [ ] Приватный ключ указан в `private_key`
- [ ] Адрес кошелька указан в `wallet_address`
- [ ] `signature_type` соответствует способу входа
- [ ] Есть USDC на Polygon сети

### Opinion:
- [ ] Приватный ключ указан в `private_key`
- [ ] Прокси-адрес Opinion указан в `multi_sig_address`
- [ ] Есть USDT на прокси-кошельке Opinion (BSC)

---

## Тестирование

1. Проверить конфигурацию:
```bash
python -m arb_core.main --health
```

2. Запустить в режиме симуляции (без реальных сделок):
```bash
python -m arb_core.main --dry-run
```

3. Запустить полноценно:
```bash
python -m arb_core.main
```

---

## Безопасность

⚠️ **НИКОГДА не публикуйте приватные ключи!**

- Храните `accounts.json` в безопасном месте
- Добавьте `config/accounts.json` в `.gitignore`
- Используйте отдельный кошелек только для торговли
- Не храните большие суммы на торговых кошельках

---

## FAQ

**Q: Где взять multi_sig_address для Opinion?**  
A: Это адрес создается автоматически при первом входе на opinion.trade. Найти его можно в профиле или через API.

**Q: Можно ли использовать один приватный ключ для обоих?**  
A: Да, если кошелек зарегистрирован на обеих платформах.

**Q: Что если api_key/secret/passphrase пустые для Polymarket?**  
A: Они будут автоматически получены из приватного ключа при первом запуске.

**Q: Минимальный размер ордера?**  
A: Polymarket: ~$1, Opinion: ~$5
