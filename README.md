# 📈 APEX Trading AI — Home Assistant Add-on

<div align="center">

[![Version](https://img.shields.io/badge/version-1.69.19-teal)](https://github.com/sam3gp8/apex-trading-addon/releases)
[![HA Addon](https://img.shields.io/badge/Home%20Assistant-Add--on-41BDF5?logo=home-assistant)](https://www.home-assistant.io/addons/)
[![License](https://img.shields.io/github/license/sam3gp8/apex-trading-addon)](LICENSE)

**Autonomous AI-powered trading bot running permanently inside Home Assistant OS.**  
Rules-based engine · Claude AI adviser · Alpaca 24/5 support · Full tax engine · Real-time SIP streaming

[Installation](#-installation) · [Configuration](#-configuration) · [Features](#-features) · [Changelog](CHANGELOG.md)

</div>

---

## ✨ Features

| Feature | Detail |
|---|---|
| 📐 **Rules-Based Engine** | Pure rules engine handles 95%+ of trades at zero API cost. Claude only called on distress thresholds |
| 🤖 **Claude AI Adviser** | Fires on: 4 consecutive losses, WR < 35%, daily loss > 3%, drawdown > 8%, VIX ≥ 35, or every 24h |
| 📋 **Bot Rules Editor** | Full rules tab — edit every parameter live, Claude auto-optimizes when performance drops |
| 🕐 **Alpaca 24/5** | Full support for pre-market (4AM), after-hours (8PM), and overnight (8PM–4AM) sessions |
| 📡 **Live Market Data** | Alpaca SIP stream (up to 10,000 symbols) + Yahoo Finance, Finnhub, Alpha Vantage as fallbacks |
| 🔍 **Full Universe Scan** | Groq screens ALL available symbols in batches — no 250-symbol cap |
| 🔌 **Broker Integration** | Alpaca Markets (paper + live) — orders placed server-side, no CORS |
| 💰 **Full Balance View** | Cash, long/short market value, margin, buying power — all synced from Alpaca every 10s |
| 📊 **Sortable Tables** | All columns in History, P&L, and Orders are clickable to sort ▲/▼ |
| 💰 **Full Tax Engine** | §1091 wash sales, §1222 ST/LT, §1411 NIIT, §1211/§1212 loss limits — computed from full trade history |
| 🌿 **Harvest Candidates** | Live view of open positions at a loss with exact tax savings if closed |
| 🛡 **Trailing Stops** | Breakeven lock at +3%, 50% profit lock at +6% |
| 🔑 **Key Vault** | API keys survive add-on reinstall — stored in `/config/apex_trading/apex_user.json` |
| 💾 **Persistent Storage** | All data in `/config/apex_trading/` — visible in HA File Editor |
| 📱 **Mobile UI** | Frosted glass bottom nav, 44px touch targets, iPhone notch support |
| 🏠 **HA Ingress** | Appears in HA sidebar, authenticated via HA |

---

## 🚀 Installation

### One-Click (via HA Add-on Repository)

[![Add Repository to HA](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Fsam3gp8%2Fapex-trading-addon)

Or manually:
1. **Settings → Add-ons → Store → ⋮ → Repositories**
2. Add: `https://github.com/sam3gp8/apex-trading-addon`
3. Find **APEX Trading AI** → Install

### SSH / HA Terminal

```bash
curl -fsSL https://raw.githubusercontent.com/sam3gp8/apex-trading-addon/main/install.sh | bash
```

### Manual (Samba)

1. Download the [latest release zip](https://github.com/sam3gp8/apex-trading-addon/releases/latest)
2. Unzip and copy `apex_trading/` to the HA `addons` share
3. **Settings → Add-ons → Store → ⋮ → Check for updates**

---

## ⚙️ Configuration

### Required

| Key | Description |
|---|---|
| `anthropic_api_key` | Anthropic API key — [console.anthropic.com](https://console.anthropic.com) |
| `broker_platform` | `alpaca` |
| `broker_api_url` | `https://paper-api.alpaca.markets` (paper) or `https://api.alpaca.markets` (live) |
| `broker_api_key` / `broker_api_secret` | Alpaca API credentials |

### Trading

| Key | Default | Description |
|---|---|---|
| `trading_mode` | `paper` | `paper` or `live` |
| `stop_loss_pct` | `2.0` | Stop loss % |
| `take_profit_pct` | `6.0` | Take profit % |
| `max_position_pct` | `5.0` | Max % of portfolio per position |
| `daily_loss_limit` | `500.0` | Bot pauses after this daily loss ($) |
| `allow_fractional` | `false` | Enable fractional shares |
| `allow_margin` | `false` | Use broker buying power (includes margin) |

### AI Screener (optional — Groq recommended)

| Provider | Cost | Setup |
|---|---|---|
| `groq` | Free | [console.groq.com](https://console.groq.com) — set `screener_provider: groq` |
| `ollama` | Free (local) | Requires Ollama HACS add-on |
| `disabled` | — | Rules engine still works without screener |

---

## 🤖 How It Works

```
Every 2 min: Groq screens ALL symbols → discovers top candidates (discovery only, no scoring)
                         ↓
Every 3 min: Rules engine scores candidates → generates BUY/SELL/HOLD signals
  • Momentum weight × screener order score
  • ML Pattern weight × Groq score
  • Breakout weight × high-score filter
  • Sentiment weight × Fear/Greed + VIX + Breadth
  • Mean Reversion weight × oversold/overbought proxy
                         ↓
  AI ADVISER fires ONLY when:
  • ≥ 4 consecutive losses
  • Win rate < 35% (with ≥ 10 trades)
  • Today's loss > 3% of portfolio
  • Drawdown > 8% from high-water
  • VIX ≥ 35
  • Every 24 hours (scheduled review)
```

**Result:** ~95% of trading cycles cost $0 in API fees. Claude is reserved for genuine distress.

---

## 🕐 Alpaca 24/5 Session Support

| Session | Hours (ET) | Order Type | APEX |
|---|---|---|---|
| Pre-market | 4:00 AM – 9:30 AM | Limit + `extended_hours: true` | ✅ |
| Regular | 9:30 AM – 4:00 PM | Market | ✅ |
| After-hours | 4:00 PM – 8:00 PM | Limit + `extended_hours: true` | ✅ |
| Overnight | 8:00 PM – 4:00 AM | Limit + `extended_hours: true` | ✅ |
| Weekend | Sat + Sun pre-8PM | Blocked (crypto only) | ✅ |

Assets checked for `overnight_tradable` via Alpaca Assets API before overnight orders. Margin capped at 2x during all extended sessions.

---

## 📂 Data Files

All files at `/config/apex_trading/` (visible in HA File Editor as `/homeassistant/apex_trading/`):

| File | Contents |
|---|---|
| `state.json` | Bot state, positions, prices, strategy weights |
| `trades.json` | Full trade history (used by tax engine and P&L) |
| `analyses.json` | Claude AI analysis reports |
| `optlog.json` | Strategy optimization and rules change log |
| `trading_memory.json` | Bot learning memory |
| `apex_user.json` | Key vault — survives reinstall |

---

## 💡 Projected API Cost

| Usage | Monthly |
|---|---|
| Bot loop (AI adviser, ~1.3 triggers/day) | ~$0.27 |
| Pre-market scan (once/trading day) | ~$0.46 |
| Manual UI scans (5/day) | ~$1.81 |
| Chat console (3/day) | ~$0.63 |
| **Total (normal usage)** | **~$3.17/mo** |

Previous version (Claude every cycle): ~$152/mo. **98% cost reduction.**

---

## 🔒 Live Trading Checklist

Before switching `trading_mode: live`:
- ✅ Run in paper mode for at least one week
- ✅ Set a conservative `daily_loss_limit` (e.g. `200.0`)
- ✅ Start with `max_position_pct: 2.0`
- ✅ Keep `allow_fractional: false` and `allow_margin: false`
- ✅ Verify P&L tab shows expected trades

---

## 🤝 Contributing

Pull requests welcome. Please test in paper mode before submitting.  
[Open an issue](https://github.com/sam3gp8/apex-trading-addon/issues) for bugs or feature requests.

---

## 📄 License

MIT — see [LICENSE](LICENSE)

---

<div align="center">
<sub>Built for Home Assistant OS · Powered by Claude AI · Not financial advice · Use at your own risk</sub>
</div>
