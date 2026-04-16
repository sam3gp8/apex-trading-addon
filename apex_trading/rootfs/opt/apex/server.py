#!/usr/bin/env python3
"""
APEX Trading AI — Home Assistant Add-on Server

Persistence model:
  - options.json  — set in HA Configuration tab, writable via Supervisor API
  - /data/        — HAOS guarantees this persists across restarts for this addon
  - All credentials live in options.json (the only truly reliable store)
  - Runtime data (trades, analyses, state) lives in /data/
"""

import asyncio
import json
import logging
import os
import re as _re
import time
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import web, ClientSession, ClientTimeout

# ── Paths ──────────────────────────────────────────────────────────────────
# Use /config/apex_trading/ so all files are visible at /homeassistant/apex_trading/
# This is the config:rw mount — accessible from HA file editor and SSH at /homeassistant/
DATA_DIR      = Path("/config/apex_trading")
OPTIONS_FILE  = Path("/data/options.json")         # HA Supervisor always writes here
UI_SETTINGS   = Path("/config/apex_trading/apex_settings.json")
USER_VAULT    = Path("/config/apex_trading/apex_user.json")
STATIC_DIR    = Path("/opt/apex/static")
PORT          = 7123
SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")

DATA_DIR.mkdir(parents=True, exist_ok=True)
print(f"[APEX] Data dir: {DATA_DIR} (host: /homeassistant/apex_trading/) | Supervisor token: {'YES' if SUPERVISOR_TOKEN else 'NO'}")

# ── Read options.json ──────────────────────────────────────────────────────
def _load_options() -> Dict[str, Any]:
    DEFAULTS: Dict[str, Any] = {
        "anthropic_api_key":      "",
        "market_data_source":     "yahoo",
        "market_data_key":        "",
        # Multi-source support (data_source_1..5 / data_key_1..5)
        "data_source_1":          "yahoo",
        "data_key_1":             "",
        "data_source_2":          "finnhub",
        "data_key_2":             "",
        "data_source_3":          "polygon",
        "data_key_3":             "",
        "data_source_4":          "alphavantage",
        "data_key_4":             "",
        "data_source_5":          "",
        "data_key_5":             "",
        # Alpaca real-time WebSocket streaming
        # feed: "iex" (free, ~15min delayed) or "sip" (paid $99/mo or $1,089/yr, true real-time)
        # Uses broker_api_key/broker_api_secret automatically when enabled
        "alpaca_stream_enabled":  False,
        "alpaca_stream_feed":     "iex",
        # Secondary AI screener — runs every 2 min, pre-qualifies symbols for Claude
        # Supported providers: "ollama" (local), "openai", "groq", "disabled"
        # Ollama: set screener_url to http://your-host:11434
        # OpenAI/Groq: set screener_api_key
        "screener_provider":      "disabled",  # "ollama" | "openai" | "groq" | "disabled"
        "screener_url":           "http://localhost:11434",  # Ollama base URL
        "screener_model":         "llama3.2",  # Ollama model or openai/groq model name
        "screener_api_key":       "",           # API key for openai/groq
        "screener_interval":      120,          # seconds between screener runs
        "screener_top_n":         75,           # max symbols to pass to Claude per scan
        "screener_min_score":     60,           # min score (0-100) to pass to Claude
        # Brokerage commission structure
        # Alpaca: $0 stocks/ETFs, $0.65/contract options, 0.15-0.25% crypto
        # Coinbase Advanced: 0.40% taker / 0.20% maker
        # Kraken: 0.25% taker / 0.16% maker
        # Set to 0 if your broker is commission-free for equities
        "commission_per_trade":   0.0,    # flat $ fee per trade (equities, most brokers = 0)
        "commission_pct":         0.0,    # % of notional (crypto: 0.25 = 0.25%)
        "commission_min":         0.0,    # minimum commission per trade
        "trading_mode":           "paper",
        "risk_level":             "moderate",
        "tax_bracket":            0.24,
        "stop_loss_pct":          2.0,
        "take_profit_pct":        6.0,
        "max_position_pct":       5.0,
        "daily_loss_limit":       500.0,
        "allow_fractional":       False,
        "allow_shorting":         True,           # allow bot to open SHORT positions
        "allow_margin":           False,
        "auto_analysis_interval": 600,
        "broker_platform":        "",
        "broker_api_url":         "",
        "broker_api_key":         "",
        "broker_api_secret":      "",
        "log_level":              "info",
    }
    for path in [OPTIONS_FILE, Path("/config/apex_trading/options.json"), Path("/data/options.json")]:
        try:
            if path.exists():
                raw = json.loads(path.read_text())
                merged = dict(DEFAULTS)
                for k, dv in DEFAULTS.items():
                    rv = raw.get(k)
                    if rv is None or rv == "": continue
                    try:
                        if isinstance(dv, bool):
                            # bool("False") == True in Python — must handle specially
                            merged[k] = str(rv).lower() in ("true", "1", "yes")
                        elif isinstance(dv, int):
                            merged[k] = int(float(str(rv)))   # "120" or 120.0 → 120
                        elif isinstance(dv, float):
                            merged[k] = float(str(rv))
                        else:
                            merged[k] = str(rv)
                    except (ValueError, TypeError):
                        merged[k] = dv
                # Also pick up any keys in raw not in DEFAULTS (future-proofing)
                for k, v in raw.items():
                    if k not in merged:
                        merged[k] = v
                print(f"[APEX] options.json loaded from {path}")
                # Merge USER_VAULT on top — restores API keys that survive addon reinstall
                # HA overwrites options.json on reinstall; apex_user.json is never touched by HA
                vault_keys = []
                try:
                    vault_path = USER_VAULT
                    if vault_path.exists():
                        vault = json.loads(vault_path.read_text())
                        for k, v in vault.items():
                            if v and v != "" and k in merged:
                                if not merged.get(k):  # only fill blanks
                                    merged[k] = v
                                    vault_keys.append(k)
                        if vault_keys:
                            print(f"[APEX] User vault restored {len(vault_keys)} key(s): {vault_keys}")
                except Exception as ve:
                    print(f"[APEX] User vault read error: {ve}")
                return merged
        except Exception as e:
            print(f"[APEX] Warning: could not read {path}: {e}")
    print("[APEX] Using defaults (no options.json found)")
    return DEFAULTS

OPT = _load_options()
OPT["broker_platform"] = OPT.get("broker_platform", "").strip().lower()

# Write initial vault on startup — captures any keys set via HA config tab
# that might get wiped on next reinstall
def _startup_vault_write():
    """Called once at startup to snapshot current OPT into the vault."""
    vault_path = USER_VAULT
    try:
        existing = {}
        if vault_path.exists():
            try: existing = json.loads(vault_path.read_text())
            except: pass
        # Merge current OPT into vault (non-empty values only)
        for k, v in OPT.items():
            if v not in ("", None, False) or k in ("allow_fractional","allow_margin","alpaca_stream_enabled"):
                existing[k] = v
        vault_path.write_text(json.dumps(existing, indent=2))
        print(f"[APEX] Startup vault snapshot: {vault_path} ({len(existing)} keys)")
    except Exception as e:
        print(f"[APEX] Startup vault write error: {e}")
_startup_vault_write()

# ── Graceful shutdown: flush all pending saves on SIGTERM/SIGINT ──────────
import signal as _signal

def _shutdown_handler(signum, frame):
    log.info("APEX shutdown signal received — flushing all pending saves...")
    _flush_all_saves()
    # Force-write all critical data synchronously
    try: _save_json("trades.json",   TRADES)
    except: pass
    try: _save_json("optlog.json",   OPTLOG[-400:])
    except: pass
    try: _save_json("analyses.json", ANALYSES[:50])
    except: pass
    try:
        state_to_save = {k: v for k, v in STATE.items() if k != "prices"}
        _save_json("state.json", state_to_save)
    except: pass
    log.info("APEX shutdown complete — all data saved to /config/apex_trading/")
    import sys; sys.exit(0)

_signal.signal(_signal.SIGTERM, _shutdown_handler)
_signal.signal(_signal.SIGINT,  _shutdown_handler)

# ── Persist effective config to apex_settings.json on every startup ────────
# This ensures config set via HA config tab also survives as apex_settings
# ── Save options back via Supervisor API (persists across ALL restarts) ────

def _write_user_vault(updates: dict):
    """
    Write ALL current settings to /config/apex_trading/apex_user.json (USER_VAULT).
    This file lives in the HA config volume — survives addon uninstall/reinstall.
    Also writes a backup options.json copy to /config/apex_trading/ so keys can
    be restored even if the Supervisor wipes /data/options.json on reinstall.
    Called every time UI settings are saved.
    """
    vault_path = USER_VAULT
    try:
        # Load existing vault
        existing = {}
        if vault_path.exists():
            try: existing = json.loads(vault_path.read_text())
            except: pass
        # Merge — keep existing values, overwrite with new updates
        existing.update({k: v for k, v in updates.items() if v is not None})
        # Also snapshot entire current OPT so vault is complete
        for k, v in OPT.items():
            if k not in existing and v not in ("", None):
                existing[k] = v
        vault_path.write_text(json.dumps(existing, indent=2))
        print(f"[APEX] User vault updated: {vault_path} ({len(existing)} keys)")
        # Also write a backup options.json to /config/apex_trading/ so it survives reinstall
        try:
            backup_opts = Path("/config/apex_trading/options.json")
            backup_opts.write_text(json.dumps(existing, indent=2))
        except Exception:
            pass
    except Exception as e:
        print(f"[APEX] User vault write failed: {e}")

async def _save_options_via_supervisor(updates: Dict) -> bool:
    """
    Write updated options back to options.json via the Supervisor API.
    This is the ONLY method that guarantees persistence across HAOS restarts.
    """
    if not SUPERVISOR_TOKEN:
        print("[APEX] No SUPERVISOR_TOKEN — cannot save via Supervisor API")
        # Fallback: write directly to options.json
        try:
            current = json.loads(OPTIONS_FILE.read_text()) if OPTIONS_FILE.exists() else {}
            current.update(updates)
            OPTIONS_FILE.write_text(json.dumps(current, indent=2))
            # Also update OPT in memory
            for k, v in updates.items():
                OPT[k] = v
            _write_user_vault(updates)
            print(f"[APEX] Options saved directly to {OPTIONS_FILE}")
            return True
        except Exception as e:
            print(f"[APEX] Failed to write options.json directly: {e}")
            return False

    try:
        # Get current options first
        async with ClientSession() as session:
            async with session.get(
                "http://supervisor/addons/self/options/config",
                headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}"},
                timeout=ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    current_opts = (await r.json()).get("data", {})
                else:
                    current_opts = dict(OPT)

            # Merge and post back
            current_opts.update(updates)
            async with session.post(
                "http://supervisor/addons/self/options",
                headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}",
                         "Content-Type": "application/json"},
                json={"options": current_opts},
                timeout=ClientTimeout(total=10),
            ) as r:
                ok = r.status == 200
                body = await r.text()
                if ok:
                    # Update in-memory OPT too
                    for k, v in updates.items():
                        OPT[k] = v
                    print(f"[APEX] Options saved via Supervisor API: {list(updates.keys())}")
                    # Also persist to vault so keys survive addon reinstall
                    _write_user_vault(updates)
                else:
                    print(f"[APEX] Supervisor API save failed: {r.status} {body[:200]}")
                return ok
    except Exception as e:
        print(f"[APEX] Supervisor API error: {e}")
        # Fallback: write directly
        try:
            current = json.loads(OPTIONS_FILE.read_text()) if OPTIONS_FILE.exists() else {}
            current.update(updates)
            OPTIONS_FILE.write_text(json.dumps(current, indent=2))
            for k, v in updates.items():
                OPT[k] = v
            print(f"[APEX] Options saved via direct write (fallback)")
            return True
        except Exception as e2:
            print(f"[APEX] Direct write also failed: {e2}")
            return False

# ── Logging ────────────────────────────────────────────────────────────────
def _et_now() -> datetime:
    utc = datetime.now(timezone.utc)
    yr  = utc.year
    mar1 = date(yr,3,1)
    dst_start = date(yr,3,1+(6-mar1.weekday())%7+7)
    nov1 = date(yr,11,1)
    dst_end   = date(yr,11,1+(6-nov1.weekday())%7)
    offset = timedelta(hours=-4) if dst_start <= utc.date() < dst_end else timedelta(hours=-5)
    return utc + offset


class _ETFormatter(logging.Formatter):
    """Format log timestamps in US/Eastern time (EST/EDT)."""
    def formatTime(self, record, datefmt=None):
        et = _et_now()
        return et.strftime(datefmt or "%H:%M:%S")

logging.basicConfig(
    level=getattr(logging, OPT["log_level"].upper(), logging.INFO),
    format="%(asctime)s [APEX] %(levelname)s: %(message)s",
)
for handler in logging.root.handlers:
    handler.setFormatter(_ETFormatter("%(asctime)s [APEX] %(levelname)s: %(message)s"))
log = logging.getLogger("apex")
STARTUP_TIME = int(time.time())

log.info("=== APEX Trading AI v%s ===", OPT.get("version", "1.69.39"))
log.info("options.json:   trading_mode=%s market_data=%s broker_platform=%s",
         OPT["trading_mode"], OPT["market_data_source"], OPT["broker_platform"] or "(none)")
log.info("effective:      broker=%s has_claude_key=%s has_broker_key=%s",
         OPT["broker_platform"] or "(none)",
         "YES" if OPT["anthropic_api_key"] else "NO",
         "YES" if OPT["broker_api_key"] else "NO")

# ── Symbol universe ────────────────────────────────────────────────────────
# APEX tracks a configurable set of symbols. Yahoo Finance supports every
# US-listed stock, ETF, mutual fund, index, and crypto — 10,000+ tickers.
# Finnhub and Alpha Vantage support global equities (see market_data_source).
#
# Market data sources:
#   FREE:
#     yahoo        — No key. All US stocks/ETFs/crypto. Real-time delayed 15min.
#                    https://finance.yahoo.com  (just use the ticker)
#     finnhub      — Free key, 60 req/min. US + global stocks, forex, crypto.
#                    https://finnhub.io  (free tier: 60 calls/min)
#     alphavantage — Free key, 25 req/day (500/day paid). US + international.
#                    https://alphavantage.co  (free: 25/day, premium: $50+/mo)
#     polygon_io   — Free key, 5 req/min unlimited history. US stocks/options.
#                    https://polygon.io  (free tier works great for EOD data)
#   PAID (premium real-time):
#     tradier      — $10/mo. Real-time US equities + options chains.
#                    https://tradier.com/products/markets/streaming
#     intrinio     — $10/mo+. Fundamentals, real-time, international.
#                    https://intrinio.com
#     quandl/nasdaq— $0-500/mo. Premium financial datasets.
#                    https://data.nasdaq.com

# Default tracked symbols (user can add any ticker via the UI sidebar)
# Symbols to track — prices initialized to 0 so first Yahoo fetch is always accepted.
# Hardcoded price placeholders were causing the sanity filter to reject real prices
# that had moved since the defaults were written.
DEFAULT_SYMBOLS = {sym: 0.0 for sym in [
    # Index ETFs
    "SPY","QQQ","IWM","DIA","VTI",
    # Mega-cap Tech
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AVGO","AMD","INTC",
    # Finance
    "JPM","BAC","GS","BRK-B","V",
    # Healthcare
    "JNJ","LLY","UNH","PFE","ABBV",
    # Energy
    "XOM","CVX","XLE",
    # Consumer
    "COST","WMT","MCD","NKE",
    # Industrial
    "BA","CAT","HON",
    # Sector ETFs
    "XLF","XLK","XLV","GLD","TLT",
    # High-growth
    "PLTR","COIN","MSTR","RBLX","SOFI","ARM","SMCI","MELI","SHOP","SQ",
    # Crypto
    "BTC/USD","ETH/USD","SOL/USD",
]}
# Remove any accidental duplicates
DEFAULT_SYMBOLS = dict(DEFAULT_SYMBOLS)

# ── Extended US Equity Universe ──────────────────────────────────────────
# Broad liquid stocks for screener and price tracking
# These are fetched via Yahoo batch alongside DEFAULT_SYMBOLS
EXTENDED_SYMBOLS: list = [
    # S&P 500 additions beyond DEFAULT_SYMBOLS
    "LLY","UNH","JNJ","PG","JPM","V","MA","HD","MRK","CVX","ABBV","BAC","PFE",
    "KO","WMT","PEP","TMO","COST","AVGO","MCD","DIS","CSCO","ACN","ABT","DHR",
    "VZ","CRM","NEE","TXN","LIN","PM","MS","BMY","RTX","UPS","SPGI","GS","HON",
    "LOW","ELV","ISRG","MDLZ","T","SYK","ADP","BLK","CI","GILD","REGN","VRTX",
    "ZTS","BSX","CB","ADI","SO","PLD","MMC","DE","SHW","ITW","NOC","EMR","EOG",
    "GD","DG","TJX","MCO","WM","PGR","AON","EW","AMP","TFC","USB","F","GM","GE",
    "WELL","CARR","ETN","A","OXY","FCX","NEM","FDX","NSC","CSX","UNP",
    "HCA","HUM","CVS","MCK","AIG","ALL","PRU","MET","TRV","AFL",
    "PANW","CRWD","FTNT","NET","S","ZS","OKTA","DDOG","SNOW","PLTR",
    "UBER","LYFT","ABNB","DASH","RBLX","RIVN","LCID","NIO","XPEV","LI",
    "BABA","JD","PDD","BIDU","TME","IQ","BILI",
    "SMCI","ARM","AEHR","CAVA","RXRX","IONQ","QUBT","QBTS",
    "XLE","XLF","XLK","XLV","XLI","XLC","XLRE","XLY","XLP","XLB","XLU",
    "GLD","SLV","USO","UNG","TLT","IEF","HYG","LQD",
    "VXX","UVXY","SQQQ","TQQQ","SPXU","UPRO","LABD","LABU",
]


# Symbol metadata for UI display (sector, name)
SYMBOL_INFO = {
    "SPY": ("ETF","S&P 500 ETF"), "QQQ": ("ETF","NASDAQ-100 ETF"),
    "IWM": ("ETF","Russell 2000"), "DIA": ("ETF","Dow Jones ETF"),
    "VTI": ("ETF","Total Market"), "GLD": ("ETF","Gold ETF"),
    "TLT": ("ETF","20yr Treasury"), "XLF": ("ETF","Financials"),
    "XLK": ("ETF","Technology"), "XLV": ("ETF","Healthcare"),
    "XLE": ("ETF","Energy ETF"),
    "AAPL": ("Tech","Apple"), "MSFT": ("Tech","Microsoft"),
    "NVDA": ("Tech","NVIDIA"), "GOOGL": ("Tech","Alphabet"),
    "AMZN": ("Tech","Amazon"), "META": ("Tech","Meta"),
    "TSLA": ("EV","Tesla"), "AVGO": ("Tech","Broadcom"),
    "AMD": ("Tech","AMD"), "INTC": ("Tech","Intel"),
    "PLTR": ("Tech","Palantir"), "ARM": ("Tech","Arm Holdings"),
    "SMCI": ("Tech","Super Micro"), "SHOP": ("Tech","Shopify"),
    "JPM": ("Finance","JPMorgan"), "BAC": ("Finance","Bank of America"),
    "GS": ("Finance","Goldman Sachs"), "BRK-B": ("Finance","Berkshire B"),
    "V": ("Finance","Visa"), "COIN": ("Finance","Coinbase"),
    "SQ": ("Finance","Block"), "SOFI": ("Finance","SoFi"),
    "JNJ": ("Health","Johnson & Johnson"), "LLY": ("Health","Eli Lilly"),
    "UNH": ("Health","UnitedHealth"), "PFE": ("Health","Pfizer"),
    "ABBV": ("Health","AbbVie"),
    "XOM": ("Energy","ExxonMobil"), "CVX": ("Energy","Chevron"),
    "COST": ("Consumer","Costco"), "WMT": ("Consumer","Walmart"),
    "MCD": ("Consumer","McDonald's"), "NKE": ("Consumer","Nike"),
    "MELI": ("Consumer","MercadoLibre"),
    "BA": ("Industrial","Boeing"), "CAT": ("Industrial","Caterpillar"),
    "HON": ("Industrial","Honeywell"),
    "MSTR": ("Crypto","MicroStrategy"), "RBLX": ("Gaming","Roblox"),
    "BTC/USD": ("Crypto","Bitcoin"), "ETH/USD": ("Crypto","Ethereum"),
    "SOL/USD": ("Crypto","Solana"),
}

# Yahoo Finance ticker mapping (crypto needs -USD suffix)

# ── Runtime data (trades, state, etc) — /data persists per HAOS spec ──────
def _load_json(name: str, default: Any) -> Any:
    path = DATA_DIR / name
    try:
        if path.exists() and path.stat().st_size > 2:
            return json.loads(path.read_text())
    except Exception as e:
        log.warning("Could not load %s: %s", name, e)
    return default

def _save_json(name: str, data: Any) -> None:
    try:
        (DATA_DIR / name).write_text(json.dumps(data, indent=2))
    except Exception as e:
        log.error("Could not save %s: %s", name, e)

def _default_state() -> Dict:
    return {
        "botActive": False, "connected": False,
        "portfolio": 0.0, "startPortfolio": 0.0,
        "brokerBalance": None, "balanceFetched": False,
        "cash": 0.0, "buyingPower": 0.0,
        "longMarketValue": 0.0, "shortMarketValue": 0.0,
        "positionMarketValue": 0.0,
        "initMargin": 0.0, "maintMargin": 0.0,
        "regtBP": 0.0, "daytradeBP": 0.0,
        "lastEquity": 0.0, "accruedFees": 0.0,
        "todayPnlPct": 0.0,
        "winCount": 0, "lossCount": 0,
        "todayPnl": 0.0, "todayDate": date.today().isoformat(), "tradesToday": 0,
        "currentSym": "SPY", "currentSig": "HOLD", "confidence": 0,
        "optCycles": 0, "paramAdj": 0, "wrDelta": 0.0,
        "lastPriceFetch": None,
        "stratWeights": {"momentum":0.20,"meanRev":0.15,"breakout":0.20,"sentiment":0.15,"mlPat":0.10,"macro":0.10,"volatility":0.10},
        "stratWR":      {"momentum":0.0,"meanRev":0.0,"breakout":0.0,"sentiment":0.0,"mlPat":0.0,"macro":0.0,"volatility":0.0},
        "stratActive":  {"momentum":True,"meanRev":True,"breakout":False,"sentiment":True,"mlPat":True},
        "taxYear": {"stGains":0.0,"ltGains":0.0,"losses":0.0},
        "lastAIReviewMs": 0,
        "lastAIInterventionMs": 0,
        "portfolioHighWater": 0.0,
        "stopLossCooldown": {},    # sym → ET date string, no reentry same day after stop
        "aiAdviserActive": False,
        "aiAdviserReason": "",
        "rulesEngineCycles": 0,
        "aiAdviserCycles": 0,
        "alpacaDayTradeCount": 0,
        "alpacaDayTradeDate": "",
        "rulesHistory":    [],
        "claudeOverrides": {},
        "riskCfg": {
            "maxPos": f"{OPT['max_position_pct']}%",
            "stopL":  f"{OPT['stop_loss_pct']}%",
            "takeP":  f"{OPT['take_profit_pct']}%",
        },
        "sentData": {"Fear/Greed":62.0,"VIX":16.4,"Put/Call":0.82,"Breadth":74.0},
        "prices": {**dict(DEFAULT_SYMBOLS), **{s: 0.0 for s in EXTENDED_SYMBOLS}},
        "positions": [],
    }

STATE: Dict = _load_json("state.json", None) or _default_state()
if STATE.get("todayDate") != date.today().isoformat():
    STATE.update({"todayDate":date.today().isoformat(),"todayPnl":0.0,"tradesToday":0})
for k, v in _default_state().items():
    STATE.setdefault(k, v)
# Ensure all DEFAULT_SYMBOLS are in the tracked list (adds new ones added to the list)
for sym in DEFAULT_SYMBOLS:
    STATE["prices"].setdefault(sym, 0.0)
# Mark Claude key as configured in state so UI header pills show correctly
# Don't store actual key in state — server reads from OPT directly
STATE["hasAnthropicKey"] = bool(OPT.get("anthropic_api_key"))
STATE["hasBrokerKey"]    = bool(OPT.get("broker_api_key"))
STATE["tradingMode"]     = OPT.get("trading_mode", "paper")  # always sync to state

TRADES:        list = _load_json("trades.json",         [])
FAILED_TRADES: list = _load_json("failed_trades.json",   [])  # broker rejections & preflight blocks
OPTLOG:   list = _load_json("optlog.json",   [])
ANALYSES: list = _load_json("analyses.json", [])

# Debounced saves — coalesces rapid writes into one per 2s
import threading as _threading
_save_timers: dict = {}

def _deferred_write(key: str, fn, delay: float = 2.0):
    """
    Deferred write with configurable delay.
    Critical keys (trades, state) use a short 0.5s delay.
    Other keys use the standard 2s delay.
    """
    _critical = {"trades", "state", "optlog"}
    actual_delay = 0.5 if key in _critical else delay
    if key in _save_timers:
        try: _save_timers[key].cancel()
        except: pass
    t = _threading.Timer(actual_delay, fn)
    t.daemon = True
    _save_timers[key] = t
    t.start()

def _flush_all_saves():
    """Cancel pending timers and run all pending saves immediately (called on shutdown)."""
    for key, timer in list(_save_timers.items()):
        try:
            timer.cancel()
            timer.function()  # run immediately
            log.info("Flushed pending save: %s", key)
        except Exception as e:
            log.error("Flush save %s: %s", key, e)
    _save_timers.clear()

def save_state():
    # Exclude prices dict — it's rebuilt from market data on startup, no need to persist
    state_to_save = {k: v for k, v in STATE.items() if k != "prices"}
    # Write immediately for critical state (positions, portfolio, bot status)
    try:
        _save_json("state.json", state_to_save)
    except Exception as e:
        log.error("save_state immediate write: %s", e)
    _deferred_write("state", lambda: _save_json("state.json", {k: v for k, v in STATE.items() if k != "prices"}))
def save_trades():
    global TRADES, FAILED_TRADES
    if len(TRADES) > 2000: TRADES = TRADES[:2000]
    if len(FAILED_TRADES) > 500: FAILED_TRADES = FAILED_TRADES[:500]
    try: _save_json("trades.json", TRADES[:500])
    except Exception as e: log.error("save_trades: %s", e)
    _deferred_write("trades",   lambda: _save_json("trades.json",   TRADES[:500]))
    _deferred_write("failed_trades", lambda: _save_json("failed_trades.json", FAILED_TRADES[:200]))
    _MEMORY_CACHE["built_at"] = None
def save_optlog():
    global OPTLOG, ANALYSES
    if len(OPTLOG)   > 500:  OPTLOG   = OPTLOG[-400:]   # trim in-memory to prevent unbounded growth
    if len(ANALYSES) > 60:   ANALYSES = ANALYSES[:50]    # trim in-memory
    _deferred_write("optlog",   lambda: _save_json("optlog.json",   OPTLOG[-400:]))
def save_analyses():
    global ANALYSES
    if len(ANALYSES) > 60: ANALYSES = ANALYSES[:50]
    _deferred_write("analyses", lambda: _save_json("analyses.json", ANALYSES[:50]))

def save_all_immediate():
    """Force-flush all pending saves (used on shutdown)."""
    for key, t in list(_save_timers.items()):
        try: t.cancel()
        except: pass
    state_to_save = {k: v for k, v in STATE.items() if k != "prices"}
    _save_json("state.json",    state_to_save)
    _save_json("trades.json",   TRADES[:500])
    _save_json("optlog.json",   OPTLOG[-400:])
    _save_json("analyses.json", ANALYSES[:50])

# ── Market hours ────────────────────────────────────────────────────────────
CRYPTO_SYMBOLS = {"BTC/USD","ETH/USD","BTC-USD","ETH-USD","BTCUSD","ETHUSD"}
US_HOLIDAYS = {
    date(2025,1,1),date(2025,1,20),date(2025,2,17),date(2025,4,18),
    date(2025,5,26),date(2025,6,19),date(2025,7,4),date(2025,9,1),
    date(2025,11,27),date(2025,12,25),
    date(2026,1,1),date(2026,1,19),date(2026,2,16),date(2026,4,3),
    date(2026,5,25),date(2026,6,19),date(2026,7,3),date(2026,9,7),
    date(2026,11,26),date(2026,12,25),
}

def market_status(symbol: str = "") -> Dict:
    if symbol.upper() in CRYPTO_SYMBOLS or "/" in symbol:
        return {"open":True,"session":"crypto","message":"Crypto markets trade 24/7"}
    et = _et_now()
    wd = et.weekday()
    hm = et.hour*60 + et.minute

    if wd >= 5:
        # Saturday (5): fully closed all day
        if wd == 5:
            return {"open":False,"session":"weekend",
                    "message":"Saturday — markets closed. Equities resume Sunday 8PM ET (Alpaca 24/5)"}
        # Sunday (6): closed until 8PM ET (hm=1200), then overnight_extended opens
        if wd == 6:
            if hm >= 1200:  # Sunday 8PM ET → 24/5 overnight_extended begins
                return {"open":False,"session":"overnight_extended",
                        "message":"Sunday overnight (8PM ET) — Alpaca 24/5 active. Limit orders only.",
                        "extended_tradeable": True}
            return {"open":False,"session":"weekend",
                    "message":f"Sunday — markets open tonight at 8PM ET (Alpaca 24/5). {1200-hm}min away."}

    if et.date() in US_HOLIDAYS:
        return {"open":False,"session":"holiday","message":f"US market holiday ({et.date()})"}

    # ── Alpaca 24/5 session map (all times ET) ────────────────────────────
    # 8:00 PM – 4:00 AM  → overnight_extended (limit orders, extended_hours=true)
    # 4:00 AM – 9:30 AM  → pre_market         (limit orders, extended_hours=true)
    # 9:30 AM – 4:00 PM  → regular            (market orders)
    # 4:00 PM – 8:00 PM  → after_hours        (limit orders, extended_hours=true)
    # Note: hm=0 is midnight, hm=240 is 4AM, hm=570 is 9:30AM,
    #       hm=960 is 4PM,    hm=1200 is 8PM

    if 570 <= hm < 960:    # 9:30 AM – 4:00 PM
        return {"open":True, "session":"regular",
                "message":f"Market OPEN — closes in {960-hm}min",
                "closes_in_minutes": 960-hm}

    if 240 <= hm < 570:    # 4:00 AM – 9:30 AM
        return {"open":False, "session":"pre_market",
                "message":f"Pre-market (4AM-9:30AM) — opens in {570-hm}min",
                "opens_in_minutes": 570-hm,
                "extended_tradeable": True}

    if 960 <= hm < 1200:   # 4:00 PM – 8:00 PM
        return {"open":False, "session":"after_hours",
                "message":"After-hours (4PM-8PM ET) — limit orders via Alpaca",
                "extended_tradeable": True}

    # 8:00 PM – midnight  OR  midnight – 4:00 AM  →  overnight_extended
    return {"open":False, "session":"overnight_extended",
            "message":"Overnight session (8PM-4AM ET) — Alpaca 24/5 limit orders available",
            "extended_tradeable": True}

# ── Market data ────────────────────────────────────────────────────────────
# Yahoo Finance: any valid US ticker works. Crypto uses -USD suffix.
# For international stocks append the exchange: "VOD.L" (London), "7203.T" (Tokyo)
YAHOO_MAP = {sym: sym.replace("/USD", "-USD") for sym in DEFAULT_SYMBOLS}

# Finnhub symbol mapping (crypto uses exchange prefix)
FINNHUB_MAP = {
    "BTC/USD": "BINANCE:BTCUSDT",
    "ETH/USD":  "BINANCE:ETHUSDT",
    "SOL/USD":  "BINANCE:SOLUSDT",
}

async def _yahoo(session, sym):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{YAHOO_MAP.get(sym,sym)}?interval=1m&range=1d"
        async with session.get(url,timeout=ClientTimeout(total=8),headers={"User-Agent":"Mozilla/5.0"}) as r:
            if r.status==200:
                d=json.loads(await r.text())
                p=d.get("chart",{}).get("result",[{}])[0].get("meta",{}).get("regularMarketPrice")
                return float(p) if p and float(p)>0 else None
    except Exception as e: log.debug("Yahoo %s: %s",sym,e)
    return None

async def _polygon(session, sym, key):
    """
    Polygon.io — free tier: 5 req/min, unlimited history, real-time delayed.
    Best for: previous close prices, historical data.
    Sign up: https://polygon.io (free key, no credit card)
    """
    try:
        # Use previous close endpoint (works on free tier)
        ticker = sym.replace("/USD","").replace("/","")  # BTC/USD -> BTC
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={key}"
        async with session.get(url, timeout=ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                results = d.get("results", [])
                if results:
                    return float(results[0].get("c", 0)) or None  # closing price
    except Exception as e:
        log.debug("Polygon %s: %s", sym, e)
    return None

async def _finnhub(session, sym, key):
    try:
        ticker = FINNHUB_MAP.get(sym, sym)
        url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={key}"
        async with session.get(url,timeout=ClientTimeout(total=6)) as r:
            if r.status==200:
                d=json.loads(await r.text()); c=d.get("c",0)
                return float(c) if c and float(c)>0 else None
    except Exception as e: log.debug("Finnhub %s: %s",sym,e)
    return None

async def _alphavantage(session, sym, key):
    try:
        if "/" in sym:
            ticker = {"BTC/USD":"BTC","ETH/USD":"ETH"}.get(sym,sym)
            url = f"https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol={ticker}&market=USD&apikey={key}"
        else:
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={sym}&apikey={key}"
        async with session.get(url,timeout=ClientTimeout(total=12)) as r:
            if r.status==200:
                d=json.loads(await r.text())
                if "Note" in d or "Information" in d: return None
                if "/" in sym:
                    ts=d.get("Time Series (Digital Currency Daily)",{})
                    if ts:
                        p=ts[sorted(ts.keys())[-1]].get("4a. close (USD)")
                        return float(p) if p else None
                else:
                    p=d.get("Global Quote",{}).get("05. price")
                    return float(p) if p else None
    except Exception as e: log.debug("AV %s: %s",sym,e)
    return None

# ── Alpaca WebSocket real-time data stream ─────────────────────────────────
# Connects to Alpaca's market data WebSocket and pushes prices into STATE
# as they arrive. Runs as a persistent background task alongside price_loop.
#
# Setup:
#   1. In HA Configuration tab set alpaca_stream_enabled: true
#   2. Set alpaca_stream_feed: "iex" (free) or "sip" ($99/mo real SIP feed)
#   3. Uses your existing broker_api_key and broker_api_secret automatically
#
# Feeds:
#   iex — Free. Included with all Alpaca accounts (paper + live).
#          Data from IEX exchange only. ~15min delayed for non-IEX stocks
#          but real-time for IEX-routed trades (covers ~40% of US volume).
#          wss://stream.data.alpaca.markets/v2/iex
#
#   sip — Paid ($99/mo or $1,089/yr add-on). True consolidated tape from ALL US exchanges.
#          Full NBBO, every trade and quote in real-time.
#          wss://stream.data.alpaca.markets/v2/sip
#
# Crypto (always free real-time):
#   wss://stream.data.alpaca.markets/v1beta3/crypto/us

_alpaca_ws_connected = False
_alpaca_ws_symbols: set = set()

async def alpaca_stream_loop():
    """
    Persistent WebSocket connection to Alpaca market data stream.
    Handles: connect → auth → subscribe → receive prices → reconnect on drop.

    Setup:
      1. In HA Configuration tab: alpaca_stream_enabled: true
      2. Set alpaca_stream_feed: "iex" (free) or "sip" ($99/mo)
      3. Uses your broker_api_key/broker_api_secret automatically
    """
    global _alpaca_ws_connected, _alpaca_ws_symbols

    while True:
        if not OPT.get("alpaca_stream_enabled", False):
            await asyncio.sleep(30)
            continue

        api_key    = OPT.get("broker_api_key", "").strip()
        api_secret = OPT.get("broker_api_secret", "").strip()
        feed       = OPT.get("alpaca_stream_feed", "iex").strip().lower()

        if not api_key or not api_secret:
            await asyncio.sleep(60)
            continue

        equity_syms = [s for s in STATE["prices"].keys()
                       if "/" not in s and "-USD" not in s and s not in CRYPTO_SYMBOLS]

        ws_url = f"wss://stream.data.alpaca.markets/v2/{feed}"
        log.info("Alpaca stream: connecting → %s for %d symbols", ws_url, len(equity_syms))
        STATE["alpacaStreamStatus"] = f"connecting ({feed})"
        _alpaca_ws_connected = False

        try:
            import ssl as _ssl
            ssl_ctx = _ssl.create_default_context()
            # Alpaca data stream: auth is sent as first JSON message after connection
            # (NOT as HTTP headers — that's broker API only)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    ws_url,
                    ssl=ssl_ctx,
                    heartbeat=30,
                    timeout=aiohttp.ClientWSTimeout(ws_close=30),
                    headers={
                        "User-Agent": "APEX-Trading/1.0",
                    },
                    max_msg_size=0,           # no message size limit
                    compress=False,           # disable per-message compression
                    autoclose=True,
                    autoping=True,
                ) as ws:
                    # Alpaca sends [{"T":"success","msg":"connected"}] immediately on connect
                    # Then we auth, then subscribe
                    log.info("Alpaca stream: WebSocket open — waiting for connected message")
                    # Step 1: Authenticate (Alpaca data stream uses JSON auth, not HTTP headers)
                    await ws.send_str(json.dumps({
                        "action": "auth",
                        "key":    api_key,
                        "secret": api_secret,
                    }))

                    # Step 2: Process messages
                    subscribed = False
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                items = json.loads(msg.data)
                                if not isinstance(items, list):
                                    items = [items]
                                for item in items:
                                    t = item.get("T","")

                                    if t == "success" and item.get("msg") == "connected":
                                        log.debug("Alpaca stream: connection confirmed")

                                    elif t == "success" and item.get("msg") == "authenticated":
                                        _alpaca_ws_connected = True
                                        STATE["alpacaStreamStatus"] = f"authenticated ({feed})"
                                        log.info("Alpaca stream: authenticated ✓")
                                        # Subscribe based on feed tier:
                                        # SIP ($99/mo): wildcard "*" = all symbols, unlimited
                                        # IEX (free):  explicit list only, max 30 symbols
                                        # Error 405 from Alpaca = "symbol limit exceeded"
                                        # which means wildcard on IEX is not permitted.
                                        if not subscribed:
                                            if feed == "sip":
                                                sub_symbols = ["*"]
                                                log.info("Alpaca stream (SIP): subscribing to * (all symbols)")
                                            else:
                                                # IEX free: subscribe to top 30 most important symbols
                                                # Prioritize: index ETFs, mega-cap tech, crypto
                                                priority = ["SPY","QQQ","IWM","DIA","AAPL","MSFT","NVDA",
                                                           "GOOGL","AMZN","META","TSLA","AMD","AVGO","V",
                                                           "JPM","BRK-B","JNJ","LLY","XOM","COST",
                                                           "PLTR","COIN","MSTR","SHOP","SOFI","ARM",
                                                           "GLD","TLT","XLF","XLK"]
                                                # Include any tracked symbols not in priority list
                                                tracked = [s for s in equity_syms if s not in priority]
                                                sub_symbols = (priority + tracked)[:30]
                                                log.info("Alpaca stream (IEX free): subscribing to %d symbols", len(sub_symbols))
                                            await ws.send_str(json.dumps({
                                                "action": "subscribe",
                                                "trades": sub_symbols,
                                                "quotes": [],
                                                "bars":   [],
                                            }))
                                            subscribed = True

                                    elif t == "subscription":
                                        trades_list = item.get("trades", [])
                                        trades_count = len(trades_list)
                                        # Wildcard "*" returns ["*"] = 1 entry but streams ALL symbols
                                        if trades_list == ["*"] or "*" in trades_list:
                                            STATE["alpacaStreamStatus"] = f"live ({feed}) · ALL symbols (wildcard)"
                                            log.info("Alpaca stream: live — wildcard * streaming ALL symbols on %s", feed.upper())
                                        else:
                                            STATE["alpacaStreamStatus"] = f"live ({feed}) · {trades_count} symbols"
                                            log.info("Alpaca stream: live — %d symbol(s) streaming", trades_count)

                                    elif t == "error":
                                        code = item.get("code", 0)
                                        emsg = item.get("msg", "")
                                        log.error("Alpaca stream error %d: %s", code, emsg)
                                        STATE["alpacaStreamStatus"] = f"error {code}: {emsg}"
                                        if code == 402:
                                            log.error("Alpaca stream 402: subscription not permitted for %s feed. "
                                                      "Check your Alpaca plan — SIP requires $99/mo add-on.", feed)
                                        elif code == 403:
                                            log.error("Alpaca stream 403: auth failed — check broker_api_key and broker_api_secret")
                                        # Don't permanently stop — retry after backoff
                                        # so user can fix credentials without restarting addon

                                    elif t == "t":  # trade execution — most accurate real-time price
                                        sym   = item.get("S","")
                                        price = float(item.get("p", 0) or 0)
                                        # Accept ALL SIP symbols up to 5000 soft cap
                                        if sym and price > 0:
                                            prev = STATE["prices"].get(sym, 0)
                                            if not prev or abs(price-prev)/prev < 0.40:
                                                if len(STATE["prices"]) < 10000 or sym in STATE["prices"]:
                                                    STATE["prices"][sym] = round(price, 4)
                                                    STATE["lastPriceFetch"] = _et_now().isoformat()
                                                    STATE["lastStreamUpdate"] = _et_now().isoformat()

                                    elif t == "q":  # quote update — NBBO bid/ask midpoint
                                        sym = item.get("S","")
                                        bid = float(item.get("bp", 0) or 0)
                                        ask = float(item.get("ap", 0) or 0)
                                        # Quote: fill gaps where no trade price exists yet
                                        if sym and bid > 0 and ask > 0:
                                            existing = STATE["prices"].get(sym, 0)
                                            if not existing and len(STATE["prices"]) < 5000:
                                                mid = round((bid + ask) / 2, 4)
                                                STATE["prices"][sym] = mid
                                                STATE["lastPriceFetch"] = _et_now().isoformat()
                                                STATE["lastStreamUpdate"] = _et_now().isoformat()

                            except (json.JSONDecodeError, ValueError):
                                pass

                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            log.warning("Alpaca stream: WebSocket %s — reconnecting", msg.type)
                            break

        except aiohttp.WSServerHandshakeError as e:
            # HTTP-level error during WebSocket upgrade (405, 403, etc.)
            status = getattr(e, 'status', 0)
            if status == 405:
                log.error("Alpaca stream 405: WebSocket upgrade rejected. "
                          "Possible cause: paper account key used with wrong feed, "
                          "or network proxy blocking WebSocket upgrades. "
                          "URL: %s", ws_url)
                STATE["alpacaStreamStatus"] = f"error 405: WS upgrade rejected — check feed tier vs account type"
            elif status == 403:
                log.error("Alpaca stream 403: forbidden — check API key permissions. URL: %s", ws_url)
                STATE["alpacaStreamStatus"] = "error 403: forbidden — check API key"
            elif status == 401:
                log.error("Alpaca stream 401: unauthorized — invalid API key or secret")
                STATE["alpacaStreamStatus"] = "error 401: invalid API key"
            else:
                log.warning("Alpaca stream handshake error %s: %s", status, e)
                STATE["alpacaStreamStatus"] = f"error {status}: handshake failed"
        except aiohttp.ClientConnectorError as e:
            log.warning("Alpaca stream: cannot connect: %s", e)
            STATE["alpacaStreamStatus"] = "error: cannot connect (network?)"
        except asyncio.TimeoutError:
            log.warning("Alpaca stream: connection timeout")
            STATE["alpacaStreamStatus"] = "timeout — reconnecting"
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning("Alpaca stream: %s — retrying in 30s", type(e).__name__, e)
            STATE["alpacaStreamStatus"] = f"error: {type(e).__name__}"
        finally:
            _alpaca_ws_connected = False
            prev = STATE.get("alpacaStreamStatus","")
            if not any(x in prev for x in ("error", "disabled")):
                STATE["alpacaStreamStatus"] = "reconnecting..."

        await asyncio.sleep(30)


def _get_active_sources() -> list:
    """
    Build the list of active (source, key) pairs from options.
    Supports both new multi-source format (data_source_1..5)
    and legacy single-source format (market_data_source).
    Deduplicates: if same source appears twice with different keys, uses first key.
    Returns up to 5 active sources, skipping blanks and sim.
    """
    sources = []
    seen_sources = set()

    # New multi-source format
    for i in range(1, 6):
        src = OPT.get(f"data_source_{i}", "").strip().lower()
        key = OPT.get(f"data_key_{i}", "").strip()
        if not src or src in ("", "sim"): continue
        if src == "yahoo":
            if "yahoo" not in seen_sources:
                sources.append(("yahoo", ""))
                seen_sources.add("yahoo")
        elif key:
            if src not in seen_sources:
                sources.append((src, key))
                seen_sources.add(src)
            else:
                log.debug("Skipping duplicate source: %s", src)

    # Legacy fallback: market_data_source / market_data_key
    if not sources:
        legacy_src = OPT.get("market_data_source","yahoo").strip().lower()
        legacy_key = OPT.get("market_data_key","").strip()
        if legacy_src and legacy_src != "sim":
            sources.append((legacy_src, legacy_key))

    # Always include Yahoo as a fallback if nothing else configured
    if not sources:
        sources.append(("yahoo", ""))

    return sources



async def _yahoo_batch(session, symbols: list) -> Dict[str, float]:
    """
    Fetch prices for up to 1500 symbols in ONE Yahoo Finance API call.
    Replaces individual per-symbol requests — much faster and more reliable.
    Yahoo v7/finance/quote supports comma-separated symbols (no auth needed).
    """
    results = {}
    if not symbols: return results
    # Yahoo uses different tickers for some symbols (crypto, etc.)
    mapped = [YAHOO_MAP.get(s, s) for s in symbols]
    sym_map = {YAHOO_MAP.get(s, s): s for s in symbols}  # reverse map

    # Batch in chunks of 100 (Yahoo batch limit for reliability)
    CHUNK = 100
    for i in range(0, len(mapped), CHUNK):
        chunk = mapped[i:i+CHUNK]
        symbols_str = ",".join(chunk)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_str}&fields=regularMarketPrice"
        try:
            _yahoo_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://finance.yahoo.com/",
                "Origin": "https://finance.yahoo.com",
            }
            # Try query2 as primary (less rate-limited), fall back to query1
            _yurl_primary = url.replace("query1.", "query2.")
            async with session.get(_yurl_primary, timeout=ClientTimeout(total=15),
                                   headers=_yahoo_headers) as r:
                if r.status == 200:
                    data = json.loads(await r.text())
                    quotes = data.get("quoteResponse", {}).get("result", [])
                    for q in quotes:
                        ticker  = q.get("symbol", "")
                        price   = float(q.get("regularMarketPrice") or 0)
                        orig    = sym_map.get(ticker, ticker)
                        if price > 0:
                            results[orig] = round(price, 4)
                elif r.status == 429:
                    log.warning("Yahoo batch rate limited (429) — backing off 10s")
                    await asyncio.sleep(10)
                else:
                    log.debug("Yahoo batch HTTP %d for chunk %d", r.status, i//CHUNK)
        except Exception as e:
            log.debug("Yahoo batch chunk %d error: %s", i//CHUNK, e)
        # Small delay between chunks to be polite
        if i + CHUNK < len(mapped):
            await asyncio.sleep(0.3)
    return results

async def _fetch_from_source(session, src: str, key: str, symbols: list) -> Dict[str, float]:
    """
    Fetch prices for all symbols from one source.
    Yahoo: uses batch API (one request for all symbols — fast, no rate limit issues).
    Others: individual requests with appropriate rate-limit delays.
    """
    results = {}
    if not symbols: return results

    # ── Yahoo: single batch request for ALL symbols ───────────────────────
    if src == "yahoo":
        return await _yahoo_batch(session, symbols)

    # ── Other sources: individual requests with rate limiting ─────────────
    delays = {
        "finnhub":      1.05,   # 60/min free tier
        "polygon":      12.1,   # 5/min free tier
        "alphavantage": 13.0,   # 5/min free tier
        "tradier":      0.5,
    }
    delay = delays.get(src, 1.0)
    rate_limited = {"alphavantage": 25, "polygon": 30}
    fetch_symbols = symbols[:rate_limited[src]] if src in rate_limited else symbols

    for sym in fetch_symbols:
        price = None
        try:
            if   src == "finnhub":      price = await _finnhub(session, sym, key)
            elif src == "polygon":
                # Polygon free tier has no reliable crypto data — skip to avoid conflicts
                if "/" in sym or sym.upper().endswith(("-USD", "USD")):
                    continue
                price = await _polygon(session, sym, key)
            elif src == "alphavantage": price = await _alphavantage(session, sym, key)
            elif src == "tradier":      price = await _tradier(session, sym, key)
        except Exception as e:
            log.debug("Source %s / %s: %s", src, sym, e)
        if price and price > 0:
            results[sym] = price
        await asyncio.sleep(delay)

    return results


async def _tradier(session, sym: str, key: str):
    """
    Tradier — $10/mo real-time US equities + options.
    Sign up: https://tradier.com/products/markets/streaming
    """
    try:
        url = f"https://api.tradier.com/v1/markets/quotes?symbols={sym}"
        async with session.get(url, timeout=ClientTimeout(total=8),
                               headers={"Authorization": f"Bearer {key}",
                                        "Accept": "application/json"}) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                q = d.get("quotes", {}).get("quote", {})
                if isinstance(q, list): q = q[0]
                price = q.get("last") or q.get("close")
                return float(price) if price else None
    except Exception as e:
        log.debug("Tradier %s: %s", sym, e)
    return None


def _median_price(prices: list) -> float:
    """Return the median of a list of prices (ignores outliers better than mean)."""
    if not prices: return 0.0
    s = sorted(prices)
    n = len(s)
    return s[n//2] if n % 2 else (s[n//2-1] + s[n//2]) / 2


async def fetch_all_prices() -> Dict:
    """
    Fetch prices from ALL configured sources simultaneously.
    Priority: Alpaca SIP stream (real-time) > polling sources (Yahoo, Finnhub, etc.)
    - If Alpaca SIP/IEX stream is live, equity prices come from the stream directly.
    - Polling sources supplement crypto and fill gaps.
    - For each symbol: collects all source prices, takes median for sanity.
    """
    active = _get_active_sources()

    if not active or (len(active)==1 and active[0][0]=="sim"):
        return {"fetched":0,"failed":0,"source":"sim","prices":STATE["prices"]}

    symbols = list(STATE["prices"].keys())

    # ── SIP primary check — skip equity polling when SIP is streaming ────
    # SIP provides real-time equity prices. When live and market open,
    # only poll crypto (SIP equity stream) and fill gaps for zero-price symbols.
    stream_live = _alpaca_ws_connected and OPT.get("alpaca_stream_enabled", False)
    mkt_open    = market_status().get("open", False)
    if stream_live and mkt_open:
        # Filter to crypto + zero-price symbols only
        crypto_syms = [s for s in symbols if "/" in s or "-USD" in s.upper()]
        zero_syms   = [s for s in symbols if not STATE["prices"].get(s)]
        poll_syms   = list(set(crypto_syms + zero_syms[:50]))  # cap zero-fills at 50/cycle
        if not poll_syms:
            log.debug("fetch_all_prices: SIP live + market open — no poll needed")
            return {"fetched": 0, "failed": 0, "source": "sip_primary",
                    "prices": STATE["prices"], "sip_primary": True}
        symbols = poll_syms
        log.debug("fetch_all_prices: SIP live — polling crypto+gaps only (%d syms)", len(symbols))

    source_names = [s[0] for s in active]
    log.info("Fetching %d symbols from %d source(s): %s", len(symbols), len(active), source_names)

    # Run all sources concurrently
    connector = aiohttp.TCPConnector(limit=10, ssl=True)
    async with ClientSession(connector=connector, timeout=ClientTimeout(total=300)) as session:
        tasks = [
            _fetch_from_source(session, src, key, symbols)
            for src, key in active
        ]
        source_results = await asyncio.gather(*tasks, return_exceptions=True)

    # source_results[i] = dict of {sym: price} from active[i]
    # Collect all prices per symbol across sources
    sym_prices: Dict[str, list] = {sym: [] for sym in symbols}
    source_stats = {}

    for i, result in enumerate(source_results):
        src_name = active[i][0]
        if isinstance(result, Exception):
            log.warning("Source %s failed entirely: %s", src_name, result)
            source_stats[src_name] = {"fetched": 0, "failed": len(symbols)}
            continue
        fetched_count = 0
        for sym, price in result.items():
            if price and price > 0:
                sym_prices[sym].append((src_name, price))
                fetched_count += 1
        source_stats[src_name] = {"fetched": fetched_count, "failed": len(symbols)-fetched_count}

    # Merge: for each symbol, take median of all source prices
    # Drop any price that deviates more than 2% from median (data quality guard)
    final_prices = {}
    conflicts = []
    failed = []

    for sym in symbols:
        readings = sym_prices[sym]
        if not readings:
            failed.append(sym)
            continue
        if len(readings) == 1:
            final_prices[sym] = readings[0][1]
            continue

        raw_values = [p for _, p in readings]
        median = _median_price(raw_values)

        # Filter out readings > 2% from median (catches stale/bad data)
        clean = [(src, p) for src, p in readings if abs(p - median) / median < 0.02]
        if not clean:
            clean = readings  # if all disagree, use all anyway

        # Final price = median of clean readings
        final_price = _median_price([p for _, p in clean])

        if len(readings) > 1:
            sources_used = [s for s, _ in clean]
            spread_pct = (max(p for _,p in clean) - min(p for _,p in clean)) / final_price * 100
            if spread_pct > 0.5:
                conflicts.append(f"{sym}: spread={spread_pct:.2f}% sources={readings}")
            log.debug("Price %s: median=$%.4f from %d sources (%s)",
                      sym, final_price, len(clean), sources_used)

        final_prices[sym] = round(final_price, 4)

    # Sanity filter: don't store a price that moved >40% from previous known price
    # This catches stale/wrong Yahoo weekend data before it triggers false stops
    sane_prices = {}
    rejected = []
    for sym, new_price in final_prices.items():
        prev = STATE["prices"].get(sym, 0)
        if prev and prev > 0 and new_price > 0:
            change_pct = abs(new_price - prev) / prev
            if change_pct > 0.40:
                log.warning("Price rejected (%.1f%% move): %s $%.4f → $%.4f (keeping previous)",
                            change_pct * 100, sym, prev, new_price)
                rejected.append(sym)
                sane_prices[sym] = prev  # keep previous price
            else:
                sane_prices[sym] = new_price
        else:
            # prev is 0 (uninitialized) or new price invalid — accept new price
            if new_price > 0:
                sane_prices[sym] = new_price

    if rejected:
        log.warning("Rejected %d prices as likely bad data: %s", len(rejected), rejected)

    if sane_prices:
        STATE["prices"].update(sane_prices)
        STATE["lastPriceFetch"] = datetime.now().isoformat()
        save_state()

    total_fetched = len(final_prices)
    total_failed  = len(failed)

    # Build per-source summary for logging
    src_summary = " | ".join(
        f"{s}:{stats['fetched']}ok/{stats['failed']}fail"
        for s, stats in source_stats.items()
    )
    log.info("Prices: %d fetched, %d failed | Sources: %s", total_fetched, total_failed, src_summary)
    if conflicts:
        log.debug("Price conflicts resolved: %s", conflicts[:3])

    return {
        "fetched":      total_fetched,
        "failed":       total_failed,
        "sources":      source_names,
        "source_stats": source_stats,
        "prices":       STATE["prices"],
        "timestamp":    STATE.get("lastPriceFetch"),
        "conflicts":    len(conflicts),
    }

# ── Broker ─────────────────────────────────────────────────────────────────
def _clean_url(u): return (u or "").strip().rstrip("/")
def _alpaca_base(u):
    b = _clean_url(u) or "https://paper-api.alpaca.markets"
    return b if b.endswith("/v2") else b+"/v2"

ALPACA_FRAC_CACHE: Dict[str,bool] = {}

# ══════════════════════════════════════════════════════════════════════════════
#  BROKER RULES ENGINE — v1.69.21
#  Each broker's trading rules, limits, and constraints enforced pre-order.
#  Rules are broker-specific — only the active platform's rules apply.
#  Returns (ok: bool, reason: str) for every pre-flight check.
# ══════════════════════════════════════════════════════════════════════════════



# ══════════════════════════════════════════════════════════════════════════════
#  BROKER RULES ENGINE  v2 — Live constraint fetching + hardcoded fallbacks
#
#  Architecture:
#   1. On first order attempt for a broker/symbol, fetch live constraints from
#      the broker's API (exchangeInfo, assets endpoint, account config, etc.)
#   2. Cache the result — subsequent orders use cached constraints instantly
#   3. Hardcoded rules serve as fallback if the API fetch fails
#   4. Unknown future brokers: APEX probes common API endpoints and uses Claude
#      to interpret the response into a constraint dict
#   5. Only the active platform's rules ever execute — fully isolated
#
#  Supported with live constraint fetching:
#    alpaca, binance, kraken, coinbase, interactive_brokers, tradier,
#    td_ameritrade, schwab, webull, oanda (forex), bybit, okx, kucoin,
#    mexc, gate_io, huobi, phemex, bitfinex, gemini, bitstamp
#
#  Unknown brokers: auto-probe + Claude interpretation
# ══════════════════════════════════════════════════════════════════════════════

# ── Constraint caches ─────────────────────────────────────────────────────────
_BROKER_ASSET_CACHE:      Dict[str, dict]  = {}  # symbol → asset attributes
_BROKER_CONSTRAINTS:      Dict[str, dict]  = {}  # symbol → live constraints
_BROKER_GLOBAL_RULES:     Dict[str, dict]  = {}  # platform → global rules
_BROKER_RULES_FETCHED_AT: Dict[str, float] = {}  # platform → timestamp
_BROKER_RULES_TTL = 3600.0  # re-fetch global rules every hour

# ── Known broker metadata ─────────────────────────────────────────────────────
# For each broker: how to fetch live constraints + fallback hardcoded rules
# Updated as new platforms add API constraint endpoints.
BROKER_REGISTRY: Dict[str, dict] = {
    "alpaca": {
        "name": "Alpaca Markets",
        "asset_endpoint":    "/v2/assets/{symbol}",
        "account_endpoint":  "/v2/account",
        "exchange_info":     None,
        "crypto_24_7":       True,
        "supports_equities": True,
        "supports_crypto":   True,
        "supports_shorts":   True,   # equities only, asset-dependent
        "supports_margin":   True,
        "supports_fractional": True,
        "fallback": {
            "min_notional":      1.0,
            "margin_min_equity": 2000.0,
            "pdt_threshold":     25000.0,
            "pdt_max_day_trades": 3,
            "extended_limit_only": True,
            "extended_sessions": ["pre_market","after_hours","overnight_extended"],
            "margin_extended_max": 2.0,   # 2x max during extended hours
        },
    },
    "binance": {
        "name": "Binance",
        "asset_endpoint":    None,
        "account_endpoint":  "/api/v3/account",
        "exchange_info":     "/api/v3/exchangeInfo?symbol={symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 5.0,
            "min_qty":      0.00001,
        },
    },
    "kraken": {
        "name": "Kraken",
        "asset_endpoint":    "/0/public/AssetPairs?pair={symbol}",
        "account_endpoint":  "/0/private/Balance",
        "exchange_info":     "/0/public/AssetPairs?pair={symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,  # margin available but complex, not in APEX
        "supports_fractional": True,
        "fallback": {
            "min_notional": 1.50,
            "market_price_collar_pct": 1.0,   # Kraken: 1% slippage protection
        },
    },
    "coinbase": {
        "name": "Coinbase Advanced Trade",
        "asset_endpoint":    "/api/v3/brokerage/products/{symbol}",
        "account_endpoint":  "/api/v3/brokerage/accounts",
        "exchange_info":     "/api/v3/brokerage/products/{symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 1.0,
        },
    },
    "interactive_brokers": {
        "name": "Interactive Brokers",
        "asset_endpoint":    None,
        "account_endpoint":  "/v1/api/portfolio/accounts",
        "exchange_info":     None,
        "crypto_24_7":       False,
        "supports_equities": True,
        "supports_crypto":   True,
        "supports_shorts":   True,
        "supports_margin":   True,
        "supports_fractional": True,
        "fallback": {
            "min_notional":       1.0,
            "pdt_threshold":      25000.0,
            "pdt_max_day_trades": 3,
            "margin_min_equity":  2000.0,
            "extended_limit_only": True,
            "short_requires_locate": True,   # IBKR requires share locate for shorts
        },
    },
    "tradier": {
        "name": "Tradier",
        "asset_endpoint":    "/v1/markets/quotes?symbols={symbol}",
        "account_endpoint":  "/v1/user/profile",
        "exchange_info":     None,
        "crypto_24_7":       False,
        "supports_equities": True,
        "supports_crypto":   False,
        "supports_shorts":   True,
        "supports_margin":   True,
        "supports_fractional": False,
        "fallback": {
            "min_notional":       1.0,
            "pdt_threshold":      25000.0,
            "pdt_max_day_trades": 3,
            "margin_min_equity":  2000.0,
            "commission_per_trade": 0.0,     # Tradier commission-free
        },
    },
    "td_ameritrade": {
        "name": "TD Ameritrade / Schwab",
        "asset_endpoint":    None,
        "account_endpoint":  "/v1/accounts",
        "exchange_info":     None,
        "crypto_24_7":       False,
        "supports_equities": True,
        "supports_crypto":   False,
        "supports_shorts":   True,
        "supports_margin":   True,
        "supports_fractional": False,
        "fallback": {
            "min_notional":       1.0,
            "pdt_threshold":      25000.0,
            "pdt_max_day_trades": 3,
            "margin_min_equity":  2000.0,
            "extended_limit_only": True,
        },
    },
    "schwab": {
        "name": "Charles Schwab",
        "asset_endpoint":    "/trader/v1/instruments?symbol={symbol}&projection=fundamental",
        "account_endpoint":  "/trader/v1/accounts",
        "exchange_info":     None,
        "crypto_24_7":       False,
        "supports_equities": True,
        "supports_crypto":   False,
        "supports_shorts":   True,
        "supports_margin":   True,
        "supports_fractional": True,
        "fallback": {
            "min_notional":       0.01,     # Schwab fractional: $0.01 minimum
            "pdt_threshold":      25000.0,
            "pdt_max_day_trades": 3,
            "margin_min_equity":  2000.0,
            "extended_limit_only": True,
        },
    },
    "webull": {
        "name": "Webull",
        "asset_endpoint":    None,
        "account_endpoint":  None,
        "exchange_info":     None,
        "crypto_24_7":       False,
        "supports_equities": True,
        "supports_crypto":   True,
        "supports_shorts":   True,
        "supports_margin":   True,
        "supports_fractional": False,
        "fallback": {
            "min_notional":       1.0,
            "pdt_threshold":      25000.0,
            "pdt_max_day_trades": 3,
            "margin_min_equity":  2000.0,
            "extended_limit_only": True,
            "extended_sessions":  ["pre_market","after_hours"],  # Webull: no overnight
        },
    },
    "oanda": {
        "name": "OANDA (Forex/CFD)",
        "asset_endpoint":    "/v3/accounts/{account}/instruments?instruments={symbol}",
        "account_endpoint":  "/v3/accounts",
        "exchange_info":     "/v3/accounts/{account}/instruments?instruments={symbol}",
        "crypto_24_7":       False,
        "supports_equities": False,
        "supports_crypto":   False,  # CFD crypto only
        "supports_shorts":   True,   # forex pairs are bidirectional
        "supports_margin":   True,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 1.0,
            "min_units":    1,        # OANDA: minimum 1 unit per trade
            "forex_24_5":   True,     # forex trades Sun 5PM–Fri 5PM ET
        },
    },
    "bybit": {
        "name": "Bybit",
        "asset_endpoint":    "/v5/market/instruments-info?category=spot&symbol={symbol}",
        "account_endpoint":  "/v5/account/wallet-balance?accountType=UNIFIED",
        "exchange_info":     "/v5/market/instruments-info?category=spot&symbol={symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,  # spot only in APEX; perpetuals require futures
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 1.0,
            "min_qty":      0.000001,
        },
    },
    "okx": {
        "name": "OKX",
        "asset_endpoint":    "/api/v5/public/instruments?instType=SPOT&instId={symbol}",
        "account_endpoint":  "/api/v5/account/balance",
        "exchange_info":     "/api/v5/public/instruments?instType=SPOT&instId={symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 1.0,
            "min_sz":       0.000001,
        },
    },
    "kucoin": {
        "name": "KuCoin",
        "asset_endpoint":    "/api/v1/symbols?symbol={symbol}",
        "account_endpoint":  "/api/v1/accounts",
        "exchange_info":     "/api/v1/symbols?symbol={symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 0.1,    # KuCoin minimum is very low
            "min_qty":      0.00001,
        },
    },
    "gemini": {
        "name": "Gemini",
        "asset_endpoint":    "/v1/symbols/details/{symbol}",
        "account_endpoint":  "/v1/balances",
        "exchange_info":     "/v1/symbols/details/{symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 0.01,
            "min_order_increment": 0.00001,
        },
    },
    "bitstamp": {
        "name": "Bitstamp",
        "asset_endpoint":    "/api/v2/trading-pairs-info/",
        "account_endpoint":  "/api/v2/balance/",
        "exchange_info":     "/api/v2/trading-pairs-info/",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 10.0,   # Bitstamp minimum is ~$10
            "min_qty":      0.0001,
        },
    },
    "mexc": {
        "name": "MEXC",
        "asset_endpoint":    "/api/v3/exchangeInfo?symbol={symbol}",
        "account_endpoint":  "/api/v3/account",
        "exchange_info":     "/api/v3/exchangeInfo?symbol={symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 1.0,
            "min_qty":      0.000001,
        },
    },
    "gate_io": {
        "name": "Gate.io",
        "asset_endpoint":    "/api/v4/spot/currency_pairs/{symbol}",
        "account_endpoint":  "/api/v4/spot/accounts",
        "exchange_info":     "/api/v4/spot/currency_pairs/{symbol}",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   False,
        "supports_fractional": True,
        "fallback": {
            "min_notional": 1.0,
            "min_qty":      0.000001,
        },
    },
    "bitfinex": {
        "name": "Bitfinex",
        "asset_endpoint":    "/v2/conf/pub:info:pair",
        "account_endpoint":  "/v2/auth/r/wallets",
        "exchange_info":     "/v2/conf/pub:info:pair",
        "crypto_24_7":       True,
        "supports_equities": False,
        "supports_crypto":   True,
        "supports_shorts":   False,
        "supports_margin":   True,   # Bitfinex margin trading supported
        "supports_fractional": True,
        "fallback": {
            "min_notional": 5.0,
            "min_qty":      0.000001,
        },
    },
}


# ══════════════════════════════════════════════════════════════════════════════
#  LIVE CONSTRAINT FETCHER
#  Queries broker API for symbol-specific rules and caches them.
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_broker_constraints(symbol: str) -> dict:
    """
    Query the broker's API for live trading constraints on this symbol.
    Returns a dict with keys: min_notional, min_qty, shortable, fractionable,
    overnight_tradable, marginable, easy_to_borrow, max_position, etc.
    Falls back to BROKER_REGISTRY hardcoded values on any error.
    Caches result — subsequent calls return immediately.
    """
    platform = OPT.get("broker_platform", "").lower()
    cache_key = f"{platform}:{symbol}"
    if cache_key in _BROKER_CONSTRAINTS:
        return _BROKER_CONSTRAINTS[cache_key]

    meta     = BROKER_REGISTRY.get(platform, {})
    fallback = meta.get("fallback", {})

    # Start with fallback values
    constraints = {
        "min_notional":       fallback.get("min_notional", 1.0),
        "min_qty":            fallback.get("min_qty", 0.0),
        "shortable":          meta.get("supports_shorts", False),
        "fractionable":       meta.get("supports_fractional", True),
        "overnight_tradable": meta.get("supports_equities", False),
        "marginable":         meta.get("supports_margin", False),
        "easy_to_borrow":     True,
        "tradable":           True,
        "source":             "fallback",
    }
    constraints.update(fallback)

    key    = OPT.get("broker_api_key", "").strip()
    secret = OPT.get("broker_api_secret", "").strip()
    base   = _clean_url(OPT.get("broker_api_url", "").strip()) or ""
    if not key or not base:
        _BROKER_CONSTRAINTS[cache_key] = constraints
        return constraints

    # ── Fetch live constraints per platform ──────────────────────────────────
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            if platform == "alpaca":
                constraints = await _fetch_alpaca_constraints(
                    session, base, key, secret, symbol, constraints)

            elif platform == "binance":
                constraints = await _fetch_binance_constraints(
                    session, base, key, symbol, constraints)

            elif platform == "kraken":
                constraints = await _fetch_kraken_constraints(
                    session, base, symbol, constraints)

            elif platform in ("coinbase",):
                constraints = await _fetch_coinbase_constraints(
                    session, base, key, symbol, constraints)

            elif platform in ("bybit", "okx", "kucoin", "mexc", "gate_io"):
                constraints = await _fetch_generic_crypto_constraints(
                    session, base, key, secret, platform, symbol, constraints)

            elif platform not in BROKER_REGISTRY:
                # Unknown broker — try common endpoints and interpret via Claude
                constraints = await _fetch_unknown_broker_constraints(
                    session, base, key, secret, symbol, constraints)

    except Exception as e:
        log.debug("Constraint fetch for %s/%s: %s — using fallback", platform, symbol, e)

    constraints["source"] = "live" if constraints.get("source") != "fallback" else "fallback"
    _BROKER_CONSTRAINTS[cache_key] = constraints
    log.debug("Constraints %s/%s: min_notional=$%.2f shortable=%s fractionable=%s [%s]",
              platform, symbol, constraints["min_notional"],
              constraints["shortable"], constraints["fractionable"], constraints["source"])
    return constraints


async def _fetch_alpaca_constraints(session, base, key, secret, symbol, base_c):
    """Fetch live Alpaca asset attributes."""
    h = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    try:
        async with session.get(f"{base}/v2/assets/{symbol}", headers=h,
                               timeout=ClientTimeout(total=5)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                _BROKER_ASSET_CACHE[symbol] = d
                base_c.update({
                    "shortable":          bool(d.get("shortable", False)),
                    "easy_to_borrow":     bool(d.get("easy_to_borrow", False)),
                    "fractionable":       bool(d.get("fractionable", False)),
                    "overnight_tradable": bool(d.get("overnight_tradable", False)),
                    "marginable":         bool(d.get("marginable", False)),
                    "tradable":           bool(d.get("tradable", True)),
                    "exchange":           d.get("exchange", ""),
                    "asset_class":        d.get("class", "us_equity"),
                    "source":             "live",
                })
    except Exception as e:
        log.debug("Alpaca asset fetch %s: %s", symbol, e)
    return base_c


async def _fetch_binance_constraints(session, base, key, symbol, base_c):
    """Fetch live Binance exchangeInfo for symbol min notional and lot size."""
    # Map BTC/USD → BTCUSDT for Binance
    bn_sym = symbol.replace("/", "").replace("-", "") + (
        "USDT" if not symbol.upper().endswith(("USDT","BUSD","USDC","BTC","ETH")) else "")
    bn_sym = bn_sym.upper()
    try:
        async with session.get(
            f"{base}/api/v3/exchangeInfo?symbol={bn_sym}",
            headers={"X-MBX-APIKEY": key},
            timeout=ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                for sym_info in d.get("symbols", []):
                    if sym_info.get("symbol") == bn_sym:
                        for f in sym_info.get("filters", []):
                            ft = f.get("filterType", "")
                            if ft in ("MIN_NOTIONAL", "NOTIONAL"):
                                base_c["min_notional"] = float(
                                    f.get("minNotional") or f.get("minNotional", 5.0))
                            elif ft == "LOT_SIZE":
                                base_c["min_qty"] = float(f.get("minQty", 0.00001))
                                base_c["step_size"] = float(f.get("stepSize", 0.00001))
                            elif ft == "PRICE_FILTER":
                                base_c["tick_size"] = float(f.get("tickSize", 0.01))
                        base_c["tradable"] = sym_info.get("status") == "TRADING"
                        base_c["source"] = "live"
    except Exception as e:
        log.debug("Binance exchangeInfo %s: %s", symbol, e)
    return base_c


async def _fetch_kraken_constraints(session, base, symbol, base_c):
    """Fetch live Kraken AssetPairs for min order and cost."""
    # Kraken uses XBTUSD, ETHUSD etc.
    kr_map = {"BTC/USD": "XBTUSD", "ETH/USD": "ETHUSD", "SOL/USD": "SOLUSD",
              "BTC-USD": "XBTUSD", "ETH-USD": "ETHUSD"}
    kr_sym = kr_map.get(symbol.upper(), symbol.replace("/","").replace("-","").upper())
    try:
        async with session.get(
            f"{base}/0/public/AssetPairs?pair={kr_sym}",
            timeout=ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                result = d.get("result", {})
                for pair_key, pair_data in result.items():
                    ordermin = float(pair_data.get("ordermin", 0))
                    costmin  = float(pair_data.get("costmin", 0))
                    if ordermin > 0 or costmin > 0:
                        base_c["min_qty"]      = ordermin
                        base_c["min_notional"] = max(costmin, 1.50)
                        base_c["lot_decimals"] = int(pair_data.get("lot_decimals", 8))
                        base_c["price_decimals"] = int(pair_data.get("pair_decimals", 2))
                        base_c["leverage_buy"]   = pair_data.get("leverage_buy", [])
                        base_c["leverage_sell"]  = pair_data.get("leverage_sell", [])
                        base_c["source"] = "live"
                        break
    except Exception as e:
        log.debug("Kraken AssetPairs %s: %s", symbol, e)
    return base_c


async def _fetch_coinbase_constraints(session, base, key, symbol, base_c):
    """Fetch live Coinbase product details."""
    # Coinbase uses BTC-USD format
    cb_sym = symbol.replace("/", "-").upper()
    if not cb_sym.endswith("-USD"):
        cb_sym = cb_sym.replace("USD", "") + "-USD"
    try:
        async with session.get(
            f"{base}/api/v3/brokerage/products/{cb_sym}",
            headers={"CB-ACCESS-KEY": key},
            timeout=ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                base_min = float(d.get("base_min_size") or d.get("min_market_funds") or 0)
                quote_min = float(d.get("quote_min_size") or d.get("min_market_funds") or 0)
                base_c["min_qty"]      = base_min
                base_c["min_notional"] = max(quote_min, 1.0)
                base_c["tradable"]     = not d.get("trading_disabled", False)
                base_c["base_increment"] = float(d.get("base_increment", 0.00000001))
                base_c["quote_increment"] = float(d.get("quote_increment", 0.01))
                base_c["source"] = "live"
    except Exception as e:
        log.debug("Coinbase product %s: %s", symbol, e)
    return base_c


async def _fetch_generic_crypto_constraints(session, base, key, secret,
                                             platform, symbol, base_c):
    """
    Fetch constraints for Bybit, OKX, KuCoin, MEXC, Gate.io using their
    exchangeInfo-style endpoints. All return similar filter structures.
    """
    meta = BROKER_REGISTRY.get(platform, {})
    ep_tmpl = meta.get("exchange_info", "")
    if not ep_tmpl:
        return base_c

    # Symbol format normalization
    sym_fmt = symbol.replace("/", "-").upper()
    ep = ep_tmpl.replace("{symbol}", sym_fmt)

    try:
        async with session.get(f"{base}{ep}",
                               timeout=ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                # Try to extract min size from common response shapes
                # Bybit: result.list[0].lotSizeFilter
                # OKX: data[0].minSz
                # KuCoin: data.baseMinSize / quoteMinSize
                data = (d.get("result", {}).get("list") or
                        d.get("data") or [d])
                if isinstance(data, list) and data:
                    item = data[0]
                    min_sz = (
                        float(item.get("lotSizeFilter", {}).get("minOrderQty", 0)) or
                        float(item.get("minSz", 0)) or
                        float(item.get("baseMinSize", 0)) or
                        float(item.get("minQty", 0)) or 0
                    )
                    min_notional = (
                        float(item.get("minNotional", 0)) or
                        float(item.get("minValue", 0)) or
                        float(item.get("quoteMiniSize", 0)) or 0
                    )
                    if min_sz > 0:
                        base_c["min_qty"] = min_sz
                    if min_notional > 0:
                        base_c["min_notional"] = min_notional
                    base_c["source"] = "live"
    except Exception as e:
        log.debug("Generic crypto constraints %s/%s: %s", platform, symbol, e)
    return base_c


async def _fetch_unknown_broker_constraints(session, base, key, secret,
                                             symbol, base_c):
    """
    For unknown/future brokers: probe common API patterns and optionally
    ask Claude to interpret the response into constraint fields.
    """
    # Common endpoints that brokers tend to use for asset info
    PROBE_ENDPOINTS = [
        f"/v2/assets/{symbol}",
        f"/api/v3/exchangeInfo?symbol={symbol}",
        f"/v1/instruments?symbol={symbol}",
        f"/api/v1/symbols?symbol={symbol}",
        f"/v1/markets/{symbol}",
        f"/api/v1/products/{symbol}",
        f"/v1/reference/tickers/{symbol}",
    ]
    h = {}
    if key:
        h = {"Authorization": f"Bearer {key}",
             "X-API-KEY": key, "X-MBX-APIKEY": key}

    best_response = None
    for ep in PROBE_ENDPOINTS:
        try:
            async with session.get(f"{base}{ep}", headers=h,
                                   timeout=ClientTimeout(total=4)) as r:
                if r.status == 200:
                    text = await r.text()
                    d = json.loads(text)
                    if d and not d.get("error") and not d.get("errors"):
                        best_response = (ep, d)
                        break
        except Exception:
            continue

    if best_response and OPT.get("anthropic_api_key"):
        ep, d = best_response
        # Ask Claude to extract trading constraints from the response
        prompt = (
            f"You are a trading API parser. Extract trading constraints from this broker API response.\n"
            f"Endpoint: {ep}\n"
            f"Response: {json.dumps(d)[:1500]}\n\n"
            f"Return ONLY a JSON object with these fields (use null if unknown):\n"
            f'{{"min_notional": float, "min_qty": float, "shortable": bool, '
            f'"fractionable": bool, "overnight_tradable": bool, "marginable": bool, '
            f'"tradable": bool, "max_leverage": float}}'
        )
        try:
            result = await call_claude(prompt, max_tokens=200)
            if result.get("ok"):
                text = result["text"].strip()
                # Strip markdown fences if present
                text = re.sub(r'^```[a-z]*\n?|\n?```$', '', text, flags=re.M).strip()
                parsed = json.loads(text)
                for k, v in parsed.items():
                    if v is not None and k in base_c:
                        base_c[k] = v
                base_c["source"] = "live_claude_parsed"
                log.info("Unknown broker %s: constraints parsed by Claude from %s",
                         OPT.get("broker_platform"), ep)
        except Exception as e:
            log.debug("Claude constraint parse: %s", e)

    return base_c


# ══════════════════════════════════════════════════════════════════════════════
#  PER-BROKER PREFLIGHT FUNCTIONS (use live constraints + hardcoded rules)
# ══════════════════════════════════════════════════════════════════════════════

async def _alpaca_preflight_async(symbol: str, side: str, notional: float,
                                   qty: float, price: float, session: str) -> tuple:
    """Full Alpaca pre-flight using live asset constraints + regulatory rules."""
    c        = await _fetch_broker_constraints(symbol)
    is_crypto = "/" in symbol or symbol in CRYPTO_SYMBOLS
    portfolio = float(STATE.get("portfolio", 0))
    registry  = BROKER_REGISTRY["alpaca"]["fallback"]

    # ── Rule 1: Asset tradable ────────────────────────────────────────────────
    if not c.get("tradable", True):
        return False, f"{symbol} is not currently tradable on Alpaca."

    # ── Rule 2: Minimum notional ──────────────────────────────────────────────
    if notional < c["min_notional"]:
        return False, (f"Alpaca minimum order is ${c['min_notional']:.2f}. "
                       f"Got ${notional:.2f}.")

    # ── Rule 3: $2,000 equity minimum for margin/shorting ────────────────────
    if side in ("sell", "short") and not is_crypto:
        if portfolio < registry["margin_min_equity"]:
            return False, (f"Alpaca requires ${registry['margin_min_equity']:,.0f}+ "
                           f"equity for shorting. Current: ${portfolio:,.0f}")

    # ── Rule 4: Shortable — live check ───────────────────────────────────────
    if side in ("sell", "short") and not is_crypto:
        if not c.get("shortable", False) or not c.get("easy_to_borrow", False):
            return False, (f"Alpaca: {symbol} is not shortable "
                           f"(shortable={c.get('shortable')}, "
                           f"easy_to_borrow={c.get('easy_to_borrow')})")

    # ── Rule 5: PDT protection ────────────────────────────────────────────────
    if not is_crypto and OPT.get("allow_margin", False):
        if portfolio < registry["pdt_threshold"]:
            day_trades = int(STATE.get("alpacaDayTradeCount", 0))
            if day_trades >= registry["pdt_max_day_trades"] and side == "sell":
                today = _et_now().strftime("%Y-%m-%d")
                pos   = next((p for p in STATE.get("positions", [])
                              if p.get("sym") == symbol
                              and p.get("dir") == "LONG"), None)
                if pos:
                    open_date = _et_now().fromtimestamp(
                        pos.get("open", 0)/1000).strftime("%Y-%m-%d")
                    if open_date == today:
                        return False, (
                            f"PDT protection: {day_trades}/{registry['pdt_max_day_trades']} "
                            f"day trades used. Account < ${registry['pdt_threshold']:,.0f}. "
                            f"Close {symbol} tomorrow to avoid PDT flag.")

    # ── Rule 6: Fractional short restriction ─────────────────────────────────
    if qty > 0 and qty != int(qty) and side in ("sell", "short"):
        return False, (f"Alpaca does not support short sales on fractional quantities. "
                       f"qty={qty:.6f}")

    # ── Rule 7: Fractionable check ────────────────────────────────────────────
    if qty > 0 and qty != int(qty):
        if not c.get("fractionable", False):
            return False, (f"{symbol} is not fractionable on Alpaca. "
                           f"Use whole shares only.")

    # ── Rule 8: Overnight tradable check for overnight session ───────────────
    if session == "overnight_extended" and not is_crypto:
        if not c.get("overnight_tradable", False):
            return False, (f"{symbol} is not marked overnight_tradable on Alpaca. "
                           f"Cannot trade in overnight session (8PM-4AM ET).")

    # ── Rule 9: Extended hours require limit price ────────────────────────────
    if session in registry.get("extended_sessions", []) and not is_crypto:
        if price <= 0:
            return False, (f"Extended hours ({session}) requires a limit price. "
                           f"No price available for {symbol}.")

    # ── Rule 10: OTC / Pink sheet buy restriction ─────────────────────────────
    exchange = c.get("exchange", "").upper()
    if exchange in ("OTC", "PINK") and side == "buy":
        return False, (f"{symbol} is an OTC/Pink-sheet security "
                       f"(exchange={exchange}). Alpaca restricts buying OTC via API.")

    # ── Rule 11: Marginable check ─────────────────────────────────────────────
    if OPT.get("allow_margin", False) and not is_crypto:
        if not c.get("marginable", True) and side == "buy":
            log.warning("%s is non-marginable — using cash only for this position", symbol)

    return True, ""


async def _binance_preflight_async(symbol: str, side: str, notional: float,
                                    qty: float, price: float, session: str) -> tuple:
    """Full Binance pre-flight using live exchangeInfo constraints."""
    c = await _fetch_broker_constraints(symbol)

    if not c.get("tradable", True):
        return False, f"{symbol} is not currently trading on Binance."

    if not ("/" in symbol or symbol.upper().endswith(("USDT","BTC","ETH","BNB","BUSD","USDC"))):
        return False, f"Binance only supports crypto. {symbol} is not a crypto pair."

    if notional < c["min_notional"]:
        return False, (f"Binance min notional: ${c['min_notional']:.2f}. "
                       f"Got ${notional:.2f}.")

    step = c.get("step_size", 0)
    if step > 0 and qty > 0:
        # Qty must be a multiple of step_size
        remainder = qty % step
        if remainder > step * 0.01:  # 1% tolerance for float precision
            corrected = round(qty - remainder, 8)
            log.debug("Binance %s qty %.8f rounded to %.8f (step=%.8f)",
                      symbol, qty, corrected, step)

    if side == "short":
        has_pos = any(p.get("sym") == symbol and p.get("dir") == "LONG"
                      for p in STATE.get("positions", []))
        if not has_pos:
            return False, ("Binance spot does not support short selling. "
                           "This is a closing sell — no matching long position found.")

    return True, ""


async def _kraken_preflight_async(symbol: str, side: str, notional: float,
                                   qty: float, price: float, session: str) -> tuple:
    """Full Kraken pre-flight using live AssetPairs constraints."""
    c = await _fetch_broker_constraints(symbol)

    if notional < c["min_notional"]:
        return False, (f"Kraken minimum notional: ${c['min_notional']:.2f}. "
                       f"Got ${notional:.2f}.")

    min_qty = c.get("min_qty", 0)
    if min_qty > 0 and qty < min_qty:
        return False, (f"Kraken minimum order size: {min_qty}. Got {qty:.8f}.")

    if side == "short":
        leverage = c.get("leverage_sell", [])
        if not leverage:
            return False, (f"Kraken spot does not support short selling for {symbol}. "
                           f"Use Kraken Futures for shorts.")

    if not ("/" in symbol or "-" in symbol or symbol.upper().endswith("USD")):
        return False, f"Kraken only supports crypto. {symbol} is not recognized."

    if price <= 0 and notional > 5000:
        log.warning("Kraken market order $%.0f on %s: 1%% price collar applies — "
                    "may partially fill", notional, symbol)

    return True, ""


async def _coinbase_preflight_async(symbol: str, side: str, notional: float,
                                     qty: float, price: float, session: str) -> tuple:
    """Full Coinbase Advanced Trade pre-flight using live product constraints."""
    c = await _fetch_broker_constraints(symbol)

    if not c.get("tradable", True):
        return False, f"{symbol} trading is currently disabled on Coinbase."

    if notional < c["min_notional"]:
        return False, (f"Coinbase minimum order: ${c['min_notional']:.2f}. "
                       f"Got ${notional:.2f}.")

    min_qty = c.get("min_qty", 0)
    if min_qty > 0 and qty < min_qty:
        return False, (f"Coinbase minimum quantity: {min_qty}. Got {qty:.8f}.")

    if side == "short":
        return False, ("Coinbase Advanced Trade does not support short selling. "
                       "Use Coinbase Prime for institutional-level shorts.")

    if not ("/" in symbol or "-" in symbol or symbol.upper().endswith("USD")):
        return False, f"Coinbase only supports crypto. {symbol} is not recognized."

    return True, ""


async def _generic_broker_preflight_async(platform: str, symbol: str, side: str,
                                           notional: float, qty: float,
                                           price: float, session: str) -> tuple:
    """
    Pre-flight for known non-US-equity brokers (Bybit, OKX, KuCoin, etc.)
    and unknown future brokers. Uses live constraints where fetchable.
    """
    c    = await _fetch_broker_constraints(symbol)
    meta = BROKER_REGISTRY.get(platform, {})

    if not c.get("tradable", True):
        return False, f"{symbol} is not currently tradable on {meta.get('name', platform)}."

    if notional < c["min_notional"]:
        return False, (f"{meta.get('name', platform)} minimum order: "
                       f"${c['min_notional']:.2f}. Got ${notional:.2f}.")

    min_qty = c.get("min_qty", 0)
    if min_qty > 0 and qty > 0 and qty < min_qty:
        return False, (f"{meta.get('name', platform)} minimum qty: "
                       f"{min_qty}. Got {qty:.8f}.")

    if side == "short" and not meta.get("supports_shorts", False):
        return False, (f"{meta.get('name', platform)} does not support short selling "
                       f"via the spot API.")

    if not meta.get("supports_equities", False):
        if "/" not in symbol and "-" not in symbol and not symbol.upper().endswith("USD"):
            return False, (f"{meta.get('name', platform)} is crypto-only. "
                           f"{symbol} appears to be an equity symbol.")

    return True, ""


# ══════════════════════════════════════════════════════════════════════════════
#  UNIVERSAL PRE-FLIGHT DISPATCHER  (async)
# ══════════════════════════════════════════════════════════════════════════════

async def broker_preflight(symbol: str, side: str, notional: float,
                            qty: float, price: float) -> tuple:
    """
    Run the active broker's pre-flight checks before every order.
    Routes to the correct broker function based on OPT['broker_platform'].
    For unknown future brokers, falls back to generic checks + live constraints.
    Returns (ok: bool, reason: str).
    """
    mkt      = market_status(symbol)
    session  = mkt.get("session", "unknown")
    platform = OPT.get("broker_platform", "").lower()

    try:
        if platform == "alpaca":
            return await _alpaca_preflight_async(symbol, side, notional, qty, price, session)
        elif platform == "binance":
            return await _binance_preflight_async(symbol, side, notional, qty, price, session)
        elif platform == "kraken":
            return await _kraken_preflight_async(symbol, side, notional, qty, price, session)
        elif platform == "coinbase":
            return await _coinbase_preflight_async(symbol, side, notional, qty, price, session)
        elif platform in BROKER_REGISTRY:
            # Known broker with fallback rules
            return await _generic_broker_preflight_async(
                platform, symbol, side, notional, qty, price, session)
        else:
            # Completely unknown broker — fetch what we can, apply universal minimums
            c = await _fetch_broker_constraints(symbol)
            if notional < c.get("min_notional", 1.0):
                return False, (f"Minimum order ${c['min_notional']:.2f}. "
                               f"Got ${notional:.2f}.")
            return True, ""
    except Exception as e:
        # Never block a trade due to a rules engine crash — log and allow
        log.error("broker_preflight error for %s %s: %s — allowing order",
                  side.upper(), symbol, e)
        return True, ""


# ── PDT day trade counter (Alpaca + IBKR + US equity brokers) ────────────────
def _alpaca_increment_day_trade(symbol: str) -> None:
    """Increment PDT day trade counter after a same-day round-trip close."""
    platform = OPT.get("broker_platform", "").lower()
    if platform not in ("alpaca", "interactive_brokers", "tradier",
                        "td_ameritrade", "schwab", "webull"):
        return
    today = _et_now().strftime("%Y-%m-%d")
    if STATE.get("alpacaDayTradeDate") != today:
        STATE["alpacaDayTradeDate"]  = today
        STATE["alpacaDayTradeCount"] = 0
    pos = next((p for p in STATE.get("positions", [])
                if p.get("sym") == symbol
                and p.get("dir") == "LONG"), None)
    if pos:
        open_date = (
            _et_now().fromtimestamp(pos.get("open", 0)/1000).strftime("%Y-%m-%d")
            if pos.get("open") else "")
        if open_date == today:
            STATE["alpacaDayTradeCount"] = STATE.get("alpacaDayTradeCount", 0) + 1
            log.info("Day trade count: %d/%d (PDT threshold: 4 in 5 days, "
                     "applies to accounts < $25k on margin)",
                     STATE["alpacaDayTradeCount"],
                     BROKER_REGISTRY.get(platform, {}).get(
                         "fallback", {}).get("pdt_max_day_trades", 3))

async def _alpaca_connect(session, url, key, secret) -> Dict:
    base = _alpaca_base(url)
    h = {"APCA-API-KEY-ID":key.strip(),"APCA-API-SECRET-KEY":secret.strip(),"Content-Type":"application/json"}
    log.info("Alpaca → GET %s/account", base)
    try:
        async with session.get(f"{base}/account",headers=h,timeout=ClientTimeout(total=15)) as r:
            body = await r.text()
            log.info("Alpaca /account HTTP %d | %.300s",r.status,body)
            if r.status==401: return {"ok":False,"error":"Alpaca 401 — Key ID or Secret Key is wrong."}
            if r.status==403: return {"ok":False,"error":"Alpaca 403 — Account forbidden or IP blocked."}
            if r.status!=200: return {"ok":False,"error":f"Alpaca HTTP {r.status}: {body[:200]}"}
            d = json.loads(body)
    except asyncio.TimeoutError: return {"ok":False,"error":"Alpaca timed out (15s)."}
    except aiohttp.ClientConnectorError as e: return {"ok":False,"error":f"Cannot connect: {e}"}

    balance = float(d.get("equity") or d.get("portfolio_value") or d.get("last_equity") or 0)
    cash    = float(d.get("cash") or d.get("buying_power") or 0)
    if balance<=0 and cash>0: balance=cash
    log.info("Alpaca balance=$%.2f cash=$%.2f status=%s",balance,cash,d.get("status",""))

    positions_raw = []
    try:
        async with session.get(f"{base}/positions",headers=h,timeout=ClientTimeout(total=10)) as r:
            if r.status==200: positions_raw=json.loads(await r.text())
    except Exception: pass

    parsed=[]
    for p in positions_raw[:30]:
        try:
            qty=float(p.get("qty") or 0); entry=float(p.get("avg_entry_price") or 0)
            if abs(qty)>0:
                parsed.append({"sym":p.get("symbol","?"),"dir":"LONG" if qty>0 else "SHORT",
                    "entry":round(entry,4),"size":abs(qty),"cur":round(float(p.get("current_price") or entry),4),
                    "pnl":round(float(p.get("unrealized_pl") or 0),2),
                    "stop":round(entry*(1+0.02) if qty<0 else entry*(1-0.02),4),
                    "target":round(entry*(1-0.06) if qty<0 else entry*(1+0.06),4),"open":int(time.time()*1000)})
        except Exception: pass
    return {"ok":True,"balance":balance,"cash":cash,"platform":"alpaca",
            "account_status":d.get("status",""),"positions":parsed,"positions_count":len(parsed)}

async def fetch_broker_balance() -> Dict:
    platform   = OPT["broker_platform"]
    url_base   = OPT["broker_api_url"]
    api_key    = OPT["broker_api_key"]
    api_secret = OPT["broker_api_secret"]
    if not api_key:
        return {"ok":False,"error":"No broker API key in options.json. Add it in the HA Configuration tab."}
    log.info("fetch_broker_balance: platform=%s key_len=%d",platform or "(none)",len(api_key))
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            if platform=="alpaca":
                result = await _alpaca_connect(session,url_base,api_key,api_secret)
                if not result["ok"]: return result
                balance,parsed = result["balance"],result["positions"]
                extra = {"cash":result["cash"],"account_status":result["account_status"]}
            elif platform=="binance":
                base = _clean_url(url_base) or "https://api.binance.com/api/v3"
                async with session.get(f"{base}/account",headers={"X-MBX-APIKEY":api_key},
                                       timeout=ClientTimeout(total=15)) as r:
                    if r.status!=200: return {"ok":False,"error":f"Binance HTTP {r.status}"}
                    d=json.loads(await r.text())
                stable=[b for b in d.get("balances",[]) if b["asset"] in ("USDT","BUSD","USDC")]
                balance=sum(float(b["free"])+float(b["locked"]) for b in stable) or 0.0
                parsed,extra=[],{}
            elif platform=="kraken":
                base = _clean_url(url_base) or "https://api.kraken.com/0"
                async with session.post(f"{base}/private/Balance",
                                        headers={"API-Key":api_key},
                                        data={"nonce":str(int(time.time()*1000))},
                                        timeout=ClientTimeout(total=15)) as r:
                    if r.status!=200: return {"ok":False,"error":f"Kraken HTTP {r.status}"}
                    d=json.loads(await r.text())
                if d.get("error"): return {"ok":False,"error":f"Kraken: {d['error']}"}
                res=d.get("result",{})
                balance=float(res.get("ZUSD",0)) or sum(float(v) for v in res.values())
                parsed,extra=[],{}
            elif platform=="coinbase":
                base = _clean_url(url_base) or "https://api.coinbase.com/api/v3/brokerage"
                async with session.get(f"{base}/accounts",headers={"CB-ACCESS-KEY":api_key},
                                       timeout=ClientTimeout(total=15)) as r:
                    if r.status!=200: return {"ok":False,"error":f"Coinbase HTTP {r.status}"}
                    d=json.loads(await r.text())
                balance=sum(float(a.get("native_balance",{}).get("amount",0)) for a in d.get("data",[]))
                parsed,extra=[],{}
            else:
                if not url_base: return {"ok":False,"error":f"No URL for platform '{platform}'. Set broker_api_url in HA Configuration tab."}
                balance,parsed,extra=0.0,[],{}
                for ep in ["/account","/portfolio","/balance","/user/balance"]:
                    try:
                        async with session.get(f"{_clean_url(url_base)}{ep}",
                                               headers={"Authorization":f"Bearer {api_key}"},
                                               timeout=ClientTimeout(total=10)) as r:
                            if r.status==200:
                                d=json.loads(await r.text())
                                for k in ("portfolio_value","equity","balance","cash","total"):
                                    try:
                                        v=float(d.get(k) or 0)
                                        if v>0: balance=v; break
                                    except: pass
                                if balance: break
                    except: continue
    except aiohttp.ClientSSLError as e: return {"ok":False,"error":f"SSL error: {e}"}
    except asyncio.TimeoutError:         return {"ok":False,"error":"Broker timed out."}
    except Exception as e:
        log.error("fetch_broker_balance: %s",e)
        return {"ok":False,"error":str(e)}

    if not balance or balance<=0:
        return {"ok":False,"error":"Balance is 0. Check API key has read permissions."}

    STATE.update({"portfolio":balance,"startPortfolio":balance,"brokerBalance":balance,
                  "balanceFetched":True,"connected":True})
    if parsed: STATE["positions"]=parsed
    save_state()
    log.info("Broker OK: $%.2f | %d positions",balance,len(parsed))
    return {"ok":True,"balance":balance,"platform":platform,
            "positions":parsed,"positions_count":len(parsed),
            **(extra if isinstance(extra,dict) else {})}

# ── Position sizing ────────────────────────────────────────────────────────
def _calc_commission(notional: float, symbol: str = "") -> float:
    """
    Calculate brokerage commission for a trade.
    Uses commission_per_trade (flat $) + commission_pct (% of notional).
    Crypto symbols get commission_pct applied by default if configured.
    Returns 0 for Alpaca equities (commission-free).
    """
    flat   = float(OPT.get("commission_per_trade", 0) or 0)
    pct    = float(OPT.get("commission_pct", 0) or 0) / 100
    min_c  = float(OPT.get("commission_min", 0) or 0)

    # Auto-apply crypto commission if none configured but symbol is crypto
    is_crypto = "/" in symbol or symbol.endswith("-USD") or symbol in CRYPTO_SYMBOLS
    if is_crypto and pct == 0 and flat == 0:
        # Alpaca crypto default: 0.15% taker fee
        broker = OPT.get("broker_platform","").lower()
        defaults = {"coinbase": 0.004, "kraken": 0.0025, "binance": 0.001}
        pct = defaults.get(broker, 0.0015)  # alpaca default

    commission = flat + (notional * pct)
    return round(max(commission, min_c if (flat > 0 or pct > 0) else 0), 4)


def calc_position(symbol,price,side="buy"):
    # Capital base: use buying_power (includes margin) or just cash depending on setting
    allow_margin = OPT.get("allow_margin", False)
    allow_frac   = OPT.get("allow_fractional", False)

    portfolio    = STATE.get("portfolio", 0)
    buying_power = STATE.get("buyingPower", 0)
    cash         = STATE.get("cash", 0)

    # Capital available for this trade
    # During extended/overnight sessions, Alpaca limits margin to 2x (vs 4x intraday)
    _mkt_for_sizing = market_status(symbol)
    _is_extended = _mkt_for_sizing.get("extended_tradeable", False)
    if allow_margin and buying_power > 0:
        if _is_extended:
            # 2x margin cap during extended hours: use min(buying_power, portfolio*2)
            portfolio_2x = STATE.get("portfolio", 0) * 2.0
            available = min(buying_power, portfolio_2x)
            log.debug("Extended hours 2x margin cap: bp=$%.0f → capped=$%.0f", buying_power, available)
        else:
            available = buying_power   # full intraday buying power (up to 4x)
    elif cash > 0:
        available = cash           # only settled cash
    elif portfolio > 0:
        available = portfolio      # fallback: portfolio equity (cash not synced yet)
    else:
        available = 0

    if available <= 0:
        return {"ok":False,"error":"No capital. Connect broker first or set capital manually."}

    rc       = STATE.get("riskCfg", {})
    max_pct  = float(str(rc.get("maxPos","5%")).replace("%",""))/100
    stop_pct = float(str(rc.get("stopL","2%")).replace("%",""))/100
    take_pct = float(str(rc.get("takeP","6%")).replace("%",""))/100

    notional  = round(available * max_pct, 2)
    qty_frac  = round(notional / price, 6) if price > 0 else 0
    qty_whole = int(notional / price)      if price > 0 else 0

    # Shorts: always whole shares (brokers don't allow fractional short sales)
    # Cap short exposure at 3% of available capital
    if side in ("sell", "short"):
        short_max_pct  = min(max_pct, 0.03)
        short_notional = round(available * short_max_pct, 2)
        qty_whole = int(short_notional / price) if price > 0 else 0
        qty_frac  = float(qty_whole)
        notional  = round(qty_whole * price, 2)
    elif not allow_frac:
        # Fractional trading disabled — use whole shares only
        qty_frac = float(qty_whole)
        notional = round(qty_whole * price, 2)

    stop   = round(price * (1 - stop_pct if side == "buy" else 1 + stop_pct), 4)
    target = round(price * (1 + take_pct if side == "buy" else 1 - take_pct), 4)
    rr     = round(abs(target-price)/abs(price-stop), 2) if price != stop else 0

    capital_src = "buying_power" if (allow_margin and buying_power > 0) else "cash"
    log.debug("calc_position %s %s: notional=$%.2f qty_frac=%.6f qty_whole=%d "
              "capital_src=%s available=$%.2f frac=%s margin=%s",
              side.upper(), symbol, notional, qty_frac, qty_whole,
              capital_src, available, allow_frac, allow_margin)
    return {"ok":True,"symbol":symbol,"side":side,"price":price,
            "notional":notional,"qty_frac":qty_frac,"qty_whole":qty_whole,
            "stop":stop,"target":target,"rr_ratio":rr,
            "capital_source":capital_src,"available":available}

# Cache for overnight_tradable status to avoid repeated API calls
_OVERNIGHT_TRADABLE_CACHE: Dict[str, bool] = {}
_SHORTABLE_CACHE: Dict[str, bool] = {}  # cache of Alpaca shortable attribute

async def _check_overnight_tradable(session, base, headers, symbol: str) -> bool:
    """Check Alpaca Assets API for overnight_tradable attribute. Cached per symbol."""
    if symbol in _OVERNIGHT_TRADABLE_CACHE:
        return _OVERNIGHT_TRADABLE_CACHE[symbol]
    # Crypto always tradeable overnight
    if "/" in symbol or symbol in CRYPTO_SYMBOLS:
        _OVERNIGHT_TRADABLE_CACHE[symbol] = True
        return True
    try:
        async with session.get(f"{base}/assets/{symbol}", headers=headers,
                               timeout=ClientTimeout(total=5)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                tradeable = bool(d.get("overnight_tradable", False))
                _OVERNIGHT_TRADABLE_CACHE[symbol] = tradeable
                if not tradeable:
                    log.info("Asset %s: overnight_tradable=False — skipping overnight order", symbol)
                return tradeable
    except Exception as e:
        log.debug("overnight_tradable check for %s: %s", symbol, e)
    # Default: assume tradeable if check fails (don't block on API error)
    return True


async def _check_shortable(session, base, headers, symbol: str) -> bool:
    """Check Alpaca Assets API for shortable attribute. Cached per symbol.
    Returns True if the asset can be sold short on Alpaca."""
    if symbol in _SHORTABLE_CACHE:
        return _SHORTABLE_CACHE[symbol]
    # Crypto: shorting not supported via standard equity API
    if "/" in symbol or symbol in CRYPTO_SYMBOLS:
        _SHORTABLE_CACHE[symbol] = False
        return False
    try:
        async with session.get(f"{base}/assets/{symbol}", headers=headers,
                               timeout=ClientTimeout(total=5)) as r:
            if r.status == 200:
                d = json.loads(await r.text())
                shortable = bool(d.get("shortable", False)) and bool(d.get("easy_to_borrow", False))
                _SHORTABLE_CACHE[symbol] = shortable
                if not shortable:
                    log.info("Asset %s: shortable=%s easy_to_borrow=%s — skipping short",
                             symbol, d.get("shortable"), d.get("easy_to_borrow"))
                return shortable
    except Exception as e:
        log.debug("shortable check for %s: %s", symbol, e)
    # On API error: default False for safety (don't attempt un-verified shorts)
    _SHORTABLE_CACHE[symbol] = False
    return False

async def _place_alpaca_order(session,base,headers,symbol,side,notional,qty_whole,price):
    # Respect fractional trading setting — force whole-share path if disabled
    if not OPT.get("allow_fractional", False):
        ALPACA_FRAC_CACHE[symbol] = True  # treat as non-fractionable → whole-share path
    if not ALPACA_FRAC_CACHE.get(symbol,False):
        # Use "day" during regular hours, "gtc" outside hours so order queues for next open
        mkt_now = market_status(symbol)
        is_crypto_sym = "/" in symbol or symbol in CRYPTO_SYMBOLS
        # ── Shortability check — skip if asset cannot be sold short ──────────
        if side == "sell" and not (await _check_shortable(session, base, headers, symbol)):
            return {"ok": False, "skipped": True,
                    "reason": f"{symbol} is not shortable on Alpaca (not shortable or hard-to-borrow)"}

        ALPACA_EXTENDED_SESSIONS = ("pre_market", "after_hours", "overnight_extended")
        if mkt_now["open"] or is_crypto_sym:
            tif = "day"
            ext = False
            order_type = "market"
        elif mkt_now["session"] in ALPACA_EXTENDED_SESSIONS:
            # Alpaca 24/5: limit orders required for all extended sessions
            # extended_hours=True enables trading outside regular hours
            # For overnight_extended: verify asset is overnight_tradable
            if mkt_now["session"] == "overnight_extended":
                _ot = await _check_overnight_tradable(session, base, headers, symbol)
                if not _ot:
                    return {"ok":False,"skipped":True,
                            "reason":f"{symbol} not overnight_tradable on Alpaca"}
            tif = "day"
            ext = True
            order_type = "limit"
        else:
            tif = "gtc"   # weekend/holiday — queue GTC market, fills at next regular open
            ext = False
            order_type = "market"
        body={"symbol":symbol,"notional":str(round(notional,2)),"side":side,"type":order_type,"time_in_force":tif}
        if ext:
            body["extended_hours"] = True
            body["limit_price"] = str(round(price, 2))  # required for extended hours limit orders
        if ext:
            log.warning("EXTENDED HOURS ORDER: %s %s $%.2f | LIMIT @ $%.2f | "
                        "Lower liquidity and wider spreads expected in %s session",
                        side.upper(), symbol, notional, price, mkt_now.get("session","?"))
        else:
            log.info("Alpaca order (notional): %s %s $%.2f tif=%s ext=%s",side.upper(),symbol,notional,tif,ext)
        try:
            async with session.post(f"{base}/orders",headers=headers,json=body,timeout=ClientTimeout(total=15)) as r:
                resp=await r.text()
                log.info("Alpaca order HTTP %d: %.300s",r.status,resp)
                if r.status in (200,201):
                    d=json.loads(resp)
                    return {"ok":True,"order_id":d.get("id",""),"symbol":symbol,"side":side,
                            "qty":float(d.get("filled_qty") or d.get("qty") or round(notional/price,6)),
                            "notional":notional,"price":float(d.get("filled_avg_price") or price),
                            "order_type":"notional_fractional","status":d.get("status","submitted")}
                if r.status==422:
                    d=json.loads(resp) if resp else {}
                    msg=d.get("message","")
                    if "fractionable" in msg.lower() or "notional" in msg.lower():
                        ALPACA_FRAC_CACHE[symbol]=True
                    else:
                        return {"ok":False,"error":f"Alpaca rejected: {msg or resp[:200]}"}
                else:
                    return {"ok":False,"error":f"Alpaca order HTTP {r.status}: {resp[:200]}"}
        except asyncio.TimeoutError: return {"ok":False,"error":"Alpaca order timed out."}
    if qty_whole<1:
        return {"ok":False,"error":f"Need ${price:,.2f} for 1 share of {symbol}, have ${notional:.2f}. Symbol not fractionable."}
    mkt_ws = market_status(symbol)
    is_crypto_ws = "/" in symbol or symbol in CRYPTO_SYMBOLS
    _EXT_SESSIONS_WS = ("pre_market", "after_hours", "overnight_extended")
    if mkt_ws["open"] or is_crypto_ws:
        tif_ws, ext_ws, ot_ws = "day", False, "market"
    elif mkt_ws["session"] in _EXT_SESSIONS_WS:
        tif_ws, ext_ws, ot_ws = "day", True, "limit"   # Alpaca 24/5: limit + extended_hours=True
    else:
        tif_ws, ext_ws, ot_ws = "gtc", False, "market" # weekend/holiday: queue for next open
    body={"symbol":symbol,"qty":str(qty_whole),"side":side,"type":ot_ws,"time_in_force":tif_ws}
    if ext_ws:
        body["extended_hours"] = True
        body["limit_price"] = str(round(price, 2))
    log.info("Alpaca order (whole): %s %s qty=%d tif=%s ext=%s",side.upper(),symbol,qty_whole,tif_ws,ext_ws)
    async with session.post(f"{base}/orders",headers=headers,json=body,timeout=ClientTimeout(total=15)) as r:
        resp=await r.text()
        if r.status in (200,201):
            d=json.loads(resp)
            return {"ok":True,"order_id":d.get("id",""),"symbol":symbol,"side":side,
                    "qty":float(d.get("qty") or qty_whole),"notional":qty_whole*price,
                    "price":float(d.get("filled_avg_price") or price),
                    "order_type":"whole_shares","status":d.get("status","submitted")}
        d=json.loads(resp) if resp else {}
        err_msg = d.get("message", resp[:200])
        # If Alpaca says not shortable, bust the cache
        if "cannot be sold short" in err_msg.lower() or "not shortable" in err_msg.lower():
            _SHORTABLE_CACHE[symbol] = False
            _BROKER_CONSTRAINTS.pop(f"alpaca:{symbol}", None)
            log.warning("Alpaca rejected %s as not-shortable — cache busted, signal dropped next cycle", symbol)
        # Record broker rejection for learning memory
        FAILED_TRADES.insert(0, {
            "sym": symbol, "side": side, "dir": "LONG" if side=="buy" else "SHORT",
            "reason": err_msg[:120], "type": "broker_rejection",
            "ts": _et_now().strftime("%H:%M:%S"),
            "date": _et_now().strftime("%Y-%m-%d"),
        })
        if len(FAILED_TRADES) > 500: FAILED_TRADES.pop()
        _MEMORY_CACHE["built_at"] = None  # so next Claude call sees this rejection
        return {"ok":False,"error":f"Alpaca order failed: {err_msg}"}

# ── Broker-Side Stop/TP Orders ───────────────────────────────────────────────
async def _submit_broker_stop(session, base, headers, symbol: str, side: str,
                              qty: float, stop_price: float, tp_price: float,
                              is_extended: bool) -> dict:
    """
    Submit broker-side stop (and optionally take-profit) orders after entry.
    These fire at the broker even if the APEX addon goes offline.

    Regular market hours: single GTC stop order (market order when triggered)
    Extended/overnight:   GTC stop-limit order (limit to avoid bad fills)

    Args:
        side: "buy" or "sell" (the entry side — stop is the OPPOSITE)
        is_extended: True if order placed during extended/overnight session
    Returns:
        {"ok": True, "stop_order_id": "...", "tp_order_id": "..."} or {"ok": False}
    """
    qty_str = str(int(qty)) if qty == int(qty) else str(round(qty, 6))
    # Stop side is opposite of entry: buy entry → sell stop; sell(short) entry → buy stop
    stop_side = "sell" if side == "buy" else "buy"
    tp_side   = "sell" if side == "buy" else "buy"
    result = {"ok": False, "stop_order_id": None, "tp_order_id": None}

    try:
        # ── Submit GTC stop order ─────────────────────────────────────────
        if is_extended:
            # Extended hours: use stop-limit to avoid catastrophic fills on thin markets
            # Limit price gives 0.5% buffer beyond stop for fill execution
            stop_limit = round(stop_price * (0.995 if stop_side == "sell" else 1.005), 2)
            stop_body = {
                "symbol": symbol, "qty": qty_str, "side": stop_side,
                "type": "stop_limit", "stop_price": str(round(stop_price, 2)),
                "limit_price": str(stop_limit),
                "time_in_force": "gtc"
            }
        else:
            # Regular hours: plain stop (market order on trigger — best execution)
            stop_body = {
                "symbol": symbol, "qty": qty_str, "side": stop_side,
                "type": "stop", "stop_price": str(round(stop_price, 2)),
                "time_in_force": "gtc"
            }
        async with session.post(f"{base}/orders", headers=headers,
                                json=stop_body, timeout=ClientTimeout(total=10)) as r:
            resp = await r.text()
            if r.status in (200, 201):
                d = json.loads(resp)
                result["stop_order_id"] = d.get("id", "")
                log.info("Broker-side STOP submitted for %s: stop=$%.2f order_id=%s",
                         symbol, stop_price, result["stop_order_id"])
            else:
                log.warning("Broker stop order failed for %s: HTTP %d %s",
                            symbol, r.status, resp[:150])

        # ── Submit GTC take-profit limit order ────────────────────────────
        tp_body = {
            "symbol": symbol, "qty": qty_str, "side": tp_side,
            "type": "limit", "limit_price": str(round(tp_price, 2)),
            "time_in_force": "gtc"
        }
        async with session.post(f"{base}/orders", headers=headers,
                                json=tp_body, timeout=ClientTimeout(total=10)) as r:
            resp = await r.text()
            if r.status in (200, 201):
                d = json.loads(resp)
                result["tp_order_id"] = d.get("id", "")
                log.info("Broker-side TP submitted for %s: tp=$%.2f order_id=%s",
                         symbol, tp_price, result["tp_order_id"])
            else:
                log.warning("Broker TP order failed for %s: HTTP %d %s",
                            symbol, r.status, resp[:150])

        result["ok"] = bool(result["stop_order_id"])  # stop is required; TP is bonus
    except Exception as e:
        log.warning("_submit_broker_stop error for %s: %s", symbol, e)
    return result


async def _cancel_broker_orders(session, base, headers, order_ids: list) -> None:
    """Cancel a list of broker-side stop/TP order IDs (cleanup on close)."""
    for oid in order_ids:
        if not oid: continue
        try:
            async with session.delete(f"{base}/orders/{oid}", headers=headers,
                                      timeout=ClientTimeout(total=8)) as r:
                if r.status in (200, 204, 404):
                    log.info("Cancelled broker order %s (%d)", oid, r.status)
                else:
                    log.warning("Cancel order %s: HTTP %d", oid, r.status)
        except Exception as e:
            log.warning("Cancel order %s error: %s", oid, e)


async def _ensure_broker_stops(session, base, headers, pos: dict) -> dict:
    """
    Ensure a broker-side GTC stop and take-profit exist for an imported position.
    Called after live_sync imports a position that APEX did not open itself.

    Strategy:
      1. Fetch all open orders from Alpaca for this symbol.
      2. If a stop/TP already exists (from a previous APEX session or manual order),
         link its ID to the position dict — do NOT post a duplicate.
      3. If no stop/TP found, post new GTC stop + TP via _submit_broker_stop.

    Returns the position dict with broker_stop_id / broker_tp_id populated.
    Only runs in live trading mode.
    """
    if OPT.get("trading_mode") != "live":
        return pos

    sym      = pos.get("sym", "")
    is_long  = pos.get("dir", "LONG") == "LONG"
    stop_px  = float(pos.get("stop", 0))
    tp_px    = float(pos.get("target", 0))
    qty      = float(pos.get("size", 0))
    is_crypto = "/" in sym or sym in CRYPTO_SYMBOLS
    is_ext   = not market_status(sym)["open"] and not is_crypto

    if not sym or qty <= 0 or stop_px <= 0 or tp_px <= 0:
        return pos

    # Expected order sides for this position
    # LONG position: protected by sell-stop + sell-limit TP
    # SHORT position: protected by buy-stop + buy-limit TP
    close_side = "sell" if is_long else "buy"

    existing_stop_id = None
    existing_tp_id   = None

    try:
        # ── Step 1: scan existing open orders for this symbol ──────────
        async with session.get(f"{base}/orders?status=open&limit=100", headers=headers,
                               timeout=ClientTimeout(total=8)) as r:
            if r.status == 200:
                open_orders = json.loads(await r.text())
                for o in open_orders:
                    if o.get("symbol") != sym: continue
                    if o.get("side") != close_side: continue
                    otype = o.get("type", "")
                    ostatus = o.get("status", "")
                    if ostatus not in ("new", "accepted", "pending_new", "held"): continue
                    if otype in ("stop", "stop_limit") and not existing_stop_id:
                        existing_stop_id = o.get("id")
                        log.info("Linked existing broker stop for %s: %s @ $%s",
                                 sym, existing_stop_id, o.get("stop_price","?"))
                    elif otype == "limit" and not existing_tp_id:
                        existing_tp_id = o.get("id")
                        log.info("Linked existing broker TP for %s: %s @ $%s",
                                 sym, existing_tp_id, o.get("limit_price","?"))
    except Exception as e:
        log.debug("_ensure_broker_stops: open order scan for %s: %s", sym, e)

    # ── Step 2: post missing orders ────────────────────────────────────
    if existing_stop_id and existing_tp_id:
        # Both already exist — just link them
        pos["broker_stop_id"] = existing_stop_id
        pos["broker_tp_id"]   = existing_tp_id
        return pos

    # Need to post at least one (or both)
    # Cancel any partial existing orders first to avoid conflicting duplication
    partials = [oid for oid in [existing_stop_id, existing_tp_id] if oid]
    if partials:
        log.info("Cancelling partial broker orders for %s before reposting: %s", sym, partials)
        await _cancel_broker_orders(session, base, headers, partials)

    alpaca_entry_side = "buy" if is_long else "sell"
    stops = await _submit_broker_stop(
        session, base, headers, sym, alpaca_entry_side,
        qty, stop_px, tp_px, is_ext)
    pos["broker_stop_id"] = stops.get("stop_order_id")
    pos["broker_tp_id"]   = stops.get("tp_order_id")
    if stops.get("stop_order_id"):
        log.info("Broker stops ensured for imported position %s: stop=$%.2f tp=$%.2f",
                 sym, stop_px, tp_px)
    return pos


# ── Claude AI ───────────────────────────────────────────────────────────────
# ── Trading Memory Engine ─────────────────────────────────────────────────
# Analyzes TRADES history to extract persistent patterns that Claude uses
# across every scan — wins, losses, best symbols, worst symbols, timing edges.
# This is the "long-term memory" that accumulates as the bot trades.

# ── Trading Memory — 3-tier retention ─────────────────────────────────────
# Tier 1 (7d):   full detail in TRADES list — current patterns
# Tier 2 (60d):  aggregated in TRADES list — medium-term edges
# Tier 3 (∞):    permanent summary in trading_memory.json — never purged

MEMORY_RETENTION_DAYS = {"recent": 7, "medium": 60}
MEMORY_FILE = DATA_DIR / "trading_memory.json"
_MEMORY_CACHE: dict = {"text": "", "built_at": None, "trade_count": 0}
_MEMORY_TTL_SECONDS = 300


def _trade_date(t: dict):
    """Parse trade date string → date object."""
    ds = t.get("date", "")
    if not ds: return None
    try: return date.fromisoformat(ds)
    except (ValueError, AttributeError): return None


def _load_lifetime_summary() -> dict:
    """Load persistent all-time stats from disk."""
    try:
        if MEMORY_FILE.exists():
            return json.loads(MEMORY_FILE.read_text())
    except Exception:
        pass
    return {"total_trades":0,"total_wins":0,"total_pnl":0.0,
            "sym_stats":{},"strat_stats":{},
            "hall_of_fame_wins":[],"hall_of_fame_losses":[],
            "first_trade_date":None,"last_purge_date":None}


def _save_lifetime_summary(summary: dict):
    try: MEMORY_FILE.write_text(json.dumps(summary, indent=2))
    except Exception as e: log.warning("Could not save trading memory: %s", e)


def _update_lifetime_summary(trades_to_archive: list):
    """Absorb old trades into permanent summary before purging."""
    if not trades_to_archive: return
    summary = _load_lifetime_summary()
    if not summary.get("first_trade_date"):
        summary["first_trade_date"] = trades_to_archive[-1].get("date", "")

    for t in trades_to_archive:
        pnl  = float(t.get("pnl", 0) or 0)
        sym  = t.get("sym", "")
        strat = t.get("strat", "unknown")
        summary["total_trades"] += 1
        summary["total_pnl"]    += pnl
        if pnl >= 0: summary["total_wins"] += 1
        if sym:
            ss = summary["sym_stats"].setdefault(sym, {"wins":0,"losses":0,"pnl":0.0})
            if pnl >= 0: ss["wins"] += 1
            else:        ss["losses"] += 1
            ss["pnl"] = round(ss["pnl"] + pnl, 2)
        if strat:
            rs = summary["strat_stats"].setdefault(strat, {"wins":0,"losses":0,"pnl":0.0})
            if pnl >= 0: rs["wins"] += 1
            else:        rs["losses"] += 1
            rs["pnl"] = round(rs["pnl"] + pnl, 2)
        entry = {"sym":sym,"pnl":round(pnl,2),"date":t.get("date",""),"strat":strat}
        if pnl > 0:
            summary["hall_of_fame_wins"].append(entry)
            summary["hall_of_fame_wins"] = sorted(
                summary["hall_of_fame_wins"], key=lambda x: x["pnl"], reverse=True)[:5]
        elif pnl < 0:
            summary["hall_of_fame_losses"].append(entry)
            summary["hall_of_fame_losses"] = sorted(
                summary["hall_of_fame_losses"], key=lambda x: x["pnl"])[:5]

    summary["last_purge_date"] = date.today().isoformat()
    _save_lifetime_summary(summary)
    log.info("Lifetime summary updated: %d all-time trades $%.2f total P&L",
             summary["total_trades"], summary["total_pnl"])


def _purge_old_trades():
    """
    Archive trades older than 60 days into lifetime summary, then remove from TRADES.
    Called on startup. Safe to call repeatedly — skips if nothing to purge.
    """
    cutoff = date.today() - timedelta(days=MEMORY_RETENTION_DAYS["medium"])
    old = [t for t in TRADES if (d := _trade_date(t)) and d < cutoff]
    if not old: return
    _update_lifetime_summary(old)
    TRADES[:] = [t for t in TRADES if not ((d := _trade_date(t)) and d < cutoff)]
    log.info("Memory purge: archived %d trades older than %d days", len(old), MEMORY_RETENTION_DAYS["medium"])
    save_trades()
    _MEMORY_CACHE["built_at"] = None  # invalidate cache


def _build_trading_memory() -> str:
    """
    Analyze TRADES list and return a concise memory block for Claude.
    L1: symbol WR, strategy WR, direction bias, recent momentum, hall-of-fame
    L4: exit reason breakdown (stop_hit / target_hit / manual_close / broker_stop)
    L5: session WR breakdown (regular / pre_market / after_hours / overnight)
    L6: regime WR breakdown (calm VIX<18 / elevated 18-30 / risk-off VIX>30)
    L8: slippage stats by session
    """
    global _MEMORY_CACHE
    now = time.time()
    if (_MEMORY_CACHE["text"]
            and _MEMORY_CACHE["trade_count"] == len(TRADES)
            and _MEMORY_CACHE["built_at"]
            and now - _MEMORY_CACHE["built_at"] < _MEMORY_TTL_SECONDS):
        return _MEMORY_CACHE["text"]

    if len(TRADES) < 5:
        return ""

    # ── L1: Per-symbol stats ──────────────────────────────────────────────
    sym_stats: dict = {}
    for t in TRADES:
        sym = t.get("sym", "")
        if not sym: continue
        if sym not in sym_stats:
            sym_stats[sym] = {"wins": 0, "losses": 0, "total_pnl": 0.0, "strats": set()}
        pnl = float(t.get("pnl", 0) or 0)
        if pnl >= 0: sym_stats[sym]["wins"] += 1
        else:        sym_stats[sym]["losses"] += 1
        sym_stats[sym]["total_pnl"] += pnl
        strat = t.get("strat", "")
        if strat: sym_stats[sym]["strats"].add(strat)

    qualified = {s: v for s, v in sym_stats.items() if v["wins"] + v["losses"] >= 2}
    if qualified:
        by_wr = sorted(qualified.items(),
                       key=lambda x: x[1]["wins"] / (x[1]["wins"] + x[1]["losses"]),
                       reverse=True)
        best_syms  = [(s, round(v["wins"] / (v["wins"]+v["losses"]) * 100), round(v["total_pnl"], 2))
                      for s, v in by_wr[:5]]
        worst_syms = [(s, round(v["wins"] / (v["wins"]+v["losses"]) * 100), round(v["total_pnl"], 2))
                      for s, v in by_wr[-3:]]
    else:
        best_syms, worst_syms = [], []

    # ── L1: Strategy performance ──────────────────────────────────────────
    strat_stats: dict = {}
    for t in TRADES:
        strat = t.get("strat", "unknown")
        if strat not in strat_stats:
            strat_stats[strat] = {"wins": 0, "losses": 0, "pnl": 0.0}
        pnl = float(t.get("pnl", 0) or 0)
        if pnl >= 0: strat_stats[strat]["wins"] += 1
        else:        strat_stats[strat]["losses"] += 1
        strat_stats[strat]["pnl"] += pnl

    # ── L1: Direction bias ────────────────────────────────────────────────
    long_trades  = [t for t in TRADES if t.get("dir", "LONG") == "LONG"]
    short_trades = [t for t in TRADES if t.get("dir", "SHORT") == "SHORT"]
    long_pnl  = sum(float(t.get("pnl", 0) or 0) for t in long_trades)
    short_pnl = sum(float(t.get("pnl", 0) or 0) for t in short_trades)
    long_wr   = round(sum(1 for t in long_trades  if float(t.get("pnl", 0) or 0) >= 0) / max(len(long_trades), 1) * 100)
    short_wr  = round(sum(1 for t in short_trades if float(t.get("pnl", 0) or 0) >= 0) / max(len(short_trades), 1) * 100)

    # ── L1: Recent momentum ───────────────────────────────────────────────
    recent = TRADES[:10]
    recent_wins   = sum(1 for t in recent if float(t.get("pnl", 0) or 0) >= 0)
    recent_losses = len(recent) - recent_wins
    recent_pnl    = sum(float(t.get("pnl", 0) or 0) for t in recent)
    momentum = "improving" if recent_wins > recent_losses else "declining" if recent_wins < recent_losses else "neutral"

    # ── L1: Hall of fame ─────────────────────────────────────────────────
    sorted_by_pnl = sorted(TRADES, key=lambda t: float(t.get("pnl", 0) or 0), reverse=True)
    top_wins   = [(t["sym"], round(float(t.get("pnl", 0)), 2))
                  for t in sorted_by_pnl[:3] if float(t.get("pnl", 0) or 0) > 0]
    top_losses = [(t["sym"], round(float(t.get("pnl", 0)), 2))
                  for t in sorted_by_pnl[-3:] if float(t.get("pnl", 0) or 0) < 0]

    # ── L1: Overall stats ─────────────────────────────────────────────────
    hold_days    = [float(t.get("heldDays", 0) or 0) for t in TRADES if t.get("heldDays")]
    avg_hold     = round(sum(hold_days) / len(hold_days), 1) if hold_days else 0
    total_pnl    = sum(float(t.get("pnl", 0) or 0) for t in TRADES)
    total_trades = len(TRADES)
    total_wins   = sum(1 for t in TRADES if float(t.get("pnl", 0) or 0) >= 0)
    overall_wr   = round(total_wins / total_trades * 100) if total_trades else 0

    # ── L4: Exit reason breakdown ─────────────────────────────────────────
    exit_stats: dict = {}
    for t in TRADES:
        reason = t.get("exit_reason") or ("broker_stop" if t.get("type") == "BROKER_STOP" else None)
        if not reason: continue
        if reason not in exit_stats:
            exit_stats[reason] = {"wins": 0, "losses": 0, "pnl": 0.0}
        pnl = float(t.get("pnl", 0) or 0)
        if pnl >= 0: exit_stats[reason]["wins"] += 1
        else:        exit_stats[reason]["losses"] += 1
        exit_stats[reason]["pnl"] += pnl

    # ── L5: Session-aware WR ─────────────────────────────────────────────
    SESSION_GROUPS = {
        "regular":         ["regular", "market_open"],
        "pre_market":      ["pre_market", "pre-market", "premarket"],
        "after_hours":     ["after_hours", "after-hours", "afterhours"],
        "overnight":       ["overnight", "overnight_extended", "24_5"],
    }
    session_stats: dict = {g: {"wins": 0, "losses": 0, "pnl": 0.0, "slippage": []}
                           for g in SESSION_GROUPS}
    for t in TRADES:
        raw_sess = (t.get("session_entry") or "regular").lower()
        matched = "regular"
        for group, variants in SESSION_GROUPS.items():
            if any(raw_sess.startswith(v) or v in raw_sess for v in variants):
                matched = group
                break
        pnl = float(t.get("pnl", 0) or 0)
        if pnl >= 0: session_stats[matched]["wins"] += 1
        else:        session_stats[matched]["losses"] += 1
        session_stats[matched]["pnl"] += pnl
        slip = t.get("slippage_pct")
        if slip is not None:
            session_stats[matched]["slippage"].append(float(slip))

    # ── L6: Regime WR (VIX-based) ────────────────────────────────────────
    regime_stats: dict = {
        "calm (VIX<18)":     {"wins": 0, "losses": 0, "pnl": 0.0},
        "elevated (VIX18-30)": {"wins": 0, "losses": 0, "pnl": 0.0},
        "risk-off (VIX>30)": {"wins": 0, "losses": 0, "pnl": 0.0},
    }
    for t in TRADES:
        vix = float(t.get("vix_entry", 0) or 0)
        if vix <= 0: continue
        if vix < 18:   bucket = "calm (VIX<18)"
        elif vix <= 30: bucket = "elevated (VIX18-30)"
        else:           bucket = "risk-off (VIX>30)"
        pnl = float(t.get("pnl", 0) or 0)
        if pnl >= 0: regime_stats[bucket]["wins"] += 1
        else:        regime_stats[bucket]["losses"] += 1
        regime_stats[bucket]["pnl"] += pnl

    # ── Build memory string ───────────────────────────────────────────────
    lines_out = [
        f"=== TRADING MEMORY ({total_trades} trades, ${total_pnl:+.2f} lifetime P&L) ===",
        f"Overall WR: {overall_wr}% | Avg hold: {avg_hold}d | Recent momentum: {momentum} ({recent_wins}W/{recent_losses}L last 10)",
        f"LONG: {long_wr}% WR ${long_pnl:+.2f} ({len(long_trades)} trades) | SHORT: {short_wr}% WR ${short_pnl:+.2f} ({len(short_trades)} trades)",
    ]

    if best_syms:
        lines_out.append("BEST SYMBOLS: " + " | ".join(f"{s} {wr}%WR ${pnl:+.2f}" for s, wr, pnl in best_syms))
    if worst_syms:
        lines_out.append("WORST SYMBOLS: " + " | ".join(f"{s} {wr}%WR ${pnl:+.2f}" for s, wr, pnl in worst_syms))
    if top_wins:
        lines_out.append("BIGGEST WINS: " + " | ".join(f"{s} ${p:+.2f}" for s, p in top_wins))
    if top_losses:
        lines_out.append("BIGGEST LOSSES: " + " | ".join(f"{s} ${p:.2f}" for s, p in top_losses))

    if strat_stats:
        strat_parts = []
        for strat, ss in sorted(strat_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
            t = ss["wins"] + ss["losses"]
            if t >= 2:
                wr = round(ss["wins"] / t * 100)
                strat_parts.append(f"{strat}: {wr}%WR ${ss['pnl']:+.2f} ({t}t)")
        if strat_parts:
            lines_out.append("STRATEGY P&L: " + " | ".join(strat_parts))

    # ── L4: Exit reason summary ───────────────────────────────────────────
    if exit_stats:
        exit_parts = []
        for reason, es in sorted(exit_stats.items(), key=lambda x: x[1]["wins"]+x[1]["losses"], reverse=True):
            t = es["wins"] + es["losses"]
            if t >= 2:
                wr = round(es["wins"] / t * 100)
                exit_parts.append(f"{reason}: {wr}%WR ${es['pnl']:+.2f} ({t}t)")
        if exit_parts:
            lines_out.append("EXIT REASONS: " + " | ".join(exit_parts))

    # ── L5: Session WR summary ────────────────────────────────────────────
    sess_parts = []
    for sess, ss in session_stats.items():
        t = ss["wins"] + ss["losses"]
        if t >= 2:
            wr = round(ss["wins"] / t * 100)
            avg_slip = round(sum(ss["slippage"]) / len(ss["slippage"]), 3) if ss["slippage"] else 0
            slip_str = f" slip={avg_slip:+.3f}%" if avg_slip != 0 else ""
            sess_parts.append(f"{sess}: {wr}%WR ${ss['pnl']:+.2f} ({t}t{slip_str})")
    if sess_parts:
        lines_out.append("SESSION WR: " + " | ".join(sess_parts))

    # ── L6: Regime WR summary ─────────────────────────────────────────────
    regime_parts = []
    for bucket, rs in regime_stats.items():
        t = rs["wins"] + rs["losses"]
        if t >= 2:
            wr = round(rs["wins"] / t * 100)
            regime_parts.append(f"{bucket}: {wr}%WR ${rs['pnl']:+.2f} ({t}t)")
    if regime_parts:
        lines_out.append("REGIME WR: " + " | ".join(regime_parts))

    # ── L8: Slippage alert ────────────────────────────────────────────────
    all_slips = [(t.get("session_entry", "regular"), float(t.get("slippage_pct", 0) or 0))
                 for t in TRADES if t.get("slippage_pct") is not None]
    if len(all_slips) >= 5:
        overnight_slips = [abs(s) for sess, s in all_slips if "overnight" in (sess or "").lower() or "extended" in (sess or "").lower()]
        regular_slips   = [abs(s) for sess, s in all_slips if "regular" in (sess or "").lower()]
        if overnight_slips and regular_slips:
            avg_ov  = round(sum(overnight_slips) / len(overnight_slips), 3)
            avg_reg = round(sum(regular_slips)   / len(regular_slips),   3)
            if avg_ov > avg_reg * 2:
                lines_out.append(f"⚠️ SLIPPAGE: overnight avg={avg_ov:+.3f}% vs regular={avg_reg:+.3f}% — extended-hours fill quality is poor")

    # ── Failed trades ─────────────────────────────────────────────────────
    if FAILED_TRADES:
        recent_failures = FAILED_TRADES[:30]
        fail_by_sym: dict = {}
        for f in recent_failures:
            sym = f.get("sym", "?")
            reason = f.get("reason", "unknown")
            ftype  = f.get("type", "unknown")
            key = f"{sym}|{reason[:40]}"
            fail_by_sym.setdefault(key, {"sym": sym, "reason": reason, "type": ftype, "count": 0})
            fail_by_sym[key]["count"] += 1
        top_fails = sorted(fail_by_sym.values(), key=lambda x: x["count"], reverse=True)[:8]
        broker_rejects  = [f for f in top_fails if f["type"] == "broker_rejection"]
        preflight_blocks = [f for f in top_fails if f["type"] == "preflight_block"]
        if broker_rejects:
            lines_out.append("BROKER REJECTIONS (avoid): " +
                " | ".join(f"{f['sym']} ({f['count']}x: {f['reason'][:50]})" for f in broker_rejects))
        if preflight_blocks:
            lines_out.append("PREFLIGHT BLOCKS: " +
                " | ".join(f"{f['sym']} ({f['count']}x: {f['reason'][:40]})" for f in preflight_blocks))

    lines_out.append("=== USE THIS HISTORY TO IMPROVE CURRENT SIGNALS ===")

    memory_text = "\n".join(lines_out)
    _MEMORY_CACHE.update({"text": memory_text, "built_at": now, "trade_count": len(TRADES)})
    return memory_text


def _ctx():
    """
    Lean system prompt — identity and role only.
    Prices, positions, and full context are in the TRADE_PROMPT (user turn) to avoid duplication.
    This saves ~1,300 tokens per Claude call across all endpoints.
    """
    tot  = STATE["winCount"] + STATE["lossCount"]
    wr   = round(STATE["winCount"] / tot * 100) if tot else 0
    mode = OPT.get("trading_mode", "paper").upper()
    memory = _build_trading_memory()
    return f"""You are APEX, an elite autonomous AI trading system operating in {mode} mode.
Win rate: {wr}% over {tot} trades | Portfolio: ${STATE.get('portfolio',0):,.2f} | Mode: {mode}
{memory}
Be specific, reference exact numbers, and be actionable. Favor symbols with proven track records."""

async def call_claude(prompt, max_tokens=1200):
    """
    Call Claude API with exponential backoff on 429 rate limits.
    Retries up to 3 times with 15s / 30s / 60s delays.
    Timeout raised to 90s for large prompts during market hours.
    """
    key = (OPT.get("anthropic_api_key") or "").strip()
    if not key:
        return {"ok": False, "text": "No Anthropic API key. Add it in the HA Configuration tab (anthropic_api_key field)."}

    _MAX_RETRIES = 3
    _BACKOFF     = [15, 30, 60]  # seconds between retries on 429

    for attempt in range(_MAX_RETRIES + 1):
        try:
            async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
                async with session.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                             "Content-Type": "application/json"},
                    json={"model": "claude-sonnet-4-5", "max_tokens": max_tokens,
                          "system": _ctx(), "messages": [{"role": "user", "content": prompt}]},
                    timeout=ClientTimeout(total=90)
                ) as r:
                    body = await r.text()
                    if r.status == 401:
                        return {"ok": False, "text": "Claude 401 — API key invalid or expired."}
                    if r.status == 429:
                        wait = _BACKOFF[min(attempt, len(_BACKOFF)-1)]
                        log.warning("Claude rate limited (429) — attempt %d/%d, retrying in %ds",
                                    attempt+1, _MAX_RETRIES, wait)
                        if attempt < _MAX_RETRIES:
                            await asyncio.sleep(wait)
                            continue
                        return {"ok": False, "text": "Claude rate limited after 3 retries. Will retry next cycle."}
                    if r.status == 529:
                        # Anthropic overloaded — treat like 429
                        wait = _BACKOFF[min(attempt, len(_BACKOFF)-1)]
                        log.warning("Claude overloaded (529) — retrying in %ds", wait)
                        if attempt < _MAX_RETRIES:
                            await asyncio.sleep(wait)
                            continue
                        return {"ok": False, "text": "Claude overloaded. Will retry next cycle."}
                    d = json.loads(body)
                    if r.status == 200 and d.get("content"):
                        return {"ok": True, "text": d["content"][0]["text"]}
                    return {"ok": False, "text": f"Claude error: {d.get('error',{}).get('message', f'HTTP {r.status}')}"}
        except asyncio.TimeoutError:
            log.warning("Claude timeout (90s) — attempt %d/%d", attempt+1, _MAX_RETRIES)
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_BACKOFF[min(attempt, len(_BACKOFF)-1)])
                continue
            return {"ok": False, "text": "Claude timed out after 3 attempts (90s each)."}
        except Exception as e:
            return {"ok": False, "text": f"Claude error: {e}"}
    return {"ok": False, "text": "Claude: max retries exceeded."}

# ── HTTP Routes ─────────────────────────────────────────────────────────────
routes = web.RouteTableDef()

@routes.get("/")
async def index(req):
    p=STATIC_DIR/"index.html"
    if not p.exists(): return web.Response(status=404,text="index.html not found")
    html = p.read_text()
    # Inject ingress base path so JS works behind any proxy (local, nabu.casa, etc.)
    # HA sets X-Ingress-Path = /api/hassio_ingress/TOKEN when proxying
    ingress_path = req.headers.get("X-Ingress-Path", "").strip().rstrip("/")
    if ingress_path:
        inject = f'<script>window.APEX_BASE={repr(ingress_path)};</script>'
    else:
        inject = '<script>window.APEX_BASE="";</script>'
    html = html.replace("</head>", inject + "</head>", 1)
    return web.Response(text=html, content_type="text/html",
        headers={"Cache-Control":"no-store, no-cache, must-revalidate","Pragma":"no-cache","Expires":"0"})

@routes.get("/api/state")
async def get_state(req):
    slim = req.rel_url.query.get("slim","")
    if slim:
        # Exclude large fields that are fetched separately
        exclude = {"prices", "brokerBalance"}
        return web.json_response({k:v for k,v in STATE.items() if k not in exclude})
    return web.json_response(STATE)

@routes.post("/api/risk/update")
async def update_risk(req):
    """Update risk config and strategy weights. Used by both UI and bot."""
    try:
        d = await req.json()
        changed = []
        if "riskCfg" in d and isinstance(d["riskCfg"], dict):
            STATE["riskCfg"].update(d["riskCfg"])
            changed.append("riskCfg")
        if "stratWeights" in d and isinstance(d["stratWeights"], dict):
            STATE["stratWeights"].update(d["stratWeights"])
            changed.append("stratWeights")
        if "stratActive" in d and isinstance(d["stratActive"], dict):
            STATE["stratActive"].update(d["stratActive"])
            changed.append("stratActive")
        if changed:
            save_state()
            log.info("Risk/strategy updated: %s", changed)
        return web.json_response({"ok":True,"updated":changed,"riskCfg":STATE["riskCfg"],
                                   "stratWeights":STATE["stratWeights"]})
    except Exception as e:
        return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.post("/api/state")
async def set_state(req):
    try:
        data = await req.json()
        # riskCfg:null means reset to config-file defaults
        if "riskCfg" in data and data["riskCfg"] is None:
            data["riskCfg"] = {
                "maxPos": f"{OPT['max_position_pct']}%",
                "stopL":  f"{OPT['stop_loss_pct']}%",
                "takeP":  f"{OPT['take_profit_pct']}%",
            }
        STATE.update(data); save_state()
        return web.json_response({"ok":True})
    except Exception as e: return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.get("/api/trades")
async def get_trades(req): return web.json_response(TRADES)

@routes.get("/api/failed_trades")
async def get_failed_trades(req): return web.json_response(FAILED_TRADES)

@routes.delete("/api/failed_trades")
async def clear_failed_trades(req):
    global FAILED_TRADES
    FAILED_TRADES = []; save_trades()
    return web.json_response({"ok": True, "cleared": True})

@routes.post("/api/trades")
async def add_trade(req):
    try:
        t=await req.json(); t["timestamp"]=datetime.now().isoformat()
        TRADES.insert(0,t); save_trades()
        pnl=t.get("pnl",0)
        STATE["taxYear"]["ltGains" if t.get("heldDays",0)>=365 else "stGains"]+=(pnl if pnl>0 else 0)
        STATE["taxYear"]["losses"]+=abs(pnl) if pnl<0 else 0
        save_state(); return web.json_response({"ok":True,"count":len(TRADES)})
    except Exception as e: return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.delete("/api/trades")
async def clear_trades(req): TRADES.clear(); save_trades(); return web.json_response({"ok":True})

@routes.delete("/api/optlog")
async def clear_optlog(req):
    global OPTLOG; OPTLOG = []; save_optlog()
    return web.json_response({"ok":True})

@routes.delete("/api/analyses")
async def clear_analyses(req):
    global ANALYSES; ANALYSES = []; _save_json("analyses.json", [])
    return web.json_response({"ok":True})


@routes.delete("/api/training/memory")
async def clear_training_memory(req):
    """Wipe the permanent training memory (trading_memory.json).
    This clears all lifetime learned patterns: symbol WR, strategy P&L,
    hall-of-fame trades. This is IRREVERSIBLE.
    Requires explicit confirmation — should only be called from the
    dedicated Training Data Reset UI, never from the standard hard reset.
    """
    global _MEMORY_CACHE
    try:
        # Wipe the file
        MEMORY_FILE.write_text(json.dumps({
            "first_trade_date": "", "last_purge_date": "",
            "total_trades": 0, "total_wins": 0, "total_pnl": 0.0,
            "sym_stats": {}, "strat_stats": {},
            "hall_of_fame_wins": [], "hall_of_fame_losses": []
        }, indent=2))
        # Invalidate in-memory cache
        _MEMORY_CACHE = {"text": "", "built_at": None, "trade_count": 0}
        log.info("Training memory cleared by user request")
        OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "lw",
            "msg": "⚠️ Training memory RESET by user — lifetime learned patterns cleared"})
        save_optlog()
        return web.json_response({"ok": True, "msg": "Training memory cleared"})
    except Exception as e:
        log.error("clear_training_memory: %s", e)
        return web.json_response({"ok": False, "error": str(e)}, status=500)

@routes.get("/api/optlog")
async def get_optlog(req): return web.json_response(OPTLOG)

@routes.post("/api/optlog")
async def add_optlog(req):
    try:
        e=await req.json(); e["ts"]=datetime.now().strftime("%H:%M:%S")
        OPTLOG.append(e); save_optlog(); return web.json_response({"ok":True})
    except Exception as e: return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.get("/api/analyses")
async def get_analyses(req): return web.json_response(ANALYSES)

@routes.post("/api/analyses")
async def add_analysis(req):
    try:
        a=await req.json(); a["savedAt"]=datetime.now().isoformat()
        ANALYSES.insert(0,a); save_analyses(); return web.json_response({"ok":True,"count":len(ANALYSES)})
    except Exception as e: return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.get("/api/symbols")
async def get_symbols(req):
    """Return currently tracked symbols with metadata and last prices."""
    symbols_out = []
    for sym, price in STATE["prices"].items():
        sector, name = SYMBOL_INFO.get(sym, ("Stock", sym))
        symbols_out.append({"symbol":sym,"price":price,"sector":sector,"name":name})
    return web.json_response({
        "symbols": symbols_out,
        "prices":  STATE["prices"],
        "count":   len(STATE["prices"]),
        "sources": {
            "yahoo":       {"free":True,"key_required":False,"limit":"unlimited","notes":"All US stocks/ETFs/crypto, 15min delayed"},
            "finnhub":     {"free":True,"key_required":True, "limit":"60/min",   "notes":"US+global stocks, real-time, get key at finnhub.io"},
            "polygon":     {"free":True,"key_required":True, "limit":"5/min",    "notes":"US stocks, prev close, get key at polygon.io"},
            "alphavantage":{"free":True,"key_required":True, "limit":"25/day",   "notes":"US+international, get key at alphavantage.co"},
        }
    })

@routes.get("/api/symbols/universe")
async def symbol_universe(req):
    """
    Returns the full accessible symbol universe.
    Yahoo Finance covers ~50,000+ securities globally:
    - All US-listed stocks (NYSE, NASDAQ, AMEX, OTC)
    - All ETFs and mutual funds
    - All major crypto pairs
    - International stocks (append exchange suffix: .L .T .DE .HK etc.)
    - IPOs appear same day they list on the exchange
    
    This endpoint is informational — the actual tradeable universe is unlimited.
    Any valid Yahoo Finance ticker can be added via search or the bot can pick any ticker.
    """
    return web.json_response({
        "total_accessible": "50,000+",
        "tracked_count": len(STATE["prices"]),
        "tracked_symbols": list(STATE["prices"].keys()),
        "coverage": {
            "us_stocks": "All NYSE/NASDAQ/AMEX listed (4,000+ active)",
            "etfs": "All US-listed ETFs (3,000+)",
            "crypto": "Major pairs via Yahoo (BTC-USD, ETH-USD, etc.)",
            "international": "Global stocks with exchange suffix (VOD.L, 7203.T, SAP.DE)",
            "ipos": "Available same day of listing",
            "otc": "OTC/Pink sheets supported",
        },
        "how_to_add": "Use symbol search to add any ticker. Bot can trade any ticker it names.",
        "note": "There is no symbol limit. Search any ticker or let the bot discover opportunities."
    })

@routes.get("/api/symbols/search")
async def search_symbol(req):
    """
    Two modes:
      ?q=AAPL        — fetch price for exact ticker (any Yahoo Finance ticker)
      ?q=Apple+Inc   — full-text search via Yahoo autocomplete → list of matches

    Yahoo Finance covers ~50,000+ securities worldwide:
      US stocks/ETFs, options, crypto, forex, international (use exchange suffix):
      VOD.L=Vodafone London, 7203.T=Toyota Tokyo, SAP.DE=SAP Frankfurt
    """
    q = req.rel_url.query.get("q","").strip()
    if not q:
        return web.json_response({"ok":False,"error":"q parameter required"},status=400)

    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:

            # ── Step 1: Yahoo autocomplete to find matching tickers ──────────
            auto_url = f"https://query2.finance.yahoo.com/v1/finance/search?q={q}&quotesCount=10&newsCount=0&listsCount=0"
            matches = []
            try:
                async with session.get(auto_url, timeout=ClientTimeout(total=6),
                                       headers=headers) as r:
                    if r.status == 200:
                        d = json.loads(await r.text())
                        for item in d.get("quotes", []):
                            sym = item.get("symbol","")
                            if not sym: continue
                            matches.append({
                                "symbol":   sym,
                                "name":     item.get("longname") or item.get("shortname") or sym,
                                "exchange": item.get("exchange",""),
                                "type":     item.get("quoteType",""),
                                "tracked":  sym in STATE["prices"],
                            })
            except Exception as e:
                log.debug("Yahoo autocomplete: %s", e)

            # If autocomplete found results, return them (user can then pick one)
            if matches:
                # If exact ticker match exists, also fetch its live price
                exact = next((m for m in matches if m["symbol"].upper()==q.upper()), None)
                if exact:
                    yahoo_sym = exact["symbol"].replace("/USD","-USD")
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?interval=1d&range=1d"
                    try:
                        async with session.get(url, timeout=ClientTimeout(total=6),
                                               headers=headers) as r:
                            if r.status == 200:
                                meta = json.loads(await r.text()).get("chart",{}).get("result",[{}])[0].get("meta",{})
                                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                                if price:
                                    exact["price"]    = float(price)
                                    exact["currency"] = meta.get("currency","USD")
                    except Exception: pass
                return web.json_response({"ok":True,"results":matches,"query":q,"count":len(matches)})

            # ── Step 2: Try direct ticker lookup if autocomplete returned nothing
            yahoo_sym = q.upper().replace("/USD","-USD")
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?interval=1d&range=1d"
            async with session.get(url, timeout=ClientTimeout(total=8),
                                   headers=headers) as r:
                if r.status != 200:
                    return web.json_response({
                        "ok":False,
                        "error":f"No results for '{q}'. Try: company name (e.g. 'Apple'), ticker (e.g. 'AAPL'), or international ticker (e.g. 'VOD.L' for London).",
                        "results":[],
                    }, status=404)
                meta = json.loads(await r.text()).get("chart",{}).get("result",[{}])[0].get("meta",{})
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if not price:
                    return web.json_response({"ok":False,"error":f"No price data for {q}","results":[]},status=404)
                return web.json_response({
                    "ok":True,"query":q,"count":1,
                    "results":[{
                        "symbol":   yahoo_sym,
                        "name":     meta.get("longName") or meta.get("shortName") or yahoo_sym,
                        "price":    float(price),
                        "exchange": meta.get("exchangeName",""),
                        "currency": meta.get("currency","USD"),
                        "type":     meta.get("instrumentType",""),
                        "tracked":  yahoo_sym in STATE["prices"],
                    }]
                })
    except Exception as e:
        log.error("search_symbol: %s", e)
        return web.json_response({"ok":False,"error":str(e),"results":[]},status=500)

@routes.post("/api/symbols/add")
async def add_symbol(req):
    """Add a new symbol to track. Works with any Yahoo Finance ticker."""
    try:
        d = await req.json()
        sym = (d.get("symbol") or "").upper().strip()
        if not sym:
            return web.json_response({"ok":False,"error":"symbol required"},status=400)
        if sym not in STATE["prices"]:
            STATE["prices"][sym] = 0.0
            # Immediately fetch the price via batch (faster, same rate limit)
            async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
                batch = await _yahoo_batch(session, [sym])
                price = batch.get(sym)
                if price:
                    STATE["prices"][sym] = price
                    save_state()
                    log.info("Added symbol %s @ $%.4f", sym, price)
                    return web.json_response({"ok":True,"symbol":sym,"price":price})
                else:
                    del STATE["prices"][sym]
                    return web.json_response({"ok":False,"error":f"Could not fetch price for {sym}. Check the ticker is valid on Yahoo Finance."},status=400)
        return web.json_response({"ok":True,"symbol":sym,"price":STATE["prices"][sym],"note":"already tracked"})
    except Exception as e:
        return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.delete("/api/symbols/{symbol}")
async def remove_symbol(req):
    """Remove a symbol from tracking."""
    sym = req.match_info.get("symbol","").upper()
    if sym in STATE["prices"]:
        del STATE["prices"][sym]
        save_state()
        return web.json_response({"ok":True,"symbol":sym})
    return web.json_response({"ok":False,"error":f"{sym} not in tracked list"},status=404)

@routes.get("/api/prices")
async def get_prices(req):
    return web.json_response({"prices":STATE["prices"],"lastFetch":STATE.get("lastPriceFetch")})

@routes.post("/api/prices/fetch")
async def trigger_fetch(req): return web.json_response(await fetch_all_prices())

@routes.get("/api/orders")
async def get_orders(req):
    """Fetch pending/open orders from broker."""
    platform = OPT.get("broker_platform","").lower()
    key      = OPT.get("broker_api_key","").strip()
    secret   = OPT.get("broker_api_secret","").strip()
    url_base = OPT.get("broker_api_url","").strip()
    if not key or not secret:
        return web.json_response({"ok":False,"error":"No broker configured","orders":[]})
    if platform != "alpaca":
        return web.json_response({"ok":True,"orders":[],"note":"Order list only supported for Alpaca"})
    base = _alpaca_base(url_base)
    h = {"APCA-API-KEY-ID":key,"APCA-API-SECRET-KEY":secret,"Content-Type":"application/json"}
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            async with session.get(f"{base}/orders?status=open&limit=50",
                                   headers=h, timeout=ClientTimeout(total=10)) as r:
                if r.status == 200:
                    orders = json.loads(await r.text())
                    return web.json_response({"ok":True,"orders":[{
                        "id":          o.get("id",""),
                        "symbol":      o.get("symbol",""),
                        "side":        o.get("side",""),
                        "qty":         o.get("qty") or o.get("notional",""),
                        "type":        o.get("type","market"),
                        "status":      o.get("status",""),
                        "tif":         o.get("time_in_force",""),
                        "submitted":   o.get("submitted_at",""),
                        "filled_qty":  o.get("filled_qty","0"),
                        "limit_price": o.get("limit_price"),
                        "notional":    o.get("notional"),
                    } for o in orders]})
                return web.json_response({"ok":False,"error":f"Alpaca orders HTTP {r.status}","orders":[]})
    except Exception as e:
        return web.json_response({"ok":False,"error":str(e),"orders":[]})


@routes.delete("/api/orders/{order_id}")
async def cancel_order(req):
    """Cancel a specific pending order by ID."""
    order_id = req.match_info.get("order_id","").strip()
    if not order_id:
        return web.json_response({"ok":False,"error":"order_id required"},status=400)
    platform = OPT.get("broker_platform","").lower()
    key      = OPT.get("broker_api_key","").strip()
    secret   = OPT.get("broker_api_secret","").strip()
    url_base = OPT.get("broker_api_url","").strip()
    if platform != "alpaca":
        return web.json_response({"ok":False,"error":"Order cancellation only supported for Alpaca"})
    base = _alpaca_base(url_base)
    h = {"APCA-API-KEY-ID":key,"APCA-API-SECRET-KEY":secret}
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            async with session.delete(f"{base}/orders/{order_id}",
                                      headers=h, timeout=ClientTimeout(total=10)) as r:
                if r.status in (200, 204):
                    log.info("Order cancelled: %s", order_id)
                    # Also remove from TRADES if it's there as pending
                    before = len(TRADES)
                    TRADES[:] = [t for t in TRADES if t.get("order_id") != order_id]
                    if len(TRADES) < before:
                        save_trades()
                    return web.json_response({"ok":True,"order_id":order_id,"status":"cancelled"})
                body = await r.text()
                d = json.loads(body) if body else {}
                return web.json_response({"ok":False,"error":d.get("message",f"HTTP {r.status}: {body[:100]})")})
    except Exception as e:
        return web.json_response({"ok":False,"error":str(e)},status=500)


@routes.delete("/api/orders")
async def cancel_all_orders(req):
    """Cancel ALL open orders on the broker."""
    platform = OPT.get("broker_platform","").lower()
    key      = OPT.get("broker_api_key","").strip()
    secret   = OPT.get("broker_api_secret","").strip()
    url_base = OPT.get("broker_api_url","").strip()
    if platform != "alpaca":
        return web.json_response({"ok":False,"error":"Only Alpaca supported"})
    base = _alpaca_base(url_base)
    h = {"APCA-API-KEY-ID":key,"APCA-API-SECRET-KEY":secret}
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            async with session.delete(f"{base}/orders", headers=h, timeout=ClientTimeout(total=15)) as r:
                if r.status in (200, 204, 207):
                    body = await r.text()
                    cancelled = json.loads(body) if body and body.strip() != "" else []
                    count = len(cancelled) if isinstance(cancelled, list) else 0
                    log.info("Cancelled all orders: %d", count)
                    return web.json_response({"ok":True,"cancelled":count})
                return web.json_response({"ok":False,"error":f"HTTP {r.status}"})
    except Exception as e:
        return web.json_response({"ok":False,"error":str(e)},status=500)


async def _auto_broker_sync():
    """Background task: sync positions and trades right after broker connect."""
    await asyncio.sleep(1)  # give connect response time to return
    platform = OPT.get("broker_platform","").lower()
    key      = OPT.get("broker_api_key","").strip()
    secret   = OPT.get("broker_api_secret","").strip()
    url_base = OPT.get("broker_api_url","").strip()
    if not key or platform != "alpaca": return
    base = _alpaca_base(url_base)
    h    = {"APCA-API-KEY-ID":key,"APCA-API-SECRET-KEY":secret,"Content-Type":"application/json"}
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            async with session.get(f"{base}/positions",headers=h,timeout=ClientTimeout(total=10)) as r:
                if r.status == 200:
                    local_syms = {p["sym"] for p in STATE.get("positions",[])}
                    added = 0
                    for bp in json.loads(await r.text()):
                        sym  = bp.get("symbol","")
                        qty  = float(bp.get("qty") or 0)
                        side = bp.get("side","long")
                        entry= float(bp.get("avg_entry_price") or 0)
                        upnl = float(bp.get("unrealized_pl") or 0)
                        if not sym or qty == 0 or sym in local_syms: continue
                        STATE["positions"].append({"sym":sym,"dir":"LONG" if side=="long" else "SHORT",
                            "entry":entry,"size":qty,
                            "stop":round(entry*(1+0.02) if side!="long" else entry*(1-0.02),4),
                            "target":round(entry*(1-0.06) if side!="long" else entry*(1+0.06),4),
                            "open":int(time.time()*1000),"pnl":round(upnl,2),"source":"auto_sync"})
                        added += 1
                    if added: save_state(); log.info("Auto-sync: imported %d broker positions", added)
            async with session.get(f"{base}/orders?status=filled&limit=100&direction=desc",
                                   headers=h,timeout=ClientTimeout(total=10)) as r:
                if r.status == 200:
                    existing_ids = {t.get("order_id","") for t in TRADES if t.get("order_id")}
                    today_str    = date.today().isoformat()
                    added = 0
                    for o in json.loads(await r.text()):
                        oid = o.get("id","")
                        if oid in existing_ids: continue
                        sym   = o.get("symbol","")
                        side  = o.get("side","buy")
                        fqty  = float(o.get("filled_qty") or 0)
                        fprice= float(o.get("filled_avg_price") or 0)
                        if not sym or fqty == 0 or fprice == 0: continue
                        notional = fqty * fprice
                        filled_at= o.get("filled_at") or o.get("submitted_at","")
                        try: trade_dt,trade_ts = filled_at[:10],filled_at[11:19]
                        except Exception: trade_dt,trade_ts = today_str,datetime.now().strftime("%H:%M:%S")
                        TRADES.append({"sym":sym,"side":side,"dir":"LONG" if side=="buy" else "SHORT",
                            "qty":fqty,"notional":round(notional,2),"entry":fprice,"exit":fprice if side=="sell" else 0,
                            "gross_pnl":0.0,"commission":round(_calc_commission(notional,sym),4),"pnl":0.0,
                            "heldDays":0,"type":"LIVE_SYNC","order_id":oid,"order_type":o.get("order_type","market"),
                            "status":"filled","strat":"manual","time":trade_ts,"date":trade_dt,
                            "timestamp":filled_at,"source":"auto_sync"})
                        added += 1
                    if added:
                        TRADES.sort(key=lambda t: t.get("timestamp",""), reverse=True)
                        save_trades(); STATE["tradesToday"]=len([t for t in TRADES if t.get("date")==today_str])
                        save_state(); log.info("Auto-sync: imported %d filled orders", added)
    except Exception as e:
        log.warning("Auto-sync failed: %s", e)


@routes.post("/api/broker/connect")
async def broker_connect(req):
    try:
        d=await req.json()
        platform=(d.get("platform") or OPT["broker_platform"] or "").strip().lower()
        url     =(d.get("url")      or OPT["broker_api_url"]   or "").strip()
        # Use OPT keys — client may pass empty strings for auto-connect (server uses config)
        api_key =(d.get("apiKey")   or OPT.get("broker_api_key","")   or "").strip()
        secret  =(d.get("apiSecret")or OPT.get("broker_api_secret","")or "").strip()
        if not api_key:
            return web.json_response({"ok":False,"error":"No API key. Add broker_api_key in HA Configuration tab or apex_settings.json."})

        # Save to options.json via Supervisor API — this persists across ALL restarts
        saves = {}
        if platform and platform != OPT.get("broker_platform",""): saves["broker_platform"] = platform
        if url      and url      != OPT.get("broker_api_url",""):  saves["broker_api_url"]  = url
        if api_key  and api_key  != OPT.get("broker_api_key",""):  saves["broker_api_key"]  = api_key
        if secret   and secret   != OPT.get("broker_api_secret",""):saves["broker_api_secret"]=secret
        if saves:
            ok = await _save_options_via_supervisor(saves)
            if ok: log.info("Broker credentials saved to options.json via Supervisor API")

        # ── If keys changed, reset stream + caches so new account is used immediately ──
        if saves:
            global _alpaca_ws_connected
            _alpaca_ws_connected = False  # trigger WS reconnect with new keys (within 30s)
            _SHORTABLE_CACHE.clear()       # paper vs live have different HTB lists
            _BROKER_CONSTRAINTS.clear()    # constraints may differ (margin, options)
            _OVERNIGHT_TRADABLE_CACHE.clear()
            ALPACA_FRAC_CACHE.clear()
            # ── If URL changed (paper ↔ live), clear stale positions ──────────────
            if "broker_api_url" in saves:
                _old_pos_count = len(STATE.get("positions", []))
                STATE["positions"] = []  # wipe — fetch_broker_balance imports live ones
                save_state()
                log.info("Account URL changed (paper↔live): cleared %d stale position(s)", _old_pos_count)
                OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "li",
                    "msg": f"🔄 Account switch: cleared {_old_pos_count} stale position(s) — new account loading"})
                save_optlog()
            log.info("Broker key change: stream reset, caches cleared")
        result = await fetch_broker_balance()
        if result.get("ok"):
            log.info("broker_connect OK: $%.2f",result.get("balance",0))
        else:
            log.error("broker_connect FAILED: %s",result.get("error","?"))
        return web.json_response(result)
    except Exception as e:
        log.error("broker_connect: %s",e)
        return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.post("/api/broker/sync")
async def broker_sync(req):
    """Pull open positions + filled orders from Alpaca and merge into addon state."""
    platform = OPT.get("broker_platform","").lower()
    key      = OPT.get("broker_api_key","").strip()
    secret   = OPT.get("broker_api_secret","").strip()
    url_base = OPT.get("broker_api_url","").strip()
    if not key or platform != "alpaca":
        return web.json_response({"ok":False,"error":"Alpaca broker key required for sync"})
    base = _alpaca_base(url_base)
    h    = {"APCA-API-KEY-ID":key,"APCA-API-SECRET-KEY":secret,"Content-Type":"application/json"}
    synced_pos = 0; synced_trades = 0; errors = []
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            # ── Sync open positions ───────────────────────────────────────
            async with session.get(f"{base}/positions", headers=h,
                                   timeout=ClientTimeout(total=10)) as r:
                if r.status == 200:
                    local_syms = {p["sym"] for p in STATE.get("positions",[])}
                    _brc  = STATE.get("riskCfg", {})
                    _bsl  = float(str(_brc.get("stopL","2%")).replace("%","")) / 100
                    _btp  = float(str(_brc.get("takeP","6%")).replace("%","")) / 100
                    _bsync_new = []
                    for bp in json.loads(await r.text()):
                        sym  = bp.get("symbol","")
                        qty  = float(bp.get("qty") or 0)
                        side = bp.get("side","long")
                        entry= float(bp.get("avg_entry_price") or 0)
                        upnl = float(bp.get("unrealized_pl") or 0)
                        if not sym or qty == 0: continue
                        if sym not in local_syms:
                            is_long_bs = (side == "long")
                            _bpos = {"sym":sym,"dir":"LONG" if is_long_bs else "SHORT",
                                "entry":entry,"size":qty,
                                "stop":   round(entry*(1-_bsl if is_long_bs else 1+_bsl),4),
                                "target": round(entry*(1+_btp if is_long_bs else 1-_btp),4),
                                "open":int(time.time()*1000),"pnl":round(upnl,2),
                                "source":"broker_sync","broker_stop_id":None,"broker_tp_id":None}
                            _bsync_new.append(_bpos)
                            synced_pos += 1
                            log.info("Sync: imported position %s %s %.4f @ $%.4f", side.upper(), sym, qty, entry)
                        else:
                            for p in STATE["positions"]:
                                if p["sym"] == sym: p["pnl"] = round(upnl,2)
                    # Ensure broker-side stops for all newly imported positions
                    for _bpos in _bsync_new:
                        _bpos = await _ensure_broker_stops(session, base, h, _bpos)
                        STATE["positions"].append(_bpos)
                    save_state()
                else:
                    errors.append(f"positions HTTP {r.status}")
            # ── Sync filled orders as trade records ───────────────────────
            async with session.get(f"{base}/orders?status=filled&limit=100&direction=desc",
                                   headers=h, timeout=ClientTimeout(total=10)) as r:
                if r.status == 200:
                    existing_ids = {t.get("order_id","") for t in TRADES if t.get("order_id")}
                    today_str    = date.today().isoformat()
                    for o in json.loads(await r.text()):
                        oid = o.get("id","")
                        if oid in existing_ids: continue
                        sym   = o.get("symbol","")
                        side  = o.get("side","buy")
                        fqty  = float(o.get("filled_qty") or 0)
                        fprice= float(o.get("filled_avg_price") or 0)
                        if not sym or fqty == 0 or fprice == 0: continue
                        notional = fqty * fprice
                        filled_at= o.get("filled_at") or o.get("submitted_at","")
                        try: trade_dt,trade_ts = filled_at[:10],filled_at[11:19]
                        except Exception: trade_dt,trade_ts = today_str,datetime.now().strftime("%H:%M:%S")
                        TRADES.append({"sym":sym,"side":side,"dir":"LONG" if side=="buy" else "SHORT",
                            "qty":fqty,"notional":round(notional,2),"entry":fprice,
                            "exit":fprice if side=="sell" else 0,
                            "gross_pnl":0.0,"commission":round(_calc_commission(notional,sym),4),"pnl":0.0,
                            "heldDays":0,"type":"LIVE_SYNC","order_id":oid,"order_type":o.get("order_type","market"),
                            "status":"filled","strat":"manual","time":trade_ts,"date":trade_dt,
                            "timestamp":filled_at,"source":"broker_sync"})
                        synced_trades += 1
                        log.info("Sync: imported filled order %s %s %.4f @ $%.4f", side.upper(), sym, fqty, fprice)
                    if synced_trades > 0:
                        TRADES.sort(key=lambda t: t.get("timestamp",""), reverse=True)
                        save_trades()
                        STATE["tradesToday"] = len([t for t in TRADES if t.get("date")==today_str])
                        save_state()
                else:
                    errors.append(f"orders HTTP {r.status}")
    except Exception as e:
        log.error("broker_sync error: %s", e)
        return web.json_response({"ok":False,"error":str(e)},status=500)
    msg = f"Synced {synced_pos} position(s), {synced_trades} new trade(s) from Alpaca"
    log.info("broker_sync: %s", msg)
    return web.json_response({"ok":True,"synced_positions":synced_pos,"synced_trades":synced_trades,
        "total_positions":len(STATE.get("positions",[])),"total_trades":len(TRADES),
        "errors":errors,"message":msg})

@routes.post("/api/capital/manual")
async def set_capital(req):
    try:
        amount=float((await req.json()).get("amount",0))
        if amount<=0: return web.json_response({"ok":False,"error":"Must be > 0"},status=400)
        STATE.update({"portfolio":amount,"startPortfolio":amount,"brokerBalance":amount,"balanceFetched":True})
        save_state(); log.info("Manual capital: $%.2f",amount)
        return web.json_response({"ok":True,"balance":amount})
    except Exception as e: return web.json_response({"ok":False,"error":str(e)},status=400)

# ── /api/config — what options.json contains (always reliable) ─────────────
@routes.get("/api/config")
async def get_config(req):
    return web.json_response({
        "version":             "1.0.0",
        "tradingMode":         OPT["trading_mode"],
        "riskLevel":           OPT["risk_level"],
        "taxBracket":          OPT["tax_bracket"],
        "stopLossPct":         OPT["stop_loss_pct"],
        "takeProfitPct":       OPT["take_profit_pct"],
        "maxPositionPct":      OPT["max_position_pct"],
        "dailyLossLimit":      OPT["daily_loss_limit"],
        "allowFractional":     OPT.get("allow_fractional", False),
        "allowMargin":         OPT.get("allow_margin", False),
        "buyingPower":         STATE.get("buyingPower", 0),
        "cash":                STATE.get("cash", 0),
        "autoAnalysisInterval":OPT["auto_analysis_interval"],
        "marketDataSource":    OPT["market_data_source"],
        "activeSources":       [s for s,k in _get_active_sources()],
        "alpacaStream": {
            "enabled": OPT.get("alpaca_stream_enabled", False),
            "feed":    OPT.get("alpaca_stream_feed", "iex"),
            "status":  STATE.get("alpacaStreamStatus", "disabled"),
        },
        "dataSources": {
            str(i): {"source": OPT.get(f"data_source_{i}",""), "hasKey": bool(OPT.get(f"data_key_{i}",""))}
            for i in range(1, 6)
        },
        "brokerPlatform":      OPT["broker_platform"],
        "brokerUrl":           OPT["broker_api_url"],
        "hasAnthropicKey":     bool(OPT["anthropic_api_key"]),
        "hasBrokerKey":        bool(OPT["broker_api_key"]),
        "hasMarketKey":        bool(OPT["market_data_key"]),
        # Aliases for restoreUIFromState compatibility
        "eff_mktSrc":          OPT["market_data_source"],
        "eff_brokerPlatform":  OPT["broker_platform"],
        "eff_brokerUrl":       OPT["broker_api_url"],
        "eff_hasAnthropicKey": bool(OPT["anthropic_api_key"]),
        "eff_hasBrokerKey":    bool(OPT["broker_api_key"]),
        "eff_hasMarketKey":    bool(OPT["market_data_key"]),
        "opt_brokerPlatform":  OPT["broker_platform"],
        "opt_brokerUrl":       OPT["broker_api_url"],
        "opt_hasAnthropicKey": bool(OPT["anthropic_api_key"]),
        "opt_hasBrokerKey":    bool(OPT["broker_api_key"]),
        # Screener settings — needed by UI to populate sidebar
        "screenerProvider":    OPT.get("screener_provider", "disabled"),
        "screenerUrl":         OPT.get("screener_url", "http://localhost:11434"),
        "screenerModel":       OPT.get("screener_model", "llama3.2"),
        "screenerInterval":    OPT.get("screener_interval", 120),
        "screenerTopN":        OPT.get("screener_top_n", 10),
        "screenerMinScore":    OPT.get("screener_min_score", 60),
    })

# ── /api/ui/save — saves to options.json via Supervisor API ───────────────
@routes.post("/api/ui/save")
async def save_ui(req):
    """Save UI-entered values to options.json via Supervisor API. Persists across all restarts."""
    try:
        d = await req.json()
        # Map UI field names to options.json field names
        UI_TO_OPT = {
            "anthropicKey":   "anthropic_api_key",
            "mktSrc":         "market_data_source",
            "mktApiKey":      "market_data_key",
            "brokerPlatform": "broker_platform",
            "brokerUrl":      "broker_api_url",
            "brokerApiKey":   "broker_api_key",
            "brokerApiSecret":"broker_api_secret",
            "tradingMode":    "trading_mode",
            "riskLevel":      "risk_level",
            "taxBracket":     "tax_bracket",
            # Multi-source data fields
            "dataSource1": "data_source_1", "dataKey1": "data_key_1",
            "dataSource2": "data_source_2", "dataKey2": "data_key_2",
            "dataSource3": "data_source_3", "dataKey3": "data_key_3",
            "dataSource4": "data_source_4", "dataKey4": "data_key_4",
            "dataSource5": "data_source_5", "dataKey5": "data_key_5",
            # Alpaca stream
            "alpacaStreamFeed": "alpaca_stream_feed",
            # Screener config (camelCase versions)
            "screenerProvider":  "screener_provider",
            "screenerUrl":       "screener_url",
            "screenerModel":     "screener_model",
            "screenerApiKey":    "screener_api_key",
            "screenerInterval":  "screener_interval",
            "screenerTopN":      "screener_top_n",
            "screenerMinScore":  "screener_min_score",
            # Trade execution settings
            "allowFractional":   "allow_fractional",
            "allowMargin":       "allow_margin",
        }
        saves = {}
        for ui_key, opt_key in UI_TO_OPT.items():
            if ui_key in d:
                val = d[ui_key]
                if val is not None and str(val).strip() != "":
                    saves[opt_key] = val
        # Also allow direct snake_case passthrough for screener_ and other direct keys
        # (JS saveScreenerConfig sends snake_case directly)
        DIRECT_KEYS = {
            "screener_provider", "screener_url", "screener_model", "screener_api_key",
            "screener_interval", "screener_top_n", "screener_min_score",
            "trading_mode", "alpaca_stream_enabled", "alpaca_stream_feed",
        }
        for key in DIRECT_KEYS:
            if key in d and d[key] is not None and str(d[key]).strip() != "":
                saves[key] = d[key]
        if saves:
            ok = await _save_options_via_supervisor(saves)
            # Apply to live OPT immediately so changes take effect without restart
            for k, v in saves.items():
                if k in OPT:
                    # Cast to correct type
                    if isinstance(OPT[k], bool):
                        OPT[k] = str(v).lower() in ("true","1","yes")
                    elif isinstance(OPT[k], int):
                        try: OPT[k] = int(v)
                        except: pass
                    elif isinstance(OPT[k], float):
                        try: OPT[k] = float(v)
                        except: pass
                    else:
                        OPT[k] = v
                else:
                    OPT[k] = v  # new key — set directly
            # Sync tradingMode to STATE if changed
            if "trading_mode" in saves:
                STATE["tradingMode"] = OPT.get("trading_mode","paper")
            safe_keys = [k for k in saves if "key" not in k and "secret" not in k]
            log.info("UI saved + applied live: %s (supervisor=%s)", safe_keys, ok)
            return web.json_response({"ok":True,"saved":list(saves.keys())})
        log.info("UI saved: [] (nothing to save)")
        return web.json_response({"ok":True,"saved":[]})
    except Exception as e:
        return web.json_response({"ok":False,"error":str(e)},status=400)

# Keep compat alias
@routes.post("/api/config/keys")
async def config_keys_compat(req):
    try:
        d=await req.json()
        UI_TO_OPT={"anthropicKey":"anthropic_api_key","mktSrc":"market_data_source","mktApiKey":"market_data_key"}
        saves={UI_TO_OPT[k]:v for k,v in d.items() if k in UI_TO_OPT and v not in (None,"")}
        if saves: await _save_options_via_supervisor(saves)
        return web.json_response({"ok":True})
    except Exception as e: return web.json_response({"ok":False,"error":str(e)},status=400)

SCAN_PROMPTS={
    "full":      "Scan all symbols. For each: SYMBOL | BUY/SELL/HOLD | CONF% | Entry $X | Target $X | Stop $X | Reason\nThen: MARKET OVERVIEW (3 sentences), TOP TRADE, RISK WARNING.",
    "portfolio": "Review open positions. For each: HOLD/EXIT/ADD + reason. Allocation analysis, top 3 actions.",
    "tax":       "Tax optimization: 1.Liability 2.Harvesting 3.LTCG staging 4.Wash sale risks 5.Year-end moves",
    "risk":      "Risk assessment: 1.Heat map 2.Correlation 3.Max drawdown 4.VIX exposure 5.Immediate actions",
}

@routes.post("/api/claude/chat")
async def claude_chat(req):
    try:
        d=await req.json()
        if d.get("apiKey","").strip():
            await _save_options_via_supervisor({"anthropic_api_key":d["apiKey"].strip()})
        result=await call_claude(d.get("prompt",""),int(d.get("maxTokens",1000)))
        if result["ok"]:
            ANALYSES.insert(0,{"type":"chat","ts":datetime.now().strftime("%H:%M:%S"),
                                "prompt":d.get("prompt","")[:100],"text":result["text"],
                                "savedAt":_et_now().isoformat()})
            save_analyses()
        return web.json_response(result)
    except Exception as e: return web.json_response({"ok":False,"text":str(e)},status=500)

@routes.post("/api/claude/scan")
async def claude_scan(req):
    try:
        d=await req.json()
        if d.get("apiKey","").strip():
            await _save_options_via_supervisor({"anthropic_api_key":d["apiKey"].strip()})
        result=await call_claude(SCAN_PROMPTS.get(d.get("type","full"),SCAN_PROMPTS["full"]),1600)
        if result["ok"]:
            ANALYSES.insert(0,{"type":d.get("type","full"),"ts":datetime.now().strftime("%H:%M:%S"),
                                "text":result["text"],"prices":dict(STATE["prices"]),
                                "savedAt":_et_now().isoformat()})
            save_analyses()
            log.info("Claude %s scan complete",d.get("type","full"))
        return web.json_response(result)
    except Exception as e: return web.json_response({"ok":False,"text":str(e)},status=500)

@routes.post("/api/claude/symbol")
async def claude_symbol(req):
    try:
        d=await req.json()
        if d.get("apiKey","").strip():
            await _save_options_via_supervisor({"anthropic_api_key":d["apiKey"].strip()})
        sym=d.get("symbol","SPY"); price=STATE["prices"].get(sym,0)
        prompt=f"Deep dive {sym} at ${price:,.2f}: technical (RSI/MACD/S&R), exact entry, T1/T2 targets, stop, position size, R:R, time horizon, VERDICT + confidence%, key levels."
        result=await call_claude(prompt,1000)
        if result["ok"]:
            ANALYSES.insert(0,{"type":"deepDive","sym":sym,"ts":datetime.now().strftime("%H:%M:%S"),
                                "text":result["text"],"savedAt":_et_now().isoformat()})
            save_analyses()
        return web.json_response(result)
    except Exception as e: return web.json_response({"ok":False,"text":str(e)},status=500)

@routes.post("/api/trade/size")
async def get_position_size(req):
    try:
        d=await req.json()
        sym=(d.get("symbol") or "").upper().strip()
        price=float(d.get("price") or STATE["prices"].get(sym,0) or 0)
        if not sym or not price: return web.json_response({"ok":False,"error":"symbol and price required"},status=400)
        return web.json_response(calc_position(sym,price,d.get("side","buy")))
    except Exception as e: return web.json_response({"ok":False,"error":str(e)},status=400)

@routes.post("/api/trade/execute")
async def execute_trade(req):
    try:
        d=await req.json()
        symbol=(d.get("symbol") or "").upper().strip()
        side  =(d.get("side")   or "buy").lower()
        # Server is authoritative for trading mode — always read from OPT/config
        # This prevents the UI sending stale "paper" when live is configured
        mode = OPT.get("trading_mode", "paper")
        client_mode = d.get("mode", "")
        if client_mode and client_mode != mode:
            log.info("execute_trade: client sent mode=%s but config says mode=%s — using config",
                     client_mode, mode)
        if not symbol or side not in ("buy","sell"):
            return web.json_response({"ok":False,"error":"symbol and side (buy/sell) required"},status=400)
        # Get price from request, then state cache, then fetch on-demand
        _req_price = d.get("price")
        price = float(_req_price) if _req_price else float(STATE["prices"].get(symbol, 0) or 0)
        if price <= 0:
            log.info("execute_trade: fetching price on-demand for %s", symbol)
            try:
                async with ClientSession(connector=aiohttp.TCPConnector(ssl=True),
                                         timeout=ClientTimeout(total=12)) as _s:
                    price = await _yahoo(_s, symbol) or 0.0
                if price > 0:
                    STATE["prices"][symbol] = price
                    log.info("execute_trade: on-demand price %s = $%.4f", symbol, price)
                else:
                    return web.json_response(
                        {"ok": False, "error": f"Could not fetch price for {symbol}. Is the market open? Is the ticker valid?"},
                        status=400)
            except Exception as e:
                log.warning("execute_trade: price fetch error for %s: %s", symbol, e)
                return web.json_response(
                    {"ok": False, "error": f"Price fetch failed for {symbol}: {e}"},
                    status=400)
        sizing=calc_position(symbol,price,side)
        if not sizing["ok"]: return web.json_response(sizing,status=400)

        mkt=market_status(symbol); is_crypto="/" in symbol or symbol in CRYPTO_SYMBOLS
        if mode=="paper":
            # Shorts must be whole shares only — most brokers don't allow fractional shorts
            is_short = side in ("sell","short")
            qty = sizing["qty_whole"] if is_short else sizing["qty_frac"]
            if qty <= 0:
                return web.json_response({"ok":False,"error":f"Insufficient capital or price too high for {side} of {symbol}"},status=400)
            if side=="buy":
                STATE["positions"].append({"sym":symbol,"dir":"LONG","entry":price,"size":qty,
                    "stop":sizing["stop"],"target":sizing["target"],"open":int(time.time()*1000)})
            # Deduct sell-side commission from P&L on close
            sell_commission = _calc_commission(qty * price, symbol)
            if sell_commission > 0:
                STATE["portfolio"] = STATE.get("portfolio",0) - sell_commission
                STATE["todayPnl"]  = STATE.get("todayPnl",0)  - sell_commission
            STATE["tradesToday"]=STATE.get("tradesToday",0)+1; save_state()
            log.info("Paper %s %s %.6f @ $%.4f ($%.2f)",side.upper(),symbol,qty,price,sizing["notional"])
            return web.json_response({"ok":True,"mode":"paper","symbol":symbol,"side":side,
                "qty":qty,"notional":sizing["notional"],"price":price,
                "stop":sizing["stop"],"target":sizing["target"],"rr_ratio":sizing["rr_ratio"],
                "order_type":"fractional_paper","fractional":True,
                "market_open":mkt["open"],"market_session":mkt["session"],
                "message":f"Paper: {side.upper()} {qty:.6f} {symbol} @ ${price:,.4f}"})

        # Allow extended-hours orders — Alpaca supports pre/after-market trading.
        # Regular session flag is informational; broker will accept or reject.
        if not mkt["open"] and not is_crypto:
            log.info("Manual order outside regular hours (%s) — submitting to broker", mkt["session"])

        platform=OPT["broker_platform"]; api_key=OPT["broker_api_key"]
        api_secret=OPT["broker_api_secret"]; url_base=OPT["broker_api_url"]
        if not api_key: return web.json_response({"ok":False,"error":"No broker key. Add broker_api_key in HA Configuration tab."},status=400)

        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            if platform=="alpaca":
                base=_alpaca_base(url_base)
                h={"APCA-API-KEY-ID":api_key,"APCA-API-SECRET-KEY":api_secret,"Content-Type":"application/json"}
                result=await _place_alpaca_order(session,base,h,symbol,side,sizing["notional"],sizing["qty_whole"],price)
                if not result["ok"]: return web.json_response(result,status=400)
                # ── Submit broker-side GTC stop + take-profit (live mode only) ──────────────
                _m_stop_oid = None
                _m_tp_oid   = None
                if OPT["trading_mode"] == "live":
                    _is_ext_manual = not market_status(symbol)["open"] and "/" not in symbol and symbol not in CRYPTO_SYMBOLS
                    _m_alpaca_side = "buy" if side == "buy" else "sell"
                    _m_stops = await _submit_broker_stop(
                        session, base, h, symbol, _m_alpaca_side,
                        result["qty"], sizing["stop"], sizing["target"], _is_ext_manual)
                    _m_stop_oid = _m_stops.get("stop_order_id")
                    _m_tp_oid   = _m_stops.get("tp_order_id")
                if side in ("buy", "short"):
                    STATE["positions"].append({"sym":symbol,
                        "dir":"LONG" if side=="buy" else "SHORT",
                        "entry":result["price"],"size":result["qty"],
                        "stop":sizing["stop"],"target":sizing["target"],
                        "open":int(time.time()*1000),
                        "broker_stop_id":_m_stop_oid,"broker_tp_id":_m_tp_oid})
                # Deduct commission — use result["notional"] (live path has no local qty variable)
                live_qty = result.get("qty", sizing["qty_frac"])
                sell_commission = _calc_commission(result.get("notional", live_qty * price), symbol)
                if sell_commission > 0:
                    STATE["portfolio"] = STATE.get("portfolio",0) - sell_commission
                    STATE["todayPnl"]  = STATE.get("todayPnl",0)  - sell_commission
                STATE["tradesToday"]=STATE.get("tradesToday",0)+1; save_state()
                TRADES.insert(0,{"sym":symbol,"side":side,"dir":"LONG" if side=="buy" else "SHORT",
                    "qty":result["qty"],"notional":result["notional"],"entry":result["price"],
                    "stop":sizing["stop"],"target":sizing["target"],"pnl":0.0,"heldDays":0,
                    "type":"LIVE","order_id":result["order_id"],"order_type":result["order_type"],
                    "status":result["status"],"time":datetime.now().strftime("%H:%M:%S"),
                    "date":datetime.now().strftime("%Y-%m-%d"),"strat":"manual",
                    "timestamp":datetime.now().isoformat(),
                    "broker_stop_id":_m_stop_oid,"broker_tp_id":_m_tp_oid})
                save_trades()
                is_frac=result["order_type"]=="notional_fractional"
                return web.json_response({**result,"stop":sizing["stop"],"target":sizing["target"],
                    "rr_ratio":sizing["rr_ratio"],"fractional":is_frac,
                    "message":f"{'Fractional' if is_frac else 'Whole'} order: {side.upper()} {result['qty']:.6f} {symbol} @ ~${result['price']:,.4f}"})
            else:
                return web.json_response({"ok":False,"error":f"Live orders: only Alpaca supported. Platform='{platform}'."},status=400)
    except Exception as e:
        log.error("execute_trade: %s",e)
        return web.json_response({"ok":False,"error":str(e)},status=500)

@routes.get("/api/screener/status")
async def screener_status(req):
    """Return current screener state and recent candidates."""
    candidates = STATE.get("screenerCandidates",[])
    return web.json_response({
        "ok":           True,
        "active":       STATE.get("screenerActive", False),
        "provider":     STATE.get("screenerProvider","disabled"),
        "last_run":     STATE.get("screenerLastRun"),
        "candidates":   candidates,
        "candidate_count": len(candidates),
        "config": {
            "provider":     OPT.get("screener_provider","disabled"),
            "model":        OPT.get("screener_model",""),
            "url":          OPT.get("screener_url",""),
            "interval":     OPT.get("screener_interval",120),
            "top_n":        OPT.get("screener_top_n",75),
            "min_score":    OPT.get("screener_min_score",60),
        }
    })


@routes.post("/api/screener/test")
async def screener_test(req):
    """Test screener connectivity with a single symbol ping."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    # Allow test to pass provider/url/model from request for testing before save
    provider = (body.get("provider") or OPT.get("screener_provider","disabled")).lower()
    if provider == "disabled":
        return web.json_response({"ok":False,"error":"Screener is disabled. Select a provider and save first."})
    test_prompt = "Score AAPL from 0-100 for momentum. Respond with exactly: AAPL | SCORE | REASON"
    try:
        if provider == "ollama":
            raw = await _call_screener_ollama(test_prompt)
        elif provider in ("openai","groq"):
            raw = await _call_screener_openai(test_prompt)
        else:
            return web.json_response({"ok":False,"error":f"Unknown provider: {provider}"})
        return web.json_response({
            "ok":       bool(raw),
            "provider": provider,
            "model":    OPT.get("screener_model",""),
            "response": raw[:300] if raw else "No response",
            "latency":  "see logs"
        })
    except Exception as e:
        return web.json_response({"ok":False,"error":str(e)},status=500)




# ── Tax Summary API ─────────────────────────────────────────────────────────
@routes.get("/api/tax")
async def get_tax_summary(req):
    """Return comprehensive tax summary computed from full TRADES history."""
    try:
        bracket  = float(OPT.get("tax_bracket", 0.24))
        trades_w_pnl = [t for t in TRADES if t.get("pnl") is not None]
        summary = _tax_summary(trades_w_pnl, bracket)
        # Also build harvest candidates from open positions
        px_now = STATE.get("prices", {})
        harvest = []
        for p in STATE.get("positions", []):
            sym   = p.get("sym", "")
            entry = float(p.get("entry", 0))
            cur   = float(px_now.get(sym, 0) or entry)
            size  = float(p.get("size", 0))
            upnl  = (cur - entry) * size * (1 if p.get("dir","LONG")=="LONG" else -1)
            held_ms = int(time.time()*1000) - p.get("open", int(time.time()*1000))
            held_days = held_ms / 86400000
            if upnl < -50:
                harvest.append({
                    "sym": sym, "upnl": round(upnl, 2),
                    "held_days": round(held_days, 1),
                    "lt_eligible": held_days >= 365,
                    "tax_savings": round(abs(upnl) * (0.15 if held_days >= 365 else bracket), 2)
                })
        harvest.sort(key=lambda x: x["upnl"])
        return web.json_response({**summary, "harvest_candidates": harvest,
                                   "trade_count": len(trades_w_pnl),
                                   "bracket": bracket})
    except Exception as e:
        log.error("tax summary: %s", e)
        return web.json_response({"error": str(e)}, status=500)


# ── Rules Engine API ─────────────────────────────────────────────────────────

@routes.get("/api/rules")
async def get_rules(req):
    """Return all current bot rules in one response."""
    rc   = STATE.get("riskCfg", {})
    sw   = STATE.get("stratWeights", {})
    swr  = STATE.get("stratWR", {})
    thr  = AI_INTERVENTION_THRESHOLDS
    return web.json_response({
        # ── Core risk params ──
        "stop_loss_pct":        float(str(rc.get("stopL",  f"{OPT.get('stop_loss_pct',2.0)}%")).replace("%","")),
        "take_profit_pct":      float(str(rc.get("takeP",  f"{OPT.get('take_profit_pct',6.0)}%")).replace("%","")),
        "max_position_pct":     float(str(rc.get("maxPos", f"{OPT.get('max_position_pct',5.0)}%")).replace("%","")),
        "daily_loss_limit":     float(OPT.get("daily_loss_limit", 500)),
        "max_short_hold_days":  int(STATE.get("maxShortHoldDays", 5)),
        "allow_fractional":     bool(OPT.get("allow_fractional", False)),
        "allow_margin":         bool(OPT.get("allow_margin", False)),
        # ── AI intervention thresholds ──
        "ai_consecutive_losses":   int(thr["consecutive_losses"]),
        "ai_wr_floor_pct":         int(thr["wr_floor_pct"]),
        "ai_wr_min_trades":        int(thr["wr_min_trades"]),
        "ai_daily_loss_pct":       float(thr["daily_loss_pct"]),
        "ai_drawdown_pct":         float(thr["drawdown_pct"]),
        "ai_vix_spike":            float(thr["vix_spike"]),
        "ai_deep_review_hours":    int(thr["deep_review_hours"]),
        "ai_approve_new_entries":  bool(thr.get("ai_approve_new_entries", False)),
        # ── Confidence thresholds ──
        "conf_min_wr_low":  75,   # WR < 45%
        "conf_min_wr_mid":  70,   # WR 45-55%
        "conf_min_wr_high": 65,   # WR > 55%
        # ── Strategy weights & win rates ──
        "strat_weights": sw,
        "strat_wr":      swr,
        # ── Screener settings ──
        "screener_provider":  OPT.get("screener_provider", "disabled"),
        "screener_interval":  int(OPT.get("screener_interval", 120)),
        "screener_top_n":     int(OPT.get("screener_top_n", 75)),
        "screener_min_score": int(OPT.get("screener_min_score", 60)),
        # ── Session cadence ──
        "scan_interval_regular":    180,
        "scan_interval_extended":   1800,
        "scan_interval_overnight":  3600,
        # ── Performance context ──
        "win_rate":       round(STATE.get("winCount",0) / max(1, STATE.get("winCount",0)+STATE.get("lossCount",0)) * 100, 1),
        "today_pnl":      STATE.get("todayPnl", 0),
        "portfolio":      STATE.get("portfolio", 0),
        "hw_mark":        STATE.get("portfolioHighWater", 0),
        "rules_cycles":   STATE.get("rulesEngineCycles", 0),
        "ai_cycles":      STATE.get("aiAdviserCycles", 0),
        "ai_active":      STATE.get("aiAdviserActive", False),
        "ai_reason":      STATE.get("aiAdviserReason", ""),
        "claude_overrides": STATE.get("claudeOverrides", {}),
        "rules_history":    STATE.get("rulesHistory", [])[:10],
    })

@routes.post("/api/rules")
async def set_rules(req):
    """Update bot rules. Applies immediately to running bot.
    Accepts optional fields:
      source: "user" | "claude" (default "user")
      reasoning: str — shown in UI when source="claude"
    User changes always apply. Claude changes are marked so UI can show priority info.
    """
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    source    = d.pop("source", "user")        # "user" or "claude"
    reasoning = d.pop("reasoning", "")         # claude's reasoning for this change
    changed   = []
    _ts       = _et_now().isoformat()

    # ── Risk params → riskCfg ──
    rc = STATE.setdefault("riskCfg", {})
    if "stop_loss_pct" in d:
        v = max(0.5, min(10.0, float(d["stop_loss_pct"])))
        rc["stopL"] = f"{v}%"; OPT["stop_loss_pct"] = v
        changed.append(f"stop_loss={v}%")
    if "take_profit_pct" in d:
        v = max(1.0, min(25.0, float(d["take_profit_pct"])))
        rc["takeP"] = f"{v}%"; OPT["take_profit_pct"] = v
        changed.append(f"take_profit={v}%")
    if "max_position_pct" in d:
        v = max(1.0, min(20.0, float(d["max_position_pct"])))
        rc["maxPos"] = f"{v}%"; OPT["max_position_pct"] = v
        changed.append(f"max_pos={v}%")
    if "daily_loss_limit" in d:
        v = max(100.0, float(d["daily_loss_limit"]))
        OPT["daily_loss_limit"] = v; changed.append(f"daily_loss_limit=${v:.0f}")
    if "max_short_hold_days" in d:
        v = max(1, min(30, int(d["max_short_hold_days"])))
        STATE["maxShortHoldDays"] = v; changed.append(f"max_short_hold={v}d")

    # ── AI thresholds ──
    thr = AI_INTERVENTION_THRESHOLDS
    if "ai_consecutive_losses" in d:
        v = max(2, min(10, int(d["ai_consecutive_losses"])))
        thr["consecutive_losses"] = v; changed.append(f"ai_consec_loss={v}")
    if "ai_wr_floor_pct" in d:
        v = max(10, min(60, int(d["ai_wr_floor_pct"])))
        thr["wr_floor_pct"] = v; changed.append(f"ai_wr_floor={v}%")
    if "ai_daily_loss_pct" in d:
        v = max(0.5, min(15.0, float(d["ai_daily_loss_pct"])))
        thr["daily_loss_pct"] = v; changed.append(f"ai_daily_loss={v}%")
    if "ai_drawdown_pct" in d:
        v = max(2.0, min(30.0, float(d["ai_drawdown_pct"])))
        thr["drawdown_pct"] = v; changed.append(f"ai_drawdown={v}%")
    if "ai_vix_spike" in d:
        v = max(20.0, min(80.0, float(d["ai_vix_spike"])))
        thr["vix_spike"] = v; changed.append(f"ai_vix={v}")
    if "ai_deep_review_hours" in d:
        v = max(1, min(168, int(d["ai_deep_review_hours"])))
        thr["deep_review_hours"] = v; changed.append(f"ai_review={v}h")
    if "ai_approve_new_entries" in d:
        thr["ai_approve_new_entries"] = bool(d["ai_approve_new_entries"])
        changed.append(f"ai_approve_entries={thr['ai_approve_new_entries']}")

    # ── Strategy weights ──
    if "strat_weights" in d:
        sw = d["strat_weights"]
        if isinstance(sw, dict) and len(sw) >= 3:
            total = sum(float(v) for v in sw.values())
            if total > 0:
                STATE["stratWeights"] = {k: round(float(v)/total, 4) for k,v in sw.items()}
                changed.append("strat_weights updated")

    save_state()

    # ── Record change history and Claude overrides ────────────────────────
    if changed:
        entry = {"ts": _ts, "source": source, "changes": changed,
                 "reasoning": reasoning[:200] if reasoning else ""}
        history = STATE.setdefault("rulesHistory", [])
        history.insert(0, entry)
        STATE["rulesHistory"] = history[:20]  # keep last 20 entries

        if source == "claude":
            overrides = STATE.setdefault("claudeOverrides", {})
            for c in changed:
                field = c.split("=")[0].strip()
                overrides[field] = {"ts": _ts, "value": c, "reasoning": reasoning[:200]}
        else:
            # User explicitly changing a field removes Claude's override flag for it
            overrides = STATE.setdefault("claudeOverrides", {})
            for c in changed:
                field = c.split("=")[0].strip()
                overrides.pop(field, None)

    log.info("Rules updated [%s]: %s", source, " | ".join(changed))
    OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "li",
        "msg": f"{'🤖' if source=='claude' else '👤'} Rules [{source}]: {' | '.join(changed) if changed else 'no changes'}"})
    save_optlog()
    return web.json_response({"ok": True, "changed": changed, "source": source})

@routes.post("/api/rules/optimize")
async def optimize_rules(req):
    """
    Ask Claude to review current rules and suggest optimal parameters.
    Called when performance is poor OR manually by user.
    """
    try:
        d = await req.json()
    except Exception:
        d = {}

    force = d.get("force", False)   # True = user-triggered regardless of performance

    # Build optimization context
    rc   = STATE.get("riskCfg", {})
    sw   = STATE.get("stratWeights", {})
    swr  = STATE.get("stratWR", {})
    wins = STATE.get("winCount", 0)
    losses = STATE.get("lossCount", 0)
    tot  = wins + losses
    wr   = round(wins / tot * 100, 1) if tot else 0
    pnl  = STATE.get("todayPnl", 0)
    port = STATE.get("portfolio", 0)
    hwm  = STATE.get("portfolioHighWater", port)
    dd   = round((hwm - port) / hwm * 100, 2) if hwm > 0 else 0
    vix  = float(STATE.get("sentData", {}).get("VIX", 20))
    thr  = AI_INTERVENTION_THRESHOLDS
    max_short = STATE.get("maxShortHoldDays", 5)
    rules_cyc = STATE.get("rulesEngineCycles", 0)
    ai_cyc    = STATE.get("aiAdviserCycles", 0)

    OPT_PROMPT = f"""You are APEX Rules Optimizer. Review the bot's current rules and suggest improvements for maximum profit with minimum loss.

CURRENT PERFORMANCE:
  Portfolio: ${port:,.2f} | High-water: ${hwm:,.2f} | Drawdown: {dd:.1f}%
  Win Rate: {wr}% ({wins}W/{losses}L of {tot} trades)
  Today P&L: ${pnl:+,.2f} | VIX: {vix:.1f}
  Engine cycles: {rules_cyc} rules / {ai_cyc} AI ({round(ai_cyc/(rules_cyc+ai_cyc)*100) if rules_cyc+ai_cyc>0 else 0}% AI)

CURRENT RULES:
  Stop Loss:      {rc.get('stopL','2%')}
  Take Profit:    {rc.get('takeP','6%')}
  Max Position:   {rc.get('maxPos','5%')} of portfolio
  Daily Loss Limit: ${OPT.get('daily_loss_limit',500):,.0f}
  Max Short Hold: {max_short} days

  AI Triggers:
    Consecutive losses: {thr['consecutive_losses']}
    WR floor: {thr['wr_floor_pct']}% (min {thr['wr_min_trades']} trades)
    Daily loss: {thr['daily_loss_pct']}% of portfolio
    Drawdown: {thr['drawdown_pct']}%
    VIX spike: {thr['vix_spike']}
    Deep review: every {thr['deep_review_hours']}h

  Strategy Weights & Win Rates:
{chr(10).join(f"    {k}: {round(v*100)}% weight | {swr.get(k,0):.1f}% WR" for k,v in sw.items())}

OPTIMIZATION REQUEST: {"USER-FORCED REVIEW" if force else "PERFORMANCE REVIEW"}

Analyze the rules above against the performance data. Suggest specific numeric changes that will:
1. Maximize profit capture (improve take_profit timing)
2. Minimize losses (tighten stops if WR is low, loosen if chopping out)
3. Tune AI intervention thresholds (should AI trigger more or less often?)
4. Rebalance strategy weights toward higher-WR strategies
5. Adjust max_short_hold_days based on current market conditions

OUTPUT FORMAT — use EXACTLY these lines (parser reads them):
RULE_UPDATE:
stop_loss_pct: X.X
take_profit_pct: X.X
max_position_pct: X.X
daily_loss_limit: XXXX
max_short_hold_days: X
ai_consecutive_losses: X
ai_wr_floor_pct: XX
ai_daily_loss_pct: X.X
ai_drawdown_pct: X.X
ai_vix_spike: XX
ai_deep_review_hours: XX
strat_momentum: XX
strat_meanRev: XX
strat_breakout: XX
strat_sentiment: XX
strat_mlPat: XX
END_RULE_UPDATE

REASONING: (2-3 sentences explaining the key changes and expected impact)"""

    result = await call_claude(OPT_PROMPT, 600)
    if not result["ok"]:
        return web.json_response({"ok": False, "error": "Claude unavailable: " + result.get("text","")})

    text = result["text"]

    # Auto-apply parsed rules from Claude's response
    applied = []
    ru_start = text.find("RULE_UPDATE:")
    ru_end   = text.find("END_RULE_UPDATE")
    if ru_start > 0 and ru_end > ru_start:
        block = text[ru_start+len("RULE_UPDATE:"):ru_end]
        rc = STATE.setdefault("riskCfg", {})
        for line in block.splitlines():
            line = line.strip()
            if not line or ':' not in line: continue
            key, _, val = line.partition(':')
            key = key.strip(); val = val.strip()
            try:
                if key == "stop_loss_pct":
                    v = max(0.5, min(10.0, float(val)))
                    rc["stopL"] = f"{v}%"; OPT["stop_loss_pct"] = v; applied.append(f"stop={v}%")
                elif key == "take_profit_pct":
                    v = max(1.0, min(25.0, float(val)))
                    rc["takeP"] = f"{v}%"; OPT["take_profit_pct"] = v; applied.append(f"tp={v}%")
                elif key == "max_position_pct":
                    v = max(1.0, min(20.0, float(val)))
                    rc["maxPos"] = f"{v}%"; OPT["max_position_pct"] = v; applied.append(f"maxpos={v}%")
                elif key == "daily_loss_limit":
                    v = max(100.0, float(val))
                    OPT["daily_loss_limit"] = v; applied.append(f"dll=${v:.0f}")
                elif key == "max_short_hold_days":
                    v = max(1, min(30, int(float(val))))
                    STATE["maxShortHoldDays"] = v; applied.append(f"short_hold={v}d")
                elif key == "ai_consecutive_losses":
                    AI_INTERVENTION_THRESHOLDS["consecutive_losses"] = max(2, min(10, int(float(val))))
                    applied.append(f"ai_consec={val}")
                elif key == "ai_wr_floor_pct":
                    AI_INTERVENTION_THRESHOLDS["wr_floor_pct"] = max(10, min(60, int(float(val))))
                    applied.append(f"ai_wr={val}%")
                elif key == "ai_daily_loss_pct":
                    AI_INTERVENTION_THRESHOLDS["daily_loss_pct"] = max(0.5, min(15.0, float(val)))
                    applied.append(f"ai_dll={val}%")
                elif key == "ai_drawdown_pct":
                    AI_INTERVENTION_THRESHOLDS["drawdown_pct"] = max(2.0, min(30.0, float(val)))
                    applied.append(f"ai_dd={val}%")
                elif key == "ai_vix_spike":
                    AI_INTERVENTION_THRESHOLDS["vix_spike"] = max(20.0, min(80.0, float(val)))
                    applied.append(f"ai_vix={val}")
                elif key == "ai_deep_review_hours":
                    AI_INTERVENTION_THRESHOLDS["deep_review_hours"] = max(1, min(168, int(float(val))))
                    applied.append(f"ai_rev={val}h")
                elif key.startswith("strat_"):
                    sname = key[6:]
                    if sname in STATE.get("stratWeights", {}):
                        STATE["stratWeights"][sname] = round(max(0.01, min(0.99, float(val)/100)), 4)
                        applied.append(f"{sname}={val}%")
            except (ValueError, TypeError):
                pass

        # Normalize strategy weights after partial update
        sw = STATE.get("stratWeights", {})
        total = sum(sw.values())
        if total > 0:
            STATE["stratWeights"] = {k: round(v/total, 4) for k,v in sw.items()}

        # Tag all applied changes as claude-sourced
        STATE.setdefault("rulesHistory", []).insert(0, {
            "ts": _et_now().isoformat(), "source": "claude",
            "changes": applied, "reasoning": ""
        })
        STATE["rulesHistory"] = STATE["rulesHistory"][:20]
        overrides = STATE.setdefault("claudeOverrides", {})
        for a in applied:
            field = a.split("=")[0].strip()
            overrides[field] = {"ts": _et_now().isoformat(), "value": a, "reasoning": ""}
        save_state()

    # Extract reasoning
    reasoning = ""
    r_idx = text.find("REASONING:")
    if r_idx > 0:
        reasoning = text[r_idx+len("REASONING:"):].strip()[:500]

    log.info("Rules optimized by Claude: %s", " | ".join(applied))
    OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "li",
        "msg": f"🤖 Claude Rules Optimization: {' | '.join(applied[:5])}"})
    save_optlog()

    # Store full analysis for display
    ANALYSES.insert(0, {"type": "rulesOptimize", "ts": _et_now().strftime("%H:%M:%S"),
                         "text": text, "savedAt": _et_now().isoformat(), "applied": applied})
    save_analyses()

    return web.json_response({"ok": True, "applied": applied, "reasoning": reasoning, "full_text": text})


# ── Wash Sale Engine ────────────────────────────────────────────────────────
# IRS §1091: selling at a loss then buying the same security within 30 days
# before or after that sale disallows the loss for current tax purposes.
# The disallowed loss is added to the replacement shares' cost basis.

_WASH_SALE_WINDOW = 30 * 86400  # 30 days in seconds

def _compute_held_days(trade: dict) -> float:
    """Calculate how many days a position was held from open → close timestamps."""
    open_ms  = trade.get("open")   or trade.get("openedAt")
    close_ms = trade.get("close")  or trade.get("closedAt")
    if open_ms and close_ms:
        try:
            return max(0, (int(close_ms) - int(open_ms)) / 86400000)  # ms → days
        except Exception:
            pass
    # Fallback: parse ISO timestamps
    ts_open  = trade.get("timestamp","")
    ts_close = trade.get("closed_at","")
    if ts_open and ts_close:
        try:
            from datetime import datetime
            fmt = "%Y-%m-%dT%H:%M:%S"
            o = datetime.fromisoformat(ts_open[:19].replace("Z",""))
            c = datetime.fromisoformat(ts_close[:19].replace("Z",""))
            return max(0, (c - o).total_seconds() / 86400)
        except Exception:
            pass
    return 0.0

def _detect_wash_sales(trades: list) -> list:
    """
    Run wash sale detection across a list of trade records.
    Returns a new list with wash_sale, disallowed_loss, and adjusted_pnl
    fields added to loss trades where the rule applies.
    
    Logic:
      For each SELL trade with pnl < 0:
        Look for any BUY of the same symbol within [close_time - 30d, close_time + 30d]
        If found → mark as wash sale, disallowed = abs(pnl)
    
    Note: This is a simplified implementation. It does not handle:
      - Options or warrants on the same stock
      - Cost-basis carryover to replacement shares
      - Partial wash sales (where replacement qty < loss qty)
    Consult a tax professional for definitive wash sale determinations.
    """
    import time as _time

    # Build a lookup: symbol → list of (timestamp_seconds, trade_index, side)
    sym_times = {}  # sym → [(ts, idx, side)]
    for i, t in enumerate(trades):
        sym  = (t.get("sym") or t.get("symbol","")).upper()
        if not sym: continue
        side = t.get("side","").lower()
        # Get timestamp in seconds
        ts_ms = t.get("close") or t.get("closedAt") or t.get("open") or 0
        try:
            ts_sec = int(ts_ms) / 1000 if ts_ms > 1e9 else int(ts_ms)
        except Exception:
            ts_sec = 0
        if not ts_sec:
            # Try ISO timestamp
            raw_ts = t.get("timestamp","")
            if raw_ts:
                try:
                    from datetime import datetime, timezone
                    ts_sec = datetime.fromisoformat(raw_ts[:19].replace("Z","")).timestamp()
                except Exception:
                    ts_sec = 0
        if sym not in sym_times:
            sym_times[sym] = []
        sym_times[sym].append((ts_sec, i, side))

    result = []
    for i, t in enumerate(trades):
        t = dict(t)  # copy to avoid mutating original
        pnl  = float(t.get("pnl", 0) or 0)
        side = t.get("side","").lower()

        # Only loss-generating sell/close trades can be wash sales
        if side in ("sell", "close") and pnl < 0:
            sym  = (t.get("sym") or t.get("symbol","")).upper()
            ts_ms = t.get("close") or t.get("closedAt") or t.get("open") or 0
            try:
                ts_sec = int(ts_ms)/1000 if ts_ms > 1e9 else int(ts_ms)
            except Exception:
                ts_sec = 0

            if ts_sec and sym in sym_times:
                # Check for any BUY of same symbol within ±30 days
                window_start = ts_sec - _WASH_SALE_WINDOW
                window_end   = ts_sec + _WASH_SALE_WINDOW
                wash_buys = [
                    (ts, idx, s) for (ts, idx, s) in sym_times[sym]
                    if s in ("buy","open") and ts != ts_sec
                    and window_start <= ts <= window_end
                    and idx != i
                ]
                if wash_buys:
                    t["wash_sale"]        = True
                    t["disallowed_loss"]  = round(abs(pnl), 4)
                    t["adjusted_pnl"]     = 0.0  # loss disallowed — treated as $0 for tax
                    t["wash_sale_note"]   = (
                        f"Loss of ${abs(pnl):.2f} disallowed — "
                        f"{sym} repurchased within 30 days (IRS §1091)"
                    )
        # Always compute held days
        t["heldDays"] = round(_compute_held_days(t), 4)
        result.append(t)

    return result

# ── Federal Long-Term Capital Gains Rate Brackets (2024) ─────────────────
# Single filer:    0% ≤ $47,025 | 15% ≤ $518,900 | 20% above
# MFJ:            0% ≤ $94,050 | 15% ≤ $583,750 | 20% above
# NIIT (§1411):   3.8% on NII for incomes above $200k (single) / $250k (MFJ)
# These are simplified — actual liability depends on total income, deductions, AMT, etc.

_LT_BRACKETS_SINGLE = [(47025, 0.00), (518900, 0.15), (float("inf"), 0.20)]
_LT_BRACKETS_MFJ    = [(94050, 0.00), (583750, 0.15), (float("inf"), 0.20)]
_NIIT_THRESHOLD_SINGLE = 200_000
_NIIT_THRESHOLD_MFJ    = 250_000
_NIIT_RATE = 0.038

def _lt_rate(agi: float, filing: str = "single") -> float:
    """Return applicable LT capital gains rate given AGI and filing status."""
    brackets = _LT_BRACKETS_MFJ if filing == "mfj" else _LT_BRACKETS_SINGLE
    for limit, rate in brackets:
        if agi <= limit:
            return rate
    return 0.20

def _niit_applies(agi: float, filing: str = "single") -> bool:
    """Return True if NIIT (§1411) applies at this AGI."""
    threshold = _NIIT_THRESHOLD_MFJ if filing == "mfj" else _NIIT_THRESHOLD_SINGLE
    return agi > threshold

def _tax_summary(trades: list, bracket: float = 0.24,
                 agi: float = 0.0, filing: str = "single") -> dict:
    """
    Comprehensive US federal tax summary for trading income.

    Rules applied:
      §1091  Wash sale — 30-day window loss disallowance
      §1222  ST (<1yr) vs LT (≥1yr) capital gains
      §1(h)  Progressive LT rates (0% / 15% / 20%) based on AGI + filing status
      §1411  NIIT — 3.8% on net investment income when AGI > $200k/$250k
      §1211  Capital loss limitation — losses limited to gains + $3,000/yr
      §1212  Loss carryover note — excess losses carry to future years
      Tax-loss harvesting — flag unrealized losses ≥$500 for harvest consideration

    Args:
      trades:  list of closed trade dicts
      bracket: ordinary income tax rate (used for short-term gains)
      agi:     estimated adjusted gross income for rate selection (0 = use bracket)
      filing:  "single" or "mfj" (married filing jointly)

    Note: This is an approximation for planning purposes only.
    Actual tax liability depends on total income, deductions, credits, state taxes,
    AMT, and other factors. Always consult a qualified tax professional.
    """
    processed = _detect_wash_sales(trades)

    st_gains        = 0.0   # §1222: short-term gains (held < 1yr), taxed as ordinary income
    lt_gains        = 0.0   # §1222: long-term gains (held ≥ 1yr), preferred rates
    lt_gains_0pct   = 0.0   # LT gains eligible for 0% rate
    lt_gains_15pct  = 0.0   # LT gains taxed at 15%
    lt_gains_20pct  = 0.0   # LT gains taxed at 20%
    st_losses       = 0.0   # deductible short-term losses
    lt_losses       = 0.0   # deductible long-term losses
    wash_disallowed = 0.0   # §1091: wash sale disallowed losses
    wash_count      = 0

    # Determine LT rate and NIIT applicability
    # If agi not provided, estimate from trading income + bracket
    effective_agi = agi if agi > 0 else (st_gains + lt_gains) / max(bracket, 0.01)
    lt_rate   = _lt_rate(effective_agi, filing)
    niit_on   = _niit_applies(effective_agi, filing)
    niit_rate = _NIIT_RATE if niit_on else 0.0

    for t in processed:
        pnl      = float(t.get("pnl", 0) or 0)
        held     = float(t.get("heldDays", 0) or 0)
        is_lt    = held >= 365
        is_wash  = t.get("wash_sale", False)

        if pnl > 0:
            if is_lt:
                lt_gains += pnl
                # Assign to bracket tier based on effective AGI + LT income
                if lt_rate == 0.0:      lt_gains_0pct  += pnl
                elif lt_rate == 0.15:   lt_gains_15pct += pnl
                else:                   lt_gains_20pct += pnl
            else:
                st_gains += pnl
        elif pnl < 0:
            if is_wash:
                wash_disallowed += abs(pnl)
                wash_count += 1
            elif is_lt:
                lt_losses += pnl   # negative
            else:
                st_losses += pnl   # negative

    # §1211: Loss netting — ST losses offset ST gains first, then LT gains
    net_st = st_gains + st_losses          # net short-term (can be negative)
    net_lt = lt_gains + lt_losses          # net long-term (can be negative)

    # §1211/§1212: Capital loss limitation — max $3,000 deductible against ordinary income
    total_net = net_st + net_lt
    deductible_against_ordinary = 0.0
    loss_carryover = 0.0
    if total_net < 0:
        deductible_against_ordinary = max(-3000.0, total_net)  # up to -$3,000
        loss_carryover = total_net - deductible_against_ordinary  # negative remainder

    # Tax calculations
    # ST gains: taxed at ordinary income rate (bracket)
    st_tax = max(0.0, net_st * bracket) if net_st > 0 else 0.0

    # LT gains: taxed at progressive rates
    lt_tax = max(0.0,
        lt_gains_0pct  * 0.00 +
        lt_gains_15pct * 0.15 +
        lt_gains_20pct * 0.20
    )

    # §1411 NIIT: 3.8% on NET investment income (NII = net gains)
    nii = max(0.0, total_net)
    niit_tax = round(nii * niit_rate, 2) if niit_on and nii > 0 else 0.0

    # §1211 ordinary income deduction (losses up to $3k)
    ord_income_deduction = abs(deductible_against_ordinary) if total_net < 0 else 0.0
    ord_tax_saved = round(ord_income_deduction * bracket, 2)

    total_tax = round(max(0.0, st_tax + lt_tax + niit_tax), 2)
    effective_rate = round(total_tax / max(1, total_net) * 100, 2) if total_net > 0 else 0.0

    # Tax-loss harvesting opportunities (unrealized positions — needs open positions)
    # (Unrealized losses are checked separately in the bot prompt)

    return {
        # Gains
        "st_gains":              round(st_gains, 2),
        "lt_gains":              round(lt_gains, 2),
        "lt_gains_0pct":         round(lt_gains_0pct, 2),
        "lt_gains_15pct":        round(lt_gains_15pct, 2),
        "lt_gains_20pct":        round(lt_gains_20pct, 2),
        # Losses
        "st_losses":             round(abs(st_losses), 2),
        "lt_losses":             round(abs(lt_losses), 2),
        "wash_disallowed":       round(wash_disallowed, 2),
        "wash_sale_count":       wash_count,
        "deductible_against_ordinary": round(ord_income_deduction, 2),
        "loss_carryover":        round(abs(loss_carryover), 2),
        # Net
        "net_st":                round(net_st, 2),
        "net_lt":                round(net_lt, 2),
        "net_taxable_gain":      round(total_net, 2),
        # Tax
        "st_tax":                round(st_tax, 2),
        "lt_tax":                round(lt_tax, 2),
        "niit_tax":              niit_tax,
        "niit_applies":          niit_on,
        "ord_tax_saved":         ord_tax_saved,
        "estimated_tax":         total_tax,
        "effective_rate":        effective_rate,
        "lt_rate_applied":       lt_rate,
        "bracket":               bracket,
        "filing":                filing,
        "agi_used":              round(effective_agi, 2),
        "note": (
            f"Short-term gains at {bracket*100:.0f}% ordinary rate. "
            f"Long-term at {lt_rate*100:.0f}% (§1(h) bracket). "
            + (f"NIIT §1411: +3.8% on NII (AGI exceeds ${_NIIT_THRESHOLD_SINGLE:,}). " if niit_on else "")
            + ("Wash sale losses (§1091) disallowed. " if wash_count else "")
            + (f"§1212 loss carryover: ${abs(loss_carryover):,.2f} to future years. " if loss_carryover < 0 else "")
            + "Consult a tax professional — this is an estimate only."
        ),
    }

# ── Weekly P&L / Tax Report ───────────────────────────────────────────────
@routes.get("/api/report/weekly")
async def weekly_report(req):
    """Generate weekly P&L and tax summary. Returns JSON + optionally calls Claude for insights."""
    from datetime import timedelta
    et     = _et_now()
    today  = et.date()
    cutoff = today - timedelta(days=7)

    # Filter trades to the past 7 days
    week_trades = []
    for t in TRADES:
        td = t.get("date","") or t.get("timestamp","")[:10] if t.get("timestamp") else ""
        try:
            from datetime import date as _date
            tdate = _date.fromisoformat(td[:10]) if td else None
            if tdate and tdate >= cutoff:
                week_trades.append(t)
        except Exception:
            pass

    # Aggregate P&L
    total_gross  = sum(t.get("gross_pnl", t.get("pnl", 0)) for t in week_trades)
    total_comm   = sum(t.get("commission", 0) for t in week_trades)
    total_net    = sum(t.get("pnl", 0) for t in week_trades)
    wins         = [t for t in week_trades if (t.get("pnl",0) or 0) > 0]
    losses       = [t for t in week_trades if (t.get("pnl",0) or 0) < 0]
    wr           = round(len(wins)/len(week_trades)*100) if week_trades else 0
    best_trade   = max(week_trades, key=lambda t: t.get("pnl",0), default=None)
    worst_trade  = min(week_trades, key=lambda t: t.get("pnl",0), default=None)

    # Per-symbol breakdown
    by_sym = {}
    for t in week_trades:
        s = t.get("sym","?")
        if s not in by_sym: by_sym[s] = {"trades":0,"wins":0,"net":0,"gross":0,"comm":0}
        by_sym[s]["trades"] += 1
        if (t.get("pnl",0) or 0) > 0: by_sym[s]["wins"] += 1
        by_sym[s]["net"]   += t.get("pnl",0) or 0
        by_sym[s]["gross"] += t.get("gross_pnl", t.get("pnl",0)) or 0
        by_sym[s]["comm"]  += t.get("commission",0) or 0

    # Tax breakdown with full wash sale detection
    tax_bracket = float(OPT.get("tax_bracket", 0.24))
    tax_data    = _tax_summary(week_trades, tax_bracket)
    st_gains    = tax_data["st_gains"]
    lt_gains    = tax_data["lt_gains"]
    st_losses   = -tax_data["st_losses"]  # negative for backward compat
    net_gains   = tax_data["net_taxable_gain"]
    tax_est     = tax_data["estimated_tax"]

    # Daily P&L series
    from datetime import timedelta as _td
    daily = {}
    for i in range(7):
        d = (today - _td(days=6-i)).isoformat()
        daily[d] = sum(t.get("pnl",0) for t in week_trades if (t.get("date","") or "")[:10] == d)

    # Portfolio context
    port_start = STATE.get("startPortfolio", STATE.get("portfolio",0))
    port_now   = STATE.get("portfolio",0)
    port_change = port_now - port_start if port_start else 0

    report = {
        "ok":          True,
        "period":      f"{cutoff.isoformat()} → {today.isoformat()}",
        "generated":   et.isoformat(),
        "summary": {
            "trades":      len(week_trades),
            "wins":        len(wins),
            "losses":      len(losses),
            "win_rate":    wr,
            "gross_pnl":   round(total_gross, 2),
            "commissions": round(total_comm, 4),
            "net_pnl":     round(total_net, 2),
            "best_trade":  {"sym": best_trade.get("sym",""), "pnl": round(best_trade.get("pnl",0),2), "date": best_trade.get("date","")} if best_trade else None,
            "worst_trade": {"sym": worst_trade.get("sym",""), "pnl": round(worst_trade.get("pnl",0),2), "date": worst_trade.get("date","")} if worst_trade else None,
            "portfolio_start": round(port_start, 2),
            "portfolio_now":   round(port_now, 2),
            "portfolio_change": round(port_change, 2),
        },
        "by_symbol": {s: {**d, "net": round(d["net"],2), "gross": round(d["gross"],2), "wr": round(d["wins"]/d["trades"]*100) if d["trades"] else 0} for s,d in sorted(by_sym.items(), key=lambda x: -x[1]["net"])},
        "daily_pnl":  {d: round(v,2) for d,v in daily.items()},
        "tax": {
            "bracket":          tax_bracket,
            "st_gains":         round(st_gains, 2),
            "lt_gains":         round(lt_gains, 2),
            "losses":           round(abs(st_losses), 2),
            "wash_disallowed":  tax_data["wash_disallowed"],
            "wash_sale_count":  tax_data["wash_sale_count"],
            "net_gains":        round(net_gains, 2),
            "estimated_tax":    round(tax_est, 2),
            "note":             tax_data["note"],
        },
        "trades": week_trades[:200],
    }
    return web.json_response(report)


@routes.get("/api/vault/status")
async def vault_status(req):
    """Return vault status — which keys are saved and when."""
    vault_path = USER_VAULT
    if not vault_path.exists():
        return web.json_response({"ok": True, "exists": False, "keys": [], "size": 0})
    try:
        import os
        vault = json.loads(vault_path.read_text())
        # Return which sensitive keys are present (not their values)
        key_categories = {
            "claude_key":    bool(vault.get("anthropic_api_key","")),
            "broker_key":    bool(vault.get("broker_api_key","")),
            "broker_secret": bool(vault.get("broker_api_secret","")),
            "broker_platform": vault.get("broker_platform",""),
            "screener_key":  bool(vault.get("screener_api_key","")),
            "data_keys":     sum(1 for i in range(1,6) if vault.get(f"data_key_{i}","")),
            "trading_mode":  vault.get("trading_mode",""),
            "total_keys":    len(vault),
        }
        mtime = os.path.getmtime(vault_path)
        from datetime import datetime
        saved_at = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
        return web.json_response({"ok": True, "exists": True, "saved_at": saved_at,
                                  "keys": key_categories, "total": len(vault)})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})

@routes.post("/api/vault/save")
async def vault_save_now(req):
    """Manually trigger a full vault snapshot of current OPT."""
    _write_user_vault(dict(OPT))
    vault_path = USER_VAULT
    total = 0
    try:
        vault = json.loads(vault_path.read_text())
        total = len(vault)
    except: pass
    log.info("Vault manually saved: %d keys", total)
    return web.json_response({"ok": True, "saved": total, "path": str(vault_path)})

@routes.get("/api/health")
async def health(req):
    return web.json_response({
        "status":             "ok",
        "version":            "1.0.0",
        "startup_time":       STARTUP_TIME,
        # Counts for smart poll — client skips fetch when unchanged
        "analyses_count":     len(ANALYSES),
        "trades_count":       len(TRADES),
        "optlog_count":       len(OPTLOG),
        "positions_count":    len(STATE.get("positions",[])),
        # Price data status
        "last_price_fetch":   STATE.get("lastPriceFetch"),
        "alpaca_stream_live": _alpaca_ws_connected,
        "alpaca_stream_status": STATE.get("alpacaStreamStatus","disabled"),
        "last_stream_update": STATE.get("lastStreamUpdate"),
        "stream_symbol_count": len([v for v in STATE.get("prices",{}).values() if v > 0]),
        "price_mode": "sip_primary" if (_alpaca_ws_connected and OPT.get("alpaca_stream_enabled") and market_status().get("open")) else "polling",
        "bot_loop_error": STATE.get("botLoopError"),
        "aiAdviserActive":    STATE.get("aiAdviserActive", False),
        "aiAdviserReason":    STATE.get("aiAdviserReason", ""),
        "rulesEngineCycles":  STATE.get("rulesEngineCycles", 0),
        "aiAdviserCycles":    STATE.get("aiAdviserCycles", 0),
        "portfolioHighWater": STATE.get("portfolioHighWater", 0.0),   # non-None if bot loop crashed last cycle

        # Bot / broker status
        "botActive":          STATE.get("botActive", False),
        "portfolio":          STATE.get("portfolio", 0),
        "hasAnthropicKey":    bool(OPT.get("anthropic_api_key","")),
        "hasBrokerKey":       bool(OPT.get("broker_api_key","")),
        "botTradesTotal":     STATE.get("tradesToday",0),
        "botTradesWeek":      len([t for t in TRADES if t.get("date","") >= 
                              (__import__("datetime").date.today() - 
                               __import__("datetime").timedelta(days=7)).isoformat()]),
        "brokerConnected":    STATE.get("connected", False),
        # Screener status — show configured provider even before first run
        "screenerActive":     STATE.get("screenerActive", False),
        "screenerProvider":   OPT.get("screener_provider", STATE.get("screenerProvider","disabled")),
        "screenerLastRun":    STATE.get("screenerLastRun"),
        "screenerCandidates": len(STATE.get("screenerCandidates",[])),
        "screenerConfigured": OPT.get("screener_provider","disabled") != "disabled",
    })

@routes.post("/api/bot/restart")
async def bot_restart(req):
    """Re-run auto-start logic without restarting the addon process."""
    if OPT.get("broker_api_key"):
        STATE["botActive"] = True
        save_state()
        log.info("Bot restarted via /api/bot/restart")
        return web.json_response({"ok": True, "botActive": True})
    return web.json_response({"ok": False, "error": "No broker key configured"})

@routes.get("/api/bot/status")
async def bot_status(req):
    pos=STATE.get("positions",[])
    return web.json_response({"active":STATE.get("botActive",False),"mode":OPT["trading_mode"],
        "portfolio":STATE.get("portfolio",0),"open_positions":len(pos),"positions":pos,
        "trades_today":STATE.get("tradesToday",0),"today_pnl":STATE.get("todayPnl",0),
        "win_count":STATE.get("winCount",0),"loss_count":STATE.get("lossCount",0),
        "last_scan":ANALYSES[0].get("ts") if ANALYSES else None,
        "market":market_status(),"broker_connected":STATE.get("connected",False),
        "has_broker_key":bool(OPT["broker_api_key"])})

@routes.get("/api/market/status")
async def market_status_route(req):
    symbol=req.rel_url.query.get("symbol","").upper()
    status=market_status(symbol); et=_et_now()
    return web.json_response({**status,"et_time":et.strftime("%H:%M:%S"),
        "et_date":et.strftime("%Y-%m-%d"),"et_weekday":et.strftime("%A"),
        "utc_time":datetime.now(timezone.utc).strftime("%H:%M:%S UTC")})

@routes.options("/{path:.*}")
async def options_handler(req):
    return web.Response(status=200,headers={"Access-Control-Allow-Origin":"*",
        "Access-Control-Allow-Methods":"GET,POST,PUT,DELETE,OPTIONS",
        "Access-Control-Allow-Headers":"Content-Type,Authorization"})

@routes.get("/{path:.*}")
async def static_fallback(req):
    path=req.match_info.get("path") or "index.html"
    if path.startswith("api/"): return web.json_response({"ok":False,"error":f"Unknown endpoint: /{path}"},status=404)
    fp=STATIC_DIR/path
    if fp.exists() and fp.is_file(): return web.FileResponse(fp)
    idx=STATIC_DIR/"index.html"
    if idx.exists():
        return web.Response(text=idx.read_text(),content_type="text/html",
            headers={"Cache-Control":"no-store, no-cache, must-revalidate","Pragma":"no-cache","Expires":"0"})
    return web.Response(status=404,text="Not found")

# ── Bot trading loop ────────────────────────────────────────────────────────
def _apply_bot_risk_params(text: str) -> None:
    """
    Parse RISK_PARAMS block from Claude's bot response and update STATE.riskCfg.
    Only applies changes within safe bounds. User can override via UI.
    """
    import re
    try:
        sl_m  = _re.search(r'STOP_LOSS:\s*([\d.]+)%',   text, _re.IGNORECASE)
        tp_m  = _re.search(r'TAKE_PROFIT:\s*([\d.]+)%', text, _re.IGNORECASE)
        mp_m  = _re.search(r'MAX_POSITION:\s*([\d.]+)%',text, _re.IGNORECASE)

        changed = []
        rc = STATE.get("riskCfg", {})

        if sl_m:
            val = float(sl_m.group(1))
            if 0.5 <= val <= 10.0:  # sanity bounds: 0.5% – 10%
                rc["stopL"] = f"{val}%"
                changed.append(f"stop={val}%")

        if tp_m:
            val = float(tp_m.group(1))
            if 1.0 <= val <= 30.0:  # sanity bounds: 1% – 30%
                rc["takeP"] = f"{val}%"
                changed.append(f"target={val}%")

        if mp_m:
            val = float(mp_m.group(1))
            if 1.0 <= val <= 25.0:  # sanity bounds: 1% – 25%
                rc["maxPos"] = f"{val}%"
                changed.append(f"max_pos={val}%")

        if changed:
            STATE["riskCfg"] = rc
            save_state()
            log.info("Bot auto-adjusted risk params: %s", ", ".join(changed))
            OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "li",
                           "msg": f"⚠️ Risk params updated: {', '.join(changed)}"})
            save_optlog()
    except Exception as e:
        log.debug("_apply_bot_risk_params: %s", e)

def _apply_bot_strategy_weights(text: str) -> None:
    """
    Parse STRATEGY_WEIGHTS block from Claude's response and update STATE.stratWeights.
    Normalizes to sum=1.0. User can override via UI strategy toggles.
    """
    import re
    STRAT_KEYS = ["momentum", "meanRev", "breakout", "sentiment", "mlPat", "macro", "volatility"]
    try:
        # Find the STRATEGY_WEIGHTS section
        section_m = _re.search(r'STRATEGY_WEIGHTS:(.*?)(?:REASONING:|$)', text,
                               _re.IGNORECASE | _re.DOTALL)
        if not section_m:
            return

        section = section_m.group(1)
        new_weights = {}
        for key in STRAT_KEYS:
            m = _re.search(rf'{key}:\s*([\d.]+)%', section, _re.IGNORECASE)
            if m:
                val = float(m.group(1)) / 100.0
                if 0.0 < val < 1.0:
                    new_weights[key] = val

        if len(new_weights) < 3:  # need at least 3 strategies specified
            return

        # Normalize to sum = 1.0, filling missing strategies with equal share
        total = sum(new_weights.values())
        if total <= 0:
            return
        for key in STRAT_KEYS:
            if key not in new_weights:
                new_weights[key] = (1.0 - total) / (len(STRAT_KEYS) - len(new_weights))
        total2 = sum(new_weights.values())
        normalized = {k: round(v/total2, 4) for k,v in new_weights.items()}

        STATE["stratWeights"] = normalized
        save_state()
        log.info("Bot auto-adjusted strategy weights: %s",
                 " | ".join(f"{k}={v*100:.0f}%" for k,v in normalized.items()))
    except Exception as e:
        log.debug("_apply_bot_strategy_weights: %s", e)

def _parse_signals(text):
    signals=[]
    # (?:^|\n)[ \t]* — matches at line start even if Claude indents signal lines
    pattern=_re.compile(
        r'(?:^|\n)[ \t]*([A-Z\/\-]{2,12})[ \t]*\|[ \t]*(BUY|SELL|HOLD|SHORT|CLOSE)[ \t]*\|[ \t]*(\d+)%?[ \t]*'
        r'\|[ \t]*Entry[ \t]*\$?([\d,.]+)[ \t]*\|[ \t]*Target[ \t]*\$?([\d,.]+)[ \t]*\|[ \t]*Stop[ \t]*\$?([\d,.]+)',
        _re.IGNORECASE | _re.MULTILINE)
    seen = set()
    for m in pattern.finditer(text):
        try:
            sym = m.group(1).upper().strip()
            act = m.group(2).upper().strip()
            if (sym, act) in seen: continue
            seen.add((sym, act))
            signals.append({"symbol":sym,"action":act,"confidence":int(m.group(3)),
                             "entry":float(m.group(4).replace(",","")),"target":float(m.group(5).replace(",","")),
                             "stop":float(m.group(6).replace(",",""))})
        except: pass
    return signals


def _entry_context(symbol: str, entry_price: float, fill_price: float) -> dict:
    """
    Build learning metadata captured at trade entry time.
    L5: session_entry  — market session when trade was placed
    L6: regime context — VIX, Fear/Greed, Breadth at entry
    L8: slippage_pct   — fill vs intended price (highlights extended-hours cost)
    """
    mkt  = market_status(symbol)
    sent = STATE.get("sentData", {})
    slippage = round((fill_price - entry_price) / entry_price * 100, 4) if entry_price > 0 else 0.0
    return {
        "session_entry":  mkt.get("session", "regular"),
        "vix_entry":      float(sent.get("VIX", 0)),
        "fg_entry":       float(sent.get("Fear/Greed", 50)),
        "breadth_entry":  float(sent.get("Breadth", 50)),
        "slippage_pct":   slippage,
    }

async def _execute_bot_trade(symbol,side,confidence,entry,target,stop):
    log.info("_execute_bot_trade: %s %s conf=%d%% mode=%s botActive=%s",
             side.upper(), symbol, confidence, OPT.get("trading_mode","?"), STATE.get("botActive",False))
    # ── Close orders (cover/sell) bypass all new-entry guards ────────────
    # They close existing positions — confidence, limits, and cooldowns don't apply
    _is_close_order = side in ("cover", "sell")
    if not _is_close_order:
        if not STATE.get("botActive", False): return {"ok":False,"skipped":True,"reason":"bot not active"}
        if confidence<60: return {"ok":False,"skipped":True,"reason":f"confidence {confidence}%<60%"}
        if STATE.get("todayPnl",0)<-abs(OPT.get("daily_loss_limit",500)): return {"ok":False,"skipped":True,"reason":"daily loss limit"}
        # Dynamic max positions based on available capital
        _avail   = STATE.get("buyingPower",0) if OPT.get("allow_margin") else STATE.get("cash",0)
        if _avail <= 0: _avail = STATE.get("portfolio", 10000)
        _min_pos = max(500, _avail * float(str(STATE.get("riskCfg",{}).get("maxPos","5%")).replace("%","")) / 100)
        _max_pos = max(3, min(20, int(_avail / _min_pos))) if _min_pos > 0 else 5
        _conf_bonus = max(0, (confidence - 75) // 5) if confidence >= 75 else 0
        _effective_max = min(20, _max_pos + _conf_bonus)
    if not _is_close_order:
        if len(STATE.get("positions",[]))>=_effective_max and side in ("buy","short"):
            return {"ok":False,"skipped":True,"reason":f"max {_effective_max} positions (capital-scaled)"}
        already_long  = any(p.get("sym")==symbol and p.get("dir")=="LONG"  for p in STATE.get("positions",[]))
        already_short = any(p.get("sym")==symbol and p.get("dir","LONG").upper()=="SHORT" for p in STATE.get("positions",[]))
        if side=="buy"   and already_long:  return {"ok":False,"skipped":True,"reason":"already long"}
        if side=="short" and already_short: return {"ok":False,"skipped":True,"reason":"already short"}
        # Stop-loss cooldown: don't re-enter a symbol that hit stop-loss today
        _today_et = _et_now().strftime("%Y-%m-%d")
        _sl_cooldown = STATE.get("stopLossCooldown", {})
        if _sl_cooldown.get(symbol) == _today_et:
            return {"ok":False,"skipped":True,"reason":f"{symbol} on stop-loss cooldown until tomorrow"}
        # Pre-flight short check using cache (avoids broker rejection round-trip)
        if side == "short" and symbol in _SHORTABLE_CACHE and not _SHORTABLE_CACHE[symbol]:
            return {"ok": False, "skipped": True, "reason": f"{symbol} not shortable (cached)"}
        # ── Broker rules pre-flight (new entries only) ──────────────────────
        _sz_preview = calc_position(symbol, STATE["prices"].get(symbol, entry or 1), side)
        _notional_preview = _sz_preview.get("notional", 0) if _sz_preview.get("ok") else 0
        _qty_preview      = _sz_preview.get("qty_frac", 0)  if _sz_preview.get("ok") else 0
        _pf_ok, _pf_reason = await broker_preflight(
            symbol, side, _notional_preview, _qty_preview, entry or 0)
        if not _pf_ok:
            log.info("Broker preflight blocked %s %s: %s", side.upper(), symbol, _pf_reason)
            return {"ok": False, "skipped": True, "reason": _pf_reason}
    price=STATE["prices"].get(symbol) or entry
    if not price: return {"ok":False,"skipped":True,"reason":f"no price for {symbol}"}
    mkt=market_status(symbol); is_crypto="/" in symbol or symbol in CRYPTO_SYMBOLS
    # For live equity orders outside regular hours: warn but allow through.
    # Alpaca supports pre/after-market. Broker will accept or queue the order.
    if OPT["trading_mode"]=="live" and not mkt["open"] and not is_crypto:
        session_now = mkt.get("session", "unknown")
        # Alpaca supports 24/5 trading: pre_market, after_hours, overnight_extended all valid
        # Only block NEW entries (buy/short) during weekend/holiday market closure
        TRADEABLE_EXTENDED = ("pre_market", "after_hours", "overnight_extended")
        if side in ("buy", "short") and session_now not in TRADEABLE_EXTENDED:
            log.debug("Bot: queuing %s %s for next open (%s)", side.upper(), symbol, session_now)
            return {"ok": False, "skipped": True, "reason": f"market closed ({session_now}) — weekend/holiday"}
        if side in ("buy", "short"):
            log.info("Bot: %s extended-hours order — %s session", side.upper(), session_now)
    sizing=calc_position(symbol,price,side)
    if not sizing["ok"]: return {"ok":False,"skipped":True,"reason":sizing["error"]}
    _et = _et_now(); ts=_et.strftime("%H:%M:%S"); dt=_et.strftime("%Y-%m-%d")
    if OPT["trading_mode"]=="paper":
        # Qty selection: shorts=whole shares always; buy=frac if allowed, whole if not
        # calc_position already sets qty_frac=float(qty_whole) when allow_fractional=False
        qty = sizing["qty_whole"] if side in ("sell","short") else sizing["qty_frac"]
        log.debug("Bot sizing: %s %s qty=%.6f notional=$%.2f capital_src=%s frac=%s",
                  side.upper(), symbol, qty, sizing["notional"],
                  sizing.get("capital_source","?"), OPT.get("allow_fractional",True))
        if qty <= 0: return {"ok":False,"skipped":True,"reason":f"zero qty for {side} {symbol}"}
        if side=="buy":
            STATE["positions"].append({"sym":symbol,"dir":"LONG","entry":price,"size":qty,"stop":sizing["stop"],"target":sizing["target"],"open":int(time.time()*1000),"confidence":confidence,"bot":True,**_entry_context(symbol,price,price)})
        elif side=="sell":
            closed=[]; remaining=[]
            for p in STATE.get("positions",[]):
                if p.get("sym")==symbol and p.get("dir")=="LONG":
                    pnl=(price-p["entry"])*p["size"]
                    STATE["todayPnl"]=STATE.get("todayPnl",0)+pnl; STATE["portfolio"]=STATE.get("portfolio",0)+pnl
                    STATE["winCount" if pnl>=0 else "lossCount"]=STATE.get("winCount" if pnl>=0 else "lossCount",0)+1
                    _close_ctx = {"exit_reason": "stop_hit" if price<=p.get("stop",0) else "target_hit" if price>=p.get("target",999999) else "manual_close"}
                    TRADES.insert(0,{"sym":symbol,"dir":"LONG","side":"sell","entry":p["entry"],"exit":price,"qty":p["size"],"notional":price*p["size"],"pnl":round(pnl,2),"heldDays":round((int(time.time()*1000)-p.get("open",int(time.time()*1000)))/86400000,4),"type":"PAPER","order_type":"bot_close","time":ts,"date":dt,"strat":"bot","timestamp":_et_now().isoformat(),"open":p.get("open"),"close":int(time.time()*1000),"session_entry":p.get("session_entry","regular"),"vix_entry":p.get("vix_entry",0),"fg_entry":p.get("fg_entry",50),"breadth_entry":p.get("breadth_entry",50),"slippage_pct":p.get("slippage_pct",0),**_close_ctx})
                    _alpaca_increment_day_trade(symbol)  # PDT tracking
                    closed.append(p)
                else: remaining.append(p)
            STATE["positions"]=remaining; save_trades()
            if not closed: return {"ok":False,"skipped":True,"reason":"no position to close"}
        STATE["tradesToday"]=STATE.get("tradesToday",0)+1; save_state()
        log.info("Bot PAPER %s %s %.6f @ $%.4f conf=%d%%",side.upper(),symbol,qty,price,confidence)
        # Push notification for UI to pick up
        STATE["botLastTrade"] = {
            "sym": symbol, "side": side, "dir": "LONG" if side=="buy" else "SHORT",
            "price": price, "qty": qty, "confidence": confidence,
            "mode": "paper", "ts": datetime.now().strftime("%H:%M:%S"),
        }
        return {"ok":True,"mode":"paper","symbol":symbol,"side":side,"qty":qty,"price":price,"confidence":confidence}
    # Live
    api_key=OPT["broker_api_key"]; api_secret=OPT["broker_api_secret"]; url_base=OPT["broker_api_url"]
    if not api_key: return {"ok":False,"error":"No broker key for live trading."}
    if OPT["broker_platform"]=="alpaca":
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            base=_alpaca_base(url_base)
            h={"APCA-API-KEY-ID":api_key,"APCA-API-SECRET-KEY":api_secret,"Content-Type":"application/json"}
            if side in ("buy","short"):
                result=await _place_alpaca_order(session,base,h,symbol,"buy" if side=="buy" else "sell",sizing["notional"],sizing["qty_whole"],price)
                if not result["ok"]: return result
                # ── Submit broker-side GTC stop + take-profit (live mode only) ──────────────
                _stop_oid = None
                _tp_oid   = None
                if OPT["trading_mode"] == "live":
                    _is_ext_entry = not market_status(symbol)["open"] and not is_crypto
                    _broker_stops = await _submit_broker_stop(
                        session, base, h, symbol, "buy" if side=="buy" else "sell",
                        result["qty"], sizing["stop"], sizing["target"], _is_ext_entry)
                    _stop_oid = _broker_stops.get("stop_order_id")
                    _tp_oid   = _broker_stops.get("tp_order_id")
                _ectx_live = _entry_context(symbol, entry, result["price"])  # L5+L6+L8
                STATE["positions"].append({"sym":symbol,"dir":"LONG" if side=="buy" else "SHORT","entry":result["price"],"size":result["qty"],"stop":sizing["stop"],"target":sizing["target"],"open":int(time.time()*1000),"confidence":confidence,"bot":True,"order_id":result["order_id"],"broker_stop_id":_stop_oid,"broker_tp_id":_tp_oid,**_ectx_live})
                TRADES.insert(0,{"sym":symbol,"side":side,"dir":"LONG" if side=="buy" else "SHORT","qty":result["qty"],"notional":result["notional"],"entry":result["price"],"stop":sizing["stop"],"target":sizing["target"],"pnl":0.0,"heldDays":0,"type":"LIVE","order_id":result["order_id"],"order_type":result["order_type"],"status":result["status"],"time":ts,"date":dt,"strat":"bot","timestamp":datetime.now().isoformat(),"broker_stop_id":_stop_oid,"broker_tp_id":_tp_oid,**_ectx_live})
            elif side=="sell":
                existing=[p for p in STATE.get("positions",[]) if p.get("sym")==symbol and p.get("dir")=="LONG"]
                if not existing: return {"ok":False,"error":f"No long position in {symbol}"}
                total_qty=sum(p["size"] for p in existing)
                # Use correct TIF for extended hours
                _smkt = market_status(symbol)
                _stif = "day"
                _sext = False
                if not _smkt["open"] and not is_crypto:
                    if _smkt["session"] in ("pre_market","after_hours","overnight_extended"):
                        _stif, _sext = "day", True   # Alpaca 24/5: limit sell in extended sessions
                    else:
                        _stif = "gtc"
                result = {"ok":False, "order_id":"", "qty":total_qty, "notional":total_qty*price}
                # ── Cancel any broker-side GTC stop/TP orders for this position ──────────────
                _sell_orders_to_cancel = []
                for _lp in existing:
                    if _lp.get("broker_stop_id"): _sell_orders_to_cancel.append(_lp["broker_stop_id"])
                    if _lp.get("broker_tp_id"):   _sell_orders_to_cancel.append(_lp["broker_tp_id"])
                if _sell_orders_to_cancel:
                    await _cancel_broker_orders(session, base, h, _sell_orders_to_cancel)
                # Check for already-pending sell order first
                _pending_sell_id = None
                try:
                    async with session.get(f"{base}/orders?status=open&limit=50", headers=h,
                                          timeout=ClientTimeout(total=8)) as _ro:
                        if _ro.status == 200:
                            for _o in json.loads(await _ro.text()):
                                if (_o.get("symbol") == symbol and _o.get("side") == "sell" and
                                        _o.get("status") in ("new","accepted","pending_new","held")):
                                    _pending_sell_id = _o.get("id", "pending")
                                    log.info("Close %s: sell order already pending (%s) — clearing STATE", symbol, _pending_sell_id)
                                    break
                except Exception as _e:
                    log.debug("Close %s: could not check pending orders: %s", symbol, _e)
                if _pending_sell_id:
                    result = {"ok":True,"order_id":_pending_sell_id,"qty":total_qty,"notional":total_qty*price}
                else:
                    # DELETE /positions/{symbol} — Alpaca canonical close for LONG position
                    async with session.delete(f"{base}/positions/{symbol}", headers=h,
                                              timeout=ClientTimeout(total=15)) as r:
                        resp_text = await r.text()
                        if r.status in (200, 201):
                            order_d = json.loads(resp_text)
                            result = {"ok":True,"order_id":order_d.get("id",""),"qty":total_qty,"notional":total_qty*price}
                        elif r.status in (404, 422):
                            _err = json.loads(resp_text) if resp_text else {}
                            if "held_for_orders" in resp_text or _err.get("code") == 40310000:
                                log.info("Close %s: already held for orders — clearing STATE", symbol)
                            else:
                                log.info("Close %s: position not found (%d) — clearing STATE", symbol, r.status)
                            result = {"ok":True,"order_id":"closed","qty":total_qty,"notional":total_qty*price}
                        else:
                            FAILED_TRADES.insert(0, {"sym": symbol, "side": "sell", "dir": "LONG",
                                "reason": resp_text[:120], "type": "broker_rejection",
                                "ts": _et_now().strftime("%H:%M:%S"), "date": _et_now().strftime("%Y-%m-%d")})
                            if len(FAILED_TRADES) > 500: FAILED_TRADES.pop()
                            _MEMORY_CACHE["built_at"] = None
                            return {"ok":False,"error":f"Alpaca close failed: {resp_text[:200]}"}
                for p in existing:
                    pnl=(price-p["entry"])*p["size"]; STATE["todayPnl"]=STATE.get("todayPnl",0)+pnl; STATE["portfolio"]=STATE.get("portfolio",0)+pnl
                    STATE["winCount" if pnl>=0 else "lossCount"]=STATE.get("winCount" if pnl>=0 else "lossCount",0)+1
                    TRADES.insert(0,{"sym":symbol,"dir":"LONG","side":"sell","entry":p["entry"],"exit":price,"qty":p["size"],"notional":price*p["size"],"pnl":round(pnl,2),"heldDays":round((int(time.time()*1000)-p.get("open",int(time.time()*1000)))/86400000,4),"type":"LIVE","order_id":result.get("order_id",""),"order_type":"close","status":"submitted","time":ts,"date":dt,"strat":"bot","timestamp":datetime.now().isoformat()})
                STATE["positions"]=[p for p in STATE.get("positions",[]) if not(p.get("sym")==symbol and p.get("dir")=="LONG")]
                save_trades()
            elif side == "cover":  # buy-to-cover — close an existing SHORT position
                existing_short = [p for p in STATE.get("positions",[]) if p.get("sym")==symbol and p.get("dir","LONG").upper()=="SHORT"]
                if not existing_short: return {"ok":False,"skipped":True,"reason":f"No short position in {symbol} to cover"}
                total_qty = sum(p["size"] for p in existing_short)
                _smkt = market_status(symbol)
                _stif = "day"
                _sext = False
                if not _smkt["open"] and not is_crypto:
                    if _smkt["session"] in ("pre_market","after_hours","overnight_extended"):
                        _stif, _sext = "day", True
                    else:
                        _stif = "gtc"
                result = {"ok":False, "order_id":"", "qty":total_qty, "notional":total_qty*price}
                # ── Cancel any broker-side GTC stop/TP orders for this position ──────────────
                _orders_to_cancel = []
                for _ep in existing_short:
                    if _ep.get("broker_stop_id"): _orders_to_cancel.append(_ep["broker_stop_id"])
                    if _ep.get("broker_tp_id"):   _orders_to_cancel.append(_ep["broker_tp_id"])
                if _orders_to_cancel:
                    await _cancel_broker_orders(session, base, h, _orders_to_cancel)
                # ── Step 1: Check for already-pending cover order (prevents duplicate orders) ──
                _pending_order_id = None
                try:
                    async with session.get(f"{base}/orders?status=open&limit=50", headers=h,
                                          timeout=ClientTimeout(total=8)) as _ro:
                        if _ro.status == 200:
                            _open_orders = json.loads(await _ro.text())
                            for _o in _open_orders:
                                if (_o.get("symbol") == symbol and
                                        _o.get("side") == "buy" and
                                        _o.get("status") in ("new","accepted","pending_new","held")):
                                    _pending_order_id = _o.get("id", "pending")
                                    log.info("Cover %s: buy order already pending (%s) — clearing STATE", symbol, _pending_order_id)
                                    break
                except Exception as _e:
                    log.debug("Cover %s: could not check pending orders: %s", symbol, _e)
                if _pending_order_id:
                    result = {"ok":True,"order_id":_pending_order_id,"qty":total_qty,"notional":total_qty*price}
                else:
                    # ── Step 2: DELETE /positions/{symbol} — Alpaca canonical close endpoint ──
                    # Works for both live and paper. No position_intent, no buying_power needed.
                    # Alpaca closes the full position and returns a pending order object.
                    async with session.delete(f"{base}/positions/{symbol}", headers=h,
                                              timeout=ClientTimeout(total=15)) as r:
                        resp_text = await r.text()
                        if r.status in (200, 201):
                            order_d = json.loads(resp_text)
                            result = {"ok":True,"order_id":order_d.get("id",""),"qty":total_qty,"notional":total_qty*price}
                        elif r.status in (404, 422):
                            # 404 = already closed, 422 = order pending or other constraint
                            _err = json.loads(resp_text) if resp_text else {}
                            if "held_for_orders" in resp_text or _err.get("code") == 40310000:
                                log.info("Cover %s: already held for orders — clearing STATE", symbol)
                            else:
                                log.info("Cover %s: position not found on broker (%d) — clearing STATE", symbol, r.status)
                            result = {"ok":True,"order_id":"closed","qty":total_qty,"notional":total_qty*price}
                        else:
                            FAILED_TRADES.insert(0, {"sym": symbol, "side": "cover", "dir": "SHORT",
                                "reason": resp_text[:120], "type": "broker_rejection",
                                "ts": _et_now().strftime("%H:%M:%S"), "date": _et_now().strftime("%Y-%m-%d")})
                            if len(FAILED_TRADES) > 500: FAILED_TRADES.pop()
                            _MEMORY_CACHE["built_at"] = None
                            return {"ok":False,"error":f"Alpaca cover failed: {resp_text[:200]}"}
                for p in existing_short:
                    pnl = (p["entry"] - price) * p["size"]  # SHORT profit = entry - exit
                    STATE["todayPnl"] = STATE.get("todayPnl",0) + pnl
                    STATE["portfolio"] = STATE.get("portfolio",0) + pnl
                    STATE["winCount" if pnl>=0 else "lossCount"] = STATE.get("winCount" if pnl>=0 else "lossCount",0) + 1
                    _cover_exit_reason = "stop_hit" if price>=p.get("stop",0) else "target_hit" if price<=p.get("target",0) else "manual_close"
                    TRADES.insert(0,{"sym":symbol,"dir":"SHORT","side":"cover","entry":p["entry"],"exit":price,"qty":p["size"],"notional":price*p["size"],"pnl":round(pnl,2),"heldDays":round((int(time.time()*1000)-p.get("open",int(time.time()*1000)))/86400000,4),"type":"LIVE","order_id":result.get("order_id",""),"order_type":"cover","status":"submitted","time":ts,"date":dt,"strat":"bot","timestamp":datetime.now().isoformat(),"exit_reason":_cover_exit_reason,"session_entry":p.get("session_entry","regular"),"vix_entry":p.get("vix_entry",0),"fg_entry":p.get("fg_entry",50),"breadth_entry":p.get("breadth_entry",50),"slippage_pct":p.get("slippage_pct",0)})
                STATE["positions"] = [p for p in STATE.get("positions",[]) if not(p.get("sym")==symbol and p.get("dir","LONG").upper()=="SHORT")]
                save_trades()
            # Deduct sell-side commission — result is now always defined for both buy and sell paths
            _comm_notional = result.get("notional", 0) if result.get("ok") else sizing["notional"]
            sell_commission = _calc_commission(_comm_notional or sizing["notional"], symbol)
            if sell_commission > 0:
                STATE["portfolio"] = STATE.get("portfolio",0) - sell_commission
                STATE["todayPnl"]  = STATE.get("todayPnl",0)  - sell_commission
            STATE["tradesToday"]=STATE.get("tradesToday",0)+1; save_state()
            log.info("Bot LIVE %s %s @ $%.4f conf=%d%%",side.upper(),symbol,price,confidence)
            STATE["botLastTrade"] = {
                "sym": symbol, "side": side, "dir": "LONG" if side=="buy" else "SHORT",
                "price": price, "qty": result.get("qty",0) if side!="sell" else 0,
                "confidence": confidence, "mode": "live",
                "order_id": result.get("order_id","") if side!="sell" else "",
                "ts": datetime.now().strftime("%H:%M:%S"),
            }
            return {"ok":True,"mode":"live","symbol":symbol,"side":side,"price":price,"confidence":confidence}
    return {"ok":False,"error":f"Live bot: only Alpaca supported. Platform={OPT['broker_platform']}"}

async def _check_stops_and_targets():
    """Check all open positions against stop loss and take profit. Called frequently."""
    rc = STATE.get("riskCfg", {})
    MAX_SHORT_HOLD_DAYS = 5   # force-close shorts after this many days
    now_ms = int(time.time() * 1000)
    bot_active = STATE.get("botActive", False)
    for pos in list(STATE.get("positions", [])):
        # Skip broker-imported positions when bot is paused —
        # these are Alpaca positions from previous sessions and
        # attempting to cover them while paused creates ghost loops
        if not bot_active and pos.get("source") in ("live_sync", "auto_sync", "broker_sync"):
            continue
        sym   = pos.get("sym","")
        price = STATE["prices"].get(sym, 0)
        if not price: continue

        stop   = float(pos.get("stop", 0))
        target = float(pos.get("target", 0))
        d      = pos.get("dir", "LONG")
        entry  = float(pos.get("entry", price))

        # Recalculate stop/target from riskCfg if position has bad values
        if stop == 0 or target == 0:
            sl_pct = float(str(rc.get("stopL","2%")).replace("%","")) / 100
            tp_pct = float(str(rc.get("takeP","6%")).replace("%","")) / 100
            stop   = round(entry * (1-sl_pct if d=="LONG" else 1+sl_pct), 4)
            target = round(entry * (1+tp_pct if d=="LONG" else 1-tp_pct), 4)
            pos["stop"]   = stop
            pos["target"] = target

        # ── Sanity check: reject prices that deviate >25% from entry ───────
        # Weekend/overnight Yahoo data can return stale or incorrect prices.
        # A real stop/TP should never require a >25% move to trigger legitimately
        # unless it was set that way. This prevents false triggers on bad data.
        if entry > 0:
            deviation = abs(price - entry) / entry
            # Sanity cap: 40% for equities, 60% for crypto (legitimate volatility is higher)
            is_crypto_pos = "/" in sym or sym in CRYPTO_SYMBOLS
            sanity_cap = 0.60 if is_crypto_pos else 0.40
            if deviation > sanity_cap:
                log.warning("Price sanity fail for %s: current=$%.4f entry=$%.4f deviation=%.1f%% (cap=%.0f%%) — skipping stop/target check",
                            sym, price, entry, deviation * 100, sanity_cap * 100)
                continue

        # ── Trailing stop logic ───────────────────────────────────────────
        # Automatically raise stop as position gains to lock in profit:
        #   Gain >= trail_trigger (3%): move stop to breakeven + 0.5%
        #   Gain >= 2× trail_trigger (6%): trail stop to capture 50% of gain
        if entry > 0 and price > 0:
            gain_pct = (price - entry) / entry if d == "LONG" else (entry - price) / entry
            sl_pct   = float(str(rc.get("stopL", "2%")).replace("%", "")) / 100
            trail_trigger = max(sl_pct * 1.5, 0.03)   # trail when gain > 1.5× stop or 3%

            if gain_pct >= trail_trigger * 2:           # large gain — trail to lock 50%
                if d == "LONG":
                    new_stop = round(entry + (price - entry) * 0.5, 4)
                    if new_stop > stop:
                        pos["stop"] = new_stop; stop = new_stop
                        log.debug("Trail stop raised (50%% gain lock): %s → $%.4f", sym, new_stop)
                else:
                    new_stop = round(entry - (entry - price) * 0.5, 4)
                    if new_stop < stop:
                        pos["stop"] = new_stop; stop = new_stop
                        log.debug("Trail stop lowered (50%% gain lock SHORT): %s → $%.4f", sym, new_stop)
            elif gain_pct >= trail_trigger:             # moderate gain — move to breakeven+
                be_buffer = entry * 0.005              # 0.5% above entry for LONG
                if d == "LONG":
                    new_stop = round(entry + be_buffer, 4)
                    if new_stop > stop:
                        pos["stop"] = new_stop; stop = new_stop
                        log.debug("Trail stop at breakeven+: %s → $%.4f", sym, new_stop)
                else:
                    new_stop = round(entry - be_buffer, 4)
                    if new_stop < stop:
                        pos["stop"] = new_stop; stop = new_stop
                        log.debug("Trail stop at breakeven+ (SHORT): %s → $%.4f", sym, new_stop)

        # ── Time-based exit for shorts ────────────────────────────────────
        if d == "SHORT":
            open_ms   = pos.get("open", now_ms)
            held_days = (now_ms - open_ms) / 86400000
            if held_days >= MAX_SHORT_HOLD_DAYS:
                log.info("Short time-stop: %s held %.1fd >= %dd limit — force closing",
                         sym, held_days, MAX_SHORT_HOLD_DAYS)
                OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "lw",
                    "msg": f"⏱ SHORT TIME-STOP: {sym} held {held_days:.1f}d — forced close"})
                save_optlog()
                _ts_close_side = "cover"  # time-stop closes a SHORT with buy-to-cover
                r_ts = await _execute_bot_trade(sym, _ts_close_side, 99, price, price, price)
                if r_ts.get("ok") or r_ts.get("skipped"):
                    STATE["positions"] = [
                        p for p in STATE.get("positions", [])
                        if not (p.get("sym") == sym and p.get("dir","LONG").upper() == "SHORT")
                    ]
                    save_state()
                continue

        hit_stop   = (d=="LONG" and price<=stop)   or (d=="SHORT" and price>=stop)
        hit_target = (d=="LONG" and price>=target)  or (d=="SHORT" and price<=target)

        if hit_stop or hit_target:
            reason = "STOP LOSS" if hit_stop else "TAKE PROFIT"
            pnl    = (price - entry) * pos.get("size", 1) * (1 if d=="LONG" else -1)
            log.info("🔔 %s HIT: %s %s @ $%.4f | entry=$%.4f | P&L=$%.2f",
                     reason, d, sym, price, entry, pnl)
            # SHORT positions close with "buy"; LONG positions close with "sell"
            close_side = "cover" if d == "SHORT" else "sell"
            r = await _execute_bot_trade(sym, close_side, 99, price, price, price)
            if r.get("ok") or r.get("skipped"):
                # ── Tag exit_reason on the most recent matching TRADE record ──
                _exit_tag = "stop_hit" if hit_stop else "target_hit"
                for _t in TRADES[:5]:  # only check recent trades
                    if _t.get("sym") == sym and _t.get("dir","LONG").upper() == d and not _t.get("exit_reason"):
                        _t["exit_reason"] = _exit_tag
                        break
                # ── Remove position from STATE immediately so the loop
                #    does not re-fire on the same position every 30s ──
                STATE["positions"] = [
                    p for p in STATE.get("positions", [])
                    if not (p.get("sym") == sym and p.get("dir","LONG").upper() == d)
                ]
                save_state()
                log.info("Position removed from STATE: %s %s (reason=%s ok=%s skipped=%s side=%s)",
                         d, sym, reason, r.get("ok"), r.get("skipped"), close_side)
                STATE.pop(f"_close_fails_{sym}_{d}", None)  # clear failure counter on success
                if hit_stop:  # only cooldown on stop-loss, not take-profit
                    _et_date = _et_now().strftime("%Y-%m-%d")
                    STATE.setdefault("stopLossCooldown", {})[sym] = _et_date
                    log.info("Stop-loss cooldown set for %s until %s ET", sym, _et_date)
                OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "lw",
                    "msg": f"🔔 {reason}: closed {sym} @ ${price:.2f} | P&L=${pnl:+.2f}"})
                save_optlog()
            else:
                err_msg = r.get("error", "")
                # If Alpaca says position doesn't exist, it was already closed — clear STATE
                already_closed = ("no position" in err_msg.lower() or
                                  "not found" in err_msg.lower() or
                                  "no long position" in err_msg.lower() or
                                  "no short position" in err_msg.lower())
                if already_closed:
                    STATE["positions"] = [
                        p for p in STATE.get("positions", [])
                        if not (p.get("sym") == sym and p.get("dir","LONG").upper() == d)
                    ]
                    save_state()
                    log.info("Position already closed externally: %s %s — removed from STATE", d, sym)
                else:
                    # Track consecutive close failures per position
                    _fail_key = f"_close_fails_{sym}_{d}"
                    _fails = STATE.get(_fail_key, 0) + 1
                    STATE[_fail_key] = _fails
                    log.warning("Stop/target close FAILED for %s %s: ok=%s err=%s — will retry next tick (attempt %d)",
                                d, sym, r.get("ok"), err_msg, _fails)
                    # After 5 consecutive failures, force-clear from STATE to stop the loop
                    # The position may be unresolvable via API (extended hours, data issue, etc.)
                    if _fails >= 5:
                        STATE["positions"] = [
                            p for p in STATE.get("positions", [])
                            if not (p.get("sym") == sym and p.get("dir","LONG").upper() == d)
                        ]
                        STATE.pop(_fail_key, None)
                        save_state()
                        log.error("FORCE-CLEARED %s %s from STATE after 5 failed close attempts. "
                                  "CHECK ALPACA MANUALLY — position may still be open. err=%s",
                                  d, sym, err_msg)
                        OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "lw",
                            "msg": f"⚠️ MANUAL CHECK NEEDED: {d} {sym} force-cleared after 5 close failures. "
                                   f"Verify position closed on broker. err={err_msg[:80]}"})
                        save_optlog()

async def balance_sync_loop():
    """
    Polls Alpaca every 10 seconds and syncs:
      - Account balance (equity, cash, buying power, today P&L)
      - Open positions (unrealized P&L, current qty)
      - Filled orders (any new fills since last check → added to TRADES)
    Keeps APEX display in sync with broker reality without manual refresh.
    """
    await asyncio.sleep(5)  # let startup complete first
    _last_order_ids: set = set()  # track seen order IDs to detect new fills
    while True:
        try:
            key    = OPT.get("broker_api_key","").strip()
            secret = OPT.get("broker_api_secret","").strip()
            base   = _alpaca_base(OPT.get("broker_api_url","").strip())
            if key and secret and OPT.get("broker_platform","").lower() == "alpaca":
                h = {"APCA-API-KEY-ID":key,"APCA-API-SECRET-KEY":secret,"Content-Type":"application/json"}
                async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:

                    # ── 1. Account balance ────────────────────────────────
                    async with session.get(f"{base}/account", headers=h,
                                           timeout=ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            acct        = json.loads(await r.text())
                            equity      = float(acct.get("equity")       or acct.get("portfolio_value") or 0)
                            cash        = float(acct.get("cash")         or 0)
                            buying_power= float(acct.get("buying_power") or 0)
                            day_pnl     = float(acct.get("unrealized_intraday_pl") or 0)
                            long_mv     = float(acct.get("long_market_value")  or 0)
                            short_mv    = float(acct.get("short_market_value") or 0)
                            init_margin = float(acct.get("initial_margin")     or 0)
                            maint_margin= float(acct.get("maintenance_margin") or 0)
                            regt_bp     = float(acct.get("regt_buying_power")  or 0)
                            daytrade_bp = float(acct.get("daytrading_buying_power") or 0)
                            last_equity = float(acct.get("last_equity")        or equity)
                            day_pnl_pct = float(acct.get("unrealized_intraday_plpc") or 0)
                            accrued_fees= float(acct.get("accrued_fees")       or 0)
                            if equity > 0:
                                prev = STATE.get("portfolio", 0)
                                STATE["portfolio"]      = round(equity, 2)
                                STATE["cash"]           = round(cash, 2)
                                STATE["buyingPower"]    = round(buying_power, 2)
                                STATE["todayPnl"]       = round(day_pnl, 2)
                                STATE["todayPnlPct"]    = round(day_pnl_pct * 100, 4)
                                STATE["longMarketValue"]= round(long_mv, 2)
                                STATE["shortMarketValue"]= round(short_mv, 2)
                                STATE["initMargin"]     = round(init_margin, 2)
                                STATE["maintMargin"]    = round(maint_margin, 2)
                                STATE["regtBP"]         = round(regt_bp, 2)
                                STATE["daytradeBP"]     = round(daytrade_bp, 2)
                                STATE["lastEquity"]     = round(last_equity, 2)
                                STATE["accruedFees"]    = round(accrued_fees, 2)
                                STATE["positionMarketValue"] = round(long_mv + short_mv, 2)
                                STATE["balanceFetched"] = True
                                STATE["connected"]      = True
                                if abs(equity - prev) > 0.01:
                                    log.debug("Balance sync: equity=$%.2f cash=$%.2f long=$%.2f short=$%.2f bp=$%.2f",
                                              equity, cash, long_mv, short_mv, buying_power)

                    # ── 2. Open positions — sync unrealized P&L ───────────
                    async with session.get(f"{base}/positions", headers=h,
                                           timeout=ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            broker_pos = json.loads(await r.text())
                            bp_map     = {p["symbol"]: p for p in broker_pos}
                            local_syms = {p["sym"] for p in STATE.get("positions",[])}

                            # Update P&L on existing positions
                            if "dayOpen" not in STATE: STATE["dayOpen"] = {}
                            for p in STATE.get("positions",[]):
                                bp = bp_map.get(p["sym"])
                                if bp:
                                    p["pnl"] = round(float(bp.get("unrealized_pl") or 0), 2)
                                    cur_price = float(bp.get("current_price") or p.get("entry",0))
                                    p["cur"] = round(cur_price, 4)
                                    # Estimate day open: current_price - (intraday_pl / qty)
                                    qty = float(bp.get("qty") or 1)
                                    intraday_pl = float(bp.get("unrealized_intraday_pl") or 0)
                                    day_open = cur_price - (intraday_pl / qty) if qty else cur_price
                                    if day_open > 0:
                                        STATE["dayOpen"][p["sym"]] = round(day_open, 4)
                                    # Also store lastday_price as fallback
                                    lastday = float(bp.get("lastday_price") or 0)
                                    if lastday > 0 and p["sym"] not in STATE["dayOpen"]:
                                        STATE["dayOpen"][p["sym"]] = round(lastday, 4)

                            # Import positions opened directly on broker —
                            # only when bot is active to avoid pulling in stale
                            # positions from previous sessions on restart/pause
                            added = 0
                            if STATE.get("botActive", False):
                                for sym, bp in bp_map.items():
                                    if sym not in local_syms:
                                        qty   = float(bp.get("qty") or 0)
                                        entry = float(bp.get("avg_entry_price") or 0)
                                        side  = bp.get("side","long")
                                        upnl  = float(bp.get("unrealized_pl") or 0)
                                        if qty > 0 and entry > 0:
                                            rc_i   = STATE.get("riskCfg", {})
                                            sl_pct = float(str(rc_i.get("stopL","2%")).replace("%","")) / 100
                                            tp_pct = float(str(rc_i.get("takeP","6%")).replace("%","")) / 100
                                            is_long = side == "long"
                                            _new_pos = {
                                                "sym":    sym, "dir": "LONG" if is_long else "SHORT",
                                                "entry":  entry, "size": qty,
                                                "stop":   round(entry*(1-sl_pct if is_long else 1+sl_pct), 4),
                                                "target": round(entry*(1+tp_pct if is_long else 1-tp_pct), 4),
                                                "open":   int(time.time()*1000), "pnl": round(upnl,2),
                                                "cur":    float(bp.get("current_price") or entry),
                                                "source": "live_sync",
                                                "broker_stop_id": None, "broker_tp_id": None,
                                            }
                                            # Ensure broker-side GTC stop/TP exists for this position
                                            _new_pos = await _ensure_broker_stops(
                                                session, base, h, _new_pos)
                                            STATE["positions"].append(_new_pos)
                                            added += 1
                            if added:
                                save_state()
                                log.info("Balance sync: imported %d new broker position(s)", added)

                            # Remove live_sync positions that no longer exist on broker
                            # (means they were closed — by broker stop, TP, or manual close)
                            # Cancel any APEX-posted GTC stop/TP orders for those positions
                            _to_remove = [
                                p for p in STATE.get("positions",[])
                                if p.get("source") in ("live_sync","auto_sync","broker_sync")
                                and p["sym"] not in bp_map
                            ]
                            if _to_remove:
                                _orphan_ids = []
                                for _rp in _to_remove:
                                    if _rp.get("broker_stop_id"): _orphan_ids.append(_rp["broker_stop_id"])
                                    if _rp.get("broker_tp_id"):   _orphan_ids.append(_rp["broker_tp_id"])
                                if _orphan_ids:
                                    log.info("Cancelling orphaned broker stop/TP orders: %s", _orphan_ids)
                                    await _cancel_broker_orders(session, base, h, _orphan_ids)
                            # ── Reconcile broker-stopped bot positions ─────────────
                            # If a bot-opened position has a broker_stop_id but no longer
                            # exists on the broker, the GTC stop/TP fired while we were online.
                            # Close it in STATE and record approximate P&L.
                            for _rpos in list(STATE.get("positions",[])):
                                _rsym = _rpos.get("sym","")
                                _rsrc = _rpos.get("source")
                                _has_broker_stop = bool(_rpos.get("broker_stop_id") or _rpos.get("broker_tp_id"))
                                if (_rsrc not in ("live_sync","auto_sync","broker_sync")
                                        and _has_broker_stop
                                        and _rsym not in bp_map
                                        and _rsym in STATE.get("prices",{})):
                                    _exit_px = float(STATE["prices"].get(_rsym, _rpos.get("entry",0)))
                                    _entry   = float(_rpos.get("entry",0))
                                    _qty     = float(_rpos.get("size",0))
                                    _dir     = _rpos.get("dir","LONG")
                                    _pnl     = (_exit_px - _entry) * _qty if _dir=="LONG" else (_entry - _exit_px) * _qty
                                    STATE["todayPnl"]  = STATE.get("todayPnl",0) + _pnl
                                    STATE["portfolio"] = STATE.get("portfolio",0) + _pnl
                                    STATE["winCount" if _pnl>=0 else "lossCount"] = STATE.get(
                                        "winCount" if _pnl>=0 else "lossCount",0)+1
                                    TRADES.insert(0,{"sym":_rsym,"dir":_dir,"side":"cover" if _dir=="SHORT" else "sell",
                                        "entry":_entry,"exit":_exit_px,"qty":_qty,
                                        "notional":round(_qty*_exit_px,2),"pnl":round(_pnl,2),
                                        "heldDays":0,"type":"BROKER_STOP","strat":"broker_stop",
                                        "time":_et_now().strftime("%H:%M:%S"),
                                        "date":_et_now().strftime("%Y-%m-%d"),
                                        "timestamp":_et_now().isoformat()})
                                    STATE["positions"] = [p for p in STATE.get("positions",[]) if p is not _rpos]
                                    log.info("Broker stop/TP fired for %s %s — P&L $%.2f. Reconciled.", _dir, _rsym, _pnl)
                                    OPTLOG.append({"ts":_et_now().strftime("%H:%M:%S"),"type":"li",
                                        "msg":f"🛑 Broker stop/TP fired: {_rsym} {_dir} P&L=${_pnl:+.2f}"})
                                    save_optlog(); save_trades()
                            # ── Remove imported positions that closed on broker side ──
                            before = len(STATE.get("positions",[]))
                            STATE["positions"] = [
                                p for p in STATE.get("positions",[])
                                if p.get("source") not in ("live_sync","auto_sync","broker_sync")
                                or p["sym"] in bp_map
                            ]
                            removed = before - len(STATE["positions"])
                            if removed:
                                save_state()
                                log.info("Balance sync: removed %d closed position(s)", removed)

                    # ── 3. Filled orders — import all new fills ───────────
                    async with session.get(f"{base}/orders?status=filled&limit=100&direction=desc",
                                           headers=h, timeout=ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            filled       = json.loads(await r.text())
                            new_ids      = {o["id"] for o in filled}
                            existing_ids = {t.get("order_id","") for t in TRADES if t.get("order_id")}
                            # Import every filled order not already in TRADES
                            # (covers both startup catch-up and ongoing new fills)
                            to_import    = [o for o in filled if o["id"] not in existing_ids]
                            today_str    = date.today().isoformat()
                            imported     = 0
                            for o in to_import:
                                sym    = o.get("symbol","")
                                side   = o.get("side","buy")
                                fqty   = float(o.get("filled_qty") or 0)
                                fprice = float(o.get("filled_avg_price") or 0)
                                if not sym or fqty == 0 or fprice == 0: continue
                                notional  = fqty * fprice
                                filled_at = o.get("filled_at") or o.get("submitted_at","")
                                try:    trade_dt, trade_ts = filled_at[:10], filled_at[11:19]
                                except: trade_dt, trade_ts = today_str, datetime.now().strftime("%H:%M:%S")
                                TRADES.insert(0, {
                                    "sym":sym,"side":side,"dir":"LONG" if side=="buy" else "SHORT",
                                    "qty":fqty,"notional":round(notional,2),"entry":fprice,
                                    "exit":fprice if side=="sell" else 0,
                                    "gross_pnl":0.0,"commission":round(_calc_commission(notional,sym),4),
                                    "pnl":0.0,"heldDays":0,"type":"LIVE_SYNC",
                                    "order_id":o["id"],"order_type":o.get("order_type","market"),
                                    "status":"filled","strat":"manual",
                                    "time":trade_ts,"date":trade_dt,
                                    "timestamp":filled_at,"source":"live_sync",
                                })
                                imported += 1
                            # Track seen IDs; save if anything was imported
                            _last_order_ids.update(new_ids)
                            if imported:
                                TRADES.sort(key=lambda t: t.get("timestamp",""), reverse=True)
                                save_trades()
                                STATE["tradesToday"] = len([t for t in TRADES if t.get("date")==today_str])
                                save_state()
                                log.info("Live sync: imported %d filled order(s)", imported)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.debug("balance_sync_loop: %s", e)
        await asyncio.sleep(10)


async def stop_loss_loop():
    """
    Dedicated stop-loss enforcement loop — runs every 30 seconds.
    Independent of the bot trading loop so stops fire promptly.
    """
    await asyncio.sleep(60)
    while True:
        try:
            if STATE.get("positions") and STATE.get("prices"):
                await _check_stops_and_targets()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("stop_loss_loop: %s", e)
        await asyncio.sleep(30)  # check every 30 seconds

# ── Pre-market opening scan tracker ──────────────────────────────────────
_premarket_scan_done_date: str = ""   # tracks which date we've done the pre-market scan
_premarket_scan_done_ts: float = 0.0  # monotonic time of last scan (restart-aware)

async def _run_premarket_opening_scan():
    """
    Comprehensive pre-market scan running at 9:09-9:25 ET.
    Analyzes ALL available market data (SIP prices, positions, screener candidates)
    to optimize the portfolio and queue opening trades for maximum profit.
    Executes portfolio rebalancing, closes poor positions, and opens high-confidence setups.
    Runs ONCE per trading day.
    """
    global _premarket_scan_done_date, _premarket_scan_done_ts
    et = _et_now()
    today = et.strftime("%Y-%m-%d")
    # Already ran today AND within last 2 hours → skip (handles restart gracefully)
    import time as _t
    if (_premarket_scan_done_date == today and
            hasattr(_premarket_scan_done_ts, '__sub__') and
            _t.monotonic() - _premarket_scan_done_ts < 7200):
        return
    _premarket_scan_done_date = today
    _premarket_scan_done_ts   = _t.monotonic()
    log.info("Pre-market scan: starting comprehensive opening analysis for %s", today)

    px    = STATE.get("prices", {})
    pos   = STATE.get("positions", [])
    rc    = STATE.get("riskCfg", {})
    sw    = STATE.get("stratWeights", {})
    swr   = STATE.get("stratWR", {})
    port  = STATE.get("portfolio", 0)
    wins  = STATE.get("winCount", 0)
    losses= STATE.get("lossCount", 0)
    tot   = wins + losses
    wr    = round(wins / tot * 100) if tot else 0
    candidates = STATE.get("screenerCandidates", [])

    # Build price snapshot — all symbols with live SIP data, sorted by activity
    live_prices = {s: v for s, v in px.items() if v > 0}
    # Separate into asset classes
    crypto_px = {s: v for s, v in live_prices.items() if "/" in s or "USD" in s}
    equity_px  = {s: v for s, v in live_prices.items() if s not in crypto_px}

    # Top movers from overnight (compare to any prior close if available)
    price_snap = " | ".join(
        f"{s}:${v:,.2f}" for s, v in sorted(equity_px.items(), key=lambda x: x[0])[:60]
    )
    crypto_snap = " | ".join(f"{s}:${v:,.2f}" for s, v in list(crypto_px.items())[:15])

    # Open positions summary
    open_pos_lines = []
    for p in pos:
        sym  = p.get("sym") or p.get("symbol","?")
        entry= p.get("entry", 0)
        cur  = px.get(sym, entry)
        pnl  = p.get("pnl") or ((cur - entry) * abs(p.get("size", p.get("qty", 0))))
        pnl_pct = (cur - entry) / entry * 100 if entry else 0
        open_pos_lines.append(
            f"  {sym} {p.get('dir','?')} entry=${entry:.2f} now=${cur:.2f} "
            f"pnl=${pnl:+.2f} ({pnl_pct:+.1f}%) stop=${p.get('stop',0):.2f} target=${p.get('target',0):.2f}"
        )
    open_pos_txt = "\n".join(open_pos_lines) or "  None"

    # Screener candidates
    screener_txt = ""
    if candidates:
        screener_txt = "OVERNIGHT SCREENER TOP CANDIDATES:\n" + "\n".join(
            f"  {c.get('sym','?')} score={c.get('score',0)}/100 — {c.get('reason','')}"
            for c in candidates[:15]
        ) + "\n"

    # Strategy performance
    strat_txt = " | ".join(
        f"{k}={v*100:.0f}% (WR:{swr.get(k,0):.1f}%)" for k, v in sw.items()
    )

    PREMARKET_PROMPT = f"""You are APEX, an autonomous AI trading bot. Market opens in 15-21 minutes (9:30 ET).
Today: {et.strftime('%A %B %d, %Y')} | Portfolio: ${port:,.2f} | Win Rate: {wr}% ({wins}W/{losses}L)

TASK: Comprehensive pre-market portfolio optimization. Make ALL necessary changes NOW to maximize profit at open.

═══ CURRENT OPEN POSITIONS ═══
{open_pos_txt}

═══ LIVE PRE-MARKET PRICES ({len(equity_px)} equities, {len(crypto_px)} crypto) ═══
EQUITIES: {price_snap}
CRYPTO: {crypto_snap}

═══ {screener_txt}
STRATEGY WEIGHTS & WIN RATES: {strat_txt}
RISK CONFIG: stop={rc.get('stopL','2%')} take_profit={rc.get('takeP','6%')} max_pos={rc.get('maxPos','5%')}

═══ YOUR INSTRUCTIONS ═══
1. PORTFOLIO REVIEW — For each open position:
   - CLOSE if: significant overnight gap against position, stop is too tight, thesis broken
   - HOLD if: still valid, target not reached, risk/reward still favorable
   - ADJUST STOP if: position moved in our favor (trail stop to lock profit)

2. NEW OPENING TRADES — Identify 3-5 highest-conviction setups for 9:30 open:
   - Gap-up/gap-down momentum plays (first 30 min most volatile — use tight stops)
   - Key technical breakouts from overnight price action
   - Screener candidates above with strong fundamentals
   - Only BUY for confirmed uptrends; only SHORT for confirmed downtrends
   - SHORTS: whole shares only, max 3% portfolio each

3. RISK MANAGEMENT:
   - Max positions is capital-scaled (min 3, max 20 — more capital = more slots)
   - Total risk exposure should not exceed 15% of portfolio
   - First 30 min after open: tighter stops (half normal), smaller size (half normal)
   - If win rate < 50%: be conservative, max 3 positions, higher confidence threshold (75%+)
   - If win rate > 65%: normal sizing, up to 5 positions, normal threshold (65%+)

4. OUTPUT FORMAT — Use these exact formats:
   For each action, one line:
   CLOSE: SYMBOL | REASON (one line per close)
   SYMBOL | BUY/SELL/SHORT/HOLD | CONFIDENCE% | Entry $X | Target $X | Stop $X | REASON

5. End with:
   OPENING_STRATEGY: [one sentence on overall market bias and approach for today's open]
   RISK_SUMMARY: [total positions planned, total estimated exposure %]"""

    result = await call_claude(PREMARKET_PROMPT, 3000)
    if not result["ok"]:
        log.warning("Pre-market scan: Claude failed — %s", result.get("text","?")[:100])
        return

    scan_text = result["text"]
    et_ts = _et_now()

    # Store in analyses
    ANALYSES.insert(0, {
        "type":    "preMarket",
        "ts":      et_ts.strftime("%H:%M:%S"),
        "text":    scan_text,
        "savedAt": et_ts.isoformat(),
        "bot":     True,
    })
    save_analyses()

    # ── Execute CLOSE signals ─────────────────────────────────────────────
    close_lines = [l.strip() for l in scan_text.split("\n")
                   if l.strip().upper().startswith("CLOSE:")]
    for cl in close_lines:
        # Format: CLOSE: SYMBOL | REASON
        parts = cl[6:].strip().split("|")
        if parts:
            sym_to_close = parts[0].strip().upper()
            open_sym = next((p for p in pos if (p.get("sym") or p.get("symbol","")).upper() == sym_to_close), None)
            if open_sym and px.get(sym_to_close, 0) > 0:
                close_side = "sell" if open_sym.get("dir","LONG").upper() != "SHORT" else "buy"
                try:
                    r = await _execute_bot_trade(sym_to_close, close_side, 100,
                                                  px[sym_to_close], px[sym_to_close], px[sym_to_close])
                    reason = parts[1].strip() if len(parts) > 1 else "pre-market close"
                    if r.get("ok"):
                        OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "lw",
                            "msg": f"🔔 PRE-MKT CLOSE: {sym_to_close} @ ${px[sym_to_close]:.2f} — {reason}"})
                        log.info("Pre-market: closed %s @ $%.2f (%s)", sym_to_close, px[sym_to_close], reason)
                except Exception as e:
                    log.error("Pre-market close %s: %s", sym_to_close, e)

    # ── Execute BUY/SHORT signals ─────────────────────────────────────────
    signals = _parse_signals(scan_text)
    conf_threshold = 75 if wr < 50 else 65
    actionable = [s for s in signals if s["action"] not in ("HOLD", "CLOSE") and s["confidence"] >= conf_threshold]
    log.info("Pre-market: %d signals parsed, %d actionable (conf>=%d%%)",
             len(signals), len(actionable), conf_threshold)

    # Cap opening trades: base 5, scales with win rate
    _pm_max = 5 if wr < 50 else 8 if wr < 65 else 12
    for sig in actionable[:_pm_max]:  # dynamic cap
        side = "buy" if sig["action"] == "BUY" else "sell" if sig["action"] == "CLOSE" else "short"
        try:
            r = await _execute_bot_trade(sig["symbol"], side, sig["confidence"],
                                          sig.get("entry", px.get(sig["symbol"], 0)),
                                          sig.get("target", 0), sig.get("stop", 0))
            if r.get("ok"):
                OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "ls",
                    "msg": f"🔔 PRE-MKT {sig['action']}: {sig['symbol']} @ ${r.get('price', sig.get('entry',0)):.2f} conf={sig['confidence']}%"})
                log.info("Pre-market: opened %s %s @ $%.2f conf=%d%%",
                         sig["action"], sig["symbol"], r.get("price",0), sig["confidence"])
        except Exception as e:
            log.error("Pre-market execute %s: %s", sig["symbol"], e)

    save_optlog()
    log.info("Pre-market scan complete — %d closes, %d new positions queued",
             len(close_lines), len(actionable))


def _scan_interval(session: str) -> int:
    """
    Return how many seconds to wait between Claude API calls based on market session.
    Balances API cost vs. signal freshness.

    Session           Tradeable                        Interval
    ────────────────────────────────────────────────────────────
    regular           All equities + crypto (24/5)     3 min (fast response)
    pre_market        All equities + crypto (Alpaca)   30 min
    after_hours       All equities + crypto (Alpaca)   30 min
    overnight_extended All equities (Alpaca 24/5)      30 min
    overnight         legacy fallback                  60 min
    weekend           All equities queue for open      2 hours
    holiday           All equities queue for open      2 hours
    crypto            24/7                             3 min
    """
    base = max(60, int(OPT.get("auto_analysis_interval", 600)))
    # During regular market hours: cap at 3 minutes for faster signal response
    # (screener runs every 2 min; Claude should act on fresh candidates quickly)
    regular_interval = min(base, 180)   # 3-min cap during open market
    return {
        "regular":            regular_interval,
        "pre_market":         max(base, 1800),   # 30min minimum
        "after_hours":        max(base, 1800),   # 30min minimum
        "overnight_extended": max(base, 1800),   # 30min — Alpaca 24/5 overnight is tradeable
        "overnight":          max(base, 3600),   # 60min — legacy fallback (shouldn't hit)
        "weekend":            max(base, 7200),   # 2hr minimum
        "holiday":            max(base, 7200),   # 2hr minimum
        "crypto":             regular_interval,  # crypto trades 24/7
    }.get(session, base)


# ═══════════════════════════════════════════════════════════════════════════
# SECONDARY AI SCREENER
# Runs continuously every ~2 minutes (configurable), independent of Claude.
# Scores all tracked symbols using fast/cheap inference and populates
# STATE["screenerCandidates"] — a ranked shortlist Claude reads each cycle.
#
# Supported backends:
#   ollama  — local Llama/Mistral/Phi via Ollama REST API (free, private)
#   openai  — GPT-4o-mini (cheap, ~$0.001/scan) via OpenAI API
#   groq    — Llama-3/Mixtral via Groq API (free tier, very fast)
#   disabled — screener off, Claude gets full symbol universe as before
#
# The screener does NOT make trades. It only scores and ranks symbols.
# Claude remains the sole decision maker — screener candidates are passed as
# additional context so Claude can focus its reasoning on pre-qualified names.
# ═══════════════════════════════════════════════════════════════════════════

async def _call_screener_ollama(prompt: str) -> str:
    """Call a local Ollama instance for fast inference."""
    url    = OPT.get("screener_url","http://localhost:11434").rstrip("/")
    model  = OPT.get("screener_model","llama3.2")
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            async with session.post(
                f"{url}/api/generate",
                json={"model":model,"prompt":prompt,"stream":False,
                      "options":{"temperature":0.1,"num_predict":600}},
                timeout=ClientTimeout(total=45)) as r:
                if r.status == 200:
                    d = json.loads(await r.text())
                    return d.get("response","")
                log.warning("Screener Ollama HTTP %d", r.status)
                return ""
    except Exception as e:
        log.debug("Screener Ollama error: %s", e)
        return ""


async def _call_screener_openai(prompt: str) -> str:
    """Call OpenAI or Groq API (OpenAI-compatible) for screening."""
    provider = OPT.get("screener_provider","").lower()
    key      = OPT.get("screener_api_key","").strip()
    model    = OPT.get("screener_model","gpt-4o-mini")
    if provider == "groq":
        base_url = "https://api.groq.com/openai/v1/chat/completions"
        # Auto-correct Ollama model names to Groq equivalents
        # Use 8b-instant by default — much smaller token usage, avoids daily TPD limits
        # 70b is more capable but hits Groq's 100k TPD limit quickly at 2-min intervals
        if not model or model in ("llama3.2","llama3","llama2","gpt-4o-mini","mistral","llama-3.3-70b-versatile"):
            model = "llama-3.1-8b-instant"  # fast, efficient, stays under TPD limits
    else:
        base_url = "https://api.openai.com/v1/chat/completions"
    if not key: return ""
    try:
        async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
            async with session.post(
                base_url,
                headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},
                json={"model":model,"max_tokens":600,"temperature":0.1,
                      "messages":[{"role":"user","content":prompt}]},
                timeout=ClientTimeout(total=30)) as r:
                if r.status == 200:
                    d = json.loads(await r.text())
                    return d.get("choices",[{}])[0].get("message",{}).get("content","")
                elif r.status == 429:
                    # Rate limited — log clearly and return empty so backoff triggers
                    body = (await r.text())[:200]
                    log.warning("Screener %s rate limit (429): %s", provider, body)
                    # Auto-downgrade Groq to faster/smaller model on rate limit
                    if provider == "groq" and OPT.get("screener_model","") == "llama-3.3-70b-versatile":
                        OPT["screener_model"] = "llama-3.1-8b-instant"
                        log.info("Screener: auto-downgraded to llama-3.1-8b-instant to avoid rate limits")
                    return ""
                log.warning("Screener %s HTTP %d: %s", provider, r.status, (await r.text())[:100])
                return ""
    except Exception as e:
        log.debug("Screener %s error: %s", provider, e)
        return ""


def _parse_screener_scores(text: str, symbols: list) -> list:
    """
    Parse screener discovery output and enrich with bot-computed scores.

    Handles two formats from Groq:
      AAPL | BUY              (new lean format — discovery only)
      AAPL | 82 | BUY | ...  (legacy scored format — still supported)

    For lean format, the rules engine computes scores from:
      - Price momentum proxy (distance from 52-period mean using current price)
      - Volatility bucket (higher vol = higher potential score)
      - Sentiment overlay (Fear/Greed, VIX, Breadth from STATE)
      - Position in screener list (Groq orders by interest level)
    """
    # Pull live data for scoring
    prices   = STATE.get("prices", {})
    sent     = STATE.get("sentData", {})
    fg       = float(sent.get("Fear/Greed", 50))
    vix      = float(sent.get("VIX", 20))
    breadth  = float(sent.get("Breadth", 65))

    # Sentiment base modifier: 0.7 (bad) to 1.15 (good)
    sent_mod = max(0.70, min(1.15,
        0.85 + (fg - 50) / 200 +          # Fear/Greed contribution
        max(0, (30 - vix)) / 100 +         # VIX contribution (calm = bonus)
        (breadth - 50) / 300               # Breadth contribution
    ))

    results  = []
    position = 0  # Groq lists most interesting first — use order as signal

    for line in text.splitlines():
        line = line.strip()
        if not line or "|" not in line: continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 2: continue
        sym = parts[0].upper().replace("$","").strip()
        if not sym or not any(c.isalpha() for c in sym): continue
        if len(sym) > 12: continue

        direction = "BUY"
        reason    = "bot-scored"
        score     = None  # will compute if not provided

        # ── Detect format ────────────────────────────────────────────────
        if len(parts) >= 3:
            # Could be legacy: SYMBOL | SCORE | BUY | REASON
            try:
                maybe_score = int("".join(c for c in parts[1] if c.isdigit()))
                if 0 <= maybe_score <= 100:
                    # Legacy scored format — use the score directly
                    score = maybe_score
                    dir_str = parts[2].upper().strip() if len(parts) >= 3 else "BUY"
                    direction = "SHORT" if "SHORT" in dir_str else "BUY"
                    reason = parts[3].strip() if len(parts) >= 4 else "screener"
                else:
                    raise ValueError
            except (ValueError, IndexError):
                # Not a score — check col 2 for direction
                col2 = parts[1].upper().strip()
                direction = "SHORT" if "SHORT" in col2 else "BUY"
        elif len(parts) == 2:
            # New lean format: SYMBOL | BUY or SHORT
            col2 = parts[1].upper().strip()
            direction = "SHORT" if "SHORT" in col2 else "BUY"

        # ── Bot-computed score (only when Groq didn't provide one) ───────
        if score is None:
            price = float(prices.get(sym, 0) or 0)
            if price <= 0:
                position += 1
                continue

            # 1. Groq ordering bonus: first pick gets 100, last pick gets ~50
            #    Assumes Groq lists ~75 symbols; linear decay
            order_score = max(50, 100 - position * (50 / max(1, 74)))

            # 2. Volatility bucket: low price = higher vol proxy = more movement potential
            #    $1-$10: +8, $10-$50: +4, $50-$200: 0, $200+: -4
            if price < 10:
                vol_bonus = 8
            elif price < 50:
                vol_bonus = 4
            elif price < 200:
                vol_bonus = 0
            else:
                vol_bonus = -4

            # 3. Round number proximity: price near round number = support/resistance
            import math
            magnitude = 10 ** math.floor(math.log10(price)) if price >= 1 else 1
            nearest_round = round(price / magnitude) * magnitude
            dist_pct = abs(price - nearest_round) / nearest_round if nearest_round > 0 else 0.5
            round_bonus = max(0, 6 - dist_pct * 60)  # 0-6 pts; closer = higher

            # 4. Sentiment overlay
            raw_score = order_score + vol_bonus + round_bonus
            score = round(min(99, max(1, raw_score * sent_mod)))
            reason = f"bot-scored:order{position+1}:${price:.2f}"

        position += 1
        results.append({"sym": sym, "score": score, "dir": direction, "reason": reason})

    seen = set()
    ranked = []
    for r in sorted(results, key=lambda x: x["score"], reverse=True):
        if r["sym"] not in seen:
            seen.add(r["sym"])
            ranked.append(r)
    return ranked



async def secondary_screener_loop():
    """
    Continuously screens all tracked symbols using a fast secondary AI.
    Runs independently of the bot — does not sleep when bot is paused.
    Populates STATE["screenerCandidates"] for Claude to read each cycle.
    """
    await asyncio.sleep(15)  # wait for prices to populate first
    _consecutive_errors = 0

    while True:
        provider = OPT.get("screener_provider","disabled").lower()
        interval = int(OPT.get("screener_interval",120))
        # Honour Claude's expansion request (lasts one cycle then resets)
        _expand_req = STATE.pop("screenerExpandRequest", 0)
        top_n    = int(_expand_req or OPT.get("screener_top_n", 75))
        if _expand_req:
            log.info("Screener: honouring Claude expansion request — top_n=%d", top_n)
        min_score= int(OPT.get("screener_min_score",60))

        if provider == "disabled":
            STATE["screenerActive"]   = False
            STATE["screenerProvider"] = "disabled"
            await asyncio.sleep(30)
            continue

        STATE["screenerActive"]   = True
        STATE["screenerProvider"] = provider

        try:
            px       = STATE.get("prices", {})
            pos_syms = [p["sym"] for p in STATE.get("positions", [])]

            # ── Full universe: ALL live symbols, sorted by activity ──────────
            # Equities first (no slash/USD), then crypto
            _sl_cd     = set(STATE.get("stopLossCooldown", {}).keys())
            all_live = [(s, v) for s, v in px.items()
                        if v > 0 and s not in pos_syms
                        and s not in _sl_cd
                        and not s.startswith("__")]
            equities = sorted(
                [(s,v) for s,v in all_live if "/" not in s and "-USD" not in s.upper()],
                key=lambda x: x[0]  # alphabetical — consistent ordering
            )
            crypto = [(s,v) for s,v in all_live if "/" in s or "-USD" in s.upper()]
            # Full universe — no cap. Groq receives all available symbols.
            screener_universe = equities + crypto
            if not screener_universe:
                await asyncio.sleep(30)
                continue

            # ── Batch handling: Groq has ~6k token context limit ─────────────
            # Each symbol entry "AAPL:$185.50 | " ≈ 15 chars / ~4 tokens
            # Safe batch: 500 symbols ≈ 2,000 tokens leaving room for prompt overhead
            # Run multiple batches if universe > 500; merge all candidates at end
            BATCH_SIZE = 500
            all_candidates: list = []
            total_syms   = len(screener_universe)
            num_batches  = max(1, -(-total_syms // BATCH_SIZE))  # ceiling division

            log.info("Screener [%s]: %d symbols across %d batch(es)",
                     provider, total_syms, num_batches)

            from datetime import datetime as _dt
            _now = _dt.now().strftime('%Y-%m-%d %H:%M ET')
            mkt  = market_status()

            for _batch_idx in range(num_batches):
                batch      = screener_universe[_batch_idx * BATCH_SIZE : (_batch_idx + 1) * BATCH_SIZE]
                price_snap = " | ".join(f"{s}:${v:.2f}" for s, v in batch)
                syms       = [s for s, v in batch]

                # ── LEAN DISCOVERY PROMPT (per batch) ─────────────────────────
                batch_top_n = max(10, top_n // max(1, num_batches))
                SCREENER_PROMPT = (
                    f"SCREENER | {_now} | {mkt['session'].upper()} | BATCH {_batch_idx+1}/{num_batches}\n\n"
                    f"PRICES ({len(syms)} symbols):\n{price_snap}\n\n"
                    f"HELD (exclude): {', '.join(pos_syms) or 'none'}\n\n"
                    f"TASK: From the prices above, pick the top {batch_top_n} symbols showing\n"
                    f"the best trading setups right now. Flag SHORT candidates explicitly.\n\n"
                    f"OUTPUT — one line per symbol, nothing else:\n"
                    f"SYMBOL | BUY or SHORT\n\n"
                    f"Return exactly {batch_top_n} lines. No scores, no preamble."
                )

                # Call the appropriate backend
                if provider == "ollama":
                    raw = await _call_screener_ollama(SCREENER_PROMPT)
                elif provider in ("openai", "groq"):
                    raw = await _call_screener_openai(SCREENER_PROMPT)
                else:
                    raw = ""

                if not raw:
                    _consecutive_errors += 1
                    log.warning("Screener batch %d/%d: no response — provider=%s",
                                _batch_idx+1, num_batches, provider)
                    continue  # skip this batch, try next

                _consecutive_errors = 0
                batch_candidates = _parse_screener_scores(raw, syms)
                all_candidates.extend(batch_candidates)
                log.debug("Screener batch %d/%d: %d candidates",
                          _batch_idx+1, num_batches, len(batch_candidates))

                # Rate-limit guard between batches (Groq: 30 req/min free tier)
                if _batch_idx < num_batches - 1:
                    await asyncio.sleep(2.5)

            # ── Merge all batches: de-dup, score-sort, filter, cap ──────────
            if not all_candidates:
                if _consecutive_errors >= 3:
                    log.warning("Screener: %d consecutive failures — provider=%s",
                                _consecutive_errors, provider)
                    backoff = min(interval * 2, 600)
                    await asyncio.sleep(backoff)
                    continue
                # No candidates from any batch — wait and retry
                await asyncio.sleep(min(interval, 60))
                continue

            seen_syms: set = set()
            merged: list = []
            for c in sorted(all_candidates, key=lambda x: x["score"], reverse=True):
                if c["sym"] not in seen_syms:
                    seen_syms.add(c["sym"])
                    merged.append(c)

            candidates = [c for c in merged if c["score"] >= min_score][:top_n]

            STATE["screenerCandidates"] = candidates
            STATE["screenerLastRun"]    = datetime.now().strftime("%H:%M:%S")

            log.info("Screener [%s]: %d candidates → %s",
                     provider, len(candidates),
                     ", ".join(f"{c['sym']}({c['score']})" for c in candidates[:5]))

            if candidates:
                OPTLOG.append({
                    "ts":  datetime.now().strftime("%H:%M:%S"),
                    "type":"li",
                    "msg": f"🔍 Screener [{provider}]: {len(candidates)} candidates — " +
                           " | ".join(f"{c['sym']} {c['score']}" for c in candidates[:5])
                })
                save_optlog()
                # Also store screener run in analyses for visibility in Claude tab
                screener_report = (
                    f"SECONDARY SCREENER [{provider.upper()}] — {datetime.now().strftime('%H:%M:%S')}\n\n" +
                    f"Scanned symbols, {len(candidates)} passed min score ({OPT.get('screener_min_score',60)}):\n\n" +
                    "\n".join(f"{c['sym']} | Score: {c['score']}/100 | {c['reason']}" for c in candidates) +
                    f"\n\nThese candidates will be passed to Claude at next bot scan cycle."
                )
                ANALYSES.insert(0, {"type":"screener","ts":datetime.now().strftime("%H:%M:%S"),
                    "text":screener_report,"savedAt":_et_now().isoformat(),
                    "provider":provider,"candidates":len(candidates)})
                save_analyses()

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("secondary_screener_loop: %s", e)
            _consecutive_errors += 1

        await asyncio.sleep(interval)


# ══════════════════════════════════════════════════════════════════════════════
#  APEX RULES ENGINE — version 2.0
#  Primary decision maker. Claude/AI is an *adviser* called only when the
#  bot is genuinely struggling. 95%+ of trades are pure rules-based.
# ══════════════════════════════════════════════════════════════════════════════

# ── AI Intervention Thresholds ────────────────────────────────────────────────
# Claude is called when ANY of these conditions are met (checked each cycle):
AI_INTERVENTION_THRESHOLDS = {
    # 1. Sustained losing streak
    "consecutive_losses":     4,      # ≥4 losses in a row → AI reviews strategy
    # 2. Win rate collapse (requires enough trades to be meaningful)
    "wr_floor_pct":           35,     # WR drops below 35% (with ≥10 trades) → AI
    "wr_min_trades":          10,     # minimum trades before WR threshold applies
    # 3. Daily loss bleeding
    "daily_loss_pct":         3.0,    # today's loss exceeds 3% of portfolio → AI
    # 4. Drawdown from recent high
    "drawdown_pct":           8.0,    # portfolio down ≥8% from 30-day high → AI
    # 5. Market regime shock
    "vix_spike":              35.0,   # VIX ≥ 35 → AI assesses regime
    # 6. Scheduled deep review (not every cycle — saves ~95% of API calls)
    "deep_review_hours":      24,     # full AI portfolio review every 24 hours
    # 7. New position approval (optional — off by default, can be enabled)
    "ai_approve_new_entries": False,  # True = AI must approve every new entry
}

# ── Technical Signal Engine ────────────────────────────────────────────────────
def _compute_rules_signals(candidates: list, prices: dict, positions: list,
                            sent: dict, strat_weights: dict) -> list:
    """
    Pure rules-based signal generation. No API calls.
    Returns list of signal dicts identical to what Claude would produce.

    Strategies (weighted):
      momentum  — price trending above SMA20; positive momentum score
      meanRev   — RSI oversold/overbought extremes
      breakout  — price near 52-week high with volume confirmation (score proxy)
      sentiment — Fear/Greed + VIX + Breadth composite
      mlPat     — screener score as ML proxy (Groq pre-scores candidates)
    """
    signals = []
    fg    = float(sent.get("Fear/Greed", 50))
    vix   = float(sent.get("VIX", 20))
    pc    = float(sent.get("Put/Call", 0.85))
    brd   = float(sent.get("Breadth", 65))
    open_syms = {p.get("sym","") for p in positions}

    # ── Sentiment regime filter ──────────────────────────────────────────────
    # Extreme fear or high VIX = reduce aggression; high greed = normal sizing
    if vix >= 35:
        regime = "risk_off"
    elif vix >= 25 or fg < 25:
        regime = "cautious"
    elif fg > 80:
        regime = "greed_warning"   # still trade but tighter stops
    else:
        regime = "normal"

    # ── Weights (fall back to equal if not set) ──────────────────────────────
    w_mom  = float(strat_weights.get("momentum",  0.30))
    w_rev  = float(strat_weights.get("meanRev",   0.15))
    w_brk  = float(strat_weights.get("breakout",  0.20))
    w_sent = float(strat_weights.get("sentiment", 0.15))
    w_ml   = float(strat_weights.get("mlPat",     0.20))

    for c in candidates:
        sym   = c.get("sym","").upper()
        score = float(c.get("score", 0))    # screener 0-100
        direc = c.get("dir","BUY").upper()  # screener direction

        price = prices.get(sym, 0)
        if not price or price <= 0:
            continue

        # ── Compute sub-scores for each strategy ────────────────────────────

        # MOMENTUM sub-score (uses screener score as proxy — it already
        # incorporates recent price action from Groq's scan)
        mom_score = score                               # 0-100

        # MEAN REVERSION sub-score — crude RSI proxy from screener context
        # Groq assigns lower scores to overextended moves; invert for mean-rev
        rev_score = max(0, 100 - score) if direc == "BUY" else score

        # BREAKOUT sub-score — screener high-score with BUY dir = near breakout
        brk_score = score if score >= 70 else 0

        # SENTIMENT sub-score
        # Fear/Greed: 50=neutral → 100 pts; 25 fear=25pts; 75 greed=75pts
        # VIX: 15=calm→100, 30=elevated→40, 40+=0
        sent_fg   = max(0, min(100, fg))
        sent_vix  = max(0, 100 - (vix - 15) * 4) if vix > 15 else 100
        sent_pc   = max(0, 100 - (pc - 0.7) * 80) if pc > 0.7 else 100
        sent_brd  = brd  # breadth already 0-100
        sent_score = (sent_fg * 0.35 + sent_vix * 0.30 + sent_brd * 0.25 + sent_pc * 0.10)

        # ML PATTERN sub-score — raw screener score from Groq model
        ml_score = score

        # ── Weighted composite signal ────────────────────────────────────────
        composite = (
            w_mom  * mom_score +
            w_rev  * rev_score +
            w_brk  * brk_score +
            w_sent * sent_score +
            w_ml   * ml_score
        )
        # Normalise to 0-100 (weights sum to ~1.0)
        confidence = round(min(99, composite))

        # ── Regime adjustments ───────────────────────────────────────────────
        if regime == "risk_off":
            confidence = round(confidence * 0.6)   # sharp cut in extreme fear
        elif regime == "cautious":
            confidence = round(confidence * 0.80)
        elif regime == "greed_warning":
            confidence = round(confidence * 0.92)  # slight discount in euphoria

        # ── Sell signals for open positions ──────────────────────────────────
        if sym in open_syms:
            pos = next((p for p in positions if p.get("sym")==sym), None)
            if pos:
                entry  = float(pos.get("entry", price))
                stop   = float(pos.get("stop", entry * 0.98))
                target = float(pos.get("target", entry * 1.06))
                upnl_pct = (price - entry) / entry * 100 if entry > 0 else 0

                # Sell rule: price hit stop, or screener reversed, or confidence collapsed
                if price <= stop:
                    signals.append({"symbol":sym,"action":"CLOSE","confidence":90,
                                    "entry":price,"target":target,"stop":stop,
                                    "reason":"stop_hit","rules_based":True})
                    continue
                if upnl_pct >= (target - entry) / entry * 100:
                    signals.append({"symbol":sym,"action":"CLOSE","confidence":85,
                                    "entry":price,"target":target,"stop":stop,
                                    "reason":"target_hit","rules_based":True})
                    continue
                # Hold if confident, sell if screener lost conviction
                if confidence >= 55:
                    signals.append({"symbol":sym,"action":"HOLD","confidence":confidence,
                                    "entry":price,"target":target,"stop":stop,
                                    "reason":"rules_hold","rules_based":True})
                else:
                    signals.append({"symbol":sym,"action":"CLOSE","confidence":70,
                                    "entry":price,"target":target,"stop":stop,
                                    "reason":"conviction_loss","rules_based":True})
            continue

        # ── New entry signals ─────────────────────────────────────────────────
        if direc == "SHORT":
            # Skip if already known to be non-shortable on this broker
            if _SHORTABLE_CACHE.get(sym) == False:
                continue
            # Short entry: only in normal/cautious regime, tight stop
            stop_pct   = 0.02  # 2% stop
            target_pct = 0.04  # 4% target
            stop   = round(price * (1 + stop_pct), 2)
            target = round(price * (1 - target_pct), 2)
            action = "SHORT"
        else:
            stop_pct   = 0.02
            target_pct = 0.06
            stop   = round(price * (1 - stop_pct), 2)
            target = round(price * (1 + target_pct), 2)
            action = "BUY"

        signals.append({
            "symbol": sym, "action": action, "confidence": confidence,
            "entry": price, "target": target, "stop": stop,
            "reason": f"rules:{regime}:score{score:.0f}", "rules_based": True
        })

    return signals


def _check_ai_intervention(state: dict, trades: list, thresholds: dict) -> tuple:
    """
    Returns (should_call_ai: bool, reason: str).

    Adaptive threshold system — base thresholds scale with market regime and
    recent performance so intervention fires when it's actually needed:

    Regime scaling rules:
      VIX calm (<18):      looser loss streak (+2), tighter daily loss (-1%), longer review (+12h)
      VIX elevated (25-35): tighter daily loss (-0.5%), tighter drawdown (-2%)
      VIX shock (≥35):      immediate intervention regardless of other thresholds
      High breadth (>75):  looser loss streak (+1) — trending market, losses expected
      Low breadth (<35):   tighter loss streak (-1) — choppy market, cut faster
      Strong WR (≥65%, 20+ trades): extend review interval (+24h), loosen loss streak (+1)
      Weak WR (<40%, 10+ trades):   tighten daily loss (-0.5%), tighten drawdown (-1%)
    """
    import time as _time

    portfolio = float(state.get("portfolio", 0) or 1)
    today_pnl = float(state.get("todayPnl", 0))
    wins      = int(state.get("winCount", 0))
    losses    = int(state.get("lossCount", 0))
    total     = wins + losses
    wr        = round(wins / total * 100) if total >= thresholds["wr_min_trades"] else 100
    sent      = state.get("sentData", {})
    vix       = float(sent.get("VIX", 20))
    fg        = float(sent.get("Fear/Greed", 50))
    breadth   = float(sent.get("Breadth", 65))
    now_ms    = int(_time.time() * 1000)

    # ── Compute recent win streak for momentum context ────────────────────
    recent = [t for t in trades[:20] if t.get("pnl") is not None]
    consec_losses = 0
    consec_wins   = 0
    for t in recent:
        pnl = t.get("pnl") or 0
        if pnl < 0:
            if consec_wins == 0: consec_losses += 1
            else: break
        else:
            if consec_losses == 0: consec_wins += 1
            else: break

    # ── Derive adaptive thresholds from base + regime ─────────────────────
    base_consec    = thresholds["consecutive_losses"]      # default 4
    base_wr_floor  = thresholds["wr_floor_pct"]            # default 35
    base_daily_pct = thresholds["daily_loss_pct"]          # default 3.0
    base_drawdown  = thresholds["drawdown_pct"]            # default 8.0
    base_review_h  = thresholds["deep_review_hours"]       # default 24
    base_vix_spike = thresholds["vix_spike"]               # default 35

    adj_consec   = base_consec
    adj_daily    = base_daily_pct
    adj_drawdown = base_drawdown
    adj_review_h = base_review_h
    reasons      = []  # scaling reasons for transparency in logs

    # ── VIX regime scaling ────────────────────────────────────────────────
    if vix < 18:
        # Calm market — some losses are normal noise, be more patient
        adj_consec   += 2
        adj_daily    -= 1.0   # but cap downside tighter ($) — volatility is low so moves are real
        adj_review_h += 12
        reasons.append(f"VIX calm({vix:.0f}): +2 streak tolerance, -1% daily, +12h review")
    elif vix < 25:
        # Normal range — no scaling
        pass
    elif vix < 35:
        # Elevated — tighten protection
        adj_daily    -= 0.5
        adj_drawdown -= 2.0
        reasons.append(f"VIX elevated({vix:.0f}): -0.5% daily, -2% drawdown")
    # VIX ≥ 35 handled as direct trigger below — no scaling needed

    # ── Market breadth scaling ────────────────────────────────────────────
    if breadth > 75:
        # Broad uptrend — mean-reversion losses expected, be more patient
        adj_consec += 1
        reasons.append(f"Breadth strong({breadth:.0f}): +1 streak tolerance")
    elif breadth < 35:
        # Breadth collapsed — high risk, cut faster
        adj_consec = max(2, adj_consec - 1)
        reasons.append(f"Breadth weak({breadth:.0f}): -1 streak tolerance")

    # ── Performance history scaling ────────────────────────────────────────
    if total >= 20 and wr >= 65:
        # Strong track record — trust the system, reduce interruptions
        adj_consec   += 1
        adj_review_h += 24
        reasons.append(f"WR strong({wr}% over {total}t): +1 streak, +24h review")
    elif total >= thresholds["wr_min_trades"] and wr < 40:
        # Weak track record — tighten protection
        adj_daily    -= 0.5
        adj_drawdown -= 1.0
        reasons.append(f"WR weak({wr}%): -0.5% daily, -1% drawdown")

    # ── Clamp to safe ranges ──────────────────────────────────────────────
    adj_consec   = max(2, min(10, adj_consec))
    adj_daily    = max(0.5, min(10.0, adj_daily))
    adj_drawdown = max(2.0, min(20.0, adj_drawdown))
    adj_review_h = max(6, min(72, adj_review_h))

    if reasons:
        log.debug("AI thresholds adapted: consec=%d daily=%.1f%% drawdown=%.1f%% review=%dh | %s",
                  adj_consec, adj_daily, adj_drawdown, adj_review_h, " | ".join(reasons))

    # ── Apply adapted thresholds ──────────────────────────────────────────

    # 1. Consecutive losses
    if consec_losses >= adj_consec:
        if interv_on_cooldown:
            log.debug("Consecutive loss intervention suppressed (cooldown)")
            return False, ""
        return True, (f"⚠️ {consec_losses} consecutive losses"
                      f" (threshold={adj_consec}, VIX={vix:.0f}, breadth={breadth:.0f}) — AI strategy review")

    # ── Non-scheduled intervention cooldown ─────────────────────────────
    # Prevents hammering Claude API every 3-min cycle when WR/losses are
    # persistently bad. 30-min minimum between non-scheduled interventions.
    last_interv = int(state.get("lastAIInterventionMs", 0))
    interv_cooldown_ms = 30 * 60 * 1000  # 30 minutes
    interv_on_cooldown = (now_ms - last_interv) < interv_cooldown_ms

    # 2. Win rate floor
    if total >= thresholds["wr_min_trades"] and wr < base_wr_floor:
        if interv_on_cooldown:
            mins_left = round((interv_cooldown_ms - (now_ms - last_interv)) / 60000)
            log.debug("WR intervention suppressed (cooldown %dm remaining)", mins_left)
            return False, ""
        return True, f"⚠️ Win rate {wr}% below floor {base_wr_floor}% — AI intervention"

    # 3. Daily loss
    if portfolio > 0 and today_pnl < -(portfolio * adj_daily / 100):
        if interv_on_cooldown:
            log.debug("Daily loss intervention suppressed (cooldown)")
            return False, ""
        return True, (f"⚠️ Daily loss ${today_pnl:+.0f} ({today_pnl/portfolio*100:.1f}%)"
                      f" exceeds adapted {adj_daily:.1f}% threshold — AI damage control")

    # 4. Drawdown from high-water mark
    hwm = float(state.get("portfolioHighWater", portfolio))
    if hwm > 0 and portfolio < hwm * (1 - adj_drawdown / 100):
        dd = (hwm - portfolio) / hwm * 100
        if interv_on_cooldown:
            log.debug("Drawdown intervention suppressed (cooldown)")
            return False, ""
        return True, (f"⚠️ Drawdown {dd:.1f}% from high ${hwm:,.0f}"
                      f" (adapted threshold={adj_drawdown:.1f}%) — AI portfolio review")

    # 5. VIX spike / market shock
    if vix >= base_vix_spike:
        return True, f"⚠️ VIX {vix:.1f} ≥ {base_vix_spike} — AI regime assessment"

    # 6. Scheduled deep review (adaptive interval)
    last_review = int(state.get("lastAIReviewMs", 0))
    review_interval_ms = adj_review_h * 3600 * 1000
    if now_ms - last_review > review_interval_ms:
        return True, f"📋 Scheduled {adj_review_h}h AI deep review (adapted from {base_review_h}h)"

    return False, ""


def _update_high_water_mark(state: dict) -> None:
    """Keep a rolling 30-day high-water mark for drawdown calculation."""
    portfolio = float(state.get("portfolio", 0))
    hwm = float(state.get("portfolioHighWater", 0))
    if portfolio > hwm:
        state["portfolioHighWater"] = portfolio


async def bot_trading_loop():
    await asyncio.sleep(120)
    while True:
        # Expire stop-loss cooldowns from previous trading days
        try:
            _today_cd = _et_now().strftime("%Y-%m-%d")
            _cd = STATE.get("stopLossCooldown", {})
            expired = [s for s, d in _cd.items() if d != _today_cd]
            for s in expired:
                del _cd[s]
                log.debug("Stop-loss cooldown expired for %s", s)
        except Exception:
            pass
        interval = _scan_interval("overnight")  # safe default before first mkt check
        try:
            if not STATE.get("botActive"): await asyncio.sleep(30); continue
            mkt = market_status(); et = _et_now()
            interval = _scan_interval(mkt["session"])

            log.info("Bot: cycle — session=%s et=%s next_scan=%dmin",
                     mkt["session"], et.strftime("%H:%M"), interval//60)

            # ── Pre-market opening scan: runs once per day at 9:10-9:25 ET ──
            hm = et.hour * 60 + et.minute
            if mkt["session"] == "pre_market" and 549 <= hm <= 565:  # 9:09–9:25 ET
                try:
                    await _run_premarket_opening_scan()
                except Exception as _pme:
                    log.error("Pre-market scan error: %s", _pme)
            # Build the bot prompt with current risk params and strategy weights
            rc = STATE.get("riskCfg", {})
            sw = STATE.get("stratWeights", {})
            swr = STATE.get("stratWR", {})
            px_now = STATE.get("prices", {})
            def _pos_line(p):
                sym      = p["sym"]
                entry    = float(p.get("entry", 0))
                cur      = float(px_now.get(sym, p.get("cur", entry)) or entry)
                size     = float(p.get("size", 0))
                unreal   = round((cur - entry) * size * (1 if p.get("dir","LONG")=="LONG" else -1), 2)
                pct      = round((cur - entry) / entry * 100, 2) if entry > 0 else 0
                source   = "imported" if p.get("source") in ("live_sync","broker_sync","auto_sync") else "bot"
                stop_v   = p.get("stop", round(entry*0.98,2))
                target_v = p.get("target", round(entry*1.06,2))
                held_h   = round((time.time()*1000 - p.get("open", time.time()*1000)) / 3600000, 1)
                return (f"{sym} {p.get('dir','LONG')} [{source}] entry=${entry:.2f} now=${cur:.2f} "
                        f"P&L={unreal:+.2f}({pct:+.1f}%) stop=${stop_v} target=${target_v} held={held_h}h")
            pos_summary = " | ".join(_pos_line(p) for p in STATE.get("positions",[])) or "none"
            wins    = STATE.get("winCount", 0)
            losses  = STATE.get("lossCount", 0)
            tot     = wins + losses
            wr      = round(wins / tot * 100) if tot else 0

            # Build session context so Claude knows what's tradeable right now
            tradeable_note = ""
            if mkt["session"] == "regular":
                tradeable_note = "Regular market hours — all instruments tradeable."
            elif mkt["session"] in ("pre_market","after_hours"):
                tradeable_note = f"{mkt['session'].replace('_',' ').title()} — crypto+extended hours equities tradeable. Liquidity lower."
            elif mkt["session"] == "weekend":
                tradeable_note = "Weekend — equity markets closed. Crypto only. Alpaca 24/5 resumes Sunday 8PM ET."
            elif mkt["session"] == "overnight_extended":
                tradeable_note = "Overnight extended (8PM-4AM ET) — Alpaca 24/5 active. Equities tradeable via limit orders. Lower liquidity, wider spreads expected."
            elif mkt["session"] == "overnight":
                tradeable_note = "Overnight — only crypto tradeable. Skip equity signals."
            else:
                tradeable_note = f"Session: {mkt['session']} — crypto always tradeable."

            # Build full price context — include all live SIP symbols
            px = STATE.get("prices", {})
            live_px = [(k,v) for k,v in px.items() if v > 0]
            # Sort: positions first, then by symbol name
            pos_syms = {p.get("sym","") for p in STATE.get("positions",[])}
            live_px.sort(key=lambda x: (0 if x[0] in pos_syms else 1, x[0]))
            price_list = " | ".join(f"{k}:${v:,.2f}" for k,v in live_px[:80])

            bot_memory = _build_trading_memory()
            has_imported = any(p.get("source") in ("live_sync","broker_sync","auto_sync")
                               for p in STATE.get("positions",[]))

            # Build tax optimization context for Claude
            try:
                _tax_trades = [t for t in TRADES[:100] if t.get("pnl") is not None]
                tax_data = _tax_summary(_tax_trades, float(OPT.get("tax_bracket",0.24)))
                _niit = tax_data["niit_applies"]
                _lt_r = tax_data["lt_rate_applied"]
                _wash_ct = tax_data["wash_sale_count"]
                _carryover = tax_data["loss_carryover"]
                px_now = STATE.get("prices",{})
                harvest_candidates = []
                for p in STATE.get("positions",[]):
                    s = p.get("sym",""); entry = p.get("entry",0)
                    cur = px_now.get(s,0) or entry
                    upnl = p.get("pnl") or ((cur-entry)*abs(p.get("size",0))*(1 if p.get("dir","LONG")=="LONG" else -1))
                    if upnl < -100:
                        harvest_candidates.append(f"{s}(${upnl:+.0f})")
                tax_ctx = (
                    f"YTD realized: ST=${tax_data['st_gains']:.0f} gains, ${tax_data['st_losses']:.0f} losses | "
                    f"LT=${tax_data['lt_gains']:.0f} gains | Est.Tax=${tax_data['estimated_tax']:.0f}\n"
                    f"LT rate: {_lt_r*100:.0f}% | {'NIIT 3.8% APPLIES (AGI>$200k)' if _niit else 'NIIT: N/A'} | "
                    f"{'⚠ '+str(_wash_ct)+' wash sales detected' if _wash_ct else 'No wash sales'} | "
                    f"Loss carryover: ${_carryover:.0f}\n"
                    + (f"HARVEST CANDIDATES: {', '.join(harvest_candidates)}" if harvest_candidates
                       else "No tax-loss harvest candidates.")
                )
            except Exception as _te:
                tax_ctx = "Tax data unavailable"

            # Adaptive confidence threshold — uses wr (win rate) computed above
            bot_wr   = wr  # alias so rest of loop is consistent
            conf_min = 75 if wr < 45 else 70 if wr < 55 else 65

            # Dynamic position capacity for prompt context
            try:
                _cap_avail = STATE.get("buyingPower",0) if OPT.get("allow_margin") else STATE.get("cash",0)
                if _cap_avail <= 0: _cap_avail = STATE.get("portfolio", 10000)
                _rc_pct = float(str(STATE.get("riskCfg",{}).get("maxPos","5%")).replace("%","")) / 100
                _per_pos = max(500, _cap_avail * _rc_pct)
                pos_capacity = max(3, min(20, int(_cap_avail / _per_pos))) if _per_pos > 0 else 5
            except Exception:
                pos_capacity = 5
            pos_used      = len(STATE.get("positions", []))
            pos_available = max(0, pos_capacity - pos_used)

            # Inject screener candidates if available
            candidates = STATE.get("screenerCandidates",[])
            screener_ctx = ""
            if candidates and STATE.get("screenerActive"):
                provider = STATE.get("screenerProvider","?")
                screener_ctx = (
                    f"\nSECONDARY AI PRE-SCREEN [{provider.upper()}] — top picks from full market scan:\n" +
                    "\n".join(
                        f"  {c['sym']} | score={c['score']}/100 | {c.get('dir','BUY')} | {c['reason']}"
                        for c in candidates
                    ) +
                    "\nAct on these picks. Direction (BUY/SHORT) is the screener's recommendation.\n"
                )

            # ══════════════════════════════════════════════════════════════
            #  RULES-BASED DECISION ENGINE (v2.0)
            #  Claude is an adviser — only called on distress thresholds
            # ══════════════════════════════════════════════════════════════

            # Update high-water mark for drawdown tracking
            _update_high_water_mark(STATE)

            # Check AI intervention thresholds
            _ai_needed, _ai_reason = _check_ai_intervention(
                STATE, TRADES, AI_INTERVENTION_THRESHOLDS)

            signals = []
            scan_text = ""

            STATE["aiAdviserActive"] = _ai_needed
            STATE["aiAdviserReason"] = _ai_reason if _ai_needed else ""
            if _ai_needed:
                # ── AI ADVISER MODE — build prompt and call Claude ────────────
                log.info("Bot: AI intervention triggered — %s", _ai_reason)
                OPTLOG.append({"ts": et.strftime("%H:%M:%S"), "type": "lw",
                    "msg": f"🤖 AI ADVISER: {_ai_reason}"})
                save_optlog()
                STATE["lastAIReviewMs"] = int(time.time() * 1000)
                if "Scheduled" not in _ai_reason:
                    STATE["lastAIInterventionMs"] = int(time.time() * 1000)

                # Compact adviser prompt — focused on what's wrong, not full scan
                _is_scheduled = "Scheduled" in _ai_reason
                _pos_count = len(STATE.get("positions", []))
                _max_pos = STATE.get("riskCfg", {}).get("maxPos", "5%")
                _at_max_pos = _pos_count >= 20
                _adviser_focus = (
                    "SCHEDULED DEEP REVIEW: Assess full portfolio, rebalance strategy weights, "
                    "flag any positions to close, and confirm rules-based parameters are optimal."
                    if _is_scheduled else
                    (
                        f"DISTRESS SITUATION: {_ai_reason}\n"
                        f"CRITICAL: Portfolio is at MAX POSITIONS ({_pos_count}/20). "
                        "No new entries are possible until existing positions close. "
                        "Focus on: (1) Which open positions should be closed NOW to free capital? "
                        "(2) Which positions are losing and should be cut? "
                        "(3) Adjust stop/target params to trigger exits on losers. "
                        "OUTPUT CLOSE SIGNALS using CLOSE action for specific symbols to exit. "
                        "Be concise — max 300 words."
                        if _at_max_pos else
                        f"DISTRESS SITUATION: {_ai_reason}\n"
                        "Focus ONLY on: (1) should we pause trading? (2) which positions need immediate "
                        "action? (3) what rule adjustments fix the problem? Be concise — max 300 words."
                    )
                )
                rc = STATE.get("riskCfg", {})
                sw = STATE.get("stratWeights", {})
                swr= STATE.get("stratWR", {})
                px_now = STATE.get("prices", {})
                wins    = STATE.get("winCount", 0)
                losses  = STATE.get("lossCount", 0)
                tot     = wins + losses
                wr      = round(wins / tot * 100) if tot else 0
                def _pos_line(p):
                    sym    = p["sym"]
                    entry  = float(p.get("entry", 0))
                    cur    = float(px_now.get(sym, entry) or entry)
                    size   = float(p.get("size", 0))
                    unreal = round((cur - entry) * size * (1 if p.get("dir","LONG")=="LONG" else -1), 2)
                    pct    = round((cur - entry) / entry * 100, 2) if entry > 0 else 0
                    return f"{sym} {p.get('dir','LONG')} entry=${entry:.2f} now=${cur:.2f} P&L={unreal:+.2f}({pct:+.1f}%)"
                pos_summary = " | ".join(_pos_line(p) for p in STATE.get("positions",[])) or "none"
                bot_memory  = _build_trading_memory()
                candidates  = STATE.get("screenerCandidates", [])
                cand_lines  = "\n".join(
                    f"  {c['sym']} score={c['score']}/100 {c.get('dir','BUY')} {c['reason']}"
                    for c in candidates[:20]
                ) or "none"

                ADVISER_PROMPT = f"""You are APEX AI Adviser. The rules-based bot called you because:
{_adviser_focus}

DATE: {et.strftime('%A %B %d %Y %H:%M ET')} | SESSION: {mkt['session'].upper()}
PORTFOLIO: ${STATE.get('portfolio',0):,.2f} | TODAY P&L: ${STATE.get('todayPnl',0):+.2f}
WIN RATE: {wr}% ({wins}W / {losses}L of {tot} trades)
POSITIONS: {pos_summary}
SCREENER TOP 20: {cand_lines}
STRATEGY WEIGHTS: {' | '.join(f"{k}={v*100:.0f}%" for k,v in sw.items())}
RISK: Stop={rc.get('stopL','2%')} TakeProfit={rc.get('takeP','6%')} MaxPos={rc.get('maxPos','5%')}
{bot_memory}

OUTPUT FORMAT (use exactly — rules engine parses these):
SIGNALS:
SYMBOL | BUY/SELL/HOLD/SHORT | CONFIDENCE% | Entry $X | Target $X | Stop $X | Reason

RISK_PARAMS:
STOP_LOSS: X%
TAKE_PROFIT: X%
MAX_POSITION: X%
REASONING: (one line)

STRATEGY_WEIGHTS:
momentum: X%
meanRev: X%
breakout: X%
sentiment: X%
mlPat: X%
REASONING: (one line)

AI_VERDICT: PAUSE_TRADING | REDUCE_EXPOSURE | CONTINUE | ADJUST_STRATEGY
ADVISER_NOTE: (one concise sentence on root cause and fix)"""

                result = await call_claude(ADVISER_PROMPT, 800)
                if result["ok"]:
                    scan_text = result["text"]
                    ANALYSES.insert(0, {"type": "aiAdviser", "ts": et.strftime("%H:%M:%S"),
                                        "text": scan_text, "savedAt": _et_now().isoformat(),
                                        "bot": True, "reason": _ai_reason})
                    save_analyses()
                    _apply_bot_risk_params(scan_text)
                    _apply_bot_strategy_weights(scan_text)

                    # Check for PAUSE_TRADING verdict
                    if "AI_VERDICT: PAUSE_TRADING" in scan_text:
                        log.warning("Bot: AI adviser issued PAUSE_TRADING")
                        OPTLOG.append({"ts": et.strftime("%H:%M:%S"), "type": "lw",
                            "msg": "🛑 AI ADVISER: PAUSE_TRADING issued — bot pausing 1 cycle"})
                        save_optlog()
                        await asyncio.sleep(interval)
                        continue

                    # Parse AI signals (override rules signals for this cycle)
                    ai_signals = _parse_signals(scan_text)
                    if ai_signals:
                        signals = ai_signals
                        log.info("Bot: AI adviser provided %d signals", len(signals))
                    else:
                        # AI gave no parseable signals — fall through to rules
                        log.info("Bot: AI adviser gave no signals — using rules engine")
                else:
                    log.warning("Bot: AI call failed (%s) — falling back to rules engine",
                                result.get("text","?"))

            if not signals:
                # ── RULES-BASED ENGINE — primary decision maker ───────────────
                candidates = STATE.get("screenerCandidates", [])
                if not candidates:
                    log.info("Bot: no screener candidates — skipping cycle")
                    await asyncio.sleep(interval)
                    continue

                signals = _compute_rules_signals(
                    candidates,
                    STATE.get("prices", {}),
                    STATE.get("positions", []),
                    STATE.get("sentData", {}),
                    STATE.get("stratWeights", {})
                )
                log.info("Bot: rules engine generated %d signals", len(signals))
                OPTLOG.append({"ts": et.strftime("%H:%M:%S"), "type": "li",
                    "msg": f"📐 Rules engine: {len(signals)} signals from {len(candidates)} candidates"
                           + (f" | AI on: {_ai_reason}" if _ai_needed else "")})
                save_optlog()
                STATE["rulesEngineCycles"] = STATE.get("rulesEngineCycles", 0) + 1
                STATE["aiAdviserActive"] = False

            # Adaptive confidence threshold
            wins   = STATE.get("winCount", 0)
            losses = STATE.get("lossCount", 0)
            tot    = wins + losses
            wr     = round(wins / tot * 100) if tot else 0
            conf_min = 75 if wr < 45 else 70 if wr < 55 else 65
            bot_wr   = wr

            actionable = [s for s in signals if s.get("action") not in ("HOLD","CLOSE") and s.get("confidence", 0) >= conf_min]
            log.info("Bot: %d signals, %d actionable (conf_min=%d%%, WR=%d%%, AI=%s)",
                     len(signals), len(actionable), conf_min, bot_wr, _ai_needed)

            for sig in actionable:  # use conf_min-filtered list, not raw signals
                side = "buy" if sig["action"]=="BUY" else "sell" if sig["action"]=="CLOSE" else "short"
                try:
                    r = await _execute_bot_trade(sig["symbol"], side, sig["confidence"],
                                                  sig["entry"], sig["target"], sig["stop"])
                    if r.get("ok"):
                        log.info("Bot executed: %s %s conf=%d%%", sig["action"], sig["symbol"], sig["confidence"])
                        OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "li",
                            "msg": f"{'📄 PAPER' if OPT['trading_mode']=='paper' else '💰 LIVE'} {sig['action']} {sig['symbol']} @ ${r.get('price', sig['entry']):.2f} | conf={sig['confidence']}% | stop=${sig['stop']:.2f} | target=${sig['target']:.2f}"})
                        save_optlog()
                    elif r.get("skipped"):
                        log.info("Bot skipped %s: %s", sig["symbol"], r.get("reason"))
                        # Record preflight/guard rejections for learning
                        _reason = r.get("reason", "unknown")
                        if not (_reason in ("bot not active", "daily loss limit",
                                            "already long", "already short") or
                                (_reason.startswith("max ") and "positions" in _reason)):
                            FAILED_TRADES.insert(0, {
                                "sym": sig["symbol"], "side": side, "dir": "LONG" if side=="buy" else "SHORT",
                                "reason": _reason, "type": "preflight_block",
                                "ts": _et_now().strftime("%H:%M:%S"),
                                "date": _et_now().strftime("%Y-%m-%d"),
                                "conf": sig.get("confidence", 0),
                                "entry": sig.get("entry", 0),
                            })
                            _MEMORY_CACHE["built_at"] = None  # invalidate so next Claude call sees new failure
                        OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "lw",
                            "msg": f"⏭ SKIP {sig['symbol']}: {r.get('reason','?')} ({'PAPER' if OPT['trading_mode']=='paper' else 'LIVE'})"})
                    else:
                        log.error("Bot error %s: %s", sig["symbol"], r.get("error","?"))
                        OPTLOG.append({"ts": _et_now().strftime("%H:%M:%S"), "type": "lw",
                            "msg": f"❌ ERROR {sig['symbol']}: {r.get('error','?')}"})
                except Exception as e:
                    log.error("Bot trade %s: %s", sig["symbol"], e)
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("bot_trading_loop: %s", e)
            STATE["botLoopError"] = str(e)
            interval = _scan_interval(market_status()["session"])
        else:
            STATE.pop("botLoopError", None)  # clear error flag on successful cycle
        log.info("Bot: sleeping %dmin until next scan", interval//60)
        await asyncio.sleep(interval)

# ── Background tasks ────────────────────────────────────────────────────────

async def _fetch_prices_backup(crypto_only: bool = False, fill_zeros_only: bool = False):
    """
    Lightweight backup fetch used during SIP primary mode.
    crypto_only: only fetch crypto symbols (BTC/USD, ETH/USD, etc.)
    fill_zeros_only: only fetch symbols with no price yet (price == 0)
    """
    active = _get_active_sources()
    if not active or (len(active)==1 and active[0][0]=="sim"):
        return

    px = STATE.get("prices", {})
    if crypto_only:
        target = [s for s in px if "/" in s or "-USD" in s.upper()]
    elif fill_zeros_only:
        target = [s for s in px if not px[s]]  # price is 0 or None
    else:
        target = list(px.keys())

    if not target:
        return

    connector = aiohttp.TCPConnector(limit=10, ssl=True)
    async with ClientSession(connector=connector, timeout=ClientTimeout(total=60)) as session:
        # Use Yahoo batch as primary backup source — fast, no key needed
        result = await _yahoo_batch(session, target)
        for sym, price in result.items():
            prev = px.get(sym, 0)
            if price > 0:
                if not prev or abs(price-prev)/max(prev,0.01) < 0.40:
                    STATE["prices"][sym] = round(price, 4)
        if result:
            STATE["lastPriceFetch"] = datetime.now().isoformat()

async def price_loop():
    """
    Price update loop.
    Priority:
      1. SIP WebSocket (primary) — real-time during market hours, all symbols automatically
      2. Yahoo batch / other sources (backup) — used when:
           a) SIP is disabled or disconnected
           b) Market is closed (pre/after/overnight/weekend) — SIP has no equity ticks
           c) Crypto symbols (SIP covers equities only)
    """
    await asyncio.sleep(15)
    _last_full_poll = 0.0  # monotonic time of last full poll
    import time as _time

    while True:
        try:
            stream_live = _alpaca_ws_connected and OPT.get("alpaca_stream_enabled", False)
            mkt = market_status()
            session_name = mkt.get("session", "")
            market_open  = mkt.get("open", False)

            if stream_live and market_open:
                # ── SIP PRIMARY MODE ─────────────────────────────────────
                # SIP streams ALL equity ticks in real-time — no polling needed.
                # Only do a backstop poll every 5 min to catch any gaps.
                sip_count = len([v for v in STATE.get("prices", {}).values() if v > 0])
                log.debug("price_loop: SIP primary — %d symbols live", sip_count)

                if _time.monotonic() - _last_full_poll > 300:  # 5-min backstop
                    # Backstop: fetch crypto (not covered by equity SIP) + fill any zeros
                    await _fetch_prices_backup(crypto_only=False, fill_zeros_only=True)
                    _last_full_poll = _time.monotonic()
                    log.debug("price_loop: SIP backstop poll complete")

                await asyncio.sleep(5)  # tight loop — just checking SIP health

            else:
                # ── POLLING MODE ─────────────────────────────────────────
                # SIP offline OR market closed → poll all sources for prices
                active = _get_active_sources()
                has_real = any(s != "sim" for s, _ in active)
                if has_real:
                    await fetch_all_prices()
                    _last_full_poll = _time.monotonic()
                    reason = "SIP disconnected" if not stream_live else f"market {session_name}"
                    log.debug("price_loop: polling mode (%s)", reason)
                await asyncio.sleep(30)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("price_loop: %s", e)
            await asyncio.sleep(30)

async def sentiment_loop():
    """
    Fetch real market sentiment data every 5 minutes.
    Sources: Fear & Greed from CNN API, VIX from Yahoo Finance.
    Falls back to gentle random walk only if all fetches fail.
    """
    import random
    _SENT_INTERVAL = 300  # 5 minutes
    _err_count = 0

    while True:
        await asyncio.sleep(_SENT_INTERVAL if _err_count == 0 else 60)
        try:
            updated = False

            # ── Fear & Greed Index (CNN) ──────────────────────────────────
            try:
                async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
                    async with session.get(
                        "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                        headers={"User-Agent":"Mozilla/5.0"},
                        timeout=ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            d = json.loads(await r.text())
                            score = float(d.get("fear_and_greed",{}).get("score", 50))
                            STATE["sentData"]["Fear/Greed"] = round(score, 1)
                            updated = True
            except Exception:
                pass

            # ── VIX from Yahoo Finance ────────────────────────────────────
            try:
                async with ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
                    vix = await _yahoo(session, "^VIX")
                    if vix and 5 < vix < 100:
                        STATE["sentData"]["VIX"] = round(vix, 2)
                        updated = True
            except Exception:
                pass

            # ── Put/Call ratio — gentle random walk (no free API) ─────────
            pc = STATE["sentData"].get("Put/Call", 0.85)
            STATE["sentData"]["Put/Call"] = round(max(0.4, min(1.8, pc + random.uniform(-0.02, 0.02))), 2)

            # ── Market Breadth — derive from prices if available ──────────
            px = STATE.get("prices", {})
            if len(px) >= 10:
                # Simple breadth: pct of tracked symbols above their ~recent avg
                # Use VIX as inverse proxy: low VIX → strong breadth
                vix_now = STATE["sentData"].get("VIX", 20)
                breadth = round(max(10, min(95, 85 - (vix_now - 15) * 1.5)), 1)
                STATE["sentData"]["Breadth"] = breadth
                updated = True

            if updated:
                _err_count = 0
                save_state()
            else:
                # Full fallback: gentle drift only
                _err_count += 1
                for k,(lo,hi) in {"Fear/Greed":(0,100),"VIX":(10,60),"Put/Call":(0.4,1.8),"Breadth":(10,95)}.items():
                    v = STATE["sentData"].get(k, 50)
                    STATE["sentData"][k] = round(max(lo, min(hi, v + random.uniform(-0.5, 0.5))), 2)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.debug("sentiment_loop: %s", e)
            _err_count += 1

async def shortability_cache_refresh_loop():
    """
    Periodically re-queries Alpaca for shortable/easy_to_borrow status on all
    symbols in _SHORTABLE_CACHE and _BROKER_CONSTRAINTS. Runs every 45 minutes.
    This keeps the cache warm and accurate without relying on restarts.
    """
    REFRESH_INTERVAL = 45 * 60   # 45 minutes
    await asyncio.sleep(60)      # let startup settle first
    while True:
        try:
            key    = OPT.get("broker_api_key", "").strip()
            secret = OPT.get("broker_api_secret", "").strip()
            base   = _alpaca_base(OPT.get("broker_api_url", "").strip())
            if key and secret and OPT.get("broker_platform", "").lower() == "alpaca":
                # Collect all symbols we've ever checked
                symbols_to_refresh = set(_SHORTABLE_CACHE.keys()) | {
                    k.split(":", 1)[1]
                    for k in _BROKER_CONSTRAINTS
                    if k.startswith("alpaca:")
                }
                # Also add all symbols currently being watched (prices + positions)
                symbols_to_refresh |= set(STATE.get("prices", {}).keys())
                symbols_to_refresh |= {p.get("sym") for p in STATE.get("positions", []) if p.get("sym")}
                symbols_to_refresh = {s for s in symbols_to_refresh
                                      if s and "/" not in s and s not in CRYPTO_SYMBOLS}

                if symbols_to_refresh:
                    log.info("Shortability cache refresh: checking %d symbols", len(symbols_to_refresh))
                    h = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret,
                         "Content-Type": "application/json"}
                    refreshed = updated = 0
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=True)) as session:
                        for sym in sorted(symbols_to_refresh):
                            try:
                                async with session.get(
                                        f"{base}/assets/{sym}", headers=h,
                                        timeout=aiohttp.ClientTimeout(total=5)) as r:
                                    if r.status == 200:
                                        d = json.loads(await r.text())
                                        shortable = bool(d.get("shortable", False)) and bool(d.get("easy_to_borrow", False))
                                        old = _SHORTABLE_CACHE.get(sym)
                                        _SHORTABLE_CACHE[sym] = shortable
                                        # Also update _BROKER_CONSTRAINTS entry
                                        ck = f"alpaca:{sym}"
                                        if ck in _BROKER_CONSTRAINTS:
                                            _BROKER_CONSTRAINTS[ck]["shortable"]      = shortable
                                            _BROKER_CONSTRAINTS[ck]["easy_to_borrow"] = bool(d.get("easy_to_borrow", False))
                                            _BROKER_CONSTRAINTS[ck]["source"]         = "live"
                                        else:
                                            # Include min_notional so broker_preflight never KeyErrors
                                            _mn = BROKER_REGISTRY.get("alpaca",{}).get("fallback",{}).get("min_notional",1.0)
                                            _BROKER_CONSTRAINTS[ck] = {
                                                "shortable":          shortable,
                                                "easy_to_borrow":     bool(d.get("easy_to_borrow", False)),
                                                "fractionable":       bool(d.get("fractionable", False)),
                                                "overnight_tradable": bool(d.get("overnight_tradable", False)),
                                                "tradable":           bool(d.get("tradable", True)),
                                                "min_notional":       _mn,
                                                "min_qty":            0.0,
                                                "source":             "live",
                                            }
                                        refreshed += 1
                                        if old != shortable:
                                            updated += 1
                                            log.info("Shortability changed: %s %s → %s",
                                                     sym, old, shortable)
                                    elif r.status == 429:
                                        log.debug("Shortability refresh rate-limited — stopping early")
                                        break
                            except Exception as e:
                                log.debug("Shortability refresh %s: %s", sym, e)
                            await asyncio.sleep(0.15)   # ~7/sec — well under Alpaca's 200/min limit
                    log.info("Shortability cache refresh complete: %d checked, %d changed", refreshed, updated)
        except Exception as e:
            log.warning("shortability_cache_refresh_loop error: %s", e)
        await asyncio.sleep(REFRESH_INTERVAL)


async def on_startup(app):
    log.info("Dashboard ready on port %d",PORT)
    # Run memory purge on startup — archives old trades to lifetime summary
    try:
        _purge_old_trades()
        summary = _load_lifetime_summary()
        if summary.get("total_trades",0) > 0:
            log.info("Trading memory: %d lifetime trades archived | last purge: %s",
                     summary["total_trades"], summary.get("last_purge_date","never"))
    except Exception as e:
        log.warning("Memory purge on startup failed: %s", e)
    asyncio.create_task(fetch_all_prices())
    asyncio.create_task(shortability_cache_refresh_loop())  # refresh shortable/ETB cache every 45min
    asyncio.create_task(balance_sync_loop())  # sync balance, positions, fills every 10s
    asyncio.create_task(secondary_screener_loop())  # secondary AI screener (when configured)
    app["p"] = asyncio.create_task(price_loop())
    app["s"] = asyncio.create_task(sentiment_loop())
    app["b"] = asyncio.create_task(bot_trading_loop())
    app["sl"]= asyncio.create_task(stop_loss_loop())
    app["ws"]= asyncio.create_task(alpaca_stream_loop())  # Alpaca WS stream (no-op if disabled)
    # Auto-start bot if broker key is configured
    STATE["tradingMode"] = OPT.get("trading_mode", "paper")  # ensure STATE is current
    if OPT.get("broker_api_key"):
        STATE["botActive"] = True
        log.info("Bot auto-started (broker key found in config)")
        # Immediately connect broker so UI shows balance on first page load
        # (balance_sync_loop has a 5s delay; this gives instant feedback)
        try:
            result = await fetch_broker_balance()
            if result.get("ok"):
                log.info("Startup broker connect OK: $%.2f | %d positions",
                         result.get("balance",0), len(STATE.get("positions",[])))
            else:
                log.warning("Startup broker connect failed: %s", result.get("error","?"))
        except Exception as _e:
            log.warning("Startup broker connect error: %s", _e)
    if OPT.get("alpaca_stream_enabled"):
        feed = OPT.get("alpaca_stream_feed","iex")
        log.info("Alpaca WebSocket stream ENABLED — feed=%s", feed)
    else:
        log.info("Alpaca WebSocket stream disabled (alpaca_stream_enabled=false)")

async def on_cleanup(app):
    log.info("Shutdown — flushing all data");
    save_all_immediate()
    for t in ("p","s","b","sl","ws"):
        task = app.get(t)
        if task: task.cancel()

@web.middleware
async def cors_mw(req,handler):
    resp=await handler(req)
    resp.headers["Access-Control-Allow-Origin"]="*"
    resp.headers["Access-Control-Allow-Methods"]="GET,POST,PUT,DELETE,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"]="Content-Type,Authorization"
    return resp

if __name__=="__main__":
    app=web.Application(middlewares=[cors_mw])
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    log.info("Starting on port %d",PORT)
    # Custom access logger using ET time
class _ETAccessLogger(aiohttp.abc.AbstractAccessLogger):
    def log(self, request, response, time_taken):
        et_ts = _et_now().strftime("%d/%b/%Y:%H:%M:%S %z")
        log.info('%s [%s] "%s %s" %d %s', 
                  request.remote, et_ts,
                  request.method, request.path_qs,
                  response.status, response.content_length or "-")

web.run_app(app, host="0.0.0.0", port=PORT, print=None,
            access_log_class=_ETAccessLogger)