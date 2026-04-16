# Changelog — APEX Trading AI

## [1.69.19] - 2026-04-14
### Fixed
- Bot Rules sidebar link appearing in wrong location (orphaned `<li>` prepended before `<!DOCTYPE>`)
- Startup log now shows actual version (`v1.69.19`) instead of hardcoded `v1.0.0`
- Version number now increments with every build — visible in HA info page and log

## [1.69.18] - 2026-04-14
### Added
- Alpaca 24/5 overnight session fully implemented: Sunday 8PM ET through Friday 8PM ET
- `_check_overnight_tradable()` — calls Alpaca Assets API before placing overnight orders; skips non-eligible assets
- Margin 2x cap during extended/overnight sessions (Alpaca restricts to 2x vs 4x intraday)
- Extended hours warning log for every limit order placed outside regular hours
### Fixed
- Saturday now fully blocked; Sunday before 8PM blocked; Sunday after 8PM → `overnight_extended`
- Weekend session message updated to note 24/5 resumes Sunday 8PM ET

## [1.69.17] - 2026-04-14
### Added
- Full Alpaca 24/5 support: new `overnight_extended` session (8PM–4AM ET weekdays)
- All extended sessions (pre-market, after-hours, overnight) now use `limit` orders with `extended_hours: true`
- Session label shows "24/5 ACTIVE" in amber during overnight extended hours
- Overnight equity buys now allowed in all three extended sessions (was blocked outside pre/after-hours)
- Scan interval during overnight_extended = 30 min (same as after-hours, was 60 min dead zone)

## [1.69.16] - 2026-04-14
### Added
- Rules priority system: Claude-owned fields shown with teal left border and tooltip
- Override confirmation dialog when user changes a field Claude last set
- Change history panel in Live Rule Status (last 6 changes with 🤖/👤 icons and timestamps)
- `source` field on all rule saves ("user" vs "claude") stored in `rulesHistory`
- `claudeOverrides` dict tracks which fields Claude last set and with what reasoning
- User saves clear Claude's override flag on changed fields only (partial override supported)

## [1.69.15] - 2026-04-14
### Added
- **Bot Rules tab** — full rules editor accessible from sidebar, desktop tabs, and mobile nav
- All rules editable in real-time: stop loss, take profit, max position, daily loss limit, max short hold
- All AI intervention thresholds editable: consecutive losses, WR floor, daily loss %, drawdown %, VIX spike, review interval
- Strategy weight sliders for all 7 strategies with live sum validator and WR shown per strategy
- Live Rule Status panel: portfolio vs high-water, drawdown, WR, today P&L, rules/AI cycle split
- Claude Rule Optimizer: `/api/rules/optimize` endpoint — Claude reviews all rules against performance and auto-applies changes
- Force Optimize button for manual trigger regardless of performance state
- `/api/rules` GET and POST endpoints for programmatic rule access

## [1.69.14] - 2026-04-13
### Fixed
- Short position stop/target reversed on broker-imported positions (was `entry×0.98`/`entry×1.06` for both longs and shorts)
- All three import paths now assign direction-correct stops: SHORT `stop=entry×1.02`, `target=entry×0.94`
- New: 5-day maximum short hold time — shorts force-closed after 5 calendar days
### Added
- Full broker account panel on dashboard: Long Market Value, Short Market Value, Buying Power, Init/Maint Margin, Daytrade BP, Position Market Value
- Cash Balance stat card on dashboard (was missing despite being synced)
- Portfolio equity sub-label shows `±$X vs last close` using Alpaca's `last_equity` field
- All Alpaca account fields now stored: `longMarketValue`, `shortMarketValue`, `positionMarketValue`, `initMargin`, `maintMargin`, `regtBP`, `daytradeBP`, `lastEquity`, `accruedFees`

## [1.69.13] - 2026-04-13
### Fixed
- HTML/JS corruption (duplicate `<script>` tags, orphaned `visibilitychange` fragment, duplicate page lifecycle block)
- Clean rebuild from v69.08 reference base with all features transplanted correctly
### Included (all features from v69.08–v69.12)
- Sortable columns in History, P&L, Orders (all columns, ascending/descending)
- Tax engine fully connected to `_tax_summary()` via `/api/tax` endpoint
- Harvest candidates table, 4 summary cards, ST/LT breakdown table
- Phone UI redesign, desktop polish, frosted glass header, responsive CSS

## [1.69.12] - 2026-04-13
### Added
- Full symbol universe scan: screener now passes ALL available symbols (was capped at 250 equities + 30 crypto)
- Batch processing: 500 symbols per Groq call, 2.5s pause between batches (stays within free tier limits)
- SIP symbol cap raised from 5,000 to 10,000
- Version number in `config.yaml` matches zip file number going forward

## [1.69.11] - 2026-04-13
### Added
- Groq screener prompt reduced to discovery-only (symbol list, no scoring) — 75% token reduction per call
- Bot now scores candidates itself using: Groq ordering bonus, volatility bucket, round-number proximity, sentiment overlay
- Legacy scored format (Groq providing scores) still supported as fallback

## [1.69.10] - 2026-04-13
### Added
- Rules-based trading engine — Claude called only on intervention thresholds (95%+ of trades are pure rules)
- AI intervention thresholds: consecutive losses, WR floor, daily loss %, drawdown %, VIX spike, scheduled 24h review
- `📐 RULES` / `🤖 AI ADVISER` pill in header showing current engine mode
- Engine Mode stat card showing rules vs AI cycle counts and % AI usage
- `portfolioHighWater` for drawdown tracking
- AI adviser prompt is compact (800 tokens max) focused only on the problem

## [1.69.09] - 2026-04-09
### Added
- Sortable columns in History table (11 columns), P&L table (6 columns), Orders table (8 columns)
- Shared sort engine: `_sortState`, `_sortRows()`, `_makeTh()` — click headers to sort ▲/▼

## [1.69.08] - 2026-04-09
### Fixed
- Missing `<script>` tag corruption that caused blank UI (persistent bug from v69.07)
- JS rebuilt cleanly from v69.05 reference base with all new features transplanted
### Added
- Tax engine fully connected: `/api/tax` endpoint runs `_tax_summary()` against full trade history
- Tax tab: 4 summary cards, full §1091/§1222/§1411/§1211 breakdown table, harvest candidates

## [1.1.0] - 2026-04-09
### Added
- Phone UI complete redesign: frosted glass bottom nav, 44px touch targets, safe-area support
- Desktop polish: frosted glass header, card hover glow, pill glow
- `viewport-fit=cover` for iPhone notch support
- `@media(max-width:480px)` breakpoint for small phones
- Server-side trailing stops: breakeven at +3%, 50% profit lock at +6%
- Persistent key vault: API keys survive add-on reinstall
- SIP-primary price architecture with Yahoo/Finnhub fallback
- Extended hours order handling: after-hours uses LIMIT orders
- Claude API retry with exponential backoff (15s/30s/60s)

## [1.0.0] - 2025-03-25
### Added
- Initial release as a Home Assistant OS add-on
- Claude AI market analysis (claude-sonnet-4-5)
- Live market data: Yahoo Finance, Finnhub, Alpha Vantage (server-side, no CORS)
- Broker integration: Alpaca Markets (paper + live), Binance, Coinbase, Kraken, IBKR
- Persistent storage in `/config/apex_trading/`
- Self-optimization engine (Kelly criterion, Sharpe ratio)
- Tax engine: YTD ST/LT tracking, loss harvesting, wash sale detection
- HA ingress, multi-arch (amd64, aarch64, armv7, armhf, i386)
