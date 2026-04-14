#!/usr/bin/env bash
# ╔══════════════════════════════════════════════════════════════════════╗
# ║          APEX Trading AI — Self-Installer for Home Assistant OS      ║
# ║                        Version 1.69.19                                 ║
# ╚══════════════════════════════════════════════════════════════════════╝
#
# USAGE (run from your HA host terminal or via SSH):
#   curl -fsSL https://raw.githubusercontent.com/sam3gp8/apex-trading-addon/main/install.sh | bash
#
#   Or if you downloaded this file:
#   chmod +x install.sh && ./install.sh
#
# REQUIREMENTS:
#   - Home Assistant OS or Home Assistant Supervised
#   - SSH access to your HA host (or use the Terminal & SSH add-on)
#   - Internet access from the HA host

set -euo pipefail

# ── COLOURS ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
PURPLE='\033[0;35m'

ok()   { echo -e "${GREEN}  ✓${RESET} $*"; }
info() { echo -e "${CYAN}  ⓘ${RESET} $*"; }
warn() { echo -e "${YELLOW}  ⚠${RESET} $*"; }
err()  { echo -e "${RED}  ✗ ERROR:${RESET} $*" >&2; }
step() { echo -e "\n${BOLD}${PURPLE}══ $* ${RESET}"; }
banner() {
  echo -e "${CYAN}"
  echo '  ███████████████████████████████████████████'
  echo '  █                                         █'
  echo '  █   ▄▄▄    ██████  ███████ ██   ██       █'
  echo '  █  ██  ██  ██   ██ ██      ╚██ ██        █'
  echo '  █  ██████  ██████  █████    ████         █'
  echo '  █  ██  ██  ██      ██        ██          █'
  echo '  █  ██  ██  ██      ███████   ██          █'
  echo '  █                                         █'
  echo '  █   AI Trading Intelligence for HAOS      █'
  echo '  █            Version 1.69.19                █'
  echo '  ███████████████████████████████████████████'
  echo -e "${RESET}"
}

# ── CONFIG ─────────────────────────────────────────────────────────────
ADDON_SLUG="apex_trading"
ADDON_DIR="/addons/${ADDON_SLUG}"
ADDON_DATA_DIR="/addon_configs/${ADDON_SLUG}"
REPO_URL="${APEX_REPO_URL:-https://github.com/your-repo/apex-trading-addon}"
RELEASE_ZIP="${APEX_ZIP_URL:-${REPO_URL}/archive/refs/heads/main.zip}"
TEMP_DIR="$(mktemp -d /tmp/apex_install.XXXXXX)"
HA_CONFIG_DIR="/homeassistant"
HA_ADDONS_DIR="/addons"

# ── CLEANUP ON EXIT ─────────────────────────────────────────────────────
cleanup() { rm -rf "${TEMP_DIR}"; }
trap cleanup EXIT

# ── DETECT ENVIRONMENT ──────────────────────────────────────────────────
detect_environment() {
  step "Detecting Environment"

  # Check if running on HAOS/Supervised
  if [ -f /etc/os-release ]; then
    source /etc/os-release 2>/dev/null || true
    if echo "${ID:-}" | grep -qi "haos\|homeassistant"; then
      ok "Detected: Home Assistant OS"
      ENV_TYPE="haos"
    elif [ -f /usr/bin/hassio ] || [ -f /usr/local/bin/hassio ]; then
      ok "Detected: Home Assistant Supervised"
      ENV_TYPE="supervised"
    else
      warn "OS: ${PRETTY_NAME:-Unknown}. Attempting installation anyway."
      ENV_TYPE="unknown"
    fi
  elif command -v hassio &>/dev/null; then
    ok "Detected: Home Assistant (hassio CLI available)"
    ENV_TYPE="supervised"
  else
    err "Cannot detect Home Assistant environment."
    err "Run this script on your HA host via SSH or the Terminal add-on."
    exit 1
  fi

  # Check for required tools
  for tool in curl unzip cp mkdir; do
    if command -v "$tool" &>/dev/null; then
      ok "Found: $tool"
    else
      err "Required tool not found: $tool"
      exit 1
    fi
  done
}

# ── CHECK EXISTING INSTALLATION ─────────────────────────────────────────
check_existing() {
  step "Checking for Existing Installation"
  if [ -d "${ADDON_DIR}" ]; then
    warn "APEX is already installed at ${ADDON_DIR}"
    echo -n -e "  ${YELLOW}Overwrite/upgrade existing installation? [y/N]${RESET} "
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
      info "Installation cancelled. Existing installation preserved."
      exit 0
    fi
    info "Upgrading existing installation..."
    # Back up existing data
    if [ -d "${ADDON_DATA_DIR}" ]; then
      BACKUP_DIR="${ADDON_DATA_DIR}_backup_$(date +%Y%m%d_%H%M%S)"
      cp -r "${ADDON_DATA_DIR}" "${BACKUP_DIR}" 2>/dev/null || true
      ok "Data backed up to: ${BACKUP_DIR}"
    fi
  else
    ok "No existing installation found — fresh install"
  fi
}

# ── DOWNLOAD ────────────────────────────────────────────────────────────
download_addon() {
  step "Downloading APEX Trading AI"

  # Check if files were provided locally (e.g., extracted zip)
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [ -f "${SCRIPT_DIR}/apex_trading/config.yaml" ]; then
    info "Using local files from: ${SCRIPT_DIR}"
    SOURCE_DIR="${SCRIPT_DIR}"
    return 0
  fi

  info "Downloading from: ${RELEASE_ZIP}"
  if curl -fsSL --progress-bar "${RELEASE_ZIP}" -o "${TEMP_DIR}/apex.zip"; then
    ok "Download complete"
  else
    err "Download failed. Check your internet connection and the repository URL."
    err "URL attempted: ${RELEASE_ZIP}"
    exit 1
  fi

  info "Extracting..."
  unzip -q "${TEMP_DIR}/apex.zip" -d "${TEMP_DIR}/extracted"

  # Find the extracted directory (handles GitHub's nested folder)
  EXTRACTED_DIR=$(find "${TEMP_DIR}/extracted" -maxdepth 2 -name "config.yaml" -path "*/apex_trading/*" | head -1 | xargs dirname 2>/dev/null || true)
  if [ -z "${EXTRACTED_DIR}" ]; then
    # Try root level
    EXTRACTED_DIR=$(find "${TEMP_DIR}/extracted" -maxdepth 1 -type d | head -2 | tail -1)
  fi

  if [ -z "${EXTRACTED_DIR}" ] || [ ! -d "${EXTRACTED_DIR}" ]; then
    err "Could not find addon files in the downloaded archive."
    ls -la "${TEMP_DIR}/extracted/" || true
    exit 1
  fi

  SOURCE_DIR="$(dirname "${EXTRACTED_DIR}")"
  ok "Extracted to: ${EXTRACTED_DIR}"
}

# ── WRITE ADDON FILES (used when downloading is not available) ───────────
write_embedded_files() {
  # This function writes the addon files directly to disk
  # Used as a fallback when the repository is not yet public
  step "Writing Add-on Files"

  mkdir -p "${ADDON_DIR}"
  mkdir -p "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/apex"
  mkdir -p "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/init-apex"
  mkdir -p "${ADDON_DIR}/rootfs/opt/apex/static"
  mkdir -p "${ADDON_DIR}/rootfs/opt/apex/data"
  mkdir -p "${ADDON_DIR}/translations"

  info "Writing config.yaml..."
  cat > "${ADDON_DIR}/config.yaml" << 'CONFIG_EOF'
name: "APEX Trading AI"
description: "AI-powered trading bot with Claude market analysis, live price feeds, portfolio tracking, tax optimization, and self-optimization engine."
version: "1.0.0"
slug: "apex_trading"
url: "https://github.com/your-repo/apex-trading-addon"
arch:
  - aarch64
  - amd64
  - armhf
  - armv7
  - i386
startup: application
boot: auto
ports:
  7123/tcp: 7123
ports_description:
  7123/tcp: "APEX Trading Dashboard"
ingress: true
ingress_port: 7123
panel_icon: mdi:chart-line
panel_title: "APEX Trading"
options:
  anthropic_api_key: ""
  market_data_source: "yahoo"
  market_data_key: ""
  trading_mode: "paper"
  risk_level: "moderate"
  tax_bracket: 0.24
  stop_loss_pct: 2.0
  take_profit_pct: 6.0
  max_position_pct: 5.0
  daily_loss_limit: 500.0
  auto_analysis_interval: 600
  broker_platform: ""
  broker_api_url: ""
  broker_api_key: ""
  broker_api_secret: ""
  log_level: "info"
schema:
  anthropic_api_key: str
  market_data_source: "list(yahoo|finnhub|alphavantage|sim)"
  market_data_key: str
  trading_mode: "list(paper|live)"
  risk_level: "list(conservative|moderate|aggressive)"
  tax_bracket: float
  stop_loss_pct: float
  take_profit_pct: float
  max_position_pct: float
  daily_loss_limit: float
  auto_analysis_interval: int
  broker_platform: str
  broker_api_url: str
  broker_api_secret: str
  log_level: "list(debug|info|warning|error)"
map:
  - config:rw
  - data:rw
CONFIG_EOF
  ok "config.yaml written"

  info "Writing Dockerfile..."
  cat > "${ADDON_DIR}/Dockerfile" << 'DOCKER_EOF'
ARG BUILD_FROM
FROM $BUILD_FROM
RUN apk add --no-cache python3 py3-pip py3-aiohttp py3-aiofiles
RUN pip3 install --no-cache-dir aiohttp aiofiles httpx --break-system-packages 2>/dev/null || \
    pip3 install --no-cache-dir aiohttp aiofiles httpx
COPY rootfs /
WORKDIR /opt/apex
RUN chmod +x /etc/s6-overlay/s6-rc.d/apex/run \
    && chmod +x /etc/s6-overlay/s6-rc.d/apex/finish \
    && chmod +x /etc/s6-overlay/s6-rc.d/init-apex/run
LABEL io.hass.name="APEX Trading AI" \
      io.hass.type="addon" \
      io.hass.version=${BUILD_VERSION}
DOCKER_EOF
  ok "Dockerfile written"

  # s6 service files
  cat > "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/apex/type" << 'EOF'
longrun
EOF

  cat > "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/apex/run" << 'EOF'
#!/command/with-contenv bashio
set -e
bashio::log.info "Starting APEX Trading AI..."
export ANTHROPIC_API_KEY="$(bashio::config 'anthropic_api_key')"
export MARKET_DATA_SOURCE="$(bashio::config 'market_data_source')"
export MARKET_DATA_KEY="$(bashio::config 'market_data_key')"
export TRADING_MODE="$(bashio::config 'trading_mode')"
export RISK_LEVEL="$(bashio::config 'risk_level')"
export TAX_BRACKET="$(bashio::config 'tax_bracket')"
export STOP_LOSS_PCT="$(bashio::config 'stop_loss_pct')"
export TAKE_PROFIT_PCT="$(bashio::config 'take_profit_pct')"
export MAX_POSITION_PCT="$(bashio::config 'max_position_pct')"
export DAILY_LOSS_LIMIT="$(bashio::config 'daily_loss_limit')"
export AUTO_ANALYSIS_INTERVAL="$(bashio::config 'auto_analysis_interval')"
export BROKER_PLATFORM="$(bashio::config 'broker_platform')"
export BROKER_API_URL="$(bashio::config 'broker_api_url')"
export BROKER_API_KEY="$(bashio::config 'broker_api_key')"
export BROKER_API_SECRET="$(bashio::config 'broker_api_secret')"
export LOG_LEVEL="$(bashio::config 'log_level')"
export DATA_DIR="/data/apex"
export PORT=7123
mkdir -p "${DATA_DIR}"
exec python3 /opt/apex/server.py
EOF

  cat > "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/apex/finish" << 'EOF'
#!/command/with-contenv bashio
bashio::log.info "APEX stopped (exit: $1)"
EOF

  cat > "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/init-apex/type" << 'EOF'
oneshot
EOF

  cat > "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/init-apex/run" << 'EOF'
#!/command/with-contenv bashio
set -e
mkdir -p /data/apex
for f in trades.json state.json optlog.json analyses.json; do
  [ -f "/data/apex/$f" ] || echo "[]" > "/data/apex/$f"
done
bashio::log.info "APEX data directory ready"
EOF

  chmod +x "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/apex/run"
  chmod +x "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/apex/finish"
  chmod +x "${ADDON_DIR}/rootfs/etc/s6-overlay/s6-rc.d/init-apex/run"

  ok "s6 service files written"

  info "Writing translation file..."
  cat > "${ADDON_DIR}/translations/en.json" << 'EOF'
{
  "configuration": {
    "anthropic_api_key": {"name": "Anthropic API Key", "description": "Your Claude AI key from console.anthropic.com"},
    "market_data_source": {"name": "Market Data Source", "description": "yahoo=free/no key, finnhub=free key, alphavantage=free key, sim=offline"},
    "trading_mode": {"name": "Trading Mode", "description": "paper=simulation, live=real orders"},
    "risk_level": {"name": "Risk Level", "description": "conservative=1-2%, moderate=2-4%, aggressive=4-8%"},
    "broker_platform": {"name": "Broker Platform", "description": "alpaca, binance, coinbase, kraken, etc."},
    "broker_api_url": {"name": "Broker API URL", "description": "e.g. https://api.alpaca.markets/v2"},
    "broker_api_key": {"name": "Broker API Key"},
    "broker_api_secret": {"name": "Broker API Secret"}
  }
}
EOF
  ok "Translation file written"

  # Copy server.py and index.html from the zip if available
  if [ -n "${SOURCE_DIR:-}" ] && [ -f "${SOURCE_DIR}/apex_trading/rootfs/opt/apex/server.py" ]; then
    cp "${SOURCE_DIR}/apex_trading/rootfs/opt/apex/server.py" "${ADDON_DIR}/rootfs/opt/apex/server.py"
    cp "${SOURCE_DIR}/apex_trading/rootfs/opt/apex/static/index.html" "${ADDON_DIR}/rootfs/opt/apex/static/index.html"
    ok "server.py and index.html copied from archive"
  else
    warn "server.py not found in archive — you must copy it manually to ${ADDON_DIR}/rootfs/opt/apex/server.py"
    warn "index.html not found — copy to ${ADDON_DIR}/rootfs/opt/apex/static/index.html"
  fi
}

# ── INSTALL ─────────────────────────────────────────────────────────────
install_addon() {
  step "Installing Add-on Files"

  mkdir -p "${HA_ADDONS_DIR}"
  mkdir -p "${ADDON_DIR}"

  if [ -n "${SOURCE_DIR:-}" ] && [ -d "${SOURCE_DIR}/apex_trading" ]; then
    # Copy from downloaded/local source
    cp -r "${SOURCE_DIR}/apex_trading/." "${ADDON_DIR}/"
    ok "Files copied from source: ${SOURCE_DIR}/apex_trading"
  else
    # Write embedded files
    write_embedded_files
  fi

  # Ensure all scripts are executable
  find "${ADDON_DIR}/rootfs/etc/s6-overlay" -name "run" -o -name "finish" | xargs chmod +x 2>/dev/null || true
  if [ -f "${ADDON_DIR}/rootfs/opt/apex/server.py" ]; then
    chmod +x "${ADDON_DIR}/rootfs/opt/apex/server.py"
  fi

  ok "Add-on files installed to: ${ADDON_DIR}"
}

# ── CREATE DATA DIR ─────────────────────────────────────────────────────
setup_data_dir() {
  step "Setting Up Data Directory"
  mkdir -p "${ADDON_DATA_DIR}/apex"

  # Initialize empty JSON files if they don't exist
  for f in trades.json optlog.json analyses.json; do
    target="${ADDON_DATA_DIR}/apex/${f}"
    if [ ! -f "${target}" ]; then
      echo "[]" > "${target}"
      ok "Created: ${target}"
    else
      ok "Exists (preserved): ${target}"
    fi
  done

  # Initialize state.json with defaults if it doesn't exist
  if [ ! -f "${ADDON_DATA_DIR}/apex/state.json" ]; then
    cat > "${ADDON_DATA_DIR}/apex/state.json" << 'EOF'
{
  "botActive": false,
  "portfolio": 0,
  "startPortfolio": 0,
  "winCount": 0,
  "lossCount": 0,
  "todayPnl": 0,
  "tradesToday": 0,
  "positions": [],
  "taxYear": {"stGains": 0, "ltGains": 0, "losses": 0}
}
EOF
    ok "Created: state.json"
  else
    ok "Exists (preserved): state.json"
  fi
}

# ── HASSIO API ──────────────────────────────────────────────────────────
reload_addons() {
  step "Registering Add-on with Home Assistant"

  # Try hassio CLI first
  if command -v hassio &>/dev/null; then
    info "Using hassio CLI to reload add-ons..."
    if hassio addons reload 2>/dev/null; then
      ok "Add-ons reloaded via hassio CLI"
      return 0
    fi
  fi

  # Try ha CLI (newer HAOS)
  if command -v ha &>/dev/null; then
    info "Using ha CLI to reload add-ons..."
    if ha addons reload 2>/dev/null; then
      ok "Add-ons reloaded via ha CLI"
      return 0
    fi
  fi

  # Try supervisord API directly
  if [ -S /var/run/supervisor.sock ] || [ -f /run/supervisor.sock ]; then
    info "Trying Supervisor API..."
    SOCK="/var/run/supervisor.sock"
    [ -f /run/supervisor.sock ] && SOCK="/run/supervisor.sock"
    if curl -sf --unix-socket "${SOCK}" "http://localhost/addons/reload" -X POST &>/dev/null; then
      ok "Add-ons reloaded via Supervisor API"
      return 0
    fi
  fi

  warn "Could not auto-reload add-ons. You'll need to manually trigger a reload."
  info "In HA UI: Settings → Add-ons → ⋮ → Check for updates, OR restart HA."
}

# ── OPTIONAL: ADD REPOSITORY ────────────────────────────────────────────
add_repository() {
  step "Adding Repository to Home Assistant"
  if command -v ha &>/dev/null; then
    info "Adding repository via ha CLI..."
    ha supervisor options --channel stable 2>/dev/null || true
  fi
  # The local addon doesn't need a repo entry, but inform user
  info "Local add-ons (in /addons/) don't require repository configuration."
  ok "Your add-on will appear as a 'Local add-on' in the store."
}

# ── POST-INSTALL INSTRUCTIONS ───────────────────────────────────────────
show_completion() {
  echo ""
  echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${RESET}"
  echo -e "${GREEN}║         APEX Trading AI — Installation Complete!          ║${RESET}"
  echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${RESET}"
  echo ""
  echo -e "${BOLD}Next Steps:${RESET}"
  echo ""
  echo -e "  ${CYAN}1.${RESET} ${BOLD}Open Home Assistant${RESET}"
  echo -e "     Go to: Settings → Add-ons → Add-on Store"
  echo ""
  echo -e "  ${CYAN}2.${RESET} ${BOLD}Find APEX Trading AI${RESET}"
  echo -e "     Look under 'Local add-ons' or refresh the page"
  echo -e "     If not visible: click ⋮ → Check for updates → Refresh"
  echo ""
  echo -e "  ${CYAN}3.${RESET} ${BOLD}Install & Configure${RESET}"
  echo -e "     Click APEX Trading AI → Install"
  echo -e "     Go to the Configuration tab and set:"
  echo -e "       • ${YELLOW}anthropic_api_key${RESET}: your sk-ant-... key"
  echo -e "       • ${YELLOW}market_data_source${RESET}: yahoo (free, no key needed)"
  echo -e "       • ${YELLOW}trading_mode${RESET}: paper (safe to start)"
  echo ""
  echo -e "  ${CYAN}4.${RESET} ${BOLD}Start the Add-on${RESET}"
  echo -e "     Click Start → Open Web UI"
  echo -e "     Or navigate to: ${CYAN}http://$(hostname -I | awk '{print $1}' 2>/dev/null || echo 'your-ha-ip'):7123${RESET}"
  echo ""
  echo -e "  ${CYAN}5.${RESET} ${BOLD}Set Your Capital${RESET}"
  echo -e "     In the sidebar Capital panel → enter your balance → Set"
  echo ""
  echo -e "${BOLD}File Locations:${RESET}"
  echo -e "  Add-on:  ${ADDON_DIR}"
  echo -e "  Data:    ${ADDON_DATA_DIR}/apex/"
  echo ""
  echo -e "${BOLD}Uninstall:${RESET}"
  echo -e "  rm -rf ${ADDON_DIR}"
  echo -e "  (data is preserved in ${ADDON_DATA_DIR}/)"
  echo ""
  echo -e "${YELLOW}Need help?${RESET} See DOCS.md or open an issue on GitHub."
  echo ""
}

# ── MAIN ────────────────────────────────────────────────────────────────
main() {
  banner
  echo -e "${BOLD}APEX Trading AI Self-Installer${RESET}"
  echo -e "This script installs APEX as a local Home Assistant add-on."
  echo ""

  # Check we're running as root (required on HAOS)
  if [ "$(id -u)" -ne 0 ]; then
    warn "Not running as root. Some operations may fail."
    warn "On HAOS Terminal: you are automatically root. Via SSH: use sudo."
  fi

  detect_environment
  check_existing
  download_addon
  install_addon
  setup_data_dir
  add_repository
  reload_addons
  show_completion
}

main "$@"
