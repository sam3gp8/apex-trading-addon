#!/bin/sh
# APEX Trading AI — startup
# options.json at /data/options.json is the ONLY persistence store
# Runtime data (trades, analyses, state) stored in /data/

set -e

# Runtime data goes to /config/apex_trading/ — visible in HA file editor
# /config maps to /homeassistant/ on the host (config:rw mount)
mkdir -p /config/apex_trading
mkdir -p /data

# Initialize runtime data files in /config/apex_trading/ (never overwrite existing)
[ -f /config/apex_trading/trades.json         ] || echo "[]" > /config/apex_trading/trades.json
[ -f /config/apex_trading/optlog.json         ] || echo "[]" > /config/apex_trading/optlog.json
[ -f /config/apex_trading/analyses.json       ] || echo "[]" > /config/apex_trading/analyses.json
[ -f /config/apex_trading/state.json          ] || echo "{}" > /config/apex_trading/state.json
[ -f /config/apex_trading/trading_memory.json ] || echo "{}" > /config/apex_trading/trading_memory.json
[ -f /config/apex_trading/apex_user.json      ] || echo "{}" > /config/apex_trading/apex_user.json

# Confirm
echo "[APEX] Data directory: /config/apex_trading/ (visible at /homeassistant/apex_trading/ on host)"
echo "[APEX] Data files:"
ls -la /config/apex_trading/*.json 2>/dev/null | awk '{print "[APEX]  " $NF " (" $5 " bytes)"}'
echo "[APEX] Writable: $(touch /config/apex_trading/.write_test && rm /config/apex_trading/.write_test && echo YES || echo NO)"

echo "[APEX] Runtime data: /data/"
echo "[APEX] options.json: $([ -f /data/options.json ] && echo FOUND || echo MISSING)"
echo "[APEX] Supervisor token: $([ -n "$SUPERVISOR_TOKEN" ] && echo SET || echo MISSING)"

exec python3 /opt/apex/server.py
