#!/bin/bash
set -euo pipefail

echo "======================================"
echo " Avvio Run_strategia"
echo "======================================"

# Vai alla root suite (robusto anche con spazi)
cd "$(cd "$(dirname "$0")/.." && pwd)"
export PY_SUITE_ROOT="$(pwd)"

# Defaults se non arrivano dalla pipeline
: "${PY_SUITE_DATA_DIR:="$PY_SUITE_ROOT/_data/Test Data"}"
: "${PY_SUITE_OUT_DIR:="$PY_SUITE_DATA_DIR"}"
: "${PY_SUITE_STRATEGY_DIR:="$PY_SUITE_ROOT/_data/config_strategia"}"

echo "[ENV] PY_SUITE_ROOT=$PY_SUITE_ROOT"
echo "[ENV] PY_SUITE_DATA_DIR=$PY_SUITE_DATA_DIR"
echo "[ENV] PY_SUITE_OUT_DIR=$PY_SUITE_OUT_DIR"
echo "[ENV] PY_SUITE_STRATEGY_DIR=$PY_SUITE_STRATEGY_DIR"
echo "[ENV] PIPELINE_MODE=${PIPELINE_MODE:-0}"

# Python: preferisci venv locale se presente
VENV_PY="$PY_SUITE_ROOT/3. Run_strategia/.venv/bin/python3"
if [ -x "$VENV_PY" ]; then
  PY="$VENV_PY"
else
  PY="/usr/bin/python3"
fi

echo "Python in uso: $("$PY" --version)"
echo ""

# Lancia lo script (quote IMPORTANTI per spazi nel path)
"$PY" -u "$PY_SUITE_ROOT/3. Run_strategia/run_strategia.py"
