#!/bin/zsh
set -euo pipefail

# ============================================================
# Strategy_QC_Preflight.command (SOLO Caso B)
# Scopo: QC regole strategia contro sheet KPI_COLUMNS (nessun KPI CSV)
# ============================================================

# Hard-disable tracing ereditato (evita output indesiderato)
set +x 2>/dev/null || true
unsetopt xtrace 2>/dev/null || true
unsetopt verbose 2>/dev/null || true

# I/O sempre su TTY (robusto contro stdin rotto)
exec 3</dev/tty 4>/dev/tty

# Salva stato TTY e ripristina sempre all'uscita
_TTY_OLD_STATE="$(stty -g <&3 2>/dev/null || true)"
TRAPEXIT() {
  if [[ -n "${_TTY_OLD_STATE:-}" ]]; then
    stty "$_TTY_OLD_STATE" <&3 2>/dev/null || true
  else
    stty sane <&3 2>/dev/null || true
  fi
}
stty sane <&3 2>/dev/null || true

# Ctrl+C: torna al chiamante (menu pipeline) senza chiudere terminale
TRAPINT() {
  echo "" >&4
  echo "Interrotto. Ritorno al menu..." >&4
  stty sane <&3 2>/dev/null || true
  sleep 1
  # In oneshot: esci subito, così torni alla Pipeline
  if [[ "${PREFLIGHT_ONESHOT:-0}" == "1" ]]; then
    exit 0
  fi
  return 0
}

# --- trova root suite ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${PY_SUITE_ROOT:-}" ]]; then
  PY_SUITE_ROOT="$PY_SUITE_ROOT"
else
  PY_SUITE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi
export PY_SUITE_ROOT

# --- percorsi standard suite ---
STRATEGY_DIR="$PY_SUITE_ROOT/_data/config_strategia"
QC_SCRIPT="$PY_SUITE_ROOT/3. Run_strategia/strategy_qc.py"

# --- python: preferisci venv in root, altrimenti python3 ---
PY_BIN="$PY_SUITE_ROOT/.venv/bin/python3"
if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="$(command -v python3 || true)"
fi

# --- sanity checks ---
if [[ -z "${PY_BIN:-}" ]]; then
  echo "❌ python3 non trovato." >&4
  exit 0
fi
if [[ ! -f "$QC_SCRIPT" ]]; then
  echo "❌ Script QC non trovato: $QC_SCRIPT" >&4
  echo "   Verifica che esista: 3. Run_strategia/strategy_qc.py" >&4
  exit 0
fi

# ============================================================
# Helpers
# ============================================================
list_xlsx_files() {
  [[ -d "$STRATEGY_DIR" ]] || return 0
  ls -1 "$STRATEGY_DIR"/*.xlsx 2>/dev/null || true
}

pause() {
  echo "" >&4
  printf "Premi INVIO per tornare al menu..." >&4
  IFS= read -r _ <&3 || true
}

select_file_menu_into() {
  local __outvar="$1"
  local title="$2"
  local dir_label="$3"
  shift 3

  if [[ ! "$__outvar" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "❌ Nome variabile output non valido: $__outvar" >&4
    return 2
  fi

  setopt localoptions
  unsetopt nounset

  local -a files
  files=( "$@" )

  typeset -g "$__outvar"
  eval "$__outvar=''"

  if (( ${#files[@]} == 0 )); then
    return 0
  fi

  echo "" >&4
  echo "$title" >&4
  echo "Directory: $dir_label" >&4
  echo "" >&4

  local i=1
  local f
  for f in "${files[@]}"; do
    echo "  $i) $(basename "$f")" >&4
    i=$((i+1))
  done
  echo "" >&4

  # Rinforzo: TTY sano (evita read vuote “a raffica”)
  stty sane <&3 2>/dev/null || true

  local empty_hits=0
  while true; do
    printf "Seleziona numero (0=Esci): " >&4

    local choice
    if ! IFS= read -r choice <&3; then
      echo "" >&4
      echo "⚠️ Input non disponibile (EOF). Ritorno al menu..." >&4
      return 1
    fi

    # 0 = esci dalla selezione
    if [[ "${choice:-}" == "0" ]]; then
      return 1
    fi

    # Vuoto ripetuto: evita spam infinito
    if [[ -z "${choice:-}" ]]; then
      empty_hits=$((empty_hits + 1))
      if (( empty_hits >= 3 )); then
        echo "" >&4
        echo "⚠️ Nessun input rilevato (TTY non pronto). Ritorno al menu..." >&4
        return 1
      fi
      sleep 0.15
      continue
    fi
    empty_hits=0

    if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice>=1 && choice<=${#files[@]} )); then
      local selected="${files[$choice]}"   # zsh: array 1-based
      eval "$__outvar=\$selected"
      return 0
    fi

    echo "Scelta non valida. Inserisci un numero 1-${#files[@]} (oppure 0 per uscire)." >&4
  done
}

# ============================================================
# UI
# - In ONESHOT: una sola esecuzione e poi exit 0 (ritorna alla Pipeline)
# - Standalone: loop e ripeti
# ============================================================
while true; do
  clear
  echo "======================================"
  echo " Strategy QC Preflight (solo regole)"
  echo " ROOT: $PY_SUITE_ROOT"
  echo " Python: $PY_BIN"
  echo " Strategy dir: $STRATEGY_DIR"
  echo "======================================"
  echo ""

  # ============================================================
  # Select strategy xlsx
  # ============================================================
  typeset -a STRAT_FILES
  STRAT_FILES=( ${(f)"$(list_xlsx_files)"} )

  if (( ${#STRAT_FILES[@]} == 0 )); then
    echo "❌ Nessun file .xlsx trovato in: $STRATEGY_DIR"
    pause
    if [[ "${PREFLIGHT_ONESHOT:-0}" == "1" ]]; then
      clear
      exit 0
    fi
    continue
  fi

  if ! select_file_menu_into STRAT_PATH \
      "File strategia disponibili (.xlsx):" \
      "$STRATEGY_DIR" \
      "${STRAT_FILES[@]}"; then
    echo ""
    echo "Operazione annullata."
    pause
    if [[ "${PREFLIGHT_ONESHOT:-0}" == "1" ]]; then
      clear
      exit 0
    fi
    continue
  fi

  if [[ -z "${STRAT_PATH:-}" ]]; then
    echo "❌ Nessuna strategia selezionata."
    pause
    if [[ "${PREFLIGHT_ONESHOT:-0}" == "1" ]]; then
      clear
      exit 0
    fi
    continue
  fi

  echo ""
  echo "--------------------------------------"
  echo " Strategia selezionata: $(basename "$STRAT_PATH")"
  echo " QC: sheet KPI_COLUMNS"
  echo "--------------------------------------"
  echo ""

  # ============================================================
  # Run QC (solo KPI_COLUMNS sheet)
  # ============================================================
  rc=0
  set +e
  "$PY_BIN" "$QC_SCRIPT" \
    --strategy-xlsx "$STRAT_PATH" \
    --use-kpi-columns-sheet
  rc=$?
  set -e

  echo ""
  if [[ $rc -eq 0 ]]; then
    echo "✅ QC completata (nessun ERROR)."
  else
    echo "⚠️ QC completata con ERROR (exit code=$rc)."
  fi

  pause

  # Se lanciato dalla Pipeline: esci e torna al menu Pipeline
  if [[ "${PREFLIGHT_ONESHOT:-0}" == "1" ]]; then
    clear
    exit 0
  fi
done


