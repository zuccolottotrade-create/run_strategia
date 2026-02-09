#!/usr/bin/env python3
from __future__ import annotations

"""
run_strategia.py - Apply a strategy (Excel) to a KPI CSV and generate SIGNAL/HOLD/VALUE.

Key goals:
- Single, deterministic KPI loader with robust EU numeric parsing (comma decimals)
- Single normalization path for supertrend_dir / supertrend_dir_txt
- Keep pipeline env overrides + optional strategy QC
- Keep engine integration (load_engine.load_config_strategy / generate_signals)
- IMPORTANT: QC/preflight single source of truth (same behavior as stand-alone preflight)
"""

# ============================================================
# BOOTSTRAP – must stay BEFORE importing suite modules
# ============================================================
import sys
from pathlib import Path

SUITE_ROOT = Path(__file__).resolve().parents[1]  # .../Py_SUITE_TRADING
if str(SUITE_ROOT) not in sys.path:
    sys.path.insert(0, str(SUITE_ROOT))

# ============================================================
# IMPORT STANDARD
# ============================================================
import math
import os
import shutil
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Set, Tuple

import pandas as pd
import numpy as np

# ============================================================
# IMPORT SUITE
# ============================================================
from shared.paths import DATA_DIR

# ============================================================
# STRATEGY QC (single source of truth: use strategy_qc.py)
# ============================================================
try:
    from strategy_qc import qc_strategy_preflight, _print_qc_summary  # type: ignore
except Exception as _qc_err:  # pragma: no cover
    qc_strategy_preflight = None
    _print_qc_summary = None
    _QC_IMPORT_ERR = _qc_err
else:
    _QC_IMPORT_ERR = None

# ============================================================
# ENV HELPERS (defaults decided by pipeline)
# ============================================================
def _env_path(name: str) -> Optional[Path]:
    v = os.environ.get(name, "").strip()
    return Path(v) if v else None


PIPELINE_MODE = os.environ.get("PIPELINE_MODE", "").strip() == "1"
PREFLIGHT_ONESHOT = os.environ.get("PREFLIGHT_ONESHOT", "").strip() == "1"




ENV_DATA_DIR = _env_path("PY_SUITE_DATA_DIR")
ENV_OUT_DIR = _env_path("PY_SUITE_OUT_DIR")

ENV_STRATEGY_FILE = _env_path("PY_SUITE_STRATEGY_FILE")
ENV_STRATEGY_DIR = _env_path("PY_SUITE_STRATEGY_DIR")
ENV_KPI_FILE = _env_path("PY_SUITE_KPI_INPUT_CSV")

# ============================================================
# CONFIG
# ============================================================
PY_SUITE_ROOT = Path(os.environ.get("PY_SUITE_ROOT", str(SUITE_ROOT))).resolve()

TEST_DATA_DIR = (DATA_DIR / "Test Data").resolve()
DEFAULT_STRATEGY_DIR = PY_SUITE_ROOT / "_data" / "config_strategia"
STRATEGY_DIR = (ENV_STRATEGY_DIR if ENV_STRATEGY_DIR else DEFAULT_STRATEGY_DIR).resolve()
STRATEGY_DIR.mkdir(parents=True, exist_ok=True)

CSV_SEP = ";"

ALLOWED_SIGNALS = {"LONG", "SHORT", "NEUTRAL"}
HOLD_IN = "IN"
HOLD_OUT = "OUT"

# ============================================================
# IMPORT ENGINE
# ============================================================
try:
    from load_engine import load_config_strategy, generate_signals, Condition  # type: ignore
except Exception as e:  # pragma: no cover
    load_config_strategy = None
    generate_signals = None
    Condition = None
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


# ============================================================
# SMALL UTILS
# ============================================================
def graceful_exit(msg: str, code: int = 0) -> None:
    print(msg)
    raise SystemExit(code)


def ensure_engine_available() -> None:
    if _IMPORT_ERR is not None or load_config_strategy is None or generate_signals is None:
        print("ERRORE: impossibile importare load_engine.")
        print(f"Dettaglio: {_IMPORT_ERR!r}")
        raise SystemExit(2)


def list_files(directory: Path, exts: Optional[Sequence[str]] = None) -> List[Path]:
    if not directory.exists():
        graceful_exit(f"Directory inesistente: {directory}", 2)

    files = [p for p in directory.iterdir() if p.is_file()]
    files = [p for p in files if not p.name.startswith(("~$", "."))]
    files = [p for p in files if not p.name.lower().endswith(".bak")]

    if exts:
        extset = {e.lower().lstrip(".") for e in exts}
        files = [p for p in files if p.suffix.lower().lstrip(".") in extset]

    return sorted(files, key=lambda p: p.name.lower())


def list_kpi_files_only(directory: Path) -> List[Path]:
    return [p for p in list_files(directory, ["csv"]) if p.name.startswith("KPI_")]


def select_from_menu(prompt: str, options: Sequence[Path]) -> Path:
    if not options:
        graceful_exit("Nessun file disponibile.", 2)

    while True:
        print("\n" + prompt)
        for i, p in enumerate(options, 1):
            print(f"  {i}. {p.name}")

        c = input("Seleziona numero: ").strip()
        if c.isdigit():
            idx = int(c)
            if 1 <= idx <= len(options):
                return options[idx - 1]

        print("Scelta non valida.")


# ============================================================
# NORMALIZATION HELPERS
# ============================================================
def _norm_str_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.strip()
        .str.upper()
        .replace({"NAN": pd.NA, "NONE": pd.NA, "": pd.NA})
    )


def normalize_signal_hold(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "SIGNAL" in out.columns:
        out["SIGNAL"] = _norm_str_series(out["SIGNAL"])
    if "HOLD" in out.columns:
        out["HOLD"] = _norm_str_series(out["HOLD"])
    return out


# ============================================================
# TRADE ENRICHMENT (Profit/Trade, Trade_ID, Timing, Frequency)
# ============================================================
def _detect_datetime_series(df: pd.DataFrame) -> pd.Series | None:
    """
    Tries to build a datetime Series from common column conventions.
    Supported:
      - 'datetime' / 'DateTime' / 'timestamp'
      - 'date' + 'time'
    Returns pd.Series[datetime64[ns]] or None.
    """
    cols = {c.lower(): c for c in df.columns}

    # single column datetime
    for key in ("datetime", "date_time", "timestamp"):
        if key in cols:
            s = pd.to_datetime(df[cols[key]], errors="coerce", utc=False)
            if s.notna().any():
                return s

    # date + time
    if "date" in cols and "time" in cols:
        s = pd.to_datetime(
            df[cols["date"]].astype(str).str.strip() + " " + df[cols["time"]].astype(str).str.strip(),
            errors="coerce",
            utc=False,
        )
        if s.notna().any():
            return s

    return None


def add_trade_enrichment(
    df: pd.DataFrame,
    trading_day_minutes: float = 480.0,   # 8h
    trading_days_per_week: float = 5.0,
) -> tuple[pd.DataFrame, dict]:
    """
    Adds trade-based columns without altering existing interactivity.
    Trade pairing logic:
      - ENTRY when HOLD == 'IN' and VALUE is present
      - EXIT  when HOLD == 'OUT' and VALUE is present and an ENTRY exists
    Output columns:
      - Profit/Trade (on EXIT row only)
      - Trade_ID (on EXIT row only)
      - Minutes_IN_to_OUT (on EXIT row only, if datetime available)
      - Minutes_OUT_to_next_IN (on ENTRY row only, if datetime available)
      - Sum Profit/Trade, Avg Profit/Trade, Win Rate
      - Max DD Start, Max DD Peak
      - Trades/Day_8h_AvgIdle, Trades/Day_8h_MedIdle, Trades/Week_8h_AvgIdle, Trades/Week_8h_MedIdle (constant)
    Returns: (df_enriched, summary_dict)
    """
    out = df.copy()

    # Ensure required columns exist
    if "HOLD" not in out.columns or "VALUE" not in out.columns:
        # nothing to do
        return out, {}

    hold = out["HOLD"].astype("object")
    value = out["VALUE"]

    dt = _detect_datetime_series(out)

    # Pre-allocate columns
    profit = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")
    trade_id = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")

    dur_in_out = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")
    dur_out_next_in = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")

    sum_profit = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")
    avg_profit = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")
    win_rate = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")

    max_dd_start = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")
    max_dd_peak = pd.Series([pd.NA] * len(out), index=out.index, dtype="Float64")

    # State
    entry_value = None
    entry_time = None
    last_exit_time = None

    equity = 0.0
    equity_start = 0.0
    peak_equity = 0.0

    n_trades = 0
    n_wins = 0
    mdd_start = 0.0
    mdd_peak = 0.0

    trade_counter = 0

    prev_h = ""

    # Iterate rows in order
    for i in out.index:
        h = str(hold.loc[i]).upper() if not pd.isna(hold.loc[i]) else ""
        v = value.loc[i]

        v_num = None
        if not pd.isna(v):
            try:
                v_num = float(v)
            except Exception:
                v_num = None

        t = None
        if dt is not None:
            t = dt.loc[i]
            if pd.isna(t):
                t = None

        # ============================================================
        # ENTRY event (solo transizione OUT -> IN)
        # ============================================================
        if h == "IN" and v_num is not None and prev_h != "IN":
            entry_value = v_num
            entry_time = t

            # OUT -> next IN (idle)
            if last_exit_time is not None and t is not None:
                dur_out_next_in.loc[i] = (
                    t - last_exit_time
                ).total_seconds() / 60.0

        # ============================================================
        # EXIT event
        # ============================================================
        if h == "OUT" and v_num is not None and entry_value is not None:
            p = v_num - entry_value
            equity += p

            trade_counter += 1
            trade_id.loc[i] = trade_counter
            profit.loc[i] = p

            n_trades += 1
            if p > 0:
                n_wins += 1

            # IN -> OUT duration
            if entry_time is not None and t is not None:
                dur_in_out.loc[i] = (
                    t - entry_time
                ).total_seconds() / 60.0

            # DD from start
            dd_s = max(0.0, equity_start - equity)
            mdd_start = max(mdd_start, dd_s)

            # DD from peak
            peak_equity = max(peak_equity, equity)
            dd_p = peak_equity - equity
            mdd_peak = max(mdd_peak, dd_p)

            last_exit_time = t

            # reset
            entry_value = None
            entry_time = None

        # ============================================================
        # aggiornamento stato precedente (SEMPRE, fine giro)
        # ============================================================
        prev_h = h

        if h == "OUT" and v_num is not None and entry_value is not None:
            p = v_num - entry_value
            equity += p

            trade_counter += 1
            trade_id.loc[i] = trade_counter
            profit.loc[i] = p

            n_trades += 1
            if p > 0:
                n_wins += 1

            # IN -> OUT duration
            if entry_time is not None and t is not None:
                dur_in_out.loc[i] = (t - entry_time).total_seconds() / 60.0

            # DD from start
            dd_s = max(0.0, equity_start - equity)
            mdd_start = max(mdd_start, dd_s)

            # DD from peak
            peak_equity = max(peak_equity, equity)
            dd_p = peak_equity - equity
            mdd_peak = max(mdd_peak, dd_p)

            last_exit_time = t


            # reset
            entry_value = None
            entry_time = None



        # Running metrics every row (so the report can read last row)
        sum_profit.loc[i] = equity
        if n_trades > 0:
            avg_profit.loc[i] = equity / n_trades
            win_rate.loc[i] = n_wins / n_trades
        max_dd_start.loc[i] = mdd_start
        max_dd_peak.loc[i] = mdd_peak

    # Attach columns
    out["Profit/Trade"] = profit
    out["Trade_ID"] = trade_id
    out["Minutes_IN_to_OUT"] = dur_in_out
    out["Minutes_OUT_to_next_IN"] = dur_out_next_in

    out["Sum Profit/Trade"] = sum_profit
    out["Avg Profit/Trade"] = avg_profit
    out["Win Rate"] = win_rate
    out["Max DD Start"] = max_dd_start
    out["Max DD Peak"] = max_dd_peak

    # Frequency (8h/day) using idle minutes
    idle_vals = dur_out_next_in.dropna().astype(float)
    avg_idle = float(idle_vals.mean()) if len(idle_vals) else None
    med_idle = float(idle_vals.median()) if len(idle_vals) else None

    def safe_div(a, b):
        if b is None or b == 0:
            return None
        return a / b

    trades_day_avg = safe_div(trading_day_minutes, avg_idle)
    trades_day_med = safe_div(trading_day_minutes, med_idle)
    trades_week_avg = (trades_day_avg * trading_days_per_week) if trades_day_avg is not None else None
    trades_week_med = (trades_day_med * trading_days_per_week) if trades_day_med is not None else None

    # Constant columns (same value on all rows)
    out["Trades/Day_8h_AvgIdle"] = trades_day_avg
    out["Trades/Day_8h_MedIdle"] = trades_day_med
    out["Trades/Week_8h_AvgIdle"] = trades_week_avg
    out["Trades/Week_8h_MedIdle"] = trades_week_med

    # Summary dict for reporting / console
    trade_dur_vals = dur_in_out.dropna().astype(float)
    summary = {
        "trades_closed": int(trade_counter),
        "avg_trade_minutes": float(trade_dur_vals.mean()) if len(trade_dur_vals) else None,
        "med_trade_minutes": float(trade_dur_vals.median()) if len(trade_dur_vals) else None,
        "avg_idle_minutes": avg_idle,
        "med_idle_minutes": med_idle,
        "trades_day_8h_avg_idle": trades_day_avg,
        "trades_day_8h_med_idle": trades_day_med,
        "trades_week_8h_avg_idle": trades_week_avg,
        "trades_week_8h_med_idle": trades_week_med,
    }

    return out, summary


def coerce_numeric_eu_inplace(df: pd.DataFrame, cols: List[str]) -> None:
    """
    Convert EU formatted numeric strings to floats.
    Handles both:
      - 28,385 -> 28.385
      - 1.234,56 -> 1234.56
    """
    for c in cols:
        if c not in df.columns:
            continue

        s = df[c]
        if pd.api.types.is_numeric_dtype(s):
            continue

        s2 = (
            s.astype(str)
            .str.strip()
            .replace({"": None, "nan": None, "None": None, "NAN": None, "NONE": None})
        )
        # thousands "." and decimal ","
        s2 = s2.str.replace(".", "", regex=False)
        s2 = s2.str.replace(",", ".", regex=False)

        df[c] = pd.to_numeric(s2, errors="coerce")


def normalize_supertrend_direction(df: pd.DataFrame) -> None:
    # normalizza eventuale txt esistente
    if "supertrend_dir_txt" in df.columns:
        df["supertrend_dir_txt"] = _norm_str_series(df["supertrend_dir_txt"])

    # garantisci colonna target
    if "supertrend_dir" not in df.columns:
        df["supertrend_dir"] = pd.Series([pd.NA] * len(df), dtype="Int64")

    # ------------------------------------------------------------
    # 1) Prova a popolare da supertrend_dir (se già presente e sensata)
    # 2) Altrimenti: usa KPI_SUPERTREND_DIR_* (es. KPI_SUPERTREND_DIR_10_3p0)
    # ------------------------------------------------------------
    candidates = [("supertrend_dir", df["supertrend_dir"])]
    kpi_dir_cols = sorted([c for c in df.columns if c.startswith("KPI_SUPERTREND_DIR_")])
    if kpi_dir_cols:
        candidates.append((kpi_dir_cols[0], df[kpi_dir_cols[0]]))

    filled = False
    for src_name, src in candidates:
        s_norm = _norm_str_series(src)
        s_num = pd.to_numeric(s_norm.astype(str).str.replace(",", ".", regex=False), errors="coerce")

        # se è valorizzata abbastanza, la usiamo
        if s_num.notna().mean() >= 0.5:
            s_sign = np.sign(s_num)
            s_sign = s_sign.replace({0: pd.NA})
            df["supertrend_dir"] = s_sign.round().astype("Int64")
            filled = True
            break

    # fallback: mappa dal testo
    if (not filled) and "supertrend_dir_txt" in df.columns:
        m_txt = {"UP": 1, "DOWN": -1, "LONG": 1, "SHORT": -1}
        mapped = df["supertrend_dir_txt"].map(m_txt)
        if mapped.notna().mean() >= 0.5:
            df["supertrend_dir"] = mapped.astype("Int64")
            filled = True

    # fallback finale: deriva da close vs supertrend (solo dove mancante)
    if df["supertrend_dir"].isna().any() and {"close", "supertrend"}.issubset(df.columns):
        mask = df["supertrend_dir"].isna() & df["close"].notna() & df["supertrend"].notna()
        df.loc[mask, "supertrend_dir"] = (
            (df.loc[mask, "close"] > df.loc[mask, "supertrend"])
            .map({True: 1, False: -1})
            .astype("Int64")
        )

    df["supertrend_dir_txt"] = df["supertrend_dir"].map({1: "UP", -1: "DOWN"}).astype("object")



# ============================================================
# GROUPING
# ============================================================
def iter_groups(df: pd.DataFrame):
    has_symbol = "symbol" in df.columns
    has_isin = "isin" in df.columns

    if not has_symbol:
        yield ("__ALL__", "__ALL__"), df
        return

    sym = df["symbol"].astype("string").fillna("").str.strip()

    if not has_isin:
        yield from df.groupby(sym, sort=False)
        return

    isin = df["isin"].astype("string").fillna("").str.strip()

    if (isin == "").mean() > 0.95:
        for k, g in df.groupby(sym, sort=False):
            yield (k, ""), g
        return

    tmp = df.copy()
    tmp["__symbol_key__"] = sym
    tmp["__isin_key__"] = isin

    for (k1, k2), g in tmp.groupby(["__symbol_key__", "__isin_key__"], sort=False, dropna=False):
        g = g.drop(columns=["__symbol_key__", "__isin_key__"])
        yield (k1, k2), g


# ============================================================
# STRATEGY ADAPTER
# ============================================================
@dataclass(frozen=True)
class Strategy:
    path: Path
    conditions: List[Any]  # List[Condition]
    required_indicators: List[str]


def compute_required_indicators_from_conditions(conditions: List[Any]) -> List[str]:
    req: Set[str] = set()
    for c in conditions:
        if not getattr(c, "enabled", False):
            continue

        lhs = str(getattr(c, "lhs_col", "")).strip()
        if lhs:
            req.add(lhs)

        if str(getattr(c, "rhs_type", "")).upper() == "COLUMN":
            rhs_col = str(getattr(c, "rhs_col", "")).strip()
            if rhs_col:
                req.add(rhs_col)

    return sorted(req)


def load_strategy(path: str) -> Strategy:
    p = Path(path)
    conds = load_config_strategy(str(p), sheet_name="CONDITIONS")
    required = compute_required_indicators_from_conditions(conds)
    return Strategy(path=p, conditions=conds, required_indicators=required)


# ============================================================
# KPI LOADING (CLEAN, DETERMINISTIC)
# ============================================================
def load_kpi_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        graceful_exit(f"File KPI non trovato: {path}", 2)

    df = pd.read_csv(path, sep=CSV_SEP, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # ------------------------------------------------------------
    # AUTO-FIX separatore: alcuni KPI_* sono TSV (\t) e non ';'
    # Se abbiamo 1 sola colonna e contiene i TAB nel nome, rileggiamo come TSV.
    # ------------------------------------------------------------
    if "date" not in df.columns and len(df.columns) == 1 and "\t" in df.columns[0]:
        df = pd.read_csv(path, sep="\t", dtype=str)
        df.columns = [c.strip() for c in df.columns]

    print("[DBG] ncols=", len(df.columns), "cols_head=", df.columns[:10].tolist())


    for col in ("date", "time", "close"):
        if col not in df.columns:
            graceful_exit(f"Colonna mancante: {col}", 2)

    non_numeric = {
        "symbol",
        "isin",
        "date",
        "time",
        "datetime",
        "exchange",
        "currency",
        "supertrend_dir",
        "supertrend_dir_txt",
        "SIGNAL",
        "HOLD",
        "VALUE",
    }

    # --- Preserve REGIME_* textual columns (keep *_CODE and *_SWITCH numeric) ---
    regime_text_cols = [
        c for c in df.columns
        if c.startswith("REGIME_") and not (c.endswith("_CODE") or c.endswith("_SWITCH"))
    ]
    non_numeric = non_numeric | set(regime_text_cols)

    numeric_candidates = [c for c in df.columns if c not in non_numeric]
    coerce_numeric_eu_inplace(df, numeric_candidates)

    # --- QC: REGIME textual columns must not be wiped by coercion ---
    qc_cols = list(regime_text_cols)

    if qc_cols:
        wiped = [c for c in qc_cols if df[c].isna().mean() > 0.99]
        if wiped:
            raise ValueError(f"[QC] REGIME textual columns wiped by coercion: {wiped}")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if df["close"].isna().all():
        graceful_exit("Colonna 'close' non convertibile a numerico (verifica separatore/decimali).", 2)

    normalize_supertrend_direction(df)

    # ------------------------------------------------------------
    # COLUMN ORDERING: supertrend_dir* DEVONO stare prima del blocco REGIME_*
    # Sequenza richiesta:
    # supertrend_dir, supertrend_dir_txt, REGIME_L1, REGIME_L1_CODE, REGIME_L1_RAW, REGIME_L1_REASON, REGIME_L1_SWITCH
    # ------------------------------------------------------------
    st_cols = [c for c in ["supertrend_dir", "supertrend_dir_txt"] if c in df.columns]
    regime_cols = [c for c in df.columns if c.startswith("REGIME_")]

    if st_cols and regime_cols:
        first_regime = regime_cols[0]
        cols = list(df.columns)

        # rimuovi st_cols dall'ordine attuale
        for c in st_cols:
            if c in cols:
                cols.remove(c)

        # inserisci st_cols subito prima del primo REGIME_*
        idx = cols.index(first_regime)
        cols = cols[:idx] + st_cols + cols[idx:]
        df = df[cols]



    dt_raw = df["date"].astype(str).str.strip()
    sample = dt_raw.dropna().head(20).tolist()
    looks_iso = any(len(x) >= 10 and x[4:5] == "-" for x in sample)

    df["datetime"] = pd.to_datetime(
        df["date"].astype(str).str.strip() + " " + df["time"].astype(str).str.strip(),
        errors="coerce",
        dayfirst=(not looks_iso),
    )

    if df["datetime"].isna().any():
        graceful_exit("Datetime non valide nel CSV KPI.", 2)

    return df.reset_index(drop=True)


# ============================================================
# COERCE REQUIRED INDICATORS (ENGINE INPUT SANITY)
# ============================================================
def _safe_numeric_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return s
    raw = s.astype(str).str.strip()
    raw = raw.replace({"": None, "nan": None, "None": None, "NAN": None, "NONE": None})
    raw = raw.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    num = pd.to_numeric(raw, errors="coerce")
    return num


def coerce_required_indicators(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = _safe_numeric_series(out[c])
    return out


# ============================================================
# SIGNAL COMPUTATION (ENGINE WRAPPER)
# ============================================================
def debug_conditions_true_rate(df: pd.DataFrame, conditions: List[Any], max_rows: int = 5) -> None:
    """
    Stampa, per ogni condition enabled, quante righe risultano True.
    Nota: questa è diagnostica, non la logica ufficiale dell'engine.
    """
    print("\n[DBG] CONDITION TRUE-RATE")
    for i, c in enumerate(conditions, 1):
        if not getattr(c, "enabled", False):
            continue

        lhs = str(getattr(c, "lhs_col", "")).strip()
        # engine/Condition può usare 'operator' o 'op' a seconda dell'implementazione
        op = str(getattr(c, "operator", getattr(c, "op", ""))).strip().upper()
        rhs_type = str(getattr(c, "rhs_type", "")).strip().upper()
        rhs_col = str(getattr(c, "rhs_col", "")).strip()
        rhs_val = getattr(c, "rhs_value", None)

        if lhs not in df.columns:
            print(f"  {i}. MISSING lhs_col={lhs!r}")
            continue

        sL = df[lhs]

        if rhs_type == "COLUMN":
            if rhs_col not in df.columns:
                print(f"  {i}. MISSING rhs_col={rhs_col!r} (lhs={lhs!r} op={op})")
                continue
            sR = df[rhs_col]
        else:
            sR = rhs_val

        try:
            if op in ("==", "="):
                mask = (sL == sR)
            elif op == "!=":
                mask = (sL != sR)
            elif op == ">":
                mask = (sL > sR) if rhs_type == "COLUMN" else (sL > float(str(sR).replace(",", ".")))
            elif op == ">=":
                mask = (sL >= sR) if rhs_type == "COLUMN" else (sL >= float(str(sR).replace(",", ".")))
            elif op == "<":
                mask = (sL < sR) if rhs_type == "COLUMN" else (sL < float(str(sR).replace(",", ".")))
            elif op == "<=":
                mask = (sL <= sR) if rhs_type == "COLUMN" else (sL <= float(str(sR).replace(",", ".")))
            else:
                print(f"  {i}. UNSUPPORTED op={op!r} lhs={lhs!r} rhs_type={rhs_type!r} rhs={rhs_col or rhs_val!r}")
                continue

            true_n = int(mask.fillna(False).sum())
            tot = int(len(mask))
            pct = 100.0 * true_n / tot if tot else 0.0
            sample_idx = mask[mask.fillna(False)].head(max_rows).index.tolist()
            print(
                f"  {i}. lhs={lhs!r} op={op!r} rhs={rhs_col or rhs_val!r} -> TRUE {true_n}/{tot} ({pct:.2f}%) sample_idx={sample_idx}"
            )

        except Exception as e:
            print(f"  {i}. ERROR eval lhs={lhs!r} op={op!r} rhs={rhs_col or rhs_val!r}: {e!r}")

def compute_signal(
    df: pd.DataFrame,
    strategy: Strategy,
    debug: bool = False,
    *,
    use_regime_filter: bool = True,
) -> pd.Series:
    print("[DBG] compute_signal ENTER debug=", debug, "rows=", len(df))  # TEMP (sempre)

    # --- generate signals ---

    # --- REGIME_L1 gating ----------------------------------------------
    # NB: la policy esiste già; qui abilitiamo/disabilitiamo il gating per costruire anche la baseline NO_REGIME.

    allow_trade_mask = None
    allow_long_mask = None
    allow_short_mask = None

    if use_regime_filter:
        if "REGIME_L1" not in df.columns:
            raise ValueError(
                "REGIME_L1 assente nel CSV KPI. "
                "Rigenera il file con PyKPI_calcolo + plugin regime."
            )

        # Policy ALLINEATA agli output del filtro REGIME_L1 (no legacy)
        # Stati attesi: LATERAL, RANGE, TREND_UP, TREND_DOWN, VOLATILE
        # Permission puro: il regime NON impone direzione.
        # Blocca solo nuove entry in VOLATILE.
        policy = {
            "VOLATILE": {"allow_long": False, "allow_short": False},  # evitare entry
            "LATERAL": {"allow_long": True, "allow_short": True},
            "RANGE": {"allow_long": True, "allow_short": True},
            "TREND_UP": {"allow_long": True, "allow_short": True},
            "TREND_DOWN": {"allow_long": True, "allow_short": True},
        }

        reg = (
            df["REGIME_L1"]
            .astype(str)
            .fillna("")
            .str.strip()
            .str.upper()
        )

        def _p(r: str) -> dict:
            # fallback prudente: regime sconosciuto => OFF (no entry)
            return policy.get(r, {"allow_long": False, "allow_short": False})

        # Permission layer = blocco SOLO delle entry.
        # Evitiamo allow_trade_mask per non influenzare eventuali exit/close
        # se generate_signals lo interpretasse in modo "forte".
        allow_trade_mask = None
        allow_long_mask = reg.map(lambda r: bool(_p(r)["allow_long"]))
        allow_short_mask = reg.map(lambda r: bool(_p(r)["allow_short"]))

        # Robustezza: se la colonna è tutta vuota/sconosciuta, le maschere diventano False.
        # (comportamento voluto: meglio zero trade che trade senza regime quando ON)
    # -------------------------------------------------------------------

    out = generate_signals(
        df,
        strategy.conditions,
        reverse_immediate=True,
        exit_priority=True,
        allow_trade_mask=allow_trade_mask,
        allow_long_mask=allow_long_mask,
        allow_short_mask=allow_short_mask,
    )

    if "position" not in out.columns:
        raise ValueError("generate_signals non ha prodotto la colonna 'position'.")

    if debug:
        debug_conditions_true_rate(df, strategy.conditions)
        print("[DBG] engine position dtype:", out["position"].dtype)
        print("[DBG] engine position head:", out["position"].head(15).tolist())
        print(
            "[DBG] engine position vc:",
            out["position"].astype(str).value_counts(dropna=False).head(10).to_dict(),
        )

    pos = pd.to_numeric(out["position"], errors="coerce")
    pos_i = pos.fillna(0).astype(int)

    sig = (
        pos_i.map({1: "LONG", -1: "SHORT", 0: "NEUTRAL"})
        .fillna("NEUTRAL")
        .astype("object")
    )
    sig.index = df.index
    return sig

def _gate_entries_by_regime(
    df_g: pd.DataFrame,
    sig: pd.Series,
    *,
    regime_col: str = "REGIME_L1",
) -> pd.Series:
    """
    Applica gating REGIME_L1 SOLO sulle nuove ENTRY (OUT->IN).
    - In VOLATILE: blocca entry LONG/SHORT (qui SHORT è spesso "exit", quindi blocchiamo SOLO se OUT).
    - Le EXIT (IN->OUT) devono SEMPRE passare.
    """
    if regime_col not in df_g.columns:
        return sig  # nessun regime => non tocchiamo

    reg = (
        df_g[regime_col]
        .astype(str)
        .fillna("")
        .str.strip()
        .str.upper()
        .reindex(sig.index)
        .fillna("")
    )

    # Policy: VOLATILE = no nuove entry
    def allow_entry(r: str, side: str) -> bool:
        if r == "VOLATILE":
            return False
        # default: consenti
        return True

    out = sig.copy()
    hold = HOLD_OUT  # stato locale per applicare "entry-only"

    for i in out.index:
        s = str(out.at[i]).strip().upper()
        r = str(reg.at[i]).strip().upper()

        # Se già IN, i segnali LONG ripetuti non sono nuove entry -> ok lasciarli
        if s == "LONG":
            if hold != HOLD_IN:
                # nuova ENTRY: consenti solo se regime permette
                if allow_entry(r, "LONG"):
                    hold = HOLD_IN
                else:
                    out.at[i] = "NEUTRAL"
            else:
                # già IN, lasciamo LONG (o potresti neutralizzare)
                pass

        elif s == "SHORT":
            if hold == HOLD_IN:
                # EXIT: deve SEMPRE passare
                hold = HOLD_OUT
            else:
                # SHORT mentre OUT = segnale spurio / entry short (dipende dalla strategia)
                # qui lo trattiamo come "entry" e lo blocchiamo se regime non permette
                if allow_entry(r, "SHORT"):
                    # se la tua semantica usa SHORT come entry short, qui potresti mettere hold=IN
                    # ma nel tuo engine SHORT è usato come EXIT. Quindi, quando OUT, lo neutralizziamo.
                    out.at[i] = "NEUTRAL"
                else:
                    out.at[i] = "NEUTRAL"

        else:
            # NEUTRAL
            pass

    return out


def apply_signals(df: pd.DataFrame, strategy: Strategy, debug: bool = False) -> pd.DataFrame:
    out = df.copy()

    print("[DBG] apply_signals debug=", debug, "rows=", len(out))  # TEMP

    sig_all = pd.Series("NEUTRAL", index=out.index, dtype="object")
    sig_all_no = pd.Series("NEUTRAL", index=out.index, dtype="object")

    groups = list(iter_groups(out))
    print("[DBG] iter_groups count =", len(groups))  # TEMP
    for k, g in groups[:3]:
        print("[DBG] group sample:", k, "rows=", len(g))  # TEMP

    for key, g in groups:
        s = compute_signal(g.copy(), strategy, debug=debug, use_regime_filter=True)
        s_no = compute_signal(g.copy(), strategy, debug=debug, use_regime_filter=False)

        s = _norm_str_series(s).fillna("NEUTRAL")
        s = s.reindex(g.index).fillna("NEUTRAL")

        # --- REGIME gating "entry-only" (post-engine): blocca nuove entry in VOLATILE ---
        reg_col = "REGIME_L1_RAW" if "REGIME_L1_RAW" in g.columns else "REGIME_L1"
        s = _gate_entries_by_regime(g, s, regime_col=reg_col)

        s_no = _norm_str_series(s_no).fillna("NEUTRAL")
        s_no = s_no.reindex(g.index).fillna("NEUTRAL")

        sig_all.loc[g.index] = s
        sig_all_no.loc[g.index] = s_no

        if debug:
            print(f"[DBG] group={key} signal vc:", s.value_counts(dropna=False).head(5).to_dict())

    out["SIGNAL"] = sig_all
    out["SIGNAL_no_regime"] = sig_all_no
    return out


# ============================================================
# HOLD + VALUE
# ============================================================
def apply_hold_value(
    df: pd.DataFrame,
    *,
    signal_col: str = "SIGNAL",
    hold_col: str = "HOLD",
    value_col: str = "VALUE",
) -> Tuple[pd.DataFrame, int, int]:
    # ============================================================
    # INIT
    # ============================================================
    out = df.copy()
    out[hold_col] = HOLD_OUT
    out[value_col] = math.nan

    entry, exit_ = 0, 0

    # ============================================================
    # NO_REGIME: FIX VALUE_no_regime sulle entry (OUT->IN)
    # Motivo: in alcuni flussi VALUE_no_regime resta NaN anche se HOLD_no_regime è corretto.
    # ============================================================
    if (
            "HOLD_no_regime" in out.columns
            and "VALUE_no_regime" in out.columns
            and "close" in out.columns
    ):
        hold_nr = out["HOLD_no_regime"].astype(str).str.strip().str.upper()
        m_entry_nr = hold_nr.eq("IN") & ~hold_nr.shift(1).fillna("OUT").eq("IN")

        # Se VALUE_no_regime è NaN sulle entry, la valorizziamo con close della barra di entry
        m_fix = m_entry_nr & out["VALUE_no_regime"].isna()
        if m_fix.any():
            out.loc[m_fix, "VALUE_no_regime"] = out.loc[m_fix, "close"]

    for _, g in iter_groups(out):
        hold = HOLD_OUT
        for i in g.index:
            sig = str(out.at[i, signal_col]).strip().upper()
            close = out.at[i, "close"]

            # ENTRY: OUT -> IN
            if sig == "LONG" and hold != HOLD_IN:
                hold = HOLD_IN
                out.at[i, value_col] = close
                entry += 1

            # EXIT: IN -> OUT  (FIX)
            elif sig == "SHORT" and hold != HOLD_OUT:
                hold = HOLD_OUT
                out.at[i, value_col] = close
                exit_ += 1

            # HOLD state (sempre coerente)
            out.at[i, hold_col] = hold

    return out, entry, exit_



# ============================================================
# OUTPUT
# ============================================================
def build_output_path_in_dir(kpi: Path, out_dir: Path) -> Path:
    name = kpi.name
    if not name.startswith("SIGNAL_"):
        name = "SIGNAL_" + name
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / name


def write_output_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(
        path,
        sep=CSV_SEP,
        index=False,
        decimal=",",
        na_rep="",
        float_format="%.3f",
    )


def print_summary(df: pd.DataFrame, entry: int, exit_: int) -> None:
    vc = df["SIGNAL"].value_counts(dropna=False).to_dict() if "SIGNAL" in df.columns else {}
    print("\n===== RIEPILOGO =====")
    print(f"Righe: {len(df)}")
    print(f"LONG={vc.get('LONG', 0)} SHORT={vc.get('SHORT', 0)} NEUTRAL={vc.get('NEUTRAL', 0)}")
    print(f"IN={entry} OUT={exit_}")


# ============================================================
# FLOW
# ============================================================
@dataclass(frozen=True)
class Selection:
    kpi: Path
    strategy: Path


def _auto_pick_kpi() -> Optional[Path]:
    if ENV_KPI_FILE and ENV_KPI_FILE.exists():
        return ENV_KPI_FILE
    return None


def run_strategy_preflight_or_reject(strategy_path: Path, df_kpi: pd.DataFrame) -> bool:
    """
    Single source of truth del preflight:
    - usa qc_strategy_preflight (strategy_qc.py)
    - stampa SEMPRE la tabella (stessa identica dello stand-alone)
    - scrive QC_*.csv vicino alla strategia + copia in OUT_DIR/TEST_DATA_DIR
    - ritorna True se no ERROR, False se ERROR
      (in PIPELINE_MODE esce con code 2)
    """
    if qc_strategy_preflight is None or _print_qc_summary is None:
        print("⚠️ QC non disponibile (strategy_qc.py non importabile).")
        if _QC_IMPORT_ERR is not None:
            print(f"   Dettaglio import QC: {_QC_IMPORT_ERR!r}")
        if PIPELINE_MODE:
            graceful_exit("QC non disponibile in PIPELINE_MODE: impossibile garantire robustezza strategia.", 2)
        return True

    strategy_path = Path(strategy_path).resolve()

    qc_df, issues = qc_strategy_preflight(
        strategy_xlsx=strategy_path,
        kpi_columns=list(df_kpi.columns),
        sheet_name="CONDITIONS",
        strict_logic_support=False,
    )

    # ------------------------------------------------------------
    # STAMPA TABELLARE (OBBLIGATORIA) — stessa dello stand-alone
    # ------------------------------------------------------------
    n_err = _print_qc_summary(qc_df, issues)

    # ------------------------------------------------------------
    # Scrittura report QC
    # ------------------------------------------------------------
    qc_out1 = strategy_path.with_name(f"QC_{strategy_path.stem}.csv")
    qc_df.to_csv(qc_out1, sep=";", index=False)
    print(f"\n[STRATEGY_QC] Report scritto: {qc_out1}")

    base_out = (ENV_OUT_DIR if ENV_OUT_DIR else TEST_DATA_DIR)
    qc_out2 = Path(base_out) / f"QC_{strategy_path.stem}.csv"
    if qc_out2.resolve() != qc_out1.resolve():
        qc_df.to_csv(qc_out2, sep=";", index=False)
        print(f"[STRATEGY_QC] Copia report: {qc_out2}")

    # ------------------------------------------------------------
    # Esito
    # ------------------------------------------------------------
    if n_err > 0:
        if PIPELINE_MODE:
            graceful_exit("Config strategia invalida (QC ERROR) in PIPELINE_MODE.", 2)
        return False

    return True



def _auto_pick_strategy(df_kpi: pd.DataFrame) -> Optional[Path]:
    if ENV_STRATEGY_FILE and ENV_STRATEGY_FILE.exists():
        strat = ENV_STRATEGY_FILE

        if not run_strategy_preflight_or_reject(strat, df_kpi):
            print("⚠️ Strategia da pipeline rifiutata: QC ERROR nel file.")
            return None

        strategy = load_strategy(str(strat))
        missing = [c for c in strategy.required_indicators if c not in df_kpi.columns]
        if not missing:
            return strat

        print("⚠️ Strategia da pipeline non compatibile (indicatori mancanti):", ", ".join(missing))

    return None


def interactive_selection() -> Selection:
    # --- directory input KPI (default: _data/Test Data) con conferma/modifica ---
    data_dir = ENV_DATA_DIR if ENV_DATA_DIR else TEST_DATA_DIR
    data_dir = Path(data_dir).expanduser().resolve()

    while True:
        print("\n======================================")
        print("Directory di input (file KPI)")
        print("======================================")
        print(f"Default: {data_dir}")
        ans = input("Confermi? [Y/n] (invio=Y, '?'=aiuto): ").strip()

        if ans == "" or ans.lower() == "y":
            break

        if ans == "?":
            print("\nAiuto:")
            print("- Invio / Y : usa la directory default")
            print("- n         : inserisci un path alternativo")
            print("- Puoi incollare direttamente un path completo")
            continue

        if ans.lower() == "n":
            newp = input("Inserisci path directory (o invio per annullare): ").strip()
            if not newp:
                continue
            cand = Path(newp).expanduser().resolve()
            if cand.exists() and cand.is_dir():
                data_dir = cand
                continue
            print(f"❌ Directory non valida: {cand}")
            continue

        # caso: l'utente incolla direttamente un path
        cand = Path(ans).expanduser().resolve()
        if cand.exists() and cand.is_dir():
            data_dir = cand
            continue

        print("❌ Scelta non valida. Riprova.")

    # --- normalizzazione: se passano _data, usa _data/Test Data se esiste ---
    td = (data_dir / "Test Data")
    if data_dir.name.lower() == "_data" and td.exists() and td.is_dir():
        data_dir = td.resolve()

    kpi = _auto_pick_kpi()
    if kpi is None:
        kpi = select_from_menu(
            "Su quale file vuoi testare la strategia? (solo KPI_*.csv)",
            list_kpi_files_only(data_dir),
        )

    df_kpi = load_kpi_csv(kpi)

    # In QC-only non facciamo auto-pick strategy basato su required indicators
    if not PREFLIGHT_ONESHOT:
        strat = _auto_pick_strategy(df_kpi)
        if strat is not None:
            return Selection(kpi, strat)

    while True:
        strat = select_from_menu(
            f"Quale strategia vuoi utilizzare?\n(directory: {STRATEGY_DIR})",
            list_files(STRATEGY_DIR, ["xlsx"]),
        )

        if not run_strategy_preflight_or_reject(strat, df_kpi):
            print("⚠️ Strategia rifiutata: correggi gli ERROR nel foglio CONDITIONS.")
            continue

        # QC-only: NON caricare engine/strategy (evita crash Condition/notes)
        if PREFLIGHT_ONESHOT:
            return Selection(kpi, strat)

        # Run normale: dopo QC OK carichiamo strategia e controlliamo indicatori richiesti
        strategy = load_strategy(str(strat))
        missing = [c for c in strategy.required_indicators if c not in df_kpi.columns]
        if not missing:
            return Selection(kpi, strat)

        print("Indicatori mancanti nel KPI:", ", ".join(missing))




def main() -> None:
    print("[DBG] RUNNING FILE:", __file__)

    ensure_engine_available()

    # ------------------------------------------------------------
    # MODALITÀ QC-ONLY (richiamata da pipeline opzione 7)
    # ------------------------------------------------------------
    if PREFLIGHT_ONESHOT:
        # Interattivo: scegli KPI + strategia, fai QC, scrivi report, poi esci.
        sel = interactive_selection()

        strategy_path = Path(sel.strategy).resolve()
        kpi_path = Path(sel.kpi).resolve()

        print(f"[DBG] QC-ONLY Selected strategy = {strategy_path}")
        print(f"[DBG] QC-ONLY Selected KPI      = {kpi_path}")

        df = load_kpi_csv(kpi_path)
        # --- Column safety: preserve KPI_ column order ---
        original_cols = list(df.columns)

        ok = run_strategy_preflight_or_reject(strategy_path, df)
        # In QC-only: exit code 0 se ok, 2 se error (pipeline già protegge e torna al menu)
        raise SystemExit(0 if ok else 2)

    # ------------------------------------------------------------
    # RUN NORMALE (genera SIGNAL)
    # ------------------------------------------------------------
    sel = interactive_selection()

    strategy_path = Path(sel.strategy).resolve()
    kpi_path = Path(sel.kpi).resolve()

    print(f"[DBG] Selected strategy file = {strategy_path}")
    st = strategy_path.stat()
    print(f"[DBG] Strategy mtime={st.st_mtime} size={st.st_size} bytes")

    df = load_kpi_csv(kpi_path)
    # --- Column safety: preserve original KPI_/REGIME_ column order ---
    original_cols = list(df.columns)

    # SOLO SE QC OK → carica strategia
    strategy = load_strategy(str(strategy_path))

    # Coerce only required indicators (engine comparisons become deterministic)
    df = coerce_required_indicators(df, strategy.required_indicators)

    missing = [c for c in strategy.required_indicators if c not in df.columns]
    if missing:
        print("[WARN] Missing required indicators:", ", ".join(missing))

    debug_engine = True  # metti False quando hai finito il debug

    groups = list(iter_groups(df))
    print("[DBG] iter_groups count =", len(groups))
    for k, g in groups[:3]:
        print("[DBG] group sample:", k, "rows=", len(g))

    df = apply_signals(df, strategy, debug=debug_engine)

    print("[DBG] gated vs no_regime signal equality ratio:",
          float((df["SIGNAL"] == df["SIGNAL_no_regime"]).mean()))
    print("[DBG] gated SIGNAL vc:", df["SIGNAL"].value_counts().head(10).to_dict())
    print("[DBG] no_regime SIGNAL vc:", df["SIGNAL_no_regime"].value_counts().head(10).to_dict())

    # --- NO_REGIME context marker (sempre abilitato) ---
    # Serve solo come colonna di confronto/report; non influenza la strategia.
    df["REGIME_no_regime"] = "ALL"  # oppure "ALWAYS_ON"

    print("[DBG] SIGNAL dtype:", df["SIGNAL"].dtype if "SIGNAL" in df.columns else None)
    print("[DBG] SIGNAL notna ratio:", float(df["SIGNAL"].notna().mean()) if "SIGNAL" in df.columns else None)
    print("[DBG] SIGNAL head:", df["SIGNAL"].head(10).tolist() if "SIGNAL" in df.columns else None)
    print(
        "[DBG] SIGNAL vc:",
        df["SIGNAL"].value_counts(dropna=False).head(10).to_dict() if "SIGNAL" in df.columns else None,
    )

    # ============================================================
    # REGIME gating - BLOCCA ENTRY e FORZA EXIT in VOLATILE (RAW)
    # ============================================================
    if "REGIME_L1_RAW" in df.columns and "SIGNAL" in df.columns:
        reg = df["REGIME_L1_RAW"].astype(str).str.upper().str.strip()
        sig = df["SIGNAL"].astype(str).str.upper().str.strip()

        out_sig = sig.copy()
        hold_state = HOLD_OUT
        forced_exits = 0
        blocked_entries = 0

        for j in out_sig.index:
            r = reg.at[j]
            s = out_sig.at[j]

            # --- FORZA USCITA se siamo IN e il regime diventa VOLATILE ---
            if r == "VOLATILE" and hold_state == HOLD_IN:
                # forza exit sulla prima barra VOLATILE mentre siamo IN
                out_sig.at[j] = "SHORT"
                hold_state = HOLD_OUT
                forced_exits += 1
                continue

            if s == "LONG":
                if hold_state != HOLD_IN:
                    # nuova entry: in VOLATILE la blocchiamo
                    if r == "VOLATILE":
                        out_sig.at[j] = "NEUTRAL"
                        blocked_entries += 1
                    else:
                        hold_state = HOLD_IN
                # se già IN, lasciamo LONG

            elif s == "SHORT":
                # EXIT sempre permessa se siamo IN
                if hold_state == HOLD_IN:
                    hold_state = HOLD_OUT
                else:
                    # SHORT mentre OUT non deve aprire nuove posizioni
                    out_sig.at[j] = "NEUTRAL"

            else:
                # NEUTRAL o altro
                pass

        df["SIGNAL"] = out_sig

    # DBG: verifica gating applicato
    if "REGIME_L1_RAW" in df.columns and "SIGNAL" in df.columns:
        _mask = df["REGIME_L1_RAW"].astype(str).str.upper().str.strip().eq("VOLATILE")
        _sigL = df["SIGNAL"].astype(str).str.upper().str.strip().eq("LONG")
        _hold = df["SIGNAL"].astype(str).str.upper().str.strip().eq("LONG")  # proxy
        print(
            "[DBG][POST-GATE] VOLATILE rows =", int(_mask.sum()),
            "SIGNAL LONG in VOLATILE =", int((_mask & _sigL).sum()),
        )

    df, entry, exit_ = apply_hold_value(df)
    df, entry_nr, exit_nr = apply_hold_value(
        df,
        signal_col="SIGNAL_no_regime",
        hold_col="HOLD_no_regime",
        value_col="VALUE_no_regime",
    )

    df = normalize_signal_hold(df)

    df, trade_summary = add_trade_enrichment(df, trading_day_minutes=480.0, trading_days_per_week=5.0)

    # ------------------------------------------------------------
    # NO_REGIME trade enrichment (Profit/Trade, Trade_ID, timing, freq)
    # calcolato usando HOLD_no_regime / VALUE_no_regime
    # ------------------------------------------------------------
    required_nr = ["HOLD_no_regime", "VALUE_no_regime"]
    if all(c in df.columns for c in required_nr):
        df_tmp = df.copy()

        # Rimappo le colonne attese da add_trade_enrichment
        df_tmp["HOLD"] = df_tmp["HOLD_no_regime"]
        df_tmp["VALUE"] = df_tmp["VALUE_no_regime"]

        # se VALUE_no_regime è NaN sulle entry, usiamo close (robusto)
        hold_nr = df_tmp["HOLD_no_regime"].astype(str).str.strip().str.upper()
        m_entry_nr = hold_nr.eq("IN") & ~hold_nr.shift(1).fillna("OUT").eq("IN")

        # fill SOLO sulla barra di entry
        df_tmp.loc[m_entry_nr & df_tmp["VALUE_no_regime"].isna(), "VALUE_no_regime"] = df_tmp.loc[
            m_entry_nr & df_tmp["VALUE_no_regime"].isna(), "close"
        ]

        # riallineo VALUE atteso (entry-only)
        df_tmp.loc[m_entry_nr & df_tmp["VALUE"].isna(), "VALUE"] = df_tmp.loc[
            m_entry_nr & df_tmp["VALUE"].isna(), "close"
        ]

        # RICALCOLO enrichment su df_tmp usando HOLD/VALUE rimappati (NO_REGIME)
        df_tmp, _ = add_trade_enrichment(
            df_tmp,
            trading_day_minutes=480.0,
            trading_days_per_week=5.0
        )




        # colonne prodotte da add_trade_enrichment che vuoi anche in versione _no_regime
        enrich_cols = [
            "Profit/Trade",
            "Trade_ID",
            "Minutes_IN_to_OUT",
            "Minutes_OUT_to_next_IN",
            "Sum Profit/Trade",
            "Avg Profit/Trade",
            "Win Rate",
            "Max DD Start",
            "Max DD Peak",
            "Trades/Day_8h_AvgIdle",
            "Trades/Day_8h_MedIdle",
            "Trades/Week_8h_AvgIdle",
            "Trades/Week_8h_MedIdle",
        ]

        for c in enrich_cols:
            if c in df_tmp.columns:
                df[f"{c}_no_regime"] = df_tmp[c]
    else:
        print("[WARN] NO_REGIME enrichment skipped: missing columns", required_nr)

    out_dir = ENV_OUT_DIR if ENV_OUT_DIR else sel.kpi.parent
    out = build_output_path_in_dir(sel.kpi, out_dir)

    # ------------------------------------------------------------
    # Metadata strategia (colonne costanti nel SIGNAL)
    # ------------------------------------------------------------

    config_strategy_file = Path(strategy_path).name
    strategy_name = Path(strategy_path).stem

    df["strategy_name"] = strategy_name
    df["config_strategy_file"] = config_strategy_file

    # --- Final column order: preserve KPI_/REGIME_ and append new columns only ---
    new_cols = [c for c in df.columns if c not in original_cols]

    # 1) separa il blocco ungated: tutte le colonne *_no_regime devono stare in fondo, contigue
    # Ordine ungated = stesso ordine delle gated, con suffisso _no_regime
    order_template = [
        # --- context marker ---
        "REGIME",

        # --- signal layer ---
        "SIGNAL",
        "HOLD",
        "VALUE",

        # --- trade enrichment (report-like columns) ---
        "Profit/Trade",
        "Trade_ID",
        "Minutes_IN_to_OUT",
        "Minutes_OUT_to_next_IN",
        "Sum Profit/Trade",
        "Avg Profit/Trade",
        "Win Rate",
        "Max DD Start",
        "Max DD Peak",
        "Trades/Day_8h_AvgIdle",
        "Trades/Day_8h_MedIdle",
        "Trades/Week_8h_AvgIdle",
        "Trades/Week_8h_MedIdle",


    ]

    # Lista desiderata in output (solo se esiste)
    no_regime_cols = []
    for base in order_template:
        c = f"{base}_no_regime"
        if c in df.columns:
            no_regime_cols.append(c)

    # Aggiungi eventuali altre *_no_regime non previste in template, ma senza rompere la contiguità
    extra_nr = [c for c in new_cols if c.endswith("_no_regime") and c not in no_regime_cols]
    no_regime_cols += extra_nr

    new_cols_base = [c for c in new_cols if c not in no_regime_cols]

    # 2) Vuoi: supertrend_dir + supertrend_dir_txt SUBITO PRIMA delle colonne REGIME_*
    st_cols = [c for c in ["supertrend_dir", "supertrend_dir_txt"] if c in new_cols_base]
    new_cols_rest = [c for c in new_cols_base if c not in st_cols]

    regime_cols = [c for c in original_cols if c.startswith("REGIME_")]

    if regime_cols and st_cols:
        first_regime_idx = original_cols.index(regime_cols[0])
        prefix = original_cols[:first_regime_idx]
        suffix = original_cols[first_regime_idx:]  # include tutte le REGIME_* (e ciò che segue nel KPI)
        df = df[prefix + st_cols + suffix + new_cols_rest + no_regime_cols]
    else:
        df = df[original_cols + new_cols_rest + no_regime_cols]

    # DBG: verifica finale prima del salvataggio
    if "REGIME_L1_RAW" in df.columns and "SIGNAL" in df.columns and "HOLD" in df.columns:
        _mask = df["REGIME_L1_RAW"].astype(str).str.upper().str.strip().eq("VOLATILE")
        _hold_in = df["HOLD"].astype(str).str.upper().str.strip().eq("IN")
        _sigL = df["SIGNAL"].astype(str).str.upper().str.strip().eq("LONG")
        print("[DBG][PRE-SAVE] VOLATILE rows =", int(_mask.sum()),
              "HOLD IN in VOLATILE =", int((_mask & _hold_in).sum()),
              "SIGNAL LONG in VOLATILE =", int((_mask & _sigL).sum()))


    write_output_csv(df, out)

    print(f"\nOutput scritto: {out}")
    print_summary(df, entry, exit_)

    # ------------------------------------------------------------
    # Trade frequency summary (8h/day)
    # ------------------------------------------------------------
    if isinstance(trade_summary, dict) and trade_summary:
        print("\n===== TRADE FREQUENCY (8h/day) =====")
        print(f"Trades closed: {trade_summary.get('trades_closed')}")
        print(f"Avg trade duration (min): {trade_summary.get('avg_trade_minutes')}")
        print(f"Median trade duration (min): {trade_summary.get('med_trade_minutes')}")
        print(f"Avg idle OUT->next IN (min): {trade_summary.get('avg_idle_minutes')}")
        print(f"Median idle OUT->next IN (min): {trade_summary.get('med_idle_minutes')}")
        print(f"Trades/day (avg idle): {trade_summary.get('trades_day_8h_avg_idle')}")
        print(f"Trades/day (median idle): {trade_summary.get('trades_day_8h_med_idle')}")
        print(f"Trades/week 5d (avg): {trade_summary.get('trades_week_8h_avg_idle')}")
        print(f"Trades/week 5d (median): {trade_summary.get('trades_week_8h_med_idle')}")

        # write compact CSV for Report_Strategia
        try:
            summary_path = out.with_name(f"TRADE_FREQ_{out.stem}.csv")
            pd.DataFrame([trade_summary]).to_csv(summary_path, sep=";", index=False, decimal=",")
            print(f"[OK] Trade summary scritto: {summary_path}")
        except Exception as e:
            print(f"[WARN] Impossibile scrivere TRADE_FREQ_*.csv: {e!r}")


if __name__ == "__main__":
    main()
