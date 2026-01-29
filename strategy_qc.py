#!/usr/bin/env python3
from __future__ import annotations

"""
strategy_qc.py — QC-preflight autonoma per config_strategy_v2.xlsx

Uso tipico (validazione contro KPI reale):
  python3 strategy_qc.py --strategy-xlsx /path/config_strategy_v2.xlsx --kpi-csv /path/KPI_....csv

Fallback (usa sheet KPI_COLUMNS dentro l'xlsx):
  python3 strategy_qc.py --strategy-xlsx /path/config_strategy_v2.xlsx --use-kpi-columns-sheet

Output:
  - stampa riepilogo QC + tabella regole (enabled=VERO)
  - crea QC_<strategy_stem>.csv accanto al file strategia (o --out-csv)

Exit code:
  - 0 se OK/WARN soltanto
  - 2 se presenti ERROR
"""

import argparse
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import pandas as pd

CSV_SEP = ";"

# domini attesi (derivati dal tuo file)
_ALLOWED_SCOPE = {"REGIME", "ENTRY", "EXIT"}
_ALLOWED_SIDE = {"BOTH", "LONG", "SHORT"}
_ALLOWED_LOGIC = {"AND", "OR"}  # OR: ammesso come input (se engine non lo supporta ancora -> warning)
_ALLOWED_RHS_TYPE = {"LIST", "VALUE", "COLUMN"}
_ALLOWED_OPERATOR = {"in", "==", "!=", ">", ">=", "<", "<=", "between", "cross_above", "cross_below"}

_list_pat = re.compile(r"^\s*[\(\[]\s*([^\)\]]+)\s*[\)\]]\s*$")  # (a,b) o [a,b]


# ============================================================
# Data model
# ============================================================
@dataclass(frozen=True)
class StrategyIssue:
    level: str  # "ERROR" | "WARN"
    rule_id: str
    message: str


# ============================================================
# Helpers
# ============================================================
def _to_bool(x: Any, default: bool = True) -> bool:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return default
    s = str(x).strip().lower()
    if s in {"1", "true", "t", "y", "yes", "si", "sì", "on", "vero"}:
        return True
    if s in {"0", "false", "f", "n", "no", "off", "falso"}:
        return False
    return default


def _to_float(x: Any) -> Optional[float]:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _parse_list_rhs(x: Any) -> Optional[List[float]]:
    """
    Accetta '(1,-1)' oppure '[45,65]' con separatore virgola. Gestisce anche decimali con virgola.
    Ritorna lista di float oppure None se non parsabile.
    """
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    m = _list_pat.match(s)
    if not m:
        return None
    inner = m.group(1)
    parts = [p.strip() for p in inner.split(",") if p.strip() != ""]
    out: List[float] = []
    for p in parts:
        f = _to_float(p)
        if f is None:
            return None
        out.append(f)
    return out


def _read_kpi_columns_from_csv(kpi_csv: Path) -> List[str]:
    # leggiamo solo header, veloce
    df0 = pd.read_csv(kpi_csv, sep=CSV_SEP, nrows=0, dtype=str)
    return [str(c).strip() for c in df0.columns]


def _read_kpi_columns_from_sheet(strategy_xlsx: Path, sheet_name: str = "KPI_COLUMNS") -> List[str]:
    """
    Atteso sheet con almeno una colonna che contenga i nomi indicatori.
    Cerchiamo la prima colonna non-vuota e la usiamo come elenco.
    """
    df = pd.read_excel(strategy_xlsx, sheet_name=sheet_name, engine="openpyxl")



    df.columns = [str(c).strip() for c in df.columns]
    if df.empty:
        return []
    for c in df.columns:
        series = df[c].dropna().astype(str).str.strip()
        vals = [v for v in series.tolist() if v != ""]
        if vals:
            return vals
    return []


# ============================================================
# Core QC
# ============================================================
def qc_strategy_preflight(
    strategy_xlsx: Path,
    kpi_columns: Sequence[str],
    sheet_name: str = "CONDITIONS",
    strict_logic_support: bool = False,
) -> Tuple[pd.DataFrame, List[StrategyIssue]]:
    """
    QC su foglio CONDITIONS, validando contro kpi_columns.
    - strict_logic_support=False: logic=OR produce solo WARN
    - strict_logic_support=True: logic=OR produce ERROR (se l'engine non lo supporta)
    """
    import warnings

    warnings.filterwarnings(
        "ignore",
        message="Data Validation extension is not supported and will be removed",
        category=UserWarning,
        module="openpyxl",
    )

    df = pd.read_excel(strategy_xlsx, sheet_name=sheet_name, engine="openpyxl")



    df.columns = [str(c).strip() for c in df.columns]

    required_cols = {
        "id", "enabled", "scope", "side", "group", "logic",
        "lhs_col", "operator", "rhs_type", "rhs_value", "rhs_col",
        "shift", "negate", "notes",
    }

    issues: List[StrategyIssue] = []

    missing = sorted(required_cols - set(df.columns))
    if missing:
        issues.append(
            StrategyIssue(
                "ERROR",
                "GLOBAL",
                f"Colonne mancanti nel foglio '{sheet_name}': {', '.join(missing)}",
            )
        )
        df = df.copy()
        df["qc_status"] = "ERROR"
        df["qc_message"] = f"Missing columns: {', '.join(missing)}"
        df["enabled_norm"] = False
        df["excel_row"] = [int(i) + 2 for i in df.index]
        return df, issues

    # KPI columns reference
    kpi_set = {str(c).strip() for c in kpi_columns if str(c).strip() != ""}
    kpi_lower = {c.lower(): c for c in kpi_set}

    df = df.copy()

    # id: stringa libera
    df["id"] = df["id"].astype(str).str.strip().replace({"nan": "", "NAN": ""})

    # normalizza enabled (serve al filtro)
    df["enabled"] = df["enabled"].astype(str).str.strip().replace({"nan": "", "NAN": ""})

    # normalizza campi core (pulizia)
    df["lhs_col"] = df["lhs_col"].astype(str).str.strip().replace({"nan": "", "NAN": ""})
    df["operator"] = df["operator"].astype(str).str.strip().replace({"nan": "", "NAN": ""})
    df["rhs_type"] = (
        df["rhs_type"]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"nan": "", "NAN": ""})
    )

    # regola identificata solo da id non vuoto
    mask_has_id = df["id"] != ""

    # enabled vero (accetta VERO/TRUE/1 ecc.)
    mask_enabled_true = df["enabled"].apply(lambda x: _to_bool(x, default=False))

    # include SEMPRE enabled=VERO + id
    mask_include_enabled_true = mask_has_id & mask_enabled_true

    # (opzionale) includi disabled solo se "sembra" una regola (per report)
    mask_any_core = (df["lhs_col"] != "") | (df["operator"] != "") | (df["rhs_type"] != "")
    mask_include_disabled_rules = mask_has_id & (~mask_enabled_true) & mask_any_core

    df = df.loc[mask_include_enabled_true | mask_include_disabled_rules].copy()

    qc_status: List[str] = []
    qc_message: List[str] = []
    enabled_norm: List[bool] = []

    has_kpi = bool(kpi_set)

    for idx, row in df.iterrows():
        excel_row = int(idx) + 2
        rid = str(row.get("id", "")).strip() or f"ROW_{excel_row}"

        row_err: List[str] = []
        row_warn: List[str] = []

        enabled = _to_bool(row.get("enabled", True), default=True)
        enabled_norm.append(bool(enabled))

        if not enabled:
            qc_status.append("DISABLED")
            qc_message.append("")
            continue

        scope = str(row.get("scope", "")).strip().upper()
        side = str(row.get("side", "")).strip().upper()
        group = str(row.get("group", "")).strip()
        logic = str(row.get("logic", "")).strip().upper()
        lhs = str(row.get("lhs_col", "")).strip()
        op = str(row.get("operator", "")).strip().lower()
        rhs_type = str(row.get("rhs_type", "")).strip().upper()
        rhs_val = row.get("rhs_value", None)
        rhs_col = str(row.get("rhs_col", "")).strip() if pd.notna(row.get("rhs_col", None)) else ""
        notes = str(row.get("notes", "")).strip() if pd.notna(row.get("notes", None)) else ""

        shift = row.get("shift", 0)
        if shift == "" or pd.isna(shift):
            shift = 0

        # scope / side
        if scope not in _ALLOWED_SCOPE:
            row_err.append(f"scope non valido: '{scope}' (ammessi: {sorted(_ALLOWED_SCOPE)})")
        if side not in _ALLOWED_SIDE:
            row_err.append(f"side non valido: '{side}' (ammessi: {sorted(_ALLOWED_SIDE)})")

        if scope in {"ENTRY", "EXIT"} and group == "":
            row_err.append("group vuoto: obbligatorio per scope=ENTRY/EXIT")

        # logic
        if logic not in _ALLOWED_LOGIC:
            row_warn.append(f"logic non standard: '{logic}'")
        elif logic == "OR":
            msg = "logic=OR presente: verifica supporto nell'engine"
            if strict_logic_support:
                row_err.append(msg)
            else:
                row_warn.append(msg)

        # shift
        try:
            shift_i = int(shift)
            if shift_i < 0:
                row_err.append("shift deve essere >= 0")
        except Exception:
            row_err.append(f"shift non intero: '{shift}'")

        # operator / rhs_type
        if op == "":
            row_err.append("operator vuoto (obbligatorio)")
        elif op not in _ALLOWED_OPERATOR:
            row_err.append(f"operator non valido: '{op}'")

        if rhs_type not in _ALLOWED_RHS_TYPE:
            row_err.append(f"rhs_type non valido: '{rhs_type}'")

        # lhs
        if lhs == "":
            row_err.append("lhs_col vuoto")
        elif not has_kpi:
            row_warn.append(f"lhs_col non verificabile (KPI columns non disponibili): '{lhs}'")
        elif lhs not in kpi_set:
            if lhs.lower() in kpi_lower:
                row_warn.append(f"lhs_col case mismatch: '{lhs}' → '{kpi_lower[lhs.lower()]}'")
            else:
                row_err.append(f"lhs_col non presente nel KPI: '{lhs}'")

        # rhs
        if rhs_type == "COLUMN":
            if rhs_col == "":
                row_err.append("rhs_type=COLUMN ma rhs_col vuoto")
            elif not has_kpi:
                row_warn.append(f"rhs_col non verificabile (KPI columns non disponibili): '{rhs_col}'")
            elif rhs_col not in kpi_set:
                if rhs_col.lower() in kpi_lower:
                    row_warn.append(f"rhs_col case mismatch: '{rhs_col}' → '{kpi_lower[rhs_col.lower()]}'")
                else:
                    row_err.append(f"rhs_col non presente nel KPI: '{rhs_col}'")

        elif rhs_type == "VALUE":
            if _to_float(rhs_val) is None:
                row_err.append(f"rhs_value non numerico: '{rhs_val}'")

        elif rhs_type == "LIST":
            lst = _parse_list_rhs(rhs_val)
            if not lst:
                row_err.append(f"rhs_value LIST non parsabile: '{rhs_val}'")
            if op == "between" and (not lst or len(lst) != 2):
                row_err.append("between richiede 2 valori")

        if notes == "":
            row_warn.append("notes vuoto (consigliato)")

        if row_err:
            qc_status.append("ERROR")
            qc_message.append(" | ".join(row_err + row_warn))
            issues.append(StrategyIssue("ERROR", rid, " | ".join(row_err)))
            for w in row_warn:
                issues.append(StrategyIssue("WARN", rid, w))
        elif row_warn:
            qc_status.append("WARN")
            qc_message.append(" | ".join(row_warn))
            for w in row_warn:
                issues.append(StrategyIssue("WARN", rid, w))
        else:
            qc_status.append("OK")
            qc_message.append("")

    df["qc_status"] = qc_status
    df["qc_message"] = qc_message
    df["enabled_norm"] = enabled_norm
    df["excel_row"] = [int(i) + 2 for i in df.index]

    return df, issues


# ============================================================
# Output
# ============================================================
def _print_qc_summary(df_qc: pd.DataFrame, issues: List[StrategyIssue]) -> int:
    # conteggi da df
    n_ok = int((df_qc["qc_status"] == "OK").sum()) if "qc_status" in df_qc.columns else 0
    n_warn = int((df_qc["qc_status"] == "WARN").sum()) if "qc_status" in df_qc.columns else 0
    n_err = int((df_qc["qc_status"] == "ERROR").sum()) if "qc_status" in df_qc.columns else 0
    n_dis = int((df_qc["qc_status"] == "DISABLED").sum()) if "qc_status" in df_qc.columns else 0

    print("\n[STRATEGY_QC] Riepilogo")
    print(f"  OK={n_ok} WARN={n_warn} ERROR={n_err} DISABLED={n_dis}")

    # separa GLOBAL vs non-GLOBAL
    global_err = [i for i in issues if i.level == "ERROR" and i.rule_id == "GLOBAL"]
    global_warn = [i for i in issues if i.level == "WARN" and i.rule_id == "GLOBAL"]
    row_err = [i for i in issues if i.level == "ERROR" and i.rule_id != "GLOBAL"]
    row_warn = [i for i in issues if i.level == "WARN" and i.rule_id != "GLOBAL"]

    # stampa GLOBAL per primi
    if global_err or global_warn:
        print("\n[STRATEGY_QC] Note globali")
        for i in global_err:
            print(f"  ❌ GLOBAL: {i.message}")
        for i in global_warn:
            print(f"  ⚠️ GLOBAL: {i.message}")

    # stampa dettagli errori/warn sintetici
    if row_err or row_warn:
        print("\n[STRATEGY_QC] Dettagli regole (errori / warning)")
        for i in row_err[:20]:
            print(f"  ❌ {i.rule_id}: {i.message}")
        for i in row_warn[:12]:
            print(f"  ⚠️ {i.rule_id}: {i.message}")

    # ============================================================
    # Tabella regole verificate (OK / WARN / ERROR) — solo enabled=VERO
    # ============================================================
    print("\n[STRATEGY_QC] Regole verificate (solo enabled=VERO)")

    required_cols = {
        "id", "qc_status", "enabled_norm", "scope", "side", "group",
        "lhs_col", "operator", "rhs_type", "rhs_value", "rhs_col", "shift"
    }

    def _fit(val: object, width: int) -> str:
        if val is None:
            s = "❌"
        else:
            s = str(val).replace("\n", " ").strip()
            if s == "" or s.lower() == "nan":
                s = "❌"
        if len(s) <= width:
            return s.ljust(width)
        if width <= 1:
            return "…"[:width]
        return s[: width - 1] + "…"

    cols = [
        ("ST", 2),
        ("ID", 10),
        ("SCOPE", 7),
        ("SIDE", 5),
        ("GRP", 4),
        ("LHS", 18),
        ("OP", 12),
        ("RHS", 18),
        ("SH", 3),
    ]

    header = " | ".join(_fit(name, w) for name, w in cols)
    sep = "-" * len(header)
    print("  " + header)
    print("  " + sep)

    if not required_cols.issubset(df_qc.columns):
        missing = sorted(required_cols - set(df_qc.columns))
        print(f"  (tabella non disponibile: mancano colonne {missing})")
    else:
        shown = 0
        for _, r in df_qc.iterrows():
            if not bool(r.get("enabled_norm", False)):
                continue

            status = str(r.get("qc_status", "")).upper()
            if status not in {"OK", "WARN", "ERROR"}:
                continue

            icon = {"OK": "✅", "WARN": "⚠️", "ERROR": "❌"}.get(status, "?")

            rhs_type = str(r.get("rhs_type", "")).upper()
            rhs = r.get("rhs_col", "") if rhs_type == "COLUMN" else r.get("rhs_value", "")

            row_vals = [
                icon,
                r.get("id", ""),
                r.get("scope", ""),
                r.get("side", ""),
                r.get("group", ""),
                r.get("lhs_col", ""),
                r.get("operator", ""),
                rhs,
                r.get("shift", ""),
            ]

            line = " | ".join(_fit(v, w) for v, (_, w) in zip(row_vals, cols))
            print("  " + line)
            shown += 1

        if shown == 0:
            print("  (nessuna regola enabled=VERO da mostrare)")

    # ritorna numero errori (global + righe)
    return len(global_err) + len(row_err)


# ============================================================
# CLI
# ============================================================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QC-preflight config_strategy (foglio CONDITIONS).")
    p.add_argument("--strategy-xlsx", type=str, required=True, help="Path al file config_strategy_v2.xlsx")
    p.add_argument("--sheet", type=str, default="CONDITIONS", help="Nome sheet condizioni (default: CONDITIONS)")

    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--kpi-csv", type=str, default=None, help="Path al file KPI_*.csv (valida contro colonne reali)")
    g.add_argument("--use-kpi-columns-sheet", action="store_true", help="Usa sheet KPI_COLUMNS dentro lo xlsx")
    g.add_argument("--kpi-columns", type=str, default=None, help="Lista colonne KPI separata da virgola")

    p.add_argument("--kpi-columns-sheet-name", type=str, default="KPI_COLUMNS", help="Nome sheet KPI_COLUMNS")
    p.add_argument("--out-csv", type=str, default=None, help="Path output QC csv (default: accanto a strategy)")
    p.add_argument("--strict-logic-support", action="store_true", help="Se True: logic=OR diventa ERROR")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    strategy = Path(args.strategy_xlsx).expanduser().resolve()
    if not strategy.exists():
        raise SystemExit(f"File strategia non trovato: {strategy}")

    if strategy.name.startswith("~$"):
        raise SystemExit(
            f"File non valido (lock file Excel): {strategy.name}. "
            "Chiudi Excel o seleziona il file .xlsx reale (senza prefisso '~$')."
        )
    # decide kpi_columns
    kpi_cols: List[str] = []
    if args.kpi_csv:
        kpi_csv = Path(args.kpi_csv).expanduser().resolve()
        if not kpi_csv.exists():
            raise SystemExit(f"File KPI non trovato: {kpi_csv}")
        kpi_cols = _read_kpi_columns_from_csv(kpi_csv)
        print(f"[STRATEGY_QC] KPI columns da CSV: {kpi_csv.name} ({len(kpi_cols)} colonne)")

    elif args.kpi_columns:
        kpi_cols = [c.strip() for c in args.kpi_columns.split(",") if c.strip() != ""]
        print(f"[STRATEGY_QC] KPI columns da argomento: {len(kpi_cols)}")

    else:
        # fallback automatico: prova sempre a usare KPI_COLUMNS (se presente e popolato)
        kpi_cols = _read_kpi_columns_from_sheet(strategy, sheet_name=args.kpi_columns_sheet_name)
        if kpi_cols:
            print(f"[STRATEGY_QC] KPI columns da sheet '{args.kpi_columns_sheet_name}': {len(kpi_cols)}")

    if not kpi_cols:
        print("⚠️ Nessuna colonna KPI disponibile: QC limitata (non posso verificare lhs/rhs_col).")

    df_qc, issues = qc_strategy_preflight(
        strategy_xlsx=strategy,
        kpi_columns=kpi_cols,
        sheet_name=args.sheet,
        strict_logic_support=args.strict_logic_support,
    )

    n_err = _print_qc_summary(df_qc, issues)

    out_csv = Path(args.out_csv).expanduser().resolve() if args.out_csv else strategy.with_name(f"QC_{strategy.stem}.csv")

    cols_front = [
        "excel_row", "id", "enabled", "scope", "side", "group", "logic",
        "lhs_col", "operator", "rhs_type", "rhs_value", "rhs_col", "shift", "negate",
        "qc_status", "qc_message", "enabled_norm"
    ]
    cols_front = [c for c in cols_front if c in df_qc.columns]
    df_qc = df_qc[cols_front + [c for c in df_qc.columns if c not in cols_front]]

    df_qc.to_csv(out_csv, sep=";", index=False)
    print(f"\n[STRATEGY_QC] Report scritto: {out_csv}")

    raise SystemExit(2 if n_err > 0 else 0)


if __name__ == "__main__":
    main()
