#!/usr/bin/env python3
from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Any, Set
import math

import pandas as pd

# ============================================================
# CONFIG (modifica se necessario)
# ============================================================
TEST_DATA_DIR = Path("/Users/claudio 1/n8n-shared/Test Data")
STRATEGY_DIR = Path("/Users/claudio 1/n8n-shared/config_strategy")
CSV_SEP = ";"

ALLOWED_SIGNALS = {"LONG", "SHORT", "NEUTRAL"}
HOLD_IN = "IN"
HOLD_SHORT = "SHORT"
HOLD_OUT = "OUT"


# ============================================================
# IMPORT ENGINE
# ============================================================
try:
    # load_engine.py è nello stesso livello di run_strategia.py
    from load_engine import load_config_strategy, generate_signals, Condition  # type: ignore
except Exception as e:  # pragma: no cover
    load_config_strategy = None
    generate_signals = None
    Condition = None
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


# ============================================================
# ADAPTER: interfaccia richiesta
# ============================================================
@dataclass(frozen=True)
class Strategy:
    """
    Adapter per rendere l'engine compatibile con:
    - strategy.required_indicators
    - compute_signal(df, strategy)
    """
    path: Path
    conditions: List[Any]  # List[Condition]
    required_indicators: List[str]


def ensure_engine_available() -> None:
    if _IMPORT_ERR is not None or load_config_strategy is None or generate_signals is None:
        print("ERRORE: impossibile importare il load-engine.")
        print("Verifica che 'load_engine.py' sia nello stesso livello e che l'import sia corretto.")
        print(f"Dettaglio errore import: {_IMPORT_ERR!r}")
        raise SystemExit(2)


def compute_required_indicators_from_conditions(conditions: List[Any]) -> List[str]:
    """
    Required indicators = lhs_col (enabled) + rhs_col (enabled and rhs_type=COLUMN).
    """
    req: Set[str] = set()
    for c in conditions:
        if not getattr(c, "enabled", False):
            continue

        lhs = str(getattr(c, "lhs_col", "")).strip()
        if lhs:
            req.add(lhs)

        rhs_type = str(getattr(c, "rhs_type", "")).strip().upper()
        if rhs_type == "COLUMN":
            rhs_col = str(getattr(c, "rhs_col", "")).strip()
            if rhs_col:
                req.add(rhs_col)

    return sorted(req)


def load_strategy(path: str) -> Strategy:
    p = Path(path)
    conds = load_config_strategy(str(p), sheet_name="CONDITIONS")
    required = compute_required_indicators_from_conditions(conds)
    return Strategy(path=p, conditions=conds, required_indicators=required)


def compute_signal(df: pd.DataFrame, strategy: Strategy) -> pd.Series:
    """
    Usa generate_signals() e mappa position -> LONG/SHORT/NEUTRAL.
    """
    out = generate_signals(df, strategy.conditions, reverse_immediate=True, exit_priority=True)

    if "position" not in out.columns:
        raise ValueError("generate_signals non ha prodotto la colonna 'position'.")

    pos = pd.to_numeric(out["position"], errors="coerce").fillna(0).astype(int)
    sig = pos.map({1: "LONG", -1: "SHORT", 0: "NEUTRAL"}).astype(str)

    sig.index = df.index
    return sig


# ============================================================
# UTILITIES CLI
# ============================================================
def graceful_exit(message: str, code: int = 0) -> None:
    print(message)
    raise SystemExit(code)


def list_files(directory: Path, exts: Optional[Sequence[str]] = None) -> List[Path]:
    if not directory.exists() or not directory.is_dir():
        graceful_exit(f"ERRORE: directory non valida o inesistente: {directory}", code=2)

    files = [p for p in directory.iterdir() if p.is_file()]
    if exts:
        exts_norm = {e.lower().lstrip(".") for e in exts}
        files = [p for p in files if p.suffix.lower().lstrip(".") in exts_norm]

    files.sort(key=lambda p: p.name.lower())
    return files


def select_from_menu(prompt: str, options: Sequence[Path]) -> Path:
    if not options:
        graceful_exit("ERRORE: nessun file disponibile per la selezione.", code=2)

    while True:
        print("\n" + prompt)
        for i, p in enumerate(options, start=1):
            print(f"  {i}. {p.name}")

        choice = input("Seleziona un numero: ").strip()
        if not choice.isdigit():
            print("Input non valido. Inserisci un numero.")
            continue

        idx = int(choice)
        if not (1 <= idx <= len(options)):
            print("Scelta fuori range.")
            continue

        return options[idx - 1]


# ============================================================
# LOADING KPI
# ============================================================
def load_kpi_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        graceful_exit(f"ERRORE: file KPI non trovato: {path}", code=2)

    df = pd.read_csv(path, sep=CSV_SEP, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    required_base = {"date", "time", "close"}
    missing_base = [c for c in required_base if c not in df.columns]
    if missing_base:
        graceful_exit(
            f"ERRORE: il file KPI non contiene le colonne minime richieste: {missing_base}",
            code=2,
        )

    # close numerico (supporta virgola decimale)
    df["close"] = df["close"].astype(str).str.replace(",", ".", regex=False).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if df["close"].isna().any():
        bad = int(df["close"].isna().sum())
        graceful_exit(
            f"ERRORE: colonna 'close' non numerica per {bad} righe. Correggi il CSV KPI.",
            code=2,
        )

    # datetime = date + " " + time
    df["datetime"] = pd.to_datetime(
        df["date"].astype(str).str.strip() + " " + df["time"].astype(str).str.strip(),
        errors="coerce",
    )
    if df["datetime"].isna().any():
        bad = int(df["datetime"].isna().sum())
        graceful_exit(
            f"ERRORE: impossibile calcolare 'datetime' per {bad} righe (controlla date/time).",
            code=2,
        )

    # Ordinamento deterministico (migliore per file multi-strumento)
    if "symbol" in df.columns and "isin" in df.columns:
        df = df.sort_values(by=["symbol", "isin", "datetime"], ascending=True).reset_index(drop=True)
    else:
        df = df.sort_values(by=["datetime"], ascending=True).reset_index(drop=True)

    return df


def has_group_keys(df: pd.DataFrame) -> bool:
    return ("symbol" in df.columns) and ("isin" in df.columns)


def iter_groups(df: pd.DataFrame) -> Iterable[Tuple[Tuple[str, str], pd.DataFrame]]:
    if has_group_keys(df):
        for (sym, isin), g in df.groupby(["symbol", "isin"], sort=False):
            yield (str(sym), str(isin)), g
    else:
        yield ("__ALL__", "__ALL__"), df


# ============================================================
# VALIDAZIONE INDICATORI
# ============================================================
def validate_required_indicators(df: pd.DataFrame, required_indicators: Sequence[str]) -> List[str]:
    cols = set(df.columns)
    return [c for c in required_indicators if c not in cols]

def diagnose_strategy_xlsx(path_xlsx: Path, sheet_name: str = "CONDITIONS") -> List[str]:
    try:
        df = pd.read_excel(path_xlsx, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        return [f"Impossibile leggere Excel: {e!r}"]

    needed = {"id", "enabled", "rhs_type", "rhs_value", "rhs_col", "operator"}
    missing = sorted(needed - set(df.columns))
    if missing:
        return [f"Excel non conforme: mancano colonne {missing}"]

    def norm(x: Any) -> str:
        if x is None:
            return ""
        s = str(x)
        if s.lower() == "nan":
            return ""
        return s.strip()

    def to_bool(x: Any) -> bool:
        s = norm(x).upper()
        return s in {"TRUE", "VERO", "1", "YES", "Y"}

    issues: List[str] = []

    for _, row in df.iterrows():
        cid = norm(row.get("id"))
        if not cid:
            continue

        # DEBUG mirato su C001 (temporaneo)
        if cid == "C001":
            raw_rhs_col = row.get("rhs_col")
            print("DEBUG C001 rhs_col RAW repr:", repr(raw_rhs_col))

        enabled = to_bool(row.get("enabled"))
        if not enabled:
            continue

        rhs_type = norm(row.get("rhs_type")).upper()
        rhs_col = norm(row.get("rhs_col"))
        rhs_value = norm(row.get("rhs_value"))
        op = norm(row.get("operator"))

        # ... qui sotto restano le tue regole di diagnosi (LIST/COLUMN/VALUE) ...

    return issues


    # Se manca qualche colonna, segnala e basta
    needed = {"id", "enabled", "rhs_type", "rhs_value", "rhs_col", "operator"}
    missing = sorted(needed - set(df.columns))
    if missing:
        return [f"Excel non conforme: mancano colonne {missing}"]

    def norm(x: Any) -> str:
        if x is None:
            return ""
        s = str(x)
        if s.lower() == "nan":
            return ""
        return s.strip()

    def to_bool(x: Any) -> bool:
        s = norm(x).upper()
        return s in {"TRUE", "VERO", "1", "YES", "Y"}

    issues: List[str] = []

    for _, row in df.iterrows():
        cid = norm(row.get("id"))
        if not cid:
            continue

        enabled = to_bool(row.get("enabled"))
        if not enabled:
            continue

        rhs_type = norm(row.get("rhs_type")).upper()
        rhs_col = norm(row.get("rhs_col"))
        rhs_value = norm(row.get("rhs_value"))
        op = norm(row.get("operator"))

        # Regole coerenti con il tuo engine:
        if rhs_type == "LIST":
            if rhs_col != "":
                issues.append(
                    f"[{cid}] rhs_type=LIST ma rhs_col='{rhs_col}' (deve essere vuoto)"
                )
            if rhs_value == "":
                issues.append(
                    f"[{cid}] rhs_type=LIST ma rhs_value è vuoto (richiesto, formato '(a,b,...)')"
                )
            elif not (rhs_value.startswith("(") and rhs_value.endswith(")")):
                issues.append(
                    f"[{cid}] rhs_type=LIST ma rhs_value='{rhs_value}' non è nel formato '(a,b,...)'"
                )
            if op == "between":
                # tra parentesi devono esserci 2 elementi
                inner = rhs_value[1:-1].strip() if (rhs_value.startswith("(") and rhs_value.endswith(")")) else ""
                parts = [p.strip() for p in inner.split(",") if p.strip()] if inner else []
                if len(parts) != 2:
                    issues.append(
                        f"[{cid}] operator=between richiede LIST con 2 elementi (min,max), trovato: {rhs_value}"
                    )

        elif rhs_type == "COLUMN":
            if rhs_col == "":
                issues.append(
                    f"[{cid}] rhs_type=COLUMN ma rhs_col è vuoto (obbligatorio)"
                )
            if rhs_value != "":
                issues.append(
                    f"[{cid}] rhs_type=COLUMN ma rhs_value='{rhs_value}' non deve essere valorizzato"
                )

        elif rhs_type == "VALUE":
            if rhs_value == "":
                issues.append(
                    f"[{cid}] rhs_type=VALUE ma rhs_value è vuoto (obbligatorio)"
                )
            if rhs_col != "":
                issues.append(
                    f"[{cid}] rhs_type=VALUE ma rhs_col='{rhs_col}' (deve essere vuoto)"
                )

        else:
            # rhs_type non riconosciuto (utile per typo in Excel)
            issues.append(
                f"[{cid}] rhs_type='{rhs_type}' non riconosciuto (attesi: VALUE/COLUMN/LIST)"
            )

    return issues


def coerce_required_indicators(df: pd.DataFrame, required_indicators: Sequence[str]) -> pd.DataFrame:
    """
    Converte a numerico (quando possibile) SOLO le colonne richieste dalla strategia.
    Evita confronti stringa-stringa per operatori > < between.
    """
    out = df.copy()
    for col in required_indicators:
        if col not in out.columns:
            continue

        # Normalizza virgola decimale prima di tentare cast
        s = out[col].astype(str).str.replace(",", ".", regex=False).str.strip()

        # errors="ignore" mantiene stringhe quando non è numerico
        out[col] = pd.to_numeric(s, errors="ignore")

    return out


# ============================================================
# APPLY SIGNALS + HOLD/VALUE
# ============================================================
def apply_signals(df: pd.DataFrame, strategy: Strategy) -> pd.DataFrame:
    df_out = df.copy()
    signal_all = pd.Series(index=df_out.index, dtype="object")

    for (_gk1, _gk2), g in iter_groups(df_out):
        s = compute_signal(g.copy(), strategy)

        if not isinstance(s, pd.Series):
            graceful_exit("ERRORE: compute_signal non ha restituito una pandas.Series.", code=2)
        if len(s) != len(g):
            graceful_exit(
                f"ERRORE: compute_signal ha restituito {len(s)} valori ma il gruppo ha {len(g)} righe.",
                code=2,
            )

        s = s.astype(str).str.strip().str.upper()
        bad = sorted(set(s.unique()) - ALLOWED_SIGNALS)
        if bad:
            graceful_exit(
                f"ERRORE: compute_signal ha prodotto valori non ammessi: {bad}. "
                f"Valori ammessi: {sorted(ALLOWED_SIGNALS)}",
                code=2,
            )

        signal_all.loc[g.index] = s.values

    df_out["SIGNAL"] = signal_all
    return df_out


def apply_hold_value(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    """
    HOLD binario con logica evento:
      - LONG  => HOLD diventa IN (se non già IN)
      - SHORT => HOLD diventa OUT (se non già OUT)
      - NEUTRAL => nessun cambio (HOLD resta invariato)

    VALUE:
      - valorizzata SOLO quando HOLD cambia (evento)
      - VALUE = close della stessa riga dell'evento
      - altrimenti vuota (NaN)
    """
    df_out = df.copy()
    df_out["HOLD"] = HOLD_OUT
    df_out["VALUE"] = math.nan  # mantiene dtype float

    entry_in = 0
    exit_out = 0

    for (_gk1, _gk2), g in iter_groups(df_out):
        idx = list(g.index)

        hold_state = HOLD_OUT
        hold_col: List[str] = []
        value_col: List[float] = []

        for i in idx:
            sig = str(df_out.at[i, "SIGNAL"]).upper()
            close = float(df_out.at[i, "close"])

            v = math.nan  # default: cella vuota

            if sig == "LONG":
                if hold_state != HOLD_IN:
                    hold_state = HOLD_IN
                    entry_in += 1
                    v = close  # evento: entry

            elif sig == "SHORT":
                if hold_state != HOLD_OUT:
                    hold_state = HOLD_OUT
                    exit_out += 1
                    v = close  # evento: exit

            # NEUTRAL: nessun cambio

            hold_col.append(hold_state)
            value_col.append(v)

        df_out.loc[idx, "HOLD"] = hold_col
        df_out.loc[idx, "VALUE"] = value_col

    # forza numerico e mantiene NaN per celle vuote
    df_out["VALUE"] = pd.to_numeric(df_out["VALUE"], errors="coerce")

    return df_out, entry_in, exit_out




# ============================================================
# OUTPUT + SUMMARY
# ============================================================
def build_output_path(kpi_path: Path, output_dir: Optional[Path] = None) -> Path:
    out_dir = output_dir if output_dir is not None else kpi_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    name = kpi_path.name
    out_name = ("SIGNAL_" + name) if not name.upper().startswith("SIGNAL_") else name
    return out_dir / out_name


def write_output_csv(df: pd.DataFrame, out_path: Path) -> None:
    df.to_csv(
        out_path,
        sep=CSV_SEP,
        index=False,
        decimal=",",          # virgola decimale
        na_rep="",            # NaN => cella vuota
        float_format="%.3f",  # se vuoi 3 decimali come 26,355
    )





def print_summary(df: pd.DataFrame, entry_in: int, exit_out: int) -> None:
    total = len(df)
    counts = df["SIGNAL"].value_counts(dropna=False).to_dict()
    long_n = int(counts.get("LONG", 0))
    short_n = int(counts.get("SHORT", 0))
    neutral_n = int(counts.get("NEUTRAL", 0))

    print("\n===== RIEPILOGO =====")
    print(f"Righe elaborate: {total}")
    print(f"Conteggi SIGNAL: LONG={long_n}  SHORT={short_n}  NEUTRAL={neutral_n}")
    print(f"Cambi HOLD OUT->IN: {entry_in}")
    print(f"Cambi HOLD IN->OUT: {exit_out}")




# ============================================================
# FLOW INTERATTIVO
# ============================================================
@dataclass(frozen=True)
class Selection:
    kpi_path: Path
    strategy_path: Path


def interactive_selection() -> Selection:
    kpi_files = list_files(TEST_DATA_DIR, exts=["csv"])
    kpi_path = select_from_menu("Su quale file vuoi testare la strategia?", kpi_files)

    # carica KPI una sola volta; se mancano indicatori riparte solo dalla strategia
    df_kpi = load_kpi_csv(kpi_path)

    # Strategie: filtra per .xlsx visto il tuo engine
    strategy_files = list_files(STRATEGY_DIR, exts=["xlsx"])

    while True:
        strategy_path = select_from_menu("Quale strategia vuoi utilizzare?", strategy_files)

        try:
            strategy = load_strategy(str(strategy_path))
        except ValueError as e:
            print("\nSTRATEGIA NON VALIDA (validazione engine):")
            print(str(e))

            # Diagnostica aggiuntiva (lista errori potenziali nel foglio)
            issues = diagnose_strategy_xlsx(strategy_path, sheet_name="CONDITIONS")
            if issues:
                print("\nDettaglio problemi trovati nel file Excel:")
                for msg in issues[:50]:
                    print(" - " + msg)
                if len(issues) > 50:
                    print(f" ... ({len(issues) - 50} altri problemi non mostrati)")
            print("\nRipeti la selezione della strategia (punto 2).")
            continue

        missing = validate_required_indicators(df_kpi, strategy.required_indicators)

        if missing:
            print(
                "\nATTENZIONE IL FILE CONFIG_STRATEGY RICHIAMA INDICATORI NON PRESENTI: "
                + ", ".join(missing)
            )
            print("Ripeti la selezione della strategia (punto 2).")
            continue

        return Selection(kpi_path=kpi_path, strategy_path=strategy_path)


def run_once(selection: Selection, output_dir: Optional[Path] = None) -> None:
    df = load_kpi_csv(selection.kpi_path)
    strategy = load_strategy(str(selection.strategy_path))

    # MUST: cast indicatori richiesti per confronti numerici corretti
    df = coerce_required_indicators(df, strategy.required_indicators)

    df = apply_signals(df, strategy)
    df, entry_in, exit_out = apply_hold_value(df)


    out_path = build_output_path(selection.kpi_path, output_dir=output_dir)
    write_output_csv(df, out_path)

    print(f"\nOutput scritto: {out_path}")
    print_summary(df, entry_in, exit_out)


def main() -> None:
    ensure_engine_available()
    try:
        selection = interactive_selection()
        run_once(selection, output_dir=None)
    except KeyboardInterrupt:
        print("\nInterruzione richiesta (Ctrl+C). Uscita senza errori.")
        raise SystemExit(0)


if __name__ == "__main__":
    main()

