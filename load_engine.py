from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd


# -----------------------------
# Data model
# -----------------------------
@dataclass(frozen=True)
class Condition:
    id: str
    enabled: bool
    scope: str          # REGIME / ENTRY / EXIT
    side: str           # LONG / SHORT / BOTH
    group: str          # e.g., G0, G1
    logic: str          # AND / OR (within group)
    lhs_col: str
    operator: str       # == != > < >= <= in between cross_above cross_below (if supported)
    rhs_type: str       # VALUE / COLUMN / LIST
    rhs_value: Any
    rhs_col: str
    shift: int
    negate: bool
    notes: str


# -----------------------------
# Loader + validation
# -----------------------------
REQUIRED_COLUMNS = [
    "id", "enabled", "scope", "side", "group", "logic",
    "lhs_col", "operator", "rhs_type", "rhs_value", "rhs_col",
    "shift", "negate", "notes"
]

ALLOWED_SCOPE = {"REGIME", "ENTRY", "EXIT"}
ALLOWED_SIDE = {"LONG", "SHORT", "BOTH"}
ALLOWED_LOGIC = {"AND", "OR"}
ALLOWED_RHS_TYPE = {"VALUE", "COLUMN", "LIST"}

# Minimal operator set. Add more only when engine supports them.
ALLOWED_OPERATOR = {"==", "!=", ">", "<", ">=", "<=", "in", "between", "cross_above", "cross_below"}


def load_config_strategy(path_xlsx: str, sheet_name: str = "CONDITIONS") -> List[Condition]:
    """
    Reads config_strategy.xlsx and returns enabled conditions as Condition objects.
    Disabled rows are loaded too (optional), but typically filtered later.
    """
    df = pd.read_excel(path_xlsx, sheet_name=sheet_name, dtype=str)

    # Ensure required columns exist
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in '{sheet_name}': {missing}")

    # Normalize booleans
    def to_bool(x: Any) -> bool:
        if x is None:
            return False
        s = str(x).strip().upper()
        if s in {"TRUE", "VERO", "1", "YES", "Y"}:
            return True
        if s in {"FALSE", "FALSO", "0", "NO", "N", ""}:
            return False
        raise ValueError(f"Invalid boolean value: {x}")

    # Normalize ints
    def to_int(x: Any, default: int = 0) -> int:
        if x is None or str(x).strip() == "":
            return default
        return int(float(str(x).strip()))

    # Parse LIST formats like "(1,-1)" or "(A,B,C)"
    def parse_list(s: str) -> List[str]:
        t = str(s).strip()
        if not (t.startswith("(") and t.endswith(")")):
            raise ValueError(f"LIST rhs_value must be in format '(a,b,...)'. Got: {s}")
        inner = t[1:-1].strip()
        if inner == "":
            return []
        return [x.strip() for x in inner.split(",")]

    # Uniqueness of id
    ids = df["id"].astype(str).str.strip()
    if ids.duplicated().any():
        dups = ids[ids.duplicated()].unique().tolist()
        raise ValueError(f"Duplicate condition ids found: {dups}")

    conditions: List[Condition] = []
    for _, row in df.iterrows():
        cid = str(row["id"]).strip()
        if cid == "" or cid.lower() == "nan":
            continue  # skip empty rows

        enabled = to_bool(row["enabled"])
        scope = str(row["scope"]).strip().upper()
        side = str(row["side"]).strip().upper()
        group = str(row["group"]).strip().upper()
        logic = str(row["logic"]).strip().upper()

        lhs_col = str(row["lhs_col"]).strip()
        operator = str(row["operator"]).strip()
        rhs_type = str(row["rhs_type"]).strip().upper()
        rhs_value_raw = row["rhs_value"]

        rhs_col_raw = row["rhs_col"]
        rhs_col = "" if pd.isna(rhs_col_raw) else str(rhs_col_raw).strip()

        shift = to_int(row["shift"], 0)
        negate = to_bool(row["negate"])
        notes = "" if row["notes"] is None else str(row["notes"])

        # Basic validations (fail fast)
        if scope not in ALLOWED_SCOPE:
            raise ValueError(f"[{cid}] invalid scope: {scope}")
        if side not in ALLOWED_SIDE:
            raise ValueError(f"[{cid}] invalid side: {side}")
        if logic not in ALLOWED_LOGIC:
            raise ValueError(f"[{cid}] invalid logic: {logic}")
        if rhs_type not in ALLOWED_RHS_TYPE:
            raise ValueError(f"[{cid}] invalid rhs_type: {rhs_type}")
        if operator not in ALLOWED_OPERATOR:
            raise ValueError(f"[{cid}] operator '{operator}' not allowed/implemented")

        if group == "" or group.lower() == "nan":
            raise ValueError(f"[{cid}] group cannot be empty")
        if lhs_col == "" or lhs_col.lower() == "nan":
            raise ValueError(f"[{cid}] lhs_col cannot be empty")
        if shift < 0:
            raise ValueError(f"[{cid}] shift must be >= 0")

        # RHS coherence
        if rhs_type == "VALUE":
            if rhs_value_raw is None or str(rhs_value_raw).strip() == "" or str(rhs_value_raw).lower() == "nan":
                raise ValueError(f"[{cid}] rhs_value required for rhs_type=VALUE")
            if rhs_col not in {"", "nan", "None"}:
                # rhs_col must be empty if rhs_type=VALUE
                if rhs_col.strip().lower() not in {"", "nan", "none"}:
                    raise ValueError(f"[{cid}] rhs_col must be empty for rhs_type=LIST")

            rhs_value = rhs_value_raw  # keep as string for now; cast later using KPI dtypes
        elif rhs_type == "COLUMN":
            if rhs_col == "" or rhs_col.lower() == "nan":
                raise ValueError(f"[{cid}] rhs_col required for rhs_type=COLUMN")
            rhs_value = None
        else:  # LIST
            if rhs_value_raw is None or str(rhs_value_raw).strip() == "" or str(rhs_value_raw).lower() == "nan":
                raise ValueError(f"[{cid}] rhs_value required for rhs_type=LIST")
            if rhs_col.strip() != "":
                raise ValueError(f"[{cid}] rhs_col must be empty for rhs_type=LIST")
            rhs_value = parse_list(str(rhs_value_raw))

        conditions.append(Condition(
            id=cid,
            enabled=enabled,
            scope=scope,
            side=side,
            group=group,
            logic=logic,
            lhs_col=lhs_col,
            operator=operator,
            rhs_type=rhs_type,
            rhs_value=rhs_value,
            rhs_col=rhs_col,
            shift=shift,
            negate=negate,
            notes=notes,
        ))

    return conditions


# -----------------------------
# Condition evaluation helpers
# -----------------------------
def _cast_rhs_value(rhs_value: Any, lhs_series: pd.Series) -> Any:
    """
    Cast rhs_value to the dtype of lhs_series when possible.
    Keeps strings as-is if casting fails.
    """
    # If list: cast each element
    if isinstance(rhs_value, list):
        return [_cast_rhs_value(x, lhs_series) for x in rhs_value]

    s = str(rhs_value).strip()

    # Try numeric casting if lhs is numeric-like
    if pd.api.types.is_numeric_dtype(lhs_series.dtype):
        try:
            # int if looks like int
            if s.lstrip("-").isdigit():
                return int(s)
            return float(s)
        except Exception:
            return rhs_value

    # Boolean casting if lhs is boolean
    if pd.api.types.is_bool_dtype(lhs_series.dtype):
        u = s.upper()
        if u in {"TRUE", "VERO", "1"}:
            return True
        if u in {"FALSE", "FALSO", "0"}:
            return False
        return rhs_value

    # Otherwise keep as string
    return s


def eval_condition_at(df: pd.DataFrame, i: int, cond: Condition) -> bool:
    """
    Evaluate a single condition at bar index i (integer position).
    Applies shift and negate. Does NOT handle enabled filtering (caller decides).
    """
    ii = i - cond.shift
    if ii < 0:
        return False

    if cond.lhs_col not in df.columns:
        raise KeyError(f"[{cond.id}] lhs_col not found in df: {cond.lhs_col}")

    lhs_series = df[cond.lhs_col]
    lhs = lhs_series.iloc[ii]

    # RHS resolution
    if cond.rhs_type == "COLUMN":
        if cond.rhs_col not in df.columns:
            raise KeyError(f"[{cond.id}] rhs_col not found in df: {cond.rhs_col}")
        rhs = df[cond.rhs_col].iloc[ii]
    elif cond.rhs_type == "LIST":
        rhs = _cast_rhs_value(cond.rhs_value, lhs_series)
    else:  # VALUE
        rhs = _cast_rhs_value(cond.rhs_value, lhs_series)

    op = cond.operator

    # Evaluate
    if op == "==":
        res = (lhs == rhs)
    elif op == "!=":
        res = (lhs != rhs)
    elif op == ">":
        res = (lhs > rhs)
    elif op == "<":
        res = (lhs < rhs)
    elif op == ">=":
        res = (lhs >= rhs)
    elif op == "<=":
        res = (lhs <= rhs)
    elif op == "in":
        if not isinstance(rhs, list):
            raise ValueError(f"[{cond.id}] operator 'in' requires rhs_type=LIST")
        res = lhs in rhs
    elif op == "between":
        # between expects rhs like [min, max]
        if not isinstance(rhs, list) or len(rhs) != 2:
            raise ValueError(f"[{cond.id}] operator 'between' requires rhs_type=LIST with 2 elements (min,max)")
        lo, hi = rhs[0], rhs[1]
        res = (lhs >= lo) and (lhs <= hi)
    elif op in {"cross_above", "cross_below"}:
        # Requires previous bar (after shift)
        if ii - 1 < 0:
            res = False
        else:
            lhs_prev = lhs_series.iloc[ii - 1]
            if cond.rhs_type == "COLUMN":
                rhs_series = df[cond.rhs_col]
                rhs_prev = rhs_series.iloc[ii - 1]
                rhs_now = df[cond.rhs_col].iloc[ii]
            else:
                # VALUE only for cross against a level
                rhs_prev = rhs
                rhs_now = rhs

            if op == "cross_above":
                res = (lhs_prev <= rhs_prev) and (lhs > rhs_now)
            else:
                res = (lhs_prev >= rhs_prev) and (lhs < rhs_now)
    else:
        raise ValueError(f"[{cond.id}] Unsupported operator: {op}")

    if cond.negate:
        res = not bool(res)

    return bool(res)


# -----------------------------
# Group evaluation (AND/OR within group, OR across groups)
# -----------------------------
def eval_scope_side(df: pd.DataFrame, i: int, conditions: List[Condition], scope: str, side: str) -> bool:
    """
    Evaluate conditions for (scope, side) using:
    - AND/OR within each group (group.logic)
    - OR across groups (implicit)
    - Only enabled conditions are considered
    """
    relevant = [c for c in conditions if c.enabled and c.scope == scope and (c.side == side or c.side == "BOTH")]
    if not relevant:
        return False

    # Group them
    groups: Dict[str, List[Condition]] = {}
    for c in relevant:
        groups.setdefault(c.group, []).append(c)

    # Evaluate each group
    group_results: List[bool] = []
    for gid, conds in groups.items():
        # Enforce single logic per group (use the first row logic)
        glogic = conds[0].logic
        if any(cc.logic != glogic for cc in conds):
            raise ValueError(f"Inconsistent 'logic' within group {gid} for scope={scope}, side={side}")

        vals = [eval_condition_at(df, i, cc) for cc in conds]

        if glogic == "AND":
            g_res = all(vals)
        else:
            g_res = any(vals)

        group_results.append(g_res)

    # OR across groups
    return any(group_results)


# -----------------------------
# Signal generation (reverse immediate)
# -----------------------------
def generate_signals(
    df: pd.DataFrame,
    conditions: List[Condition],
    reverse_immediate: bool = True,
    exit_priority: bool = True,
) -> pd.DataFrame:
    """
    Generates a signal/position series.
    Returns df copy with columns:
      - regime_ok (bool)
      - entry_long (bool)
      - entry_short (bool)
      - exit_long (bool)
      - exit_short (bool)
      - signal (int: +1 buy, -1 sell, 0 none)  (signal as "action", not target position)
      - position (int: -1 short, 0 flat, +1 long)
    """
    out = df.copy()

    n = len(out)
    pos = 0

    regime_ok = []
    entry_long = []
    entry_short = []
    exit_long = []
    exit_short = []
    signal = []
    position = []

    for i in range(n):
        reg = eval_scope_side(out, i, conditions, scope="REGIME", side="BOTH")
        regime_ok.append(reg)

        el = eval_scope_side(out, i, conditions, scope="ENTRY", side="LONG") if reg else False
        es = eval_scope_side(out, i, conditions, scope="ENTRY", side="SHORT") if reg else False
        entry_long.append(el)
        entry_short.append(es)

        xl = eval_scope_side(out, i, conditions, scope="EXIT", side="LONG")
        xs = eval_scope_side(out, i, conditions, scope="EXIT", side="SHORT")
        exit_long.append(xl)
        exit_short.append(xs)

        act = 0

        # EXIT first (priority) when in position
        if exit_priority and pos != 0:
            if pos == 1 and xl:
                act = -1 if reverse_immediate else 0
                pos = -1 if reverse_immediate else 0
            elif pos == -1 and xs:
                act = 1 if reverse_immediate else 0
                pos = 1 if reverse_immediate else 0

        # ENTRY only if flat
        if pos == 0 and reg:
            if el and not es:
                act = 1
                pos = 1
            elif es and not el:
                act = -1
                pos = -1
            # if both True: ignore (ambiguous). Alternative: priority rule.

        # If not exit_priority, you can choose different ordering (not recommended).
        signal.append(act)
        position.append(pos)

    out["regime_ok"] = regime_ok
    out["entry_long"] = entry_long
    out["entry_short"] = entry_short
    out["exit_long"] = exit_long
    out["exit_short"] = exit_short
    out["signal"] = signal
    out["position"] = position
    return out


# -----------------------------
# Example usage in main program
# -----------------------------
if __name__ == "__main__":
    # 1) Load KPI data (already computed indicators)
    # df_kpi = pd.read_csv("KPI_VANECK.csv", parse_dates=["datetime"]).sort_values("datetime").set_index("datetime")
    # 2) Load config
    conds = load_config_strategy("config_strategy.xlsx", sheet_name="CONDITIONS")

    # 3) Validate columns exist in your df before running:
    # missing_cols = sorted({c.lhs_col for c in conds if c.enabled} - set(df_kpi.columns))
    # if missing_cols: raise ValueError(f"KPI missing columns: {missing_cols}")

    # 4) Generate signals
    # out = generate_signals(df_kpi, conds, reverse_immediate=True, exit_priority=True)
    # out.to_csv("signals.csv")
    pass
