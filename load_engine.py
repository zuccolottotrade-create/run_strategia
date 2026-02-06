from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
from pathlib import Path
import ast

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
    operator: str       # == != > < >= <= in between cross_above cross_below
    rhs_type: str       # VALUE / COLUMN / LIST
    rhs_value: Any
    rhs_col: str
    shift: int
    negate: bool
    notes: str = ""


# -----------------------------
# Allowed sets (QC/validation)
# -----------------------------
ALLOWED_SCOPE = {"REGIME", "ENTRY", "EXIT"}
ALLOWED_SIDE = {"LONG", "SHORT", "BOTH"}
ALLOWED_LOGIC = {"AND", "OR"}
ALLOWED_RHS_TYPE = {"VALUE", "COLUMN", "LIST"}

# Minimal operator set. Add more only when engine supports them.
ALLOWED_OPERATOR = {
    "==", "!=", ">", "<", ">=", "<=",
    "in", "between",
    "cross_above", "cross_below"
}


# -----------------------------
# Utils
# -----------------------------
def _clean_str(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def _to_bool_enabled(v) -> bool:
    s = _clean_str(v).lower()
    return s in {"1", "true", "t", "yes", "y", "si", "sì", "vero", "v"}


def _to_bool(v) -> bool:
    s = _clean_str(v).lower()
    return s in {"1", "true", "t", "yes", "y", "si", "sì", "vero", "v"}


def _to_int(v, default: int = 0) -> int:
    s = _clean_str(v)
    if s == "":
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def _parse_list_literal(s: str):
    """
    Converte stringhe tipo "(1,-1)" o "(45,65)" o "[1,2]" in tuple.
    Usa ast.literal_eval (sicuro).
    """
    s = _clean_str(s)
    if s == "":
        return None
    try:
        obj = ast.literal_eval(s)
    except Exception:
        raise ValueError(f"rhs_value LIST non parsabile: {s!r}")

    if isinstance(obj, (list, tuple)):
        return tuple(obj)

    # se arriva un singolo numero/stringa, lo impacchetto
    return (obj,)


def _parse_value(s: str):
    """
    Converte VALUE in int/float quando possibile, altrimenti stringa.
    """
    s = _clean_str(s)
    if s == "":
        return None

    # prova int
    try:
        if s.lstrip("-").isdigit():
            return int(s)
        # caso "3.0"
        f = float(s)
        if float(int(f)) == f:
            return int(f)
    except Exception:
        pass

    # prova float
    try:
        return float(s)
    except Exception:
        return s


def _norm_upper(v) -> str:
    return _clean_str(v).upper()


def _norm_logic(v) -> str:
    s = _clean_str(v).upper()
    return s if s in ALLOWED_LOGIC else "AND"


def _norm_rhs_type(v) -> str:
    s = _clean_str(v).upper()
    if s in {"COL", "COLUMN"}:
        return "COLUMN"
    if s in {"VAL", "VALUE"}:
        return "VALUE"
    if s == "LIST":
        return "LIST"
    return "VALUE"


def _norm_operator(v) -> str:
    """
    Normalizza operatori provenienti da Excel.
    Accetta alias comuni:
      - "=" -> "=="
      - "IN" -> "in"
      - "Between" -> "between"
    """
    s = _clean_str(v)
    if s == "":
        return ""
    s = s.strip()

    if s == "=":
        s = "=="

    # uniforma minuscolo per set letterali
    s_low = s.lower()
    # mantieni simboli così come sono, porta solo i keyword a lower
    if s_low in {"in", "between", "cross_above", "cross_below"}:
        return s_low

    return s  # per == != > < >= <=


def _bool_safe(x) -> bool:
    """
    Convert any value to a safe boolean.
    - None / NaN / pd.NA / "" -> False
    - array / Series / list -> False
    - otherwise: bool(x)
    """
    if x is None:
        return False

    if isinstance(x, str) and x.strip() == "":
        return False

    if isinstance(x, (list, tuple, set, dict, pd.Series, pd.Index)):
        return False

    try:
        import numpy as np
        if isinstance(x, np.ndarray):
            return False
    except Exception:
        pass

    try:
        if pd.isna(x):
            return False
    except Exception:
        pass

    try:
        return bool(x)
    except Exception:
        return False


def _is_missing_scalar(x) -> bool:
    """
    True solo per missing SCALARI (None / NaN / pd.NA / stringa vuota).
    Se x è una lista/array/Series, ritorna False (non è uno scalare).
    """
    if x is None:
        return True

    if isinstance(x, str) and x.strip() == "":
        return True

    if isinstance(x, (list, tuple, set, dict, pd.Series, pd.Index)):
        return False

    try:
        import numpy as np
        if isinstance(x, np.ndarray):
            return False
    except Exception:
        pass

    try:
        return bool(pd.isna(x))
    except Exception:
        return False


# -----------------------------
# Loader
# -----------------------------
def load_config_strategy(path: str, sheet_name: str = "CONDITIONS") -> List[Condition]:
    """
    Carica il foglio CONDITIONS e restituisce una lista di Condition.
    REGOLE:
      - Valida e costruisce SOLO righe enabled=VERO (true).
      - Le righe enabled=FALSO vengono ignorate e non devono generare errori.
      - rhs_type:
          * VALUE  -> rhs_value obbligatorio
          * COLUMN -> rhs_col obbligatorio
          * LIST   -> rhs_value obbligatorio (tuple/list tipo "(1,-1)" o "(45,65)")
    """
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(str(p))

    df = pd.read_excel(p, sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]

    # mappa colonne case-insensitive
    colmap = {c.lower(): c for c in df.columns}

    def _col(name: str) -> str:
        return colmap.get(name.lower(), "")

    # colonne minime
    for req in ("id", "enabled", "lhs_col", "operator", "rhs_type"):
        if _col(req) == "":
            raise ValueError(
                f"Colonna obbligatoria mancante nel foglio {sheet_name}: {req}. "
                f"Colonne trovate: {list(df.columns)}"
            )

    # FILTRO ENABLED (PRIMA delle validazioni hard)
    en_col = _col("enabled")
    df["_enabled_bool"] = df[en_col].apply(_to_bool_enabled)
    df = df[df["_enabled_bool"] == True].copy()

    conds: List[Condition] = []

    for _, row in df.iterrows():
        cid = _clean_str(row[_col("id")])
        if cid == "":
            raise ValueError("[<missing id>] id is required for enabled rows")

        scope = _norm_upper(row[_col("scope")]) if _col("scope") else ""
        side = _norm_upper(row[_col("side")]) if _col("side") else ""
        group = _norm_upper(row[_col("group")]) if _col("group") else ""
        logic = _norm_logic(row[_col("logic")]) if _col("logic") else "AND"

        lhs_col = _clean_str(row[_col("lhs_col")])
        operator = _norm_operator(row[_col("operator")])
        rhs_type = _norm_rhs_type(row[_col("rhs_type")])

        rhs_value_raw = _clean_str(row[_col("rhs_value")]) if _col("rhs_value") else ""
        rhs_col = _clean_str(row[_col("rhs_col")]) if _col("rhs_col") else ""

        shift = _to_int(row[_col("shift")], default=0) if _col("shift") else 0
        negate = _to_bool(row[_col("negate")]) if _col("negate") else False
        notes = _clean_str(row[_col("notes")]) if _col("notes") else ""

        # Required base
        if lhs_col == "":
            raise ValueError(f"[{cid}] lhs_col required")

        if operator == "" or _is_missing_scalar(operator):
            raise ValueError(f"[{cid}] operator required (cell is empty/NaN)")

        # Normalizza scope/side/group quando vuoti: default conservativi
        if scope == "":
            scope = "ENTRY"
        if side == "":
            side = "BOTH"
        if group == "":
            group = "G0"

        # Validate enums (solo su enabled)
        if scope not in ALLOWED_SCOPE:
            raise ValueError(f"[{cid}] invalid scope={scope!r}. Allowed={sorted(ALLOWED_SCOPE)}")
        if side not in ALLOWED_SIDE:
            raise ValueError(f"[{cid}] invalid side={side!r}. Allowed={sorted(ALLOWED_SIDE)}")
        if logic not in ALLOWED_LOGIC:
            raise ValueError(f"[{cid}] invalid logic={logic!r}. Allowed={sorted(ALLOWED_LOGIC)}")
        if rhs_type not in ALLOWED_RHS_TYPE:
            raise ValueError(f"[{cid}] invalid rhs_type={rhs_type!r}. Allowed={sorted(ALLOWED_RHS_TYPE)}")

        # operator validate (case-insensitive for keywords)
        op_norm = operator.lower() if operator.lower() in {"in", "between", "cross_above", "cross_below"} else operator
        if op_norm not in ALLOWED_OPERATOR:
            raise ValueError(f"[{cid}] invalid operator={operator!r}. Allowed={sorted(ALLOWED_OPERATOR)}")
        operator = op_norm

        # rhs validation + parsing
        rhs_value = None
        if rhs_type == "COLUMN":
            if rhs_col == "":
                raise ValueError(f"[{cid}] rhs_col required for rhs_type=COLUMN")
        elif rhs_type == "LIST":
            if rhs_value_raw == "":
                raise ValueError(f"[{cid}] rhs_value required for rhs_type=LIST")
            rhs_value = _parse_list_literal(rhs_value_raw)  # tuple
        else:  # VALUE
            if rhs_value_raw == "":
                raise ValueError(f"[{cid}] rhs_value required for rhs_type=VALUE")
            rhs_value = _parse_value(rhs_value_raw)

        conds.append(
            Condition(
                id=cid,
                enabled=True,
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
            )
        )

    return conds


# -----------------------------
# Condition evaluation helpers
# -----------------------------
def _cast_rhs_value(rhs_value: Any, lhs_series: pd.Series) -> Any:
    """
    Cast rhs_value to the dtype of lhs_series when possible.
    Keeps strings as-is if casting fails.

    NOTE: per LIST in loader usiamo tuple; qui accettiamo tuple/list e normalizziamo a list.
    """
    # If list/tuple: cast each element
    if isinstance(rhs_value, (list, tuple)):
        return [_cast_rhs_value(x, lhs_series) for x in rhs_value]

    s = str(rhs_value).strip()

    # Try numeric casting if lhs is numeric-like
    if pd.api.types.is_numeric_dtype(lhs_series.dtype):
        try:
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
    ii = i - int(cond.shift or 0)
    if ii < 0:
        return False

    # --- LHS ---
    if cond.lhs_col not in df.columns:
        raise KeyError(f"[{cond.id}] lhs_col not found in df: {cond.lhs_col}")

    lhs_series = df[cond.lhs_col]
    lhs = lhs_series.iloc[ii]

    # --- RHS ---
    if cond.rhs_type == "COLUMN":
        if not cond.rhs_col or cond.rhs_col not in df.columns:
            raise KeyError(f"[{cond.id}] rhs_col not found in df: {cond.rhs_col}")
        rhs = df[cond.rhs_col].iloc[ii]

    elif cond.rhs_type == "LIST":
        rhs = _cast_rhs_value(cond.rhs_value, lhs_series)  # list

    else:  # VALUE
        rhs = _cast_rhs_value(cond.rhs_value, lhs_series)

    # --- operator normalization ---
    op = _clean_str(cond.operator)
    op_norm = op.lower()

    if op_norm == "" or op_norm == "nan":
        raise ValueError(f"[{cond.id}] Missing operator (empty/NaN)")

    # --- Evaluate ---
    res: Any = False

    if op_norm == "==":
        if _is_missing_scalar(lhs) or _is_missing_scalar(rhs):
            res = False
        else:
            res = (lhs == rhs)

    elif op_norm == "!=":
        if _is_missing_scalar(lhs) or _is_missing_scalar(rhs):
            res = False
        else:
            res = (lhs != rhs)

    elif op_norm == ">":
        if _is_missing_scalar(lhs) or _is_missing_scalar(rhs):
            res = False
        else:
            res = (lhs > rhs)

    elif op_norm == "<":
        if _is_missing_scalar(lhs) or _is_missing_scalar(rhs):
            res = False
        else:
            res = (lhs < rhs)

    elif op_norm == ">=":
        if _is_missing_scalar(lhs) or _is_missing_scalar(rhs):
            res = False
        else:
            res = (lhs >= rhs)

    elif op_norm == "<=":
        if _is_missing_scalar(lhs) or _is_missing_scalar(rhs):
            res = False
        else:
            res = (lhs <= rhs)

    elif op_norm == "in":
        if not isinstance(rhs, list):
            raise ValueError(f"[{cond.id}] operator 'in' requires rhs_type=LIST")
        if _is_missing_scalar(lhs):
            res = False
        else:
            try:
                res = lhs in rhs
            except TypeError:
                res = False

    elif op_norm == "between":
        if not isinstance(rhs, list) or len(rhs) != 2:
            raise ValueError(
                f"[{cond.id}] operator 'between' requires rhs_type=LIST with 2 elements (min,max)"
            )
        lo, hi = rhs
        if _is_missing_scalar(lhs) or _is_missing_scalar(lo) or _is_missing_scalar(hi):
            res = False
        else:
            res = (lo <= lhs <= hi)

    elif op_norm in {"cross_above", "cross_below"}:
        if ii - 1 < 0:
            res = False
        else:
            lhs_prev = lhs_series.iloc[ii - 1]

            if cond.rhs_type == "COLUMN":
                rhs_series = df[cond.rhs_col]
                rhs_prev = rhs_series.iloc[ii - 1]
                rhs_now = rhs_series.iloc[ii]
            else:
                # VALUE/LIST not supported for cross against list; only VALUE makes sense
                rhs_prev = rhs
                rhs_now = rhs

            if _is_missing_scalar(lhs_prev) or _is_missing_scalar(lhs) or _is_missing_scalar(rhs_prev) or _is_missing_scalar(rhs_now):
                res = False
            else:
                if op_norm == "cross_above":
                    res = (lhs_prev <= rhs_prev) and (lhs > rhs_now)
                else:
                    res = (lhs_prev >= rhs_prev) and (lhs < rhs_now)

    else:
        raise ValueError(f"[{cond.id}] Unsupported operator: {op_norm}")

    # --- negate ---
    if bool(getattr(cond, "negate", False)):
        res = not _bool_safe(res)

    return _bool_safe(res)


# -----------------------------
# Group evaluation
# -----------------------------
    #
def eval_scope_side(df: pd.DataFrame, i: int, conditions: List[Condition], scope: str, side: str) -> bool:
        """
        Evaluate conditions for (scope, side) using:
        - AND/OR within each group (group.logic)
        - OR across groups (implicit)
        - Only enabled conditions are considered

        REGIME rule:
          - If no enabled REGIME conditions exist -> REGIME is OK (True).
        ENTRY/EXIT rule:
          - If no enabled conditions exist -> False (conservative).
        """
        relevant = [
            c for c in conditions
            if c.enabled and c.scope == scope and (c.side == side or c.side == "BOTH")
        ]

        # -----------------------------
        # DEFAULT BEHAVIOR WHEN EMPTY
        # -----------------------------
        if not relevant:
            # Regime always OK if not specified
            if scope == "REGIME":
                return True
            # Entry/Exit default conservative
            return False

        # -----------------------------
        # GROUP EVALUATION (unchanged)
        # -----------------------------
        groups: Dict[str, List[Condition]] = {}
        for c in relevant:
            groups.setdefault(c.group, []).append(c)

        group_results: List[bool] = []
        for gid, conds in groups.items():
            glogic = conds[0].logic
            if any(cc.logic != glogic for cc in conds):
                raise ValueError(f"Inconsistent 'logic' within group {gid} for scope={scope}, side={side}")

            vals = [eval_condition_at(df, i, cc) for cc in conds]
            g_res = all(vals) if glogic == "AND" else any(vals)
            group_results.append(g_res)

        return any(group_results)


# -----------------------------
# Signal generation
# -----------------------------
def generate_signals(
    df: pd.DataFrame,
    conditions: List[Condition],
    reverse_immediate: bool = True,
    exit_priority: bool = True,
    allow_trade_mask=None,
    allow_long_mask=None,
    allow_short_mask=None,
) -> pd.DataFrame:

    """
    Generates a signal/position series.
    Columns:
      - regime_ok (bool)
      - entry_long (bool)
      - entry_short (bool)
      - exit_long (bool)
      - exit_short (bool)
      - signal (int: +1 buy, -1 sell, 0 none)  (azione)
      - position (int: -1 short, 0 flat, +1 long)

    Nota:
      - Con reverse_immediate=True, quando esci da long e contemporaneamente entri short,
        la tua "azione" in un singolo bar idealmente sarebbe doppia (sell to close + sell to open).
        Per non rompere la semantica, qui manteniamo:
          * signal = -1 o +1 come "direzione di cambiamento"
          * position indica il nuovo stato finale.
        Se vuoi la contabilità precisa delle 2 operazioni, conviene introdurre signal2 o qty.
    """
    out = df.copy()
    n = len(out)
    pos = 0

    regime_ok: List[bool] = []
    entry_long: List[bool] = []
    entry_short: List[bool] = []
    exit_long: List[bool] = []
    exit_short: List[bool] = []
    signal: List[int] = []
    position: List[int] = []

    for i in range(n):
        reg = eval_scope_side(out, i, conditions, scope="REGIME", side="BOTH")
        regime_ok.append(reg)

        el = eval_scope_side(out, i, conditions, scope="ENTRY", side="LONG") if reg else False
        es = eval_scope_side(out, i, conditions, scope="ENTRY", side="SHORT") if reg else False

        # --- REGIME_L1 gating (ENTRY only) -------------------------------
        # Nota: blocchiamo SOLO gli ENTRY, mai le EXIT.
        if allow_trade_mask is not None:
            if not bool(allow_trade_mask.iloc[i]):
                el = False
                es = False

        if allow_long_mask is not None:
            if not bool(allow_long_mask.iloc[i]):
                el = False

        if allow_short_mask is not None:
            if not bool(allow_short_mask.iloc[i]):
                es = False
        # ----------------------------------------------------------------


        entry_long.append(el)
        entry_short.append(es)

        xl = eval_scope_side(out, i, conditions, scope="EXIT", side="LONG")
        xs = eval_scope_side(out, i, conditions, scope="EXIT", side="SHORT")
        exit_long.append(xl)
        exit_short.append(xs)

        act = 0

        if i % 200 == 0:
            print(
                f"[DBG i={i}] "
                f"reg={reg} "
                f"EL={el} ES={es} "
                f"XL={xl} XS={xs} "
                f"pos_before={pos}"
            )

        # EXIT first (priority)
        if exit_priority and pos != 0:
            if pos == 1 and xl:
                if reverse_immediate and reg and es and not el:
                    # reverse to short
                    pos = -1
                    act = -1
                else:
                    # close long
                    pos = 0
                    act = -1  # sell to close
            elif pos == -1 and xs:
                if reverse_immediate and reg and el and not es:
                    # reverse to long
                    pos = 1
                    act = 1
                else:
                    # close short
                    pos = 0
                    act = 1  # buy to close

        # ENTRY only if flat
        if pos == 0 and reg:
            if el and not es:
                pos = 1
                act = 1
            elif es and not el:
                pos = -1
                act = -1
            # both True -> ambiguous: ignore

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


if __name__ == "__main__":
    # Example
    conds = load_config_strategy("config_strategy.xlsx", sheet_name="CONDITIONS")
    # df_kpi = pd.read_csv(...)

    # out = generate_signals(df_kpi, conds)
    # out.to_csv("signals.csv", index=False)
    pass

