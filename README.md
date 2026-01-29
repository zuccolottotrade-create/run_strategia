Ecco una **README pronta** (Markdown) per il tuo modulo. Include scopo, requisiti, uso, output, e un **diagramma a blocchi** (Mermaid). Copia/incolla in `README_run_strategy.md` (o `README.md` nella cartella del modulo).

````md
# Run_strategy (runner interattivo per applicare una strategia ai KPI)

Questo modulo esegue un flusso end-to-end per:
1) caricare un file KPI CSV,
2) caricare una strategia (Excel .xlsx) basata su condizioni,
3) generare la colonna `SIGNAL` tramite l’engine (`load_engine.py`),
4) derivare `HOLD` e `VALUE` (eventi IN/OUT) a partire da `SIGNAL`,
5) esportare un CSV di output prefissato con `SIGNAL_`.

È pensato come **tool di test** (CLI interattiva) per verificare rapidamente il comportamento di una strategia su uno o più dataset KPI.

---

## Dipendenze e prerequisiti

- Python 3.10+ (consigliato)
- `pandas`
- File `load_engine.py` presente **nello stesso livello** del file `run_strategy.py` (o `run_strategia.py`)
- Cartelle configurate nel codice:
  - `TEST_DATA_DIR`: directory con i KPI CSV
  - `STRATEGY_DIR`: directory con le strategie Excel (`.xlsx`)
- Formato CSV KPI richiesto:
  - colonne minime: `date`, `time`, `close`
  - opzionali (per multi-strumento): `symbol`, `isin`
- Il separatore CSV atteso è `;` (`CSV_SEP=";"`)

Installazione dipendenze:
```bash
pip install pandas openpyxl
````

> Nota: `openpyxl` serve per leggere i file Excel con `pd.read_excel()`.

---

## Struttura input

### KPI CSV

Il CSV deve contenere almeno:

* `date` (stringa data)
* `time` (stringa orario)
* `close` (prezzo di chiusura; supporta virgola decimale)

Opzionale (consigliato se multi-strumento):

* `symbol`
* `isin`

Il modulo costruisce una colonna `datetime` come `date + time` e ordina i dati:

* se presenti `symbol` e `isin`: sort per `symbol`, `isin`, `datetime`
* altrimenti: sort per `datetime`

### Strategia Excel (.xlsx)

Il file strategia deve essere leggibile dall’engine e contenere il foglio:

* `CONDITIONS`

Il modulo calcola automaticamente `required_indicators` in base alle condizioni abilitate:

* include `lhs_col`
* include `rhs_col` quando `rhs_type="COLUMN"`

---

## Output

Il modulo produce un CSV in output nella stessa cartella del KPI (salvo override), con nome:

* se input è `KPI.csv` → output `SIGNAL_KPI.csv`
* se il file è già prefissato `SIGNAL_...` → mantiene il nome

Colonne aggiunte/gestite:

* `SIGNAL`: valori ammessi `LONG`, `SHORT`, `NEUTRAL`
* `HOLD`: stato evento con logica binaria `IN/OUT`
* `VALUE`: valorizzato **solo quando cambia HOLD**, altrimenti vuoto (NaN)

### Semantica HOLD/VALUE (attuale)

* `SIGNAL=LONG`  → se non già IN: `HOLD` passa a `IN` e `VALUE=close` (evento entry)
* `SIGNAL=SHORT` → se non già OUT: `HOLD` passa a `OUT` e `VALUE=close` (evento exit)
* `SIGNAL=NEUTRAL` → nessun cambio (HOLD invariato, VALUE vuoto)

> Nota: in questa implementazione `SHORT` è trattato come “uscita / flat (OUT)”, non come posizione short reale.

---

## Esecuzione (CLI)

Lancia lo script:

```bash
python run_strategy.py
```

Flusso interattivo:

1. selezioni un file KPI tra quelli presenti in `TEST_DATA_DIR`
2. selezioni una strategia Excel tra quelle presenti in `STRATEGY_DIR`
3. il modulo valida che tutti gli indicatori richiesti dalla strategia esistano nel KPI
4. genera output CSV e stampa un riepilogo a console

---

## Riepilogo a console

A fine esecuzione viene stampato:

* numero righe elaborate
* conteggi di `SIGNAL` (LONG/SHORT/NEUTRAL)
* numero cambi `HOLD` OUT→IN (entry)
* numero cambi `HOLD` IN→OUT (exit)

---

## Diagramma a blocchi (flow)

```mermaid
flowchart TD
    A[Start] --> B[ensure_engine_available()]
    B --> C[Interactive selection]
    C --> C1[List KPI CSV in TEST_DATA_DIR]
    C1 --> C2[User selects KPI file]
    C2 --> C3[Load KPI CSV: validate date/time/close, parse datetime, sort]
    C3 --> C4[List Strategy XLSX in STRATEGY_DIR]
    C4 --> C5[User selects Strategy file]
    C5 --> D[Load strategy via load_engine.py]
    D --> D1[Compute required_indicators from CONDITIONS]
    D1 --> E{All required indicators in KPI?}
    E -- No --> C4
    E -- Yes --> F[Coerce required indicator columns (numeric when possible)]
    F --> G[Apply signals: generate_signals() -> SIGNAL]
    G --> H[Derive HOLD + VALUE events]
    H --> I[Write output CSV: SIGNAL_*.csv]
    I --> J[Print summary]
    J --> K[End]
```

---

## Note tecniche e integrazione con altri moduli

* Il file `SIGNAL_*.csv` può essere usato come base per moduli successivi (es. generazione trades/equity e report).
* La logica `HOLD/VALUE` produce eventi adatti a ricostruire entry/exit; se in futuro vuoi supportare posizioni short reali, la semantica HOLD va estesa a 3 stati (`IN/SHORT/OUT`).

---

## Risoluzione problemi

### Errore import engine

Se vedi:

* “impossibile importare load-engine” / `_IMPORT_ERR`

Verifica:

* `load_engine.py` è nello stesso folder dello script
* i nomi importati esistono: `load_config_strategy`, `generate_signals`, `Condition`

### Indicatori mancanti

Se selezioni una strategia che richiede indicatori non presenti nel KPI, il modulo lo segnala e ti fa ripetere la selezione della strategia.

# Config Strategia (v2) — Specifica e QC

Questo file descrive lo **schema** del file strategia Excel (v2) e come usare la QC-preflight.

---

## 1) Foglio: CONDITIONS

### Colonne obbligatorie
- **id**: identificativo regola (es. C001)
- **enabled**: TRUE/FALSE (accettati: 1/0, yes/no, si/no)
- **scope**: REGIME | ENTRY | EXIT
- **side**: BOTH | LONG | SHORT
- **group**: obbligatorio per ENTRY/EXIT (es. ST_01)
- **logic**: AND (OR accettato ma potrebbe non essere supportato dall’engine)
- **lhs_col**: nome colonna KPI (deve esistere nel CSV KPI)
- **operator**: in | == | != | > | >= | < | <= | between | cross_above | cross_below
- **rhs_type**: LIST | VALUE | COLUMN
- **rhs_value**:
  - VALUE: numero (accetta virgola europea: 12,5)
  - LIST: lista tipo (1,-1) oppure (45,65)
- **rhs_col**: nome colonna KPI (solo se rhs_type=COLUMN)
- **shift**: intero >= 0
- **negate**: TRUE/FALSE
- **notes**: consigliato (audit / debug)

### Vincoli QC (hardening)
- ENTRY/EXIT → **group obbligatorio**
- operator=in → rhs_type=LIST
- operator=between → rhs_type=LIST con **2 valori**
- cross_above/cross_below → rhs_type=COLUMN o VALUE

---

## 2) Foglio: KPI_COLUMNS
Dizionario delle colonne KPI disponibili (aiuta a evitare typo in lhs_col / rhs_col).

---

## 3) QC Preflight
### Metodo A (consigliato): contro KPI reale
- usa un file KPI_*.csv reale per validare che lhs_col/rhs_col esistano.

### Metodo B: usando KPI_COLUMNS (senza KPI)
- valido per controlli di schema e coerenza base.

Output:
- viene generato **QC_<strategy>.csv** con qc_status/qc_message per ogni riga.

---

## 4) Comandi rapidi

### Stand-alone
- `Strategy_QC_Preflight.command`

### Da terminale
- Caso A:
  - `python3 strategy_qc.py --strategy-xlsx <file.xlsx> --kpi-csv <KPI_*.csv>`
- Caso B:
  - `python3 strategy_qc.py --strategy-xlsx <file.xlsx> --use-kpi-columns-sheet`
