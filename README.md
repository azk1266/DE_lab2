# F1 Qualifying ETL Pipeline

End-to-end **ETL pipeline** that turns raw Formula 1 CSV data (1950–2024) into a **star-schema MySQL warehouse** for qualifying-session analysis.

The pipeline extracts multi-file source data, applies domain-specific cleaning and business rules, and loads a dimensional model that supports analytical SQL over driver, constructor, circuit, and session performance.

---

## Why this project

Qualifying times live in messy CSVs: inconsistent formats, missing keys, and session outcomes that must be derived from partial timing data. This project builds a **reproducible data pipeline** that:

- Cleans and validates historical F1 records before they reach the warehouse
- Models the domain as dimensions + facts (Kimball-style star schema)
- Produces query-ready metrics for performance trends, team competitiveness, and circuit patterns

That foundation—reliable extract/transform/load, data quality, and analytics-oriented schema design—is the same pattern used in ML feature stores, reporting platforms, and production data workflows.

---

## Tech stack

| Area | Tools |
|------|--------|
| Language | Python 3.8+ |
| Data processing | pandas, NumPy |
| Database | MySQL 5.7+ / 8.0+, SQLAlchemy, PyMySQL |
| Config & ops | python-dotenv, structured logging, resumable batch state |
| Modeling | Dimensional (star) schema: dimensions + fact table |

---

## Architecture

```
CSV sources          Transform                 Warehouse
───────────          ─────────                 ─────────
circuits.csv    ─┐
constructors.csv ├─► clean · map · derive  ──► dim_circuit
drivers.csv     ├─► times · status · FKs   ──► dim_constructor
races.csv       ├─► batch validate         ──► dim_driver
qualifying.csv  ─┘                         ──► dim_date
                                               facts
```

**Modular layout** (extract → transform → load):

| Layer | Responsibility |
|-------|----------------|
| `src/extractors/` | Typed CSV reads, file checks, memory-aware batches |
| `src/transformers/` | Dimension/fact mapping, lap-time parsing, status rules |
| `src/loaders/` | MySQL batch loads, transactions, integrity checks |
| `src/config/` | Environment-based settings (DB, batch size, paths) |
| `src/utils/` | Logging and resumable pipeline state |

Also supports related fact schemas (pit stops, race results) via pluggable transformers.

---

## What the pipeline does

### Extract
- Reads circuits, constructors, drivers, races, and qualifying CSVs
- Enforces expected columns/types and validates source files before processing

### Transform
- **Lap times**: `"1:26.572"` → milliseconds for aggregation and ranking
- **Session status**: derives `OK` / `DNQ` / `DNS` / `DSQ` from which Q1–Q3 times exist
- **Qualifying date**: race date − 1 day into `dim_date`
- **Identity resolution**: builds dimension keys and FK lookups for the fact table
- **Quality rules**: skip rows missing critical FKs; default unknown circuit; keep NULL times when absent

### Load
- Truncates/reloads into a MySQL star schema
- Batch inserts with configurable size and optional resume from last position
- Optional post-load integrity validation

---

## Skills demonstrated

| Skill | How it shows up here |
|-------|----------------------|
| **Python data engineering** | Orchestrated ETL (`run_etl.py`), typed modules, CLI flags |
| **pandas / NumPy** | Cleaning, joins, vectorized transforms on multi-file datasets |
| **Dimensional modeling** | `dim_*` + `facts` designed for slice-and-dice analytics |
| **SQL & relational DBs** | MySQL schema, FK design, analytical query patterns |
| **Data quality** | Validation rules, skip/default policies, integrity checks |
| **Production habits** | `.env` config, logging, batching, resumable state, error handling |
| **Domain logic** | F1 qualifying rules turned into deterministic transform code |
| **Analytics mindset** | Schema and sample queries aimed at real performance questions |

---

## Quick start

### Prerequisites
- Python 3.8+
- MySQL 5.7+ or 8.0+
- ~4GB RAM (8GB recommended for full runs)

### 1. Create the schema
```bash
mysql -u <user> -p < schemas/qualification_sessions_schema.sql
```

### 2. Configure
Copy or create a `.env` with your DB credentials and options:

```env
DATABASE_HOST=127.0.0.1
DATABASE_PORT=3306
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
DATABASE_NAME=f1_qlf_db
BATCH_SIZE=10000
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run
```bash
# Smoke test on a sample
python run_etl.py --sample-size 100

# Full pipeline
python run_etl.py
```

---

## Project structure

```
.
├── run_etl.py                 # Pipeline entry point & CLI
├── requirements.txt
├── .env                       # Local config (not committed)
├── schemas/                   # MySQL DDL (qualifying, pit stop, race results)
├── data/                      # Source CSVs
├── src/
│   ├── config/                # Settings from environment
│   ├── extractors/            # CSV extraction
│   ├── transformers/          # Dimensions + facts
│   ├── loaders/               # MySQL loading
│   └── utils/                 # Logging & state
├── logs/                      # Per-run logs (generated)
└── reports/                   # Design notes & usage docs
```

---

## Usage

```bash
python run_etl.py                          # Full run
python run_etl.py --sample-size 1000       # Limited rows (testing)
python run_etl.py --skip-validation        # Faster run, skip integrity checks
python run_etl.py --show-state             # Inspect resume state
python run_etl.py --fresh-start            # Ignore previous state
python run_etl.py --reset-position --start-row 50000
```

Tune memory via `BATCH_SIZE` in `.env` (e.g. `1000`–`20000`).

---

## Data model (star schema)

| Table | Role |
|-------|------|
| `dim_circuit` | Circuit name, location, coordinates |
| `dim_constructor` | Teams |
| `dim_driver` | Driver bio / identity |
| `dim_date` | Qualifying calendar attributes |
| `facts` | Position, Q1/Q2/Q3 (ms), status, FKs to all dims |

```sql
facts.dim_circuit_circuit_key     → dim_circuit.circuit_key
facts.dim_constructor_constructor_key → dim_constructor.constructor_key
facts.dim_driver_driver_key       → dim_driver.driver_key
facts.dim_date_date_key           → dim_date.date_key
```

---

## Transform rules (summary)

**Skip record** if driver, constructor, or race/date is missing.

**Defaults**: unknown circuit → key `999`; missing position → `0`; missing lap times → `NULL`.

**Status derivation**

| Status | Meaning |
|--------|---------|
| `OK` | Q1, Q2, and Q3 present |
| `DNQ` | Q1 only (did not advance) |
| `DNS` | No times |
| `DSQ` | Invalid / penalty timing |

**Time conversion**

```text
"1:26.572" → (1×60 + 26)×1000 + 572 = 86572 ms
```

---

## Example analyses

Once loaded, the warehouse supports questions such as:

- Average qualifying position by driver over seasons
- Constructor competitiveness by circuit and era
- Q1→Q3 advancement rates and DNQ patterns
- Fastest session times by circuit

```sql
-- Top drivers by average qualifying position
SELECT
    d.name,
    AVG(f.position) AS avg_position,
    COUNT(*) AS sessions
FROM facts f
JOIN dim_driver d ON f.dim_driver_driver_key = d.driver_key
WHERE f.position > 0
GROUP BY d.driver_key, d.name
ORDER BY avg_position
LIMIT 10;
```

```sql
-- Qualifying status mix by year
SELECT
    dt.year,
    f.status,
    COUNT(*) AS count
FROM facts f
JOIN dim_date dt ON f.dim_date_date_key = dt.date_key
GROUP BY dt.year, f.status
ORDER BY dt.year, f.status;
```

More detail: [analytical objectives](reports/analytical_objectives.md).

---

## Monitoring

- Logs: `logs/f1_etl_run_YYYYMMDD_HHMMSS.log`
- State: `f1_etl_state.json` (resume / progress)

```bash
# Follow the latest run
tail -f logs/f1_etl_run_*.log
```

---

## Further reading

- [Pipeline description](reports/pipeline_description.md) — technical design
- [Usage instructions](reports/usage_instructions.md) — setup & ops detail
- [Diagrams](diagram.md) — sequence and flow charts

---

## License

Portfolio / educational project for data engineering and analytics practice.
