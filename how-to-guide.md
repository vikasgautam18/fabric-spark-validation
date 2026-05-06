# How-To Guide — Fabric Validation Suite

End-to-end walkthrough for using the validation engine to compare Lakehouse data against a PostgreSQL source of truth, plus running the self-test harness.

---

## Table of contents

1. [Prerequisites](#1-prerequisites)
2. [Set up the Variable Library](#2-set-up-the-variable-library)
3. [Deploy the notebooks to Fabric](#3-deploy-the-notebooks-to-fabric)
4. [Verify connectivity](#4-verify-connectivity)
5. [Land source data in the lakehouse](#5-land-source-data-in-the-lakehouse)
6. [Set up comparison configs](#6-set-up-comparison-configs)
7. [Run the comparison engine](#7-run-the-comparison-engine)
8. [Inspect results](#8-inspect-results)
9. [Set up the audit store (Fabric SQL DB)](#9-set-up-the-audit-store-fabric-sql-db)
10. [Run the self-test harness](#10-run-the-self-test-harness)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Prerequisites

### 1.1 Azure / source side

| Resource | Purpose | Notes |
|---|---|---|
| **PostgreSQL** instance (any flavour) | Source of truth | If VNet-integrated and not directly reachable from Fabric, use the HAProxy + PLS pattern in `docs/private-link-service-proxy-for-fabric.md`. |
| **Azure Key Vault** | Stores PG password | Secret name configurable; default `pg-password`. |
| **(Optional) VM proxy + ILB + Private Link Service** | Bridges Fabric → private PG | Only required when PG cannot expose a Private Endpoint directly. |

### 1.2 Fabric side

| Resource | Purpose |
|---|---|
| **Fabric Workspace** assigned to a capacity (F-SKU or trial) | Hosts everything below |
| **Lakehouse** (with schemas enabled) | Holds the candidate copy of source data; default `silver` schema |
| **Fabric SQL Database** | Stores audit results consumable by Power BI / alerts |
| **Managed Private Endpoint** to the PG endpoint | Approved by the PG side |
| **Variable Library** named `validation_config` | Centralises configuration — see §2 |

### 1.3 Identity & permissions

- The **user** running the notebooks needs `Workspace Contributor` on the Fabric workspace and read access to the Key Vault secret.
- The **workspace identity** needs `Key Vault Secrets User` on the Key Vault. **This is required when running notebooks via pipelines** — pipeline-invoked notebooks run as the workspace identity, not as you.
- The user / workspace identity needs CRUD on the Fabric SQL Database for audit writes.

### 1.4 Local tooling (for deployment)

| Tool | Purpose |
|---|---|
| `az` CLI, logged in (`az login`) to the right tenant | Token acquisition for the Fabric REST API |
| Python 3.10+ | Runs `scripts/deploy_nb.py` and `scripts/deploy_pipeline.py` |
| `git` | Source control |

---

## 2. Set up the Variable Library

The engine and helpers read all environment-specific values from a Fabric **Variable Library** named `validation_config`. The definition lives at:

```
validation/core/variable_library/
├── settings.json
└── variables.json
```

### 2.1 Variable reference

| Variable | Type | Example | Purpose |
|---|---|---|---|
| `pg_host` | String | `pg.example.privatelinkservice` | PostgreSQL hostname (FQDN) |
| `pg_port` | Integer | `5432` | PostgreSQL port |
| `pg_database` | String | `postgres` | Database name |
| `pg_user` | String | `sqladmin` | PG login user |
| `pg_schema` | String | `healthcare` | Default PG schema for ingestion / comparison |
| `kv_url` | String | `https://kv-xxx.vault.azure.net/` | Key Vault URL |
| `kv_pg_secret` | String | `pg-password` | Secret name holding the PG password |
| `lakehouse_name` | String | `demolh` | Target lakehouse |
| `lakehouse_schema` | String | `silver` | Default lakehouse schema for comparisons |
| `sql_server` | String | `<server>.database.fabric.microsoft.com` | Fabric SQL DB connection string fragment |
| `sql_database` | String | `<sqldb-id>` | Fabric SQL DB name (full ID-suffixed name) |
| `sql_schema` | String | `dbo` | Schema for audit tables |

### 2.2 Edit and deploy

1. Edit `validation/core/variable_library/variables.json` with your environment values (do **not** commit secrets — only references like KV URL and secret name).
2. Deploy via the Fabric portal (Workspace → New → Variable library → import) **or** via the REST API. Make sure the resulting library is named exactly `validation_config`.
3. Bind the library's "active value set" to your environment (dev/test/prod). Different value sets let you point the same notebooks at different environments without changing code.

### 2.3 How notebooks consume it

`validation/core/_common.py` (which every notebook starts with via `%run _common`) loads the library into a `cfg` dict:

```python
import notebookutils
_lib = notebookutils.variableLibrary.getLibrary("validation_config")
cfg = {
    "pg_host":          _lib.pg_host,
    "pg_port":          int(_lib.pg_port),
    # ... etc
}
```

You can override any field per-notebook *after* the `%run`:

```python
cfg["pg_schema"] = "extended_types"
```

---

## 3. Deploy the notebooks to Fabric

Two options:

### 3.1 Manual upload (one-time / quick start)

In the Fabric portal: Workspace → Import → Notebook → upload each `.py` file. Fabric will convert them to `.ipynb`. Set the default lakehouse on each notebook to your target lakehouse.

### 3.2 Scripted deployment (recommended)

`scripts/deploy_nb.py` deploys a `.py` source as a Fabric notebook with:
- Cell separators preserved (`# In[N]:`)
- The `parameters` tag added to cells that contain the marker comment (`# parameters cell` / `# parameters tag` / `# set from pipeline`)
- A default lakehouse bound via metadata

```bash
# Deploy all notebooks (folder IDs and notebook IDs are workspace-specific):
python3 scripts/deploy_nb.py validation/core/_common.py            _common            <FOLDER_ID>
python3 scripts/deploy_nb.py validation/core/comparison_setup.py   comparison_setup   <FOLDER_ID>
python3 scripts/deploy_nb.py validation/core/comparison_engine.py  comparison_engine  <FOLDER_ID>
# ...and so on for setup/ and harness/ files
```

Pass an optional fourth argument (`<existingNotebookId>`) to update an existing notebook instead of creating a new one.

> **Cell convention:** When `%run _common` is used, that line must be the **only** statement in its cell (no comments, no extra lines).

---

## 4. Verify connectivity

Run `validation/setup/test_postgres_connectivity.py` in Fabric. It performs a JDBC connect + `SELECT 1` round-trip using the Variable Library values and the Key Vault secret. A successful run prints the PG version and exits cleanly.

If it fails, see [Troubleshooting → Connectivity](#111-connectivity).

---

## 5. Land source data in the lakehouse

The engine compares **what's already in the lakehouse** against PostgreSQL. You need an initial copy.

### 5.1 First load

Run `validation/setup/getDataFromPostgres.py`. Defaults:
- `PG_TABLES = []` → discovers all tables in `cfg['pg_schema']`
- `WRITE_MODE = "overwrite"` → full replace per table

Override via notebook activity parameters (in a pipeline) or by editing the per-notebook overrides cell:

```python
PG_TABLES = ["patients", "appointments"]
WRITE_MODE = "overwrite"
```

### 5.2 Pre-flight: the `last_updated` column

The engine's filter window uses a timestamp column (default `last_updated`) on each comparable table. If your tables don't have it, run `validation/setup/add_last_updated_column.py` once. It:

- Adds `last_updated TIMESTAMP` to all PG tables in scope
- Backfills it from `created_at` (or `NOW()`) so existing rows are dateable
- Adds the same column on the lakehouse side
- Is idempotent

### 5.3 (Optional) Generate sample data

`validation/setup/generate_healthcare_data.py` is a self-contained healthcare dataset generator (patients, doctors, departments, appointments, diagnoses, prescriptions, plus extended-type and edge-case tables). Useful for first-run smoke tests when you don't yet have a real source. Each invocation **inserts new rows AND updates ~30% of existing rows** — perfect for exercising windowed comparisons.

---

## 6. Set up comparison configs

The engine is fully **metadata-driven**. Five Delta tables in the lakehouse `validation` schema control behavior:

| Table | Purpose |
|---|---|
| `validation.comparison_config` | One row per comparable table — mode, window, policies |
| `validation.comparison_key_columns` | Primary-key columns per table (composite keys = multiple rows) |
| `validation.comparison_skip_columns` | Columns to exclude from comparison |
| `validation.comparison_results` | Run summaries (one row per table per run) |
| `validation.comparison_details` | Row/column-level mismatch detail (capped) |

Run `validation/core/comparison_setup.py` once to create them. It also seeds default config rows for the bundled sample tables (skip the seed step if you're using your own tables — set `RESET_CONFIG=True` only if you want to start fresh).

### 6.1 `comparison_config` schema

| Column | Type | Meaning |
|---|---|---|
| `table_name` | STRING | Logical name (must match lakehouse table name) |
| `pg_schema` | STRING | PG schema |
| `pg_table_name` | STRING | PG table name (defaults to `table_name`) |
| `lakehouse_schema` | STRING | Lakehouse schema (e.g. `silver`) |
| `comparison_mode` | STRING | `basic` / `hash` / `hash_no_pk` / `advanced` / `advanced_no_pk` |
| `filter_column` | STRING | Timestamp column for the lookback window. May be a SQL expression with `{window_start}` / `{window_end}` placeholders |
| `filter_column_pg` | STRING | Override if PG column name differs (else uses `filter_column`) |
| `filter_days` | INT | Window length in days |
| `safety_lag_minutes` | INT | Buffer for in-flight ingestion (default 30) — excludes the most recent N minutes |
| `schema_drift_policy` | STRING | `fail` / `ignore_extra` / `intersect` |
| `numeric_tolerance` | DOUBLE | Float/decimal equality tolerance |
| `max_rows_advanced` | INT | If `advanced` mode and row count exceeds this → fall back to `hash` |
| `severity` | STRING | `critical` / `warning` / `info` (audit-only field) |
| `enabled` | BOOLEAN | Engine skips disabled rows |
| `pk_fallback_strategy` | STRING | `fail` (default — strict; raise on empty `comparison_key_columns`) / `no_pk_hash` / `no_pk_advanced`. Lets `hash` / `advanced` modes route to their PK-less variants instead of erroring out. See §6.7 |

### 6.2 Choosing a mode

| Use mode | When | PK required |
|---|---|---|
| `basic` | Smoke tests; tables where row count alone is enough; very large fact tables where per-row hashing is too expensive | No |
| `hash` | Default for medium/large tables. Detects "rows differ" without expensive column-level work | **Yes** |
| `hash_no_pk` | Same as `hash` but for tables without a PK. Multiset-aware (preserves duplicate counts). Cannot tell *which row* changed when totals match — emits `inconclusive` in that case | No |
| `advanced` | Small reference tables, dimension tables, anywhere you need to know **which column** changed | **Yes** |
| `advanced_no_pk` | PK-less tables where you need full row-content evidence on both sides. Most expensive option (`exceptAll` shuffles full row payloads) | No |

### 6.3 Choosing a `schema_drift_policy`

| Policy | Behavior on schema mismatch |
|---|---|
| `fail` | Engine emits `schema_drift` verdict and stops comparing this table. **Use for production-critical tables.** |
| `ignore_extra` | Tolerate extra columns on the lakehouse side; fail if PG has columns the lakehouse doesn't |
| `intersect` | Compare only the columns present on both sides. Silently masks drift — only use for known-tolerant cases |

### 6.4 Adding your tables

```sql
INSERT INTO validation.comparison_config VALUES
  ('orders',     'sales',      'orders',     'silver', 'hash',     'last_updated', NULL, 7, 30, 'fail',      0.001, 500000, 'critical', true),
  ('customers',  'sales',      'customers',  'silver', 'advanced', 'last_updated', NULL, 7, 30, 'fail',      0.001, 100000, 'critical', true);

INSERT INTO validation.comparison_key_columns VALUES
  ('orders',    'order_id',    1),
  ('customers', 'customer_id', 1);

-- Optional: skip auto-generated columns
INSERT INTO validation.comparison_skip_columns VALUES
  ('orders',    'created_at'),
  ('customers', 'etl_load_ts');
```

### 6.5 Composite primary keys

Insert multiple rows into `comparison_key_columns` with sequential `ordinal`:

```sql
INSERT INTO validation.comparison_key_columns VALUES
  ('order_lines', 'order_id', 1),
  ('order_lines', 'line_no',  2);
```

### 6.6 Filter expressions

For tables where the timestamp logic is complex (e.g. compare on `last_updated` OR `created_at`), put the full expression in `filter_column`:

```sql
"last_updated" >= '{window_start}' AND "last_updated" < '{window_end}'
  OR "created_at" >= '{window_start}' AND "created_at" < '{window_end}'
```

Placeholders `{window_start}` / `{window_end}` are interpolated by the engine on both sides.

> **Security note:** filter expressions are interpolated into SQL **without validation**. Treat `comparison_config` as trusted code.

### 6.7 Validating tables without a primary key

Some source tables (event logs, sensor streams, append-only landing tables)
have no natural primary key — duplicates are part of the data. The engine
supports two PK-less modes plus an opt-in fallback for tables already
configured as `hash` / `advanced`.

#### 6.7.1 The two PK-less modes

| Mode | How it works | Catches | When totals match but content diverges |
|---|---|---|---|
| `hash_no_pk` | `groupBy(_row_hash).count()` on each side, full-outer join on hash | Net count drift, multiset divergence | Reports `inconclusive` (cannot pair rows without PK) |
| `advanced_no_pk` | Project both sides through the same type-normalization, then bidirectional `exceptAll` (multiplicity preserved) | Full row content of every divergent row, both directions | Reports `hash_mismatch` with samples on **both** sides |

Pick `hash_no_pk` for cheap detection of net drift on large PK-less tables.
Pick `advanced_no_pk` when you need full row evidence and the table is small
enough that an `exceptAll` shuffle is acceptable.

#### 6.7.2 Configuring a PK-less table

```sql
INSERT INTO validation.comparison_config VALUES
  ('event_log', 'healthcare', 'event_log', 'silver',
   'hash_no_pk', 'event_ts', NULL, 7, 30, 'fail',
   0.0, 500000, 'warning', true,
   NULL);  -- pk_fallback_strategy not used when mode is already *_no_pk
```

Do **not** insert into `comparison_key_columns` — it must remain empty for
PK-less modes.

#### 6.7.3 Auto-routing via `pk_fallback_strategy`

If a table is configured as `hash` or `advanced` but has no rows in
`comparison_key_columns`, the engine consults `pk_fallback_strategy`:

| `pk_fallback_strategy` value | Behaviour for `mode='hash'` | Behaviour for `mode='advanced'` |
|---|---|---|
| `NULL` or `fail` (default) | Audit `error` with actionable message — no run | Audit `error` with actionable message — no run |
| `no_pk_hash` | Routes to `run_hash_no_pk`; `comparison_results.actual_mode` records `hash_no_pk` | Routes to `run_hash_no_pk` |
| `no_pk_advanced` | Routes to `run_advanced_no_pk` | Routes to `run_advanced_no_pk`; `actual_mode` records `advanced_no_pk` |

This lets you opt into PK-less handling per table without changing the
declared `comparison_mode` — useful when most of a workload is hash/advanced
but a few tables can't supply a PK. To enable it on an existing row:

```sql
ALTER TABLE validation.comparison_config
  ADD COLUMNS (pk_fallback_strategy STRING);  -- only on pre-Phase-5 deployments
UPDATE validation.comparison_config
   SET enabled = true, pk_fallback_strategy = 'no_pk_hash'
 WHERE table_name = 'event_log';
```

The `comparison_setup` notebook does this idempotently for the bundled
PK-less fixtures (`event_log`, `sensor_readings`, `landing_orders`).

#### 6.7.4 Caveats (why a PK is still preferable when you have one)

- **No row-level pairing.** Without a PK the engine cannot say "row X
  changed column Y from A to B"; it can only say "this row content is in
  one side and not the other".
- **`numeric_tolerance` is approximate in `advanced_no_pk`.** It is applied
  by rounding numeric columns to `ceil(-log10(tolerance))` decimal places
  before the set difference, so values straddling a rounding boundary may
  register as different even when `|lh-pg| < tolerance`. For exact per-row
  tolerance use `advanced` mode.
- **`numeric_tolerance` is NOT applied at all in `hash_no_pk`** — hashes are
  byte-exact. Floats are normalized to 8 decimal places (matching `hash`),
  JSON/JSONB compared as text, timestamps at second precision.
- **`hash_no_pk` returns `inconclusive` on a content swap** (delete N + insert N
  different rows): totals match but the multiset diverges and there is no
  way to pair the missing/extra rows.
- **`exceptAll` shuffles full row payloads.** Wide tables with millions of
  rows are dramatically more expensive in `advanced_no_pk` than in
  `hash_no_pk`. Profile before enabling.

---

## 7. Run the comparison engine

Run `validation/core/comparison_engine.py` in Fabric.

### 7.1 Default behavior

Compares **every enabled row** in `comparison_config`. Writes one row per table to `validation.comparison_results` and per-table mismatch detail (capped at 1000 rows) to `validation.comparison_details`. Also mirrors results into the Fabric SQL DB audit tables (see §9).

### 7.2 Parameters (override via pipeline activity or override cell)

| Parameter | Type | Default | Effect |
|---|---|---|---|
| `triggered_by` | str | `manual` | Free-text label stamped on the run |
| `scenario_id` | str | `None` | If set, recorded against the audit run (used by harness) |
| `pipeline_run_id` | str | `None` | Foreign key for cross-system traceability |
| `tables_filter` | list[str] / JSON str / `None` | `None` | If set, compare only these tables. Engine raises if any requested table isn't in `comparison_config` |
| `fail_on_validation_failure` | bool | `True` | If `False`, the engine still audits failures but does NOT raise — needed when failures are an *expected* outcome (the harness uses this) |

### 7.3 Verdict types

| Verdict | When |
|---|---|
| `pass` | Counts equal AND (mode-specific) row/column data matches within tolerance |
| `count_mismatch` | Row counts differ in the window |
| `hash_mismatch` | Counts equal but row hashes differ (`hash`/`advanced` modes) |
| `column_mismatch` | At least one column-level diff (`advanced` mode) |
| `schema_drift` | Schema drift detected and `policy=fail` |
| `inconclusive` | Mode/checks couldn't run (missing PK, all-null filter column, etc.) — also emitted by `hash_no_pk` when totals match but the multiset diverges (content swap with no PK to pair on) |
| `error` | Unhandled exception — see `error_message` |

`hash` mode short-circuits when counts differ → returns `count_mismatch` immediately (no expensive hash compare).

---

## 8. Inspect results

### 8.1 In the lakehouse

```sql
-- Most recent run per table:
SELECT * FROM validation.comparison_results
WHERE run_id = (SELECT MAX(run_id) FROM validation.comparison_results);

-- Drill into mismatch detail:
SELECT * FROM validation.comparison_details
WHERE run_id = '<run_id>' AND table_name = 'orders';
```

### 8.2 In the Fabric SQL DB (audit)

```sql
SELECT TOP 100 * FROM dbo.validation_runs
ORDER BY run_started_utc DESC;

SELECT * FROM dbo.validation_mismatch_samples
WHERE run_id = '<run_id>';
```

### 8.3 Power BI

Connect Power BI Desktop to the Fabric SQL DB endpoint; build trend / drift dashboards over `dbo.validation_runs`.

---

## 9. Set up the audit store (Fabric SQL DB)

Run `validation/engine_tests/validation_audit_setup.py` once. It creates:

| Table | Purpose |
|---|---|
| `dbo.validation_runs` | One row per (run_id, table) — verdict, counts, durations |
| `dbo.validation_mismatch_samples` | Sample mismatched rows for forensics |
| `dbo.validation_scenario_runs` | Scenario-level outcomes (used by harness) |

The script is idempotent. It also runs ALTER COLUMN migrations to widen columns that have proven too narrow in practice (e.g. `verdict` → `VARCHAR(20)`).

---

## 10. Run the self-test harness

The harness proves the engine produces the expected verdict for known mutations.

### 10.1 One-time setup

Run, in order:
1. `validation/engine_tests/validation_audit_setup.py` — creates audit tables (if not already done).
2. `validation/engine_tests/scenario_setup.py` — creates `validation.scenarios` and seeds bundled scenarios.

### 10.2 Scenario taxonomy

Scenarios live in `validation.scenarios`. Each row defines:

| Field | Meaning |
|---|---|
| `scenario_id` | Unique name |
| `target_table` | Which table to mutate (must exist in `comparison_config`) |
| `mutation_type` | `noop` / `delete_rows` / `insert_extra_rows` / `update_column` / `add_extra_column` / `drop_column` / `null_out_pk` / `clear_key_cols` / `delete_rows_no_pk` / `insert_extra_rows_no_pk` / `content_swap_no_pk` |
| `mutation_params` | JSON with mutation-specific knobs (e.g. `{"count": 50}`; for `insert_extra_rows_no_pk` the `mutate` flag toggles unique-vs-verbatim duplicate) |
| `expected_status` | What the engine should return (e.g. `pass`, `count_mismatch`, `hash_mismatch`, `inconclusive`, `error`, `schema_drift`) |
| `valid_comparison_modes` | Comma-separated list, e.g. `hash_no_pk,advanced_no_pk`. Scenarios are skipped (`not_applicable`) when the table's active mode is not listed |
| `enabled` | Toggle |

### 10.3 Manual run (single scenario)

You can run any one scenario by hand via the three notebooks:

```text
validation/engine_tests/scenario_seeder.py         (with scenario_id parameter)
validation/core/comparison_engine.py          (with tables_filter=[target_table], fail_on_validation_failure=False)
validation/engine_tests/scenario_assert.py         (with scenario_id parameter)
```

### 10.4 Pipeline run (all scenarios)

Deploy and trigger the parent pipeline:

```bash
python3 scripts/deploy_pipeline.py validation/engine_tests/pipelines/validation_scenario_runner.json       validation_scenario_runner
python3 scripts/deploy_pipeline.py validation/engine_tests/pipelines/validation_scenario_group_runner.json validation_scenario_group_runner
python3 scripts/deploy_pipeline.py validation/engine_tests/pipelines/validation_test_suite.json           validation_test_suite
```

Trigger `validation_test_suite` from the Fabric portal. It:

1. Calls `scenario_list` to enumerate enabled scenarios, **grouped by `target_table`**.
2. ForEach (**parallel**, `batchCount=4`) over the groups → invokes `validation_scenario_group_runner` per group.
3. Inside each group, ForEach (sequential) over the scenarios → invokes `validation_scenario_runner`:
   - **RestoreBaseline** — re-imports the target table from PG (clean slate)
   - **RunSeeder** — applies the mutation
   - **RunEngine** — runs comparison_engine with `tables_filter=[target_table]`, `fail_on_validation_failure=False`
   - **RunAssert** — reads the audit row and verifies actual verdict matches expected

Two scenarios on the **same** `target_table` always run sequentially (mutations would cross-contaminate). Scenarios on **different** tables run concurrently.

A scenario passes when `actual_verdict == expected_verdict`.

### 10.5 Adding your own scenarios

```sql
INSERT INTO validation.scenarios VALUES
  ('delete-rows-orders-50', 'orders', 'delete_rows', '{"n": 50}', 'count_mismatch', true);
```

The harness will pick it up on the next pipeline run.

---

## 11. Troubleshooting

### 11.1 Connectivity

| Symptom | Likely cause | Fix |
|---|---|---|
| `Connection refused` | Fabric cannot reach PG | MPE not approved on PG side, or PLS/ILB misconfigured |
| `password authentication failed` | Wrong KV secret value | Verify the value at the configured `kv_url` / `kv_pg_secret` |
| `403 Forbidden` from KV | Identity lacks access | Pipeline runs use **workspace identity** — grant it `Key Vault Secrets User` |
| `Public network access is disabled` (KV) | KV has no firewall exception | Enable "Allow trusted Microsoft services" or use private endpoint |

### 11.2 Engine

| Symptom | Likely cause | Fix |
|---|---|---|
| `relation "schema.table" does not exist` (from PG) | Stale row in `comparison_config` pointing at non-existent PG table | `DELETE` the row, or fix `pg_schema` / `pg_table_name` |
| Engine compares all tables when `tables_filter` was set | Parameters cell missing the `parameters` tag | Ensure marker comment present (`# parameters cell` / `# set from pipeline`) and redeploy |
| `inconclusive` verdict on every run | Filter window excludes all rows; or PK column missing | Check `filter_days` / `safety_lag_minutes`; ensure `comparison_key_columns` populated |
| `schema_drift` on a table you expected to pass | Lakehouse schema legitimately diverged, OR `policy=fail` is too strict | Reconcile schemas, or change `schema_drift_policy` |

### 11.3 Pipelines / notebooks

| Symptom | Likely cause | Fix |
|---|---|---|
| `File '_common' not found` | `%run` cell isn't isolated | Make `%run _common` the only statement in its cell |
| `SystemExit: 0` shows up as activity failure | `notebook.exit()` called mid-cell or wrapped in try/except | Move `notebook.exit(value)` to be the last statement of the last cell, no wrapping |
| `String or binary data would be truncated` (audit table) | Verdict / column wider than the SQL DB schema | Re-run `validation_audit_setup.py` (it widens columns) |
| `dependencyConditions` template error | Duplicate values like `["Succeeded","Completed"]` | Use a single value, e.g. `["Succeeded"]` |

### 11.4 Variable Library

| Symptom | Likely cause | Fix |
|---|---|---|
| `'NoneType' object has no attribute ...` early in `_common.py` | Library not bound to the workspace, or the active value set is empty | Bind `validation_config` and select an active value set |
| Wrong environment values | Active value set points elsewhere | Switch the active value set in the library |

---

## Appendix A — Notebook execution order (cheat sheet)

```text
First-time setup (per environment):
  1.  Variable Library 'validation_config' deployed and bound
  2.  validation/setup/test_postgres_connectivity.py        ← verify
  3.  validation/setup/add_last_updated_column.py           ← if needed
  4.  validation/setup/getDataFromPostgres.py               ← initial load
  5.  validation/core/comparison_setup.py                   ← create metadata tables
  6.  validation/engine_tests/validation_audit_setup.py          ← create audit tables
  7.  validation/engine_tests/scenario_setup.py                  ← seed scenarios (optional)

Per validation run:
  •  validation/core/comparison_engine.py
        (optionally with tables_filter, scenario_id, etc.)

Self-test:
  •  Trigger pipeline 'validation_test_suite'
```

## Appendix B — Useful SQL snippets

```sql
-- Disable a flaky table without deleting its config:
UPDATE validation.comparison_config SET enabled = false WHERE table_name = 'staging_blob';

-- Promote a table from hash to advanced:
UPDATE validation.comparison_config SET comparison_mode = 'advanced' WHERE table_name = 'customers';

-- Last 7 days of failures across all tables:
SELECT table_name, verdict, COUNT(*) AS n
FROM dbo.validation_runs
WHERE run_started_utc >= DATEADD(day, -7, SYSUTCDATETIME())
  AND verdict <> 'pass'
GROUP BY table_name, verdict
ORDER BY n DESC;
```
