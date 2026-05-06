# Fabric Validation Suite

A metadata-driven framework for validating data parity between **Microsoft Fabric Lakehouse** tables and a **source-of-truth PostgreSQL** database. Built around a comparison engine that runs in Fabric Spark notebooks, audits results into a Fabric SQL Database, and ships with a self-test harness so you can prove the engine actually catches the failures it claims to.

---

## What this project does

1. **Compares** Lakehouse Delta tables against PostgreSQL tables using configurable modes (count / hash / column-level).
2. **Persists** every run's verdict + mismatch samples into a Fabric SQL DB for trend analysis, alerts, and audits.
3. **Tests itself.** A scenario harness mutates the lakehouse copy in known ways (delete rows, insert extras, update a column, add a column) and verifies the engine produces the expected verdict for each.
4. **Centralises configuration** in a Fabric **Variable Library** so the same notebooks run against dev/test/prod by swapping one library binding.

It is **source-system agnostic** in design (PostgreSQL is the reference implementation) and table-agnostic — any table you register in `validation.comparison_config` is in scope.

---

## Repository layout

```
fabric-validation/
├── README.md                              ← This file
├── how-to-guide.md                        ← Detailed step-by-step usage guide
├── copy_from_lakehouse.py                 ← Lakehouse-to-lakehouse table copy utility
├── validate_lakehouse_copy.py             ← Post-copy schema/row validation
│
├── validation/
│   ├── setup/                             ← Pre-test data prep & connectivity
│   │   ├── test_postgres_connectivity.py     PG JDBC reachability check
│   │   ├── generate_healthcare_data.py       Sample data generator (PG side)
│   │   ├── add_last_updated_column.py        One-time PG schema migration
│   │   ├── getDataFromPostgres.py            Bulk copy: PG schema → Lakehouse silver
│   │   └── cleanup_silver_admissions.py      One-shot orphan-table cleanup helper
│   │
│   ├── core/                              ← The validation engine itself
│   │   ├── _common.py                        Shared helpers (cfg, JDBC, KV, retry)
│   │   ├── comparison_setup.py               Creates validation.* metadata tables
│   │   ├── comparison_engine.py              Runs comparisons, writes results + samples
│   │   └── variable_library/                 Fabric Variable Library definition
│   │
│   └── engine_tests/                      ← Self-test scaffolding for the engine
│       ├── validation_audit_setup.py         Creates audit tables in Fabric SQL DB
│       ├── scenario_setup.py                 Defines scenarios in validation.scenarios
│       ├── scenario_seeder.py                Mutates the lakehouse per scenario
│       ├── scenario_assert.py                Verifies engine verdict matches expected
│       ├── scenario_list.py                  Emits enabled scenarios for ForEach
│       └── pipelines/
│           ├── validation_scenario_runner.json   Child pipeline (one scenario)
│           └── validation_test_suite.json        Parent pipeline (all scenarios)
│
├── docs/                                  ← Architecture & deep-dive guides
└── scripts/                               ← Local helpers (deploy notebooks/pipelines)
```

---

## Engine overview

### Comparison modes

| Mode | What it does | Cost | Catches | Requires PK |
|---|---|---|---|---|
| `basic` | Row counts only | Low | Counts diverging | No |
| `hash` | Count + per-row MD5 of all comparable columns | Medium | Which rows differ (no column detail) | **Yes** |
| `hash_no_pk` | Multiset count diff over row hashes (full-outer join on `_row_hash`) | Medium | Net count drift + which row content "buckets" diverged | No |
| `advanced` | Full column-by-column diff between matched rows | High | Which rows AND which columns differ | **Yes** |
| `advanced_no_pk` | Bidirectional `exceptAll` over normalized rows | High–Very High | Full row content of every divergent row, both directions | No |

`advanced` auto-falls-back to `hash` once row count exceeds `max_rows_advanced`.

**Tables without a primary key.** Use `hash_no_pk` (cheap, multiset-aware) or
`advanced_no_pk` (full row evidence, bidirectional). For tables already
configured as `hash` or `advanced` whose `comparison_key_columns` is empty,
set `pk_fallback_strategy` to `no_pk_hash` / `no_pk_advanced` and the engine
will route automatically. See
[how-to-guide.md §6.7 Validating tables without a primary key](how-to-guide.md#67-validating-tables-without-a-primary-key).

### Verdict types written to audit

`pass`, `count_mismatch`, `hash_mismatch`, `column_mismatch`, `schema_drift`, `inconclusive`, `error`.

### Outputs per run

- **Fabric Lakehouse** (`validation.comparison_results`, `validation.comparison_details`) — Delta tables
- **Fabric SQL DB** (`dbo.validation_runs`, `dbo.validation_mismatch_samples`) — for cross-workspace consumption, Power BI, alerts

---

## Quick start

```text
1.  Provision the prerequisites (PG instance, Fabric workspace + lakehouse + SQL DB,
    Key Vault with PG password, Variable Library populated). See how-to-guide.md.

2.  Deploy the notebooks under validation/core/ and validation/setup/ to Fabric.

3.  Run validation/setup/test_postgres_connectivity.py to verify private connectivity.

4.  Run validation/core/comparison_setup.py once to create metadata tables and
    seed config rows for the tables you want to compare.

5.  (One-time) Copy your source tables to the lakehouse via
    validation/setup/getDataFromPostgres.py.

6.  Run validation/core/comparison_engine.py — verdicts land in
    validation.comparison_results AND in dbo.validation_runs.
```

Detailed steps, parameter reference, and troubleshooting are in **[how-to-guide.md](how-to-guide.md)**.

---

## Self-test harness

The harness verifies that the engine returns the *correct verdict* for known mutations.

| Scenario | Mutation | Expected verdict |
|---|---|---|
| `baseline-noop-*` | None | `pass` |
| `delete-rows-*` | Remove N rows from lakehouse | `count_mismatch` |
| `insert-extra-rows-*` | Insert N rows into lakehouse | `count_mismatch` |
| `update-column-*` | Mutate a non-PK column on N recent rows | `hash_mismatch` |
| `add-extra-column-*` | Add a column to lakehouse | `schema_drift` (when policy=`fail`) |

The parent pipeline `validation_test_suite` enumerates enabled scenarios and runs each through the child pipeline `validation_scenario_runner`:

```
RestoreBaseline (re-import from PG)
   → RunSeeder (apply mutation)
   → RunEngine (compare)
   → RunAssert (compare actual verdict to expected)
```

---

## Architecture: Fabric ↔ PostgreSQL private connectivity

```
Fabric Notebook → Managed Private Endpoint → Private Link Service
   → Internal Load Balancer → VM (HAProxy) → PostgreSQL (VNet-integrated)
```

VNet-integrated PostgreSQL flexible servers cannot expose Private Endpoints directly, so a small HAProxy VM bridges the gap. See `docs/postgres-private-access-from-fabric.md` and `docs/private-link-service-proxy-for-fabric.md`.

---

## Documentation

| Document | Description |
|---|---|
| [`how-to-guide.md`](how-to-guide.md) | End-to-end walkthrough: prerequisites → comparison configs → variable library → running the engine → harness |
| `docs/postgres-private-access-from-fabric.md` | PostgreSQL private access patterns from Fabric |
| `docs/private-link-service-proxy-for-fabric.md` | Generic PLS-proxy reference for VNet-integrated services |
| `docs/fabric-postgres-vm-proxy-architecture.drawio` | Network architecture diagram |

---

## Key design choices

- **Metadata-driven, not code-driven.** Adding a table to scope = inserting one row into `validation.comparison_config`. No notebook edits.
- **Fail-closed by default.** Schema drift on critical tables raises `schema_drift`, not silent intersection.
- **Centralised config.** Hosts, schemas, secret names, lakehouse names — all live in the Variable Library. Notebooks pull from `cfg`.
- **Engine never trusts the lakehouse.** PostgreSQL is the source of truth; the lakehouse is the candidate.
- **Harness is mandatory.** A validation engine that isn't itself validated is theatre.

---

## Common gotchas

- Fabric `%run` only resolves notebook **display names**, not file paths. Cells that contain `%run` must contain *only* that one statement.
- Pipeline-injected notebook activity parameters require the target cell to carry the `parameters` tag.
- `notebookutils.notebook.exit(value)` raises an internal control-flow exception — it must be the last statement of the last cell. Do not wrap it in `try/except`.
- Pipeline-invoked notebooks run as the **workspace identity**, not the user. Grant the workspace identity `Key Vault Secrets User` (or enable trusted-services bypass) before running pipelines.
- VNet-integrated PostgreSQL flexible servers cannot have Private Endpoints — use the HAProxy + PLS bridge pattern.

---

## License

Internal demo / reference implementation. Adapt freely for your environment.
