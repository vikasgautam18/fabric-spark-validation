#!/usr/bin/env python
# coding: utf-8

# ## Validation Suite — Setup Metadata Tables
#
# Creates the metadata tables that drive the comparison engine.
# Run this ONCE (idempotent) to set up:
#   1. `validation.comparison_config`     — which tables to compare
#   2. `validation.comparison_key_columns` — PK columns for row matching
#   3. `validation.comparison_skip_columns` — columns to exclude
#   4. `validation.comparison_results`    — summary results per run
#   5. `validation.comparison_details`    — row/column mismatch details
#
# After running, populate comparison_config with your table entries
# or use the seed data cell at the bottom for the healthcare tables.

# In[1]:

# ── Configuration ─────────────────────────────────────────────────────────────

# Set to True to TRUNCATE config tables before re-seeding (wipes config + key/skip
# definitions, but preserves comparison_results and comparison_details history).
# Useful when the seed data has changed (new tables, modified filters, etc.).
RESET_CONFIG = False

# Set to True to additionally wipe the run history (results + details).
# Use with caution — destroys audit trail.
RESET_HISTORY = False

# Set to True to also (re)create the SQL DB audit tables — invokes
# `validation_audit_setup` notebook. Idempotent (CREATE IF NOT EXISTS).
SETUP_AUDIT_TABLES = False

# Set to True to also seed the scenario test driver table — invokes
# `scenario_setup` notebook. Idempotent (upserts by scenario_id).
SETUP_SCENARIOS = False


# ── Create validation schema and metadata tables ──────────────────────────────

spark.sql("CREATE SCHEMA IF NOT EXISTS validation")

# ── 1. comparison_config ──────────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS validation.comparison_config (
    table_name          STRING      COMMENT 'Logical table name',
    pg_schema           STRING      COMMENT 'PostgreSQL schema name',
    pg_table_name       STRING      COMMENT 'PostgreSQL table name (if different from table_name)',
    lakehouse_schema    STRING      COMMENT 'Lakehouse schema name',
    comparison_mode     STRING      COMMENT 'basic | hash | advanced',
    filter_column       STRING      COMMENT 'Date/timestamp column for windowed compare',
    filter_column_pg    STRING      COMMENT 'Override if Postgres filter column name differs',
    filter_days         INT         COMMENT 'Lookback window in days',
    safety_lag_minutes  INT         COMMENT 'Buffer for ingestion delay (default 30)',
    schema_drift_policy STRING      COMMENT 'fail | ignore_extra | intersect',
    numeric_tolerance   DOUBLE      COMMENT 'Tolerance for float/decimal comparison',
    max_rows_advanced   INT         COMMENT 'Row cap before falling back to hash mode',
    severity            STRING      COMMENT 'critical | warning | info',
    enabled             BOOLEAN     COMMENT 'Whether this table is active for comparison'
)
USING DELTA
COMMENT 'Metadata config for Lakehouse vs PostgreSQL comparison'
""")

# ── 2. comparison_key_columns ─────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS validation.comparison_key_columns (
    table_name      STRING  COMMENT 'References comparison_config.table_name',
    column_name     STRING  COMMENT 'Primary key column name',
    ordinal         INT     COMMENT 'Column order in composite key'
)
USING DELTA
COMMENT 'PK columns for row-level matching in advanced/hash mode'
""")

# ── 3. comparison_skip_columns ────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS validation.comparison_skip_columns (
    table_name      STRING  COMMENT 'References comparison_config.table_name',
    column_name     STRING  COMMENT 'Column to exclude from comparison'
)
USING DELTA
COMMENT 'Columns to skip during comparison'
""")

# ── 4. comparison_results ─────────────────────────────────────────────────────

# keep column level details from the comparison, primary keys as well
# in tables with no primary keys, keep all columns for debugging purposes

spark.sql("""
CREATE TABLE IF NOT EXISTS validation.comparison_results (
    run_id                  STRING      COMMENT 'Unique run identifier',
    table_name              STRING      COMMENT 'Table compared',
    comparison_mode         STRING      COMMENT 'basic | hash | advanced',
    window_start_utc        TIMESTAMP   COMMENT 'Filter window start',
    window_end_utc          TIMESTAMP   COMMENT 'Filter window end',
    safety_lag_minutes      INT         COMMENT 'Safety lag applied',
    lakehouse_count         LONG        COMMENT 'Row count in lakehouse',
    postgres_count          LONG        COMMENT 'Row count in postgres',
    count_match             BOOLEAN     COMMENT 'Whether counts match',
    rows_only_in_lakehouse  LONG        COMMENT 'Rows missing from postgres (advanced/hash)',
    rows_only_in_postgres   LONG        COMMENT 'Rows missing from lakehouse (advanced/hash)',
    rows_with_mismatches    LONG        COMMENT 'Rows with column-level diffs (advanced)',
    status                  STRING      COMMENT 'PASS | FAIL | ERROR | DATA_QUALITY_ERROR',
    error_message           STRING      COMMENT 'Error details if status=ERROR',
    executed_at             TIMESTAMP   COMMENT 'When comparison ran',
    duration_seconds        DOUBLE      COMMENT 'How long comparison took'
)
USING DELTA
COMMENT 'Summary results of each comparison run'
""")

# ── 5. comparison_details ─────────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS validation.comparison_details (
    run_id              STRING  COMMENT 'References comparison_results.run_id',
    table_name          STRING  COMMENT 'Table compared',
    pk_values           STRING  COMMENT 'JSON-encoded PK values for the mismatched row',
    column_name         STRING  COMMENT 'Column with mismatch',
    lakehouse_value     STRING  COMMENT 'Value in lakehouse (cast to string)',
    postgres_value      STRING  COMMENT 'Value in postgres (cast to string)',
    mismatch_type       STRING  COMMENT 'value_diff | only_in_lakehouse | only_in_postgres | type_mismatch'
)
USING DELTA
COMMENT 'Row/column-level mismatch details for debugging'
""")

print("✅ All validation metadata tables created/verified")


# In[2]:

# ── Seed healthcare comparison config ─────────────────────────────────────────
# Pre-populate config for the 6 healthcare tables.
# Adjust filter_days, comparison_mode, etc. as needed.

from pyspark.sql import Row
from pyspark.sql.types import *

# ── Optional: reset config / history ─────────────────────────────────────────
if RESET_CONFIG:
    print("⚠️  RESET_CONFIG=True — truncating config tables...")
    for t in ("comparison_config", "comparison_key_columns", "comparison_skip_columns"):
        spark.sql(f"TRUNCATE TABLE validation.{t}")
        print(f"   ✅ truncated validation.{t}")

if RESET_HISTORY:
    print("⚠️  RESET_HISTORY=True — truncating run history...")
    for t in ("comparison_results", "comparison_details"):
        spark.sql(f"TRUNCATE TABLE validation.{t}")
        print(f"   ✅ truncated validation.{t}")

# Check if config already has data to avoid duplicates
existing = spark.sql("SELECT COUNT(*) AS cnt FROM validation.comparison_config").collect()[0]["cnt"]

if existing > 0:
    print(f"⏭️  comparison_config already has {existing} rows — skipping seed")
else:
    HEALTHCARE_TABLES = [
        # (table_name, pk_column, comparison_mode)
        ("departments",   "department_id",   "advanced"),
        ("doctors",       "doctor_id",       "advanced"),
        ("patients",      "patient_id",       "advanced"),
        ("appointments",  "appointment_id",  "hash"),
        ("diagnoses",     "diagnosis_id",    "hash"),
        ("prescriptions", "prescription_id", "hash"),
    ]

    # Insert config rows
    config_rows = []
    for tbl, pk, mode in HEALTHCARE_TABLES:
        config_rows.append(Row(
            table_name=tbl,
            pg_schema="healthcare",
            pg_table_name=tbl,
            lakehouse_schema="silver",
            comparison_mode=mode,
            filter_column="last_updated",
            filter_column_pg=None,
            filter_days=7,
            safety_lag_minutes=30,
            # Finding #20: default to 'fail' for production-critical tables.
            # Drift on these tables (new column, dropped column, mis-aligned
            # ingestion) must surface as a validation FAIL, not be silently
            # masked by intersection.
            schema_drift_policy="fail",
            numeric_tolerance=0.001,
            max_rows_advanced=500000,
            severity="critical",
            enabled=True,
        ))

    config_schema = StructType([
        StructField("table_name", StringType()),
        StructField("pg_schema", StringType()),
        StructField("pg_table_name", StringType()),
        StructField("lakehouse_schema", StringType()),
        StructField("comparison_mode", StringType()),
        StructField("filter_column", StringType()),
        StructField("filter_column_pg", StringType()),
        StructField("filter_days", IntegerType()),
        StructField("safety_lag_minutes", IntegerType()),
        StructField("schema_drift_policy", StringType()),
        StructField("numeric_tolerance", DoubleType()),
        StructField("max_rows_advanced", IntegerType()),
        StructField("severity", StringType()),
        StructField("enabled", BooleanType()),
    ])

    spark.createDataFrame(config_rows, config_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_config")

    # Insert PK columns
    key_rows = [Row(table_name=tbl, column_name=pk, ordinal=1) for tbl, pk, _ in HEALTHCARE_TABLES]
    key_schema = StructType([
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
        StructField("ordinal", IntegerType()),
    ])
    spark.createDataFrame(key_rows, key_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_key_columns")

    # Insert skip columns (skip created_at since Postgres auto-generates it)
    skip_rows = [Row(table_name=tbl, column_name="created_at") for tbl, _, _ in HEALTHCARE_TABLES]
    skip_schema = StructType([
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
    ])
    spark.createDataFrame(skip_rows, skip_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_skip_columns")

    print(f"✅ Seeded {len(HEALTHCARE_TABLES)} table configs, PKs, and skip columns")


# In[3]:

# ── Seed extended data types comparison config ────────────────────────────────
# Adds 3 new tables that test 24+ PostgreSQL data types.
# Uses filter expressions with OR for tables that have both created_at and last_updated.

EXTENDED_TABLES = [
    # (table_name, pk_column, comparison_mode, filter_expression, severity)
    ("data_type_showcase", "id", "advanced",
     "\"created_at\" >= '{window_start}' AND \"created_at\" < '{window_end}' OR \"last_updated\" >= '{window_start}' AND \"last_updated\" < '{window_end}'",
     "warning"),
    ("complex_types_showcase", "id", "hash",
     "\"created_at\" >= '{window_start}' AND \"created_at\" < '{window_end}' OR \"last_updated\" >= '{window_start}' AND \"last_updated\" < '{window_end}'",
     "warning"),
    ("edge_cases", "id", "advanced",
     "\"created_at\" >= '{window_start}' AND \"created_at\" < '{window_end}' OR \"last_updated\" >= '{window_start}' AND \"last_updated\" < '{window_end}'",
     "critical"),
]

# Check if these tables are already configured
existing_tables = [r["table_name"] for r in spark.sql("SELECT table_name FROM validation.comparison_config").collect()]
new_tables = [(t, pk, mode, filt, sev) for t, pk, mode, filt, sev in EXTENDED_TABLES if t not in existing_tables]

if new_tables:
    config_rows = []
    for tbl, pk, mode, filt_expr, severity in new_tables:
        config_rows.append(Row(
            table_name=tbl,
            pg_schema="healthcare",
            pg_table_name=tbl,
            lakehouse_schema="silver",
            comparison_mode=mode,
            filter_column=filt_expr,
            filter_column_pg=None,  # same expression works for both (column names match)
            filter_days=7,
            safety_lag_minutes=30,
            schema_drift_policy="intersect" if severity == "warning" else "fail",
            numeric_tolerance=0.00000001,  # tight tolerance for extended numeric tests
            max_rows_advanced=500000,
            severity=severity,
            enabled=True,
        ))

    config_schema = StructType([
        StructField("table_name", StringType()),
        StructField("pg_schema", StringType()),
        StructField("pg_table_name", StringType()),
        StructField("lakehouse_schema", StringType()),
        StructField("comparison_mode", StringType()),
        StructField("filter_column", StringType()),
        StructField("filter_column_pg", StringType()),
        StructField("filter_days", IntegerType()),
        StructField("safety_lag_minutes", IntegerType()),
        StructField("schema_drift_policy", StringType()),
        StructField("numeric_tolerance", DoubleType()),
        StructField("max_rows_advanced", IntegerType()),
        StructField("severity", StringType()),
        StructField("enabled", BooleanType()),
    ])

    spark.createDataFrame(config_rows, config_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_config")

    # PK columns (all use 'id' as BIGSERIAL PK)
    key_rows = [Row(table_name=tbl, column_name=pk, ordinal=1) for tbl, pk, _, _, _ in new_tables]
    key_schema = StructType([
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
        StructField("ordinal", IntegerType()),
    ])
    spark.createDataFrame(key_rows, key_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_key_columns")

    # Skip columns — columns that can't be meaningfully compared
    skip_entries = [
        ("complex_types_showcase", "search_vector"),   # tsvector formatting varies
        ("complex_types_showcase", "location_point"),  # POINT precision varies
    ]
    skip_rows = [Row(table_name=t, column_name=c) for t, c in skip_entries if t in [x[0] for x in new_tables]]
    if skip_rows:
        skip_schema = StructType([
            StructField("table_name", StringType()),
            StructField("column_name", StringType()),
        ])
        spark.createDataFrame(skip_rows, skip_schema).write \
            .format("delta").mode("append").saveAsTable("validation.comparison_skip_columns")

    print(f"✅ Seeded {len(new_tables)} extended type table configs")
else:
    print(f"⏭️  Extended type tables already configured — skipping")


# In[3b]:

# ── Seed PK-less fixtures (no_pk validation, phases 3/4) ──────────────────────
# These tables intentionally have NO PRIMARY KEY. They are registered with
# enabled=False because the current engine raises ValueError on empty key_cols
# (Phase 1 safety guard at comparison_engine.py:575). Phase 5 will introduce
# `pk_fallback_strategy` and flip these to enabled=True.
#
# TODO(phase5): once pk_fallback_strategy column lands, set enabled=True and add
# pk_fallback_strategy values: event_log='no_pk_hash', sensor_readings='no_pk_hash',
# landing_orders='no_pk_advanced'.

PK_LESS_TABLES = [
    # (table_name, mode, filter_column, filter_days, drift_policy, severity)
    # event_log  → hash-no-PK fixture; strict drift (catches accidental column adds)
    ("event_log",       "hash",     "event_ts",    7, "fail",      "warning"),
    # sensor_readings → hash-no-PK fixture with natural duplicates (multiset behavior)
    ("sensor_readings", "hash",     "reading_ts",  7, "intersect", "warning"),
    # landing_orders → advanced-no-PK fixture; retry-dup payloads exercise exceptAll()
    ("landing_orders",  "advanced", "received_at", 7, "intersect", "warning"),
]

existing_tables = [r["table_name"] for r in spark.sql(
    "SELECT table_name FROM validation.comparison_config"
).collect()]
new_pk_less = [t for t in PK_LESS_TABLES if t[0] not in existing_tables]

if new_pk_less:
    config_rows = []
    for tbl, mode, filt_col, filt_days, drift, sev in new_pk_less:
        config_rows.append(Row(
            table_name=tbl,
            pg_schema="healthcare",
            pg_table_name=tbl,
            lakehouse_schema="silver",
            comparison_mode=mode,
            filter_column=filt_col,
            filter_column_pg=None,
            filter_days=filt_days,
            safety_lag_minutes=30,
            schema_drift_policy=drift,
            numeric_tolerance=0.0,
            max_rows_advanced=500000,
            severity=sev,
            enabled=False,  # phase5 flips to True alongside pk_fallback_strategy
        ))

    config_schema = StructType([
        StructField("table_name", StringType()),
        StructField("pg_schema", StringType()),
        StructField("pg_table_name", StringType()),
        StructField("lakehouse_schema", StringType()),
        StructField("comparison_mode", StringType()),
        StructField("filter_column", StringType()),
        StructField("filter_column_pg", StringType()),
        StructField("filter_days", IntegerType()),
        StructField("safety_lag_minutes", IntegerType()),
        StructField("schema_drift_policy", StringType()),
        StructField("numeric_tolerance", DoubleType()),
        StructField("max_rows_advanced", IntegerType()),
        StructField("severity", StringType()),
        StructField("enabled", BooleanType()),
    ])
    spark.createDataFrame(config_rows, config_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_config")

    # No key_cols and no skip_cols — empty by design. Phase 5 may add skip
    # entries for noisy timestamps if needed.

    print(f"✅ Seeded {len(new_pk_less)} PK-less fixtures (enabled=False — phase5 will enable)")
else:
    print(f"⏭️  PK-less fixtures already configured — skipping")


# In[4]:

# ── Seed advanced fixture: audit_events ───────────────────────────────────────
# This config is intentionally designed to exercise the production hardening:
#   • Composite PK (3 columns) — exposes Finding #19 metadata join fanout if
#     present (engine should NOT duplicate key columns in collected metadata).
#   • Multiple skip columns (4)  — combined with composite PK, the old SQL
#     would have produced 12 rows pre-aggregation per table.
#   • schema_drift_policy='fail' — Finding #20 hardening; any column added to
#     PG but missing in lakehouse (or vice versa) causes a hard FAIL.
#   • filter_column='last_updated' which is TIMESTAMPTZ — exercises the ISO+
#     offset literal path (Finding #21).

AUDIT_FIXTURE = {
    "table_name": "audit_events",
    "pg_schema": "healthcare",
    "pg_table": "audit_events",
    "lakehouse_schema": "silver",
    "comparison_mode": "hash",
    "filter_column": "last_updated",
    "filter_days": 7,
    "schema_drift_policy": "fail",
    "severity": "critical",
    "key_cols": [
        ("tenant_id", 1),
        ("entity_id", 2),
        ("version",   3),
    ],
    "skip_cols": ["etl_load_ts", "etl_source", "etl_batch_id", "created_at"],
}

existing_audit = [r["table_name"] for r in spark.sql(
    "SELECT table_name FROM validation.comparison_config WHERE table_name = 'audit_events'"
).collect()]

if not existing_audit:
    config_rows = [Row(
        table_name=AUDIT_FIXTURE["table_name"],
        pg_schema=AUDIT_FIXTURE["pg_schema"],
        pg_table_name=AUDIT_FIXTURE["pg_table"],
        lakehouse_schema=AUDIT_FIXTURE["lakehouse_schema"],
        comparison_mode=AUDIT_FIXTURE["comparison_mode"],
        filter_column=AUDIT_FIXTURE["filter_column"],
        filter_column_pg=None,
        filter_days=AUDIT_FIXTURE["filter_days"],
        safety_lag_minutes=30,
        schema_drift_policy=AUDIT_FIXTURE["schema_drift_policy"],
        numeric_tolerance=0.0,
        max_rows_advanced=500000,
        severity=AUDIT_FIXTURE["severity"],
        enabled=True,
    )]
    config_schema = StructType([
        StructField("table_name", StringType()),
        StructField("pg_schema", StringType()),
        StructField("pg_table_name", StringType()),
        StructField("lakehouse_schema", StringType()),
        StructField("comparison_mode", StringType()),
        StructField("filter_column", StringType()),
        StructField("filter_column_pg", StringType()),
        StructField("filter_days", IntegerType()),
        StructField("safety_lag_minutes", IntegerType()),
        StructField("schema_drift_policy", StringType()),
        StructField("numeric_tolerance", DoubleType()),
        StructField("max_rows_advanced", IntegerType()),
        StructField("severity", StringType()),
        StructField("enabled", BooleanType()),
    ])
    spark.createDataFrame(config_rows, config_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_config")

    key_rows = [Row(table_name="audit_events", column_name=c, ordinal=o)
                for c, o in AUDIT_FIXTURE["key_cols"]]
    key_schema = StructType([
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
        StructField("ordinal", IntegerType()),
    ])
    spark.createDataFrame(key_rows, key_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_key_columns")

    skip_rows = [Row(table_name="audit_events", column_name=c)
                 for c in AUDIT_FIXTURE["skip_cols"]]
    skip_schema = StructType([
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
    ])
    spark.createDataFrame(skip_rows, skip_schema).write \
        .format("delta").mode("append").saveAsTable("validation.comparison_skip_columns")

    print(f"✅ Seeded audit_events fixture (3-col PK × 4 skip cols, schema_drift=fail, TIMESTAMPTZ filter)")
else:
    print(f"⏭️  audit_events fixture already configured — skipping")


# In[5]:

# ── Show current config ───────────────────────────────────────────────────────

print("📋 Comparison Config:")
spark.sql("SELECT table_name, comparison_mode, filter_column, filter_days, severity, enabled FROM validation.comparison_config").show(truncate=False)

print("🔑 Key Columns:")
spark.sql("SELECT * FROM validation.comparison_key_columns ORDER BY table_name, ordinal").show(truncate=False)

print("⏭️  Skip Columns:")
spark.sql("SELECT * FROM validation.comparison_skip_columns ORDER BY table_name").show(truncate=False)

print("✅ Setup complete. Run the comparison engine notebook next.")

if SETUP_AUDIT_TABLES:
    print("\n🔧 SETUP_AUDIT_TABLES=True — invoking validation_audit_setup ...")
    notebookutils.notebook.run("validation_audit_setup", 600, {})
    print("✅ Audit tables ready in SQL DB")

if SETUP_SCENARIOS:
    print("\n🔧 SETUP_SCENARIOS=True — invoking scenario_setup ...")
    notebookutils.notebook.run("scenario_setup", 600, {})
    print("✅ Scenarios seeded in validation.scenarios")
