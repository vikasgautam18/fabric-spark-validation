#!/usr/bin/env python
# coding: utf-8

# ## Validation Audit Setup
#
# Idempotently creates the SQL DB tables used to audit validation runs,
# per-table results, and scenario-test outcomes.
#
# Tables (in `cfg["sql_database"]`, schema `cfg["sql_schema"]`):
#   1. validation_runs              — one row per engine invocation
#   2. validation_results_history   — one row per table per run
#   3. validation_scenario_runs     — one row per scenario test execution
#
# Run order:
#   - Standalone (one-off bootstrap), or
#   - Invoked from `comparison_setup.py` when `SETUP_AUDIT_TABLES = True`.
#
# Re-running is safe — every CREATE is wrapped in `IF NOT EXISTS`.

# In[1]:

# ── Parameters ───────────────────────────────────────────────────────────────

# Drop and re-create (DESTRUCTIVE). Default False so nightly invocations
# from comparison_setup are non-destructive.
DROP_AND_RECREATE = False


# In[2]:

%run _common


# In[3]:

# ── DDL ──────────────────────────────────────────────────────────────────────

_schema = safe_ident(cfg["sql_schema"], kind="sql_schema")

_ddl_statements = [
    # 1. validation_runs — one row per engine invocation
    f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{_schema}' AND t.name = 'validation_runs'
    )
    CREATE TABLE [{_schema}].[validation_runs] (
        run_id              UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        started_at          DATETIME2(3)     NOT NULL,
        ended_at            DATETIME2(3)     NULL,
        duration_sec        DECIMAL(12,2)    NULL,
        status              VARCHAR(20)      NOT NULL,
        triggered_by        VARCHAR(40)      NOT NULL,
        scenario_id         VARCHAR(80)      NULL,
        pipeline_run_id     VARCHAR(80)      NULL,
        notebook_version    VARCHAR(80)      NULL,
        total_tables        INT              NULL,
        pass_count          INT              NULL,
        fail_count          INT              NULL,
        error_message       NVARCHAR(MAX)    NULL
    );
    """,

    # 2. validation_results_history — one row per (run_id, table_name)
    f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{_schema}' AND t.name = 'validation_results_history'
    )
    CREATE TABLE [{_schema}].[validation_results_history] (
        run_id              UNIQUEIDENTIFIER NOT NULL,
        table_name          VARCHAR(255)     NOT NULL,
        comparison_mode     VARCHAR(20)      NOT NULL,
        status              VARCHAR(40)      NOT NULL,
        pg_count            BIGINT           NULL,
        lh_count            BIGINT           NULL,
        mismatch_count      BIGINT           NULL,
        window_start        DATETIME2(3)     NULL,
        window_end          DATETIME2(3)     NULL,
        started_at          DATETIME2(3)     NOT NULL,
        ended_at            DATETIME2(3)     NOT NULL,
        duration_sec        DECIMAL(12,2)    NOT NULL,
        error_message       NVARCHAR(MAX)    NULL,
        CONSTRAINT pk_validation_results_history
            PRIMARY KEY (run_id, table_name)
    );
    """,

    # 3. validation_scenario_runs — one row per scenario test execution
    f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{_schema}' AND t.name = 'validation_scenario_runs'
    )
    CREATE TABLE [{_schema}].[validation_scenario_runs] (
        scenario_run_id     UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        scenario_id         VARCHAR(80)      NOT NULL,
        target_table        VARCHAR(255)     NOT NULL,
        mutation_type       VARCHAR(40)      NOT NULL,
        engine_run_id       UNIQUEIDENTIFIER NULL,
        expected_status     VARCHAR(40)      NOT NULL,
        actual_status       VARCHAR(40)      NULL,
        evidence_ok         BIT              NULL,
        verdict             VARCHAR(20)      NOT NULL,
        notes               NVARCHAR(MAX)    NULL,
        pipeline_run_id     VARCHAR(80)      NULL,
        created_at          DATETIME2(3)     NOT NULL
    );
    """,

    # 4. validation_mismatch_samples — sampled PK-level forensic rows
    #    Capped at MAX_DETAIL_ROWS per (run_id, table_name, mismatch_type) by
    #    the engine. Lets you answer "which rows are missing/different?"
    f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{_schema}' AND t.name = 'validation_mismatch_samples'
    )
    CREATE TABLE [{_schema}].[validation_mismatch_samples] (
        run_id              UNIQUEIDENTIFIER NOT NULL,
        table_name          VARCHAR(255)     NOT NULL,
        sample_seq          INT              NOT NULL,
        mismatch_type       VARCHAR(30)      NOT NULL,
        pk_values           NVARCHAR(MAX)    NOT NULL,
        column_name         VARCHAR(128)     NULL,
        lakehouse_value     NVARCHAR(MAX)    NULL,
        postgres_value      NVARCHAR(MAX)    NULL,
        captured_at         DATETIME2(3)     NOT NULL,
        CONSTRAINT pk_validation_mismatch_samples
            PRIMARY KEY (run_id, table_name, sample_seq)
    );
    """,
]

# Optional secondary indexes — created idempotently in separate batch.
_index_statements = [
    f"""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'ix_results_history_table'
                     AND object_id = OBJECT_ID('[{_schema}].[validation_results_history]'))
    CREATE INDEX ix_results_history_table
        ON [{_schema}].[validation_results_history] (table_name, started_at);
    """,
    f"""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'ix_runs_started'
                     AND object_id = OBJECT_ID('[{_schema}].[validation_runs]'))
    CREATE INDEX ix_runs_started
        ON [{_schema}].[validation_runs] (started_at DESC);
    """,
    f"""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'ix_scenario_runs_pipeline'
                     AND object_id = OBJECT_ID('[{_schema}].[validation_scenario_runs]'))
    CREATE INDEX ix_scenario_runs_pipeline
        ON [{_schema}].[validation_scenario_runs] (pipeline_run_id, scenario_id);
    """,
    f"""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'ix_mismatch_samples_lookup'
                     AND object_id = OBJECT_ID('[{_schema}].[validation_mismatch_samples]'))
    CREATE INDEX ix_mismatch_samples_lookup
        ON [{_schema}].[validation_mismatch_samples] (run_id, table_name, mismatch_type);
    """,
]


# Idempotent migrations for validation_scenario_runs schema. Running the
# DDL above on an existing table is a no-op (IF NOT EXISTS). These ALTERs
# bring an old-shape table to the new shape (add target_table, mutation_type,
# evidence_ok, created_at; drop scenario_name/started_at/ended_at/duration_sec
# if they exist).
_migration_statements = [
    f"""
    IF (EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
               WHERE s.name='{_schema}' AND t.name='validation_scenario_runs')
        AND NOT EXISTS (SELECT 1 FROM sys.columns
                    WHERE Name = 'target_table'
                      AND Object_ID = OBJECT_ID('[{_schema}].[validation_scenario_runs]')))
    ALTER TABLE [{_schema}].[validation_scenario_runs]
        ADD target_table VARCHAR(255) NOT NULL DEFAULT '';
    """,
    f"""
    IF (EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
               WHERE s.name='{_schema}' AND t.name='validation_scenario_runs')
        AND NOT EXISTS (SELECT 1 FROM sys.columns
                    WHERE Name = 'mutation_type'
                      AND Object_ID = OBJECT_ID('[{_schema}].[validation_scenario_runs]')))
    ALTER TABLE [{_schema}].[validation_scenario_runs]
        ADD mutation_type VARCHAR(40) NOT NULL DEFAULT '';
    """,
    f"""
    IF (EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
               WHERE s.name='{_schema}' AND t.name='validation_scenario_runs')
        AND NOT EXISTS (SELECT 1 FROM sys.columns
                    WHERE Name = 'evidence_ok'
                      AND Object_ID = OBJECT_ID('[{_schema}].[validation_scenario_runs]')))
    ALTER TABLE [{_schema}].[validation_scenario_runs]
        ADD evidence_ok BIT NULL;
    """,
    f"""
    IF (EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
               WHERE s.name='{_schema}' AND t.name='validation_scenario_runs')
        AND NOT EXISTS (SELECT 1 FROM sys.columns
                    WHERE Name = 'created_at'
                      AND Object_ID = OBJECT_ID('[{_schema}].[validation_scenario_runs]')))
    ALTER TABLE [{_schema}].[validation_scenario_runs]
        ADD created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME();
    """,
]

# Relax legacy NOT NULL columns from the old shape so the new INSERT (which
# does not supply them) succeeds. Safe to run repeatedly: ALTER COLUMN to the
# same nullability is a no-op.
for _legacy_col, _legacy_type in [
    ("scenario_name", "VARCHAR(255)"),
    ("started_at",    "DATETIME2(3)"),
    ("ended_at",      "DATETIME2(3)"),
    ("duration_sec",  "DECIMAL(12,2)"),
]:
    _migration_statements.append(f"""
    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE Name = '{_legacy_col}'
                 AND Object_ID = OBJECT_ID('[{_schema}].[validation_scenario_runs]'))
    ALTER TABLE [{_schema}].[validation_scenario_runs]
        ALTER COLUMN [{_legacy_col}] {_legacy_type} NULL;
    """)

# Widen `verdict` if a legacy table created it as VARCHAR(10) — the new
# values include 'not_applicable' (14) and 'inconclusive' (12).
_migration_statements.append(f"""
IF EXISTS (SELECT 1 FROM sys.columns
           WHERE Name = 'verdict'
             AND Object_ID = OBJECT_ID('[{_schema}].[validation_scenario_runs]'))
ALTER TABLE [{_schema}].[validation_scenario_runs]
    ALTER COLUMN [verdict] VARCHAR(20) NOT NULL;
""")


# In[4]:

# ── Apply ────────────────────────────────────────────────────────────────────

if DROP_AND_RECREATE:
    print("⚠️  DROP_AND_RECREATE=True — destructively dropping audit tables")
    for tbl in ("validation_results_history", "validation_mismatch_samples",
                "validation_runs", "validation_scenario_runs"):
        run_tsql(f"DROP TABLE IF EXISTS [{_schema}].[{tbl}];")

print("Creating audit tables (idempotent) ...")
for ddl in _ddl_statements:
    run_tsql(ddl)
    print("  ✓ table")

print("Migrating validation_scenario_runs schema (idempotent) ...")
for stmt in _migration_statements:
    run_tsql(stmt)
    print("  ✓ migration")

print("Creating indexes (idempotent) ...")
for idx in _index_statements:
    run_tsql(idx)
    print("  ✓ index")

print(f"\n✅ Audit tables ready in {cfg['sql_database']}.{_schema}:")
print(f"  • validation_runs")
print(f"  • validation_results_history")
print(f"  • validation_scenario_runs")
print(f"  • validation_mismatch_samples")
