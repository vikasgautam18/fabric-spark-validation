#!/usr/bin/env python
# coding: utf-8

# ## Validation Suite — Scenario Driver Setup
#
# Creates the lakehouse driver table that controls scenario test execution
# and seeds an initial set of scenarios. Idempotent: re-running upserts the
# same scenarios (delete-then-insert by scenario_id).
#
# Scenarios test that the comparison_engine correctly DETECTS specific drift
# conditions. Each scenario has a target_table, a mutation_type applied to the
# lakehouse silver copy, and an expected_status the engine must report.
#
# The seeder + assert notebooks read this table; the parent pipeline iterates
# enabled rows.

# In[1]:

# ── Configuration ─────────────────────────────────────────────────────────────

# Set to True to drop and recreate the scenarios table (wipes all rows).
RESET_SCENARIOS = False


# In[2]:

# ── Create scenarios driver table ────────────────────────────────────────────

spark.sql("CREATE SCHEMA IF NOT EXISTS validation")

if RESET_SCENARIOS:
    print("⚠️  RESET_SCENARIOS=True — dropping validation.scenarios")
    spark.sql("DROP TABLE IF EXISTS validation.scenarios")

spark.sql("""
CREATE TABLE IF NOT EXISTS validation.scenarios (
    scenario_id            STRING  COMMENT 'Unique scenario identifier (kebab-case)',
    description            STRING  COMMENT 'Human-readable scenario description',
    target_table           STRING  COMMENT 'Single table_name from comparison_config to test',
    mutation_type          STRING  COMMENT 'noop | delete_rows | insert_extra_rows | update_column | add_extra_column | drop_column | null_out_pk',
    mutation_params        STRING  COMMENT 'JSON params for the mutation (e.g. {"count": 50, "column": "notes"})',
    expected_status        STRING  COMMENT 'pass | count_mismatch | hash_mismatch | schema_drift | error',
    valid_comparison_modes STRING  COMMENT 'Comma-separated list: basic,hash,advanced. Scenarios skipped (not_applicable) if active mode not listed',
    enabled                BOOLEAN COMMENT 'False to skip in pipeline iteration',
    created_at             TIMESTAMP
) USING DELTA
""")

print("✅ validation.scenarios created (or already exists)")


# In[3]:

# ── Seed initial scenarios (upsert by scenario_id) ───────────────────────────
#
# Target table: appointments (hash mode, has updated_at filter column).
# Each scenario is intentionally small (50 rows) so it executes quickly.

from datetime import datetime
from pyspark.sql import Row

NOW = datetime.utcnow()

SCENARIOS = [
    # ── Baseline control ────────────────────────────────────────────────────
    Row(
        scenario_id="baseline-appointments",
        description="No mutation — sanity check that engine reports pass on a clean import",
        target_table="appointments",
        mutation_type="noop",
        mutation_params="{}",
        expected_status="pass",
        valid_comparison_modes="basic,hash,advanced",
        enabled=True,
        created_at=NOW,
    ),

    # ── Count drift ─────────────────────────────────────────────────────────
    Row(
        scenario_id="delete-rows-appointments",
        description="Delete 50 rows from LH → engine should detect count drift",
        target_table="appointments",
        mutation_type="delete_rows",
        mutation_params='{"count": 50}',
        expected_status="count_mismatch",
        valid_comparison_modes="basic",
        enabled=True,
        created_at=NOW,
    ),
    Row(
        scenario_id="delete-rows-appointments-hash",
        description="Delete 50 rows from LH (hash mode short-circuits to count_mismatch)",
        target_table="appointments",
        mutation_type="delete_rows",
        mutation_params='{"count": 50}',
        expected_status="count_mismatch",
        valid_comparison_modes="hash,advanced",
        enabled=True,
        created_at=NOW,
    ),
    Row(
        scenario_id="extra-rows-appointments",
        description="Insert 30 extra rows in LH → count_mismatch (count diff short-circuits hash)",
        target_table="appointments",
        mutation_type="insert_extra_rows",
        mutation_params='{"count": 30}',
        expected_status="count_mismatch",
        valid_comparison_modes="hash,advanced",
        enabled=True,
        created_at=NOW,
    ),

    # ── Value drift (hash/advanced only) ────────────────────────────────────
    Row(
        scenario_id="update-column-appointments",
        description="Update diagnosis on 20 rows in LH → hash_mismatch",
        target_table="appointments",
        mutation_type="update_column",
        mutation_params='{"column": "notes", "count": 20}',
        expected_status="hash_mismatch",
        valid_comparison_modes="hash,advanced",
        enabled=True,
        created_at=NOW,
    ),

    # ── Schema drift ────────────────────────────────────────────────────────
    Row(
        scenario_id="add-column-appointments",
        description="Add an unexpected column to LH → schema_drift (policy=fail)",
        target_table="appointments",
        mutation_type="add_extra_column",
        mutation_params='{"column_name": "scenario_extra"}',
        expected_status="schema_drift",
        valid_comparison_modes="hash,advanced",
        enabled=True,
        created_at=NOW,
    ),
]

# Idempotent upsert: delete by scenario_id then append
ids = [s["scenario_id"] for s in SCENARIOS]
existing = spark.sql(
    f"SELECT scenario_id FROM validation.scenarios WHERE scenario_id IN ({','.join(repr(i) for i in ids)})"
).count()
if existing:
    spark.sql(
        f"DELETE FROM validation.scenarios WHERE scenario_id IN ({','.join(repr(i) for i in ids)})"
    )
    print(f"  refreshing {existing} existing scenario(s)")

spark.createDataFrame(SCENARIOS).write \
    .format("delta").mode("append").saveAsTable("validation.scenarios")

print(f"✅ Seeded {len(SCENARIOS)} scenario(s)")


# In[4]:

# ── Show seeded rows ─────────────────────────────────────────────────────────

spark.sql("""
    SELECT scenario_id, target_table, mutation_type, expected_status,
           valid_comparison_modes, enabled
    FROM validation.scenarios
    ORDER BY target_table, scenario_id
""").show(truncate=False)
