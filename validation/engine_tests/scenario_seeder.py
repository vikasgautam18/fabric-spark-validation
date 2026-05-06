#!/usr/bin/env python
# coding: utf-8

# ## Validation Suite — Scenario Seeder
#
# Reads a single scenario from `validation.scenarios`, validates it is
# applicable to the target table's active comparison_config, then applies
# the mutation_type to the lakehouse silver table.
#
# Mutations are applied in-window (timestamps set to `now()`) so the engine
# will see them within its default lookback window.
#
# This notebook is invoked PER SCENARIO from the child pipeline. It expects
# the lakehouse table to already be in baseline state — the pipeline runs
# `getDataFromPostgres` (re-import) BEFORE invoking this notebook.

# In[1]:


%run _common


# In[2]:

# ── Parameters (set from pipeline) ───────────────────────────────────────────

# This cell must have the `parameters` tag.
scenario_id     = "baseline-appointments"   # required — overridden by pipeline
pipeline_run_id = None                      # optional — set by pipeline so the
                                            # not_applicable verdict row links
                                            # back to the pipeline run.


# In[3]:

# ── Imports + load scenario row ──────────────────────────────────────────────

import json
from datetime import datetime
from pyspark.sql import functions as F

print(f"▶ scenario_seeder: scenario_id={scenario_id}")

scen_rows = spark.sql(
    f"SELECT * FROM validation.scenarios WHERE scenario_id = '{scenario_id}'"
).collect()

if not scen_rows:
    raise RuntimeError(f"scenario_id '{scenario_id}' not found in validation.scenarios")

s = scen_rows[0]
target_table = s["target_table"]
mutation_type = s["mutation_type"]
mutation_params = json.loads(s["mutation_params"] or "{}")
expected_status = s["expected_status"]
valid_modes = [m.strip() for m in (s["valid_comparison_modes"] or "").split(",") if m.strip()]

print(f"  target_table     = {target_table}")
print(f"  mutation_type    = {mutation_type}")
print(f"  mutation_params  = {mutation_params}")
print(f"  expected_status  = {expected_status}")
print(f"  valid_modes      = {valid_modes}")


# In[4]:

# ── Load active comparison_config row + applicability check ──────────────────

cfg_rows = spark.sql(f"""
    SELECT c.table_name, c.lakehouse_schema, c.comparison_mode, c.filter_column,
           c.schema_drift_policy, c.enabled
    FROM validation.comparison_config c
    WHERE c.table_name = '{target_table}'
""").collect()

if not cfg_rows:
    raise RuntimeError(f"target_table '{target_table}' has no row in validation.comparison_config")

c = cfg_rows[0]
if not c["enabled"]:
    raise RuntimeError(f"target_table '{target_table}' is disabled in comparison_config — re-enable to test")

active_mode = c["comparison_mode"]
lh_schema = c["lakehouse_schema"]
filter_col = c["filter_column"]
drift_policy = c["schema_drift_policy"]

print(f"  active_mode      = {active_mode}")
print(f"  lh_schema        = {lh_schema}")
print(f"  filter_column    = {filter_col}")
print(f"  drift_policy     = {drift_policy}")

# Applicability gate. If the scenario doesn't apply to the active mode, mark
# the run as skipped — the rest of the seeder is gated on _SHOULD_RUN and the
# notebook will exit with "not_applicable" at the very end (last statement).
_SHOULD_RUN = True
_SKIP_REASON = None

if valid_modes and active_mode not in valid_modes:
    print(f"⚠️  scenario '{scenario_id}' is NOT applicable to mode '{active_mode}' "
          f"(valid: {valid_modes}). Skipping mutation.")
    _SHOULD_RUN = False
    _SKIP_REASON = f"mode {active_mode} not in {valid_modes}"


# Skip schema-drift scenarios when policy isn't 'fail' — they won't trigger.
if _SHOULD_RUN and mutation_type in ("add_extra_column", "drop_column") and drift_policy != "fail":
    print(f"⚠️  schema-drift scenario but drift_policy='{drift_policy}' — engine "
          f"won't classify as schema_drift. Skipping.")
    _SHOULD_RUN = False
    _SKIP_REASON = f"drift_policy={drift_policy} (need 'fail')"


# In[5]:

# ── Pre-flight: validate column choice for column-targeting mutations ────────

skip_cols = set()
key_cols = set()

for r in spark.sql(
    f"SELECT column_name FROM validation.comparison_skip_columns WHERE table_name='{target_table}'"
).collect():
    skip_cols.add(r["column_name"])

for r in spark.sql(
    f"SELECT column_name FROM validation.comparison_key_columns WHERE table_name='{target_table}'"
).collect():
    key_cols.add(r["column_name"])

# `filter_col` may be an expression — but we only validate against simple column names
filter_col_simple = filter_col if filter_col and " " not in filter_col else None


def _assert_column_safe(col):
    if col in key_cols:
        raise RuntimeError(f"mutation column '{col}' is a PK column — would change row identity")
    if col in skip_cols:
        raise RuntimeError(f"mutation column '{col}' is in skip list — engine would not detect drift")
    if filter_col_simple and col == filter_col_simple:
        raise RuntimeError(f"mutation column '{col}' is the filter column — would move rows out of window")


# In[6]:

# ── Mutation dispatchers (all operate on lh_schema.{target_table}) ───────────

fqn = f"{lh_schema}.{target_table}"
print(f"\n▶ Applying mutation '{mutation_type}' to {fqn}")


def _list_pk_cols():
    rows = spark.sql(
        f"SELECT column_name FROM validation.comparison_key_columns "
        f"WHERE table_name='{target_table}' ORDER BY ordinal"
    ).collect()
    return [r["column_name"] for r in rows]


def mutation_noop():
    print("  noop — no changes applied")


def mutation_delete_rows():
    n = int(mutation_params.get("count", 50))
    pks = _list_pk_cols()
    if not pks:
        raise RuntimeError(f"delete_rows requires PK columns but none defined for {target_table}")
    pk_csv = ", ".join(pks)
    # Pick the MOST RECENT N rows by filter_column so they are guaranteed to
    # fall inside the engine's lookback window. Falls back to PK order if the
    # filter_column is an expression (rare).
    order_by = filter_col_simple + " DESC" if filter_col_simple else pk_csv
    victim_rows = spark.sql(
        f"SELECT {pk_csv} FROM {fqn} ORDER BY {order_by} LIMIT {n}"
    ).collect()
    if not victim_rows:
        raise RuntimeError(f"{fqn} is empty — cannot delete rows. Re-import baseline first.")
    actual = len(victim_rows)
    if actual < n:
        print(f"  ⚠️  only {actual} rows available, requested {n}")
    # Build IN-list predicate from PK tuples (works for single + composite)
    if len(pks) == 1:
        vals = ", ".join(repr(r[pks[0]]) for r in victim_rows)
        predicate = f"{pks[0]} IN ({vals})"
    else:
        clauses = []
        for r in victim_rows:
            parts = " AND ".join(f"{p} = {repr(r[p])}" for p in pks)
            clauses.append(f"({parts})")
        predicate = " OR ".join(clauses)
    spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
    print(f"  ✅ deleted {actual} row(s)")


def mutation_insert_extra_rows():
    n = int(mutation_params.get("count", 30))
    pks = _list_pk_cols()
    if not pks:
        raise RuntimeError(f"insert_extra_rows requires PK columns but none defined for {target_table}")
    # Clone N existing rows and rewrite the PK to a synthetic non-colliding value.
    # Strategy: append a large offset to the PK if it's numeric; otherwise prefix string.
    sample_df = spark.sql(f"SELECT * FROM {fqn} LIMIT {n}")
    if sample_df.count() == 0:
        raise RuntimeError(f"{fqn} is empty — cannot synthesize rows. Re-import baseline first.")

    # Detect first PK type
    pk_field = next(f for f in sample_df.schema.fields if f.name == pks[0])
    is_numeric = pk_field.dataType.typeName() in ("integer", "long", "short", "byte", "decimal", "double", "float")

    # Find a safe offset: max(pk) + 10_000_000 for numeric; "scn_test_" prefix for string
    if is_numeric:
        max_pk = spark.sql(f"SELECT MAX({pks[0]}) AS m FROM {fqn}").collect()[0]["m"]
        offset = (max_pk or 0) + 10_000_000
        synth_df = sample_df.withColumn(pks[0], F.col(pks[0]) + F.lit(offset))
    else:
        synth_df = sample_df.withColumn(pks[0], F.concat(F.lit("scn_test_"), F.col(pks[0]).cast("string")))

    # Set filter column to now() so rows fall inside engine window (if it's a timestamp column)
    if filter_col_simple:
        ftype = next((f.dataType.typeName() for f in synth_df.schema.fields if f.name == filter_col_simple), None)
        if ftype in ("timestamp",):
            synth_df = synth_df.withColumn(filter_col_simple, F.expr("current_timestamp() - INTERVAL 1 HOUR"))

    synth_df.write.format("delta").mode("append").saveAsTable(fqn)
    print(f"  ✅ inserted {synth_df.count()} synthetic row(s)")


def mutation_update_column():
    col = mutation_params["column"]
    n = int(mutation_params.get("count", 20))
    _assert_column_safe(col)
    pks = _list_pk_cols()
    if not pks:
        raise RuntimeError(f"update_column requires PK columns")
    pk_csv = ", ".join(pks)
    order_by = filter_col_simple + " DESC" if filter_col_simple else pk_csv
    victim_rows = spark.sql(
        f"SELECT {pk_csv} FROM {fqn} ORDER BY {order_by} LIMIT {n}"
    ).collect()
    if not victim_rows:
        raise RuntimeError(f"{fqn} is empty")
    if len(pks) == 1:
        vals = ", ".join(repr(r[pks[0]]) for r in victim_rows)
        predicate = f"{pks[0]} IN ({vals})"
    else:
        clauses = []
        for r in victim_rows:
            parts = " AND ".join(f"{p} = {repr(r[p])}" for p in pks)
            clauses.append(f"({parts})")
        predicate = " OR ".join(clauses)
    sentinel = f"SCENARIO_{scenario_id}_{datetime.utcnow().strftime('%H%M%S')}"
    # Bump filter timestamp too so rows stay in-window (if it's a simple ts col)
    extra = ""
    if filter_col_simple:
        ftype = next((f.dataType.typeName() for f in spark.table(fqn).schema.fields if f.name == filter_col_simple), None)
        if ftype == "timestamp":
            extra = f", {filter_col_simple} = current_timestamp() - INTERVAL 1 HOUR"
    spark.sql(f"UPDATE {fqn} SET {col} = '{sentinel}'{extra} WHERE {predicate}")
    print(f"  ✅ updated {len(victim_rows)} row(s) — set {col} = '{sentinel}'")


def mutation_add_extra_column():
    new_col = mutation_params.get("column_name", "scenario_extra")
    safe_ident(new_col)
    spark.sql(f"ALTER TABLE {fqn} ADD COLUMN ({new_col} STRING)")
    print(f"  ✅ added column {new_col}")


def mutation_drop_column():
    col = mutation_params["column_name"]
    _assert_column_safe(col)
    safe_ident(col)
    # Delta requires column mapping enabled to drop columns
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
              f"'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')")
    spark.sql(f"ALTER TABLE {fqn} DROP COLUMN {col}")
    print(f"  ✅ dropped column {col}")


def mutation_null_out_pk():
    n = int(mutation_params.get("count", 5))
    pks = _list_pk_cols()
    if not pks:
        raise RuntimeError(f"null_out_pk requires PK columns")
    # Set the FIRST pk to null on N rows. Engine's PK-quality check (hash mode)
    # should detect this and report DATA_QUALITY_ERROR (currently mapped to 'error').
    # Note: Delta won't block null insertion unless explicit NOT NULL constraint.
    pk_csv = ", ".join(pks)
    victim_rows = spark.sql(
        f"SELECT {pk_csv} FROM {fqn} WHERE {pks[0]} IS NOT NULL ORDER BY {pk_csv} LIMIT {n}"
    ).collect()
    if not victim_rows:
        raise RuntimeError(f"{fqn} has no rows to null PK on")
    vals = ", ".join(repr(r[pks[0]]) for r in victim_rows)
    spark.sql(f"UPDATE {fqn} SET {pks[0]} = NULL WHERE {pks[0]} IN ({vals})")
    print(f"  ✅ nulled {pks[0]} on {len(victim_rows)} row(s)")


# Per-scenario backup table for clear_key_cols. The asserter MUST restore from
# this table after reading the audit row, otherwise the next pipeline run finds
# the metadata still missing and every subsequent comparison for the table fails.
_PK_BACKUP_TABLE = "validation.comparison_key_columns_test_backup"


def mutation_clear_key_cols():
    """Snapshot then delete validation.comparison_key_columns rows for the target.

    Used by no-PK safety-guard regression scenarios. The asserter reads back
    from `_PK_BACKUP_TABLE` keyed on (scenario_id, table_name) and restores.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_PK_BACKUP_TABLE} (
            scenario_id  STRING,
            table_name   STRING,
            column_name  STRING,
            ordinal      INT,
            backed_up_at TIMESTAMP
        ) USING DELTA
    """)
    # Idempotent: clear any prior backup for this (scenario, table) pair before snapshotting.
    spark.sql(
        f"DELETE FROM {_PK_BACKUP_TABLE} "
        f"WHERE scenario_id = '{scenario_id}' AND table_name = '{target_table}'"
    )
    snap = spark.sql(
        f"SELECT '{scenario_id}' AS scenario_id, table_name, column_name, ordinal, "
        f"       current_timestamp() AS backed_up_at "
        f"FROM validation.comparison_key_columns WHERE table_name = '{target_table}'"
    )
    snap_count = snap.count()
    if snap_count == 0:
        raise RuntimeError(
            f"clear_key_cols: target_table '{target_table}' has no key_columns to clear "
            f"— scenario assumes pre-existing PKs"
        )
    snap.write.format("delta").mode("append").saveAsTable(_PK_BACKUP_TABLE)
    spark.sql(f"DELETE FROM validation.comparison_key_columns WHERE table_name = '{target_table}'")
    print(f"  ✅ backed up {snap_count} key_column row(s) to {_PK_BACKUP_TABLE} and cleared metadata")


def _pick_in_window_rows(n):
    """Return the most-recent N rows from {fqn} as a list of pyspark Row objects.

    Uses filter_col DESC if available so rows are guaranteed in-window for the
    next engine run. Falls back to LIMIT-only ordering when filter_col is an
    expression. Used by the no_pk mutations which cannot rely on PK-based row
    selection.
    """
    if filter_col_simple:
        rows = spark.sql(
            f"SELECT * FROM {fqn} ORDER BY {filter_col_simple} DESC LIMIT {n}"
        ).collect()
    else:
        rows = spark.sql(f"SELECT * FROM {fqn} LIMIT {n}").collect()
    if not rows:
        raise RuntimeError(f"{fqn} is empty — cannot select rows. Re-import baseline first.")
    return rows


def _bump_filter_into_window(df):
    """If filter_col is a timestamp column, set it to current_timestamp - 1h so
    the appended rows fall inside the engine lookback window."""
    if not filter_col_simple:
        return df
    ftype = next(
        (f.dataType.typeName() for f in df.schema.fields if f.name == filter_col_simple),
        None,
    )
    if ftype == "timestamp":
        return df.withColumn(filter_col_simple, F.expr("current_timestamp() - INTERVAL 1 HOUR"))
    return df


def mutation_delete_rows_no_pk():
    """Delete N rows from a PK-less table by snapshotting all columns and
    deleting on a row-equality predicate. Uses filter_col DESC to target
    in-window rows so the engine actually sees the drift.

    Built for tables with `pk_fallback_strategy='no_pk_hash'` /
    `'no_pk_advanced'` — never call on tables with declared key_cols.
    """
    n = int(mutation_params.get("count", 1))
    victim_rows = _pick_in_window_rows(n)
    actual = len(victim_rows)
    cols = [f.name for f in spark.table(fqn).schema.fields]

    def _lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, bool):
            return "true" if v else "false"
        # datetimes, decimals, numerics — delegate to repr() then strip Python wrappers
        s = repr(v)
        # Python datetime repr is `datetime.datetime(2024, 1, 1, ...)`; safer to
        # cast string. Use timestamp() for datetime-like:
        from datetime import datetime as _dt, date as _date
        if isinstance(v, (_dt, _date)):
            return "TIMESTAMP '" + v.isoformat(sep=" ") + "'"
        return s

    clauses = []
    for r in victim_rows:
        parts = []
        for c in cols:
            val = r[c]
            if val is None:
                parts.append(f"{c} IS NULL")
            else:
                parts.append(f"{c} = {_lit(val)}")
        clauses.append("(" + " AND ".join(parts) + ")")
    predicate = " OR ".join(clauses)
    # DELETE may match more than `n` if rows are exact-duplicates. That is
    # acceptable for a no-PK fixture and the asserter's count-delta tolerance
    # absorbs the variance.
    spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
    print(f"  ✅ deleted up to {actual} row(s) (no-PK row-equality predicate)")


def mutation_insert_extra_rows_no_pk():
    """Append N cloned rows to a PK-less table.

    mutation_params:
      count   (int, default 1)
      mutate  (bool, default False) — when True, alters one non-filter column to
              guarantee the appended row is byte-distinct from any existing
              row (exercises the only_in_lakehouse_multiset path). When False,
              produces a verbatim duplicate (exercises the multiset_count_diff
              path).
    """
    n = int(mutation_params.get("count", 1))
    mutate = bool(mutation_params.get("mutate", False))
    victim_rows = _pick_in_window_rows(n)
    sample_df = spark.createDataFrame(victim_rows, spark.table(fqn).schema)
    sample_df = _bump_filter_into_window(sample_df)

    if mutate:
        # Pick a string column that isn't the filter col — append a sentinel.
        # If no string col exists, fall back to a numeric col + small offset.
        str_cols = [
            f.name for f in sample_df.schema.fields
            if f.dataType.typeName() == "string" and f.name != filter_col_simple
        ]
        if str_cols:
            target_col = str_cols[0]
            sentinel = f"_scn_{scenario_id[:32]}"
            sample_df = sample_df.withColumn(
                target_col,
                F.concat(F.coalesce(F.col(target_col), F.lit("")), F.lit(sentinel)),
            )
            print(f"  · mutated column '{target_col}' (+sentinel) to ensure uniqueness")
        else:
            num_cols = [
                f.name for f in sample_df.schema.fields
                if f.dataType.typeName() in ("integer", "long", "double", "float", "decimal")
                and f.name != filter_col_simple
            ]
            if not num_cols:
                raise RuntimeError(
                    "insert_extra_rows_no_pk(mutate=True) needs a string or numeric "
                    "non-filter column to mutate"
                )
            target_col = num_cols[0]
            sample_df = sample_df.withColumn(target_col, F.col(target_col) + F.lit(1))
            print(f"  · mutated column '{target_col}' (+1) to ensure uniqueness")

    sample_df.write.format("delta").mode("append").saveAsTable(fqn)
    print(f"  ✅ appended {sample_df.count()} no-PK row(s) (mutate={mutate})")


def mutation_content_swap_no_pk():
    """Delete one in-window row and append one different (mutated) in-window row.

    Net count is unchanged but the multiset diverges. For hash_no_pk, this
    produces status=INCONCLUSIVE (totals match, hashes don't). For
    advanced_no_pk, exceptAll surfaces both sides → FAIL+count_match=True →
    status=hash_mismatch.
    """
    # Phase A: delete 1
    saved_n = int(mutation_params.get("count", 1))
    if saved_n != 1:
        # We support count=1 only. Multi-swap can be added later if a scenario
        # demands it; the assertion logic stays simpler with single-row swap.
        print(f"  ⚠️  count={saved_n} requested but content_swap_no_pk only supports 1; using 1")
    victim_rows = _pick_in_window_rows(1)
    cols = [f.name for f in spark.table(fqn).schema.fields]

    def _lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, bool):
            return "true" if v else "false"
        from datetime import datetime as _dt, date as _date
        if isinstance(v, (_dt, _date)):
            return "TIMESTAMP '" + v.isoformat(sep=" ") + "'"
        return repr(v)

    r = victim_rows[0]
    parts = [f"{c} IS NULL" if r[c] is None else f"{c} = {_lit(r[c])}" for c in cols]
    spark.sql(f"DELETE FROM {fqn} WHERE " + " AND ".join(parts))
    # Phase B: append a mutated copy. Inline so we don't smash mutation_params.
    sample_df = spark.createDataFrame([r], spark.table(fqn).schema)
    sample_df = _bump_filter_into_window(sample_df)
    str_cols = [
        f.name for f in sample_df.schema.fields
        if f.dataType.typeName() == "string" and f.name != filter_col_simple
    ]
    if str_cols:
        target_col = str_cols[0]
        sample_df = sample_df.withColumn(
            target_col,
            F.concat(F.coalesce(F.col(target_col), F.lit("")), F.lit(f"_swap_{scenario_id[:24]}")),
        )
    else:
        num_cols = [
            f.name for f in sample_df.schema.fields
            if f.dataType.typeName() in ("integer", "long", "double", "float", "decimal")
            and f.name != filter_col_simple
        ]
        if not num_cols:
            raise RuntimeError(
                "content_swap_no_pk needs a string or numeric non-filter column to mutate"
            )
        sample_df = sample_df.withColumn(num_cols[0], F.col(num_cols[0]) + F.lit(1))
    sample_df.write.format("delta").mode("append").saveAsTable(fqn)
    print(f"  ✅ swapped 1 row (delete + mutated re-insert)")


_DISPATCH = {
    "noop": mutation_noop,
    "delete_rows": mutation_delete_rows,
    "insert_extra_rows": mutation_insert_extra_rows,
    "update_column": mutation_update_column,
    "add_extra_column": mutation_add_extra_column,
    "drop_column": mutation_drop_column,
    "null_out_pk": mutation_null_out_pk,
    "clear_key_cols": mutation_clear_key_cols,
    "delete_rows_no_pk": mutation_delete_rows_no_pk,
    "insert_extra_rows_no_pk": mutation_insert_extra_rows_no_pk,
    "content_swap_no_pk": mutation_content_swap_no_pk,
}

if mutation_type not in _DISPATCH:
    raise RuntimeError(f"unknown mutation_type '{mutation_type}'. Valid: {sorted(_DISPATCH)}")

if not _SHOULD_RUN:
    print(f"\n⏭️  scenario_seeder skipped — {scenario_id} ({_SKIP_REASON})")
    # Write the not_applicable verdict directly so the audit trail in
    # validation_scenario_runs is preserved even when the pipeline's
    # IfCondition skips RunEngine + RunAssert downstream.
    try:
        run_tsql_params(
            "INSERT INTO [dbo].[validation_scenario_runs] "
            "(scenario_run_id, scenario_id, target_table, mutation_type, "
            " engine_run_id, expected_status, actual_status, evidence_ok, "
            " verdict, notes, pipeline_run_id, created_at) "
            "VALUES (NEWID(), ?, ?, ?, NULL, ?, NULL, NULL, "
            "        'not_applicable', ?, ?, ?)",
            [
                scenario_id, target_table, mutation_type,
                expected_status,
                f"Seeder reported not_applicable: {_SKIP_REASON}",
                pipeline_run_id, datetime.utcnow(),
            ],
        )
        print(f"  → verdict=not_applicable written to validation_scenario_runs")
    except Exception as verdict_exc:
        # Don't let an audit-write failure mask the actual skip — log loudly
        # and still exit cleanly so the pipeline's IfCondition fires.
        print(f"  ⚠️  failed to write not_applicable verdict: {verdict_exc}")
    _exit_value = "not_applicable"
else:
    _DISPATCH[mutation_type]()
    print(f"\n✅ scenario_seeder complete — {scenario_id}")
    _exit_value = "applied"

# notebook.exit must be the LAST statement — Fabric raises a control-flow
# exception that terminates the cell, so any code after will be reported as
# an "error" by the pipeline activity.
notebookutils.notebook.exit(_exit_value)
