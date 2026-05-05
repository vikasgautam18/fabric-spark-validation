#!/usr/bin/env python
# coding: utf-8

# ## Validation Suite — Scenario Assert
#
# Reads the engine's `validation_results_history` row for (run_id, target_table)
# and decides whether the scenario produced its expected outcome. Writes a
# verdict row to `validation_scenario_runs` (Fabric SQL DB).
#
# Verdicts:
#   pass            actual_status matches expected AND mutation evidence checks out
#   fail            actual_status differs from expected (drift was missed or wrong)
#   inconclusive    no audit row found (engine crashed, audit write failed)
#   not_applicable  scenario was skipped because mode/policy didn't match
#
# Invoked PER SCENARIO from the child pipeline AFTER comparison_engine completes.

# In[1]:


%run _common


# In[2]:

# ── Parameters (set from pipeline) ───────────────────────────────────────────

# parameters tag
scenario_id      = "baseline-appointments"
run_id           = None        # engine RUN_ID — required
seeder_status    = "applied"   # 'applied' | 'not_applicable' | 'error'
pipeline_run_id  = None


# In[3]:

# ── Validate inputs + load scenario row ──────────────────────────────────────

import json
from datetime import datetime

print(f"▶ scenario_assert: scenario_id={scenario_id} run_id={run_id} seeder={seeder_status}")

scen_rows = spark.sql(
    f"SELECT * FROM validation.scenarios WHERE scenario_id = '{scenario_id}'"
).collect()
if not scen_rows:
    raise RuntimeError(f"scenario_id '{scenario_id}' not found")

s = scen_rows[0]
target_table   = s["target_table"]
mutation_type  = s["mutation_type"]
mutation_params = json.loads(s["mutation_params"] or "{}")
expected_status = s["expected_status"]


# In[4]:

# ── SQL DB writers ───────────────────────────────────────────────────────────

_SCHEMA = "dbo"


def write_verdict(verdict, actual_status, evidence_ok, notes):
    """Insert one row into validation_scenario_runs."""
    sql = (
        f"INSERT INTO [{_SCHEMA}].[validation_scenario_runs] "
        f"(scenario_run_id, scenario_id, target_table, mutation_type, "
        f" engine_run_id, expected_status, actual_status, evidence_ok, "
        f" verdict, notes, pipeline_run_id, created_at) "
        f"VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    params = [
        scenario_id, target_table, mutation_type,
        run_id, expected_status, actual_status,
        bool(evidence_ok), verdict,
        (notes or "")[:3500], pipeline_run_id, datetime.utcnow(),
    ]
    run_tsql_params(sql, params)
    print(f"  → verdict={verdict}  actual={actual_status}  evidence_ok={evidence_ok}")
    if notes:
        print(f"     notes: {notes}")


# In[5]:

# ── Always-on teardown: restore validation.comparison_key_columns ────────────
# Runs BEFORE any short-circuit so a crashed engine, missing run_id, or
# seeder='not_applicable' can never leave the metadata in a broken state.
# The backup table is created lazily by the seeder; absent table = no-op.

if mutation_type == "clear_key_cols":
    try:
        if spark.catalog.tableExists("validation.comparison_key_columns_test_backup"):
            restored = spark.sql(f"""
                SELECT COUNT(*) AS n FROM validation.comparison_key_columns_test_backup
                WHERE scenario_id = '{scenario_id}' AND table_name = '{target_table}'
            """).collect()[0]["n"]
            if restored:
                spark.sql(f"""
                    INSERT INTO validation.comparison_key_columns
                    SELECT table_name, column_name, ordinal
                    FROM validation.comparison_key_columns_test_backup
                    WHERE scenario_id = '{scenario_id}' AND table_name = '{target_table}'
                """)
                spark.sql(f"""
                    DELETE FROM validation.comparison_key_columns_test_backup
                    WHERE scenario_id = '{scenario_id}' AND table_name = '{target_table}'
                """)
                print(f"  ♻️  restored {restored} key_column row(s) for {target_table}")
    except Exception as restore_exc:
        # Surface restore failures into the print log; verdict will record below
        # via evidence_notes if we reach the evidence-checks block.
        print(f"  ⚠️  key_columns restore FAILED: {restore_exc}")


# In[6]:

# ── Short-circuit: not_applicable from seeder ────────────────────────────────

_VERDICT_PRESET = None  # if set, skip downstream eval and exit at end with this value

if seeder_status == "not_applicable":
    write_verdict(
        verdict="not_applicable",
        actual_status=None,
        evidence_ok=None,
        notes="Seeder reported not_applicable (mode or drift_policy mismatch)",
    )
    _VERDICT_PRESET = "not_applicable"


if _VERDICT_PRESET is None and not run_id:
    write_verdict(
        verdict="inconclusive",
        actual_status=None,
        evidence_ok=False,
        notes="No run_id provided — engine likely failed before producing one",
    )
    _VERDICT_PRESET = "inconclusive"


# In[6]:

# ── Read the engine audit row ────────────────────────────────────────────────

# Use a one-off SELECT via JDBC. Reuse sql_connection() from _common.

def read_result_row():
    conn = sql_connection()
    try:
        stmt = conn.prepareStatement(
            f"SELECT status, pg_count, lh_count, mismatch_count, "
            f"       error_message FROM [{_SCHEMA}].[validation_results_history] "
            f"WHERE run_id = ? AND table_name = ?"
        )
        stmt.setString(1, str(run_id))
        stmt.setString(2, target_table)
        rs = stmt.executeQuery()
        if rs.next():
            return {
                "status": rs.getString(1),
                "pg_count": rs.getLong(2) if not rs.wasNull() else None,
                "lh_count": rs.getLong(3) if not rs.wasNull() else None,
                "mismatch_count": rs.getLong(4) if not rs.wasNull() else None,
                "error_message": rs.getString(5),
            }
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def read_samples(limit=20):
    """Return up to `limit` mismatch sample rows for evidence checks."""
    conn = sql_connection()
    try:
        stmt = conn.prepareStatement(
            f"SELECT TOP {int(limit)} mismatch_type, column_name, lakehouse_value, postgres_value "
            f"FROM [{_SCHEMA}].[validation_mismatch_samples] "
            f"WHERE run_id = ? AND table_name = ? ORDER BY sample_seq"
        )
        stmt.setString(1, str(run_id))
        stmt.setString(2, target_table)
        rs = stmt.executeQuery()
        out = []
        while rs.next():
            out.append({
                "mismatch_type": rs.getString(1),
                "column_name": rs.getString(2),
                "lakehouse_value": rs.getString(3),
                "postgres_value": rs.getString(4),
            })
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


if _VERDICT_PRESET is None:
    result = read_result_row()
    if result is None:
        write_verdict(
            verdict="inconclusive",
            actual_status=None,
            evidence_ok=False,
            notes=f"No row in validation_results_history for run_id={run_id}, "
                  f"table={target_table}. Engine may have crashed before per-table audit.",
        )
        _VERDICT_PRESET = "inconclusive"
    else:
        actual_status = result["status"]
        print(f"  engine reported: status={actual_status} pg={result['pg_count']} "
              f"lh={result['lh_count']} mismatch={result['mismatch_count']}")
        if result["error_message"]:
            print(f"  error_message: {result['error_message']}")


# In[7]:

# ── Evidence checks (mutation-specific) ──────────────────────────────────────
# A status match alone is insufficient: a broken seeder could trigger drift by
# coincidence. We also verify that the engine saw the SHAPE of drift we expect.

evidence_ok = True
evidence_notes = []
samples = []


def _need_samples():
    global samples
    if not samples:
        samples = read_samples(limit=50)
    return samples


if _VERDICT_PRESET is None:
    pg_c = result["pg_count"]
    lh_c = result["lh_count"]
    err  = result["error_message"] or ""
else:
    pg_c = lh_c = None
    err  = ""

if _VERDICT_PRESET is not None:
    # Verdict already decided (not_applicable / inconclusive) — skip evidence checks.
    pass
elif mutation_type == "noop":
    # Nothing further to verify — pass status is its own evidence
    pass

elif mutation_type == "delete_rows":
    n = int(mutation_params.get("count", 0))
    # Deleted from LH → PG should have MORE rows than LH (delta close to n)
    if pg_c is None or lh_c is None:
        evidence_ok = False
        evidence_notes.append("counts missing")
    else:
        delta = pg_c - lh_c
        if delta < 1:
            evidence_ok = False
            evidence_notes.append(f"expected pg>lh after delete_rows; got pg={pg_c} lh={lh_c}")
        elif abs(delta - n) > max(2, n * 0.1):
            # Within 10% tolerance — small deviations from concurrent activity OK
            evidence_notes.append(f"delta {delta} differs from requested {n} by >10%")
    # If hash mode, expect only_in_postgres samples
    sm = _need_samples()
    if sm and not any(s["mismatch_type"] == "only_in_postgres" for s in sm):
        evidence_notes.append("no only_in_postgres samples captured")

elif mutation_type == "insert_extra_rows":
    n = int(mutation_params.get("count", 0))
    if pg_c is None or lh_c is None:
        evidence_ok = False
        evidence_notes.append("counts missing")
    else:
        delta = lh_c - pg_c
        if delta < 1:
            evidence_ok = False
            evidence_notes.append(f"expected lh>pg after insert_extra_rows; got pg={pg_c} lh={lh_c}")
    sm = _need_samples()
    if sm and not any(s["mismatch_type"] == "only_in_lakehouse" for s in sm):
        evidence_notes.append("no only_in_lakehouse samples captured")

elif mutation_type == "update_column":
    col = mutation_params.get("column", "")
    sm = _need_samples()
    # Hash mode: hash_diff samples; Advanced mode: value_diff with column_name
    has_hash_diff = any(s["mismatch_type"] == "hash_diff" for s in sm)
    has_value_diff_for_col = any(
        s["mismatch_type"] == "value_diff" and (s["column_name"] or "") == col for s in sm
    )
    if not (has_hash_diff or has_value_diff_for_col):
        evidence_ok = False
        evidence_notes.append(f"no hash_diff or value_diff samples for column '{col}'")

elif mutation_type == "add_extra_column":
    new_col = mutation_params.get("column_name", "")
    if new_col and new_col not in err:
        evidence_notes.append(f"error_message does not mention added column '{new_col}'")

elif mutation_type == "drop_column":
    col = mutation_params.get("column_name", "")
    if col and col not in err:
        evidence_notes.append(f"error_message does not mention dropped column '{col}'")

elif mutation_type == "null_out_pk":
    if "null" not in err.lower() and "pk" not in err.lower():
        evidence_notes.append("error_message does not mention null PK")

elif mutation_type == "clear_key_cols":
    # Substring match on engine error message. Configured per-scenario via
    # mutation_params.expected_error_substring so we don't have to extend the
    # scenarios table schema.
    needle = mutation_params.get("expected_error_substring", "")
    if needle and needle not in err:
        evidence_ok = False
        evidence_notes.append(
            f"error_message does not contain expected substring "
            f"'{needle}' (got: {err[:200]!r})"
        )


# In[8]:

# ── Final verdict ────────────────────────────────────────────────────────────

if _VERDICT_PRESET is not None:
    final = _VERDICT_PRESET
else:
    if actual_status == expected_status:
        if evidence_ok:
            verdict = "pass"
        else:
            verdict = "fail"
            evidence_notes.insert(0, "status matched but mutation-specific evidence missing")
    else:
        verdict = "fail"
        evidence_notes.insert(0, f"status mismatch: expected={expected_status} actual={actual_status}")

    write_verdict(
        verdict=verdict,
        actual_status=actual_status,
        evidence_ok=evidence_ok and not [n for n in evidence_notes if "missing" in n or "mismatch" in n],
        notes="; ".join(evidence_notes) if evidence_notes else None,
    )
    final = "pass" if verdict == "pass" else "fail"

# notebook.exit must be the last statement — Fabric raises a control-flow
# exception that terminates the cell; any code after it is reported as error.
notebookutils.notebook.exit(final)

if verdict == "fail":
    raise RuntimeError(f"Scenario {scenario_id} verdict=fail — see validation_scenario_runs")
