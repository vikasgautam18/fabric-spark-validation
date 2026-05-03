#!/usr/bin/env python
# coding: utf-8

# ## Validation Suite — Comparison Engine
#
# Compares data in Fabric Lakehouse against PostgreSQL using metadata-driven
# configuration from `validation.comparison_config`.
#
# Modes:
#   - **basic**:    Row count comparison only
#   - **hash**:     Row count + per-row hash digest comparison
#   - **advanced**: Full row-by-row, column-by-column diff
#
# Prerequisites:
#   1. Run `comparison_setup` notebook first to create metadata tables
#   2. Ensure Lakehouse with target schemas is attached as default
#   3. PostgreSQL reachable via JDBC (Managed Private Endpoint)

# In[1]:
%run _common


# In[2]:
# ── Notebook parameters (set from pipeline) ───────────────────────────────────
# This cell must have the `parameters` tag so Fabric can override these values
# from the pipeline activity. Markers picked up by deploy_nb: "parameters tag",
# "parameters cell", "set from pipeline".

triggered_by      = "manual"        # manual / scenario_pipeline / nightly
scenario_id       = None            # set by engine_tests child pipeline
pipeline_run_id   = None            # @pipeline().RunId from invoking pipeline
notebook_version  = "1.0"           # bump on engine logic changes

# Engine-tests controls — leave at defaults for normal runs.
# When tables_filter is set, ONLY those tables are processed; missing entries
# raise (fail-closed) so a typo can't silently skip validation.
# When fail_on_validation_failure is False, the engine still classifies and
# audits failures but does NOT raise — used by scenario tests where a failure
# is the EXPECTED outcome and the assert step needs to see the audit row.
tables_filter                  = None     # list[str] | None
fail_on_validation_failure     = True


# In[3]:
# ── Diagnostics: confirm pipeline-injected parameters reached the engine ─────
print(f"[params] triggered_by={triggered_by!r}")
print(f"[params] scenario_id={scenario_id!r}")
print(f"[params] tables_filter={tables_filter!r}")
print(f"[params] fail_on_validation_failure={fail_on_validation_failure!r}")
print(f"[params] pipeline_run_id={pipeline_run_id!r}")


# In[4]:
# ── Derive locals from shared config + per-notebook tunables ─────────────────

PG_HOST     = cfg["pg_host"]
PG_PORT     = cfg["pg_port"]
PG_DATABASE = cfg["pg_database"]
PG_USER     = cfg["pg_user"]
PG_PASSWORD = pg_password()

# Engine reads from many schemas (per comparison_config), so don't bind to one.
JDBC_URL   = pg_jdbc_url(schema=None)
JDBC_PROPS = pg_jdbc_props()

# Capture global cfg under a new name — the comparison loop below shadows
# `cfg` with per-table config dicts.
GLOBAL_CFG = cfg

# Max detail rows to store per table (prevents result table bloat)
MAX_DETAIL_ROWS = 1000

# Whether to allow free-form filter expressions in comparison_config.filter_column.
# Expressions are interpolated into SQL (PG side) and into Spark expr() (Lakehouse
# side) WITHOUT validation — they are TRUSTED CODE from the metadata table.
#
# This must be True today because the seeded configs use multi-column window
# expressions (e.g. created_at OR last_updated). For a hardened production
# deployment, replace expression-based filter_column entries with a structured
# column-name model (filter_column + optional filter_column_secondary) and set
# this back to False. Any DBA who can write to validation.comparison_config can
# achieve arbitrary SQL execution while this flag is True.
ALLOW_FILTER_EXPRESSIONS = True

# Per-table semantic statuses written to validation_results_history (SQL DB):
#   pass / count_mismatch / hash_mismatch / schema_drift / error
# A run that has ANY of these failing statuses raises after persisting audit.
FAILING_STATUSES = ("count_mismatch", "hash_mismatch", "schema_drift", "error")

# Ensure consistent timestamp handling (Finding #4: TIMESTAMPTZ)
spark.conf.set("spark.sql.session.timeZone", "UTC")

print(f"Comparison Engine initialized")
print(f"  PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
print(f"  Timezone: UTC (forced for consistent timestamp comparison)")
print(f"  triggered_by={triggered_by} scenario_id={scenario_id} pipeline_run_id={pipeline_run_id}")


# In[5]:
# ── Core comparison engine ────────────────────────────────────────────────────

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import time
import traceback
import uuid

# UUID — primary key for SQL DB audit tables.
RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.utcnow()
print(f"Run ID: {RUN_ID}\n")


def _classify_status(result):
    """Map engine result dict to a specific audit status string.

    Returns one of: pass / count_mismatch / hash_mismatch / error
    Schema-drift is detected at the exception layer (raises ValueError),
    not here — `_classify_exception` handles that path.
    """
    s = result.get("status")
    if s == "PASS":
        return "pass"
    if s in ("ERROR", "DATA_QUALITY_ERROR"):
        return "error"
    # FAIL — disambiguate by what mismatched
    if result.get("count_match") is False:
        return "count_mismatch"
    return "hash_mismatch"


def _classify_exception(exc):
    """Map a per-table exception to an audit status string."""
    msg = str(exc)
    if msg.startswith("Schema mismatch:") or "schema_drift_policy" in msg:
        return "schema_drift"
    return "error"


# ── SQL DB audit writers (best-effort; failures logged, never crash run) ─────

_AUDIT_SCHEMA = safe_ident(GLOBAL_CFG["sql_schema"], kind="sql_schema")


def _audit_safe(label, fn):
    """Run an audit write; log failures but do NOT raise. Validation
    correctness is more important than audit completeness — a flaky SQL DB
    must not mask real validation failures."""
    try:
        fn()
    except Exception as audit_err:
        print(f"   ⚠️  audit write failed [{label}]: {str(audit_err)[:200]}")


def audit_run_start(run_id):
    sql = (
        f"INSERT INTO [{_AUDIT_SCHEMA}].[validation_runs] "
        f"(run_id, started_at, status, triggered_by, scenario_id, "
        f" pipeline_run_id, notebook_version) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    _audit_safe("run_start", lambda: run_tsql_params(sql, [
        run_id, RUN_STARTED_AT, "running", triggered_by,
        scenario_id, pipeline_run_id, notebook_version,
    ]))


def audit_table_result(run_id, table_name, mode, status, pg_count, lh_count,
                       mismatch_count, window_start, window_end,
                       started_at, ended_at, duration_sec, error_message):
    sql = (
        f"INSERT INTO [{_AUDIT_SCHEMA}].[validation_results_history] "
        f"(run_id, table_name, comparison_mode, status, pg_count, lh_count, "
        f" mismatch_count, window_start, window_end, started_at, ended_at, "
        f" duration_sec, error_message) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    _audit_safe(f"table:{table_name}", lambda: run_tsql_params(sql, [
        run_id, table_name, mode, status, pg_count, lh_count,
        mismatch_count, window_start, window_end,
        started_at, ended_at, float(duration_sec),
        (error_message[:3500] if error_message else None),
    ]))


def audit_run_end(run_id, ended_at, run_status, total, passed, failed, error_message=None):
    duration = (ended_at - RUN_STARTED_AT).total_seconds()
    sql = (
        f"UPDATE [{_AUDIT_SCHEMA}].[validation_runs] "
        f"SET ended_at = ?, duration_sec = ?, status = ?, "
        f"    total_tables = ?, pass_count = ?, fail_count = ?, "
        f"    error_message = ? "
        f"WHERE run_id = ?"
    )
    _audit_safe("run_end", lambda: run_tsql_params(sql, [
        ended_at, float(duration), run_status, total, passed, failed,
        (error_message[:3500] if error_message else None), run_id,
    ]))


def audit_mismatch_samples(run_id, table_name, samples):
    """Bulk-insert sample mismatch rows (PK-level forensics).

    Caps total rows per call at MAX_DETAIL_ROWS regardless of how many were
    collected — defense in depth in case a runner over-collects.
    """
    if not samples:
        return
    samples = samples[:MAX_DETAIL_ROWS]
    captured_at = datetime.utcnow()
    rows = []
    for seq, s in enumerate(samples, start=1):
        rows.append([
            run_id, table_name, seq,
            s.get("mismatch_type"),
            (s.get("pk_values") or "")[:8000],
            s.get("column_name"),
            (str(s["lakehouse_value"])[:8000] if s.get("lakehouse_value") is not None else None),
            (str(s["postgres_value"])[:8000] if s.get("postgres_value") is not None else None),
            captured_at,
        ])
    sql = (
        f"INSERT INTO [{_AUDIT_SCHEMA}].[validation_mismatch_samples] "
        f"(run_id, table_name, sample_seq, mismatch_type, pk_values, "
        f" column_name, lakehouse_value, postgres_value, captured_at) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    _audit_safe(f"samples:{table_name}",
                lambda: run_tsql_batch(sql, rows))


# Insert the run row immediately so even a crash mid-run leaves an audit trail.
audit_run_start(RUN_ID)


def load_config():
    """Load enabled comparison configs with their key and skip columns.

    IMPORTANT: key columns and skip columns are aggregated INDEPENDENTLY in
    sub-queries, then joined to comparison_config. Joining all three tables in
    one shot and aggregating after produces a Cartesian fanout where each PK
    column is repeated `count(skip_columns)` times and vice versa, which
    silently breaks downstream join/projection logic for tables with composite
    keys or multiple skip columns.
    """
    configs = spark.sql("""
        WITH ks AS (
            SELECT table_name,
                   collect_list(struct(column_name, ordinal)) AS key_cols
            FROM validation.comparison_key_columns
            GROUP BY table_name
        ),
        sk AS (
            SELECT table_name,
                   collect_list(column_name) AS skip_cols
            FROM validation.comparison_skip_columns
            GROUP BY table_name
        )
        SELECT c.*,
               COALESCE(ks.key_cols,  array()) AS key_cols,
               COALESCE(sk.skip_cols, array()) AS skip_cols
        FROM validation.comparison_config c
        LEFT JOIN ks ON c.table_name = ks.table_name
        LEFT JOIN sk ON c.table_name = sk.table_name
        WHERE c.enabled = true
        ORDER BY c.table_name
    """).collect()
    return configs


def read_postgres(pg_schema, pg_table, filter_col, window_start, window_end):
    """Read a filtered DataFrame from PostgreSQL.

    filter_col can be:
      - A simple column name (validated as a SQL identifier): "last_updated"
      - An expression with placeholders, gated by `allow_filter_expressions`:
        e.g. "created_at >= '{window_start}' OR last_updated >= '{window_start}'"
        Placeholders: {window_start}, {window_end}
        Expressions are NOT validated for SQL safety; treat metadata as code
        and require explicit opt-in (set ALLOW_FILTER_EXPRESSIONS = True).
    """
    schema_id = safe_ident(pg_schema, kind="pg_schema")
    table_id  = safe_ident(pg_table,  kind="pg_table")
    where_clause = _build_where_clause(filter_col, window_start, window_end)
    query = f"(SELECT * FROM {schema_id}.{table_id} WHERE {where_clause}) AS pg_data"
    return spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", query) \
        .options(**JDBC_PROPS) \
        .load()


def read_lakehouse(lh_schema, table_name, filter_col, window_start, window_end):
    """Read a filtered DataFrame from Lakehouse.

    filter_col can be:
      - A simple column name: validated and used as `col >= start AND col < end`
      - An expression with placeholders: gated by ALLOW_FILTER_EXPRESSIONS.
        PG-style "col" quoting is rewritten to Spark-style `col` quoting.
    """
    safe_ident(lh_schema, kind="lh_schema")
    safe_ident(table_name, kind="table_name")
    df = spark.table(f"{lh_schema}.{table_name}")
    if "{window_start}" in filter_col or "{window_end}" in filter_col:
        if not ALLOW_FILTER_EXPRESSIONS:
            raise IdentifierError(
                f"filter_col is an expression but ALLOW_FILTER_EXPRESSIONS is False: {filter_col!r}"
            )
        # Same ISO+offset literal as PG side (Finding #21) — Spark parses
        # 'YYYY-MM-DDTHH:MM:SS.ffffff+00:00' into TimestampType in UTC.
        where_expr = filter_col.format(
            window_start=_iso_utc(window_start),
            window_end=_iso_utc(window_end),
        )
        import re
        where_expr = re.sub(r'"(\w+)"', r'`\1`', where_expr)
        return df.filter(F.expr(where_expr))
    else:
        col_id = safe_ident(filter_col, kind="filter_column")
        # F.lit() on a naive datetime is interpreted in session TZ (UTC).
        return df \
            .filter(F.col(col_id) >= F.lit(window_start)) \
            .filter(F.col(col_id) < F.lit(window_end))


def get_pg_count(pg_schema, pg_table, filter_col, window_start, window_end):
    """Get filtered row count from PostgreSQL."""
    schema_id = safe_ident(pg_schema, kind="pg_schema")
    table_id  = safe_ident(pg_table,  kind="pg_table")
    where_clause = _build_where_clause(filter_col, window_start, window_end)
    query = f"(SELECT COUNT(*) AS cnt FROM {schema_id}.{table_id} WHERE {where_clause}) AS cnt_q"
    return spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", query) \
        .options(**JDBC_PROPS) \
        .load().collect()[0]["cnt"]


def _iso_utc(ts):
    """Format a naive UTC datetime as ISO 8601 with explicit +00:00 offset.

    Always producing an offset-bearing literal lets PG cast it to TIMESTAMPTZ
    unambiguously, regardless of the server/session timezone. Required for
    correct comparison against TIMESTAMPTZ columns, and harmless for naive
    TIMESTAMP columns when the JDBC session is forced to UTC (see _common.py).
    """
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"


def _build_where_clause(filter_col, window_start, window_end):
    """Construct the WHERE clause for PG. Validates simple-column case, gates
    expression case behind ALLOW_FILTER_EXPRESSIONS. Always emits ISO+offset
    timestamp literals cast to TIMESTAMPTZ to make the intent explicit and
    timezone-safe (Finding #21)."""
    ws_iso = _iso_utc(window_start)
    we_iso = _iso_utc(window_end)
    if "{window_start}" in filter_col or "{window_end}" in filter_col:
        if not ALLOW_FILTER_EXPRESSIONS:
            raise IdentifierError(
                f"filter_col is an expression but ALLOW_FILTER_EXPRESSIONS is False: {filter_col!r}"
            )
        return filter_col.format(window_start=ws_iso, window_end=we_iso)
    col_id = safe_ident(filter_col, kind="filter_column")
    # ISO+offset timestamps are formatted by us — no user input — safe to inline.
    return (
        f'"{col_id}" >= TIMESTAMPTZ \'{ws_iso}\' '
        f'AND "{col_id}" < TIMESTAMPTZ \'{we_iso}\''
    )


def resolve_columns(lh_df, pg_df, skip_cols, schema_drift_policy):
    """Resolve column sets based on schema drift policy."""
    lh_cols = set(lh_df.columns)
    pg_cols = set(pg_df.columns)
    skip_set = set(skip_cols) if skip_cols else set()

    if schema_drift_policy == "fail" and lh_cols != pg_cols:
        extra_lh = lh_cols - pg_cols
        extra_pg = pg_cols - lh_cols
        raise ValueError(f"Schema mismatch: extra in lakehouse={extra_lh}, extra in postgres={extra_pg}")
    elif schema_drift_policy == "intersect":
        compare_cols = lh_cols & pg_cols
    else:  # ignore_extra
        compare_cols = lh_cols & pg_cols

    compare_cols -= skip_set
    return sorted(compare_cols)


def check_pk_quality(df, pk_cols, source_name):
    """Validate PK columns exist, have no nulls, and are unique."""
    issues = []
    df_cols = set(df.columns)

    for pk in pk_cols:
        if pk not in df_cols:
            issues.append(f"PK column '{pk}' missing in {source_name}")

    if issues:
        return issues

    # Check nulls
    null_filter = None
    for pk in pk_cols:
        cond = F.col(pk).isNull()
        null_filter = cond if null_filter is None else null_filter | cond

    null_count = df.filter(null_filter).count()
    if null_count > 0:
        issues.append(f"{source_name}: {null_count} rows with NULL PK values")

    # Check uniqueness
    total = df.count()
    distinct = df.select(*pk_cols).distinct().count()
    if distinct < total:
        issues.append(f"{source_name}: {total - distinct} duplicate PK rows ({total} total, {distinct} distinct)")

    return issues


def compute_row_hash(df, compare_cols, pk_cols):
    """Add a `_row_hash` column = SHA-256 of a deterministic JSON encoding of
    all non-PK compare columns.

    Why JSON-of-struct (not concat_ws)?
      `concat_ws("||", "a||b", "c")` and `concat_ws("||", "a", "b||c")` collide.
      Encoding as a typed struct → JSON gives unambiguous boundaries, length
      delimitation, and explicit nulls.

    Type-specific normalization (must match between PG and Lakehouse readers):
      - float/double : rounded to 8 decimal places
      - decimal      : preserved as decimal string (no float coercion)
      - binary       : hex string
      - array/map/struct : JSON via to_json()
      - timestamp    : cast to long (epoch seconds, UTC) — session TZ is UTC
      - all others   : cast to string
    """
    non_pk_cols = sorted([c for c in compare_cols if c not in pk_cols])
    if not non_pk_cols:
        return df.withColumn("_row_hash", F.lit("no_compare_cols"))

    type_map = {f.name: f.dataType for f in df.schema.fields}
    norm_cols = []
    for c in non_pk_cols:
        col_type = type_map.get(c)
        col      = F.col(c)
        if isinstance(col_type, BinaryType):
            normalized = F.hex(col)
        elif isinstance(col_type, (FloatType, DoubleType)):
            normalized = F.round(col, 8).cast("string")
        elif isinstance(col_type, (ArrayType, MapType, StructType)):
            normalized = F.to_json(col)
        elif isinstance(col_type, TimestampType):
            # Both sides honor session TZ = UTC, so epoch seconds are stable
            normalized = col.cast("long").cast("string")
        else:
            normalized = col.cast("string")
        # alias preserves the column name in the resulting struct so JSON keys
        # are deterministic and include the column identity
        norm_cols.append(normalized.alias(c))

    canonical = F.to_json(F.struct(*norm_cols))
    return df.withColumn("_row_hash", F.sha2(canonical, 256))


# ── Comparison runners ────────────────────────────────────────────────────────

def run_basic(config, window_start, window_end):
    """Basic mode: row count comparison.

    On count mismatch we auto-upgrade to a PK-only LEFT ANTI JOIN to capture
    sample missing PKs from each side. PKs only — no row hashing — keeps
    the upgrade cheap relative to hash mode.
    """
    pg_schema = config["pg_schema"]
    pg_table = config["pg_table_name"] or config["table_name"]
    lh_schema = config["lakehouse_schema"]
    table_name = config["table_name"]
    filter_col = config["filter_column"]
    filter_col_pg = config["filter_column_pg"] or filter_col

    pg_count = get_pg_count(pg_schema, pg_table, filter_col_pg, window_start, window_end)

    # Use read_lakehouse for consistent expression handling
    lh_df = read_lakehouse(lh_schema, table_name, filter_col, window_start, window_end)
    lh_count = lh_df.count()

    match = (pg_count == lh_count)
    status = "PASS" if match else "FAIL"

    samples = []
    if not match:
        # Auto-upgrade: capture which PKs are missing from each side.
        try:
            key_cols_raw = config["key_cols"] or []
            pk_cols = [kc["column_name"] for kc in sorted(key_cols_raw, key=lambda x: x["ordinal"])]
            if pk_cols:
                pg_df = read_postgres(pg_schema, pg_table, filter_col_pg, window_start, window_end)
                lh_pk = lh_df.select(*pk_cols)
                pg_pk = pg_df.select(*pk_cols)
                samples.extend(_collect_pk_diff_samples(lh_pk, pg_pk, pk_cols))
        except Exception as upgrade_err:
            print(f"      ⚠️  basic-mode forensic upgrade failed: {str(upgrade_err)[:200]}")

    return {
        "lakehouse_count": lh_count,
        "postgres_count": pg_count,
        "count_match": match,
        "rows_only_in_lakehouse": None,
        "rows_only_in_postgres": None,
        "rows_with_mismatches": None,
        "status": status,
        "error_message": None if match else f"Count mismatch: lakehouse={lh_count}, postgres={pg_count}",
    }, samples


def _collect_pk_diff_samples(lh_pk_df, pg_pk_df, pk_cols, max_per_side=None):
    """Return a list of sample dicts for PK-level diffs (only_in_lh / only_in_pg).

    Caps each side at MAX_DETAIL_ROWS. Used by basic and hash modes.
    """
    cap = max_per_side or MAX_DETAIL_ROWS
    out = []
    join_cond = [lh_pk_df[pk] == pg_pk_df[pk] for pk in pk_cols]
    only_lh = lh_pk_df.alias("lh").join(pg_pk_df.alias("pg"), join_cond, "left_anti")
    only_pg = pg_pk_df.alias("pg").join(lh_pk_df.alias("lh"), join_cond, "left_anti")
    for r in only_lh.limit(cap).collect():
        pk_vals = {pk: (str(r[pk]) if r[pk] is not None else None) for pk in pk_cols}
        out.append({"mismatch_type": "only_in_lakehouse", "pk_values": json.dumps(pk_vals),
                    "column_name": None, "lakehouse_value": "EXISTS", "postgres_value": None})
    for r in only_pg.limit(cap).collect():
        pk_vals = {pk: (str(r[pk]) if r[pk] is not None else None) for pk in pk_cols}
        out.append({"mismatch_type": "only_in_postgres", "pk_values": json.dumps(pk_vals),
                    "column_name": None, "lakehouse_value": None, "postgres_value": "EXISTS"})
    return out


def run_hash(config, window_start, window_end):
    """Hash mode: row count + per-row hash comparison.

    Captures sample PKs for each mismatch class:
      • only_in_lakehouse / only_in_postgres : PKs present on only one side
      • hash_diff                            : PKs present both sides w/ diff hash
    """
    pg_schema = config["pg_schema"]
    pg_table = config["pg_table_name"] or config["table_name"]
    lh_schema = config["lakehouse_schema"]
    table_name = config["table_name"]
    filter_col = config["filter_column"]
    filter_col_pg = config["filter_column_pg"] or filter_col

    # Extract PK columns
    key_cols_raw = config["key_cols"]
    pk_cols = [kc["column_name"] for kc in sorted(key_cols_raw, key=lambda x: x["ordinal"])]
    skip_cols = config["skip_cols"] if config["skip_cols"] else []

    # Read both datasets
    pg_df = read_postgres(pg_schema, pg_table, filter_col_pg, window_start, window_end)
    lh_df = read_lakehouse(lh_schema, table_name, filter_col, window_start, window_end)

    # Resolve columns
    compare_cols = resolve_columns(lh_df, pg_df, skip_cols, config["schema_drift_policy"])

    # PK prechecks
    pk_issues = check_pk_quality(lh_df.select(*compare_cols), pk_cols, "lakehouse")
    pk_issues += check_pk_quality(pg_df.select(*compare_cols), pk_cols, "postgres")
    if pk_issues:
        return {
            "lakehouse_count": lh_df.count(),
            "postgres_count": pg_df.count(),
            "count_match": None,
            "rows_only_in_lakehouse": None,
            "rows_only_in_postgres": None,
            "rows_with_mismatches": None,
            "status": "DATA_QUALITY_ERROR",
            "error_message": "; ".join(pk_issues),
        }, []

    # Select only compare columns and compute hash
    lh_hashed = compute_row_hash(lh_df.select(*compare_cols), compare_cols, pk_cols) \
        .select(*pk_cols, "_row_hash")
    pg_hashed = compute_row_hash(pg_df.select(*compare_cols), compare_cols, pk_cols) \
        .select(*pk_cols, "_row_hash")

    lh_count = lh_hashed.count()
    pg_count = pg_hashed.count()

    # Outer join on PK
    join_cond = [lh_hashed[pk] == pg_hashed[pk] for pk in pk_cols]
    joined = lh_hashed.alias("lh").join(pg_hashed.alias("pg"), join_cond, "full_outer")

    only_in_lh_df = joined.filter(F.col(f"pg.{pk_cols[0]}").isNull())
    only_in_pg_df = joined.filter(F.col(f"lh.{pk_cols[0]}").isNull())
    hash_diff_df = joined.filter(
        F.col(f"lh.{pk_cols[0]}").isNotNull() &
        F.col(f"pg.{pk_cols[0]}").isNotNull() &
        (F.col("lh._row_hash") != F.col("pg._row_hash"))
    )

    only_in_lh = only_in_lh_df.count()
    only_in_pg = only_in_pg_df.count()
    hash_mismatches = hash_diff_df.count()

    total_issues = only_in_lh + only_in_pg + hash_mismatches
    status = "PASS" if total_issues == 0 else "FAIL"

    # ── Capture sample PKs per mismatch class (capped at MAX_DETAIL_ROWS each)
    samples = []
    if only_in_lh > 0:
        proj = [F.col(f"lh.{pk}").alias(pk) for pk in pk_cols]
        for r in only_in_lh_df.select(*proj).limit(MAX_DETAIL_ROWS).collect():
            pk_vals = {pk: (str(r[pk]) if r[pk] is not None else None) for pk in pk_cols}
            samples.append({"mismatch_type": "only_in_lakehouse",
                            "pk_values": json.dumps(pk_vals),
                            "column_name": None, "lakehouse_value": "EXISTS", "postgres_value": None})
    if only_in_pg > 0:
        proj = [F.col(f"pg.{pk}").alias(pk) for pk in pk_cols]
        for r in only_in_pg_df.select(*proj).limit(MAX_DETAIL_ROWS).collect():
            pk_vals = {pk: (str(r[pk]) if r[pk] is not None else None) for pk in pk_cols}
            samples.append({"mismatch_type": "only_in_postgres",
                            "pk_values": json.dumps(pk_vals),
                            "column_name": None, "lakehouse_value": None, "postgres_value": "EXISTS"})
    if hash_mismatches > 0:
        proj = [F.col(f"lh.{pk}").alias(pk) for pk in pk_cols]
        for r in hash_diff_df.select(*proj).limit(MAX_DETAIL_ROWS).collect():
            pk_vals = {pk: (str(r[pk]) if r[pk] is not None else None) for pk in pk_cols}
            samples.append({"mismatch_type": "hash_diff",
                            "pk_values": json.dumps(pk_vals),
                            "column_name": None, "lakehouse_value": None, "postgres_value": None})

    return {
        "lakehouse_count": lh_count,
        "postgres_count": pg_count,
        "count_match": lh_count == pg_count,
        "rows_only_in_lakehouse": only_in_lh,
        "rows_only_in_postgres": only_in_pg,
        "rows_with_mismatches": hash_mismatches,
        "status": status,
        "error_message": None if status == "PASS" else f"Mismatches: {only_in_lh} only_lh, {only_in_pg} only_pg, {hash_mismatches} hash_diff",
    }, samples


def run_advanced(config, window_start, window_end):
    """Advanced mode: full row-by-row, column-by-column comparison."""
    pg_schema = config["pg_schema"]
    pg_table = config["pg_table_name"] or config["table_name"]
    lh_schema = config["lakehouse_schema"]
    table_name = config["table_name"]
    filter_col = config["filter_column"]
    filter_col_pg = config["filter_column_pg"] or filter_col
    numeric_tol = config["numeric_tolerance"] or 0.001
    max_rows = config["max_rows_advanced"] or 500000

    # Extract PK columns
    key_cols_raw = config["key_cols"]
    pk_cols = [kc["column_name"] for kc in sorted(key_cols_raw, key=lambda x: x["ordinal"])]
    skip_cols = config["skip_cols"] if config["skip_cols"] else []

    # Read both datasets
    pg_df = read_postgres(pg_schema, pg_table, filter_col_pg, window_start, window_end)
    lh_df = read_lakehouse(lh_schema, table_name, filter_col, window_start, window_end)

    # Resolve columns
    compare_cols = resolve_columns(lh_df, pg_df, skip_cols, config["schema_drift_policy"])

    # PK prechecks
    pk_issues = check_pk_quality(lh_df.select(*compare_cols), pk_cols, "lakehouse")
    pk_issues += check_pk_quality(pg_df.select(*compare_cols), pk_cols, "postgres")
    if pk_issues:
        return {
            "lakehouse_count": lh_df.count(),
            "postgres_count": pg_df.count(),
            "count_match": None,
            "rows_only_in_lakehouse": None,
            "rows_only_in_postgres": None,
            "rows_with_mismatches": None,
            "status": "DATA_QUALITY_ERROR",
            "error_message": "; ".join(pk_issues),
        }, []

    lh_sub = lh_df.select(*compare_cols)
    pg_sub = pg_df.select(*compare_cols)

    lh_count = lh_sub.count()
    pg_count = pg_sub.count()

    # Check row cap — fall back to hash if too large
    if max(lh_count, pg_count) > max_rows:
        print(f"  ⚠️  Row count ({max(lh_count, pg_count)}) exceeds max_rows_advanced ({max_rows}), falling back to hash mode")
        result = run_hash(config, window_start, window_end)
        result["comparison_mode"] = "hash (fallback)"
        return result, []

    # Full outer join on PK
    join_cond = [lh_sub[pk] == pg_sub[pk] for pk in pk_cols]
    joined = lh_sub.alias("lh").join(pg_sub.alias("pg"), join_cond, "full_outer")

    only_in_lh = joined.filter(F.col(f"pg.{pk_cols[0]}").isNull()).count()
    only_in_pg = joined.filter(F.col(f"lh.{pk_cols[0]}").isNull()).count()

    # Column-by-column comparison for rows present in both
    both_present = joined.filter(
        F.col(f"lh.{pk_cols[0]}").isNotNull() &
        F.col(f"pg.{pk_cols[0]}").isNotNull()
    )

    non_pk_cols = [c for c in compare_cols if c not in pk_cols]
    detail_rows = []

    # Map column name → datatype (use lh_sub schema as source of truth)
    col_types = {f.name: f.dataType for f in lh_sub.schema.fields}
    numeric_types = (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)

    # Build mismatch condition across all non-PK columns
    mismatch_cond = None
    for col_name in non_pk_cols:
        lh_c = F.col(f"lh.{col_name}")
        pg_c = F.col(f"pg.{col_name}")
        col_type = col_types.get(col_name)

        if isinstance(col_type, numeric_types):
            # Numeric: allow tolerance
            col_mismatch = ~(
                lh_c.eqNullSafe(pg_c) |
                (
                    lh_c.cast("double").isNotNull() &
                    pg_c.cast("double").isNotNull() &
                    (F.abs(lh_c.cast("double") - pg_c.cast("double")) <= numeric_tol)
                )
            )
        elif isinstance(col_type, BinaryType):
            # Binary: compare via hex to be deterministic
            col_mismatch = ~F.hex(lh_c).eqNullSafe(F.hex(pg_c))
        elif isinstance(col_type, ArrayType):
            # Arrays: compare via string cast (preserves order)
            col_mismatch = ~lh_c.cast("string").eqNullSafe(pg_c.cast("string"))
        else:
            # Strings, bools, dates, timestamps, structs, maps: exact null-safe equality
            col_mismatch = ~lh_c.eqNullSafe(pg_c)

        mismatch_cond = col_mismatch if mismatch_cond is None else mismatch_cond | col_mismatch

    if mismatch_cond is None:
        rows_with_mismatches = 0
    else:
        mismatched_rows_df = both_present.filter(mismatch_cond)
        rows_with_mismatches = mismatched_rows_df.count()

        # Collect detail rows (capped). Spark Row drops the alias prefix when
        # both sides have the same column name, so we MUST project to
        # deterministically-named columns before collecting.
        if rows_with_mismatches > 0:
            proj = []
            for pk in pk_cols:
                proj.append(F.col(f"lh.{pk}").alias(f"lh__{pk}"))
                proj.append(F.col(f"pg.{pk}").alias(f"pg__{pk}"))
            for c in non_pk_cols:
                proj.append(F.col(f"lh.{c}").alias(f"lh__{c}"))
                proj.append(F.col(f"pg.{c}").alias(f"pg__{c}"))
            sample_rows = mismatched_rows_df.select(*proj).limit(MAX_DETAIL_ROWS).collect()

            for row in sample_rows:
                pk_vals = {pk: str(row[f"lh__{pk}"] if row[f"lh__{pk}"] is not None else row[f"pg__{pk}"])
                           for pk in pk_cols}
                pk_json = json.dumps(pk_vals)
                for col_name in non_pk_cols:
                    lh_val = row[f"lh__{col_name}"]
                    pg_val = row[f"pg__{col_name}"]
                    if lh_val == pg_val:
                        continue
                    if lh_val is not None and pg_val is not None:
                        try:
                            if abs(float(lh_val) - float(pg_val)) <= numeric_tol:
                                continue
                        except (ValueError, TypeError):
                            pass
                    detail_rows.append({
                        "mismatch_type": "value_diff",
                        "pk_values": pk_json,
                        "column_name": col_name,
                        "lakehouse_value": str(lh_val) if lh_val is not None else None,
                        "postgres_value": str(pg_val) if pg_val is not None else None,
                    })

    # Add "only_in" detail rows
    if only_in_lh > 0:
        only_lh_proj = [F.col(f"lh.{pk}").alias(f"lh__{pk}") for pk in pk_cols]
        only_lh_sample = (joined.filter(F.col(f"pg.{pk_cols[0]}").isNull())
                          .select(*only_lh_proj)
                          .limit(MAX_DETAIL_ROWS).collect())
        for row in only_lh_sample:
            pk_vals = {pk: str(row[f"lh__{pk}"]) for pk in pk_cols}
            detail_rows.append({
                "mismatch_type": "only_in_lakehouse",
                "pk_values": json.dumps(pk_vals),
                "column_name": None,
                "lakehouse_value": "EXISTS",
                "postgres_value": None,
            })

    if only_in_pg > 0:
        only_pg_proj = [F.col(f"pg.{pk}").alias(f"pg__{pk}") for pk in pk_cols]
        only_pg_sample = (joined.filter(F.col(f"lh.{pk_cols[0]}").isNull())
                          .select(*only_pg_proj)
                          .limit(MAX_DETAIL_ROWS).collect())
        for row in only_pg_sample:
            pk_vals = {pk: str(row[f"pg__{pk}"]) for pk in pk_cols}
            detail_rows.append({
                "mismatch_type": "only_in_postgres",
                "pk_values": json.dumps(pk_vals),
                "column_name": None,
                "lakehouse_value": None,
                "postgres_value": "EXISTS",
            })

    total_issues = only_in_lh + only_in_pg + rows_with_mismatches
    status = "PASS" if total_issues == 0 else "FAIL"

    return {
        "lakehouse_count": lh_count,
        "postgres_count": pg_count,
        "count_match": lh_count == pg_count,
        "rows_only_in_lakehouse": only_in_lh,
        "rows_only_in_postgres": only_in_pg,
        "rows_with_mismatches": rows_with_mismatches,
        "status": status,
        "error_message": None if status == "PASS" else f"Mismatches: {only_in_lh} only_lh, {only_in_pg} only_pg, {rows_with_mismatches} col_diff",
    }, detail_rows


print("✅ Comparison engine loaded")


# In[6]:
# In[7]:
# ── Execute comparisons ───────────────────────────────────────────────────────

configs = load_config()
print(f"Found {len(configs)} enabled table(s) to compare\n")

# ── tables_filter: fail-closed if requested table is not in active config ────
# Pipelines pass strings, not lists — accept both forms.
_tf = tables_filter
if isinstance(_tf, str) and _tf.strip():
    try:
        parsed = json.loads(_tf)
        _tf = parsed if isinstance(parsed, list) else [_tf]
    except Exception:
        _tf = [s.strip() for s in _tf.split(",") if s.strip()]

if _tf:
    requested = set(_tf)
    available = {c["table_name"] for c in configs}
    missing = requested - available
    if missing:
        raise RuntimeError(
            f"tables_filter requested {sorted(requested)} but these are not in "
            f"enabled comparison_config: {sorted(missing)}. Refusing to run — "
            f"a missing config row would silently skip validation."
        )
    configs = [c for c in configs if c["table_name"] in requested]
    print(f"  ↳ tables_filter active — narrowing to {len(configs)} table(s): {sorted(requested)}\n")

all_results = []

# NOTE: loop var is `tbl_cfg` not `cfg` — `cfg` is the global helper config
# from _common; shadowing it broke audit writes that need cfg["sql_database"].
for tbl_cfg in configs:
    table_name = tbl_cfg["table_name"]
    mode = tbl_cfg["comparison_mode"]
    filter_days = tbl_cfg["filter_days"] or 7
    lag_minutes = tbl_cfg["safety_lag_minutes"] or 30

    # Compute time window
    now = datetime.utcnow()
    window_end = now - timedelta(minutes=lag_minutes)
    window_start = window_end - timedelta(days=filter_days)

    print(f"── {table_name} ({mode}) ──")
    print(f"   Window: {window_start.strftime('%Y-%m-%d %H:%M')} → {window_end.strftime('%Y-%m-%d %H:%M')} UTC")

    started_at = datetime.utcnow()
    t0 = time.time()
    try:
        sample_rows = []
        if mode == "basic":
            result, sample_rows = run_basic(tbl_cfg, window_start, window_end)
        elif mode == "hash":
            result, sample_rows = run_hash(tbl_cfg, window_start, window_end)
        elif mode == "advanced":
            result, sample_rows = run_advanced(tbl_cfg, window_start, window_end)
        else:
            result = {"status": "ERROR", "error_message": f"Unknown mode: {mode}",
                      "lakehouse_count": None, "postgres_count": None, "count_match": None,
                      "rows_only_in_lakehouse": None, "rows_only_in_postgres": None,
                      "rows_with_mismatches": None}

        duration = time.time() - t0
        ended_at = datetime.utcnow()
        actual_mode = result.pop("comparison_mode", mode)
        audit_status = _classify_status(result)

        # Compute total mismatches for audit (sum of all kinds).
        mismatch_total = sum(
            (result.get(k) or 0)
            for k in ("rows_only_in_lakehouse", "rows_only_in_postgres", "rows_with_mismatches")
        )

        all_results.append(Row(
            run_id=RUN_ID,
            table_name=table_name,
            comparison_mode=actual_mode,
            window_start_utc=window_start,
            window_end_utc=window_end,
            safety_lag_minutes=lag_minutes,
            lakehouse_count=result["lakehouse_count"],
            postgres_count=result["postgres_count"],
            count_match=result["count_match"],
            rows_only_in_lakehouse=result["rows_only_in_lakehouse"],
            rows_only_in_postgres=result["rows_only_in_postgres"],
            rows_with_mismatches=result["rows_with_mismatches"],
            status=audit_status,
            error_message=result["error_message"],
            executed_at=now,
            duration_seconds=round(duration, 2),
        ))

        # Persist per-table audit row immediately — survives engine crash.
        audit_table_result(
            RUN_ID, table_name, actual_mode, audit_status,
            result["postgres_count"], result["lakehouse_count"],
            mismatch_total, window_start, window_end,
            started_at, ended_at, duration, result["error_message"],
        )

        # Persist forensic samples (if any). Best-effort; never blocks run.
        if sample_rows:
            audit_mismatch_samples(RUN_ID, table_name, sample_rows)
            print(f"      📋 captured {len(sample_rows)} sample(s) → validation_mismatch_samples")

        icon = "✅" if audit_status == "pass" else "❌"
        print(f"   {icon} {audit_status} — LH: {result['lakehouse_count']}, PG: {result['postgres_count']} ({duration:.1f}s)")
        if result["error_message"]:
            print(f"      {result['error_message']}")

    except Exception as e:
        duration = time.time() - t0
        ended_at = datetime.utcnow()
        audit_status = _classify_exception(e)
        err_msg = str(e)[:500]
        print(f"   ❌ {audit_status}: {err_msg}")
        all_results.append(Row(
            run_id=RUN_ID,
            table_name=table_name,
            comparison_mode=mode,
            window_start_utc=window_start,
            window_end_utc=window_end,
            safety_lag_minutes=lag_minutes,
            lakehouse_count=None,
            postgres_count=None,
            count_match=None,
            rows_only_in_lakehouse=None,
            rows_only_in_postgres=None,
            rows_with_mismatches=None,
            status=audit_status,
            error_message=err_msg,
            executed_at=now,
            duration_seconds=round(duration, 2),
        ))
        audit_table_result(
            RUN_ID, table_name, mode, audit_status,
            None, None, None, window_start, window_end,
            started_at, ended_at, duration, err_msg,
        )
    print()

print(f"═══ Completed {len(all_results)} comparisons ═══")


# In[8]:
# ── Summary report + finalize audit ──────────────────────────────────────────
#
# Per-table audit rows + sample mismatches have ALREADY been written to SQL DB
# inside the loop. This cell just finalizes the validation_runs SQL DB row
# with totals + final status, then fails the notebook if any table failed.

print(f"\n{'═'*70}")
print(f"  VALIDATION REPORT — {RUN_ID}")
print(f"{'═'*70}\n")

pass_count  = sum(1 for r in all_results if r.status == "pass")
fail_count  = sum(1 for r in all_results if r.status in FAILING_STATUSES)

for r in all_results:
    icon = "✅" if r.status == "pass" else "❌"
    print(f"  {icon} {r.table_name:25s} [{r.comparison_mode:10s}]  {r.status}")
    if r.status != "pass":
        print(f"     LH={r.lakehouse_count}  PG={r.postgres_count}  "
              f"only_lh={r.rows_only_in_lakehouse}  only_pg={r.rows_only_in_postgres}  "
              f"col_diff={r.rows_with_mismatches}")
        if r.error_message:
            print(f"     → {r.error_message}")

print(f"\n{'─'*70}")
print(f"  Total: {len(all_results)}  |  ✅ pass: {pass_count}  |  ❌ fail: {fail_count}")
print(f"{'─'*70}")

overall = "success" if fail_count == 0 else ("partial" if pass_count > 0 else "failed")
print(f"\n  Overall: {overall}")
print(f"\n  Audit:   {GLOBAL_CFG['sql_database']}.{_AUDIT_SCHEMA}.validation_runs (run_id = {RUN_ID})")
print(f"  Samples: {GLOBAL_CFG['sql_database']}.{_AUDIT_SCHEMA}.validation_mismatch_samples")
print()

# Finalize the SQL DB run row BEFORE we potentially raise — so the audit
# trail is complete even on failure.
audit_run_end(
    RUN_ID,
    ended_at=datetime.utcnow(),
    run_status=overall,
    total=len(all_results),
    passed=pass_count,
    failed=fail_count,
)

# ── Fail the notebook if any table is in a failing state ─────────────────────
# Engine-tests run sets fail_on_validation_failure=False because failures ARE
# the expected outcome of the test; the assert step needs to run AFTER this.
_should_fail = fail_on_validation_failure
if isinstance(_should_fail, str):
    _should_fail = _should_fail.strip().lower() not in ("false", "0", "no", "")
if _should_fail:
    fail_if_any(all_results, status_key="status",
                failure_values=FAILING_STATUSES,
                context=f"comparison_engine ({RUN_ID})")
else:
    print(f"  (fail_on_validation_failure=False — not raising on table failures)")

# Emit RUN_ID so callers (pipelines) can correlate downstream activities
# (e.g., scenario_assert needs this to look up the per-table result row).
try:
    notebookutils.notebook.exit(str(RUN_ID))
except Exception:
    pass
