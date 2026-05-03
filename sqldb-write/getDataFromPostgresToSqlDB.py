#!/usr/bin/env python
# coding: utf-8

# ## Copy Data from PostgreSQL to Fabric SQL Database
#
# Reads tables from PostgreSQL and writes them into a Fabric SQL Database via
# the Microsoft Spark Connector for SQL.
#
# Connection settings (PG host/user/db, SQL server/db, KV) come from the
# `validation_config` Variable Library; password from Key Vault. Fabric SQL DB
# auth uses Microsoft Entra (no SQL password).
#
# Modes: overwrite | append | merge (staging-table + T-SQL MERGE).

# In[1]:
%run _common

# In[2]:

# ── Per-notebook overrides ───────────────────────────────────────────────────

PG_TABLES = []                            # [] = copy all tables in cfg['pg_schema']
WRITE_MODE = "overwrite"                  # "overwrite" | "append" | "merge"
PARTITION_COLUMN = None
NUM_PARTITIONS = 4
BATCH_SIZE = 100000

# Required when WRITE_MODE = "merge": map each table to its key columns.
#   e.g. {"users": ["id"], "orders": ["order_id", "tenant_id"]}
MERGE_KEYS = {}
MERGE_DELETE_UNMATCHED = False


# In[3]:

# ── Pipeline parameter overrides ─────────────────────────────────────────────

try:
    cfg["pg_database"] = str(pg_database)        # noqa: F821
    print(f"Pipeline override: pg_database = {cfg['pg_database']}")
except NameError:
    pass

try:
    cfg["pg_schema"] = str(pg_schema)            # noqa: F821
    print(f"Pipeline override: pg_schema = {cfg['pg_schema']}")
except NameError:
    pass

try:
    cfg["sql_database"] = str(sql_database)      # noqa: F821
    print(f"Pipeline override: sql_database = {cfg['sql_database']}")
except NameError:
    pass

try:
    raw = str(pg_tables)                          # noqa: F821
    PG_TABLES = [t.strip() for t in raw.split(",") if t.strip()]
    print(f"Pipeline override: PG_TABLES = {PG_TABLES}")
except NameError:
    pass

try:
    WRITE_MODE = str(write_mode)                  # noqa: F821
    print(f"Pipeline override: WRITE_MODE = {WRITE_MODE}")
except NameError:
    pass


# In[4]:

# ── Discover tables if none specified ────────────────────────────────────────

if not PG_TABLES:
    print(f"No tables specified — discovering all tables in '{cfg['pg_schema']}'...")
    discovery_df = (spark.read
        .format("jdbc")
        .option("url", pg_jdbc_url())
        .option("query", f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{cfg['pg_schema']}' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        .options(**pg_jdbc_props())
        .load())
    PG_TABLES = [r["table_name"] for r in discovery_df.collect()]
    print(f"Found {len(PG_TABLES)} table(s): {PG_TABLES}")
else:
    print(f"Tables to copy: {PG_TABLES}")

print(f"\nSource: {cfg['pg_host']}/{cfg['pg_database']}.{cfg['pg_schema']} ({len(PG_TABLES)} tables)")
print(f"Target: {cfg['sql_server']}/{cfg['sql_database']}.{cfg['sql_schema']}")
print(f"Mode:   {WRITE_MODE}")


# In[5]:

# ── SQL DB JDBC + helpers (from _common) ─────────────────────────────────────

# NOTE: The SQL access token is no longer captured once at job start.
#   - run_tsql() opens a fresh JDBC connection (with fresh token) per call.
#   - Bulk-write SQL_OPTS is rebuilt per table inside the copy loop below.
# This makes long-running jobs safe across the AAD token lifetime (Finding #12).

SQL_URL  = sql_jdbc_url()


def run_tsql(sql):
    """Execute a single T-SQL statement against the Fabric SQL DB.

    Uses sql_connection() so each call opens a fresh JDBC connection backed by
    a fresh AAD access token (Finding #12). Wrapped with retry on transient
    SQL/AAD failures (Finding #15)."""
    @with_retry(op_name="sql:tsql")
    def _exec():
        conn = sql_connection()
        try:
            conn.setAutoCommit(False)
            stmt = conn.createStatement()
            stmt.execute(sql)
            conn.commit()
            stmt.close()
        except Exception:
            conn.rollback(); raise
        finally:
            conn.close()
    _exec()


def merge_via_staging(df, target_table, key_cols, delete_unmatched=False):
    """Stage `df` to a transient table, then MERGE into the target table."""
    import uuid
    schema = safe_ident(cfg["sql_schema"], kind="sql_schema")
    target_table = safe_ident(target_table, kind="target_table")
    key_cols = [safe_ident(k, kind="merge_key") for k in key_cols]

    df_cols_raw = df.schema.fieldNames()
    df_cols = [safe_ident(c, kind="column") for c in df_cols_raw]
    missing = [k for k in key_cols if k not in df_cols]
    if missing:
        raise ValueError(f"Merge keys {missing} not found in source columns {df_cols}")

    # Pre-flight: reject duplicate merge keys in source — MERGE behavior is
    # undefined when multiple source rows match the same target row.
    dup_count = (df.groupBy(*key_cols).count()
                 .filter("count > 1").limit(1).count())
    if dup_count > 0:
        raise ValueError(
            f"Source has duplicate merge keys for {target_table} on {key_cols}; "
            f"MERGE requires unique keys in staging."
        )

    target_fqn  = f"[{schema}].[{target_table}]"
    staging_tbl = f"stg_{target_table}_{uuid.uuid4().hex[:8]}"
    safe_ident(staging_tbl, kind="staging_table")  # paranoia
    staging_fqn = f"[{schema}].[{staging_tbl}]"

    # Refresh SQL_OPTS for THIS table — fresh access token (Finding #12).
    write_opts = sql_write_opts(batch_size=BATCH_SIZE)

    # 1. Ensure target exists (zero-row append creates schema if missing)
    (df.limit(0).write
        .format("com.microsoft.sqlserver.jdbc.spark")
        .mode("append")
        .options(**write_opts)
        .option("dbtable", f"{schema}.{target_table}")
        .save())

    try:
        # 2. Stage the full payload — wrap in try so we drop staging on any failure
        (df.write
            .format("com.microsoft.sqlserver.jdbc.spark")
            .mode("overwrite")
            .options(**write_opts)
            .option("dbtable", f"{schema}.{staging_tbl}")
            .option("truncate", "false")
            .save())

        # 3. MERGE
        on_clause = " AND ".join(f"t.[{k}] = s.[{k}]" for k in key_cols)
        update_cols = [c for c in df_cols if c not in key_cols]
        set_clause = ", ".join(f"t.[{c}] = s.[{c}]" for c in update_cols) if update_cols else None
        insert_cols = ", ".join(f"[{c}]" for c in df_cols)
        insert_vals = ", ".join(f"s.[{c}]" for c in df_cols)

        # HOLDLOCK forces serializable isolation on the target → prevents
        # phantom inserts/race during concurrent MERGE on the same key.
        merge_sql = f"MERGE {target_fqn} WITH (HOLDLOCK) AS t USING {staging_fqn} AS s ON {on_clause}"
        if set_clause:
            merge_sql += f" WHEN MATCHED THEN UPDATE SET {set_clause}"
        merge_sql += f" WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        if delete_unmatched:
            merge_sql += " WHEN NOT MATCHED BY SOURCE THEN DELETE"
        merge_sql += ";"

        run_tsql(merge_sql)
    finally:
        try:
            run_tsql(f"DROP TABLE IF EXISTS {staging_fqn};")
        except Exception as drop_err:
            print(f"    ⚠️  Failed to drop staging {staging_fqn}: {drop_err}")


# In[6]:

# ── Validate merge configuration ─────────────────────────────────────────────

if WRITE_MODE == "merge":
    missing_keys = [t for t in PG_TABLES if t not in MERGE_KEYS or not MERGE_KEYS[t]]
    if missing_keys:
        raise ValueError(
            f"WRITE_MODE = 'merge' requires MERGE_KEYS for every table. "
            f"Missing: {missing_keys}"
        )
    print(f"✅ Merge keys configured for {len(PG_TABLES)} table(s)")


# In[7]:

# ── Copy Tables ──────────────────────────────────────────────────────────────

from datetime import datetime

results = []
total = len(PG_TABLES)

for i, table_name in enumerate(PG_TABLES, 1):
    # Validate identifiers BEFORE any SQL interpolation
    try:
        safe_table = safe_ident(table_name, kind="table_name")
        safe_pg_schema = safe_ident(cfg["pg_schema"], kind="pg_schema")
        safe_sql_schema = safe_ident(cfg["sql_schema"], kind="sql_schema")
        if PARTITION_COLUMN:
            safe_part_col = safe_ident(PARTITION_COLUMN, kind="partition_column")
    except IdentifierError as e:
        print(f"\n[{i}/{total}] {table_name} ... ❌ REJECTED: {e}")
        results.append({"table": table_name, "status": "failed",
                        "rows": 0, "seconds": 0.0, "error": str(e)})
        continue

    source_fqn = f"{safe_pg_schema}.{safe_table}"
    target_fqn = f"{safe_sql_schema}.{safe_table}"
    start = datetime.now()

    print(f"\n[{i}/{total}] {source_fqn} → {cfg['sql_database']}.{target_fqn} ({WRITE_MODE}) ...", end=" ")

    try:
        @with_retry(op_name=f"copy:{table_name}")
        def _do_table():
            reader = (spark.read
                .format("jdbc")
                .option("url", pg_jdbc_url())
                .option("dbtable", source_fqn)
                .options(**pg_jdbc_props()))

            if PARTITION_COLUMN:
                bounds = (spark.read
                    .format("jdbc")
                    .option("url", pg_jdbc_url())
                    .option("query",
                        f"SELECT MIN({safe_part_col}) AS lo, MAX({safe_part_col}) AS hi "
                        f"FROM {source_fqn}")
                    .options(**pg_jdbc_props())
                    .load().collect()[0])
                local_reader = (reader
                    .option("partitionColumn", safe_part_col)
                    .option("lowerBound", str(bounds["lo"]))
                    .option("upperBound", str(bounds["hi"]))
                    .option("numPartitions", NUM_PARTITIONS))
            else:
                local_reader = reader

            df = local_reader.load()
            df = coerce_for_sqlserver(df)
            row_count = df.count()

            if WRITE_MODE == "merge":
                # merge_via_staging refreshes its own SQL_OPTS internally
                merge_via_staging(df, table_name, MERGE_KEYS[table_name],
                                  delete_unmatched=MERGE_DELETE_UNMATCHED)
            else:
                # Refresh access token for THIS table — long jobs would
                # otherwise reuse a stale token and fail mid-run (#12).
                write_opts = sql_write_opts(batch_size=BATCH_SIZE)
                writer = (df.write
                    .format("com.microsoft.sqlserver.jdbc.spark")
                    .mode(WRITE_MODE)
                    .options(**write_opts)
                    .option("dbtable", target_fqn))
                if WRITE_MODE == "overwrite":
                    writer = writer.option("truncate", "false")
                writer.save()

            return row_count

        row_count = _do_table()

        elapsed = (datetime.now() - start).total_seconds()
        print(f"✅ {row_count} rows ({elapsed:.1f}s)")
        results.append({"table": table_name, "status": "success",
                        "rows": row_count, "seconds": elapsed})

    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds()
        err_msg = str(e)[:200]
        print(f"❌ FAILED ({elapsed:.1f}s)")
        print(f"    Error: {err_msg}")
        results.append({"table": table_name, "status": "failed",
                        "rows": 0, "seconds": elapsed, "error": err_msg})


# In[8]:

# ── Summary ──────────────────────────────────────────────────────────────────

succeeded = [r for r in results if r["status"] == "success"]
failed    = [r for r in results if r["status"] == "failed"]
total_rows = sum(r["rows"] for r in succeeded)
total_time = sum(r["seconds"] for r in results)

sep = "=" * 60
print(f"\n{sep}\n  COPY SUMMARY\n{sep}")
print(f"  Source:     {cfg['pg_host']}/{cfg['pg_database']}.{cfg['pg_schema']}")
print(f"  Target:     {cfg['sql_server']}/{cfg['sql_database']}.{cfg['sql_schema']}")
print(f"  Tables:     {len(succeeded)} succeeded, {len(failed)} failed")
print(f"  Total rows: {total_rows:,}")
print(f"  Total time: {total_time:.1f}s")
print(f"{sep}\n")

if succeeded:
    print("  ✅ Succeeded:")
    for r in succeeded:
        print(f"     {r['table']:40s} {r['rows']:>10,} rows  ({r['seconds']:.1f}s)")

if failed:
    print("\n  ❌ Failed:")
    for r in failed:
        print(f"     {r['table']:40s} {r.get('error', '')[:80]}")

# Fail the notebook (and any pipeline activity wrapping it) if anything failed.
fail_if_any(results, context="getDataFromPostgresToSqlDB")
