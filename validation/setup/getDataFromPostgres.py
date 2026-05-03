#!/usr/bin/env python
# coding: utf-8

# ## Copy Data from PostgreSQL to Fabric Lakehouse
#
# Reads tables from PostgreSQL and writes them as Delta tables into a Fabric
# Lakehouse. Connection settings come from the `validation_config` Variable
# Library; password from Key Vault.
#
# Per-notebook overrides (PG_TABLES, WRITE_MODE, etc.) live in cell 2.
# Pipeline parameters can override anything in cell 3.

# In[1]:
%run _common

# In[2]:

# ── Per-notebook overrides ───────────────────────────────────────────────────

PG_TABLES = []                            # [] = copy all tables in cfg['pg_schema']
WRITE_MODE = "overwrite"                  # "overwrite" or "append"
PARTITION_COLUMN = None                   # numeric/date col for parallel reads
NUM_PARTITIONS = 4


# In[3]:

# ── Pipeline parameter overrides ─────────────────────────────────────────────
# Notebook-activity parameters override the VL/per-notebook defaults above.

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
print(f"Target: {cfg['lakehouse_name']}.{cfg['lakehouse_schema']}")
print(f"Mode:   {WRITE_MODE}")


# In[5]:

# ── Copy Tables ──────────────────────────────────────────────────────────────

from datetime import datetime

# Validate the lakehouse target up front (used in CREATE SCHEMA below)
_lh_db = safe_ident(cfg["lakehouse_name"], kind="lakehouse_name")
_lh_schema = safe_ident(cfg["lakehouse_schema"], kind="lakehouse_schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_lh_db}.{_lh_schema}")

results = []
total = len(PG_TABLES)

for i, table_name in enumerate(PG_TABLES, 1):
    # Validate identifiers BEFORE any SQL interpolation
    try:
        safe_table = safe_ident(table_name, kind="table_name")
        safe_schema = safe_ident(cfg["pg_schema"], kind="pg_schema")
        safe_lh_db = safe_ident(cfg["lakehouse_name"], kind="lakehouse_name")
        safe_lh_schema = safe_ident(cfg["lakehouse_schema"], kind="lakehouse_schema")
        if PARTITION_COLUMN:
            safe_part_col = safe_ident(PARTITION_COLUMN, kind="partition_column")
    except IdentifierError as e:
        print(f"\n[{i}/{total}] {table_name} ... ❌ REJECTED: {e}")
        results.append({"table": table_name, "status": "failed",
                        "rows": 0, "seconds": 0.0, "error": str(e)})
        continue

    source_fqn = f"{safe_schema}.{safe_table}"
    target_fqn = f"{safe_lh_db}.{safe_lh_schema}.{safe_table}"
    start = datetime.now()

    print(f"\n[{i}/{total}] {source_fqn} → {target_fqn} ...", end=" ")

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
            row_count = df.count()

            (df.write
                .format("delta")
                .mode(WRITE_MODE)
                .option("overwriteSchema", "true")
                .saveAsTable(target_fqn))
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


# In[6]:

# ── Summary ──────────────────────────────────────────────────────────────────

succeeded = [r for r in results if r["status"] == "success"]
failed    = [r for r in results if r["status"] == "failed"]
total_rows = sum(r["rows"] for r in succeeded)
total_time = sum(r["seconds"] for r in results)

sep = "=" * 60
print(f"\n{sep}\n  COPY SUMMARY\n{sep}")
print(f"  Source:     {cfg['pg_host']}/{cfg['pg_database']}.{cfg['pg_schema']}")
print(f"  Target:     {cfg['lakehouse_name']}.{cfg['lakehouse_schema']}")
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
fail_if_any(results, context="getDataFromPostgres")
