#!/usr/bin/env python
# coding: utf-8

# ## Test PostgreSQL Connectivity
#
# Quick smoke tests against the PG instance configured in the
# `validation_config` Variable Library. No credentials in this notebook —
# host/user from VL, password from Key Vault.
#
# Note: This .py file is the source-of-truth for git; the first cell uses the
# `%run` magic which Fabric runs but local Python cannot. Use
# `scripts/deploy_nb.py` to push to Fabric.

# In[1]:
%run _common

# In[2]:

# ── Test 1: Basic Connectivity (SELECT 1) ────────────────────────────────────

print(f"Host:     {cfg['pg_host']}:{cfg['pg_port']}")
print(f"Database: {cfg['pg_database']}")
print(f"User:     {cfg['pg_user']}")

try:
    df = (spark.read
        .format("jdbc")
        .option("url", pg_jdbc_url())
        .option("query", "SELECT 1 AS test")
        .options(**pg_jdbc_props())
        .load())
    df.show()
    print("✅ Connection successful")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    raise


# In[3]:

# ── Test 2: List Databases ───────────────────────────────────────────────────

try:
    dbs = (spark.read
        .format("jdbc")
        .option("url", pg_jdbc_url())
        .option("query",
                "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
        .options(**pg_jdbc_props())
        .load())
    print(f"Databases on this server:")
    dbs.show(truncate=False)
except Exception as e:
    print(f"❌ Failed: {e}")


# In[4]:

# ── Test 3: Read a Specific Table ────────────────────────────────────────────

PG_TABLE = "doctors"
schema   = cfg["pg_schema"]

print(f"Reading {schema}.{PG_TABLE}...\n")
try:
    df = (spark.read
        .format("jdbc")
        .option("url", pg_jdbc_url())
        .option("dbtable", f"{schema}.{PG_TABLE}")
        .options(**pg_jdbc_props())
        .load())
    print(f"Schema:")
    df.printSchema()
    print(f"\nRow count: {df.count():,}")
    print(f"\nSample rows:")
    df.show(5, truncate=50)
except Exception as e:
    print(f"❌ Failed: {e}")


# In[5]:

# ── Test 4: Compare Postgres Table vs Lakehouse Table ────────────────────────

LAKEHOUSE_TABLE = f"{cfg['lakehouse_name']}.{cfg['lakehouse_schema']}.{PG_TABLE}"

print(f"Comparing {schema}.{PG_TABLE} (Postgres) vs {LAKEHOUSE_TABLE} (Lakehouse)...\n")
try:
    pg_df = (spark.read
        .format("jdbc")
        .option("url", pg_jdbc_url())
        .option("query", f"SELECT COUNT(*) AS row_count FROM {schema}.{PG_TABLE}")
        .options(**pg_jdbc_props())
        .load())
    pg_count = pg_df.collect()[0]["row_count"]

    lh_count = spark.table(LAKEHOUSE_TABLE).count()

    print(f"Postgres rows:  {pg_count:,}")
    print(f"Lakehouse rows: {lh_count:,}")

    if pg_count == lh_count:
        print("✅ Row counts match")
    else:
        diff = pg_count - lh_count
        print(f"⚠️  Row count mismatch (Δ = {diff:+,})")
except Exception as e:
    print(f"❌ Failed: {e}")
