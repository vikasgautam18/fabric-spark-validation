#!/usr/bin/env python
# coding: utf-8

# ## One-Time Migration: Add `last_updated` Column
#
# Adds a `last_updated` TIMESTAMP column to all tables in:
#   1. PostgreSQL `healthcare` schema
#   2. Lakehouse `silver` schema (Delta tables)
#
# For existing rows, `last_updated` is set equal to `created_at`.
# Run this script ONCE. It is idempotent (safe to re-run).

# In[1]:

%run _common


# In[2]:

# ── Per-notebook overrides & locals ──────────────────────────────────────────

cfg["pg_schema"] = "healthcare"

PG_HOST     = cfg["pg_host"]
PG_PORT     = cfg["pg_port"]
PG_DATABASE = cfg["pg_database"]
PG_SCHEMA   = cfg["pg_schema"]
PG_USER     = cfg["pg_user"]
PG_PASSWORD = pg_password()

JDBC_URL = pg_jdbc_url()

# Tables in the healthcare schema
TABLES = ["departments", "doctors", "patients", "appointments", "diagnoses", "prescriptions"]

print("Migration: Add last_updated column to all healthcare tables")
print(f"  PostgreSQL: {PG_HOST}/{PG_DATABASE}.{PG_SCHEMA}")
print(f"  Tables: {', '.join(TABLES)}")


# In[3]:

# ── PostgreSQL: Add last_updated column ───────────────────────────────────────

print("\n═══ PostgreSQL Migration ═══\n")

driver_class = "org.postgresql.Driver"
spark._jvm.Class.forName(driver_class)
conn = spark._jvm.java.sql.DriverManager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)

try:
    conn.setAutoCommit(False)
    stmt = conn.createStatement()

    for table in TABLES:
        fqn = f"{PG_SCHEMA}.{table}"

        # Add column without default so existing rows get NULL
        add_col_sql = f"ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP"
        stmt.execute(add_col_sql)

        # Check if created_at column exists in this table
        check_col_sql = f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{PG_SCHEMA}' AND table_name = '{table}'
            AND column_name = 'created_at'
        """
        rs = stmt.executeQuery(check_col_sql.strip())
        has_created_at = rs.next()
        rs.close()

        # Backfill: use created_at if available, otherwise NOW()
        if has_created_at:
            backfill_sql = f"UPDATE {fqn} SET last_updated = created_at WHERE last_updated IS NULL"
        else:
            backfill_sql = f"UPDATE {fqn} SET last_updated = NOW() WHERE last_updated IS NULL"
        stmt.execute(backfill_sql)

        # Set default for future inserts
        default_sql = f"ALTER TABLE {fqn} ALTER COLUMN last_updated SET DEFAULT NOW()"
        stmt.execute(default_sql)

        src = "created_at" if has_created_at else "NOW()"
        print(f"  ✅ {fqn}: column added, backfilled from {src} & default set")

    conn.commit()
    print("\n✅ PostgreSQL migration complete")

except Exception as e:
    conn.rollback()
    print(f"  ❌ PostgreSQL migration failed: {e}")
    raise

finally:
    conn.close()


# In[4]:

# ── Lakehouse Silver: Add last_updated column ─────────────────────────────────

print("\n═══ Lakehouse Silver Migration ═══\n")

silver_tables = []
try:
    # Check if default lakehouse is attached
    spark.sql("SELECT 1")
    tables_df = spark.sql("SHOW TABLES IN silver")
    silver_tables = [row.tableName for row in tables_df.collect()]
    if not silver_tables:
        print("⚠️  No tables found in silver schema")
except Exception as e:
    err_msg = str(e)
    if "AnalysisException" in err_msg or "SCHEMA_NOT_FOUND" in err_msg or "silver" in err_msg.lower():
        print(f"⚠️  Silver schema not available: {e}")
        print("   Skipping lakehouse migration (attach a lakehouse with silver schema to run this)")
    else:
        print(f"⚠️  Could not list silver tables: {e}")
        print("   Skipping lakehouse migration")

for table in silver_tables:
    fqn = f"silver.{table}"
    try:
        # Check if column already exists
        cols = [f.name for f in spark.table(fqn).schema.fields]
        if "last_updated" in cols:
            print(f"  ⏭️  {fqn}: last_updated already exists")
            continue

        # Add column to Delta table
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS (last_updated TIMESTAMP)")

        # Backfill from created_at if that column exists
        if "created_at" in cols:
            spark.sql(f"UPDATE {fqn} SET last_updated = created_at WHERE last_updated IS NULL")
            print(f"  ✅ {fqn}: column added & backfilled from created_at")
        else:
            spark.sql(f"UPDATE {fqn} SET last_updated = current_timestamp() WHERE last_updated IS NULL")
            print(f"  ✅ {fqn}: column added & set to current_timestamp (no created_at found)")

    except Exception as e:
        print(f"  ❌ {fqn}: {e}")

if silver_tables:
    print("\n✅ Lakehouse silver migration complete")
else:
    print("\n⏭️  Lakehouse silver migration skipped (no tables)")


# In[5]:

# ── Verification ──────────────────────────────────────────────────────────────

print("\n═══ Verification ═══\n")

# PostgreSQL verification
print("PostgreSQL:")
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

for table in TABLES:
    fqn = f"{PG_SCHEMA}.{table}"
    df = spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("query", f"SELECT COUNT(*) AS total, COUNT(last_updated) AS has_last_updated FROM {fqn}") \
        .options(**JDBC_PROPS) \
        .load()
    row = df.collect()[0]
    print(f"  {fqn:30s} → {row['total']} rows, {row['has_last_updated']} with last_updated")

# Silver verification
if silver_tables:
    print("\nLakehouse Silver:")
    for table in silver_tables:
        fqn = f"silver.{table}"
        try:
            total = spark.table(fqn).count()
            has_col = "last_updated" in [f.name for f in spark.table(fqn).schema.fields]
            print(f"  {fqn:30s} → {total} rows, last_updated={'✅' if has_col else '❌'}")
        except Exception as e:
            print(f"  {fqn:30s} → Error: {e}")

print("\n🏁 Migration script complete. Safe to delete this notebook.")
