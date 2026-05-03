#!/usr/bin/env python3
# coding: utf-8

# # cleanup_silver_admissions
# One-shot cleanup: removes the orphan silver_admissions row from
# validation.scenarios and validation.comparison_config, and drops the
# orphan Delta table from the lakehouse silver schema.

# In[1]:

%run _common


# In[2]:

before_scen = spark.sql(
    "SELECT COUNT(*) AS n FROM validation.scenarios WHERE target_table = 'silver_admissions'"
).collect()[0]["n"]
before_cfg = spark.sql(
    "SELECT COUNT(*) AS n FROM validation.comparison_config WHERE table_name = 'silver_admissions'"
).collect()[0]["n"]
print(f"Before: scenarios={before_scen}  comparison_config={before_cfg}")


# In[3]:

spark.sql("DELETE FROM validation.scenarios WHERE target_table = 'silver_admissions'")
spark.sql("DELETE FROM validation.comparison_config WHERE table_name = 'silver_admissions'")


# In[4]:

after_scen = spark.sql(
    "SELECT COUNT(*) AS n FROM validation.scenarios WHERE target_table = 'silver_admissions'"
).collect()[0]["n"]
after_cfg = spark.sql(
    "SELECT COUNT(*) AS n FROM validation.comparison_config WHERE table_name = 'silver_admissions'"
).collect()[0]["n"]
print(f"After:  scenarios={after_scen}  comparison_config={after_cfg}")


# In[5]:

lh_db = cfg["lakehouse_name"]
lh_schema = cfg["lakehouse_schema"]
fqn = f"{lh_db}.{lh_schema}.silver_admissions"
exists = spark.catalog.tableExists(fqn)
print(f"Lakehouse table {fqn} exists: {exists}")
if exists:
    spark.sql(f"DROP TABLE {fqn}")
    print(f"Dropped {fqn}")


# In[6]:

print("Cleanup complete.")
notebookutils.notebook.exit("ok")
