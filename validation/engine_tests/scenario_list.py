#!/usr/bin/env python
# coding: utf-8

# # scenario_list
# Reads `validation.scenarios` and emits a JSON array of enabled scenarios
# (scenario_id, target_table) so the parent pipeline can ForEach over them.
# Kept tiny on purpose: pipelines need a structured "items" payload and a
# Lookup activity over a Lakehouse Delta table is awkward to maintain.

# In[1]:

%run _common


# In[2]:

import json

# Optional pipeline filter — comma-separated scenario_ids. Empty = all enabled.
try:
    only = str(scenario_ids_filter).strip()  # noqa: F821
except NameError:
    only = ""

where = "WHERE enabled = true"
if only:
    ids = ",".join("'" + s.strip().replace("'", "''") + "'" for s in only.split(",") if s.strip())
    if ids:
        where += f" AND scenario_id IN ({ids})"

rows = spark.sql(f"""
    SELECT scenario_id, target_table
    FROM validation.scenarios
    {where}
    ORDER BY target_table, scenario_id
""").collect()

items = [{"scenario_id": r["scenario_id"], "target_table": r["target_table"]} for r in rows]
payload = json.dumps(items)
print(f"Found {len(items)} enabled scenario(s)")
for it in items:
    print(f"  • {it['scenario_id']:<40s} → {it['target_table']}")

notebookutils.notebook.exit(payload)
