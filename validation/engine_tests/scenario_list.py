#!/usr/bin/env python
# coding: utf-8

# # scenario_list
# Reads `validation.scenarios` and emits a JSON array of GROUPS:
#   [ {target_table: "...", scenarios: [{scenario_id, target_table}, ...]}, ... ]
# This shape lets the parent pipeline run a parallel outer ForEach over
# distinct target_tables while keeping per-table scenario execution sequential
# (mutations on the same table would otherwise cross-contaminate).

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

# Group by target_table so the parent pipeline can fan out parallel ForEach
# *across* tables while keeping inner ForEach sequential *within* a table.
# Two scenarios that mutate the same lakehouse table cannot run concurrently
# (they would cross-contaminate before the engine sees the drift), but
# scenarios on disjoint tables are independent.
groups = {}
for r in rows:
    tt = r["target_table"]
    groups.setdefault(tt, []).append({
        "scenario_id": r["scenario_id"],
        "target_table": tt,
    })

items = [
    {"target_table": tt, "scenarios": scns}
    for tt, scns in sorted(groups.items())
]
payload = json.dumps(items)
print(f"Found {sum(len(g['scenarios']) for g in items)} enabled scenario(s) "
      f"across {len(items)} target_table group(s):")
for g in items:
    print(f"  • {g['target_table']:<28s} ({len(g['scenarios'])} scenario(s))")
    for s in g["scenarios"]:
        print(f"      - {s['scenario_id']}")

notebookutils.notebook.exit(payload)
