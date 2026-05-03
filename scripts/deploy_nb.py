#!/usr/bin/env python3
"""Deploy a Fabric notebook from a .py source.

Usage:
    deploy_nb.py <local.py> <displayName> <folderId> [<existingNotebookId>]

Cell separator in .py:  lines beginning with "# In[N]:" mark cell starts.
Magic-only cells (lines like "%run something") are written as-is into the
ipynb cell source so Fabric's magic dispatcher picks them up.
"""
import sys, json, base64, re, subprocess, time, urllib.request, urllib.error

WORKSPACE_ID  = "<your workspace id here>"
LH_ID         = "<your lakehouse id here>"
LH_NAME       = "<your lakehouse name here>"
FABRIC_AUD    = "https://api.fabric.microsoft.com"


def py_to_ipynb(py_text):
    parts = re.split(r"^# In\[\d+\]:\s*\n", py_text, flags=re.MULTILINE)
    header, cell_sources = parts[0], parts[1:]
    md_lines = [l[2:] if l.startswith("# ") else (l[1:] if l.startswith("#") else l)
                for l in header.splitlines() if l.startswith("#")]
    cells = []
    if md_lines:
        cells.append({"cell_type": "markdown", "metadata": {},
                      "source": ["\n".join(md_lines).strip()]})
    for s in cell_sources:
        s = s.strip("\n")
        lines = [l + "\n" for l in s.split("\n")]
        if lines:
            lines[-1] = lines[-1].rstrip("\n")
        # Tag cells that declare themselves as parameter cells. We accept any
        # of these case-insensitive markers anywhere in the source:
        #   "parameters tag", "parameters cell", "set from pipeline"
        meta = {}
        body = "".join(lines).lower()
        if ("parameters tag" in body or "parameters cell" in body
                or "set from pipeline" in body):
            meta["tags"] = ["parameters"]
        cells.append({"cell_type": "code", "execution_count": None,
                      "metadata": meta, "outputs": [], "source": lines})
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
            "microsoft": {"language": "python",
                          "ms_spell_check": {"ms_spell_check_language": "en"}},
            "nteract": {"version": "nteract-front-end@1.0.0"},
            "spark_compute": {"compute_id": "/trident/default",
                              "session_options": {"conf": {}, "enableDebugMode": False}},
            "trident": {"lakehouse": {"default_lakehouse": LH_ID,
                                       "default_lakehouse_name": LH_NAME,
                                       "default_lakehouse_workspace_id": WORKSPACE_ID}}
        },
        "nbformat": 4, "nbformat_minor": 5
    }


def get_token():
    return subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", FABRIC_AUD,
         "--query", "accessToken", "-o", "tsv"]).decode().strip()


def http(method, url, headers, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def poll_lro(loc, headers):
    for _ in range(30):
        time.sleep(4)
        s, h, b = http("GET", loc, headers)
        st = json.loads(b).get("status")
        if st in ("Succeeded", "Failed"):
            return st, json.loads(b)
    return "TimedOut", None


def main():
    if len(sys.argv) < 4:
        print(__doc__); sys.exit(1)
    py_path, name, folder = sys.argv[1], sys.argv[2], sys.argv[3]
    nb_id = sys.argv[4] if len(sys.argv) > 4 else None

    with open(py_path) as f:
        nb = py_to_ipynb(f.read())
    payload = base64.b64encode(json.dumps(nb, indent=2).encode()).decode()
    parts = [{"path": "notebook-content.ipynb", "payload": payload, "payloadType": "InlineBase64"}]

    token = get_token()
    H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if nb_id:
        url = f"{FABRIC_AUD}/v1/workspaces/{WORKSPACE_ID}/notebooks/{nb_id}/updateDefinition"
        body = {"definition": {"format": "ipynb", "parts": parts}}
    else:
        url = f"{FABRIC_AUD}/v1/workspaces/{WORKSPACE_ID}/items"
        body = {"displayName": name, "type": "Notebook", "folderId": folder,
                "definition": {"format": "ipynb", "parts": parts}}

    status, headers, resp = http("POST", url, H, body)
    print(f"POST {status}")
    loc = headers.get("Location") or headers.get("location")
    if loc:
        st, info = poll_lro(loc, H)
        print(f"LRO: {st}")
        if st != "Succeeded":
            print(json.dumps(info, indent=2))
            sys.exit(1)
    elif status >= 400:
        print(resp.decode()); sys.exit(1)
    print(f"✅ {name}")


if __name__ == "__main__":
    main()
