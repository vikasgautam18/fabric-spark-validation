#!/usr/bin/env python3
"""Deploy a Fabric notebook from a .py source.

Usage:
    deploy_nb.py <local.py> <displayName> [<folderId>] [<existingNotebookId>]

Both <folderId> and <existingNotebookId> are optional:
  • If <folderId> is omitted, the notebook is created/updated at workspace root.
    Auto-detect still finds an existing notebook by displayName anywhere in
    the workspace (displayName is workspace-unique).
  • If <existingNotebookId> is omitted, the script auto-detects an existing
    notebook with the same displayName and updates it; otherwise creates new.

Environment overrides:
  FABRIC_WORKSPACE_ID — target workspace GUID (default: msdemo workspace)
  FABRIC_FOLDER_ID    — default folderId when not given on the command line

Cell separator in .py:  lines beginning with "# In[N]:" mark cell starts.
Magic-only cells (lines like "%run something") are written as-is into the
ipynb cell source so Fabric's magic dispatcher picks them up.
"""
import os, sys, json, base64, re, subprocess, time, urllib.request, urllib.error

WORKSPACE_ID  = os.environ.get(
    "FABRIC_WORKSPACE_ID", "e692fb91-ab30-4b11-a11a-22da087d11d7"
)
LH_ID         = "5ab25b21-a8a5-4c8b-8237-290290db6dd9"
LH_NAME       = "lhdemo"
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


def find_notebook(name, folder, headers):
    """Return notebook id whose displayName matches name. Prefer an exact
    folderId match if one is in the same folder as `folder`; otherwise fall
    back to a name-only match across the workspace and warn loudly. Returns
    None when no notebook with that displayName exists at all."""
    url = f"{FABRIC_AUD}/v1/workspaces/{WORKSPACE_ID}/notebooks"
    name_matches = []
    while url:
        s, _, b = http("GET", url, headers)
        if s != 200:
            print(f"⚠️  could not list notebooks for auto-detect (HTTP {s}); "
                  f"will create new")
            return None
        body = json.loads(b)
        for it in body.get("value", []):
            if it.get("displayName") == name:
                name_matches.append(it)
        url = body.get("continuationUri")

    if not name_matches:
        return None

    # Prefer the entry in the requested folder if any.
    for it in name_matches:
        if folder and it.get("folderId") == folder:
            return it["id"]

    # Fall back to the first name match. Display names are unique within a
    # workspace, so there's at most one; warn so the operator knows the
    # update is happening in a different folder than the one they passed.
    pick = name_matches[0]
    if folder and pick.get("folderId") != folder:
        print(f"⚠️  notebook '{name}' lives in folderId={pick.get('folderId')!r}, "
              f"not the requested {folder!r}. Updating in place; pass an explicit "
              f"GUID as 4th arg if you'd rather create a new copy.")
    return pick["id"]


def main():
    if len(sys.argv) < 3:
        print(__doc__); sys.exit(1)
    py_path, name = sys.argv[1], sys.argv[2]
    folder = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("FABRIC_FOLDER_ID")
    nb_id  = sys.argv[4] if len(sys.argv) > 4 else None

    print(f"  workspace: {WORKSPACE_ID}"
          + (f"  folder: {folder}" if folder else "  folder: <root>"))

    with open(py_path) as f:
        nb = py_to_ipynb(f.read())
    payload = base64.b64encode(json.dumps(nb, indent=2).encode()).decode()
    parts = [{"path": "notebook-content.ipynb", "payload": payload, "payloadType": "InlineBase64"}]

    token = get_token()
    H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if not nb_id:
        nb_id = find_notebook(name, folder, H)
        if nb_id:
            print(f"  auto-detected existing notebook → {nb_id}")

    if nb_id:
        url = f"{FABRIC_AUD}/v1/workspaces/{WORKSPACE_ID}/notebooks/{nb_id}/updateDefinition"
        body = {"definition": {"format": "ipynb", "parts": parts}}
    else:
        url = f"{FABRIC_AUD}/v1/workspaces/{WORKSPACE_ID}/items"
        body = {"displayName": name, "type": "Notebook",
                "definition": {"format": "ipynb", "parts": parts}}
        if folder:
            body["folderId"] = folder

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
