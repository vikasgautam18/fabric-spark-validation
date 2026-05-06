#!/usr/bin/env python3
"""Deploy a Fabric Data Pipeline from a JSON file.

Usage:
    deploy_pipeline.py <local.json> <displayName> [<existingPipelineId>]
                       [--sub KEY=VALUE ...]
                       [--workspace-id <guid>]

If <existingPipelineId> is omitted, the script auto-detects an existing
pipeline with the same displayName in the target workspace and updates
it; if none is found, a new pipeline is created.

Placeholders auto-resolved against the target workspace before upload:

    __WORKSPACE_ID__                     → target workspace GUID
    __NOTEBOOK_ID__:<displayName>        → notebook GUID by displayName
    __PIPELINE_ID__:<displayName>        → pipeline GUID by displayName
    __LAKEHOUSE_ID__:<displayName>       → lakehouse GUID by displayName

This keeps pipeline JSONs portable across workspaces (dev/test/prod) — no
in-place edits required when promoting between environments. Override the
target workspace with --workspace-id or env var FABRIC_WORKSPACE_ID.

`--sub KEY=VALUE` performs an additional literal text substitution and
takes precedence over auto-resolution. May be repeated.
"""
import os, re, sys, json, base64, subprocess, time, urllib.request, urllib.error

DEFAULT_WORKSPACE_ID = "e692fb91-ab30-4b11-a11a-22da087d11d7"
FABRIC_AUD           = "https://api.fabric.microsoft.com"

# Map placeholder prefix → REST item-type filter used to list items.
# (Fabric `/items?type=Notebook` is the lookup endpoint.)
_PLACEHOLDER_TYPES = {
    "__NOTEBOOK_ID__":  "Notebook",
    "__PIPELINE_ID__":  "DataPipeline",
    "__LAKEHOUSE_ID__": "Lakehouse",
}
_PLACEHOLDER_RE = re.compile(
    r"(?P<prefix>__(?:NOTEBOOK|PIPELINE|LAKEHOUSE)_ID__):(?P<name>[A-Za-z0-9_\-./ ]+?)"
    r"(?=[\"' \t\r\n,}\]])"
)


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
        if s == 200:
            try:
                state = json.loads(b).get("status")
            except Exception:
                state = None
            if state in ("Succeeded", "Failed"):
                print(f"LRO: {state}")
                if state == "Failed":
                    print(b.decode(errors="replace"))
                    sys.exit(1)
                return
    print("LRO: timeout")
    sys.exit(1)


def list_items(workspace_id, item_type, headers):
    """Page through /workspaces/{id}/items?type=<type> and return [{id,displayName}]."""
    items, url = [], (f"{FABRIC_AUD}/v1/workspaces/{workspace_id}/items"
                      f"?type={item_type}")
    while url:
        s, _, b = http("GET", url, headers)
        if s != 200:
            raise RuntimeError(f"list {item_type} failed: HTTP {s}: {b.decode(errors='replace')}")
        body = json.loads(b)
        items.extend(body.get("value", []))
        url = body.get("continuationUri")
    return items


def resolve_placeholders(text, workspace_id, headers):
    """Replace __WORKSPACE_ID__ and __<TYPE>_ID__:<name> placeholders inline."""
    text = text.replace("__WORKSPACE_ID__", workspace_id)

    # Group needed lookups by item type so we list each type at most once.
    needed = {}  # type → set(displayName)
    for m in _PLACEHOLDER_RE.finditer(text):
        item_type = _PLACEHOLDER_TYPES[m.group("prefix")]
        needed.setdefault(item_type, set()).add(m.group("name").strip())

    resolved = {}  # (prefix, name) → guid
    for item_type, names in needed.items():
        catalog = {it["displayName"]: it["id"] for it in list_items(workspace_id, item_type, headers)}
        prefix = next(p for p, t in _PLACEHOLDER_TYPES.items() if t == item_type)
        for n in names:
            if n not in catalog:
                raise RuntimeError(
                    f"Cannot resolve {prefix}:{n} — no {item_type} with that "
                    f"displayName in workspace {workspace_id}. "
                    f"Available: {sorted(catalog)}"
                )
            resolved[(prefix, n)] = catalog[n]
            print(f"  resolved {prefix}:{n} → {catalog[n]}")

    def _sub(m):
        return resolved[(m.group("prefix"), m.group("name").strip())]

    return _PLACEHOLDER_RE.sub(_sub, text)


def main():
    args = sys.argv[1:]
    workspace_id = os.environ.get("FABRIC_WORKSPACE_ID", DEFAULT_WORKSPACE_ID)
    subs = {}
    while "--sub" in args:
        i = args.index("--sub")
        kv = args[i + 1]
        k, _, v = kv.partition("=")
        if not k or not v:
            print(f"Invalid --sub value '{kv}', expected KEY=VALUE")
            sys.exit(1)
        subs[k] = v
        del args[i:i + 2]
    while "--workspace-id" in args:
        i = args.index("--workspace-id")
        workspace_id = args[i + 1]
        del args[i:i + 2]

    if len(args) < 2:
        print(__doc__)
        sys.exit(1)
    src, name = args[0], args[1]
    existing_id = args[2] if len(args) > 2 else None

    with open(src) as f:
        pipeline_json = f.read()

    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    pipeline_json = resolve_placeholders(pipeline_json, workspace_id, headers)

    for k, v in subs.items():
        if k not in pipeline_json:
            print(f"⚠️  --sub key '{k}' not found in {src}")
        pipeline_json = pipeline_json.replace(k, v)

    if not existing_id:
        # Auto-detect: list pipelines and match by displayName
        try:
            for it in list_items(workspace_id, "DataPipeline", headers):
                if it.get("displayName") == name:
                    existing_id = it["id"]
                    print(f"  auto-detected existing pipeline → {existing_id}")
                    break
        except RuntimeError as e:
            print(f"⚠️  could not list pipelines for auto-detect: {e}; will create new")

    payload = base64.b64encode(pipeline_json.encode()).decode()
    parts = [{"path": "pipeline-content.json", "payload": payload, "payloadType": "InlineBase64"}]
    definition = {"parts": parts}

    if existing_id:
        url = f"{FABRIC_AUD}/v1/workspaces/{workspace_id}/dataPipelines/{existing_id}/updateDefinition"
        body = {"definition": definition}
        s, h, b = http("POST", url, headers, body)
        print(f"POST {s}")
        if s == 202:
            poll_lro(h["Location"], headers)
        elif s in (200, 201):
            print("Updated synchronously")
        else:
            print(b.decode(errors="replace"))
            sys.exit(1)
        print(f"✅ {name}")
        print(f"PIPELINE_ID={existing_id}")
        return

    url = f"{FABRIC_AUD}/v1/workspaces/{workspace_id}/dataPipelines"
    body = {"displayName": name, "definition": definition}
    s, h, b = http("POST", url, headers, body)
    print(f"POST {s}")
    if s == 202:
        poll_lro(h["Location"], headers)
        s2, h2, b2 = http("GET", url, headers)
        if s2 == 200:
            for p in json.loads(b2).get("value", []):
                if p["displayName"] == name:
                    print(f"✅ {name}")
                    print(f"PIPELINE_ID={p['id']}")
                    return
        print("Created but could not resolve ID")
        sys.exit(1)
    elif s in (200, 201):
        pid = json.loads(b)["id"]
        print(f"✅ {name}")
        print(f"PIPELINE_ID={pid}")
        return
    else:
        print(b.decode(errors="replace"))
        sys.exit(1)


if __name__ == "__main__":
    main()

