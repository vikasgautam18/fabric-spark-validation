#!/usr/bin/env python3
"""Deploy a Fabric Data Pipeline from a JSON file.

Usage:
    deploy_pipeline.py <local.json> <displayName> [<existingPipelineId>]

Creates the pipeline if no existing ID is given, otherwise updates it.
Prints the pipeline ID to stdout.
"""
import sys, json, base64, subprocess, time, urllib.request, urllib.error

WORKSPACE_ID = "<your workspace id here>"
FABRIC_AUD   = "https://api.fabric.microsoft.com"


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


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    src, name = sys.argv[1], sys.argv[2]
    existing_id = sys.argv[3] if len(sys.argv) > 3 else None

    with open(src) as f:
        pipeline_json = f.read()

    payload = base64.b64encode(pipeline_json.encode()).decode()
    parts = [{"path": "pipeline-content.json", "payload": payload, "payloadType": "InlineBase64"}]
    definition = {"parts": parts}

    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if existing_id:
        url = f"{FABRIC_AUD}/v1/workspaces/{WORKSPACE_ID}/dataPipelines/{existing_id}/updateDefinition"
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

    url = f"{FABRIC_AUD}/v1/workspaces/{WORKSPACE_ID}/dataPipelines"
    body = {"displayName": name, "definition": definition}
    s, h, b = http("POST", url, headers, body)
    print(f"POST {s}")
    if s == 202:
        poll_lro(h["Location"], headers)
        # Resolve created ID
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
