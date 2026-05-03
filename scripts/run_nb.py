#!/usr/bin/env python3
"""Run a Fabric notebook synchronously and wait for completion.

Usage:
    run_nb.py <notebookId> [parametersJSON]

Returns 0 on Completed, 1 on Failed/Cancelled. Prints final status.
"""
import sys, json, time, subprocess, urllib.request, urllib.error

WORKSPACE_ID = "<your workspace id here>"
LH_ID        = "<your lakehouse id here>"
LH_NAME      = "<your lakehouse name here>"
AUD          = "https://api.fabric.microsoft.com"


def token():
    out = subprocess.check_output(["az", "account", "get-access-token", "--resource", AUD, "-o", "json"])
    return json.loads(out)["accessToken"]


def req(method, url, body=None, tok=None):
    headers = {"Authorization": f"Bearer {tok}"}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    r = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def main():
    nb_id = sys.argv[1]
    params = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    tok = token()

    body = {
        "executionData": {
            "parameters": {k: {"value": v, "type": "string" if isinstance(v, str) else "bool" if isinstance(v, bool) else "int" if isinstance(v, int) else "string"} for k, v in params.items()},
            "configuration": {
                "useStarterPool": True,
                "defaultLakehouse": {
                    "name": LH_NAME,
                    "id": LH_ID,
                    "workspaceId": WORKSPACE_ID,
                },
            },
        }
    }
    url = f"{AUD}/v1/workspaces/{WORKSPACE_ID}/items/{nb_id}/jobs/instances?jobType=RunNotebook"
    status, headers, payload = req("POST", url, body, tok)
    print(f"POST {status}")
    if status not in (200, 202):
        print(payload.decode(errors="replace"))
        sys.exit(2)
    poll_url = headers.get("Location")
    if not poll_url:
        print("No Location header")
        print(payload.decode(errors="replace"))
        sys.exit(2)
    print(f"Poll: {poll_url}")

    deadline = time.time() + 1800
    while time.time() < deadline:
        time.sleep(15)
        s, h, p = req("GET", poll_url, tok=tok)
        if s != 200:
            print(f"  poll {s}: {p.decode(errors='replace')[:200]}")
            continue
        info = json.loads(p)
        st = info.get("status")
        print(f"  status={st}")
        if st in ("Completed", "Succeeded"):
            print("✅ Completed")
            sys.exit(0)
        if st in ("Failed", "Cancelled", "Deduped"):
            print("❌", st)
            print(json.dumps(info, indent=2)[:2000])
            sys.exit(1)
    print("⏰ timeout")
    sys.exit(3)


if __name__ == "__main__":
    main()
