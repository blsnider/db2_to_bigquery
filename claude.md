# GCP ↔ BigQuery ↔ Cloud Run Connectivity Guide

This file gives you copy‑pasteable steps to verify connectivity and scheduling for your nightly DB2 → BigQuery pipeline using Google Cloud Workstations and VS Code. Edit the placeholders in ALL_CAPS before running commands.

---

## Prerequisites

- You’re working in **Google Cloud Workstations** (or a machine with gcloud + bq CLIs).
- IAM: Your **user** or **service accounts** have appropriate roles (see [IAM Checklist](#iam-checklist)).
- If accessing IBM i from Cloud Run, you have **Cloud VPN/Interconnect** + **Serverless VPC Connector**.
- Python 3.10+ if you want to run the small local scripts.

---

## 1) Workstations → BigQuery (auth + basic query)

### A. Verify gcloud context
```bash
gcloud config set project YOUR_PROJECT_ID
gcloud auth list
gcloud auth application-default print-access-token
```

### B. Quick BigQuery checks (CLI)
```bash
# List datasets
bq ls --project_id=YOUR_PROJECT_ID

# Run a trivial query
bq query --use_legacy_sql=false 'SELECT "ok" AS status, CURRENT_TIMESTAMP() AS ts'
```

### C. Programmatic check (Python)
Create `check_bq.py`:
```python
from google.cloud import bigquery
client = bigquery.Client()
sql = 'SELECT "ok" AS status, CURRENT_TIMESTAMP() AS ts'
print(list(client.query(sql).result()))
```
Run:
```bash
python3 check_bq.py
```

---

## 2) Workstations → Cloud Run (invoke service)

### A. Find your service URL
```bash
gcloud run services list --platform=managed --region=REGION
gcloud run services describe SERVICE_NAME --region=REGION --format='value(status.url)'
```

### B. Invoke (IAM-protected recommended)
```bash
URL=$(gcloud run services describe SERVICE_NAME --region=REGION --format='value(status.url)')
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" "$URL"
```

### C. If public (unauthenticated)
```bash
curl "$URL"
```

> If you get **403**, grant **Cloud Run Invoker** to your principal on the service.

---

## 3) Cloud Run → BigQuery (end-to-end service test)

Add a health endpoint inside your Cloud Run app to run a trivial BigQuery query.

### Flask snippet
```python
# main.py (excerpt)
from flask import Flask, jsonify
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()

@app.get("/bq-health")
def bq_health():
    rows = list(bq.query('SELECT "ok" AS status, CURRENT_TIMESTAMP() ts').result())
    return jsonify({"status": rows[0]["status"], "ts": str(rows[0]["ts"])})
```

### Deploy
```bash
gcloud run deploy bq-health   --source .   --region REGION   --allow-unauthenticated=false   --service-account SA_NAME@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### Invoke
```bash
URL=$(gcloud run services describe bq-health --region REGION --format='value(status.url)')
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" "$URL/bq-health"
```

Expected JSON:
```json
{"status":"ok","ts":"2025-01-01 01:23:45+00:00"}
```

---

## 4) Cloud Run → IBM i (DB2) TCP reachability

Confirm the service can reach your on‑prem host/port through the **Serverless VPC Connector**.

### Ensure your service uses the connector
```bash
gcloud run services update SERVICE_NAME   --region REGION   --vpc-connector CONNECTOR_NAME   --egress-settings all
```

### Add a TCP probe endpoint
```python
# main.py (excerpt)
import socket
from flask import request, jsonify

@app.get("/tcpcheck")
def tcpcheck():
    host = request.args.get("host")
    port = int(request.args.get("port", "50000"))
    s = socket.socket()
    s.settimeout(3.0)
    try:
        s.connect((host, port))
        s.close()
        return jsonify({"reachable": True, "host": host, "port": port})
    except Exception as e:
        return jsonify({"reachable": False, "host": host, "port": port, "error": str(e)}), 500
```

### Invoke
```bash
URL=$(gcloud run services describe SERVICE_NAME --region REGION --format='value(status.url)')
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" "$URL/tcpcheck?host=IBM_I_DNS_OR_IP&port=50000"
```

If `reachable: true`, the connector, routes, and firewall are aligned. If not, check:
- VPC connector subnet range and egress settings
- Cloud VPN/Interconnect routes
- Firewall rules from connector range → IBM i port (DRDA often 446 or 50000)
- DNS resolution (Private DNS zone or configure on-prem DNS)

---

## 5) Cloud Scheduler → Cloud Run (trigger)

Create a scheduled job (America/Chicago) to hit your run endpoint nightly.

```bash
URL=$(gcloud run services describe SERVICE_NAME --region REGION --format='value(status.url)')

gcloud scheduler jobs create http run-daily   --schedule="15 2 * * *"   --time-zone="America/Chicago"   --uri="$URL/run"   --oauth-service-account-email SA_NAME@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

**Manual run + inspect:**
```bash
gcloud scheduler jobs run run-daily
gcloud scheduler jobs describe run-daily   --format='value(lastAttemptStatus.responseStatus.code,lastAttemptStatus.responseStatus.message)'
```

---

## IAM Checklist

Assign to the **Cloud Run service account** (the runtime identity):
- `roles/bigquery.jobUser`
- `roles/bigquery.dataViewer` (read) and/or `roles/bigquery.dataEditor` (write)
- `roles/secretmanager.secretAccessor` (if using Secret Manager)

Assign to the **Scheduler job’s service account**:
- `roles/run.invoker` on the target Cloud Run service

If invoking from your **user identity** during tests:
- `roles/run.invoker` on the service (or use an ID token as shown)

---

## Troubleshooting Tips

- **403 invoking Cloud Run** → missing `run.invoker` on the caller principal.
- **BigQuery permission errors** → add `jobUser` and `dataViewer/dataEditor` to the **service account running Cloud Run**.
- **DB2 connection timeouts** → confirm Serverless VPC Connector egress, routes, firewall to IBM i, and correct DRDA port.
- **DNS issues to on‑prem host** → configure a Private DNS zone for the IBM i domain or ensure on‑prem DNS is resolvable from the VPC.
- **Library creds** → prefer Application Default Credentials (ADC). Inside Cloud Run, the attached service account is used automatically by client libraries.

---

## Snippets You Can Reuse

### Minimal Flask app with both checks
```python
from flask import Flask, jsonify, request
from google.cloud import bigquery
import socket

app = Flask(__name__)
bq = bigquery.Client()

@app.get("/bq-health")
def bq_health():
    rows = list(bq.query('SELECT "ok" AS status, CURRENT_TIMESTAMP() ts').result())
    return jsonify({"status": rows[0]["status"], "ts": str(rows[0]["ts"])})

@app.get("/tcpcheck")
def tcpcheck():
    host = request.args.get("host")
    port = int(request.args.get("port", "50000"))
    s = socket.socket(); s.settimeout(3.0)
    try:
        s.connect((host, port)); s.close()
        return jsonify({"reachable": True, "host": host, "port": port})
    except Exception as e:
        return jsonify({"reachable": False, "host": host, "port": port, "error": str(e)}), 500
```

### Deploy and test (replace placeholders)
```bash
gcloud run deploy diag-service --source . --region REGION --allow-unauthenticated=false   --service-account SA_NAME@YOUR_PROJECT_ID.iam.gserviceaccount.com

URL=$(gcloud run services describe diag-service --region REGION --format='value(status.url)')
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" "$URL/bq-health"
curl -H "Authorization: Bearer $TOKEN" "$URL/tcpcheck?host=IBM_I_DNS_OR_IP&port=50000"
```

---

## Notes

- Use **Serverless VPC Connector** only when you need private egress to on‑prem; otherwise keep it off.
- Keep endpoints like `/tcpcheck` non-public and remove them after verification.
- For reproducible environments, keep a `requirements.txt` and pin versions where necessary.

---

Happy shipping! Edit this file as your living runbook.
