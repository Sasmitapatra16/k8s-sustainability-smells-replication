import boto3, os, csv, io, hashlib, time

STAGE     = os.environ["PIPELINE_STAGE"]
INTENSITY = int(os.environ.get("INTENSITY","150"))

s3 = boto3.client("s3",
    endpoint_url=os.environ["AWS_ENDPOINT_URL"],
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1")

INPUTS = {
    "ingestion":      ("raw-data",     "input.csv"),
    "cleaning":       ("staging-data", "01_ingested.csv"),
    "integration":    ("staging-data", "02_cleaned.csv"),
    "transformation": ("staging-data", "03_integrated.csv"),
    "loading":        ("staging-data", "04_transformed.csv"),
}
OUTPUTS = {
    "ingestion":      ("staging-data", "01_ingested.csv"),
    "cleaning":       ("staging-data", "02_cleaned.csv"),
    "integration":    ("staging-data", "03_integrated.csv"),
    "transformation": ("staging-data", "04_transformed.csv"),
    "loading":        ("output-data",  "final_output.csv"),
}

in_b,  in_k  = INPUTS[STAGE]
out_b, out_k = OUTPUTS[STAGE]

print(f"[{STAGE}] Reading s3://{in_b}/{in_k}", flush=True)
raw  = s3.get_object(Bucket=in_b, Key=in_k)["Body"].read()
print(f"[{STAGE}] {len(raw):,} bytes", flush=True)

reader = csv.DictReader(
    io.StringIO(raw.decode("utf-8", errors="replace")))
rows = list(reader)
print(f"[{STAGE}] {len(rows):,} rows", flush=True)

if STAGE == "ingestion":
    for row in rows:
        row["ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")

elif STAGE == "integration":
    idx = {}
    for row in rows:
        idx.setdefault(row["sensor"], []).append(row)
    for row in rows:
        peers = idx.get(row["sensor"], [])
        temps = [float(p["temp"]) for p in peers if p.get("temp")]
        row["avg_temp"] = round(sum(temps)/len(temps), 2) if temps else 0

elif STAGE == "cleaning":
    cleaned = []
    for row in rows:
        try:
            t = float(row.get("temp", 0))
            h = float(row.get("humidity", 0))
            if -50 <= t <= 100 and 0 <= h <= 100:
                row["status"] = row.get("status", "").lower()
                cleaned.append(row)
        except (ValueError, TypeError):
            pass
    rows = cleaned
    print(f"[{STAGE}] Kept {len(rows):,}", flush=True)

elif STAGE == "transformation":
    for row in rows:
        try:
            t = float(row.get("temp", 0))
            h = float(row.get("humidity", 0))
        except (ValueError, TypeError):
            t, h = 0.0, 0.0
        row["temp_f"]     = round(t * 9/5 + 32, 2)
        row["heat_index"] = round(-8.78 + 1.61*t + 2.34*h - 0.15*t*h, 2)
        row["alert"]      = (
            "RED"   if t > 45 or row.get("status") == "critical" else
            "AMBER" if t > 35 or row.get("status") == "warn"     else
            "GREEN")

elif STAGE == "loading":
    alerts = {"RED": 0, "AMBER": 0, "GREEN": 0}
    for row in rows:
        a = row.get("alert", "GREEN")
        if a in alerts:
            alerts[a] += 1
    print(f"[{STAGE}] Alerts: {alerts}", flush=True)

print(f"[{STAGE}] CPU work intensity={INTENSITY}", flush=True)
start = time.time()
# Use full dataset for CPU work — much heavier load
data = raw
for i in range(INTENSITY):
    # Process entire raw data in large chunks
    parts = [data[j:j+4096] for j in range(0, len(data), 4096)]
    data  = b"".join(hashlib.sha256(p).digest() for p in parts)
    # Expand data back to keep workload consistent
    data  = data * (len(raw) // len(data) + 1)
    data  = data[:len(raw)]
    if (i+1) % 10 == 0:
        print(f"[{STAGE}] {i+1}/{INTENSITY} ({time.time()-start:.0f}s)", flush=True)
print(f"[{STAGE}] CPU done ({time.time()-start:.1f}s)", flush=True)

out = io.StringIO()
if rows:
    w = csv.DictWriter(out,
        fieldnames=list(rows[0].keys()),
        extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)

body = out.getvalue().encode("utf-8")
s3.put_object(Bucket=out_b, Key=out_k, Body=body)
print(f"[{STAGE}] Wrote s3://{out_b}/{out_k} ({len(body):,} bytes) DONE", flush=True)

import time, json

def _cg():
    d = {}
    try:
        for ln in open("/sys/fs/cgroup/cpu.stat"):
            k, v = ln.split()
            d["cg_" + k] = int(v)
    except Exception as e:
        d["cg_error"] = str(e)
    d["proc_cpu_secs"] = round(time.process_time(), 3)
    return d

print("CGROUP_STATS " + json.dumps(dict(stage=STAGE, **_cg())), flush=True)
