import boto3, os, csv, io, hashlib, time, json, logging, sys

LOG_LEVEL     = os.environ.get("LOG_LEVEL", "DEBUG").upper()
VERBOSE_PRINT = os.environ.get("VERBOSE_PRINT", "true").lower() == "true"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.DEBUG),
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    stream=sys.stdout
)
log = logging.getLogger(os.environ.get("PIPELINE_STAGE", "stage"))

def vprint(msg, *args):
    if VERBOSE_PRINT:
        print(msg % args if args else msg, flush=True)

STAGE       = os.environ["PIPELINE_STAGE"]
INTENSITY   = int(os.environ.get("INTENSITY", "150"))
ENDPOINT    = os.environ["AWS_ENDPOINT_URL"]
RUN_ID      = os.environ.get("RUN_ID", "exp-unified-001")
TRACK_STATE = os.environ.get("TRACK_STATE", "false").lower() == "true"
SMELL_MODE  = os.environ.get("SMELL_MODE", "smelly")

s3 = boto3.client("s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1")

INPUTS = {
    "ingestion":      ("raw-data",     "input.csv"),
    "integration":    ("staging-data", "01_ingested.csv"),
    "cleaning":       ("staging-data", "02_integrated.csv"),
    "transformation": ("staging-data", "03_cleaned.csv"),
    "loading":        ("staging-data", "04_transformed.csv"),
}
OUTPUTS = {
    "ingestion":      ("staging-data", "01_ingested.csv"),
    "integration":    ("staging-data", "02_integrated.csv"),
    "cleaning":       ("staging-data", "03_cleaned.csv"),
    "transformation": ("staging-data", "04_transformed.csv"),
    "loading":        ("output-data",  "final_output.csv"),
}

pod_start_wall = time.time()
log.info("=== STAGE=%s  RUN_ID=%s  MODE=%s ===", STAGE, RUN_ID, SMELL_MODE)
vprint("[%s] VERBOSE mode ON", STAGE)

def update_state(stage_key, status):
    if not TRACK_STATE:
        return
    try:
        key = f"pipeline-state/{RUN_ID}/state.json"
        try:
            obj = s3.get_object(Bucket="staging-data", Key=key)
            state = json.loads(obj["Body"].read())
        except Exception:
            state = {"run_id": RUN_ID, "stages": {}}
        state["stages"][stage_key] = {
            "status": status,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        s3.put_object(Bucket="staging-data", Key=key,
            Body=json.dumps(state, indent=2).encode())
        log.info("[SS6] State updated: %s -> %s", stage_key, status)
    except Exception as e:
        log.warning("[SS6] State update failed: %s", e)

update_state(STAGE, "running")

in_b, in_k = INPUTS[STAGE]
log.info("Reading s3://%s/%s", in_b, in_k)
t_read_start = time.time()
raw  = s3.get_object(Bucket=in_b, Key=in_k)["Body"].read()
t_read_end   = time.time()
read_bytes   = len(raw)
read_ms      = (t_read_end - t_read_start) * 1000
log.info("[SS5] S3 read bytes=%d time_ms=%.1f", read_bytes, read_ms)

reader = csv.DictReader(io.StringIO(raw.decode("utf-8", errors="replace")))
rows   = list(reader)
log.info("Loaded %d rows", len(rows))

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
    dropped = 0
    for row in rows:
        try:
            t = float(row.get("temp", 0))
            h = float(row.get("humidity", 0))
            if -50 <= t <= 100 and 0 <= h <= 100:
                row["status"] = row.get("status", "").lower()
                cleaned.append(row)
            else:
                dropped += 1
        except (ValueError, TypeError):
            dropped += 1
    rows = cleaned
    log.info("Kept %d rows, dropped %d", len(rows), dropped)

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
    log.info("Alert summary: %s", alerts)

log.info("CPU work intensity=%d", INTENSITY)
data    = raw
t_cpu_s = time.time()
for i in range(INTENSITY):
    parts = [data[j:j+4096] for j in range(0, len(data), 4096)]
    data  = b"".join(hashlib.sha256(p).digest() for p in parts)
    data  = data * (len(raw) // len(data) + 1)
    data  = data[:len(raw)]
    if (i+1) % 10 == 0:
        log.debug("CPU %d/%d (%.0fs)", i+1, INTENSITY, time.time()-t_cpu_s)
t_cpu_e  = time.time()
cpu_secs = t_cpu_e - t_cpu_s
log.info("CPU done in %.2fs", cpu_secs)

out_b, out_k = OUTPUTS[STAGE]
out = io.StringIO()
if rows:
    w = csv.DictWriter(out, fieldnames=list(rows[0].keys()), extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
body = out.getvalue().encode("utf-8")

t_write_start = time.time()
s3.put_object(Bucket=out_b, Key=out_k, Body=body)
t_write_end   = time.time()
write_ms      = (t_write_end - t_write_start) * 1000
log.info("[SS5] S3 write bytes=%d time_ms=%.1f", len(body), write_ms)

pod_active_secs = time.time() - pod_start_wall

metrics = {
    "run_id": RUN_ID, "smell_mode": SMELL_MODE, "stage": STAGE,
    "rows": len(rows),
    "pod_active_secs": round(pod_active_secs, 3),
    "log_level": LOG_LEVEL, "verbose_print": VERBOSE_PRINT,
    "s3_read_bytes": read_bytes, "s3_read_ms": round(read_ms, 1),
    "s3_write_bytes": len(body), "s3_write_ms": round(write_ms, 1),
    "cpu_work_secs": round(cpu_secs, 3), "intensity": INTENSITY,
}
metrics_key = f"metrics/{SMELL_MODE}/{RUN_ID}/{STAGE}.json"
s3.put_object(Bucket="staging-data", Key=metrics_key,
    Body=json.dumps(metrics, indent=2).encode())
log.info("Metrics -> s3://staging-data/%s", metrics_key)

update_state(STAGE, "done")
log.info("=== STAGE %s COMPLETE active=%.2fs ===", STAGE, pod_active_secs)
