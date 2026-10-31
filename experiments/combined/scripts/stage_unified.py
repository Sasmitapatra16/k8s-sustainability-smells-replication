import boto3, os, csv, io, hashlib, time, json, logging, sys

def _cg():
    d = {}
    try:
        for ln in open("/sys/fs/cgroup/cpu.stat"):
            k, v = ln.split()
            d["cg_" + k] = int(v)
    except Exception as e:
        d["cg_error"] = str(e)
    for nm, fn in (("cg_mem_peak_bytes", "memory.peak"), ("cg_mem_current_bytes", "memory.current")):
        try:
            d[nm] = int(open("/sys/fs/cgroup/" + fn).read())
        except Exception as e:
            d[nm + "_err"] = str(e)
    d["proc_cpu_secs"] = round(time.process_time(), 3)
    return d


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

STAGE        = os.environ["PIPELINE_STAGE"]
INTENSITY    = int(os.environ.get("INTENSITY", "150"))
ENDPOINT     = os.environ["AWS_ENDPOINT_URL"]
RUN_ID       = os.environ.get("RUN_ID", "exp-unified-001")
TRACK_STATE  = os.environ.get("TRACK_STATE", "false").lower() == "true"
SMELL_MODE   = os.environ.get("SMELL_MODE", "smelly")
PARALLELISM  = int(os.environ.get("PARALLELISM", "1"))
CHUNK_INDEX  = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
CHUNKED      = PARALLELISM > 1

s3 = boto3.client("s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1")

def chunk_key(base_key):
    if not CHUNKED:
        return base_key
    stem, ext = base_key.rsplit(".", 1)
    return f"{stem}_chunk_{CHUNK_INDEX}.{ext}"

# ---------------------------------------------------------------------
# CORRECTED PIPELINE ORDER:
#   Ingestion -> Cleaning -> Integration -> Transformation -> Loading
# ---------------------------------------------------------------------
if CHUNKED:
    INPUTS = {
        "ingestion":      ("raw-data",     f"input_chunk_{CHUNK_INDEX}.csv"),
        "cleaning":       ("staging-data", chunk_key("01_ingested.csv")),
        "integration":    ("staging-data", chunk_key("02_cleaned.csv")),
        "transformation": ("staging-data", chunk_key("03_integrated.csv")),
        "loading":        ("staging-data", chunk_key("04_transformed.csv")),
    }
    OUTPUTS = {
        "ingestion":      ("staging-data", chunk_key("01_ingested.csv")),
        "cleaning":       ("staging-data", chunk_key("02_cleaned.csv")),
        "integration":    ("staging-data", chunk_key("03_integrated.csv")),
        "transformation": ("staging-data", chunk_key("04_transformed.csv")),
        "loading":        ("output-data",  chunk_key("final_output.csv")),
    }
else:
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

pod_start_wall = time.time()
log.info("=== STAGE=%s  RUN_ID=%s  MODE=%s  PARALLELISM=%d  CHUNK=%d ===",
          STAGE, RUN_ID, SMELL_MODE, PARALLELISM, CHUNK_INDEX)
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
        state_key = f"{stage_key}_chunk{CHUNK_INDEX}" if CHUNKED else stage_key
        state["stages"][state_key] = {
            "status": status,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        s3.put_object(Bucket="staging-data", Key=key,
            Body=json.dumps(state, indent=2).encode())
        log.info("[SS6] State updated: %s -> %s", state_key, status)
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

elif STAGE == "integration":
    if CHUNKED:
        all_rows = []
        for ci in range(PARALLELISM):
            key_i = f"02_cleaned_chunk_{ci}.csv"
            obj_i = s3.get_object(Bucket="staging-data", Key=key_i)["Body"].read()
            r_i = csv.DictReader(io.StringIO(obj_i.decode("utf-8", errors="replace")))
            all_rows.extend(list(r_i))
        log.info("[SS2 fan-in] Read %d total rows across %d chunks for global avg_temp",
                  len(all_rows), PARALLELISM)
        idx = {}
        for row in all_rows:
            idx.setdefault(row["sensor"], []).append(row)
        for row in rows:
            peers = idx.get(row["sensor"], [])
            temps = [float(p["temp"]) for p in peers if p.get("temp")]
            row["avg_temp"] = round(sum(temps)/len(temps), 2) if temps else 0
    else:
        idx = {}
        for row in rows:
            idx.setdefault(row["sensor"], []).append(row)
        for row in rows:
            peers = idx.get(row["sensor"], [])
            temps = [float(p["temp"]) for p in peers if p.get("temp")]
            row["avg_temp"] = round(sum(temps)/len(temps), 2) if temps else 0

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
    "parallelism": PARALLELISM, "chunk_index": CHUNK_INDEX if CHUNKED else None,
    "rows": len(rows),
    "pod_active_secs": round(pod_active_secs, 3),
    "log_level": LOG_LEVEL, "verbose_print": VERBOSE_PRINT,
    "s3_read_bytes": read_bytes, "s3_read_ms": round(read_ms, 1),
    "s3_write_bytes": len(body), "s3_write_ms": round(write_ms, 1),
    "cpu_work_secs": round(cpu_secs, 3), **_cg(), "intensity": INTENSITY,
}
metrics_suffix = f"{STAGE}_chunk{CHUNK_INDEX}" if CHUNKED else STAGE
metrics_key = f"metrics/{SMELL_MODE}/{RUN_ID}/{metrics_suffix}.json"
s3.put_object(Bucket="staging-data", Key=metrics_key,
    Body=json.dumps(metrics, indent=2).encode())
log.info("Metrics -> s3://staging-data/%s", metrics_key)

update_state(STAGE, "done")
log.info("=== STAGE %s COMPLETE active=%.2fs ===", STAGE, pod_active_secs)
