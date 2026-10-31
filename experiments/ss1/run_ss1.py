import os, re, json, time, argparse, subprocess, datetime
import boto3

if subprocess.run(["pgrep", "-f", "run_ss2.py"], capture_output=True).returncode == 0:
    raise SystemExit("run_ss2.py is still running - not starting")
ap = argparse.ArgumentParser()
ap.add_argument("--pairs", type=int, default=5)
args = ap.parse_args()

HOME = os.path.expanduser("~")
EXP = HOME + "/all7-experiment-v2"
SMELLY = EXP + "/manifests/smelly"
STAGES = ["ingestion", "cleaning", "integration", "transformation", "loading"]
NS = "etl-smelly"
ROOT = HOME + "/ss1-experiment/runs/ss1_" + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
os.makedirs(ROOT + "/manifests_generated", exist_ok=True)
MODES = {
    "ss1_unbounded": None,
    "ss1_right": ("500m", "256Mi", "1000m", "512Mi"),
    "ss1_over": ("2000m", "2Gi", "4000m", "4Gi"),
}
ORDERS = [["ss1_unbounded", "ss1_right", "ss1_over"], ["ss1_right", "ss1_over", "ss1_unbounded"],
          ["ss1_over", "ss1_unbounded", "ss1_right"], ["ss1_unbounded", "ss1_over", "ss1_right"],
          ["ss1_over", "ss1_right", "ss1_unbounded"]]

s3 = boto3.client("s3", endpoint_url="http://localhost:31566", aws_access_key_id="test",
                  aws_secret_access_key="test", region_name="us-east-1")

def kc(*a, inp=None, tmo=900):
    return subprocess.run(["microk8s", "kubectl"] + list(a), input=inp, capture_output=True, text=True, timeout=tmo)

def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)

def make(stage, mode, run_id):
    t = open(f"{SMELLY}/smelly-{stage}.yaml").read()
    t, n1 = re.subn(r'(name: SMELL_MODE\s*\n\s*value: )"smelly"', r'\1"' + mode + '"', t)
    assert n1 == 1
    res = MODES[mode]
    if res:
        rc, rm, lc, lm = res
        def addres(m):
            i = m.group(1)
            return (f'{i}resources:\n{i}  requests:\n{i}    cpu: "{rc}"\n{i}    memory: "{rm}"\n'
                    f'{i}  limits:\n{i}    cpu: "{lc}"\n{i}    memory: "{lm}"\n') + m.group(0)
        t, n2 = re.subn(r"(?m)^([ \t]*)volumeMounts:\n", addres, t)
        assert n2 == 1
    assert t.count('value: "smelly-001"') == 1
    return t.replace('value: "smelly-001"', f'value: "{run_id}"')

def run_variant(mode, run_id, outdir):
    log("===", mode, run_id, "start")
    kc("delete", "jobs", "--all", "-n", NS, "--ignore-not-found=true")
    time.sleep(3)
    t0 = time.time()
    for st in STAGES:
        txt = make(st, mode, run_id)
        if run_id.endswith("run1"):
            open(f"{ROOT}/manifests_generated/{run_id}_{st}.yaml", "w").write(txt)
        r = kc("apply", "-f", "-", inp=txt)
        if r.returncode:
            raise RuntimeError("apply failed: " + r.stderr)
        w = kc("wait", f"job/stage-{st}", "-n", NS, "--for=condition=complete", "--timeout=300s")
        if w.returncode:
            raise RuntimeError(f"{run_id} {st} did not complete: " + w.stderr + w.stdout)
        p = kc("get", "pods", "-n", NS, "-l", f"job-name=stage-{st}", "-o", "wide", "--no-headers")
        open(f"{outdir}/placement_{run_id}.txt", "a").write(p.stdout)
    ms = int((time.time() - t0) * 1000)
    open(f"{outdir}/{run_id}_total_ms.txt", "w").write(str(ms) + "\n")
    with open(f"{outdir}/{run_id}_metrics.jsonl", "w") as f:
        for st in STAGES:
            body = s3.get_object(Bucket="staging-data", Key=f"metrics/{mode}/{run_id}/{st}.json")["Body"].read().decode()
            f.write(json.dumps(json.loads(body)) + "\n")
    log("===", run_id, "complete", ms, "ms")

for mode in MODES:
    for st in STAGES:
        r = kc("apply", "--dry-run=client", "-f", "-", inp=make(st, mode, "preflight"))
        if r.returncode:
            raise RuntimeError("dry-run failed " + mode + " " + st + " " + r.stderr)
log("preflight OK")
s3.copy_object(Bucket="raw-data", Key="input.csv", CopySource={"Bucket": "raw-data", "Key": "iot_50000_narrow.csv"})
b = s3.get_object(Bucket="raw-data", Key="input.csv")["Body"].read().decode()
nrows = len([l for l in b.splitlines() if l.strip()]) - 1
log("input.csv rows", nrows)
if nrows != 50000:
    raise RuntimeError("input is not 50000 rows")
for n in range(1, args.pairs + 1):
    for mode in ORDERS[(n - 1) % len(ORDERS)]:
        run_variant(mode, f"{mode}-run{n}", ROOT)
log("ALL DONE. Results in", ROOT)
