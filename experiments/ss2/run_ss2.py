import os, re, sys, json, time, argparse, subprocess, datetime
import boto3

ap = argparse.ArgumentParser()
ap.add_argument("--sizes", default="10000,50000,200000")
ap.add_argument("--pairs", type=int, default=5)
args = ap.parse_args()

HOME = os.path.expanduser("~")
EXP = HOME + "/all7-experiment-v2"
SMELLY = EXP + "/manifests/smelly"
STAGES = ["ingestion", "cleaning", "integration", "transformation", "loading"]
NS = "etl-smelly"
ROOT = HOME + "/ss2-experiment/runs/ss2_" + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
GEN = ROOT + "/manifests_generated"
os.makedirs(GEN, exist_ok=True)

s3 = boto3.client("s3", endpoint_url="http://localhost:31566", aws_access_key_id="test",
                  aws_secret_access_key="test", region_name="us-east-1")

def kc(*a, inp=None, tmo=900):
    return subprocess.run(["microk8s", "kubectl"] + list(a), input=inp,
                          capture_output=True, text=True, timeout=tmo)

def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)

def make_smelly(stage, run_id):
    t = open(f"{SMELLY}/smelly-{stage}.yaml").read()
    assert t.count('value: "smelly-001"') == 1
    return t.replace('value: "smelly-001"', f'value: "{run_id}"')

def make_ss2(stage, run_id):
    t = open(f"{SMELLY}/smelly-{stage}.yaml").read()
    t, n1 = re.subn(r"(?m)^([ \t]*)parallelism:[ \t]*1[ \t]*$", r"\1completionMode: Indexed\n\1parallelism: 3", t)
    t, n2 = re.subn(r"(?m)^([ \t]*)completions:[ \t]*1[ \t]*$", r"\1completions: 3", t)
    t, n3 = re.subn(r'(name: SMELL_MODE\s*\n\s*value: )"smelly"', r'\1"ss2"', t)
    def addpar(m):
        return m.group(0) + f'{m.group(1)}- name: PARALLELISM\n{m.group(2)}value: "3"\n'
    t, n4 = re.subn(r'(?m)^([ \t]*)- name: RUN_ID\n([ \t]*)value: "smelly-001"\n', addpar, t)
    def aff(m):
        i = m.group(1)
        return m.group(0) + (f"{i}affinity:\n{i}  podAntiAffinity:\n"
            f"{i}    requiredDuringSchedulingIgnoredDuringExecution:\n"
            f"{i}    - labelSelector:\n{i}        matchLabels:\n"
            f"{i}          app: etl-smelly\n{i}          stage: {stage}\n"
            f"{i}      topologyKey: kubernetes.io/hostname\n")
    t, n5 = re.subn(r"(?m)^([ \t]*)restartPolicy: Never\n", aff, t)
    assert (n1, n2, n3, n4, n5) == (1, 1, 1, 1, 1), (stage, n1, n2, n3, n4, n5)
    assert f"stage: {stage}" in t
    return t.replace('value: "smelly-001"', f'value: "{run_id}"')

def collect(mode, run_id, out):
    n = 0
    with open(out, "w") as f:
        for st in STAGES:
            names = [f"{st}.json"] if mode == "smelly" else [f"{st}_chunk{c}.json" for c in range(3)]
            for nm in names:
                key = f"metrics/{mode}/{run_id}/{nm}"
                body = s3.get_object(Bucket="staging-data", Key=key)["Body"].read().decode()
                f.write(json.dumps(json.loads(body)) + "\n")
                n += 1
    return n

def run_variant(mode, run_id, outdir):
    log("===", mode, run_id, "start")
    kc("delete", "jobs", "--all", "-n", NS, "--ignore-not-found=true")
    time.sleep(3)
    t0 = time.time()
    for st in STAGES:
        txt = make_ss2(st, run_id) if mode == "ss2" else make_smelly(st, run_id)
        if run_id.endswith("run1"):
            open(f"{GEN}/{run_id}_{st}.yaml", "w").write(txt)
        r = kc("apply", "-f", "-", inp=txt)
        if r.returncode:
            raise RuntimeError("apply failed: " + r.stderr)
        w = kc("wait", f"job/stage-{st}", "-n", NS, "--for=condition=complete", "--timeout=300s")
        if w.returncode:
            raise RuntimeError(f"{run_id} {st} did not complete: " + w.stderr + w.stdout)
        p = kc("get", "pods", "-n", NS, "-l", f"job-name=stage-{st}", "-o", "wide", "--no-headers")
        with open(f"{outdir}/placement_{run_id}.txt", "a") as f:
            f.write(p.stdout)
        log("  ", st, "done")
    ms = int((time.time() - t0) * 1000)
    open(f"{outdir}/{run_id}_total_ms.txt", "w").write(str(ms) + "\n")
    n = collect(mode, run_id, f"{outdir}/{run_id}_metrics.jsonl")
    log("===", run_id, "complete", ms, "ms,", n, "metric records")

def setup_size(size):
    log("setup size", size)
    s3.copy_object(Bucket="raw-data", Key="input.csv",
                   CopySource={"Bucket": "raw-data", "Key": f"iot_{size}_narrow.csv"})
    kc("delete", "job", "prepare-chunks-v2", "-n", "default", "--ignore-not-found=true")
    time.sleep(3)
    r = kc("apply", "-f", EXP + "/scripts/prepare-chunks-job-v2.yaml")
    if r.returncode:
        raise RuntimeError(r.stderr)
    w = kc("wait", "job/prepare-chunks-v2", "-n", "default", "--for=condition=complete", "--timeout=300s")
    if w.returncode:
        raise RuntimeError("chunk prep failed: " + w.stderr + w.stdout)
    def rows(key):
        b = s3.get_object(Bucket="raw-data", Key=key)["Body"].read().decode()
        return len([l for l in b.splitlines() if l.strip()]) - 1
    inp = rows("input.csv")
    tot = sum(rows(f"input_chunk_{c}.csv") for c in range(3))
    log("  input rows", inp, "chunk rows", tot)
    if inp != size or tot != size:
        raise RuntimeError(f"row mismatch size={size} input={inp} chunks={tot}")

# preflight: dry-run validation of generated manifests
for st in STAGES:
    for mk in (make_smelly, make_ss2):
        r = kc("apply", "--dry-run=client", "-f", "-", inp=mk(st, "preflight"))
        if r.returncode:
            raise RuntimeError("dry-run failed: " + st + " " + r.stderr)
log("preflight OK: all generated manifests validate")

for size in [int(x) for x in args.sizes.split(",")]:
    setup_size(size)
    outdir = f"{ROOT}/size_{size}"
    os.makedirs(outdir, exist_ok=True)
    for n in range(1, args.pairs + 1):
        order = ["smelly", "ss2"] if n % 2 == 1 else ["ss2", "smelly"]
        for mode in order:
            run_variant(mode, f"{mode}-s{size}-run{n}", outdir)
log("ALL DONE. Results in", ROOT)
