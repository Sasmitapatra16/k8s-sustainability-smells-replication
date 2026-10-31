import os, sys, json, time, threading, subprocess, argparse
import boto3

ap = argparse.ArgumentParser()
ap.add_argument("--reps", type=int, default=1)
ap.add_argument("--outroot", default="results_cg")
ap.add_argument("--smoke", action="store_true")
args = ap.parse_args()

os.chdir(os.path.expanduser("~/smell-experiment-v2"))
STAGES = ["ingestion", "cleaning", "integration", "transformation", "loading"]
RUNS = [("A_baseline", "none"), ("B_smell_ingestion", "ingestion"),
        ("C_smell_cleaning", "cleaning"), ("D_smell_integration", "integration"),
        ("E_smell_transformation", "transformation"), ("F_smell_loading", "loading"),
        ("G_refactored", "none")]
if args.smoke:
    STAGES = ["ingestion"]
    RUNS = [("SMOKE", "none")]

def kc(*a, inp=None, tmo=700):
    return subprocess.run(["microk8s", "kubectl"] + list(a), input=inp,
                          capture_output=True, text=True, timeout=tmo)

def clear():
    s3 = boto3.client("s3", endpoint_url="http://localhost:31566",
                      aws_access_key_id="test", aws_secret_access_key="test",
                      region_name="us-east-1")
    for b in ["staging-data", "output-data"]:
        for o in s3.list_objects_v2(Bucket=b).get("Contents", []):
            k = o["Key"]
            if k.startswith("0") or k == "final_output.csv":
                s3.delete_object(Bucket=b, Key=k)
    print("Staging cleared", flush=True)

def sampler(path, stop):
    with open(path, "w") as f:
        f.write("ts,pod,stage,cpu_m,mem_mi\n")
        while not stop.is_set():
            ts = int(time.time())
            r = kc("top", "pods", "-l", "pipeline-stage", "--no-headers", tmo=60)
            for ln in r.stdout.splitlines():
                p = ln.split()
                if len(p) >= 3:
                    st = p[0].split("-job")[0]
                    cpu = p[1].rstrip("m")
                    mem = p[2].rstrip("Mi")
                    f.write(f"{ts},{p[0]},{st},{cpu},{mem}\n")
            f.flush()
            stop.wait(4)

def run_one(label, smell, outdir):
    print("RUN", label, "smell at", smell, flush=True)
    clear()
    stop = threading.Event()
    th = threading.Thread(target=sampler, args=(f"{outdir}/top_{label}.csv", stop))
    th.start()
    t0 = time.time()
    for st in STAGES:
        kc("delete", "job", f"{st}-job", "--force", "--grace-period=0")
        time.sleep(3)
        mdir = "manifests/smelly" if st == smell else "manifests/clean"
        txt = open(f"{mdir}/{st}.yaml").read().replace("RUN_LABEL", label)
        r = kc("apply", "-f", "-", inp=txt)
        ts0 = time.time()
        w = kc("wait", f"job/{st}-job", "--for=condition=complete", "--timeout=600s")
        if w.returncode:
            kc("wait", f"job/{st}-job", "--for=condition=failed", "--timeout=600s")
        wall = time.time() - ts0
        g = kc("get", "job", f"{st}-job", "-o", "jsonpath={.status.conditions[0].type}")
        status = g.stdout.strip()
        lg = kc("logs", f"job/{st}-job")
        rec = {"run": label, "stage": st, "mdir": mdir, "status": status, "wall_s": round(wall, 2)}
        found = False
        for ln in lg.stdout.splitlines():
            if ln.startswith("CGROUP_STATS "):
                rec.update(json.loads(ln.split("CGROUP_STATS ", 1)[1]))
                found = True
        rec["cgroup_found"] = found
        with open(f"{outdir}/cg.jsonl", "a") as f:
            f.write(json.dumps(rec) + "\n")
        print(f"  [{st}] {mdir} {status} wall={wall:.1f}s cgroup={found}", flush=True)
    dur = int((time.time() - t0) * 1000)
    stop.set()
    th.join()
    with open(f"{outdir}/durations.csv", "a") as f:
        f.write(f"{label},{smell},{dur}\n")
    print(f"  Done: {dur} ms", flush=True)

for rep in range(1, args.reps + 1):
    outdir = f"{args.outroot}/rep{rep}"
    os.makedirs(outdir, exist_ok=True)
    with open(f"{outdir}/durations.csv", "w") as f:
        f.write("run,smell,duration_ms\n")
    open(f"{outdir}/cg.jsonl", "w").close()
    for label, smell in RUNS:
        run_one(label, smell, outdir)
print("ALL DONE", flush=True)
