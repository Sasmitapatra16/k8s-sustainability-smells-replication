import glob, json, collections, statistics as st

STAGES = ["ingestion", "cleaning", "integration", "transformation", "loading"]
rows = collections.defaultdict(list)   # (config, stage) -> list of per-rep values
for f in sorted(glob.glob("results/rep*/cg.jsonl")):
    rep = f.split("/")[1]
    for line in open(f):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if "SMOKE" in r["run"] or not r.get("cgroup_found", True):
            continue
        rows[(r["run"], r["stage"])].append(
            (rep, r["cg_usage_usec"] / 1e6, r["cg_throttled_usec"] / 1e6, r["wall_s"]))

configs = sorted({k[0] for k in rows})

def table(title, idx):
    print(title)
    print("%-22s" % "config" + "".join("%15s" % s for s in STAGES) + "%12s" % "TOTAL")
    for c in configs:
        vals = []
        for s in STAGES:
            xs = [v[idx] for v in rows[(c, s)]]
            vals.append(st.mean(xs) if xs else float("nan"))
        print("%-22s" % c + "".join("%15.1f" % v for v in vals) + "%12.1f" % sum(vals))
    print()

table("CPU-seconds (mean over reps, from cgroup cpu.stat usage_usec)", 1)
table("Throttled seconds (mean over reps)", 2)
table("Stage wall-clock seconds (mean over reps)", 3)
