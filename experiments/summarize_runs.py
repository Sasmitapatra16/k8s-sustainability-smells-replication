import glob, json, sys, collections, statistics as st
folder = sys.argv[1]
STAGES = ["ingestion", "cleaning", "integration", "transformation", "loading"]
def newd():
    return collections.defaultdict(lambda: collections.defaultdict(list))
runs = collections.defaultdict(newd)
for f in sorted(glob.glob(folder + "/**/*_metrics.jsonl", recursive=True)):
    for line in open(f):
        if not line.strip():
            continue
        r = json.loads(line)
        key = (r["run_id"].rsplit("-run", 1)[0], r["smell_mode"])
        runs[key][r["run_id"]][r["stage"]].append(r)
print("%-26s %-12s %5s %10s %10s %10s %8s" % ("config", "mode", "runs", "podMAX_s", "podSUM_s", "CPU_s", "memMiB"))
for key in sorted(runs):
    mx, sm, cpu, mem = [], [], [], []
    for rid, stg in runs[key].items():
        if not all(s in stg for s in STAGES):
            continue
        mx.append(sum(max(x["pod_active_secs"] for x in stg[s]) for s in STAGES))
        sm.append(sum(sum(x["pod_active_secs"] for x in stg[s]) for s in STAGES))
        cpu.append(sum(x["cg_usage_usec"] for s in STAGES for x in stg[s]) / 1e6)
        mem.append(max(x.get("cg_mem_peak_bytes", 0) for s in STAGES for x in stg[s]) / 1048576)
    if mx:
        print("%-26s %-12s %5d %10.2f %10.2f %10.2f %8.0f" % (key[0], key[1], len(mx), st.mean(mx), st.mean(sm), st.mean(cpu), st.mean(mem)))
