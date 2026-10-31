import glob, json, collections, statistics as st, sys
FOLDER = sys.argv[1]   # e.g. runs/replication_20260702T150734
STAGES = ["ingestion", "cleaning", "integration", "transformation", "loading"]
smelly = collections.defaultdict(list)   # stage -> per-run value
rmax = collections.defaultdict(list)
rsum = collections.defaultdict(list)
for f in sorted(glob.glob(FOLDER + "/smelly-run*_metrics.jsonl")):
    for line in open(f):
        if line.strip():
            r = json.loads(line)
            smelly[r["stage"]].append(r["pod_active_secs"])
for f in sorted(glob.glob(FOLDER + "/refactored-run*_metrics.jsonl")):
    per = collections.defaultdict(list)
    for line in open(f):
        if line.strip():
            r = json.loads(line)
            per[r["stage"]].append(r["pod_active_secs"])
    for s, v in per.items():
        rmax[s].append(max(v))
        rsum[s].append(sum(v))
print("%-15s %9s %12s %12s" % ("Stage", "Smelly", "Refact-MAX", "Refact-SUM"))
tot = [0, 0, 0]
for s in STAGES:
    a, b, c = st.mean(smelly[s]), st.mean(rmax[s]), st.mean(rsum[s])
    tot = [tot[0] + a, tot[1] + b, tot[2] + c]
    print("%-15s %8.2fs %11.2fs %11.2fs" % (s, a, b, c))
print("%-15s %8.2fs %11.2fs %11.2fs" % ("TOTAL", tot[0], tot[1], tot[2]))
print("ratios: MAX/smelly %.2f  SUM/smelly %.2f" % (tot[1] / tot[0], tot[2] / tot[0]))
