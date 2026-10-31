import glob, os, sys, statistics as st
root = sys.argv[1]
groups = {}
for f in glob.glob(root + "/**/*_total_ms.txt", recursive=True):
    name = os.path.basename(f).rsplit("-run", 1)[0]
    size = os.path.basename(os.path.dirname(f))
    groups.setdefault((size, name), []).append(float(open(f).read().split()[0]) / 1000)
for k in sorted(groups):
    v = groups[k]
    print(k[0], k[1], len(v), "runs: mean %.1f sd %.1f" % (st.mean(v), st.stdev(v) if len(v) > 1 else 0))
