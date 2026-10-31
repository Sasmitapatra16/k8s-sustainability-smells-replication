#!/usr/bin/env python3
import csv, glob, importlib.util, json, os, sys
from collections import defaultdict

HOME = os.path.expanduser("~")
CORPUS = os.path.join(HOME, "prevalence_data", "yaml_corpus_clean")
REPOS = os.path.join(HOME, "prevalence_data", "repos.json")
SS1CSV = os.path.join(HOME, "ss1-detection-test", "ss1_v2_flags.csv")
OUT = os.path.join(HOME, "prevalence_all")
os.makedirs(OUT, exist_ok=True)


def load(name, folder):
    d = os.path.join(HOME, folder)
    sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, os.path.join(d, name + ".py"))
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    sys.path.pop(0)
    return m


ss2 = load("detect_ss2", "ss2-detection-test")
ss3 = load("detect_ss3", "ss3-detection-test")
ss4 = load("detect_ss4", "ss4-detection-test")
ss5 = load("detect_ss5", "ss5-detection-test")
ss6 = load("detect_ss6", "ss6-detection-test")
ss7 = load("detect_ss7", "ss7-detection-test")

files = sorted(glob.glob(os.path.join(CORPUS, "*.yaml")) + glob.glob(os.path.join(CORPUS, "*.yml")))
N = len(files)
print("corpus files:", N)


def base(p):
    return os.path.basename(p)


def repo_key(p):
    parts = base(p).split("__")
    return "__".join(parts[:2]) if len(parts) >= 2 else base(p)


SM = ["SS1", "SS2", "SS3", "SS4", "SS5", "SS6", "SS7"]
app = {s: set() for s in SM}
smy = {s: set() for s in SM}


def truthy(v):
    v = str(v).strip().lower()
    if v in ("true", "yes"):
        return True
    try:
        return float(v) > 0
    except ValueError:
        return False


with open(SS1CSV, newline="", encoding="utf-8") as fh:
    for r in csv.DictReader(fh):
        b = base(r["file"])
        app["SS1"].add(b)
        if truthy(r.get("ss1_oversized", "")):
            smy["SS1"].add(b)

groups = defaultdict(list)
for f in files:
    groups[repo_key(f)].append(f)
for g, fl in groups.items():
    cache = {}
    idx = ss2.build_hpa_index(fl, doc_cache=cache)
    for p in fl:
        for doc in cache.get(p, []):
            fd = ss2.classify_workload(doc, idx, source_file=p)
            if fd is None:
                continue
            app["SS2"].add(base(p))
            if fd.verdict == "SMELLY":
                smy["SS2"].add(base(p))

for p in files:
    try:
        docs3, _ = ss3.load_all_documents(p)
        for d in docs3:
            v = ss3.evaluate(d, p)
            if v:
                app["SS3"].add(base(p))
                if v.smelly:
                    smy["SS3"].add(base(p))
    except Exception as e:
        print("SS3 error", base(p), e, file=sys.stderr)
    try:
        docs4, _ = ss4.load_all_documents(p)
        for d in docs4:
            for v in ss4.evaluate(d, p):
                app["SS4"].add(base(p))
                if v.smelly:
                    smy["SS4"].add(base(p))
    except Exception as e:
        print("SS4 error", base(p), e, file=sys.stderr)
    for tag, mod in (("SS5", ss5), ("SS6", ss6)):
        try:
            docs, _ = mod.load_all_documents(p)
            for d in docs:
                v = mod.evaluate(d, p)
                if v:
                    app[tag].add(base(p))
                    if v.smelly:
                        smy[tag].add(base(p))
        except Exception as e:
            print(tag, "error", base(p), e, file=sys.stderr)
    try:
        v = ss7.evaluate_file(p)
        if v:
            app["SS7"].add(base(p))
            if v.smelly:
                smy["SS7"].add(base(p))
    except Exception as e:
        print("SS7 error", base(p), e, file=sys.stderr)

repos = json.load(open(REPOS))
stars = {r["full_name"].replace("/", "__"): r.get("stars", 0) for r in repos}
all_repo_keys = set(stars)
tools = ["airflow", "argo", "kubeflow", "spark", "prefect", "dagster"]
repo_tools = defaultdict(set)
for p in files:
    try:
        t = open(p, encoding="utf-8", errors="replace").read().lower()
    except Exception:
        continue
    for k in tools:
        if k in t:
            repo_tools[repo_key(p)].add(k)

with open(os.path.join(OUT, "per_file_flags.csv"), "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["file", "repo", "stars"] + [s + "_applicable" for s in SM] + [s + "_smelly" for s in SM])
    for p in files:
        b = base(p)
        k = repo_key(p)
        w.writerow([b, k, stars.get(k, "")] + [int(b in app[s]) for s in SM] + [int(b in smy[s]) for s in SM])

names = [base(p) for p in files]
any_sm = set().union(*smy.values())
print()
print("%-5s %8s %8s %9s %9s" % ("smell", "flagged", "applic.", "%applic.", "%allfiles"))
for s in SM:
    a, f = len(app[s]), len(smy[s])
    print("%-5s %8d %8d %8.2f%% %8.2f%%" % (s, f, a, 100.0 * f / a if a else 0, 100.0 * f / N))
print("any smell (union): %d files = %.2f%% of %d" % (len(any_sm), 100.0 * len(any_sm) / N, N))
print("any smell excluding SS1: %d" % len(set().union(*[smy[s] for s in SM if s != "SS1"])))

rk = {base(p): repo_key(p) for p in files}
repos_in_corpus = set(rk.values())
print()
print("repositories with >=1 retrieved file:", len(repos_in_corpus), "of", len(all_repo_keys))
print("%-5s %12s %12s %10s" % ("smell", "repos_flagged", "repos_applic.", "%applic."))
for s in SM:
    ra = {rk[b] for b in app[s]}
    rf = {rk[b] for b in smy[s]}
    print("%-5s %12d %12d %9.2f%%" % (s, len(rf), len(ra), 100.0 * len(rf) / len(ra) if ra else 0))
ra_any = {rk[b] for b in any_sm}
print("repos with any smell: %d of %d = %.2f%%" % (len(ra_any), len(repos_in_corpus), 100.0 * len(ra_any) / len(repos_in_corpus)))

print()
print("star-threshold sensitivity (files flagged / files in corpus at that threshold)")
print("%-8s %6s %6s" % (">=stars", "repos", "files") + "".join("%8s" % s for s in SM) + "%8s" % "any")
for th in (2, 5, 10, 50, 100):
    keep = {b for b in names if stars.get(rk[b], 0) >= th}
    nr = len({rk[b] for b in keep})
    row = "%-8d %6d %6d" % (th, nr, len(keep))
    for s in SM:
        row += "%7.2f%%" % (100.0 * len(smy[s] & keep) / len(keep)) if keep else "%8s" % "-"
    row += "%7.2f%%" % (100.0 * len(any_sm & keep) / len(keep)) if keep else ""
    print(row)

print()
print("repositories whose retrieved files mention each tool anywhere:")
for k in tools:
    print("  %-9s %d" % (k, sum(1 for r in repos_in_corpus if k in repo_tools.get(r, set()))))
print("repositories mentioning none of them:", sum(1 for r in repos_in_corpus if not repo_tools.get(r)))
print()
print("per-file flags written to", os.path.join(OUT, "per_file_flags.csv"))
