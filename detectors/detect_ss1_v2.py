import os, sys, csv, re, json, argparse, collections
import yaml
try:
    Loader = yaml.CSafeLoader
except AttributeError:
    Loader = yaml.SafeLoader

ap = argparse.ArgumentParser()
ap.add_argument("--corpus", default=os.path.expanduser("~/prevalence_data/yaml_corpus_clean"))
ap.add_argument("--out", default=os.path.expanduser("~/ss1-detection-test/ss1_v2_flags.csv"))
ap.add_argument("--legacy-kinds", action="store_true", help="restrict to Pod/Job/Deployment/StatefulSet (old rule coverage)")
args = ap.parse_args()

TEMPLATE = ("spec", "template", "spec")
PODSPEC = {
    "Pod": ("spec",),
    "Job": TEMPLATE,
    "Deployment": TEMPLATE,
    "StatefulSet": TEMPLATE,
    "DaemonSet": TEMPLATE,
    "CronJob": ("spec", "jobTemplate", "spec", "template", "spec"),
}
if args.legacy_kinds:
    PODSPEC = {k: v for k, v in PODSPEC.items() if k in ("Pod", "Job", "Deployment", "StatefulSet")}

MEM = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60,
       "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18, "m": 1e-3}

def dig(d, path):
    for k in path:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

def parse_cpu(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        try:
            if s.endswith("m"):
                return float(s[:-1]) / 1000.0
            return float(s)
        except ValueError:
            return None
    return None

def parse_mem(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        m = re.fullmatch(r"([0-9]*\.?[0-9]+(?:[eE][+-]?[0-9]+)?)(Ki|Mi|Gi|Ti|Pi|Ei|k|K|M|G|T|P|E|m)?", v.strip())
        if not m:
            return None
        return float(m.group(1)) * (MEM[m.group(2)] if m.group(2) else 1)
    return None

def classify(c):
    out = {"unb": None, "over": [], "large": False, "badq": False}
    if not isinstance(c, dict):
        return out
    res = c.get("resources")
    lim = res.get("limits") if isinstance(res, dict) else None
    req = res.get("requests") if isinstance(res, dict) else None
    if not res:
        out["unb"] = "case1_no_resources_block"
    elif not lim:
        out["unb"] = "case2_no_limits"
    elif not isinstance(lim, dict) or lim.get("cpu") is None:
        out["unb"] = "case3_no_cpu_limit"
    elif not isinstance(req, dict) or req.get("cpu") is None:
        out["unb"] = "case4_no_cpu_request"
    if isinstance(lim, dict) and isinstance(req, dict):
        for r, fn in (("cpu", parse_cpu), ("memory", parse_mem)):
            if r in lim and r in req:
                l, q = fn(lim[r]), fn(req[r])
                if l is None or q is None:
                    out["badq"] = True
                elif q > 0 and l > 4 * q:
                    out["over"].append(r)
    for src in (lim, req):
        if isinstance(src, dict):
            cpu = parse_cpu(src.get("cpu")) if "cpu" in src else None
            mem = parse_mem(src.get("memory")) if "memory" in src else None
            if (cpu is not None and cpu >= 4) or (mem is not None and mem >= 8 * 2**30):
                out["large"] = True
    return out

files = []
for root, dirs, fns in os.walk(args.corpus):
    for fn in sorted(fns):
        if fn.startswith("_") or fn.endswith(".log"):
            continue
        files.append(os.path.join(root, fn))

S = collections.Counter()
kind_docs = collections.Counter()
kind_files = collections.Counter()
unb_cases = collections.Counter()
rows = []
for p in files:
    S["files_total"] += 1
    try:
        text = open(p, encoding="utf-8", errors="replace").read()
        docs = [d for d in yaml.load_all(text, Loader=Loader) if d is not None]
    except Exception:
        S["files_unparsable"] += 1
        continue
    S["files_parsed"] += 1
    flat = []
    for d in docs:
        if isinstance(d, dict) and d.get("kind") == "List" and isinstance(d.get("items"), list):
            flat.extend(d["items"])
        else:
            flat.append(d)
    kinds, ncont = set(), 0
    over_res, unb, large, badq = set(), set(), False, False
    for d in flat:
        if not isinstance(d, dict):
            continue
        kind = d.get("kind")
        if kind not in PODSPEC:
            continue
        kinds.add(kind)
        kind_docs[kind] += 1
        pod = dig(d, PODSPEC[kind])
        conts = pod.get("containers") if isinstance(pod, dict) else None
        if not isinstance(conts, list):
            continue
        for c in conts:
            ncont += 1
            r = classify(c)
            if r["unb"]:
                unb.add(r["unb"])
            over_res.update(r["over"])
            large = large or r["large"]
            badq = badq or r["badq"]
    if not kinds:
        continue
    S["files_with_workload"] += 1
    for k in kinds:
        kind_files[k] += 1
    if ncont:
        S["files_with_containers"] += 1
    if over_res:
        S["ss1_oversized_files"] += 1
    if unb:
        S["unbounded_files"] += 1
        for u in unb:
            unb_cases[u] += 1
    if over_res or unb:
        S["old_rule_equivalent_files"] += 1
    if large:
        S["info_large_alloc_files"] += 1
    if badq:
        S["files_with_unparsable_quantity"] += 1
    for k in ("CronJob", "DaemonSet"):
        if k in kinds:
            S[k + "_files"] += 1
            if over_res:
                S[k + "_files_oversized"] += 1
            if unb:
                S[k + "_files_unbounded"] += 1
    rows.append([os.path.relpath(p, args.corpus), "|".join(sorted(kinds)), ncont, int(bool(over_res)),
                 "|".join(sorted(over_res)), "|".join(sorted(unb)), int(large)])

with open(args.out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["file", "kinds", "containers", "ss1_oversized", "oversized_resources", "unbounded_cases", "info_large_alloc"])
    w.writerows(rows)

T = S["files_total"]
W = S["files_with_workload"]
pc = lambda n, d: "%.2f%%" % (100.0 * n / d) if d else "n/a"
print("mode:", "LEGACY kinds (Pod/Job/Deployment/StatefulSet)" if args.legacy_kinds else "ALL kinds (Pod/Job/CronJob/Deployment/StatefulSet/DaemonSet)")
print("files total", T, "| parsed", S["files_parsed"], "| unparsable", S["files_unparsable"])
print("files with a covered workload kind:", W, "(%s of all files)" % pc(W, T))
print("documents per kind:", dict(kind_docs))
print("files per kind:", dict(kind_files))
print("--- SS1 (oversized: limit > 4x request, cpu or memory)")
print("files:", S["ss1_oversized_files"], "| of all files", pc(S["ss1_oversized_files"], T), "| of files with workload", pc(S["ss1_oversized_files"], W))
print("--- unbounded resources (old cases 1-4), reported separately")
print("files:", S["unbounded_files"], "| of all files", pc(S["unbounded_files"], T), "| of files with workload", pc(S["unbounded_files"], W))
print("by case (a file can appear in several):", dict(unb_cases))
print("--- old-rule equivalent (oversized OR unbounded):", S["old_rule_equivalent_files"], "|", pc(S["old_rule_equivalent_files"], T), "of all files")
print("--- informational: any request/limit >= 4 cores or >= 8 GiB:", S["info_large_alloc_files"], "files")
print("files with an unparsable quantity (e.g. Helm placeholders):", S["files_with_unparsable_quantity"])
print("--- CronJob/DaemonSet coverage:", {k: S[k] for k in S if k.startswith(("CronJob", "DaemonSet"))})
print("per-file flags written to", args.out)
