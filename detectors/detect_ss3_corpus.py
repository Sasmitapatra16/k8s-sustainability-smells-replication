#!/usr/bin/env python3
"""detect_ss3_corpus.py - corpus-scale SS3 scanner (wraps detect_ss3)."""
import argparse, csv, os, sys
from detect_ss3 import iter_yaml_files, load_all_documents, evaluate

def run(root, out_csv):
    files = list(iter_yaml_files(root))
    if not files:
        print(f"No YAML files under {root}", file=sys.stderr); sys.exit(1)
    verdicts, errs = [], 0
    for p in files:
        try:
            docs, _ = load_all_documents(p)
        except Exception as e:
            errs += 1; print(f"[ERROR] {p}: {e}", file=sys.stderr); continue
        for d in docs:
            v = evaluate(d, p)
            if v: verdicts.append(v)
    wf = {v.file for v in verdicts}
    sf = {v.file for v in verdicts if v.smelly}
    ns = sum(v.smelly for v in verdicts)
    nfb = sum(v.used_fallback for v in verdicts)
    print(f"Total corpus files scanned: {len(files)} ({errs} load errors)")
    print(f"Workload files (>=1 Job/CronJob): {len(wf)}")
    print(f"Files with >=1 SS3-smelly object: {len(sf)}")
    print(f"Prevalence (all files): {100*len(sf)/len(files):.2f}%")
    if wf: print(f"Prevalence (workload files only): {100*len(sf)/len(wf):.2f}%")
    print(f"Object-level: {ns} SMELLY / {len(verdicts)-ns} NOT_SMELLY / {len(verdicts)} total ({nfb} via fallback)")
    if out_csv:
        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["file","kind","name","namespace","verdict","reasons","used_fallback"])
            for v in verdicts:
                w.writerow([v.file, v.kind, v.name, v.namespace,
                            "SMELLY" if v.smelly else "NOT_SMELLY",
                            "; ".join(v.reasons), v.used_fallback])
        print(f"Wrote {out_csv}", file=sys.stderr)

ap = argparse.ArgumentParser()
ap.add_argument("root", nargs="?", default="~/prevalence_data/yaml_corpus_clean/")
ap.add_argument("--out-csv")
a = ap.parse_args()
run(os.path.expanduser(a.root), a.out_csv)
