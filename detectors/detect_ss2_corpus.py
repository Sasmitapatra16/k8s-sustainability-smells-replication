#!/usr/bin/env python3
"""
detect_ss2_corpus.py - corpus-scale SS2 scanner.

Corpus layout: flat directory, filenames encode repo as
owner__repo__path...yaml. HPA matching scoped per-repo via
--group-by repo-prefix (default) to avoid cross-repo false matches.
"""
from __future__ import annotations
import argparse, csv, glob, os, sys
from collections import defaultdict
from detect_ss2 import FALLBACK_PARSE_COUNT, build_hpa_index, classify_workload



def discover_yaml_files(root: str) -> list[str]:
    files = []
    for ext in ("*.yaml", "*.yml"):
        files.extend(glob.glob(os.path.join(root, "**", ext), recursive=True))
    return sorted(files)


def repo_prefix_key(filename: str) -> str:
    base = os.path.basename(filename)
    parts = base.split("__")
    if len(parts) >= 2:
        return "__".join(parts[:2])
    return base


def group_files(files: list[str], root: str, mode: str) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = defaultdict(list)
    if mode == "file":
        for f in files:
            groups[f] = [f]
        return groups
    if mode == "repo-prefix":
        for f in files:
            groups[repo_prefix_key(f)].append(f)
        return groups
    for f in files:
        rel = os.path.relpath(f, root)
        parts = rel.split(os.sep)
        top = parts[0] if len(parts) > 1 else "__root__"
        groups[top].append(f)
    return groups


def run(root: str, group_mode: str, out_csv, explain_grouping: bool) -> None:
    files = discover_yaml_files(root)
    if not files:
        print(f"No YAML files found under {root}", file=sys.stderr)
        sys.exit(1)

    groups = group_files(files, root, group_mode)
    if explain_grouping:
        print(f"[grouping] mode={group_mode} files={len(files)} groups={len(groups)}", file=sys.stderr)

    all_findings = []
    for group_name, group_files_list in groups.items():
        doc_cache: dict = {}
        hpa_index = build_hpa_index(group_files_list, doc_cache=doc_cache)
        for path in group_files_list:
            for doc in doc_cache.get(path, []):
                finding = classify_workload(doc, hpa_index, source_file=path)
                if finding is not None:
                    all_findings.append(finding)

    total_files = len(files)
    workload_files = {f.file for f in all_findings}
    smelly_files = {f.file for f in all_findings if f.verdict == "SMELLY"}
    n_workload_files = len(workload_files)
    n_smelly_files = len(smelly_files)
    pct_all = 100.0 * n_smelly_files / total_files if total_files else 0.0
    pct_workload = 100.0 * n_smelly_files / n_workload_files if n_workload_files else 0.0

    print(f"Total corpus files scanned:        {total_files}")
    print(f"Files containing Deployment/StatefulSet (workload files): {n_workload_files}")
    print(f"Files with >=1 SS2-smelly object:  {n_smelly_files}")
    print(f"Prevalence (all files):            {pct_all:.2f}%")
    print(f"Prevalence (workload files only):  {pct_workload:.2f}%")

    n_objects = len(all_findings)
    n_smelly_objects = sum(1 for f in all_findings if f.verdict == "SMELLY")
    n_not_smelly = sum(1 for f in all_findings if f.verdict == "NOT_SMELLY")
    print(f"\nObject-level: {n_smelly_objects} SMELLY / {n_not_smelly} NOT_SMELLY / {n_objects} total Deployment+StatefulSet objects evaluated")

    if FALLBACK_PARSE_COUNT["used"]:
        print(
            f"\n[fallback parser] {FALLBACK_PARSE_COUNT['used']} file(s) failed full YAML parsing "
            f"(typically un-rendered Helm templates using control-flow directives) and were recovered "
            f"via regex extraction; {FALLBACK_PARSE_COUNT['recovered_workloads']} Deployment/StatefulSet "
            f"object(s) came from this fallback path rather than a clean parse."
        )

    if out_csv:
        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["file", "kind", "name", "namespace", "replicas", "verdict", "reason", "matched_hpa"])
            for f in all_findings:
                writer.writerow([f.file, f.kind, f.name, f.namespace, f.replicas, f.verdict, f.reason, f.matched_hpa or ""])
        print(f"\nWrote per-object findings to {out_csv}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="SS2 corpus prevalence scan")
    parser.add_argument("root", nargs="?", default="~/prevalence_data/yaml_corpus_clean/")
    parser.add_argument("--group-by", choices=["repo-prefix", "dir", "file"], default="repo-prefix")
    parser.add_argument("--out-csv", default=None)
    parser.add_argument("--explain-grouping", action="store_true")
    args = parser.parse_args()

    root = os.path.expanduser(args.root)
    run(root, args.group_by, args.out_csv, args.explain_grouping)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
