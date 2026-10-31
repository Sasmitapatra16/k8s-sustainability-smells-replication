#!/usr/bin/env python3
"""
detect_ss5.py - SS5 (Unoptimized Data Transfers, redefined for single-cluster
context as intra-cluster pod placement risk) detector.

Scope: Deployment, Job, Pod (per Table II). StatefulSet is explicitly SS6's
resource, not SS5's -- excluded here even though StatefulSets can also
mount PVCs, to avoid double-counting across smells.

Rule: an object is SS5-SMELLY if it references at least one
PersistentVolumeClaim volume (spec.volumes[].persistentVolumeClaim) AND
has NEITHER spec.affinity NOR spec.nodeSelector set (neither constraint
present -- either one alone is enough to clear it). "Without affinity or
nodeSelector" = has neither, per De Morgan's (not A and not B).
"""
from __future__ import annotations
import argparse
import glob
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Optional

import yaml

TARGET_KINDS = {"Deployment", "Job", "Pod"}


@dataclass
class Verdict:
    kind: str
    name: str
    namespace: str
    file: str
    smelly: bool
    reasons: list = field(default_factory=list)
    used_fallback: bool = False


def load_all_documents(path: str) -> tuple[list[dict], bool]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    try:
        docs = [d for d in yaml.safe_load_all(text) if isinstance(d, dict)]
        return docs, False
    except yaml.YAMLError:
        return _regex_fallback_parse(text), True


def _regex_fallback_parse(text: str) -> list[dict]:
    """Best-effort scrape for files that fail full YAML parse (Helm
    control-flow directives). One synthetic doc per --- separated chunk."""
    docs = []
    chunks = re.split(r"^---\s*$", text, flags=re.MULTILINE)
    for chunk in chunks:
        kind_m = re.search(r"^kind:\s*['\"]?(\w+)['\"]?", chunk, re.MULTILINE)
        if not kind_m or kind_m.group(1) not in TARGET_KINDS:
            continue
        kind = kind_m.group(1)
        name_m = re.search(r"^\s{2,4}name:\s*['\"]?(.+?)['\"]?\s*$", chunk, re.MULTILINE)
        ns_m = re.search(r"^\s*namespace:\s*['\"]?([^'\"\s]+)", chunk, re.MULTILINE)

        has_pvc = bool(re.search(r"persistentVolumeClaim:", chunk))
        has_affinity = bool(re.search(r"^\s*affinity:\s*$", chunk, re.MULTILINE))
        has_node_selector = bool(re.search(r"^\s*nodeSelector:\s*$", chunk, re.MULTILINE)) or \
                             bool(re.search(r"^\s*nodeSelector:\s*\S", chunk, re.MULTILINE))

        docs.append({
            "kind": kind,
            "metadata": {
                "name": name_m.group(1) if name_m else "<unknown>",
                "namespace": ns_m.group(1) if ns_m else "default",
            },
            "_fallback": True,
            "_has_pvc": has_pvc,
            "_has_affinity": has_affinity,
            "_has_node_selector": has_node_selector,
        })
    return docs


def get_pod_spec(doc: dict) -> Optional[dict]:
    kind = doc.get("kind")
    spec = doc.get("spec") or {}
    if kind == "Pod":
        return spec
    if kind in ("Deployment", "Job"):
        return (spec.get("template") or {}).get("spec")
    return None


def check_pvc_reference(pod_spec: dict) -> bool:
    volumes = pod_spec.get("volumes") or []
    for v in volumes:
        if isinstance(v, dict) and "persistentVolumeClaim" in v:
            return True
    return False


def evaluate(doc: dict, file_path: str) -> Optional[Verdict]:
    kind = doc.get("kind")
    if kind not in TARGET_KINDS:
        return None

    if doc.get("_fallback"):
        meta = doc["metadata"]
        smelly = doc["_has_pvc"] and not doc["_has_affinity"] and not doc["_has_node_selector"]
        reasons = []
        if smelly:
            reasons.append("PVC referenced, no affinity or nodeSelector (fallback-parsed)")
        return Verdict(
            kind=kind, name=meta["name"], namespace=meta["namespace"],
            file=file_path, smelly=smelly, reasons=reasons, used_fallback=True,
        )

    pod_spec = get_pod_spec(doc)
    if pod_spec is None:
        return None
    meta = doc.get("metadata") or {}
    name = meta.get("name", "<unnamed>")
    namespace = meta.get("namespace", "default")

    has_pvc = check_pvc_reference(pod_spec)
    has_affinity = bool(pod_spec.get("affinity"))
    has_node_selector = bool(pod_spec.get("nodeSelector"))

    smelly = has_pvc and not has_affinity and not has_node_selector
    reasons = []
    if smelly:
        reasons.append("PVC volume referenced, no affinity and no nodeSelector")

    return Verdict(
        kind=kind, name=name, namespace=namespace, file=file_path,
        smelly=smelly, reasons=reasons,
    )


def iter_yaml_files(root: str):
    if os.path.isfile(root):
        yield root
        return
    for path in sorted(glob.glob(os.path.join(root, "**", "*.yaml"), recursive=True)) + \
                sorted(glob.glob(os.path.join(root, "**", "*.yml"), recursive=True)):
        yield path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="YAML file or directory")
    args = ap.parse_args()

    total_targets = 0
    smelly_count = 0
    fallback_count = 0
    verdicts = []
    total_files_scanned = 0
    workload_files = set()
    smelly_files = set()

    for path in iter_yaml_files(args.path):
        total_files_scanned += 1
        try:
            docs, used_fallback = load_all_documents(path)
        except Exception as e:
            print(f"[ERROR] {path}: {e}", file=sys.stderr)
            continue
        for doc in docs:
            v = evaluate(doc, path)
            if v is None:
                continue
            total_targets += 1
            workload_files.add(path)
            if v.used_fallback:
                fallback_count += 1
            if v.smelly:
                smelly_count += 1
                smelly_files.add(path)
            verdicts.append(v)

    for v in verdicts:
        tag = "SMELLY" if v.smelly else "NOT_SMELLY"
        fb = " [fallback]" if v.used_fallback else ""
        reason_str = "; ".join(v.reasons) if v.reasons else "no PVC, or has affinity/nodeSelector"
        print(f"[{tag}] {v.kind}/{v.name} (ns={v.namespace}) file={v.file}{fb} :: {reason_str}")

    n_workload = len(workload_files)
    n_smelly_files = len(smelly_files)
    print(f"\n{smelly_count} / {total_targets} Deployment/Job/Pod objects flagged SS5-smelly "
          f"({fallback_count} parsed via fallback)")
    print(f"Files scanned: {total_files_scanned}")
    print(f"Workload files (>=1 Deployment/Job/Pod): {n_workload}")
    print(f"Smelly files: {n_smelly_files}")
    if n_workload:
        print(f"Prevalence (workload-file basis): {n_smelly_files}/{n_workload} = "
              f"{100 * n_smelly_files / n_workload:.2f}%")
    if total_files_scanned:
        print(f"Prevalence (all-files basis): {n_smelly_files}/{total_files_scanned} = "
              f"{100 * n_smelly_files / total_files_scanned:.2f}%")


if __name__ == "__main__":
    main()
