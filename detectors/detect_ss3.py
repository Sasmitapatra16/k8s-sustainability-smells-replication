#!/usr/bin/env python3
"""
detect_ss3.py - SS3 (Ignoring Resource Lifecycles) detector.

Scope: Job, CronJob only (per Table II).

Rule (OR of two independent triggers):
  (a) Missing ttlSecondsAfterFinished on the Job spec (or, for CronJob,
      on .spec.jobTemplate.spec). A templated value (Helm/Kustomize,
      e.g. "{{ .Values.ttl }}") counts as PRESENT -- we cannot resolve
      it, so we do not flag it as missing (same conservative stance
      SS2 took on templated replicas: don't guess, don't flag).
  (b) An emptyDir volume present in the pod template whose volume name
      or a mountPath referencing it contains a checkpoint/state/data
      naming hint (checkpoint, state, data, cache) -- i.e. it looks
      like it's being used to hold pipeline data/progress that should
      be persisted, not scratch space. Reuses the naming-hint approach
      already used in SS6.

A Job/CronJob is SS3-SMELLY if (a) OR (b) is true. Both reasons are
reported when both apply.
"""
from __future__ import annotations
import argparse
import glob
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml

TARGET_KINDS = {"Job", "CronJob"}
_TEMPLATE_PATTERN = re.compile(r"\{\{.*\}\}|\$\{.*\}")
NAMING_HINTS = {"checkpoint", "state", "data", "cache"}


@dataclass
class Verdict:
    kind: str
    name: str
    namespace: str
    file: str
    smelly: bool
    reasons: list = field(default_factory=list)
    used_fallback: bool = False


def is_templated(value: Any) -> bool:
    return isinstance(value, str) and bool(_TEMPLATE_PATTERN.search(value))


def load_all_documents(path: str) -> tuple[list[dict], bool]:
    """Try full YAML parse; on failure, fall back to a regex scraper
    for un-rendered Helm templates. Returns (docs, used_fallback)."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    try:
        docs = [d for d in yaml.safe_load_all(text) if isinstance(d, dict)]
        return docs, False
    except yaml.YAMLError:
        return _regex_fallback_parse(text), True


def _regex_fallback_parse(text: str) -> list[dict]:
    """Best-effort scrape for files that aren't valid YAML (Helm control flow).
    One synthetic doc per Job/CronJob chunk of a --- separated file.
    Commented-out lines are ignored. The naming-hint check is restricted to
    the NAME of an emptyDir volume and the mountPath of a mount of that volume,
    not the whole chunk (the old whole-chunk search matched 'data' inside
    'metadata:' and flagged every chunk containing an emptyDir)."""
    docs = []
    chunks = re.split(r"^---\s*$", text, flags=re.MULTILINE)
    for chunk in chunks:
        chunk = "\n".join(l for l in chunk.splitlines() if not l.lstrip().startswith("#"))
        kind_m = re.search(r"^kind:\s*['\"]?(\w+)['\"]?", chunk, re.MULTILINE)
        if not kind_m or kind_m.group(1) not in TARGET_KINDS:
            continue
        kind = kind_m.group(1)
        name_m = re.search(r"^\s{2,4}name:\s*['\"]?(.+?)['\"]?\s*$", chunk, re.MULTILINE)
        ns_m = re.search(r"^\s*namespace:\s*['\"]?(.+?)['\"]?\s*$", chunk, re.MULTILINE)
        ttl_m = re.search(r"ttlSecondsAfterFinished:\s*(\S+)", chunk)

        # volumes whose name line is directly followed by emptyDir:
        ed_names = {
            m.group(1).lower()
            for m in re.finditer(
                r"-\s*name:\s*['\"]?([\w.-]+)['\"]?[ \t]*\n\s*emptyDir:", chunk)
        }
        hint_hit = any(h in n for n in ed_names for h in NAMING_HINTS)
        if ed_names and not hint_hit:
            # mountPath of a mount that references an emptyDir volume
            for m in re.finditer(
                    r"-\s*name:\s*['\"]?([\w.-]+)['\"]?[ \t]*\n\s*mountPath:\s*['\"]?(\S+)",
                    chunk):
                if m.group(1).lower() in ed_names and \
                        any(h in m.group(2).lower() for h in NAMING_HINTS):
                    hint_hit = True
                    break

        docs.append({
            "kind": kind,
            "metadata": {
                "name": name_m.group(1) if name_m else "<unknown>",
                "namespace": ns_m.group(1) if ns_m else "default",
            },
            "_fallback": True,
            "_ttl_present": bool(ttl_m),
            "_emptydir_hint": hint_hit,
        })
    return docs


def get_job_spec(doc: dict) -> Optional[dict]:
    if doc.get("_fallback"):
        return None  # handled separately
    if doc.get("kind") == "Job":
        return doc.get("spec") or {}
    if doc.get("kind") == "CronJob":
        spec = doc.get("spec") or {}
        return ((spec.get("jobTemplate") or {}).get("spec")) or {}
    return None


def check_ttl_missing(job_spec: dict) -> bool:
    if "ttlSecondsAfterFinished" not in job_spec:
        return True
    val = job_spec["ttlSecondsAfterFinished"]
    if is_templated(val):
        return False  # templated => present, can't resolve, don't flag
    return False


def check_emptydir_hint(job_spec: dict) -> bool:
    pod_spec = (
        (job_spec.get("template") or {}).get("spec") or {}
    )
    volumes = pod_spec.get("volumes") or []
    emptydir_names = set()
    for v in volumes:
        if not isinstance(v, dict):
            continue
        if "emptyDir" in v:
            vname = str(v.get("name", ""))
            emptydir_names.add(vname)

    if not emptydir_names:
        return False

    # Check volume name itself for a naming hint
    for vname in emptydir_names:
        low = vname.lower()
        if any(hint in low for hint in NAMING_HINTS):
            return True

    # Check mountPath of any container/initContainer mounting that volume
    containers = (pod_spec.get("containers") or []) + (pod_spec.get("initContainers") or [])
    for c in containers:
        if not isinstance(c, dict):
            continue
        for m in c.get("volumeMounts") or []:
            if not isinstance(m, dict):
                continue
            if m.get("name") in emptydir_names:
                mp = str(m.get("mountPath", "")).lower()
                if any(hint in mp for hint in NAMING_HINTS):
                    return True

    return False


def evaluate(doc: dict, file_path: str) -> Optional[Verdict]:
    kind = doc.get("kind")
    if kind not in TARGET_KINDS:
        return None

    if doc.get("_fallback"):
        meta = doc["metadata"]
        reasons = []
        if not doc["_ttl_present"]:
            reasons.append("ttlSecondsAfterFinished missing (fallback-parsed)")
        if doc["_emptydir_hint"]:
            reasons.append("emptyDir + checkpoint/state/data naming hint (fallback-parsed)")
        return Verdict(
            kind=kind,
            name=meta["name"],
            namespace=meta["namespace"],
            file=file_path,
            smelly=bool(reasons),
            reasons=reasons,
            used_fallback=True,
        )

    job_spec = get_job_spec(doc)
    if job_spec is None:
        return None
    meta = doc.get("metadata") or {}
    name = meta.get("name", "<unnamed>")
    namespace = meta.get("namespace", "default")

    reasons = []
    if check_ttl_missing(job_spec):
        reasons.append("ttlSecondsAfterFinished missing")
    if check_emptydir_hint(job_spec):
        reasons.append("emptyDir volume with checkpoint/state/data naming hint")

    return Verdict(
        kind=kind,
        name=name,
        namespace=namespace,
        file=file_path,
        smelly=bool(reasons),
        reasons=reasons,
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
    workload_files = set()   # files containing >=1 Job/CronJob object
    smelly_files = set()     # files containing >=1 smelly Job/CronJob object

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
        reason_str = "; ".join(v.reasons) if v.reasons else "ttl present, no risky emptyDir"
        print(f"[{tag}] {v.kind}/{v.name} (ns={v.namespace}) file={v.file}{fb} :: {reason_str}")

    n_workload = len(workload_files)
    n_smelly_files = len(smelly_files)
    print(f"\n{smelly_count} / {total_targets} Job/CronJob objects flagged SS3-smelly "
          f"({fallback_count} parsed via fallback)")
    print(f"Files scanned: {total_files_scanned}")
    print(f"Workload files (>=1 Job/CronJob): {n_workload}")
    print(f"Smelly files: {n_smelly_files}")
    if n_workload:
        print(f"Prevalence (workload-file basis): {n_smelly_files}/{n_workload} = "
              f"{100 * n_smelly_files / n_workload:.2f}%")
    if total_files_scanned:
        print(f"Prevalence (all-files basis): {n_smelly_files}/{total_files_scanned} = "
              f"{100 * n_smelly_files / total_files_scanned:.2f}%")


if __name__ == "__main__":
    main()
