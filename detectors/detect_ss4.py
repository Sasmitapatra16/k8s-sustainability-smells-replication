#!/usr/bin/env python3
"""
detect_ss4.py - SS4 (Excessive Logging) detector.

Scope: any container (in Deployment, Job, Pod, StatefulSet, CronJob, etc.
-- any object with a pod spec somewhere in it, plus bare Pods).

Rule (OR of two independent triggers, case-insensitive):
  (a) An env var named LOG_LEVEL with value DEBUG, TRACE, or ALL.
  (b) An env var named VERBOSE_PRINT with value "true".

Either condition alone is sufficient -- they do NOT need to co-occur.
A container is SS4-smelly if (a) OR (b) is true.
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

_TEMPLATE_PATTERN = re.compile(r"\{\{.*\}\}|\$\{.*\}")
LOG_LEVEL_SMELLY_VALUES = {"debug", "trace", "all"}
# Kinds that can actually carry a container/pod spec somewhere in them.
# Restricting to these in the fallback path mirrors what iter_pod_specs()
# already does implicitly in the full-parse path (a ConfigMap/Secret/RBAC
# object naturally yields no pod spec and gets skipped there -- the
# fallback path needs the same restriction or it inflates the workload-
# file denominator with objects that could never contain the smell).
CONTAINER_BEARING_KINDS = {
    "Deployment", "Job", "Pod", "StatefulSet", "DaemonSet",
    "CronJob", "ReplicaSet", "ReplicationController",
}


@dataclass
class Verdict:
    kind: str
    name: str
    namespace: str
    file: str
    container: str
    smelly: bool
    reasons: list = field(default_factory=list)
    used_fallback: bool = False


def is_templated(value: Any) -> bool:
    return isinstance(value, str) and bool(_TEMPLATE_PATTERN.search(value))


def load_all_documents(path: str) -> tuple[list[dict], bool]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    try:
        docs = [d for d in yaml.safe_load_all(text) if isinstance(d, dict)]
        return docs, False
    except yaml.YAMLError:
        return _regex_fallback_parse(text), True


def _regex_fallback_parse(text: str) -> list[dict]:
    """Best-effort scrape of kind/name/namespace/env-var hints from a file
    that isn't valid YAML (Helm control-flow directives). One synthetic
    doc per --- separated chunk, with a flat list of (container, name,
    value) env var hits found anywhere in the chunk via regex -- we can't
    reliably attribute env vars to specific containers without a real
    parse, so in fallback mode we report at the object level with
    container="<fallback-scan>"."""
    docs = []
    chunks = re.split(r"^---\s*$", text, flags=re.MULTILINE)
    for chunk in chunks:
        kind_m = re.search(r"^kind:\s*['\"]?(\w+)['\"]?", chunk, re.MULTILINE)
        if not kind_m or kind_m.group(1) not in CONTAINER_BEARING_KINDS:
            continue
        kind = kind_m.group(1)
        name_m = re.search(r"^\s{2,4}name:\s*['\"]?(.+?)['\"]?\s*$", chunk, re.MULTILINE)
        ns_m = re.search(r"^\s*namespace:\s*['\"]?([^'\"\s]+)", chunk, re.MULTILINE)

        env_hits = []
        # LOG_LEVEL: name/value pairs typically appear as
        #   - name: LOG_LEVEL
        #     value: DEBUG
        # or inline map style: LOG_LEVEL: DEBUG
        for m in re.finditer(
            r"name:\s*['\"]?LOG_LEVEL['\"]?\s*\n\s*value:\s*['\"]?(\w+)['\"]?",
            chunk, re.IGNORECASE,
        ):
            if m.group(1).lower() in LOG_LEVEL_SMELLY_VALUES:
                env_hits.append(f"LOG_LEVEL={m.group(1)} (fallback-parsed)")
        for m in re.finditer(r"LOG_LEVEL:\s*['\"]?(\w+)['\"]?", chunk, re.IGNORECASE):
            if m.group(1).lower() in LOG_LEVEL_SMELLY_VALUES:
                env_hits.append(f"LOG_LEVEL={m.group(1)} (fallback-parsed)")

        for m in re.finditer(
            r"name:\s*['\"]?VERBOSE_PRINT['\"]?\s*\n\s*value:\s*['\"]?(\w+)['\"]?",
            chunk, re.IGNORECASE,
        ):
            if m.group(1).lower() == "true":
                env_hits.append("VERBOSE_PRINT=true (fallback-parsed)")
        for m in re.finditer(r"VERBOSE_PRINT:\s*['\"]?(\w+)['\"]?", chunk, re.IGNORECASE):
            if m.group(1).lower() == "true":
                env_hits.append("VERBOSE_PRINT=true (fallback-parsed)")

        docs.append({
            "kind": kind,
            "metadata": {
                "name": name_m.group(1) if name_m else "<unknown>",
                "namespace": ns_m.group(1) if ns_m else "default",
            },
            "_fallback": True,
            "_env_hits": list(dict.fromkeys(env_hits)),  # de-dupe, preserve order
        })
    return docs


def iter_pod_specs(doc: dict):
    """Yield (pod_spec) for every pod-template-shaped spec found in a doc.
    Handles Deployment/Job/StatefulSet/DaemonSet (spec.template.spec),
    CronJob (spec.jobTemplate.spec.template.spec), and bare Pod
    (spec directly)."""
    kind = doc.get("kind")
    spec = doc.get("spec") or {}
    if kind == "Pod":
        yield spec
        return
    if kind == "CronJob":
        job_spec = (spec.get("jobTemplate") or {}).get("spec") or {}
        pod_spec = (job_spec.get("template") or {}).get("spec")
        if pod_spec:
            yield pod_spec
        return
    pod_spec = (spec.get("template") or {}).get("spec")
    if pod_spec:
        yield pod_spec


def _looks_like_container(d: dict) -> bool:
    """A dict plausibly represents a single container spec if it has at
    least one of the fields a real container object always/usually has."""
    return isinstance(d, dict) and any(k in d for k in ("image", "env", "command", "args"))


def find_singular_containers(node, depth: int = 0, max_depth: int = 8) -> list:
    """Recursively search for a key literally named "container" (singular,
    not "containers") whose value looks like a container spec. This covers
    Argo-style CRDs (argoproj.io/v1alpha1) that embed exactly one container
    at a CRD-specific path instead of Kubernetes' standard
    spec.template.spec.containers[] list -- e.g. Argo Events Sensor uses
    spec.template.container, Argo Workflows ExecutorPlugin uses
    spec.sidecar.container. Rather than hardcoding every such kind/path
    (which we can only discover by hand, one corpus file at a time), this
    catches any of them structurally: find a dict under a "container" key
    that itself looks like a container (has image/env/command/args).
    Bounded depth to avoid runaway recursion on pathological documents."""
    found = []
    if depth > max_depth or not isinstance(node, (dict, list)):
        return found
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "container" and _looks_like_container(value):
                found.append(value)
            else:
                found.extend(find_singular_containers(value, depth + 1, max_depth))
    elif isinstance(node, list):
        for item in node:
            found.extend(find_singular_containers(item, depth + 1, max_depth))
    return found


def check_container_env(container: dict) -> list:
    reasons = []
    env_list = container.get("env") or []
    for e in env_list:
        if not isinstance(e, dict):
            continue
        ename = str(e.get("name", ""))
        eval_ = e.get("value")
        if is_templated(eval_):
            continue  # can't resolve, don't guess
        if eval_ is None:
            continue  # e.g. valueFrom (configMapKeyRef/secretKeyRef) --
            # value not present in this manifest, can't resolve without
            # following the reference; documented limitation, not a bug.
        # Coerce to string for comparison: YAML parses an UNQUOTED true as
        # the boolean True, not the string "true" -- e.g.
        #   - name: VERBOSE_PRINT
        #     value: true
        # yields eval_ == True (bool), which would silently fail an
        # isinstance(eval_, str) check and get skipped entirely. Comparing
        # str(eval_) instead catches booleans, ints, etc. the same way a
        # human reading the YAML would.
        eval_str = str(eval_)
        if ename.upper() == "LOG_LEVEL":
            if eval_str.strip().lower() in LOG_LEVEL_SMELLY_VALUES:
                reasons.append(f"LOG_LEVEL={eval_str}")
        if ename.upper() == "VERBOSE_PRINT":
            if eval_str.strip().lower() == "true":
                reasons.append("VERBOSE_PRINT=true")
    return reasons


def evaluate(doc: dict, file_path: str) -> list[Verdict]:
    verdicts = []
    kind = doc.get("kind")

    if doc.get("_fallback"):
        meta = doc["metadata"]
        hits = doc["_env_hits"]
        verdicts.append(Verdict(
            kind=kind, name=meta["name"], namespace=meta["namespace"],
            file=file_path, container="<fallback-scan>",
            smelly=bool(hits), reasons=hits, used_fallback=True,
        ))
        return verdicts

    meta = doc.get("metadata") or {}
    name = meta.get("name", "<unnamed>")
    namespace = meta.get("namespace", "default")

    for pod_spec in iter_pod_specs(doc):
        containers = (pod_spec.get("containers") or []) + (pod_spec.get("initContainers") or [])
        for c in containers:
            if not isinstance(c, dict):
                continue
            cname = c.get("name", "<unnamed-container>")
            reasons = check_container_env(c)
            verdicts.append(Verdict(
                kind=kind, name=name, namespace=namespace, file=file_path,
                container=cname, smelly=bool(reasons), reasons=reasons,
            ))

    # Argo-style CRDs (Sensor, ExecutorPlugin, etc.) that embed a single
    # "container:" object at a non-standard path instead of the usual
    # spec.template.spec.containers[] list. Only reached for objects whose
    # kind wasn't already handled by iter_pod_specs (Deployment/Job/Pod/
    # StatefulSet/DaemonSet/CronJob/ReplicaSet/ReplicationController all
    # return zero singular "container" matches, so this is a no-op for them
    # and doesn't double-count).
    for c in find_singular_containers(doc.get("spec")):
        cname = c.get("name", "<unnamed-container>")
        reasons = check_container_env(c)
        verdicts.append(Verdict(
            kind=kind, name=name, namespace=namespace, file=file_path,
            container=cname, smelly=bool(reasons), reasons=reasons,
        ))
    return verdicts


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

    total_containers = 0
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
            for v in evaluate(doc, path):
                total_containers += 1
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
        reason_str = "; ".join(v.reasons) if v.reasons else "no excessive-logging env vars"
        print(f"[{tag}] {v.kind}/{v.name} container={v.container} (ns={v.namespace}) "
              f"file={v.file}{fb} :: {reason_str}")

    n_workload = len(workload_files)
    n_smelly_files = len(smelly_files)
    print(f"\n{smelly_count} / {total_containers} containers flagged SS4-smelly "
          f"({fallback_count} parsed via fallback)")
    print(f"Files scanned: {total_files_scanned}")
    print(f"Workload files (>=1 container evaluated): {n_workload}")
    print(f"Smelly files: {n_smelly_files}")
    if n_workload:
        print(f"Prevalence (workload-file basis): {n_smelly_files}/{n_workload} = "
              f"{100 * n_smelly_files / n_workload:.2f}%")
    if total_files_scanned:
        print(f"Prevalence (all-files basis): {n_smelly_files}/{total_files_scanned} = "
              f"{100 * n_smelly_files / total_files_scanned:.2f}%")


if __name__ == "__main__":
    main()
