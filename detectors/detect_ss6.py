#!/usr/bin/env python3
"""
detect_ss6.py - SS6 (Poor State Management) detector.

Scope: StatefulSet only (per Table II).

Rule: a StatefulSet is SS6-SMELLY if it has no volumeClaimTemplates
(missing entirely, or present but an empty list). A StatefulSet without
per-pod persistent volume claim templates isn't actually persisting
per-replica state the way StatefulSets are designed to -- it's using the
StatefulSet controller for its identity/ordering guarantees only, without
the state-persistence half of the contract. That's the Kubernetes-native
proxy for "poor state management" (Table I's TRACK_STATE:false / no
pipeline progress recorded concept, operationalized structurally since an
app-level env var convention isn't reliably detectable across an
arbitrary GitHub corpus).
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

TARGET_KINDS = {"StatefulSet"}


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
    control-flow directives). One synthetic doc per --- separated chunk.
    Scoped to StatefulSet only, same reasoning as SS3's fallback parser:
    restricting by kind here avoids inflating the workload-file
    denominator with objects that could never carry this smell."""
    docs = []
    chunks = re.split(r"^---\s*$", text, flags=re.MULTILINE)
    for chunk in chunks:
        kind_m = re.search(r"^kind:\s*['\"]?(\w+)['\"]?", chunk, re.MULTILINE)
        if not kind_m or kind_m.group(1) not in TARGET_KINDS:
            continue
        kind = kind_m.group(1)
        name_m = re.search(r"^\s{2,4}name:\s*['\"]?(.+?)['\"]?\s*$", chunk, re.MULTILINE)
        ns_m = re.search(r"^\s*namespace:\s*['\"]?([^'\"\s]+)", chunk, re.MULTILINE)

        # A present-and-non-empty volumeClaimTemplates block has at least
        # one "- " list item after the "volumeClaimTemplates:" line before
        # the next same-or-lower-indentation top-level spec key. Simple
        # heuristic: look for "volumeClaimTemplates:" followed (within a
        # reasonable window) by a "- " bullet, which indicates a non-empty
        # list rather than an empty list or the key being absent entirely.
        vct_m = re.search(r"volumeClaimTemplates:\s*\n(\s*)-\s", chunk)
        has_vct = bool(vct_m)

        # Some charts (e.g. Bitnami-style Airflow) gate the ENTIRE key
        # behind a Helm conditional and/or render its value via a fully
        # templated `include`/`tpl` call on the same line, e.g.:
        #   {{- if .Values.worker.extraVolumeClaimTemplates }}
        #   volumeClaimTemplates: {{- include "..." (dict ...) | nindent 8 }}
        #   {{- end }}
        # We cannot resolve whether that condition is true by default or
        # what the include renders to -- same situation as a templated
        # scalar value (SS2's replicas, SS3's ttlSecondsAfterFinished), so
        # per that same convention we treat it as indeterminate rather
        # than confidently "missing": don't guess, don't flag.
        vct_templated = bool(re.search(r"volumeClaimTemplates:\s*\{\{", chunk))

        docs.append({
            "kind": kind,
            "metadata": {
                "name": name_m.group(1) if name_m else "<unknown>",
                "namespace": ns_m.group(1) if ns_m else "default",
            },
            "_fallback": True,
            "_has_vct": has_vct,
            "_vct_templated": vct_templated,
        })
    return docs


def evaluate(doc: dict, file_path: str) -> Optional[Verdict]:
    kind = doc.get("kind")
    if kind not in TARGET_KINDS:
        return None

    if doc.get("_fallback"):
        meta = doc["metadata"]
        if doc.get("_vct_templated"):
            # Can't resolve a fully-templated/conditional volumeClaimTemplates
            # value -- don't guess, don't flag. Still counted as a workload
            # file (we did find a StatefulSet), just not asserted smelly.
            return Verdict(
                kind=kind, name=meta["name"], namespace=meta["namespace"],
                file=file_path, smelly=False,
                reasons=["volumeClaimTemplates present but templated/conditional -- indeterminate, not flagged"],
                used_fallback=True,
            )
        smelly = not doc["_has_vct"]
        reasons = []
        if smelly:
            reasons.append("no volumeClaimTemplates (fallback-parsed)")
        return Verdict(
            kind=kind, name=meta["name"], namespace=meta["namespace"],
            file=file_path, smelly=smelly, reasons=reasons, used_fallback=True,
        )

    spec = doc.get("spec") or {}
    meta = doc.get("metadata") or {}
    name = meta.get("name", "<unnamed>")
    namespace = meta.get("namespace", "default")

    vct = spec.get("volumeClaimTemplates")
    has_vct = isinstance(vct, list) and len(vct) > 0

    smelly = not has_vct
    reasons = []
    if smelly:
        reasons.append("no volumeClaimTemplates (missing or empty)")

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
        reason_str = "; ".join(v.reasons) if v.reasons else "has volumeClaimTemplates"
        print(f"[{tag}] {v.kind}/{v.name} (ns={v.namespace}) file={v.file}{fb} :: {reason_str}")

    n_workload = len(workload_files)
    n_smelly_files = len(smelly_files)
    print(f"\n{smelly_count} / {total_targets} StatefulSet objects flagged SS6-smelly "
          f"({fallback_count} parsed via fallback)")
    print(f"Files scanned: {total_files_scanned}")
    print(f"Workload files (>=1 StatefulSet): {n_workload}")
    print(f"Smelly files: {n_smelly_files}")
    if n_workload:
        print(f"Prevalence (workload-file basis): {n_smelly_files}/{n_workload} = "
              f"{100 * n_smelly_files / n_workload:.2f}%")
    if total_files_scanned:
        print(f"Prevalence (all-files basis): {n_smelly_files}/{total_files_scanned} = "
              f"{100 * n_smelly_files / total_files_scanned:.2f}%")


if __name__ == "__main__":
    main()
