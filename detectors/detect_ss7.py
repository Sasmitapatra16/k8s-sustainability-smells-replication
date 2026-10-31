#!/usr/bin/env python3
"""
detect_ss7.py - SS7 (Monolithic Infrastructure) detector.

Scope: any file ("Any combination" of resources, per Table II) -- this is
a per-FILE detector, not per-object like SS1-SS6, since the smell is about
the file as a whole (too many unrelated resource types bundled together
with no modularization), not about any single object within it.

Rule: a file is SS7-SMELLY if it declares MORE THAN 4 DISTINCT resource
kinds (not distinct objects -- 5 ConfigMaps count as 1 distinct kind) AND
shows no sign of Helm or Kustomize usage:
  - "no Helm" = the raw file text contains no "{{" anywhere at all, even
    if only inside a quoted string value (some charts template only
    individual fields via quoted "{{ .Release.Name }}"-style strings and
    would still fully parse as valid YAML -- a naive "does it fail to
    parse" check would miss those, so this is a direct text scan, not a
    parse-failure check).
  - "no Kustomize" = the file itself does not declare `kind: Kustomization`
    (i.e. it isn't a Kustomize control file). We deliberately do NOT check
    for a sibling kustomization.yaml in the same directory, since that
    requires directory-level context outside any single file's own
    content -- out of scope for this file-level, self-contained detector,
    consistent with how SS1-SS6 were built.

Unlike SS1-SS6, there is no regex fallback parser here: since Helm usage
is itself the exclusion condition, a file containing "{{" is excluded by
definition regardless of whether it happens to still parse as valid YAML
or not -- there's nothing to "recover" via fallback, because a templated
file can never be SS7-smelly under this rule no matter what it contains.
Files that fail to parse AND contain no "{{" (genuine YAML syntax errors,
unrelated to templating) are skipped entirely -- we can't reliably count
distinct kinds without a working parse, and guessing via regex here would
risk miscounting the one number this smell is actually about.
"""
from __future__ import annotations
import argparse
import glob
import os
import sys
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass
class Verdict:
    file: str
    smelly: bool
    n_distinct_kinds: int
    kinds: list = field(default_factory=list)
    reasons: list = field(default_factory=list)


def evaluate_file(path: str) -> Optional[Verdict]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
    except Exception:
        return None

    if "{{" in text:
        return None  # uses Helm templating -- excluded by definition, not evaluated

    try:
        docs = [d for d in yaml.safe_load_all(text) if isinstance(d, dict)]
    except yaml.YAMLError:
        return None  # genuine parse error unrelated to templating -- can't reliably
        # count distinct kinds without a working parse; skip rather than guess

    if not docs:
        return None  # empty file / no resource documents -- nothing to evaluate

    kinds = [d.get("kind") for d in docs if d.get("kind")]
    if not kinds:
        return None  # no kind fields found at all -- not a resource file

    if "Kustomization" in kinds:
        return None  # file is itself a Kustomize control file -- excluded by definition

    distinct_kinds = sorted(set(kinds))
    n_distinct = len(distinct_kinds)
    smelly = n_distinct > 4

    reasons = []
    if smelly:
        reasons.append(f">4 distinct resource kinds ({n_distinct}: {', '.join(distinct_kinds)}), no Helm/Kustomize")

    return Verdict(
        file=path, smelly=smelly, n_distinct_kinds=n_distinct,
        kinds=distinct_kinds, reasons=reasons,
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

    total_files_scanned = 0
    evaluated_files = 0
    smelly_files = 0
    verdicts = []

    for path in iter_yaml_files(args.path):
        total_files_scanned += 1
        try:
            v = evaluate_file(path)
        except Exception as e:
            print(f"[ERROR] {path}: {e}", file=sys.stderr)
            continue
        if v is None:
            continue
        evaluated_files += 1
        if v.smelly:
            smelly_files += 1
        verdicts.append(v)

    for v in verdicts:
        tag = "SMELLY" if v.smelly else "NOT_SMELLY"
        reason_str = "; ".join(v.reasons) if v.reasons else \
            f"{v.n_distinct_kinds} distinct kind(s): {', '.join(v.kinds)}"
        print(f"[{tag}] file={v.file} :: {reason_str}")

    print(f"\n{smelly_files} / {evaluated_files} evaluated files flagged SS7-smelly")
    print(f"Files scanned: {total_files_scanned}")
    print(f"Evaluated files (no Helm/Kustomize, parsed cleanly): {evaluated_files}")
    print(f"Excluded (Helm/Kustomize/unparseable/empty): {total_files_scanned - evaluated_files}")
    print(f"Smelly files: {smelly_files}")
    if evaluated_files:
        print(f"Prevalence (evaluated-file basis): {smelly_files}/{evaluated_files} = "
              f"{100 * smelly_files / evaluated_files:.2f}%")
    if total_files_scanned:
        print(f"Prevalence (all-files basis): {smelly_files}/{total_files_scanned} = "
              f"{100 * smelly_files / total_files_scanned:.2f}%")


if __name__ == "__main__":
    main()
