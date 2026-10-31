#!/usr/bin/env python3
"""
detect_ss2.py

SS2 - Lack of Auto-Scaling detector.

Detection rule (locked, matches Kosbar & Hamdaqa's Terraform analog and the
RAC-VII committee's stated expectation of "replica set I gave 5"):

    A Deployment or StatefulSet is SS2-smelly iff:
        1. spec.replicas is present, AND
        2. spec.replicas is a hardcoded integer literal (not a Helm/Kustomize
           template placeholder), AND
        3. that integer is > 1, AND
        4. no HorizontalPodAutoscaler in the same scan scope has
           scaleTargetRef.kind == this object's kind AND
           scaleTargetRef.name == this object's metadata.name

Kind scope: Deployment, StatefulSet only. (Job/CronJob are explicitly
OUT of scope for SS2 -- see SS2_Scope_QA_Addendum.docx. HPA's
scaleTargetRef only supports Deployment/ReplicaSet/StatefulSet at the
Kubernetes API level, so the canonical SS2 mechanism cannot exist on
Job-based objects. Do not resurrect the abandoned Job-parallelism version
of this smell here.)

Because an HPA can live in a *different* file than the workload it
targets, SS2 detection is inherently a two-pass, corpus/app-scoped
operation:
    Pass 1: build an index of every HPA's (kind, name) target across the
            scan scope.
    Pass 2: evaluate every Deployment/StatefulSet against that index.

This module exposes both a single-object classifier (`classify_workload`)
for unit testing / fixtures, and a scope-level driver (`scan_paths`) for
scanning a directory (e.g. one "application" = one git repo / one Helm
chart) or a single multi-document YAML file.

detect_ss2_corpus.py builds on `build_hpa_index` and `classify_workload`
to run this over the full 3,084-file prevalence corpus.
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

import yaml

TARGET_KINDS = {"Deployment", "StatefulSet"}

# Matches Helm ({{ ... }}), Kustomize/Skaffold ($VAR, ${VAR}), and other
# templating placeholders that show up as strings in raw YAML instead of
# integer literals. If replicas is templated, the author has *deferred*
# the scaling decision to an external mechanism (values file, overlay,
# CI variable) rather than hardcoding it -- this is not the SS2 pattern,
# even though we can't statically know the rendered value.
_TEMPLATE_PATTERN = re.compile(r"\{\{.*\}\}|\$\{.*\}|^\$[A-Za-z_]")


@dataclass
class Ss2Finding:
    file: str
    kind: str
    name: str
    namespace: str
    replicas: Optional[int]
    verdict: str  # "SMELLY" | "NOT_SMELLY" | "NOT_APPLICABLE"
    reason: str
    matched_hpa: Optional[str] = None


@dataclass
class HpaIndex:
    # (kind, name, namespace) -> source file of the HPA that targets it
    targets: dict = field(default_factory=dict)

    def has_target(self, kind: str, name: str, namespace: str) -> Optional[str]:
        return self.targets.get((kind, name, namespace))


_KIND_RE = re.compile(r"^\s*kind:\s*[\"']?(\w+)[\"']?\s*$", re.MULTILINE)
_NAME_RE = re.compile(r"^\s*name:\s*(.+?)\s*$", re.MULTILINE)
_NAMESPACE_RE = re.compile(r"^\s*namespace:\s*(.+?)\s*$", re.MULTILINE)
_REPLICAS_RE = re.compile(r"^\s*replicas:\s*(.+?)\s*$", re.MULTILINE)
_SCALETARGET_KIND_RE = re.compile(r"scaleTargetRef:.*?^\s*kind:\s*(.+?)\s*$", re.MULTILINE | re.DOTALL)
_SCALETARGET_NAME_RE = re.compile(r"scaleTargetRef:.*?^\s*name:\s*(.+?)\s*$", re.MULTILINE | re.DOTALL)

FALLBACK_PARSE_COUNT = {"used": 0, "recovered_workloads": 0}


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1]
    return s


def _regex_fallback_parse(raw: str, path: str) -> list[dict]:
    """Last-resort extraction for files that are not valid YAML on their
    own -- almost always un-rendered Helm templates using control-flow
    directives ({{- if }}, {{- include }}, {{- range }}, ...) that have
    no YAML equivalent and cannot be fixed by quoting a single field.

    This is a deliberately narrow, approximate parser: it pulls out just
    the handful of fields SS2 detection needs (kind, name, namespace,
    replicas, and for HPAs: scaleTargetRef.kind/name) via regex over the
    raw text, split into '---'-separated chunks. It does NOT attempt to
    validate the rest of the document. Every use of this path is counted
    in FALLBACK_PARSE_COUNT so the corpus-scan report can disclose how
    many objects were classified this way, rather than silently blending
    them in with cleanly-parsed results.

    Field regexes deliberately match ANY line content after 'name:' /
    'namespace:' / 'replicas:' (not a narrow character allow-list) --
    an earlier version restricted the value to a small charset and
    silently failed to match lines containing embedded quotes (e.g.
    name: {{ template "some.helper" . }}-suffix), which caused the
    search to skip past the real metadata.name line and latch onto an
    unrelated, coincidentally-simpler 'name:' line elsewhere in the
    file (e.g. a container port's 'name: http'). Matching broadly and
    then stripping surrounding quotes in code (via _strip_quotes) is
    more robust than trying to encode "valid YAML scalar" in a regex
    character class.
    """
    docs: list[dict] = []
    for chunk in raw.split("\n---"):
        kind_m = _KIND_RE.search(chunk)
        if not kind_m:
            continue
        kind = kind_m.group(1)

        if kind == "HorizontalPodAutoscaler":
            tk_m = _SCALETARGET_KIND_RE.search(chunk)
            tn_m = _SCALETARGET_NAME_RE.search(chunk)
            ns_m = _NAMESPACE_RE.search(chunk)
            if tk_m and tn_m:
                docs.append({
                    "kind": "HorizontalPodAutoscaler",
                    "metadata": {"namespace": _strip_quotes(ns_m.group(1)) if ns_m else "default"},
                    "spec": {"scaleTargetRef": {
                        "kind": _strip_quotes(tk_m.group(1)),
                        "name": _strip_quotes(tn_m.group(1)),
                    }},
                })
            continue

        if kind not in TARGET_KINDS:
            continue

        name_m = _NAME_RE.search(chunk)
        ns_m = _NAMESPACE_RE.search(chunk)
        repl_m = _REPLICAS_RE.search(chunk)

        FALLBACK_PARSE_COUNT["recovered_workloads"] += 1
        docs.append({
            "kind": kind,
            "metadata": {
                "name": _strip_quotes(name_m.group(1)) if name_m else "<unnamed-fallback>",
                "namespace": _strip_quotes(ns_m.group(1)) if ns_m else "default",
            },
            "spec": {"replicas": _strip_quotes(repl_m.group(1))} if repl_m else {},
            "_fallback_parsed": True,
            "_fallback_source": path,
        })
    return docs


def load_all_documents(path: str) -> list[dict]:
    """Parse a (possibly multi-document, possibly Helm-templated) YAML file.

    Full YAML parsing is tried first. Documents that fail (typically
    un-rendered Helm templates with control-flow directives like
    '{{- if }}' or '{{- include }}', which have no YAML equivalent) fall
    back to _regex_fallback_parse rather than being silently dropped --
    dropping them would deflate both the SMELLY numerator and the
    workload-file denominator in a corpus-scan, in ways that don't cancel
    out cleanly.
    """
    docs: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
    except OSError:
        return docs

    try:
        for doc in yaml.safe_load_all(raw):
            if isinstance(doc, dict):
                docs.append(doc)
        return docs
    except yaml.YAMLError:
        pass

    # Full multi-doc parse failed. Try per-chunk parsing so one malformed
    # doc in a multi-doc file doesn't drop the rest.
    any_chunk_parsed = False
    for chunk in raw.split("\n---"):
        try:
            doc = yaml.safe_load(chunk)
            if isinstance(doc, dict):
                docs.append(doc)
                any_chunk_parsed = True
        except yaml.YAMLError:
            continue

    # If nothing at all parsed as valid YAML (the common Helm-template
    # case), fall back to regex extraction rather than returning nothing.
    if not docs:
        FALLBACK_PARSE_COUNT["used"] += 1
        docs = _regex_fallback_parse(raw, path)

    return docs


def build_hpa_index(paths: Iterable[str], doc_cache: Optional[dict] = None) -> HpaIndex:
    """Pass 1: index every HorizontalPodAutoscaler's scaleTargetRef.

    If doc_cache is provided, parsed documents are stored there keyed by
    path so a subsequent classification pass over the same paths doesn't
    re-read and re-parse (and, for Helm-template files, re-run the
    fallback parser on) each file a second time.
    """
    index = HpaIndex()
    for path in paths:
        docs = load_all_documents(path)
        if doc_cache is not None:
            doc_cache[path] = docs
        for doc in docs:
            if doc.get("kind") != "HorizontalPodAutoscaler":
                continue
            spec = doc.get("spec") or {}
            target = spec.get("scaleTargetRef") or {}
            target_kind = target.get("kind")
            target_name = target.get("name")
            if not target_kind or not target_name:
                continue
            namespace = (doc.get("metadata") or {}).get("namespace", "default")
            index.targets[(target_kind, target_name, namespace)] = path
    return index


def _is_templated(value: Any) -> bool:
    return isinstance(value, str) and bool(_TEMPLATE_PATTERN.search(value))


def classify_workload(doc: dict, hpa_index: HpaIndex, source_file: str = "") -> Optional[Ss2Finding]:
    """Case 1-5 nested classifier for a single parsed YAML document.

    Mirrors the SS1 script's nested if/else structure and case ordering
    so the two detectors read the same way in the paper's algorithm boxes.
    """
    kind = doc.get("kind")

    # Case 1: out of scope kind (includes Job/CronJob -- see module docstring)
    if kind not in TARGET_KINDS:
        return None

    metadata = doc.get("metadata") or {}
    name = metadata.get("name", "<unnamed>")
    namespace = metadata.get("namespace", "default")
    spec = doc.get("spec") or {}

    # Case 2: no replicas field at all -> defaults to 1 at the API level,
    # so there is no *hardcoded* scaling decision to flag.
    if "replicas" not in spec:
        return Ss2Finding(
            file=source_file, kind=kind, name=name, namespace=namespace,
            replicas=None, verdict="NOT_SMELLY",
            reason="no replicas field present (defaults to 1)",
        )

    replicas_raw = spec.get("replicas")

    # Case 3: replicas present but templated (Helm/Kustomize placeholder) ->
    # the value is externally parameterized, not hardcoded.
    if _is_templated(replicas_raw):
        return Ss2Finding(
            file=source_file, kind=kind, name=name, namespace=namespace,
            replicas=None, verdict="NOT_SMELLY",
            reason=f"replicas is templated, not hardcoded ({replicas_raw!r})",
        )

    try:
        replicas = int(replicas_raw)
    except (TypeError, ValueError):
        # Unparseable, non-templated value (rare malformed YAML) -- treat
        # as not applicable rather than guessing.
        return Ss2Finding(
            file=source_file, kind=kind, name=name, namespace=namespace,
            replicas=None, verdict="NOT_APPLICABLE",
            reason=f"replicas value not a parseable integer ({replicas_raw!r})",
        )

    # Case 4: hardcoded replicas <= 1 -> no scaling decision to speak of.
    if replicas <= 1:
        return Ss2Finding(
            file=source_file, kind=kind, name=name, namespace=namespace,
            replicas=replicas, verdict="NOT_SMELLY",
            reason=f"hardcoded replicas={replicas} (<=1, not a scaling decision)",
        )

    # Case 5: hardcoded replicas > 1 -- the deciding case: is there a
    # matching HPA in scope?
    matched_file = hpa_index.has_target(kind, name, namespace)
    if matched_file:
        return Ss2Finding(
            file=source_file, kind=kind, name=name, namespace=namespace,
            replicas=replicas, verdict="NOT_SMELLY",
            reason=f"hardcoded replicas={replicas} but HPA targets this object",
            matched_hpa=matched_file,
        )

    return Ss2Finding(
        file=source_file, kind=kind, name=name, namespace=namespace,
        replicas=replicas, verdict="SMELLY",
        reason=f"hardcoded replicas={replicas} (>1) with no matching HPA in scope",
    )


def scan_paths(paths: list[str]) -> list[Ss2Finding]:
    """Scope-level driver: two-pass scan over an explicit list of files
    (e.g. one application / one directory / one multi-doc file)."""
    doc_cache: dict = {}
    hpa_index = build_hpa_index(paths, doc_cache=doc_cache)
    findings: list[Ss2Finding] = []
    for path in paths:
        for doc in doc_cache.get(path, []):
            finding = classify_workload(doc, hpa_index, source_file=path)
            if finding is not None:
                findings.append(finding)
    return findings


def expand_input(input_path: str) -> list[str]:
    if os.path.isdir(input_path):
        files = []
        for ext in ("*.yaml", "*.yml"):
            files.extend(glob.glob(os.path.join(input_path, "**", ext), recursive=True))
        return sorted(files)
    return [input_path]


def main() -> int:
    parser = argparse.ArgumentParser(description="SS2 (Lack of Auto-Scaling) detector")
    parser.add_argument("input", help="YAML file or directory to scan")
    parser.add_argument(
        "--only-smelly", action="store_true",
        help="print only SMELLY findings",
    )
    args = parser.parse_args()

    paths = expand_input(args.input)
    if not paths:
        print(f"No YAML files found under {args.input}", file=sys.stderr)
        return 1

    findings = scan_paths(paths)
    shown = [f for f in findings if (f.verdict == "SMELLY" or not args.only_smelly)]

    for f in shown:
        line = f"[{f.verdict}] {f.kind}/{f.name} (ns={f.namespace}) file={f.file} replicas={f.replicas} :: {f.reason}"
        print(line)

    total_target = sum(1 for f in findings)
    smelly = sum(1 for f in findings if f.verdict == "SMELLY")
    print(f"\n{smelly} / {total_target} Deployment/StatefulSet objects flagged SS2-smelly", file=sys.stderr)
    if FALLBACK_PARSE_COUNT["used"]:
        print(
            f"[fallback parser] used on {FALLBACK_PARSE_COUNT['used']} file(s) that failed full YAML "
            f"parsing (likely un-rendered Helm templates); recovered "
            f"{FALLBACK_PARSE_COUNT['recovered_workloads']} Deployment/StatefulSet object(s) via regex "
            f"extraction instead of dropping them silently.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
