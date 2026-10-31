import yaml
import sys
import os

def parse_cpu(value):
    value = str(value).strip()
    if value.endswith("m"):
        return int(value[:-1])
    return int(float(value) * 1000)

def check_container(container):
    name      = container.get("name", "unknown")
    resources = container.get("resources")

    if not resources:
        return name, "SMELL", "Case 1: no resource block declared"
    limits = resources.get("limits")
    if not limits:
        return name, "SMELL", "Case 2: no limits defined"
    cpu_limit = limits.get("cpu")
    if not cpu_limit:
        return name, "SMELL", "Case 3: no CPU limit"
    requests = resources.get("requests")
    if not requests:
        return name, "SMELL", \
            "Case 4: requests absent — K8s sets requests=limits"
    cpu_request = requests.get("cpu")
    if not cpu_request:
        return name, "SMELL", "Case 4b: cpu request absent"
    limit_mc   = parse_cpu(cpu_limit)
    request_mc = parse_cpu(cpu_request)
    if limit_mc > 4 * request_mc:
        return name, "SMELL", \
            f"Case 5: over-provisioned — " \
            f"limit={cpu_limit} > 4x request={cpu_request}"
    return name, "NO SMELL", \
        f"right-sized — request={cpu_request} limit={cpu_limit}"

def process_corpus(folder, verbose=False):
    files = sorted(
        f for f in os.listdir(folder)
        if f.endswith(".yaml") or f.endswith(".yml")
    )

    total         = 0
    smell_count   = 0
    clean_count   = 0
    skip_count    = 0
    error_count   = 0

    case_counts = {
        "Case 1": 0,
        "Case 2": 0,
        "Case 3": 0,
        "Case 4": 0,
        "Case 5": 0,
    }

    print("=" * 65)
    print("SS1 DETECTION --- CORPUS ANALYSIS")
    print(f"Folder : {folder}")
    print(f"Files  : {len(files)} YAML files found")
    print("=" * 65)

    for fname in files:
        path = os.path.join(folder, fname)
        total += 1

        try:
            with open(path) as f:
                docs = list(yaml.safe_load_all(f))
        except Exception:
            error_count += 1
            continue

        has_workload = False
        smell_found  = False
        file_reasons = []

        for doc in docs:
            if not doc or not isinstance(doc, dict):
                continue
            kind = doc.get("kind", "")
            if kind not in ["Pod","Job","Deployment","StatefulSet"]:
                continue
            has_workload = True
            try:
                if kind == "Pod":
                    containers = doc["spec"]["containers"]
                else:
                    containers = \
                        doc["spec"]["template"]["spec"]["containers"]
            except (KeyError, TypeError):
                continue
            for container in containers:
                cname, result, reason = check_container(container)
                if result == "SMELL":
                    smell_found = True
                    file_reasons.append(reason)
                    for key in case_counts:
                        if reason.startswith(key):
                            case_counts[key] += 1
                            break

        if not has_workload:
            skip_count += 1
        elif smell_found:
            smell_count += 1
            if verbose:
                for r in file_reasons:
                    print(f"  SMELL : {fname} --- {r}")
            else:
                print(f"  SMELL : {fname}")
        else:
            clean_count += 1

    workload_files = total - skip_count - error_count

    ## ── Two prevalence calculations ──────────────────────────────
    ## Method 1: denominator = ALL files (matches your paper)
    pct_all = round(smell_count / total * 100, 2) \
              if total > 0 else 0

    ## Method 2: denominator = workload files only
    pct_workload = round(smell_count / workload_files * 100, 2) \
                   if workload_files > 0 else 0

    print()
    print("=" * 65)
    print("SUMMARY")
    print("=" * 65)
    print(f"  Total files scanned        : {total}")
    print(f"  Parse errors               : {error_count}")
    print(f"  Skipped (no workload kind) : {skip_count}")
    print(f"  Files with workload kind   : {workload_files}")
    print(f"  Files with SS1 smell       : {smell_count}")
    print(f"  Files clean                : {clean_count}")
    print()
    print("  Prevalence (Method 1 — all files as denominator):")
    print(f"    {smell_count} / {total} = {pct_all}%")
    print(f"    ← this matches paper's methodology")
    print()
    print("  Prevalence (Method 2 — workload files only):")
    print(f"    {smell_count} / {workload_files} = {pct_workload}%")
    print(f"    ← stricter — only files that CAN have SS1")
    print()
    print("  Breakdown by case:")
    for case, count in case_counts.items():
        print(f"    {case} : {count} files")
    print("=" * 65)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 detect_ss1_corpus.py <folder>")
        print("  python3 detect_ss1_corpus.py <folder> --verbose")
        sys.exit(1)
    folder  = sys.argv[1]
    verbose = "--verbose" in sys.argv
    process_corpus(folder, verbose)
