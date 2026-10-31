import yaml
import sys
import os

def parse_cpu(value):
    value = str(value).strip()
    if value.endswith("m"):
        return int(value[:-1])
    return int(float(value) * 1000)

def check_container(container):
    name = container.get("name", "unknown")
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
        return name, "SMELL", "Case 4: requests absent — K8s sets requests=limits"

    cpu_request = requests.get("cpu")
    if not cpu_request:
        return name, "SMELL", "Case 4b: cpu request absent"

    limit_mc   = parse_cpu(cpu_limit)
    request_mc = parse_cpu(cpu_request)

    if limit_mc > 4 * request_mc:
        return name, "SMELL", \
            f"Case 5: over-provisioned — limit={cpu_limit} > 4x request={cpu_request}"

    return name, "NO SMELL", \
        f"right-sized — request={cpu_request} limit={cpu_limit}"

def process_file(path):
    print("=" * 65)
    print(f"FILE: {os.path.basename(path)}")
    print("=" * 65)

    with open(path) as f:
        docs = list(yaml.safe_load_all(f))

    any_smell = False
    for doc in docs:
        if not doc:
            continue
        kind = doc.get("kind", "")
        name = doc.get("metadata", {}).get("name", "unknown")

        if kind not in ["Pod","Job","Deployment","StatefulSet"]:
            print(f"  SKIP: kind={kind} (not a workload object)")
            continue

        print(f"\n  Workload : kind={kind}  name={name}")

        try:
            containers = doc["spec"]["template"]["spec"]["containers"]
        except KeyError:
            print("  WARNING  : no containers found")
            continue

        for container in containers:
            cname, result, reason = check_container(container)
            print(f"  Container: {cname}")
            print(f"  Result   : {result}")
            print(f"  Reason   : {reason}")
            if result == "SMELL":
                any_smell = True

    print()
    print("-" * 65)
    if any_smell:
        print("FINAL: SMELL DETECTED")
    else:
        print("FINAL: NO SMELL — file is clean")
    print("=" * 65)
    print()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 detect_ss1.py <file.yaml>")
        print("       python3 detect_ss1.py --all")
        sys.exit(1)

    if sys.argv[1] == "--all":
        folder = os.path.join(os.path.dirname(__file__), "yaml-files")
        files  = sorted(f for f in os.listdir(folder)
                        if f.endswith(".yaml"))
        if not files:
            print("No YAML files found in yaml-files/")
            sys.exit(1)
        for fname in files:
            process_file(os.path.join(folder, fname))
    else:
        process_file(sys.argv[1])
