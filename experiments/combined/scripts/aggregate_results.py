import json
import glob
import math
from collections import defaultdict

RESULTS_DIR = "./runs/replication_20261021T163112"
STAGES = ["ingestion", "integration", "cleaning", "transformation", "loading"]
METRICS = ["pod_active_secs", "cpu_work_secs", "s3_read_ms", "s3_write_ms"]

def load_jsonl(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                rows.append(json.loads(line))
    return rows

def mean(vals):
    return sum(vals) / len(vals)

def stdev(vals):
    if len(vals) < 2:
        return 0.0
    m = mean(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))

def ci95(vals):
    t_values = {2: 12.706, 3: 4.303, 4: 3.182, 5: 2.776, 6: 2.571}
    n = len(vals)
    if n < 2:
        return 0.0
    s = stdev(vals)
    t = t_values.get(n, 1.96)
    return t * (s / math.sqrt(n))

# ---- Load smelly ----
smelly_files = sorted(glob.glob(f"{RESULTS_DIR}/smelly-run*_metrics.jsonl"))
smelly_by_stage = defaultdict(lambda: defaultdict(list))
for path in smelly_files:
    for row in load_jsonl(path):
        stage = row["stage"]
        for m in METRICS:
            smelly_by_stage[stage][m].append(row[m])
        smelly_by_stage[stage]["rows"].append(row["rows"])

# ---- Load refactored ----
refactored_files = sorted(glob.glob(f"{RESULTS_DIR}/refactored-run*_metrics.jsonl"))
refactored_by_stage_max = defaultdict(lambda: defaultdict(list))
refactored_by_stage_sum = defaultdict(lambda: defaultdict(list))
refactored_rows_total = defaultdict(list)

for path in refactored_files:
    rows = load_jsonl(path)
    per_stage_chunks = defaultdict(list)
    for row in rows:
        per_stage_chunks[row["stage"]].append(row)
    for stage, chunk_rows in per_stage_chunks.items():
        for m in METRICS:
            vals = [r[m] for r in chunk_rows]
            refactored_by_stage_max[stage][m].append(max(vals))
            refactored_by_stage_sum[stage][m].append(sum(vals))
        refactored_rows_total[stage].append(sum(r["rows"] for r in chunk_rows))

# ---- Print summary tables ----
def print_table(title, data_dict, n_runs):
    print(f"\n{'='*100}")
    print(title)
    print(f"{'='*100}")
    header = f"{'Stage':<16}" + "".join(f"{m:<22}" for m in METRICS)
    print(header)
    for stage in STAGES:
        line = f"{stage:<16}"
        for m in METRICS:
            vals = data_dict[stage][m]
            mu = mean(vals)
            ci = ci95(vals)
            line += f"{mu:6.2f}±{ci:5.2f}        "
        print(line)
    print(f"(mean ± 95% CI, n={n_runs} runs)")

print_table("SMELLY — pod_active_secs / cpu_work_secs / s3_read_ms / s3_write_ms",
            smelly_by_stage, len(smelly_files))
print_table("REFACTORED — MAX across 3 chunks (wall-clock time for the stage)",
            refactored_by_stage_max, len(refactored_files))
print_table("REFACTORED — SUM across 3 chunks (total compute time across 3 pods)",
            refactored_by_stage_sum, len(refactored_files))

# ---- Row count sanity check ----
print(f"\n{'='*100}")
print("ROW COUNT SANITY CHECK")
print(f"{'='*100}")
for stage in STAGES:
    s_rows = set(smelly_by_stage[stage]["rows"])
    r_rows = set(refactored_rows_total[stage])
    print(f"{stage:<16} smelly rows={s_rows}  refactored rows(summed across chunks)={r_rows}")

# ---- Sustainability Impact Analysis ----
print(f"\n{'='*90}")
print("SUSTAINABILITY IMPACT ANALYSIS")
print(f"{'='*90}")

# SS1: Resource Reservation
print(f"\n--- SS1: Resource Reservation Impact ---")
print(f"{'Stage':<16} {'SmellyReserved':<16} {'RefactReserved':<16} {'Reduction%':<12}")
print(f"{'-'*60}")
smelly_reserved = 24
refact_reserved = 0.4 * 3
reduction = (1 - refact_reserved / smelly_reserved) * 100
for stage in STAGES:
    print(f"{stage:<16} {smelly_reserved:<16.1f} {refact_reserved:<16.1f} {reduction:<12.1f}")
print(f"{'Average':<16} {smelly_reserved:<16.1f} {refact_reserved:<16.1f} {reduction:<12.1f}")
print(f"\nSmelly reserved {smelly_reserved} cores (no limit)")
print(f"Refactored reserved {refact_reserved:.1f} cores (3 pods x 0.4 cores each)")
print(f"Resource reservation reduced by {reduction:.1f}%")
print(f"{smelly_reserved - refact_reserved:.1f} cores freed for other workloads per stage")

# SS2: Parallelism Impact
print(f"\n--- SS2: Parallelism Impact (wall-clock time per stage) ---")
print(f"{'Stage':<16} {'Smelly(s)':<12} {'Refact-MAX(s)':<16} {'Refact-SUM(s)':<16} {'Speedup':<10}")
print(f"{'-'*70}")
for stage in STAGES:
    s_time = mean(smelly_by_stage[stage]["pod_active_secs"])
    r_max = mean(refactored_by_stage_max[stage]["pod_active_secs"])
    r_sum = mean(refactored_by_stage_sum[stage]["pod_active_secs"])
    speedup = s_time / r_max
    print(f"{stage:<16} {s_time:<12.2f} {r_max:<16.2f} {r_sum:<16.2f} {speedup:<10.2f}")
print(f"\nMAX  = wall-clock time the user waits for one stage to complete")
print(f"SUM  = total compute time across all 3 parallel pods")
print(f"Note: Speedup < 1.0 is expected — each refactored pod is resource-capped")
print(f"      at 0.4 cores (SS1 fix), making it slower per pod but more controlled.")
print(f"      The benefit of SS2 is predictable distributed work, not raw speed.")

# SS3/SS4/SS6/SS7
print(f"\n--- SS3 / SS4 / SS6 / SS7: Configuration Sustainability ---")
print(f"{'Smell':<8} {'Description':<40} {'Smelly':<15} {'Refactored':<15}")
print(f"{'-'*80}")
print(f"{'SS3':<8} {'Job lifecycle cleanup (TTL)':<40} {'ABSENT':<15} {'120 seconds':<15}")
print(f"{'SS4':<8} {'Logging verbosity':<40} {'DEBUG/true':<15} {'INFO/false':<15}")
print(f"{'SS6':<8} {'Pipeline state tracking':<40} {'DISABLED':<15} {'ENABLED':<15}")
print(f"{'SS7':<8} {'Stage namespace isolation':<40} {'1 namespace':<15} {'5 namespaces':<15}")
print(f"\nThese smells affect operational sustainability:")
print(f"SS3 — completed jobs linger forever without TTL, wasting cluster state")
print(f"SS4 — DEBUG logging generates unnecessary I/O and storage overhead")
print(f"SS6 — no state tracking means no visibility if pipeline fails mid-run")
print(f"SS7 — all stages in one namespace means no isolation or independent deployment")
print(f"{'='*90}")
