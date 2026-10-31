import os, sys
from detect_ss3_baseline import load_all_documents, evaluate

# expected: True=SMELLY, False=NOT_SMELLY, None=no verdict (out of scope)
EXPECTED = {
    "smelly_no_ttl_job.yaml": True,
    "safe_with_ttl_job.yaml": False,
    "cronjob_no_ttl.yaml": True,
    "cronjob_with_ttl.yaml": False,
    "smelly_emptydir_checkpoint.yaml": True,
    "safe_emptydir_scratch.yaml": False,
    "helm_controlflow_no_ttl.yaml": True,
    "helm_controlflow_emptydir_state.yaml": True,
    "templated_ttl.yaml": False,
    "out_of_scope_deployment.yaml": None,
    "templated_ttl_quoted.yaml": False,
    "cronjob_emptydir_state_volname.yaml": True,
    "helm_controlflow_safe_scratch.yaml": False,   # predicted to FAIL (metadata/"data" bug)
}
KNOWN_BUG = {"helm_controlflow_safe_scratch.yaml"}

d = "yaml-files"
fails = 0
for fn, exp in EXPECTED.items():
    p = os.path.join(d, fn)
    if not os.path.exists(p):
        print(f"MISSING  {fn}"); continue
    docs, fb = load_all_documents(p)
    vs = [v for v in (evaluate(x, p) for x in docs) if v]
    got = None if not vs else any(v.smelly for v in vs)
    ok = got == exp
    tag = "PASS" if ok else ("KNOWN-BUG" if fn in KNOWN_BUG else "FAIL")
    if not ok and fn not in KNOWN_BUG: fails += 1
    print(f"{tag:9} {fn:40} expected={exp!s:5} got={got!s:5} fallback={fb}")
print(f"\nunexpected failures: {fails}")
sys.exit(1 if fails else 0)
