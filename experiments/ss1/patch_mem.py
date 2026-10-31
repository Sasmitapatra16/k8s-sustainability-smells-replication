import os, shutil, subprocess, json, py_compile
if subprocess.run(["pgrep", "-f", "run_ss2.py"], capture_output=True).returncode == 0:
    raise SystemExit("run_ss2.py is still running - wait for ALL DONE in ~/rerun_meta/ss2_cg.log")
H = os.path.expanduser("~")
U = H + "/all7-experiment-v2/scripts/stage_unified.py"
shutil.copy(U, H + "/rerun_meta/prepatch_mem_stage_unified.py")
t = open(U).read()
if "cg_mem_peak_bytes" not in t:
    old = '    d["proc_cpu_secs"] = round(time.process_time(), 3)'
    assert t.count(old) == 1, "anchor not found"
    new = ('    for nm, fn in (("cg_mem_peak_bytes", "memory.peak"), ("cg_mem_current_bytes", "memory.current")):\n'
           '        try:\n'
           '            d[nm] = int(open("/sys/fs/cgroup/" + fn).read())\n'
           '        except Exception as e:\n'
           '            d[nm + "_err"] = str(e)\n') + old
    t = t.replace(old, new)
    open(U, "w").write(t)
py_compile.compile(U, doraise=True)
print("syntax OK")
def kc(*a, inp=None):
    return subprocess.run(["microk8s", "kubectl"] + list(a), input=inp, capture_output=True, text=True)
for ns in ["default", "etl-smelly", "etl-ingestion", "etl-cleaning", "etl-integration", "etl-transformation", "etl-loading"]:
    r = kc("get", "configmap", "stage-unified-scripts-v2", "-n", ns, "-o", "json")
    keys = list(json.loads(r.stdout)["data"].keys())
    assert len(keys) == 1, keys
    d = kc("create", "configmap", "stage-unified-scripts-v2", "-n", ns, "--from-file=" + keys[0] + "=" + U, "--dry-run=client", "-o", "yaml")
    a = kc("apply", "-f", "-", inp=d.stdout)
    chk = json.loads(kc("get", "configmap", "stage-unified-scripts-v2", "-n", ns, "-o", "json").stdout)["data"][keys[0]]
    print("OK" if "cg_mem_peak_bytes" in chk else "NOT UPDATED", ns, a.stdout.strip())
