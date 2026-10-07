#!/usr/bin/env python3
"""#80 review findings — a sidecar row is a CLAIM; /proc is the COURT (standalone).

Verdict=changes review of 94f9654 (issue #80 review comment): the #61c survivor
registry persisted {pid, token} rows and the runner SIGNALLED them without
proving the CURRENT occupant is the registrant's family — so after PID reuse
both the boot sweep and ordinary judgment could kill an unrelated process.
Plus: a stuck boot proof was only logged (main() scheduled anyway), and
_aux_run's timeout kill covered only the aux PARENT (a pipe-holding grandchild
blocked communicate() past the deadline).

 R1  identity before attribution/signal: a registry row counts only while
     /proc still vouches for the occupant — the row pins the registrant's
     kernel start tick; the CURRENT /proc boottime must equal-or-postdate it.
     An unrelated (token-free, foreign-booted) process can never be attributed
     or killed through a stale-shaped row; legacy two-field rows load safely
     without any blind kill. Boot sweep AND judgment honor the gate, and the
     healthy paths still sweep/attribute (the gate never launders a real
     escapee into 'clean').
 R2  stuck/unknown boot proof BLOCKS admission before any Popen: typed
     WORKFLOW_FAILED + runner_exit reason, the predecessor stays, no child
     ever spawns (the reviewer's fault-injection shape at the admission edge).
 R3  _aux_run timeout stays BOUNDED and kills the WHOLE aux tree: a
     pipe-holding grandchild can never hold communicate() past the kill
     grace, and no aux descendant survives the timeout.

Run: env -u WF_RUNS_ROOT -u HERMES_HOME PYTHONPATH=/opt/hermes \
       /opt/hermes/.venv/bin/python tests/test_proctree_identity_80.py
"""
import json, os, shutil, subprocess, sys, threading, time, types
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
BUILD = Path(os.environ.get("WF_TEST_BUILD") or HERE.parent)
sys.path.insert(0, str(BUILD))
from wf_test_markers import read_pid_marker
import wf  # noqa: E402

ok = True
def check(label, cond, detail=""):
    global ok
    print(("PASS " if cond else "FAIL " + label + (f"  {detail}" if detail else "")))
    if not cond:
        ok = False

HOME = BUILD / "home-pt80id"
ALL = []                       # fixture pids; the finally-block proves them dead

def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        st = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return False
    return not st.startswith("Z")

def boottime(pid):
    """Kernel start tick (field 22 of /proc/pid/stat — rest[19] after the comm)."""
    for _ in range(200):
        try:
            rest = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            return int(rest[19])
        except (OSError, IndexError, ValueError):
            time.sleep(0.02)
    return None

def spawn_script(src, tag):
    f = HOME / f"fx_{tag}.py"; f.write_text(src)
    p = subprocess.Popen([sys.executable, str(f)], start_new_session=True)
    ALL.append(p.pid)
    return p

def wait_file(p, t=10):
    t0 = time.time()
    while not p.exists() and time.time() - t0 < t:
        time.sleep(0.02)
    return p.exists()

def kill_all():
    for p in ALL:
        if alive(p):
            try:
                os.kill(p, 9)
            except ProcessLookupError:
                pass
    t = time.time() + 4
    while any(alive(p) for p in ALL) and time.time() < t:
        time.sleep(0.02)

shutil.rmtree(HOME, ignore_errors=True)
HOME.mkdir(parents=True)

try:
    # ============== R1a: sidecar trust validates the occupant ==================
    run = HOME / "run-r1a"; run.mkdir()
    FUTURE = 10 ** 15
    (run / wf.SIDECAR_NAME).write_text("\n".join([
        json.dumps({"pid": 4194303, "token": "t1"}),                     # legacy 2-field: UNTRUSTED
        json.dumps({"pid": 4194302, "token": "t1", "boottime": "x"}),    # garbage: UNTRUSTED
        json.dumps({"pid": 4194301, "token": "t1", "boottime": FUTURE}), # future-booted: IMPOSSIBLE
        json.dumps({"pid": 4194300, "token": "t1", "boottime": 7}),      # /proc shows boot 9
        json.dumps({"pid": 4194304, "token": "t1", "boottime": 5}),      # the honest row
        "torn line {",                                                    # torn: skipped
    ]) + "\n")
    snap = {p: (1, p) for p in (4194303, 4194302, 4194301, 4194300, 4194304)}
    bts = {4194303: 9, 4194302: 9, 4194301: 9, 4194300: 9, 4194304: 5}
    with patch.object(wf, "_proc_snapshot", lambda: dict(snap)), \
         patch.object(wf, "_proc_boottime", lambda pid: bts.get(pid)):
        trusted, v = wf._sidecar_trusted_pids(run, ["t1"])
    check("R1a a row is trusted ONLY while /proc vouches for the occupant (legacy/garbage/mismatch: never)",
          trusted == {4194304} and v == "ok",
          json.dumps({"trusted": sorted(trusted), "verdict": v}))

    # an alive registered pid whose occupant boottime CANNOT be read => unknown
    (run / wf.SIDECAR_NAME).write_text(json.dumps({"pid": 4194303, "token": "t1", "boottime": 9}) + "\n")
    with patch.object(wf, "_proc_snapshot", lambda: dict(snap)), \
         patch.object(wf, "_proc_boottime", lambda pid: None):
        trusted, v = wf._sidecar_trusted_pids(run, ["t1"])
    check("R1a an unreadable occupant is UNKNOWN, never silently trusted or dropped-safe",
          trusted == set() and v == "unknown",
          json.dumps({"trusted": sorted(trusted), "verdict": v}))

    # ============== R1b: boot sweep cannot kill an unrelated occupant =========
    victim = spawn_script("import time; time.sleep(120)\n", "r1b-v")
    bt_v = boottime(victim.pid)
    run_b = HOME / "run-r1b"; run_b.mkdir()
    stale = {"pid": victim.pid, "token": "t-dead", "boottime": max(0, bt_v - 5000)}
    (run_b / wf.SIDECAR_NAME).write_text(json.dumps(stale) + "\n")
    meta_b = {"_run": run_b, "proctree_kill_proof_s": 1.0, "proctree_hold_s": 0}
    sweep1 = wf._boot_sweep(meta_b)
    time.sleep(0.3)
    check("R1b boot sweep on a stale-shaped row pointing at an unrelated process: fixture SURVIVES",
          alive(victim.pid) and sweep1.get("proof") in ("dead", "clean")
          and victim.pid not in (sweep1.get("pids") or []),
          json.dumps({"victim_alive": alive(victim.pid), "sweep": sweep1}))
    # legacy two-field row shape too: loads, never blind-kills
    (run_b / wf.SIDECAR_NAME).write_text(json.dumps({"pid": victim.pid, "token": "t-dead"}) + "\n")
    sweep1b = wf._boot_sweep(meta_b)
    time.sleep(0.2)
    check("R1b a legacy two-field row loads safely WITHOUT any kill",
          alive(victim.pid) and sweep1b.get("proof") in ("dead", "clean")
          and victim.pid not in (sweep1b.get("pids") or []),
          json.dumps({"victim_alive": alive(victim.pid), "sweep": sweep1b}))
    # healthy path still sweeps: pin the TRUE boottime -> registrant reaped
    victim2 = spawn_script("import time; time.sleep(120)\n", "r1b-v2")
    bt_v2 = boottime(victim2.pid)
    (run_b / wf.SIDECAR_NAME).write_text(
        json.dumps({"pid": victim2.pid, "token": "t-live", "boottime": bt_v2}) + "\n")
    sweep2 = wf._boot_sweep(meta_b)
    t = time.time() + 4
    while alive(victim2.pid) and time.time() < t:
        time.sleep(0.02)
    check("R1b the boot sweep STILL reaps a boottime-verified registrant",
          (not alive(victim2.pid)) and victim2.pid in (sweep2.get("pids") or [])
          and sweep2.get("proof") == "dead",
          json.dumps({"sweep": sweep2, "still_alive": alive(victim2.pid)}))

    # ============== R1c: judgment cannot kill an unrelated counterpart ========
    victim3 = spawn_script("import time; time.sleep(120)\n", "r1c-v")
    bt_v3 = boottime(victim3.pid)
    token = "n:0:0:abcd1234"
    run_c = HOME / "run-r1c"; run_c.mkdir()
    sidecar = run_c / wf.SIDECAR_NAME
    (HOME / "r1c_kid_main.py").write_text(
        "import os, sys, time\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "import wf\n"
        "from pathlib import Path\n"
        f"Path(r'{HOME}/r1c_kid.txt').write_text(str(os.getpid()))\n"
        "p = os.fork()\n"
        "if p == 0:\n"
        "    os.setsid()\n"
        "    q = os.fork()\n"
        "    if q == 0:\n"
        "        wf._register_survivor(sys.argv[2], sys.argv[3], os.getpid())\n"
        "        time.sleep(300); os._exit(0)\n"
        "    os._exit(0)\n"                       # intermediary exits: the gc orphanes
        "time.sleep(2)\n")
    kid = subprocess.Popen([sys.executable, str(HOME / "r1c_kid_main.py"),
                            str(BUILD), str(sidecar), token], start_new_session=True)
    ALL.append(kid.pid)
    kid_pids = read_pid_marker(HOME / "r1c_kid.txt")
    assert kid_pids and kid_pids[0] > 0, "kid fixture never came up"
    kid_pid = kid_pids[0]
    ALL.append(kid_pid)
    t = time.time() + 10
    while (not sidecar.exists() or sidecar.read_text().strip() == "") and time.time() - t < 10:
        time.sleep(0.05)
    rows_now = [json.loads(l) for l in sidecar.read_text().splitlines() if l.strip()]
    check("R1c the child-side registration PINS the registrant's boottime",
          len(rows_now) == 1 and isinstance(rows_now[0].get("boottime"), int),
          json.dumps(rows_now))
    # the reviewer's poison: a stale-shaped row for the UNRELATED victim under
    # the node's own token (valid shape, token matches, occupant does not).
    with sidecar.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"pid": victim3.pid, "token": token,
                            "boottime": max(0, bt_v3 - 5000)}) + "\n")
    meta_c = {"_run": run_c, "proctree_hold_s": 0.2, "proctree_kill_proof_s": 2.0,
              "_spawn_tokens": {"n:None": [token]}, "_procs": {},
              "_procs_lock": threading.Lock()}
    verdict = wf._account_tree(meta_c, {"id": "n"}, None, 0, kid_pid, set(), True)
    time.sleep(0.2)
    check("R1c judgment over a stale-shaped row: the unrelated counterpart SURVIVES",
          alive(victim3.pid),
          json.dumps({"victim_alive": alive(victim3.pid),
                      "verdict": verdict[0] if verdict else None}))
    check("R1c the boottime-verified escapee IS still accounted, tree proven dead",
          verdict is not None and verdict[0] == "partial"
          and verdict[1].get("tree_proof") == "dead",
          json.dumps({"kind": verdict[0] if verdict else None,
                      "proof": (verdict[1] or {}).get("tree_proof") if verdict else None}))

    # ============== R2: stuck boot proof BLOCKS admission before Popen ========
    real_popen = subprocess.Popen
    popens = []
    import wfcommon as _wfc_r2
    _OBSERVE_ONLY_SRC = {getattr(_wfc_r2, "LOCK_PROBE_CHILD", object()),
                        getattr(_wfc_r2, "_LOCK_PROBE_BATCH_CHILD", object())}
    def spy_popen(*a, **k):
        argv = a[0] if a else k.get("args")
        # #44: the read-path lock probe (find_run -> run_state ->
        # runner_lock_state -> _lock_probe_child) spawns ONE short-lived
        # `sys.executable -c <probe src> <path>` child per observation. That
        # is a read-only flock probe, not an admission spawn — the R2 law is
        # about NODE/runner children. Count everything else.
        if (isinstance(argv, (list, tuple)) and len(argv) >= 3
                and argv[1:2] == ["-c"] and argv[2] in _OBSERVE_ONLY_SRC):
            return real_popen(*a, **k)
        popens.append(argv)
        return real_popen(*a, **k)
    src = (BUILD / "wf.py").read_text()
    mod = types.ModuleType("wf_r2_under_test")
    mod.__dict__["__file__"] = str(BUILD / "wf.py")
    exec(compile(src, str(BUILD / "wf.py"), "exec"), mod.__dict__)
    runs2 = HOME / "r2-runs"; runs2.mkdir()
    run_d = runs2 / "r2"; (run_d / "nodes").mkdir(parents=True); (run_d / "gates").mkdir()
    (run_d / "graph.json").write_text(json.dumps({"name": "r2", "nodes": [
        {"id": "solo", "type": "agent", "goal": "x"}]}))
    (run_d / "run.json").write_text(json.dumps(
        {"hermes_bin": str(HERE / "fake"), "node_timeout": 20}))
    env_home = HOME / "r2-home"; env_home.mkdir()
    ghost = 4242
    (run_d / wf.SIDECAR_NAME).write_text(
        json.dumps({"pid": ghost, "token": "t-ghost", "boottime": 11}) + "\n")
    old_env = dict(os.environ)
    os.environ.update({"HERMES_HOME": str(env_home), "WF_RUNS_ROOT": str(runs2)})
    try:
        # fault ONLY the pool-killer (the reviewer's shape): boot_sweep itself
        # runs for real over the ghost row (snapshot says the ghost is in the
        # table, boottime matches the pinned identity); only the kill proof
        # is rigged stuck.
        with patch.object(subprocess, "Popen", spy_popen), \
             patch.object(mod, "_proc_snapshot", lambda: {ghost: (1, ghost)}), \
             patch.object(mod, "_proc_boottime", lambda pid: 11 if pid == ghost else None), \
             patch.object(mod, "_kill_pool",
                          lambda pids, h, p: ("stuck", sorted(pids))):
            try:
                mod.main("r2")
            except SystemExit:
                pass
    finally:
        os.environ.clear(); os.environ.update(old_env)
    rec_path = run_d / "runner_exit.json"
    rec = json.loads(rec_path.read_text()) if rec_path.exists() else {}
    check("R2 stuck boot proof BLOCKS BEFORE Popen — no child ever spawns",
          not popens, json.dumps({"popen_calls": len(popens)}))
    check("R2 the blocked admission is typed and retains the stuck-tree evidence",
          str(rec.get("reason", "")).startswith("blocked: proctree boot sweep")
          and str(ghost) in json.dumps(rec),
          json.dumps({"reason": rec.get("reason"), "detail": rec.get("detail")}))
    meta_d = {"_run": run_d, "proctree_kill_proof_s": 0.5, "proctree_hold_s": 0}
    with patch.object(mod, "_proc_snapshot", lambda: {ghost: (1, ghost)}), \
         patch.object(mod, "_proc_boottime", lambda pid: 11 if pid == ghost else None), \
         patch.object(mod, "_kill_pool", lambda pids, h, p: ("stuck", sorted(pids))):
        sweep_d = mod._boot_sweep(meta_d)
    check("R2 _boot_sweep REPORTS the stuck proof typed (proof=stuck with stuck pids)",
          isinstance(sweep_d, dict) and sweep_d.get("proof") == "stuck"
          and ghost in (sweep_d.get("stuck") or []),
          json.dumps(sweep_d))

    # ============== R3: _aux_run timeout bounded over a pipe holder ============
    holder = HOME / "r3_holder.py"
    holder.write_text(
        "import os, sys, time\n"
        "if len(sys.argv) > 1 and sys.argv[1] == 'parent':\n"
        "    if os.fork() == 0:\n"
        "        os.execv(sys.executable, [sys.executable, __file__])   # inherits the pipes\n"
        "    time.sleep(20)\n"
        "else:\n"
        "    time.sleep(20)     # pipe-holding grandchild\n")
    t0 = time.monotonic()
    r = wf._aux_run([sys.executable, str(holder), "parent"], timeout=0.4)
    elapsed = time.monotonic() - t0
    check("R3 aux timeout is BOUNDED (a pipe-holding grandchild cannot hold communicate past the kill grace)",
          elapsed < 4.0, f"elapsed={elapsed:.2f}s rc={getattr(r, 'returncode', None)}")
    snap = wf._proc_snapshot() or {}
    strays = []
    for p in snap:
        try:
            cl = Path(f"/proc/{p}/cmdline").read_bytes()
        except OSError:
            continue
        if b"r3_holder" in cl:
            strays.append(p)
    check("R3 aux timeout kills the WHOLE aux tree (no pipe-holding descendant survives)",
          not strays, json.dumps({"strays": strays}))

    # ============== R3b: a normal aux run is unchanged ========================
    r2 = wf._aux_run([sys.executable, "-c", "print('hi')"], timeout=10)
    check("R3 a healthy aux run is unchanged (rc 0, stdout collected)",
          r2.returncode == 0 and r2.stdout.strip() == "hi",
          json.dumps({"rc": r2.returncode, "out": r2.stdout[:40]}))
finally:
    kill_all()
    t = time.time() + 4
    while any(alive(p) for p in ALL) and time.time() < t:
        time.sleep(0.02)
    check("review fixtures proven dead", not any(alive(p) for p in ALL),
          str([p for p in ALL if alive(p)]))

print("DONE" if ok else "FAILED")
sys.exit(0 if ok else 1)
