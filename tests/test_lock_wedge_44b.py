#!/usr/bin/env python3
"""#44 (fix/lock-heal-44b) — the stranded runner.lock wedge and its safe heal.

The field shape (2026-09-29, two runs 20260929-223432-*): the runner died, children
gone, yet a fresh LOCK_EX|LOCK_NB on <run>/runner.lock returns Errno 11 forever,
while flock on a NEW file in the same dir succeeds. /proc/locks has NO row for the
lock's inode and no visible fd scan finds a holder: the holder is a kernel-side
orphan / inherited-fd survivor beyond our namespace view. Consequence: the door's
in-process probe reads HELD => runner_alive True => wait never respawns =>
wait-resume permanently impossible.

The fix contract (three laws; PR #47 was REJECTED at 9431a50 on B1/B2, honored here):

  L1 CHILD-PROBE (issue ask 1): the door's runner_live determination never takes the
     flock in-process — a short-lived child opens (NO O_CREAT) + flocks + exits; the
     child's death IS the release, so a SIGKILLed door cannot strand an fd it never
     opened. The door's own fd table is pinned stable across probes.

  L2 TRI-STATE + /proc/locks oracle + lease (the escape hatch's precondition):
     probe answers free|busy|unknown. EACCES on an EXISTING lock is 'unknown',
     NEVER free (B1: PR #47 mapped unreadable->missing->dead and let wait respawn
     over a LIVE holder). busy is resolved against the kernel's own ledger
     (/proc/locks rows for the lock inode) and the admission lease
     (<run>/runner.lease {pid,boottime,at}, heartbeated by the runner): a row or a
     live/fresh lease => live (91b9a3de cross-container law preserved — a sibling-ns
     holder heartbeats; a same-ns holder has a row). ZERO rows AND a dead/stale
     lease AND probe EAGAIN => 'wedged': a distinct state that lets wait take the
     escape hatch instead of watching forever. A MISSING lease (legacy dir) stays
     fail-closed busy — the 91b9a3de law verbatim; only the explicit release_lock
     verb carries operator authority over the legacy shape.

  L3 SAFE HEAL (issue ask 2 + B2): heal replaces the stranded inode (rename aside,
     never unlink — forensic inode kept) ONLY under the admission heal-barrier
     (runner.lock.heal — wf.py acquire_lock takes the SAME barrier BEFORE opening
     the lock path, so no cooperating admission is mid-flock during the heal), and
     RE-VERIFIES (fresh probe + /proc/locks scan + lease read) IMMEDIATELY before
     the rename: a holder that wakes between check and commit shows its row or wins
     the fresh probe => heal REFUSES, holder wins (B2). The new lock path is
     created fresh (new inode — the field itself proves a new file locks fine).

Determinism note: the true field shape needs a holder invisible to /proc/locks
(cross-namespace kernel orphan), which cannot be manufactured unprivileged here.
The kernel ledger is therefore read through one seam, WF_PROC_LOCKS_PATH (default
/proc/locks — production bytes unchanged), so the adversarial zero-row-with-EAGAIN
shape is pinned exactly; every NON-overridden row in this file is a real kernel
/proc/locks fact from a real live flock (the 91b9a3de regressions below).

Run (suite-style, fresh private root per invocation):
  env HERMES_HOME=$(mktemp -d) WF_RUNS_ROOT=$(mktemp -d) \
     python3 tests/test_lock_wedge_44b.py
"""
import importlib.util, json, os, shutil, signal, subprocess, sys, threading, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = Path(os.environ.get("WF_TEST_BUILD") or HERE.parent)   # dir with wf.py/__init__.py
os.environ["WF_TEST_BUILD"] = str(BUILD)
HOME = HERE / "home-44b-wedge"
shutil.rmtree(HOME, ignore_errors=True); HOME.mkdir()
os.environ["HERMES_HOME"] = str(HOME)
os.environ["WF_RUNS_ROOT"] = str(HOME / "workflows")     # #71 law: env pin, not home alone
os.environ.pop("WF_PROC_LOCKS_PATH", None)
sys.path.insert(0, str(BUILD))
import wfcommon  # noqa: E402

ok = True
def check(label, cond, detail=""):
    global ok
    print(("PASS " if cond else "FAIL ") + label + ("" if cond or not detail else f"  {detail}"))
    ok = ok and bool(cond)

RUNS = Path(os.environ["WF_RUNS_ROOT"]); RUNS.mkdir(parents=True, exist_ok=True)

def mkrun(name, seed_graph=True, pid=None):
    r = RUNS / name
    shutil.rmtree(r, ignore_errors=True)
    (r / "nodes").mkdir(parents=True)
    if seed_graph:
        (r / "graph.json").write_text(json.dumps(
            {"nodes": [{"id": "a", "type": "agent", "goal": "LIST: go"}]}))
        (r / "run.json").write_text(json.dumps({"name": name,
                                                "started": "2099-01-01T00:00:00+00:00"}))
    if pid is not None:
        (r / "wf.pid").write_text(str(pid))
    return r

HOLDER = str(HOME / "holder_44b.py")
Path(HOLDER).write_text(
    "import fcntl, os, signal, sys\n"
    "fd = os.open(sys.argv[1], os.O_CREAT | os.O_RDWR, 0o644)\n"
    "fcntl.flock(fd, fcntl.LOCK_EX)\n"
    "print('held', flush=True)\n"
    "open(sys.argv[2],'w').write(str(os.fstat(fd).st_ino))\n"
    "signal.pause()\n")

def hold(r, tag):
    """A REAL live exclusive holder; answers its lock inode through the ready file."""
    ready = HOME / f"held-{tag}"
    p = subprocess.Popen([sys.executable, HOLDER, str(r / "runner.lock"), str(ready)],
                         stdout=subprocess.PIPE, text=True)
    assert p.stdout.readline().strip() == "held", "holder failed to take the flock"
    for _ in range(200):                      # the ready inode lands right after
        if ready.exists() and ready.read_text().strip():
            return p, int(ready.read_text())
        time.sleep(0.02)
    raise AssertionError("holder never reported its inode")

def release(p):
    p.send_signal(signal.SIGKILL); p.wait()

FAKE_LEDGER = HOME / "fake-proc-locks"
def ledger(zero_rows=True):
    """Point the kernel-ledger seam at ZERO rows — the EXACT field symptom of #44
    (EAGAIN while the ledger knows no holder). None restores the real /proc/locks."""
    global _led_path
    if zero_rows:
        FAKE_LEDGER.write_text("")
        os.environ["WF_PROC_LOCKS_PATH"] = str(FAKE_LEDGER)
        return str(FAKE_LEDGER)
    os.environ.pop("WF_PROC_LOCKS_PATH", None)
    return None

def add_ledger_row(pid, ino, dev):
    # kernel key format: major:minor hex, INODE DECIMAL (verified against /proc/locks)
    cur = FAKE_LEDGER.read_text()
    row = "9: FLOCK  ADVISORY  WRITE %d %02x:%02x:%d 0 EOF" % (pid, os.major(dev), os.minor(dev), ino)
    FAKE_LEDGER.write_text((cur + "\n" if cur.strip() else "") + row + "\n")

def real_locks_rows(ino):
    out = []
    try:
        for line in Path("/proc/locks").read_text(errors="replace").splitlines():
            f = line.split()
            if len(f) >= 6 and f[1] == "FLOCK" and f[5].endswith(f":{ino}"):
                out.append(f)
    except OSError:
        return None
    return out

def raw_flock_result(path):
    import fcntl
    fd = os.open(str(path), os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return "free"
    except OSError as e:
        return "EAGAIN" if e.errno == 11 else f"errno{e.errno}"
    finally:
        os.close(fd)

# ============ L1: child-probe contract (issue asks 1 + 3) =========================
r = mkrun("l1-free")
fd_before = len(os.listdir("/proc/self/fd"))
for _ in range(5):
    wfcommon.runner_alive(r)
fd_after = len(os.listdir("/proc/self/fd"))
check("L1a door probe never holds an fd across the probe (door's fd table stable)",
      fd_after <= fd_before + 1, f"{fd_before} -> {fd_after}")

# ask 3: kill-the-probe-mid-flight — a child that flocks then gets SIGKILLed must
# leave the lock FREE for the next probe (kernel release on death: the child's
# death IS the release; the door never opened the fd).
r_k = mkrun("l1-killed")
p, ino = hold(r_k, "k")
release(p); time.sleep(0.25)
st, detail = wfcommon.runner_lock_state(r_k)
check("L1b SIGKILLed holder: kernel released, next probe reports free",
      st == "free", f"state={st} {detail}")

# ============ B1: EACCES on an existing lock is NEVER free =========================
# PR #47 ground B1: unreadable existing lock mapped missing->dead and let wait
# respawn over a LIVE holder. Here: real live holder + chmod-0 lock.
r_e = mkrun("b1-eacces")
p, ino = hold(r_e, "e")
lock_path = r_e / "runner.lock"
os.chmod(lock_path, 0o000)
try:
    st, detail = wfcommon.runner_lock_state(r_e)
    check("B1a EACCES on existing lock is its OWN fail-closed state, never 'free'",
          st == "eacces", f"state={st} detail={detail}")
    check("B1b eacces => runner_alive fail-closed True (never False over a live holder)",
          wfcommon.runner_alive(r_e) is True)
    hres = wfcommon.heal_wedged_runner_lock(r_e)
    check("B1c EACCES => heal REFUSES (never heals on permission failure)",
          hres.get("ok") is False and "eacces" in str(hres.get("reason", "")).lower(),
          json.dumps(hres))
    check("B1d heal refusal left the path byte-same inode (nothing renamed)",
          lock_path.exists() and os.stat(lock_path).st_ino == ino)
    stt = wfcommon.run_state(r_e)
    check("B1e door read-model fabricates no crash: runner_live True, not interrupted",
          stt.get("runner_live") is True and stt.get("status") != "interrupted",
          f"live={stt.get('runner_live')} status={stt.get('status')}")
finally:
    os.chmod(lock_path, 0o644)
    release(p)

# 91b9a3de REGRESSION (production seam, real /proc/locks + real holder): the
# cross-container law survives the new ledger consultation untouched.
r_c = mkrun("re-cross")
p, ino = hold(r_c, "c")
try:
    rows = real_locks_rows(ino)
    check("R1 real live holder has a /proc/locks row (oracle sanity)", bool(rows),
          f"ino={ino:x} rows={rows}")
    check("R2 held lock (real row) reads runner_alive True (91b9a3de preserved)",
          wfcommon.runner_alive(r_c) is True)
    hres = wfcommon.heal_wedged_runner_lock(r_c)
    check("R3 heal REFUSES while the kernel ledger shows a holder",
          hres.get("ok") is False and "holder" in str(hres.get("reason", "")).lower(),
          json.dumps(hres))
    check("R4 refusal renamed nothing (forensic inode intact)",
          os.stat(r_c / "runner.lock").st_ino == ino
          and not list(r_c.glob("runner.lock.stranded-*")))
finally:
    release(p)

# free lock + dead pid stays dead (inertness law, unchanged)
r_d = mkrun("re-dead", pid=999999)
check("R5 free lock + dead pid reads DEAD exactly as before",
      wfcommon.runner_alive(r_d) is False)

# ============ L2: the WEDGE state (simulated kernel orphan) =======================
r_w = mkrun("wedge-plain")
p, ino = hold(r_w, "w")                 # real EAGAIN forever (holder paused)
ledger(zero_rows=True)                  # kernel ledger knows NO holder: field shape
(r_w / "runner.lease").write_text(json.dumps(
    {"pid": 999999, "boottime": 1, "at": time.time() - 3600}))   # stale, pid absent
st, detail = wfcommon.runner_lock_state(r_w)
check("W1 EAGAIN + zero kernel rows + dead lease => 'wedged' (not free, not live)",
      st == "wedged", f"state={st} {detail}")
check("W2 wedged => runner_alive False (nothing can progress: zero holders proven)",
      wfcommon.runner_alive(r_w) is False)
stt = wfcommon.run_state(r_w)
check("W3 run_state publishes the wedge", stt.get("runner_lock") == "wedged",
      str(stt.get("runner_lock")))

# legacy dir (NO lease) with zero rows stays fail-closed LIVE (91b9a3de verbatim
# for pre-upgrade dirs; only the explicit operator verb overrides)
r_leg = mkrun("wedge-legacy")
p2, ino2 = hold(r_leg, "leg")
st, _d = wfcommon.runner_lock_state(r_leg)
check("W3b zero rows + NO lease (legacy) => busy, runner_alive True (fail-closed)",
      st == "busy" and wfcommon.runner_alive(r_leg) is True, st)

# ============ B2: holder that re-appears between check and commit WINS ============
r_b2 = mkrun("b2-race")
p3, ino3 = hold(r_b2, "b2")             # the orphan stand-in (real EAGAIN)
(r_b2 / "runner.lease").write_text(json.dumps(
    {"pid": 999998, "boottime": 1, "at": time.time() - 3600}))
dev_b2 = os.stat(r_b2 / "runner.lock").st_dev
def waking_holder():
    # a holder MATERIALIZES between the heal's check and its commit: its kernel
    # row appears with it (the ledger is the authority the classifier trusts).
    add_ledger_row(os.getpid() + 7000, ino3, dev_b2)
b2res = wfcommon.heal_wedged_runner_lock(r_b2, between_check_and_commit=waking_holder)
check("B2a holder that re-appears between check and commit => heal REFUSES",
      b2res.get("ok") is False and "holder" in str(b2res.get("reason", "")).lower(),
      json.dumps(b2res))
check("B2b refused heal renames nothing (same inode in place)",
      (r_b2 / "runner.lock").exists()
      and os.stat(r_b2 / "runner.lock").st_ino == ino3
      and not list(r_b2.glob("runner.lock.stranded-*")))
check("B2c the holder still holds the lock (it WON)",
      raw_flock_result(r_b2 / "runner.lock") == "EAGAIN")
release(p3)

# ============ L3: happy-path heal + RE-ACQUIRE ====================================
healed = wfcommon.heal_wedged_runner_lock(r_w)
check("W4 heal of a verified wedge returns ok", healed.get("ok") is True,
      json.dumps(healed))
strand = list(r_w.glob("runner.lock.stranded-*"))
check("W5 heal RENAMES the stranded inode aside (never unlinks — evidence kept)",
      len(strand) == 1 and strand[0].exists() and os.stat(strand[0]).st_ino == ino,
      json.dumps(healed))
check("W6 a fresh runner.lock exists (new inode — the field proves new files lock)",
      (r_w / "runner.lock").exists()
      and os.stat(r_w / "runner.lock").st_ino != ino)
check("W7 the wedge is gone: fresh EX|NB flock succeeds on the new path",
      raw_flock_result(r_w / "runner.lock") == "free")
check("W8 post-heal probe reads free and runner_alive False again (inert)",
      wfcommon.runner_lock_state(r_w)[0] == "free"
      and wfcommon.runner_alive(r_w) is False)
release(p)
check("W9 heal is idempotent: a second heal on a free lane returns ok",
      wfcommon.heal_wedged_runner_lock(r_w).get("ok") is True)

# live lease (pid+boottime identity verifies) => never wedged, heal refuses.
# PRODUCTION ledger (real /proc/locks seam restored): the holder's row is a real
# kernel fact here; the lease carries the cross-ns holder (row-less) shape.
ledger(None)
r_l = mkrun("wedge-lease")
vp, ino5 = hold(r_l, "lease")
stat_line = Path(f"/proc/{vp.pid}/stat").read_text(errors="replace")
bt = int(stat_line.rsplit(")", 1)[1].split()[19])
(r_l / "runner.lease").write_text(json.dumps(
    {"pid": vp.pid, "boottime": bt, "at": time.time() - 3600}))   # stale 'at'
hres = wfcommon.heal_wedged_runner_lock(r_l)
check("W10 live lease holder (pid+boottime verify) => heal REFUSES holder-alive",
      hres.get("ok") is False and "holder" in str(hres.get("reason", "")).lower(),
      json.dumps(hres))
st, _ = wfcommon.runner_lock_state(r_l)
check("W11 lease-live identity keeps zero-row EAGAIN 'busy' (fleet cross-ns shape)",
      st == "busy")
release(vp)

# ============ admission fences on the heal-barrier ================================
r_a = mkrun("admit-barrier")
barrier = r_a / "runner.lock.heal"
bfd = os.open(str(barrier), os.O_CREAT | os.O_RDWR, 0o644)
fcntl = __import__("fcntl")
fcntl.flock(bfd, fcntl.LOCK_EX)
spec = importlib.util.spec_from_file_location("hw44b_wf", BUILD / "wf.py")
wfx = importlib.util.module_from_spec(spec); spec.loader.exec_module(wfx)
t0 = time.monotonic()
admitted = []
th = threading.Thread(target=lambda: (wfx.acquire_lock(r_a), admitted.append(True)))
th.start()
time.sleep(0.35)
blocked = not admitted
fcntl.flock(bfd, fcntl.LOCK_UN); os.close(bfd)
th.join(timeout=15)
elapsed = time.monotonic() - t0
check("A1 admission waits on the heal-barrier BEFORE touching the lock path",
      blocked and admitted and elapsed >= 0.3,
      f"blocked={blocked} admitted={admitted} elapsed={elapsed:.2f}")
if admitted:
    try: fcntl.flock(wfx._LOCK_FD, fcntl.LOCK_UN)
    except Exception: pass

# ============ door surfaces: release_lock action + status legibility ==============
specd = importlib.util.spec_from_file_location("hw44b_door", BUILD / "__init__.py")
door = importlib.util.module_from_spec(specd); specd.loader.exec_module(door)
try:
    import wf_test_isolation as _iso71; _iso71.install(door)   # #71 writer pin
except Exception:
    pass
def call(**a): return json.loads(door.handle(a))

r_d2 = mkrun("door-wedge")
p4, ino4 = hold(r_d2, "d")
ledger(zero_rows=True)                      # orphan stand-in (real EAGAIN)
(r_d2 / "wf.pid").write_text(str(999997))
(r_d2 / "runner.lease").write_text(json.dumps(
    {"pid": 999997, "boottime": 1, "at": time.time() - 3600}))
out = call(action="status", run_id="door-wedge")
check("D1 door status on the wedge: runner_live False + wedge surfaced",
      out.get("runner_live") is False and out.get("runner_lock") == "wedged",
      json.dumps({k: out.get(k) for k in ("runner_live", "runner_lock", "status")}))
rel = call(action="release_lock", run_id="door-wedge")
check("D2 door release_lock heals the verified wedge",
      rel.get("ok") is True, json.dumps(rel))
ledger(None)                                # production ledger back
rel2 = call(action="release_lock", run_id="door-wedge")
check("D3 release_lock on a clean lane is honest (ok, nothing to do)",
      rel2.get("ok") is True, json.dumps(rel2))
ref = call(action="release_lock", run_id="does-not-exist-44b")
check("D4 release_lock refuses an unknown run",
      ref.get("ok") is False or "error" in ref, json.dumps(ref))
release(p4)

# ============ CLI: wf.py release-lock (issue ask 2) ================================
r_cli = mkrun("cli-wedge")
p5, ino6 = hold(r_cli, "cli")
ledger(zero_rows=True)                      # arm the seam BEFORE snapshotting env
env = {**os.environ, "HERMES_HOME": str(HOME), "WF_RUNS_ROOT": str(RUNS)}
(r_cli / "runner.lease").write_text(json.dumps(
    {"pid": 999996, "boottime": 1, "at": time.time() - 3600}))
pr = subprocess.run([sys.executable, str(BUILD / "wf.py"), "release-lock", "cli-wedge"],
                    capture_output=True, text=True, env=env, timeout=30)
check("C1 wf.py release-lock heals the wedge and says so",
      pr.returncode == 0 and "RELEASED" in pr.stdout,
      f"rc={pr.returncode} out={pr.stdout!r} err={pr.stderr[-300:]!r}")
pr2 = subprocess.run([sys.executable, str(BUILD / "wf.py"), "release-lock", "cli-wedge"],
                     capture_output=True, text=True, env=env, timeout=30)
check("C2 second release-lock on a free lane: honest no-op, exit 0",
      pr2.returncode == 0 and ("FREE" in pr2.stdout or "RELEASED" in pr2.stdout),
      f"rc={pr2.returncode} out={pr2.stdout!r}")
pr3 = subprocess.run([sys.executable, str(BUILD / "wf.py"), "release-lock",
                      "does-not-exist-44b"],
                     capture_output=True, text=True, env=env, timeout=30)
check("C3 CLI refuses an unknown run (nonzero, REFUSED)",
      pr3.returncode != 0 and "REFUSED" in (pr3.stdout + pr3.stderr),
      f"rc={pr3.returncode} out={pr3.stdout!r}")
release(p5)

shutil.rmtree(HOME, ignore_errors=True)
print(("" if ok else "FAILURES PRESENT ") + "DONE")
sys.exit(0 if ok else 1)
