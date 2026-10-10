#!/usr/bin/env python3
"""est-2ek.1.857 — the seat semaphore must never drop a LIVE ticket, and never
admit past the cap (over-admit ~2x field report).

Failure (field report 2026-10-08 12:45–12:48Z, spool key 248a5bf33fe5999a,
evidence recorded locally as seat-overadmit-20261008T1248Z):
8–9 live workflow agent children vs 4 tickets in ~/.hermes/workflows/.seats
(cap 4). Three children had NO ticket while their runner and the child were
both alive; the surviving tickets were always a same-second batch of four,
i.e. something dropped LIVE tickets wholesale and four waiters re-admitted at
once. The ticket count protecting the pinned model server was void.

Two base-code gaps this contract closes:

 (B1) prune is single-shot: `_seat_live_tickets` unlinks a ticket the FIRST
      time every holder fails `_seat_holder_live`. One transiently-wrong death
      verdict (an ESRCH/Z-state sample that does not repeat) deletes capacity
      that is in use — an unconfirmed verdict must never be load-bearing.
      Fix contract: before unlinking, RECONFIRM the death verdict for every
      holder (re-run the same liveness law after a short settle); any holder
      that reconfirms alive keeps its ticket AND counts as live (unknown is
      never undercounted).

 (B2) the prune of another holder's ticket is silent — the next incident is
      again invisible. Fix contract: every external-ticket unlink appends one
      line to the seats-dir ledger `.pruned.jsonl` (ts, pruner pid, reason,
      ticket row snapshot): loud, never silent.

 (B3) the ask verbatim — an ENGINE-level invariant under concurrent runners:
      at every sample while agents hold seats, the set of live tickets equals
      the set of live fake agent children (each ticket names a live bound
      child, every live child is named by exactly one ticket); killing one
      holder's child and letting the blocked third runner in preserves the
      invariant. This test is green-by-construction at head; it exists so a
      future ticket-drop path fails loudly instead of shipping.

Run: cd tests && python3 test_seat_overadmit_857.py
"""
import importlib.util, json, os, shutil, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
HOME = HERE / "home-857-overadmit"


def _kill_stray_fakes():
    """Kill any fake_hermes.py whose command line names our HOME dir.

    The fake children are spawned in their own session, so the runner-group
    killpg sweep cannot reach them: a previous instance of this test (whose
    holders hang 30 s) leaks live fakes whose cmdline still carries the
    fixed HOME path, and a re-run inside that window would count them as
    children. Sweep at start AND at teardown — never leave a fake alive."""
    out = subprocess.run(["ps", "-eo", "pid,args"], capture_output=True,
                         text=True).stdout
    for line in out.splitlines():
        if "fake_hermes.py" in line and str(HOME) in line:
            pid = int(line.split()[0])
            try:
                os.killpg(os.getpgid(pid), 9)
            except OSError:
                try:
                    os.kill(pid, 9)
                except OSError:
                    pass


_kill_stray_fakes()
shutil.rmtree(HOME, ignore_errors=True)
HOME.mkdir()
os.environ["HERMES_HOME"] = str(HOME)
os.environ.pop("WF_RUNS_ROOT", None)
for k in ("WF_SEATS_DIR", "WORKFLOW_MAX_SEATS", "WF_TEST_BUILD", "FAKE_LOG"):
    os.environ.pop(k, None)
_spec = importlib.util.spec_from_file_location("hw_857_overadmit", ROOT / "wf.py")
wf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wf)

ok = True
def check(label, cond, detail=""):
    global ok
    print(("PASS " if cond else "FAIL ") + label + ("" if cond or not detail else f"  {detail}"))
    ok = ok and bool(cond)

# ---------- B1: a transiently-wrong death verdict never unlinks a ticket -------
seats = HOME / "seats-B1"
seats.mkdir(parents=True)
victim = seats / "live-holder.json"
victim.write_text(json.dumps({"pid": os.getpid(), "name": "B1-holder",
                              "ts": round(time.time(), 3)}))
_orig_alive = wf._proc_alive
_calls = {"n": 0}
def _flip_alive(pid):
    _calls["n"] += 1
    if _calls["n"] == 1:
        return False                    # transient mis-sample: "dead"
    return True                         # reconfirmation: alive
wf._proc_alive = _flip_alive
try:
    live = wf._seat_live_tickets(seats)
finally:
    wf._proc_alive = _orig_alive
check("B1: a first-pass death verdict that does not reconfirm keeps the ticket "
      "(prune requires reconfirmation) — base unlinks on the single sample",
      victim.exists() and live == 1,
      f"exists={victim.exists()} live={live} proc_alive_calls={_calls['n']}")

# ---------- B2: pruning an external dead ticket is logged in the ledger -------
seats = HOME / "seats-B2"
seats.mkdir(parents=True)
dead = seats / "dead-holder.json"
dead.write_text(json.dumps({"pid": 10**9 + 7, "name": "B2-dead",
                            "ts": round(time.time(), 3)}))
live2 = wf._seat_live_tickets(seats)
check("B2 control: a verifiably dead ticket is pruned and does not count",
      not dead.exists() and live2 == 0, f"exists={dead.exists()} live={live2}")
led = seats / ".pruned.jsonl"
rows = []
if led.exists():
    for line in led.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
hit = any(isinstance(r, dict) and "dead-holder" in json.dumps(r) for r in rows)
check("B2: the external prune appended a ledger row naming the pruned ticket "
      "to <seats>/.pruned.jsonl — base prunes silently",
      hit, f"ledger rows={rows}")

# ---------- B3: tickets == live agent children under concurrent runners -------
RUNS = HOME / "workflows"
SEATS = RUNS / ".seats"

def mk(run_id, hang_s):
    r = RUNS / run_id
    shutil.rmtree(r, ignore_errors=True)
    (r / "nodes").mkdir(parents=True); (r / "gates").mkdir()
    (r / "graph.json").write_text(json.dumps(
        {"name": run_id, "nodes": [{"id": "w", "type": "agent", "goal": "work"}]}))
    (r / "run.json").write_text(json.dumps(
        {"hermes_bin": str(HERE / "fake"), "concurrency": 1,
         "node_timeout": 60, "max_seats": 2}))
    return r

def _scrubbed_env(extra=None):
    env = {k: v for k, v in os.environ.items() if not k.startswith("WF_")}
    env["HERMES_HOME"] = str(HOME)
    env["WF_RUNS_ROOT"] = str(RUNS)
    env["WF_SEATS_DIR"] = str(SEATS)
    env.update(extra or {})
    return env

procs = {}
def launch(r, hang_s):
    env = _scrubbed_env({"FAKE_MODE": "hang", "FAKE_HANG_SEC": str(hang_s)})
    env.pop("FAKE_LOG", None)
    # own session: the runner (and its killpg'able group) never shares this
    # test's process group — the cleanup sweep cannot SIGKILL the test itself.
    procs[r.name] = subprocess.Popen([sys.executable, str(ROOT / "wf.py"), "run", r.name],
                                     env=env, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL,
                                     start_new_session=True)

def fake_children():
    out = subprocess.run(["ps", "-eo", "pid,args"], capture_output=True,
                         text=True).stdout
    return {int(l.split()[0]) for l in out.splitlines()
            if "fake_hermes.py" in l and str(HOME) in l}

def ticket_rows():
    rows = {}
    if SEATS.is_dir():
        for t in SEATS.glob("*.json"):
            try:
                rows[t.name] = json.loads(t.read_text())
            except Exception:
                pass
    return rows

def invariant_once():
    """(ok, detail): live tickets == live fake children, 1:1 by bound child pid."""
    fc = fake_children()
    tr = ticket_rows()
    bound = {int(r["child"]) for r in tr.values()
             if isinstance(r, dict) and r.get("child") is not None}
    alive_bound = {p for p in bound if p in fc}
    return (fc == alive_bound and len(alive_bound) == len(tr) and len(tr) <= 2,
            f"fake_children={sorted(fc)} tickets={len(tr)} bound_alive={sorted(alive_bound)}")


def invariant_settle(timeout=10.0):
    """Poll invariant_once until it holds or the window dies.

    A single sample can straddle two writes: the waiter's ticket lands BEFORE
    its child appears in `ps` (admit-then-spawn is the engine's order), and a
    released ticket vanishes only AFTER the node commit the test just watched
    for. Those transients resolve in milliseconds and are not the 857 shape —
    a PERSISTENT violation (tickets while no child lives, children with no
    ticket) never settles green and still fails."""
    deadline = time.time() + timeout
    while True:
        g, d = invariant_once()
        if g or time.time() >= deadline:
            return g, d
        time.sleep(0.2)

r1 = mk("r857-holder-a", 30)
r2 = mk("r857-holder-b", 30)
launch(r1, 30)
launch(r2, 30)

t_end = time.time() + 40
while time.time() < t_end and len(ticket_rows()) < 2:
    time.sleep(0.15)
# Baseline gate: hold the window start until the invariant has been observed
# green at least once. A ticket lands before its child appears in `ps`
# (admit-then-spawn is the engine's order); sampling from launch would count
# that millisecond build-up as a violation. Once both children are alive and
# bound, the strict all-samples window begins — the waiter never spawns while
# two tickets are held, so any violation inside it is the 857 shape.
t_base = time.time() + 40
while time.time() < t_base and not invariant_once()[0]:
    time.sleep(0.15)

r3 = mk("r857-waiter-c", 4)
launch(r3, 4)

settled = 0
bad = []
t_end = time.time() + 18
while time.time() < t_end:
    g, d = invariant_once()
    if g and len(fake_children()) == 2:
        settled += 1
    else:
        bad.append(d)
    time.sleep(0.6)
check("B3: invariant holds across ~25 samples while two runners hold the cap "
      "(tickets == live children, cap never exceeded, 1:1 child binding)",
      settled >= 15 and len(fake_children()) == 2 and not bad,
      f"settled={settled} violations={bad[:3]}")

r3_events = (r3 / "events.jsonl")
ev3 = []
if r3_events.exists():
    for line in r3_events.read_text().splitlines():
        try:
            ev3.append(json.loads(line))
        except Exception:
            pass
check("B3: the third runner is the one queued (seat.wait logged; its child never spawned)",
      any(e.get("event") == "seat.wait" for e in ev3) and len(fake_children()) == 2,
      f"events={[e.get('event') for e in ev3]}")

# kill holder b's child: its runner must release the seat for the waiter
tb = None
for name, row in ticket_rows().items():
    if name.startswith(r2.name):
        tb = row.get("child")
check("B3 setup: holder b's ticket names its live bound child",
      tb is not None and int(tb) in fake_children(), f"child={tb}")
if tb is not None:
    try:
        os.killpg(os.getpgid(int(tb)), 9)
    except OSError:
        try: os.kill(int(tb), 9)
        except OSError: pass

def wait_node(r, timeout=45):
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = r / "nodes" / "w.json"
        if p.exists():
            try:
                d = json.loads(p.read_text())
            except Exception:
                d = {}
            if d.get("status") in ("done", "failed"):
                return d
        time.sleep(0.15)
    return {}

rec2 = wait_node(r2)
rec3 = wait_node(r3)
check("B3: the killed holder's seat went back and the waiter was admitted (its node committed)",
      bool(rec3), json.dumps({"b": rec2.get("status"), "c": rec3.get("status")}))
g, d = invariant_settle()
check("B3: invariant still holds after the re-admission (tickets == live children)",
    g, d)

led3 = SEATS / ".pruned.jsonl"
dropped_live = False
runners = {p.pid for p in procs.values()}
if led3.exists():
    for line in led3.read_text().splitlines():
        try:
            row = json.loads(line)
        except Exception:
            continue
        # any ledger row naming a still-live holder (a runner pid of r1..r3) is
        # a live-ticket drop — the 857 field shape — fail.
        inner = row.get("row") if isinstance(row.get("row"), dict) else row
        pid = inner.get("pid")
        if isinstance(pid, int) and pid in runners:
            dropped_live = True
check("B3: no live ticket was pruned during the whole run (no ledger row names a live holder)",
      not dropped_live, "a ledger row names a still-live holder — the 857 drop shape")

for p in procs.values():
    try:
        os.killpg(os.getpgid(p.pid), 9)
    except OSError:
        try: p.kill()
        except OSError: pass
for p in procs.values():
    try: p.wait(timeout=10)
    except Exception: pass

_kill_stray_fakes()      # fakes live in their own sessions — killpg can't reach them
shutil.rmtree(HOME, ignore_errors=True)
print(("" if ok else "FAILURES PRESENT ") + "DONE")
sys.exit(0 if ok else 1)
