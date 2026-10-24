#!/usr/bin/env python3
"""#61 — the runner is process-tree aware before it judges an attempt.

Evidence run 20260930-070100-fb-fix-3473882e (logs/suite.a0.log + suite.a1.log):
the child backgrounded the REAL work (detached pytest), printed progress chatter
("Suite is running ... Waiting for the actual exit."), exited 0; the runner judged
it no-result and BLIND-RETRIED attempt a1 into the SAME worktree while a0's
detached pytest was still executing — two full suites, one tree, mutually
corrupting caches. A false-green suite is the worst possible miss: the suite
verdict gates every merge lane. A progress string is not an exit.

Contracts proven here (RED before the fix):
 U1  /proc helpers exist: live children by ppid, live members of a process group
     (survives reparenting once the spawner is reaped), nonexistent pid = empty;
     left_live_descendants is in the closed set and in NEITHER retry ladder.
 U2  the /proc read itself: a same-session grandchild that outlives its spawner
     is still found by the pgid walk (killpg can reach it; death can be PROVED,
     and a killpg of a dead pgid proves it just as honestly).
 R-A a child that exits 0 with progress chatter and a live backgrounded
     descendant is NOT done and NOT blind-retried: exactly one spawn, failed
     error_class=left_live_descendants, error names progress-not-result + the
     fenced block, the tree killed and proven dead; the exit-judged node shape
     names the verdict law in the error (the 06f57ea9 suite-verdict-from-chatter
     repro shape).
 R-B a retry that DOES fire (Q4 transport, api_calls==0) proves the prior tree
     dead (node.tree_kill proof=dead) BEFORE the next spawn, and every prior
     descendant is dead at run end; the Q4 ladder itself is untouched.
 R-B2 an ordinary (non-backgrounding) rc!=0 transport death still re-spawns:
     isolation is a quarantine, not a blanket pre-respawn kill, and the
     dead-or-empty proof rides the respawn.
 R-C a healthy fenced-answer node is unchanged: one spawn, done, zero tree
     events, zero tree_* keys on the record (solo byte-identity law).

Run: PYTHONPATH=/opt/hermes /opt/hermes/.venv/bin/python tests/test_proctree_61.py
"""
import json, os, shutil, subprocess, sys, time
from pathlib import Path

from wf_test_markers import read_pid_marker   # est-jue3: one parseable-content wait family

HERE = Path(__file__).resolve().parent
BUILD = Path(os.environ.get("WF_TEST_BUILD") or HERE.parent)
sys.path.insert(0, str(BUILD))
import wf  # noqa: E402

ok = True
def check(label, cond, detail=""):
    global ok
    print(("PASS " if cond else "FAIL " + label + (f"  {detail}" if detail else "")))
    if not cond:
        ok = False

HOME = BUILD / "home-pt61"
RUNS = HOME / "workflows"
FAKE = str(HERE / "fake")
GC_PIDS = HOME / "gc_pids.txt"

def mk(run_id, nodes, **meta):
    r = RUNS / run_id
    shutil.rmtree(r, ignore_errors=True)
    (r / "nodes").mkdir(parents=True); (r / "gates").mkdir()
    (r / "graph.json").write_text(json.dumps({"name": run_id, "nodes": nodes}))
    m = {"hermes_bin": FAKE, "concurrency": 2, "node_timeout": 30,
         "retry_backoff": [0.05, 0.05], "retry_budget": 10,
         "proctree_hold_s": 0.5, "proctree_kill_proof_s": 4.0}
    m.update(meta)
    (r / "run.json").write_text(json.dumps(m))
    return r

def wfrun(run_id, extra_env=None, timeout=180):
    # WF_RUNS_ROOT pinned: an inherited shared-team root would hide this test's
    # run dirs (same isolation the golden harness practices — strip the env).
    env = dict(os.environ, HERMES_HOME=str(HOME), WF_RUNS_ROOT=str(RUNS),
               FAKE_LOG=str(HOME / "fake.log"), FAKE_GC_PIDS=str(GC_PIDS),
               **(extra_env or {}))
    return subprocess.run([sys.executable, str(BUILD / "wf.py"), "run", run_id],
                          env=env, capture_output=True, text=True, timeout=timeout).stdout.strip()

def rec_of(r, nid):
    return json.loads((r / "nodes" / f"{nid}.json").read_text())

def events(r):
    return [json.loads(l) for l in (r / "events.jsonl").read_text().splitlines()]

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

def gc_pids():
    try:
        return [int(x) for x in GC_PIDS.read_text().split() if x.strip()]
    except OSError:
        return []

shutil.rmtree(HOME, ignore_errors=True)
HOME.mkdir(parents=True); RUNS.mkdir()
(HOME / "fake.log").write_text("")

# ============ U1: helpers + closed set + retry-ladder exclusion ============
check("U1 left_live_descendants in the closed set", "left_live_descendants" in wf.ERROR_CLASSES)
check("U1 never blindly retried",
      "left_live_descendants" not in wf._RETRYABLE_CLASSES
      and "left_live_descendants" not in wf._BOUNDED_RETRY_CLASSES)
for fn in ("_proc_children_of", "_proc_pids_by_pgid"):
    check(f"U1 helper {fn} exists", callable(getattr(wf, fn, None)))

if callable(getattr(wf, "_proc_children_of", None)):
    # nonexistent pid: never invents descendants
    check("U1 nonexistent pid = no children", wf._proc_children_of(2_000_000) == [])
    check("U1 nonexistent pgid = no members", wf._proc_pids_by_pgid(2_000_000) == [])

    # ============ U2: real tree — grandchild outlives its spawner ============
    gc_marker = HOME / "u2_gc.txt"
    child_src = (
        "import os,subprocess,sys,time\n"
        f"gc=subprocess.Popen([sys.executable,'-c',"
        f"\"import os,time;open(r'{gc_marker}','w').write(str(os.getpid()));time.sleep(300)\"])\n"
        "open(sys.argv[1],'w').write(str(os.getpid()))\n"
        "time.sleep(0.4)\n"   # let the gc attach before the parent dies
    )
    cf = HOME / "u2_child.py"; cf.write_text(child_src)
    kid_pid_file = HOME / "u2_kid.txt"
    proc = subprocess.Popen([sys.executable, str(cf), str(kid_pid_file)],
                            start_new_session=True)   # mirrors run_child's spawn contract
    # est-jue3: parseable-content waits, not existence-then-int (race class of
    # est-gzmm, fixed for the 61b markers in #267): the markers are written by
    # an embedded child script and are visible at open(), before their bytes.
    kid = read_pid_marker(kid_pid_file)[0]
    kids = wf._proc_children_of(kid)
    gc = read_pid_marker(gc_marker)[0]
    check("U2 live grandchild found by ppid walk", gc in kids, f"kid={kid} kids={kids}")
    proc.wait(timeout=30)                      # reaps: grandchild reparents to init
    members = wf._proc_pids_by_pgid(kid)       # pgrp membership survives reparenting
    check("U2 same-session grandchild found by pgid walk AFTER the spawner was reaped",
          gc in members and alive(gc), f"gc={gc} members={members}")
    os.killpg(kid, 9)
    t_end = time.time() + 5
    while any(alive(p) for p in members) and time.time() < t_end:
        time.sleep(0.05)
    check("U2 killpg + /proc re-walk proves the tree dead",
          not any(alive(p) for p in members), f"members={members}")
    # the quarantine law's other half: an already-reaped tree proves dead honestly
    check("U2 dead pgid = no members (dead-or-empty proof for a reaped tree)",
          wf._proc_pids_by_pgid(kid) == [])
else:
    check("U2 helpers exist (skipped tree probes)", False, "wf._proc_children_of missing")

# ============ R-A: backgrounded descendant + progress chatter, exit 0 ============
GC_PIDS.write_text("")
(HOME / "fake.log").write_text("")
r = mk("pt61-bg", [{"id": "s", "type": "agent", "goal": "BGPROGRESS pt61-bg"}])
out = wfrun("pt61-bg", {"FAKE_MODE": "background", "FAKE_GC": "1"})
rec = rec_of(r, "s")
n_spawns = (HOME / "fake.log").read_text().count("pt61-bg")
check("R-A not done: typed left_live_descendants",
      rec.get("status") == "failed" and rec.get("error_class") == "left_live_descendants",
      json.dumps({k: rec.get(k) for k in ("status", "error_class", "error")})[:300])
check("R-A progress-not-result named in the error",
      "progress" in (rec.get("error") or "").lower()
      and "fenc" in (rec.get("error") or "").lower(), (rec.get("error") or "")[:200])
check("R-A exit-judged contract names the verdict law (06f57ea9 repro shape)",
      "verdict" in (rec.get("error") or "").lower(), (rec.get("error") or "")[:200])
check("R-A NO blind re-drive: exactly one spawn", n_spawns == 1,
      f"spawns={n_spawns} out={out[:150]}")
gcs = gc_pids()
check("R-A prior tree killed and proven dead", gcs and not any(alive(p) for p in gcs),
      f"gcs={gcs}")
check("R-A tree evidence on the record",
      isinstance(rec.get("tree_descendants"), list) and rec.get("tree_proof") == "dead",
      json.dumps({k: rec.get(k) for k in ("tree_descendants", "tree_proof")})[:200])
evs = events(r)
check("R-A node.tree_kill event carries the pids",
      any(e.get("event") == "node.tree_kill" and e.get("proof") == "dead" and e.get("pids")
          for e in evs), str([e.get("event") for e in evs])[:300])
check("R-A node.failed carries the class + attempts=1",
      any(e.get("event") == "node.failed" and e.get("error_class") == "left_live_descendants"
          and e.get("attempts") == 1 for e in evs), str(evs[-1])[:250])

# ============ R-B: retry proves the prior tree dead BEFORE the respawn ============
GC_PIDS.write_text("")
(HOME / "fake.log").write_text("")
r = mk("pt61-re", [{"id": "t", "type": "agent", "goal": "RETRYTREE pt61-re"}])
out = wfrun("pt61-re", {"FAKE_MODE": "transport", "FAKE_GC": "1", "FAKE_API_CALLS": "0"})
rec = rec_of(r, "t")
n_spawns = (HOME / "fake.log").read_text().count("pt61-re")
check("R-B Q4 ladder intact: 3 spawns, transport_exhausted",
      rec.get("error_class") == "transport_exhausted" and n_spawns == 3,
      f"class={rec.get('error_class')} spawns={n_spawns}")
evs = events(r)
names = [e.get("event") for e in evs]
# per-spawn marker: _steer_bake logs steer.baked exactly once per Popen.
# The quarantine proof: EVERY non-final tree_kill is IMMEDIATELY followed by
# the spawn marker it gates — the prior tree is dead before the next Popen.
# #61c: the quarantine may first stamp node.respawn_reap (pids, proof=dead,
# prior_alive==[] verified) — the recorded reap is the stronger form of the
# same gate, so the admitted run after a kill is [respawn_reap ->] steer.baked.
kills = [i for i, e in enumerate(evs)
         if e.get("event") == "node.tree_kill" and not e.get("final")]
spawns = [i for i, e in enumerate(evs) if e.get("event") == "steer.baked"]
reaps = [i for i, e in enumerate(evs) if e.get("event") == "node.respawn_reap"]
def _gated(k):
    nxt = evs[k + 1].get("event") if k + 1 < len(evs) else None
    if nxt == "steer.baked":
        return True
    if (nxt == "node.respawn_reap"
            and k + 2 < len(evs) and evs[k + 2].get("event") == "steer.baked"
            and evs[k + 1].get("proof") == "dead" and evs[k + 1].get("prior_alive") == []):
        return True
    # est-9ceqc: under the persisted-window law a re-drive is a FRESH node entry,
    # so the gated spawn carries its own node.started before steer.baked. Same
    # law — prior tree proven dead before the next Popen — admitted shape widened.
    return (nxt == "node.respawn_reap"
            and k + 3 < len(evs) and evs[k + 2].get("event") == "node.started"
            and evs[k + 3].get("event") == "steer.baked"
            and evs[k + 1].get("proof") == "dead" and evs[k + 1].get("prior_alive") == [])
check("R-B tree_kill proves the prior tree dead before the next spawn",
      len(kills) == len(spawns) - 1 == 2
      and all(_gated(k) for k in kills)
      and all(evs[k].get("proof") == "dead" and evs[k].get("pids") for k in kills)
      and all(i in [k + 1 for k in kills] for i in reaps),   # every reap is a quarantine stamp
      f"names={names}")
gcs = gc_pids()
check("R-B every prior descendant dead at run end", gcs and not any(alive(p) for p in gcs),
      f"gcs={gcs} alive={[p for p in gcs if alive(p)]}")

# ============ R-B2: quarantine law — a dead-or-empty tree never quarantines ============
# An ordinary rc!=0 transport death (NO backgrounded work) must still re-spawn:
# isolation is a quarantine of live trees, not a blanket pre-respawn kill.
GC_PIDS.write_text("")
(HOME / "fake.log").write_text("")
r = mk("pt61-pl", [{"id": "p", "type": "agent", "goal": "PLAINRETRY pt61-pl"}])
out = wfrun("pt61-pl", {"FAKE_MODE": "transport", "FAKE_API_CALLS": "0"})
rec = rec_of(r, "p")
n_spawns = (HOME / "fake.log").read_text().count("pt61-pl")
check("R-B2 plain transport death retries (ladder untouched)",
      rec.get("error_class") == "transport_exhausted" and n_spawns == 3,
      f"class={rec.get('error_class')} spawns={n_spawns}")
evs = events(r)
check("R-B2 plain retry carries the dead-or-empty proof",
      all(e.get("proof") == "dead" for e in evs if e.get("event") == "node.tree_kill"),
      str([e.get("event") for e in evs])[:300])

# ============ R-C: healthy fenced node unchanged (golden shape) ============
(HOME / "fake.log").write_text("")
r = mk("pt61-ok", [{"id": "h", "type": "agent", "goal": 'pt61-ok JSON:{"result":"ok"}'}])
out = wfrun("pt61-ok")
rec = rec_of(r, "h")
n_spawns = (HOME / "fake.log").read_text().count("pt61-ok")
evs = events(r)
check("R-C healthy node commits done in one spawn",
      rec.get("status") == "done" and n_spawns == 1, json.dumps(rec)[:200])
check("R-C zero tree events, zero tree_* record keys (byte-identity)",
      not any(str(e.get("event", "")).startswith("node.tree") or str(e.get("event", "")).startswith("item.tree")
              for e in evs)
      and not any(k.startswith("tree_") for k in rec), json.dumps(sorted(rec))[:300])

# ============ R-D: fenced answer + live tree => partial, never silent done ============
# The harvest law at the exit-0 edge: the answer rode stdout while the real
# backgrounded work outlived the turn — commit partial with the tree evidence,
# never a silent done (the false-green shape the issue is about).
GC_PIDS.write_text("")
(HOME / "fake.log").write_text("")
r = mk("pt61-hv", [{"id": "v", "type": "agent", "goal": "pt61-hv JSON:{\"result\":\"ok\"}"}])
out = wfrun("pt61-hv", {"FAKE_GC": "1"})
rec = rec_of(r, "v")
n_spawns = (HOME / "fake.log").read_text().count("pt61-hv")
check("R-D fenced answer + live tree commits partial (never done, never re-driven)",
      rec.get("status") == "partial" and rec.get("harvest") is not None
      and rec.get("error_class") == "left_live_descendants" and n_spawns == 1,
      json.dumps({k: rec.get(k) for k in ("status", "error_class", "harvest")})[:250])
check("R-D partial carries the proven-dead tree evidence",
      rec.get("tree_proof") == "dead" and isinstance(rec.get("tree_descendants"), list)
      and rec.get("tree_descendants"),
      json.dumps({k: rec.get(k) for k in ("tree_proof", "tree_descendants")})[:200])
gcs = gc_pids()
check("R-D backgrounded work killed before commit", gcs and not any(alive(p) for p in gcs),
      f"gcs={gcs}")

# ============ R-D2: the stuck path — a tree that never dies fails closed =======
# A same-UID SIGKILL can't legitimately be refused, so the undrainable branch
# is proven honestly at unit level: drive the REAL _tree_quiesce loop with a
# stubbed liveness observation (the one fact the harness cannot manufacture).
# The runner's caller contract — proof != "dead" => typed fail-closed record —
# is then asserted on _left_live_record directly.
_fake_target = 2_000_001
_real_alive = wf._proc_alive
wf._proc_alive = lambda p: True if p == _fake_target else _real_alive(p)
try:
    proof2, stuck2 = wf._tree_quiesce({_fake_target}, _fake_target, 0.0, 0.3)
finally:
    wf._proc_alive = _real_alive
check("R-D2 unkillable tree => quiesce reports stuck with its pids",
      proof2 == "stuck" and stuck2 == [_fake_target], f"proof={proof2} stuck={stuck2}")
rec2 = wf._left_live_record(_fake_target, stuck2, "unit probe. ", {"attempts": 2})
check("R-D2 stuck => typed left_live_descendants fail-closed record",
      rec2["status"] == "failed" and rec2["error_class"] == "left_live_descendants"
      and rec2["tree_proof"] == "stuck" and _fake_target in rec2["tree_descendants"],
      json.dumps({k: rec2.get(k) for k in ("status", "error_class", "tree_proof")}))

print("EXIT " + ("PASS" if ok else "FAIL"))
sys.exit(0 if ok else 1)
