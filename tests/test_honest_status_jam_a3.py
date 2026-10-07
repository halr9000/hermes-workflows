#!/usr/bin/env python3
"""Honest status (jam-a3): the answer is the fact, the exit is history.

(1) A committed record an odd/older runner path stamped `failed` although its
    fenced answer validated (the runner's `harvest` stamp is the proof it went
    through _harvest_death's gate) reads DONE in the read model — node_rec and
    run_state — so finished work is never re-driven or reported failed.
(2) runner_exit.json's graph-bound verdict outranks the pid-liveness guess:
    a record-valid 'done' beats a stale pid that still reads live; without the
    record the liveness heuristic keeps working untouched.

Hand-built run dirs (records written the way wf.py commits them), stdlib-only.
"""
import importlib.util, json, os, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import wfcommon

HOME = Path(tempfile.mkdtemp(prefix="home-honest-a3-", dir=str(ROOT)))
os.environ["HERMES_HOME"] = str(HOME)

ok = 0
def check(cond, msg, detail=""):
    global ok
    assert cond, f"{msg} — {detail}" if detail else msg
    ok += 1; print("PASS", msg)

SCHEMA = {"type": "object", "required": ["branch"], "properties": {"branch": {"type": "string"}}}

def mk(rid, nodes, records=None, runner_exit=None, pid=None):
    r = HOME / "workflows" / rid
    (r / "nodes").mkdir(parents=True)
    (r / "logs").mkdir()
    (r / "gates").mkdir()
    (r / "graph.json").write_text(json.dumps({"name": rid, "nodes": nodes}))
    (r / "run.json").write_text(json.dumps({"name": rid, "hermes_bin": "/bin/true"}))
    byid = {n["id"]: n for n in nodes}
    for nid, rec in (records or {}).items():
        d = dict(rec)
        d.setdefault("efp", wfcommon.efp(byid, byid[nid]))
        d.setdefault("fp_rule_version", wfcommon.FP_RULE_VERSION)
        (r / "nodes" / f"{nid}.json").write_text(json.dumps(d))
    if runner_exit is not None:
        rec = dict(runner_exit)
        rec.setdefault("graph_fingerprint", wfcommon.graph_fingerprint({"name": rid, "nodes": nodes}))
        rec.setdefault("fp_rule_version", wfcommon.FP_RULE_VERSION)
        (r / "runner_exit.json").write_text(json.dumps(rec))
    if pid is not None:
        (r / "wf.pid").write_text(str(pid))
    return r

try:
    # ---- rule 1: harvested-answer node reports done despite failed status ----
    n1 = {"id": "lane", "type": "agent", "goal": "work", "schema": SCHEMA}
    r1 = mk("hs-harvest", [n1], records={
        "lane": {"status": "failed", "error": "child exited rc=1", "error_class": "unknown",
                 "output": {"branch": "jam/x", "commits": 3},
                 "harvest": {"declared_status": None}, "ms": 500, "attempts": 1},
    })
    st, rec = wfcommon.node_rec(r1, n1, {"lane": n1})
    check(st == "done", "rule 1: failed record with validating harvest answer reads done", str(st))
    rs = wfcommon.run_state(r1)
    check(rs["status"] == "done" and rs["nodes"]["lane"]["status"] == "done",
          "rule 1: run closes done, node shown done", json.dumps(rs)[:200])
    check(rs["done"] == 1, "rule 1: harvested node counts done", str(rs["done"]))

    # declared BLOCKED is honored verbatim — never flips to done
    n2 = {"id": "lane", "type": "agent", "goal": "work", "schema": SCHEMA}
    r2 = mk("hs-blocked", [n2], records={
        "lane": {"status": "failed", "error": "child exited rc=2", "error_class": "unknown",
                 "output": {"branch": "jam/y"}, "harvest": {"declared_status": "BLOCKED"},
                 "ms": 500, "attempts": 1},
    })
    st2, _ = wfcommon.node_rec(r2, n2, {"lane": n2})
    check(st2 == "failed", "rule 1: child-declared BLOCKED harvest stays failed", str(st2))

    # failed WITHOUT harvest evidence is untouched
    n3 = {"id": "lane", "type": "agent", "goal": "work", "schema": SCHEMA}
    r3 = mk("hs-plain", [n3], records={
        "lane": {"status": "failed", "error": "child exited rc=1", "error_class": "unknown",
                 "output": {"branch": "jam/z"}, "ms": 500, "attempts": 1},
    })
    st3, _ = wfcommon.node_rec(r3, n3, {"lane": n3})
    check(st3 == "failed", "rule 1: failed WITHOUT harvest stamp unchanged (no bare-output flips)", str(st3))

    # schema-violating harvest does NOT read done
    n4 = {"id": "lane", "type": "agent", "goal": "work", "schema": SCHEMA}
    r4 = mk("hs-badschema", [n4], records={
        "lane": {"status": "failed", "error": "child exited rc=1", "error_class": "unknown",
                 "output": {"wrong": 1}, "harvest": {"declared_status": None},
                 "ms": 500, "attempts": 1},
    })
    st4, _ = wfcommon.node_rec(r4, n4, {"lane": n4})
    check(st4 == "failed", "rule 1: harvest that fails schema re-validation stays failed", str(st4))

    # efp-stale harvested record stays pending (validity law outranks honesty)
    r5 = mk("hs-stale", [n1], records={})
    (r5 / "nodes" / "lane.json").write_text(json.dumps(
        {"status": "failed", "error": "rc=1", "error_class": "unknown",
         "output": {"branch": "jam/x"}, "harvest": {"declared_status": None},
         "efp": "deadbeef", "fp_rule_version": wfcommon.FP_RULE_VERSION}))
    st5, _ = wfcommon.node_rec(r5, n1, {"lane": n1})
    check(st5 == "pending", "rule 1: efp-stale record stays pending — amend must re-drive it", str(st5))

    # ---- rule 2: runner_exit verdict beats the stale-pid live guess ----
    nA = {"id": "a", "type": "agent", "goal": "go"}
    rA = mk("hs-stalepid", [nA],
            records={"a": {"status": "done", "output": {"ok": True}, "ms": 10, "attempts": 1}},
            runner_exit={"reason": "done", "at": "2026-09-29T00:00:00+00:00"},
            pid=os.getpid())   # a live pid that is NOT a wf.py runner → honest model: not live
    rsA = wfcommon.run_state(rA)
    check(rsA["status"] == "done", "rule 2: dead-pid + exit 'done' reads done (baseline behavior holds)", rsA["status"])

    # Forge liveness: monkeypatch runner_alive to a stale-pid-alive misread.
    real_alive = wfcommon.runner_alive
    try:
        # (#44 keeps the forge honest: run_state threads a lock_state kwarg in —
        # the forged predicate ignores it, exactly as it ignored pid_path.)
        wfcommon.runner_alive = lambda r, pid_path=None, lock_state=None: True
        rsB = wfcommon.run_state(rA)
        check(rsB["status"] == "done",
              "rule 2: exit-record 'done' beats a stale-pid live guess", json.dumps(rsB)[:200])

        # ...and WITHOUT the record, the liveness guess still says running (heuristic intact)
        rC = mk("hs-norecord", [nA], records={}, pid=os.getpid())
        rsC = wfcommon.run_state(rC)
        check(rsC["status"] == "running", "rule 2: no exit record + live guess stays running", rsC["status"])

        # 'stopped' verdict beats the guess too
        rD = mk("hs-stopverdict", [nA], records={},
                runner_exit={"reason": "stopped", "at": "x"}, pid=os.getpid())
        rsD = wfcommon.run_state(rD)
        check(rsD["status"] == "stopped", "rule 2: exit 'stopped' beats the live guess", rsD["status"])
    finally:
        wfcommon.runner_alive = real_alive

    # amended graph makes the old 'done' verdict stale → honest interrupted, not done
    (rA / "graph.json").write_text(json.dumps(
        {"name": "hs-stalepid", "nodes": [dict(nA, goal="go v2")]}))
    rsA2 = wfcommon.run_state(rA)
    check(rsA2["status"] == "interrupted" and (rsA2["runner_exit"] or {}).get("reason") == "stale",
          "rule 2: an amend invalidates the old verdict (fingerprint) — no false done",
          json.dumps({"s": rsA2["status"], "rx": rsA2["runner_exit"]})[:200])

    print(f"ALL PASS ({ok})")
finally:
    import shutil
    shutil.rmtree(HOME, ignore_errors=True)
