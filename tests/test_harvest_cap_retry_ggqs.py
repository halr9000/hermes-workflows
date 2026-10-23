#!/usr/bin/env python3
"""est-ggqs / issue #256 claim 1: a harvest-on-death `partial` must reach the
#5 bounded (layer-2) re-drive, and a per-node clamp policy decides what a
schema-valid harvest commits.

Confirmed shape (triage run 20261008-112019-ra-issue-256-nightly, main@0d91f80):
a child that dies rc!=0 AFTER printing a schema-valid fenced answer is committed
`partial` by _harvest_death (error_class = the death class, e.g. cap_exhausted),
and _bounded_retry early-returned on it (`status != "failed" or harvest`), so a
turn-capped node that made real tool progress never got the ONE machine-resume
re-drive a failed record of the same class gets; every plain descendant then
failed `blocked_by_partial_ancestor`.

Pins:
  (a) partial + valid harvest + class cap_exhausted (and every other
      _BOUNDED_RETRY_CLASSES member) WITH tool-progress evidence gets exactly
      one layer-2 re-drive — same progress / stop / budget / quarantine guards.
  (b) partial WITHOUT progress evidence (or stop set, budget spent, class not
      bounded) still fails closed: no re-drive, record unchanged, a plain
      descendant still blocks typed (default policy `strict`, unchanged).
  (c) _transient_retry (Q4) is unchanged for non-harvest failed records and
      never touches a harvested partial.
  Clamp policy (`harvest_policy`, agent key): `strict` (default) = blocked as
  today; `validate` = a harvest that validates against the node's DECLARED
  schema, declares no unfinished `## Remaining` steps and no non-done terminal
  status, over a clean lane, commits `done` — the death (error_class/error/
  harvest stamp) stays on the record and on node.finished, so the turn cap is
  still visible; plain descendants release.

Run: cd tests && PYTHONPATH=/opt/hermes:.. WF_RUNS_ROOT=$(mktemp -d) \
     timeout 600 python3 test_harvest_cap_retry_ggqs.py
"""
import copy
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
FAKE = str(HERE / "fake")
SCHEMA = {"type": "object", "properties": {"result": {"type": "string"}},
          "required": ["result"]}

_TMP = tempfile.TemporaryDirectory(prefix="ggqs-")
HOME = Path(_TMP.name)
RUNS = HOME / "workflows"
RUNS.mkdir()
(HOME / "fake.log").write_text("")
os.environ["HERMES_HOME"] = str(HOME)
os.environ["WF_RUNS_ROOT"] = str(RUNS)

import wfcommon  # noqa: E402


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


engine = load("ggqs_engine", ROOT / "wf.py")

checks = 0
failures = 0


def check(label, cond, detail=""):
    global checks, failures
    checks += 1
    if cond:
        print(f"PASS {label}")
    else:
        failures += 1
        print(f"FAIL {label} :: {str(detail)[:600]}")


# ---------------------------------------------------------------------------
# in-process units: _bounded_retry / _transient_retry over synthetic records
# ---------------------------------------------------------------------------
class FakeStop:
    def __init__(self, set_=False):
        self._set = set_
        self.waited = []

    def is_set(self):
        return self._set

    def wait(self, s):
        self.waited.append(s)
        return self._set


def harvested(eclass="cap_exhausted", **kw):
    """The exact record shape run_child commits for a harvest-on-death."""
    rec = {"status": "partial",
           "error": "child hit its turn budget: max_iterations_reached(35/35) "
                    "(max_turns=35; answer harvested from stdout)",
           "error_class": eclass, "ms": 10, "final": "census written",
           "output": {"result": "census-complete"}, "remaining": [],
           "harvest": {"declared_status": None}, "skey": "wf:r:plan:abcd1234.x"}
    rec.update(kw)
    return rec


class Respawner:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)
        return copy.deepcopy(self.result)


def bounded(r, respawn, progress=True, stop=None, retries_left=6):
    """Run _bounded_retry with tool-progress evidence stubbed; returns
    (result, meta, stop)."""
    tmp = tempfile.mkdtemp(prefix="ggqs-unit-", dir=str(HOME))
    run = Path(tmp) / "run"
    run.mkdir()
    stop = stop or FakeStop()
    meta = {"_run": run, "_stop": stop, "_procs_lock": threading.Lock(),
            "_retries_left": retries_left}
    orig_tp = engine._tool_progress
    engine._tool_progress = lambda *_a, **_k: progress
    try:
        # production call sites (run_agent_node) always pass the node dict; main's
        # evolved #102 harvest preamble (_banked_work -> child_work_dir) requires it.
        out = engine._bounded_retry(meta, r, respawn, "node", {"node": "plan"},
                                    node={"id": "plan"})
    finally:
        engine._tool_progress = orig_tp
    return out, meta, stop


DONE = {"status": "done", "spawn": 1, "output": {"result": "redriven-clean"}, "ms": 5}

# (a) cap_exhausted harvest WITH progress -> exactly ONE layer-2 re-drive
rs = Respawner(DONE)
r0 = harvested()
out, meta, stop = bounded(r0, rs)
check("(a) harvested cap_exhausted partial with tool progress is re-driven exactly once",
      len(rs.calls) == 1, f"calls={rs.calls}")
check("(a) the re-drive carries the machine resume preamble naming cap_exhausted",
      rs.calls and "Prior attempt died: error_class=cap_exhausted"
      in (rs.calls[0].get("resume_preamble") or ""), rs.calls)
check("(a) the re-drive's verdict is committed (done) with the attempt ledger",
      out.get("status") == "done" and out.get("output") == {"result": "redriven-clean"}
      and [a.get("error_class") for a in out.get("attempts_log") or []] == ["cap_exhausted"]
      and (out.get("attempts_log") or [{}])[0].get("resume") is True, out)
check("(a) the turn-cap death stays visible: attempts_log entry carries the harvest",
      (out.get("attempts_log") or [{}])[0].get("harvested") is True, out.get("attempts_log"))
check("(a) the re-drive draws from the SAME shared retry budget",
      meta["_retries_left"] == 5, meta["_retries_left"])
check("(a) the re-drive waits the same fixed bounded backoff",
      stop.waited == [engine._BOUNDED_RETRY_BACKOFF], stop.waited)

for cls in engine._BOUNDED_RETRY_CLASSES:
    rs = Respawner(DONE)
    out, _m, _s = bounded(harvested(cls), rs)
    check(f"(a) harvested partial of bounded class {cls!r} with progress is re-driven once",
          len(rs.calls) == 1 and out.get("status") == "done", f"calls={len(rs.calls)} out={out}")

# (a') re-drive that dies WITHOUT an answer never trades the harvest away
rs = Respawner({"status": "failed", "error": "child exited rc=2: connection error",
                "error_class": "transport", "spawn": 1, "ms": 3})
out, _m, _s = bounded(harvested(), rs)
check("(a') a re-drive that dies answer-less keeps the schema-valid harvest (partial)",
      len(rs.calls) == 1 and out.get("status") == "partial"
      and out.get("output") == {"result": "census-complete"}
      and out.get("error_class") == "cap_exhausted", out)
check("(a') ... and its ledger names BOTH the harvested death and the lost re-drive",
      [a.get("error_class") for a in out.get("attempts_log") or []]
      == ["cap_exhausted", "transport"], out.get("attempts_log"))
rs = Respawner({"status": "partial", "error": "child hit its turn budget again",
                "error_class": "cap_exhausted", "spawn": 1, "ms": 3,
                "output": {"result": "census-v2"}, "harvest": {"declared_status": None},
                "remaining": []})
out, _m, _s = bounded(harvested(), rs)
check("(a') a re-drive that harvests again commits the NEWER harvest (never a loop)",
      len(rs.calls) == 1 and out.get("status") == "partial"
      and out.get("output") == {"result": "census-v2"}, out)
rs = Respawner({"status": "failed", "error": "tree outlived the turn", "spawn": 1,
                "error_class": "left_live_descendants", "ms": 3})
out, _m, _s = bounded(harvested(), rs)
check("(a') a re-drive that left live descendants surfaces its terminal class (#61)",
      out.get("status") == "failed" and out.get("error_class") == "left_live_descendants", out)

# (b) no progress / stop / budget / non-bounded class -> fail closed, unchanged
rs = Respawner(DONE)
r0 = harvested()
snap = copy.deepcopy(r0)
out, meta, stop = bounded(r0, rs, progress=False)
check("(b) harvested partial WITHOUT tool progress is NOT re-driven (fail closed)",
      not rs.calls and out == snap and stop.waited == [] and meta["_retries_left"] == 6,
      f"calls={rs.calls} out={out}")
rs = Respawner(DONE)
out, _m, _s = bounded(harvested(), rs, progress=None)
check("(b) unavailable progress evidence (None) never opens the re-drive",
      not rs.calls and out.get("status") == "partial", out)
rs = Respawner(DONE)
out, _m, _s = bounded(harvested(), rs, stop=FakeStop(True))
check("(b) a set stop flag blocks the harvested re-drive",
      not rs.calls and out.get("status") == "partial", out)
rs = Respawner(DONE)
out, meta, _s = bounded(harvested(), rs, retries_left=0)
check("(b) an exhausted shared retry budget blocks the harvested re-drive",
      not rs.calls and out.get("status") == "partial" and meta["_retries_left"] == 0, out)
for cls in ("unknown", "left_live_descendants", "schema", "provider_400", "cancelled"):
    rs = Respawner(DONE)
    out, _m, _s = bounded(harvested(cls), rs)
    check(f"(b) harvested partial of NON-bounded class {cls!r} is never re-driven",
          not rs.calls and out.get("status") == "partial", f"calls={len(rs.calls)}")
rs = Respawner(DONE)
cancelled_harvest = {"status": "failed", "error_class": "cancelled", "skey": "s",
                     "output": {"result": "flushed"}, "harvest": {"declared_status": None}}
out, _m, _s = bounded(cancelled_harvest, rs)
check("(b) a FAILED record carrying a harvest (cancel-harvest shape) is still never re-driven",
      not rs.calls and out is cancelled_harvest, out)
rs = Respawner(DONE)
out, _m, _s = bounded({"status": "partial", "error_class": "cap_exhausted", "skey": "s",
                       "output": {"result": "x"}}, rs)
check("(b) a partial with NO harvest stamp is never re-driven",
      not rs.calls and out.get("status") == "partial", out)
rs = Respawner(DONE)
out, _m, _s = bounded({"status": "done", "output": {"result": "x"}}, rs)
check("(b) a done record is never re-driven", not rs.calls and out.get("status") == "done", out)
rs = Respawner(DONE)
out, _m, _s = bounded({"status": "failed", "error_class": "cap_exhausted", "skey": "s",
                       "raw": "x"}, rs)
check("(b) control: a FAILED cap_exhausted record with progress keeps its one re-drive",
      len(rs.calls) == 1 and out.get("status") == "done", out)


# (c) _transient_retry unchanged for non-harvest failed records
def transient(r, respawn, api_calls=0):
    tmp = tempfile.mkdtemp(prefix="ggqs-q4-", dir=str(HOME))
    run = Path(tmp) / "run"
    run.mkdir()
    meta = {"_run": run, "_stop": threading.Event(), "_procs_lock": threading.Lock(),
            "_retries_left": 6, "retry_backoff": [0.0, 0.0]}
    orig = (engine._attempt_api_calls, engine._retry_sleep, engine.log)
    seen = []
    engine._attempt_api_calls = lambda *_a, **_k: api_calls
    engine._retry_sleep = lambda *_a, **_k: 0.0
    engine.log = lambda _run, ev, **kw: seen.append((ev, kw))
    try:
        out = engine._transient_retry(meta, r, respawn, "node", {"node": "plan"}, defer=False)  # est-9ceqc: in-place mode — this test owns the park/history handoff, not the defer law
    finally:
        engine._attempt_api_calls, engine._retry_sleep, engine.log = orig
    return out, meta, seen


class Q4Respawn:
    def __init__(self):
        self.n = 0

    def __call__(self, **_kw):
        self.n += 1
        return {"status": "failed", "error_class": "transport", "spawn": self.n,
                "skey": f"s{self.n}", "raw": "Connection error."}


q4 = Q4Respawn()
out, meta, seen = transient({"status": "failed", "error_class": "transport", "skey": "s0",
                             "raw": "Connection error.", "spawn": 0}, q4)
check("(c) Q4: zero-api-call transport death retried exactly twice, then transport_exhausted",
      q4.n == 2 and out.get("error_class") == "transport_exhausted"
      and len(out.get("attempts_log") or []) == 2 and meta["_retries_left"] == 4,
      f"n={q4.n} out={out}")
q4 = Q4Respawn()
out, meta, seen = transient({"status": "failed", "error_class": "transport", "skey": "s0",
                             "raw": "Connection error.", "spawn": 0}, q4, api_calls=3)
check("(c) Q4: a transport death WITH api calls is never Q4-respawned (unchanged)",
      q4.n == 0 and out.get("error_class") == "transport" and "attempts_log" not in out,
      f"n={q4.n} out={out}")
q4 = Q4Respawn()
out, meta, seen = transient({"status": "failed", "error_class": "cap_exhausted", "skey": "s0",
                             "raw": "x", "spawn": 0}, q4)
check("(c) Q4: a non-retryable class (cap_exhausted) is untouched",
      q4.n == 0 and out.get("error_class") == "cap_exhausted", out)
q4 = Q4Respawn()
hv = harvested("transport")
snap = copy.deepcopy(hv)
out, meta, seen = transient(hv, q4)
check("(c) Q4 never touches a harvested partial (no respawn, record byte-identical)",
      q4.n == 0 and out == snap and not seen, f"n={q4.n} seen={seen}")


# ---------------------------------------------------------------------------
# clamp policy: pure function + door
# ---------------------------------------------------------------------------
clamp = getattr(engine, "_harvest_clamp", None)
check("clamp: the runner exposes _harvest_clamp(node, r)", callable(clamp))
if callable(clamp):
    vnode = {"id": "plan", "type": "agent", "goal": "g", "schema": SCHEMA,
             "harvest_policy": "validate"}
    out = clamp(vnode, harvested())
    check("clamp validate: schema-valid cap harvest commits done",
          out.get("status") == "done" and out.get("output") == {"result": "census-complete"}, out)
    check("clamp validate: the turn-cap death stays visible on the record",
          out.get("error_class") == "cap_exhausted" and "turn budget" in (out.get("error") or "")
          and out.get("harvest") == {"declared_status": None}
          and (out.get("clamped") or {}).get("policy") == "validate"
          and (out.get("clamped") or {}).get("from_status") == "partial"
          and (out.get("clamped") or {}).get("error_class") == "cap_exhausted", out)
    for label, node, rec in (
            ("default (no key) stays partial", {k: v for k, v in vnode.items() if k != "harvest_policy"},
             harvested()),
            ("strict stays partial", {**vnode, "harvest_policy": "strict"}, harvested()),
            ("validate without a declared schema stays partial",
             {k: v for k, v in vnode.items() if k != "schema"}, harvested()),
            ("validate with declared Remaining steps stays partial", vnode,
             harvested(remaining=["reconcile the last two hosts"])),
            ("validate with a declared BLOCKED status stays partial", vnode,
             harvested(harvest={"declared_status": "BLOCKED"},
                       output={"result": "x", "status": "BLOCKED"})),
            ("validate over left_live_descendants stays partial (#61 terminal)", vnode,
             harvested("left_live_descendants")),
            ("validate with an output that fails the schema stays partial", vnode,
             harvested(output={"nope": 1})),
            ("validate never touches a failed record", vnode,
             {"status": "failed", "error_class": "cap_exhausted"})):
        snap = copy.deepcopy(rec)
        out = clamp(node, rec)
        check(f"clamp: {label}", out == snap, out)
    out = clamp(vnode, harvested(harvest={"declared_status": "done"},
                                 output={"result": "x", "status": "done"}))
    check("clamp validate: a declared done-ish status still clamps",
          out.get("status") == "done", out)

ve = wfcommon.validate_graph_errors
ok_nodes = [{"id": "a", "type": "agent", "goal": "g", "schema": SCHEMA,
             "harvest_policy": "validate"}]
check("door: harvest_policy 'validate' on an agent node is accepted", ve(ok_nodes) == [],
      ve(ok_nodes))
check("door: harvest_policy 'strict' on an agent node is accepted",
      ve([{**ok_nodes[0], "harvest_policy": "strict"}]) == [])
bad = ve([{**ok_nodes[0], "harvest_policy": "lenient"}])
check("door: an unknown harvest_policy value is refused, naming the key",
      bad and any("harvest_policy" in json.dumps(e) for e in bad), bad)
bad = ve([{**ok_nodes[0], "harvest_policy": True}])
check("door: a non-string harvest_policy is refused, naming the key",
      bad and any(e.get("field") == "harvest_policy" for e in bad), bad)
bad = ve([{"id": "a", "type": "agent", "goal": "g",
           "fanout": {"items": ["x"], "schema": SCHEMA}, "harvest_policy": "validate"}])
check("door: harvest_policy on a fan-out node is refused (aggregate owns harvest credit)",
      bad and any("harvest_policy" in json.dumps(e) for e in bad), bad)
bad = ve([{"id": "g", "type": "gate", "question": "q?", "options": ["y", "n"],
           "harvest_policy": "validate"}])
check("door: harvest_policy on a gate is refused, naming the key",
      bad and any(e.get("field") == "harvest_policy" for e in bad), bad)


# ---------------------------------------------------------------------------
# end-to-end: the real runner over the fake child
# ---------------------------------------------------------------------------
def mk(run_id, nodes):
    r = RUNS / run_id
    shutil.rmtree(r, ignore_errors=True)
    (r / "nodes").mkdir(parents=True)
    (r / "gates").mkdir()
    (r / "graph.json").write_text(json.dumps({"name": run_id, "nodes": nodes}))
    (r / "run.json").write_text(json.dumps({"hermes_bin": FAKE, "concurrency": 4,
                                            "node_timeout": 60}))
    return r


def wf(run_id, extra_env):
    env = dict(os.environ, HERMES_HOME=str(HOME), WF_RUNS_ROOT=str(RUNS),
               FAKE_LOG=str(HOME / "fake.log"), FAKE_MODE="cap_harvest_ggqs",
               FAKE_ATTEMPT_DIR=str(HOME / ("att-" + run_id)), **extra_env)
    return subprocess.run([sys.executable, str(ROOT / "wf.py"), "run", run_id], env=env,
                          capture_output=True, text=True, timeout=240).stdout.strip()


def rec(r, nid):
    try:
        return json.loads((r / "nodes" / f"{nid}.json").read_text())
    except FileNotFoundError:
        return {}


def spawns(r, nid):
    return len(list((r / "logs").glob(f"{nid}.a*.log")))


def events(r):
    try:
        return [json.loads(l) for l in (r / "events.jsonl").read_text().splitlines()]
    except FileNotFoundError:
        return []


def graph(plan_extra=None, verify_extra=None):
    return [{"id": "plan", "type": "agent", "goal": "CAPHARV census every host",
             "schema": SCHEMA, "max_turns": 35, **(plan_extra or {})},
            {"id": "verify", "type": "agent", "after": ["plan"], "goal": "VERIFY the census",
             "schema": SCHEMA, **(verify_extra or {})}]


# E1 (a) e2e: cap death + valid harvest + tool progress -> one re-drive -> done
r = mk("ggqs-e1", graph())
out = wf("ggqs-e1", {})
p, v = rec(r, "plan"), rec(r, "verify")
evs = events(r)
check("E1 (a) cap_exhausted harvest with progress: plan re-driven once and lands done",
      p.get("status") == "done" and (p.get("output") or {}).get("result") == "redriven-clean"
      and spawns(r, "plan") == 2, f"status={p.get('status')} spawns={spawns(r, 'plan')} rec={p}")
check("E1 (a) node.retry fired for the harvested cap_exhausted death",
      any(e.get("event") == "node.retry" and e.get("node") == "plan"
          and e.get("error_class") == "cap_exhausted" for e in evs),
      [e for e in evs if e.get("node") == "plan"][-4:])
check("E1 (a) turn cap visible in the committed ledger",
      [a.get("error_class") for a in p.get("attempts_log") or []] == ["cap_exhausted"]
      and (p.get("attempts_log") or [{}])[0].get("harvested") is True, p.get("attempts_log"))
check("E1 (a) the plain descendant is released (no blocked_by_partial_ancestor)",
      v.get("status") == "done" and out.startswith("WORKFLOW_DONE"), f"verify={v} out={out}")

# E2 (b) e2e: same death WITHOUT progress evidence -> unchanged fail-closed law
r = mk("ggqs-e2", graph())
out = wf("ggqs-e2", {"FAKE_NO_PROGRESS": "1"})
p, v = rec(r, "plan"), rec(r, "verify")
evs = events(r)
check("E2 (b) no progress: plan stays partial cap_exhausted with ONE spawn",
      p.get("status") == "partial" and p.get("error_class") == "cap_exhausted"
      and spawns(r, "plan") == 1 and not any(e.get("event") == "node.retry" for e in evs),
      f"rec={p} spawns={spawns(r, 'plan')}")
check("E2 (b) strict default: plain descendant still blocked typed, never spawns",
      v.get("status") == "failed" and v.get("error_class") == "precondition"
      and v.get("error") == "blocked_by_partial_ancestor: plan" and spawns(r, "verify") == 0
      and out.startswith("WORKFLOW_FAILED"), f"verify={v} out={out}")

# E3 clamp validate e2e: no progress -> no re-drive, harvest clamps to done,
# turn cap visible, plain descendant (with requires on it) releases
r = mk("ggqs-e3", graph({"harvest_policy": "validate"}, {"requires": {"plan": ["result"]}}))
out = wf("ggqs-e3", {"FAKE_NO_PROGRESS": "1"})
p, v = rec(r, "plan"), rec(r, "verify")
evs = events(r)
check("E3 validate: schema-valid cap harvest commits done with the harvested output",
      p.get("status") == "done" and (p.get("output") or {}).get("result") == "census-complete"
      and spawns(r, "plan") == 1, f"rec={p} out={out}")
check("E3 validate: turn cap stays visible on the record (error_class/error/harvest/clamped)",
      p.get("error_class") == "cap_exhausted" and "turn budget" in (p.get("error") or "")
      and isinstance(p.get("harvest"), dict)
      and (p.get("clamped") or {}).get("policy") == "validate", p)
check("E3 validate: node.finished carries the clamp + death class (metrics see the cap)",
      any(e.get("event") == "node.finished" and e.get("node") == "plan"
          and e.get("clamped") == "validate" and e.get("error_class") == "cap_exhausted"
          for e in evs), [e for e in evs if e.get("node") == "plan"][-3:])
check("E3 validate: plain descendant with requires releases and runs done",
      v.get("status") == "done" and spawns(r, "verify") == 1 and out.startswith("WORKFLOW_DONE"),
      f"verify={v} out={out}")
st = wfcommon.run_state(r)
check("E3 validate: read model agrees (plan done, run done)",
      st and st["nodes"]["plan"]["status"] == "done" and st["status"] == "done",
      st and {k: st[k] for k in ("status",)})

# E4 clamp refuses a self-declared unfinished harvest (## Remaining) -> blocked
r = mk("ggqs-e4", graph({"harvest_policy": "validate"}))
out = wf("ggqs-e4", {"FAKE_NO_PROGRESS": "1", "FAKE_REMAINING": "1"})
p, v = rec(r, "plan"), rec(r, "verify")
check("E4 validate + declared Remaining: stays partial, descendant blocked typed",
      p.get("status") == "partial" and p.get("remaining") == ["reconcile the last two hosts"]
      and v.get("error") == "blocked_by_partial_ancestor: plan", f"plan={p} verify={v}")

# E5 re-drive dies answer-less: the harvest is kept (partial), ledger shows both deaths
r = mk("ggqs-e5", graph())
out = wf("ggqs-e5", {"FAKE_REDRIVE_DIES": "1"})
p = rec(r, "plan")
check("E5 answer-less re-drive keeps the harvest partial; 2 spawns; ledger names both",
      p.get("status") == "partial" and (p.get("output") or {}).get("result") == "census-complete"
      and spawns(r, "plan") == 2
      and [a.get("error_class") for a in p.get("attempts_log") or []]
      == ["cap_exhausted", "transport"], f"rec={p}")

print(f"{checks - failures}/{checks} checks passed")
print("ALL PASS" if failures == 0 else "SOME FAILED")
sys.exit(0 if failures == 0 else 1)
