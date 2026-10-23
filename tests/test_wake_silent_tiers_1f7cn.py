#!/usr/bin/env python3
"""WF-GRAFT-1 (est-1f7cn): wake-noise control — honor the Hermes `[SILENT]` verdict
and the per-node `wake_tier` ladder, at the ONE seam that wakes the owner.

The failure mode this pins: every scheduled graph lap that finds nothing still costs
the owner a full agent turn (`gate.held`/`run.done` always push), and the only typed
suppression in the grammar was `fanout.stragglers`. A watcher graph therefore had to
be deleted to be quiet. Adapted from itechmeat/hermes-workflows
(notifications.py:29-45, engine.py:934-936 — suppress = success, never retried;
schema/nodes.ts:100-112 — per-node noise tier over the board default).

Law pinned here (in-process notify over crafted run dirs; the sink counts POSTs):
  * a `run.done` whose DECLARED verdict output (top-level `result:` node, efp-valid)
    carries `[SILENT]` POSTs nothing and records exactly ONE row typed
    `suppressed:"silent_marker"` with `delivered:false`;
  * RECORDED ONCE, NEVER RETRIED: re-driving the same instance adds zero rows and
    zero POSTs (a suppression is a decision, not a transport failure);
  * no marker, no `result` declaration, a STALE record, or an output with no commit
    all WAKE (fail-open: a suppression needs a grounded committed fact);
  * the marker is honored on a structured verdict too (read whole), case-sensitively;
  * lifecycle notices are UNAFFECTED: the same [SILENT] run still wakes on run.failed
    and on a gate hold;
  * `wake_tier` (graph default, per-node override, closed set full|quiet|silent):
    `quiet` records a fan-out's straggler observation without waking, `silent` also
    records a gate's own hold; per-node overrides the default in BOTH directions;
  * the run boundary is never tiered: `wake_tier:"silent"` cannot swallow run.done;
  * a malformed tier (door-refused shape) reads as `full` — over-wake, never swallow;
  * the operator mirror still fires for a suppressed instance (it is the operator's
    endpoint, not an owner turn);
  * MUTATION PROOF: with the suppression hook neutered the matrix goes red — the
    pins above bite, they are not theater.

RED on ddc30ec (base): notify() takes no `node` kwarg and has no suppression seam,
so every `suppressed` row assertion and the zero-POST assertions below fail.
"""
import hashlib
import json
import os
import shutil
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
import wfcommon                      # noqa: E402  (efp stamps, crafted like the door's)
import wf                            # noqa: E402

checks = 0
failures = 0


def check(label, cond, detail=""):
    global checks, failures
    checks += 1
    if cond:
        print(f"PASS {label}")
    else:
        failures += 1
        print(f"FAIL {label}: {detail}")


tmp = Path(tempfile.mkdtemp(prefix="wf-graft1-"))
OWNER = {"session_id": "graft1-session", "ui_session_id": "graft1-ui",
         "platform": "api_server"}

# ---------- the sink: one POST = one owner turn ----------
posts = []


class _Sink(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        posts.append((self.path, self.rfile.read(n)))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args, **kwargs):
        pass


_srv = ThreadingHTTPServer(("127.0.0.1", 0), _Sink)
threading.Thread(target=_srv.serve_forever, daemon=True).start()
SINK_PORT = _srv.server_address[1]

for _k in ("WF_RUNS_ROOT", "API_SERVER_KEY", "API_SERVER_HOST", "API_SERVER_PORT",
           "WF_WAKE_SINK_PORT"):
    os.environ.pop(_k, None)
os.environ["WF_WAKE_SINK_PORT"] = str(SINK_PORT)


# ---------- the operator mirror: a fake bridge recording every mirrored transition
class _FakeOb:
    def __init__(self):
        self.mirrored = []

    def bind(self, *a):
        pass

    def instance_id(self, run_name, event, disc):
        return hashlib.sha256(f"{run_name}\0{event}\0{disc}".encode()).hexdigest()[:32]

    def mirror(self, run, event, key, graph):
        self.mirrored.append((run.name, event))


_ob = _FakeOb()
wf._OUTBOUND = _ob            # pre-set + TRIED: notify() mirrors through the fake
wf._OUTBOUND_TRIED = True


def mk_run(run_id, graph, records):
    """A run dir with a crafted graph + committed node records under the door's own
    efp law (records: {node_id: {status, output}}; `_stale: True` simulates a record
    an amend invalidated)."""
    r = tmp / run_id
    (r / "nodes").mkdir(parents=True)
    (r / "gates").mkdir()
    (r / "graph.json").write_text(json.dumps(graph))
    (r / "run.json").write_text(json.dumps(
        {"hermes_bin": "fake", "concurrency": 1, "node_timeout": 60, "owner": OWNER}))
    nodes = graph.get("nodes") or []
    byid = {n["id"]: n for n in nodes}
    for nid, rec in records.items():
        rec = dict(rec)
        stale = rec.pop("_stale", False)
        stamp = wfcommon.efp(byid, byid[nid])
        if stale:
            last = stamp[-1]
            stamp = stamp[:-1] + ("e" if last != "e" else "f")
        rec["efp"] = stamp
        rec["fp_rule_version"] = wfcommon.FP_RULE_VERSION
        (r / "nodes" / f"{nid}.json").write_text(json.dumps(rec))
    return r


def wake(r, event, key=None, graph=None, node=None):
    """One notify() attempt → (wake.jsonl rows, POSTs made by this attempt)."""
    before = len(posts)
    wf.notify(r, event, key=key, graph=graph, node=node)
    p = r / "wake.jsonl"
    try:
        rows = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    except FileNotFoundError:
        rows = []
    return rows, len(posts) - before


def agent(nid, **extra):
    return dict({"id": nid, "type": "agent", "goal": "do it"}, **extra)


def gate(nid, **extra):
    return dict({"id": nid, "type": "gate", "question": "ship?"}, **extra)


# ---------- 1. [SILENT] suppresses the run-result wake: one row, zero POSTs -------
r1 = mk_run("graft1-silent", {"name": "watcher", "result": "verdict",
                              "nodes": [agent("watch"), agent("verdict")]},
            {"verdict": {"status": "done", "output": "nothing to report [SILENT]"}})
rows, n = wake(r1, "run.done", graph={"nodes": [agent("watch"), agent("verdict")]})
check("S1 zero POSTs on a [SILENT] verdict", n == 0, f"{n} POSTs")
check("S1 one row recorded", len(rows) == 1, rows)
check("S1 row typed suppressed=silent_marker",
      rows and rows[0].get("suppressed") == "silent_marker",
      str(rows))
check("S1 row is delivered:false (never a delivery claim)",
      rows and rows[0].get("delivered") is False,
      str(rows))
check("S1 row carries the runner-authored text (legible, not a hole)",
      rows and "runner-authored/v1" in rows[0].get("text", ""),
      str(rows))
check("S1 the mirror still fired", ("graft1-silent", "run.done") in _ob.mirrored,
      str(_ob.mirrored))

# ---------- 2. recorded once, never retried (re-drive + respawn of one decision) --
rows2, n2 = wake(r1, "run.done")
rows3, n3 = wake(r1, "run.done")
check("S2 re-drive adds no row", len(rows2) == 1 and len(rows3) == 1,
      f"{len(rows2)}/{len(rows3)} rows")
check("S2 re-drive adds no POST", n2 == 0 and n3 == 0, f"{n2}/{n3} POSTs")

# ---------- 3. fail-open: the suppression needs a grounded committed fact ---------
r2 = mk_run("graft1-loud", {"name": "w2", "result": "verdict",
                            "nodes": [agent("verdict")]},
            {"verdict": {"status": "done", "output": "here is the report"}})
rows, n = wake(r2, "run.done")
check("F1 a verdict WITHOUT the marker wakes (delivered row)",
      n == 1 and rows and rows[-1].get("delivered") is True, f"{n} POSTs {rows}")

r3 = mk_run("graft1-noresult", {"name": "w3", "nodes": [agent("verdict")]},
            {"verdict": {"status": "done", "output": "[SILENT]"}})
rows, n = wake(r3, "run.done")
check("F2 no `result` declaration => the marker is NOT honored (wake anyway)",
      n == 1 and rows and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

r4 = mk_run("graft1-stale", {"name": "w4", "result": "verdict", "nodes": [agent("verdict")]},
            {"verdict": {"status": "done", "output": "[SILENT]", "_stale": True}})
rows, n = wake(r4, "run.done")
check("F3 a STALE record (amend-invalidated) cannot suppress (wake anyway)",
      n == 1 and rows and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

r5 = mk_run("graft1-missing", {"name": "w5", "result": "verdict",
                               "nodes": [agent("verdict")]}, {})
rows, n = wake(r5, "run.done")
check("F4 no committed output at all => wake anyway (fail-open)",
      n == 1 and rows and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

r6 = mk_run("graft1-structured", {"name": "w6", "result": "verdict",
                                  "nodes": [agent("verdict")]},
            {"verdict": {"status": "done",
                         "output": {"summary": "no drift", "note": "[SILENT]"}}})
rows, n = wake(r6, "run.done")
check("F5 a structured verdict is read whole (marker in any field counts)",
      n == 0 and rows and rows[0].get("suppressed") == "silent_marker",
      f"{n} POSTs {rows}")

r7 = mk_run("graft1-case", {"name": "w7", "result": "verdict", "nodes": [agent("verdict")]},
            {"verdict": {"status": "done", "output": "quiet [silent] today"}})
rows, n = wake(r7, "run.done")
check("F6 the marker is case-sensitive (host convention)",
      n == 1 and rows and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

# ---------- 4. lifecycle notices are unaffected by a [SILENT] verdict -------------
rows, n = wake(r1, "run.failed", key="crash:g1",
               graph={"nodes": [agent("watch"), agent("verdict")]})
check("L1 run.failed wakes despite a [SILENT] verdict",
      n == 1 and rows and rows[-1].get("event") == "run.failed"
      and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")
rows, n = wake(r1, "run.partial", key="budget cap")
check("L2 run.partial wakes despite a [SILENT] verdict",
      n == 1 and rows and rows[-1].get("event") == "run.partial"
      and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

rg = mk_run("graft1-gate", {"name": "w8", "result": "verdict",
                            "nodes": [agent("verdict"), gate("ship")]},
            {"verdict": {"status": "done", "output": "[SILENT]"}})
rows, n = wake(rg, "gate.held", key="ship:def1", node="ship")
check("L3 a gate hold wakes despite a [SILENT] verdict",
      n == 1 and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

# ---------- 5. wake tiers: node-subject notices, graph default over by node --------
_fan = agent("lanes", fanout={"items": ["a"], "goal": "x {item}"})
rp = mk_run("graft1-quiet-graph", {"name": "w9", "wake_tier": "quiet",
                                   "nodes": [_fan, gate("ship")]},
            {"lanes": {"status": "done", "output": {}}})
rows, n = wake(rp, "fanout.stragglers", key="lanes:1", node="lanes")
check("T1 graph default quiet: stragglers record, no POST",
      n == 0 and len(rows) == 1 and rows[0].get("suppressed") == "wake_tier:quiet",
      f"{n} POSTs {rows}")
rows, n = wake(rp, "gate.held", key="ship:def1", node="ship")
check("T2 quiet does NOT silence a gate hold (a hold asks for an action)",
      n == 1 and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")
rows, n = wake(rp, "fanout.stragglers", key="lanes:1", node="lanes")
check("T2b the recorded suppression is once-only under a re-fire",
      len(rows) == 2 and rows[1].get("event") == "gate.held",
      str(rows))

rq = mk_run("graft1-node-override-loud",
            {"name": "w10", "wake_tier": "quiet", "nodes": [dict(_fan, wake_tier="full")]},
            {"lanes": {"status": "done", "output": {}}})
rows, n = wake(rq, "fanout.stragglers", key="lanes:1", node="lanes")
check("T3 per-node full overrides a graph quiet (wake)",
      n == 1 and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

rs = mk_run("graft1-node-override-quiet",
            {"name": "w11", "nodes": [dict(_fan, wake_tier="silent")]},
            {"lanes": {"status": "done", "output": {}}})
rows, n = wake(rs, "fanout.stragglers", key="lanes:1", node="lanes")
check("T4 per-node silent over an unset graph default",
      n == 0 and rows[0].get("suppressed") == "wake_tier:silent", f"{n} POSTs {rows}")
rows, n = wake(rs, "gate.held", key="lanes:1", node="lanes")
check("T4b silent on a node silences a hold-shaped notice too, recorded once",
      n == 0 and rows[-1].get("suppressed") == "wake_tier:silent", f"{n} POSTs {rows}")

rs2 = mk_run("graft1-silent-gate",
             {"name": "w12", "wake_tier": "silent", "nodes": [gate("ship")]},
             {})
rows, n = wake(rs2, "gate.held", key="ship:def1", node="ship")
check("T5 graph-default silent records the hold instead of waking",
      n == 0 and len(rows) == 1 and rows[0].get("suppressed") == "wake_tier:silent",
      f"{n} POSTs {rows}")

rows, n = wake(rs2, "run.done")
check("T6 the run boundary is NEVER tiered: silent cannot swallow run.done",
      n == 1 and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

rb = mk_run("graft1-badtier", {"name": "w13", "wake_tier": "LOUD",
                               "nodes": [dict(_fan, wake_tier=None)]},
            {"lanes": {"status": "done", "output": {}}})
rows, n = wake(rb, "fanout.stragglers", key="lanes:1", node="lanes")
check("T7 malformed tiers fail OPEN to waking (door-refused shape, runner-safe)",
      n == 1 and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

rno = mk_run("graft1-nocarry", {"name": "w14", "wake_tier": "silent",
                                "nodes": [_fan]},
             {"lanes": {"status": "done", "output": {}}})
rows, n = wake(rno, "fanout.stragglers", key="lanes:1")   # no node= => nothing to tier
check("T8 a call site that names no subject node is never tiered (wake)",
      n == 1 and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")

# ---------- 6. door law: closed value set, both levels, echo refused --------------
check("D1 the door refuses a bad top-level tier naming the set",
      any(e["field"] == "wake_tier" and "full" in e["msg"]
          for e in wfcommon.structural_graph_errors(
              {"name": "x", "wake_tier": "loud", "nodes": []})))
check("D2 the door refuses a bad per-node tier naming the set",
      any(e["field"] == "wake_tier" for e in wfcommon.validate_graph_errors(
          [dict(agent("a"), wake_tier="LOUD")])))
check("D3 echo nodes have no tier to set (closed key set names the key)",
      any(e["field"] == "wake_tier" and "unknown key" in e["msg"]
          for e in wfcommon.validate_graph_errors(
              [{"id": "e", "type": "echo", "after": [], "output": {},
                "wake_tier": "quiet"}])))
check("D4 the tier values are a closed three-rung ladder",
      wfcommon.WAKE_TIERS == ("full", "quiet", "silent"), str(wfcommon.WAKE_TIERS))

# ---------- 7. MUTATION PROOF: neuter the law and the matrix must go red ----------
_orig = wf._wake_suppression
wf._wake_suppression = lambda run, event, node, graph: None
try:
    rm = mk_run("graft1-mutation", {"name": "w15", "wake_tier": "silent",
                                    "result": "verdict", "nodes": [agent("verdict")]},
                {"verdict": {"status": "done", "output": "[SILENT]"}})
    rows, n = wake(rm, "run.done")
    check("M1 with the suppression hook removed the same wake POSTS (pin bites)",
          n == 1 and not rows[-1].get("suppressed"), f"{n} POSTs {rows}")
    rows, n = wake(rm, "gate.held", key="ship:def1", node="ship")
    check("M2 and a silent-tier hold POSTS (pin bites on tiers too)",
          n == 1, f"{n} POSTs")
finally:
    wf._wake_suppression = _orig

_srv.shutdown()
shutil.rmtree(tmp, ignore_errors=True)
print(f"\nTOTAL {checks} FAILURES {failures}")
sys.exit(1 if failures else 0)
