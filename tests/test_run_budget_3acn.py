#!/usr/bin/env python3
"""est-3acn — RunBudget: a tokens-only cap refused BEFORE spawn, closing partial.

The on-row design (comments 2026-10-13/14, issue #124): the cap is TOKENS-ONLY.
USD is EXCLUDED per the repo cost-trust law — an author who writes budget.usd
is REFUSED at admission, never silently ignored (a dropped cap key is a cap the
author believes is on). Five pinned legs:

  A  REFUSAL BEFORE SPAWN — a node under `budget:{tokens:N}` whose measured
     spend (+ its per-attempt mean) reaches the cap refuses at the run_child
     seam with typed error_class=budget_exhausted, and the seat stub logs ZERO
     further attempts (never billed, never post-hoc). The FIRST attempt under a
     cap always rides (est_next floors at 0 — no measurement, no refusal).
  B  EVENT SHAPE — `dispatch.refused` carries exactly
     {node, index?, scope, cap, spent, needed, error_class:budget_exhausted};
     solo omits index, fan-out items carry it; the ERROR_CLASSES closed set
     (and the AGENTS.md §3d pipe-list via the sibling doc-sync pin) covers it.
  C  PARTIAL-WITH-HARVEST — a capped fan-out commits `partial` WITH the
     surviving items' outputs (partial IS committed output, #4 law), the dead
     items carry the class, and the run_state read view derives spent-vs-cap
     from the SAME child_metrics join, with the budget_ledger.json
     never_launched list surfacing on a capped-out run.
  D  NO-CAP BYTE STABILITY — a graph/run with no budget validates clean, the
     run_state payload carries NO budget/error keys and its key set is the
     pre-budget golden set verbatim, `budget` never enters the node efp
     (policy keys are excluded from def-hash, same law as run_budget), and the
     door bakes meta.budget only when the graph declared one.
  E  TWO-CAP CLOSURES (est-y938s) — the terminal budget hook is
     MERGE-ON-WRITE: a run capped first at a fan-out and re-expressed at a
     downstream node keeps its FIRST cause at the top level (cause/at name
     the first closure), every distinct closure rides the occurrences[]
     chain in close order, a pre-fix legacy snapshot row is absorbed as the
     chain's first occurrence, and a benign resume re-closing the SAME set
     mints no duplicate row.

Run: env -u WF_RUNS_ROOT -u HERMES_HOME PYTHONPATH=/opt/hermes
     /opt/hermes/.venv/bin/python tests/test_run_budget_3acn.py
"""
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import threading
import types
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import wf            # noqa: E402  (the runner module — loads wfcommon itself)
import wfcommon     # noqa: E402

ok = True
def check(label, cond, detail=""):
    global ok
    print(("PASS " if cond else "FAIL " + label + ("  " + str(detail)[:400] if detail else "")))
    ok = ok and bool(cond)

TMP = Path(tempfile.mkdtemp(prefix="wf-3acn-"))
os.environ["HERMES_HOME"] = str(TMP / "home")
os.environ["WF_RUNS_ROOT"] = str(TMP / "home" / "workflows")
# est-3acn re-roll 2026-10-16: main's #439 bounds WORKFLOW_MAX_SEATS to [4,16];
# a wide ambient lane value must never fail-close this pin (env-insensitive law).
if os.environ.get("WORKFLOW_MAX_SEATS") not in (None, "0") and \
        not (4 <= int(os.environ.get("WORKFLOW_MAX_SEATS", "8")) <= 16):
    os.environ["WORKFLOW_MAX_SEATS"] = "16"

# The recording stub: every spawn appends its --continue key to the attempt
# ledger AND bills WF3_IN/WF3_OUT tokens into state.db under that session title
# (the stub IS the meter — the refusal check must read the same join).
STUB = TMP / "hermes_stub.py"
STUB.write_text(
    "#!/usr/bin/env python3\n"
    "import sys, os, sqlite3\n"
    "a = sys.argv[1:]\n"
    "title = a[a.index('--continue') + 1] if '--continue' in a else ''\n"
    "with open(os.environ['WF3_LEDGER'], 'a') as f:\n"
    "    f.write(title + '\\n')\n"
    "db = os.path.join(os.environ['HERMES_HOME'], 'state.db')\n"
    "c = sqlite3.connect(db)\n"
    "c.execute('CREATE TABLE IF NOT EXISTS sessions (title TEXT, model TEXT, '\n"
    "          'input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, '\n"
    "          'reasoning_tokens INTEGER, api_call_count INTEGER, tool_call_count INTEGER, '\n"
    "          'estimated_cost_usd REAL, last_activity_at TEXT, last_activity_description TEXT, '\n"
    "          'ended_at TEXT, started_at TEXT)')\n"
    "c.execute('INSERT INTO sessions (title, model, input_tokens, output_tokens, '\n"
    "          'api_call_count, tool_call_count, last_activity_at, started_at) '\n"
    "          'VALUES (?, ?, ?, ?, 1, 1, ?, ?)',\n"
    "          (title, 'test-model', int(os.environ.get('WF3_IN', 600)),\n"
    "           int(os.environ.get('WF3_OUT', 400)),\n"
    "           '2026-10-15T12:00:00', '2026-10-15T11:00:00'))\n"
    "c.commit(); c.close()\n"
    "print('```json\\n{\\\"result\\\": \\\"ok\\\"}\\n```')\n"
)
STUB.chmod(0o755)
LEDGER = TMP / "attempts.ledger"
os.environ["WF3_LEDGER"] = str(LEDGER)


def pops():
    try:
        return len(LEDGER.read_text().splitlines())
    except OSError:
        return 0


def mkrun(rid):
    r = Path(os.environ["WF_RUNS_ROOT"]) / rid
    (r / "nodes").mkdir(parents=True)
    (r / "gates").mkdir()
    return r


def base_meta(run):
    return {"_run": run, "hermes_bin": str(STUB), "_spawn_n": {},
            "_procs_lock": threading.Lock(), "_procs": {},
            "_stop": threading.Event(), "node_timeout": 15}


def events(r):
    try:
        return [json.loads(l) for l in (r / "events.jsonl").read_text().splitlines()]
    except (OSError, ValueError):
        return []


# ---- LEG A: refusal BEFORE spawn (typed, zero-attempt), first attempt rides --
runA = mkrun("r3acn-a")
nodeA = {"id": "work", "type": "agent", "goal": "do it", "budget": {"tokens": 1000}}
byidA = {"work": nodeA}
metaA = base_meta(runA)
# First dispatch: no state.db at all -> honest spend 0, est_next 0 -> RIDES
# (a cap never blocks the first attempt; honest absence can never fire the gate).
rA1 = wf.run_child(metaA, nodeA, byidA, "do it", "", None, skey="wf:r3acn-a:work")
check("A1 first spawn under a cap rides (no history, est floors at 0)",
      rA1.get("status") == "done" and pops() == 1, rA1)
# The stub billed 600+400 = 1000 into the meter. Spent 1000 + est(1000/1) >= 1000
# -> the next dispatch of the SAME lane must refuse BEFORE the seat is touched.
rA2 = wf.run_child(metaA, dict(nodeA), byidA, "do it", "", None, skey="wf:r3acn-a:work")
check("A2 capped lane: refused BEFORE submit, typed error_class=budget_exhausted",
      rA2.get("status") == "failed" and rA2.get("error_class") == "budget_exhausted", rA2)
check("A2 refusal bills ZERO attempts (the seat ledger is the truth)",
      pops() == 1, f"pops={pops()}")
check("A2 the error names the cap, the scope, and the cost-trust exclusion",
      "1000" in (rA2.get("error") or "") and "node-scope" in (rA2.get("error") or "")
      and "USD" in (rA2.get("error") or ""), (rA2.get("error") or "")[:200])
check("A3 budget_exhausted is in the closed ERROR_CLASSES set",
      "budget_exhausted" in wf.ERROR_CLASSES)

# ---- LEG B: dispatch.refused event shape -------------------------------------
dr = [e for e in events(runA) if e.get("event") == "dispatch.refused"]
check("B1 the refusal event is durable (dispatch.refused in events.jsonl)",
      len(dr) == 1, dr)
ev = dr[0] if dr else {}
check("B1 the event carries the pinned field set",
      ev.get("node") == "work" and ev.get("scope") == "node" and ev.get("cap") == 1000
      and ev.get("spent") == 1000 and ev.get("needed") == 1000
      and ev.get("error_class") == "budget_exhausted", ev)
check("B2 a solo refusal OMITS index (index? is optional, not null-carried)",
      "index" not in ev, ev)

# ---- LEG C: partial-with-harvest at the fan-out merge + the read view --------
runC = mkrun("r3acn-c")
nodeC = {"id": "fo", "type": "agent", "goal": "item {INDEX}",
         "budget": {"tokens": 1000},
         "fanout": {"items": [{"n": 0}, {"n": 1}, {"n": 2}]}}
byidC = {"fo": nodeC}
metaC = base_meta(runC)
metaC["item_concurrency"] = 1          # sequential items -> deterministic boundary
(runC / "graph.json").write_text(json.dumps({"name": "c", "nodes": [nodeC]}))
wf.run_agent_node(runC, metaC, byidC, nodeC, {}, [])
popsC = pops()
recC = json.loads((runC / "nodes" / "fo.json").read_text())
check("C1 capped fan-out WITH harvest commits partial (typed), never quorum",
      recC.get("status") == "partial" and recC.get("error_class") == "budget_exhausted",
      f"status={recC.get('status')} class={recC.get('error_class')}")
check("C1 the harvest survives: one item output committed, all three results kept",
      len((recC.get("output") or {}).get("items") or []) == 1
      and len((recC.get("output") or {}).get("all_results") or []) == 3,
      {k: len(v) if isinstance(v, list) else v for k, v in (recC.get("output") or {}).items()})
check("C2 only ONE item ever billed (the rest refused before the seat)",
      popsC == 2, f"pops={popsC}")     # 1 from leg A + 1 here
drc = [e for e in events(runC) if e.get("event") == "dispatch.refused"]
check("C3 fan-out refusals carry index (1 and 2)",
      sorted(e.get("index") for e in drc) == [1, 2], [e.get("index") for e in drc])
check("C4 the aggregate event names the class on the partial close",
      any(e.get("event") == "node.finished" and e.get("harvested") is True
          and e.get("error_class") == "budget_exhausted" for e in events(runC)))
# The read view: door-baked run cap in run.json + the runner-written ledger.
(runC / "run.json").write_text(json.dumps({"name": "c", "budget": {"tokens": 1000}}))
(runC / "budget_ledger.json").write_text(json.dumps(
    {"cause": "budget_exhausted", "at": "2026-10-15T12:00:00+00:00",
     "exhausted_nodes": ["fo"], "never_launched": []}))
(runC / "events.jsonl").open("a").write(
    json.dumps({"event": "run.partial", "ts": "2026-10-15T12:00:01+00:00"}) + "\n")
stC = wfcommon.run_state(runC)
check("C5 the derived budget view names cap and per-node spent from the meters",
      (stC.get("budget") or {}).get("cap_tokens") == 1000
      and ((stC.get("budget") or {}).get("per_node") or {}).get("fo", {}).get("spent_tokens") == 1000,
      stC.get("budget"))
check("C5 the node-level cap rides the view too",
      ((stC.get("budget") or {}).get("per_node") or {}).get("fo", {}).get("cap_tokens") == 1000,
      stC.get("budget"))
check("C6 run.partial + ledger: status partial, error budget_exhausted, never_launched surfaced",
      stC.get("status") == "partial" and stC.get("error") == "budget_exhausted"
      and (stC.get("budget") or {}).get("never_launched") == [],
      {"status": stC.get("status"), "error": stC.get("error")})

# ---- LEG D: no-cap byte stability + admission law ----------------------------
nodePlain = {"id": "a", "type": "agent", "goal": "g"}
byidD = {"a": nodePlain}
check("D1 a no-cap graph validates clean at BOTH validators",
      wfcommon.validate_graph_errors([nodePlain]) == []
      and list(wfcommon.structural_graph_errors({"name": "d", "nodes": [nodePlain]})) == [])
# USD is EXCLUDED: an author who writes it is refused, never ignored.
usd_root = list(wfcommon.structural_graph_errors(
    {"name": "d", "nodes": [nodePlain], "budget": {"usd": 5}}))
check("D2 root budget.usd is REFUSED (tokens-only, cost-trust law)",
      len(usd_root) == 1 and "tokens-only" in usd_root[0]["msg"], usd_root)
usd_node = wfcommon.validate_graph_errors([dict(nodePlain, budget={"usd": 5})])
check("D2 node budget.usd is REFUSED too",
      len(usd_node) == 1 and "cost-trust" in usd_node[0]["msg"], usd_node)
bad = wfcommon.validate_graph_errors([dict(nodePlain, budget={"tokens": 0})])
badf = wfcommon.validate_graph_errors([dict(nodePlain, budget={"tokens": 1.5})])
check("D2 budget.tokens must be a POSITIVE INTEGER (0 and 1.5 refused)",
      len(bad) == 1 and len(badf) == 1, bad + badf)
check("D2 budget and its STRUCTURAL/GRAPH key gates are wired",
      "budget" in wfcommon.STRUCTURAL_GRAPH_KEYS
      and "budget" in wfcommon.AGENT_KEYS)
# efp law: budget is policy, not work — excluded from def-hash like run_budget,
# so raising a cap NEVER un-freezes committed nodes (byte-identical work).
e1 = wfcommon.efp(byidD, nodePlain)
e2 = wfcommon.efp(byidD, dict(nodePlain, budget={"tokens": 999}))
e3 = wfcommon.efp(byidD, dict(nodePlain, run_budget={"seconds": 999}))
check("D3 node efp ignores the budget key (policy key, same law as run_budget)",
      e1 == e2 == e3, (e1, e2, e3))
# Door: bake + terminal-set + read-view absence, via the plugin module itself.
spec = importlib.util.spec_from_file_location("wf_door_3acn", ROOT / "__init__.py")
door = importlib.util.module_from_spec(spec)
spec.loader.exec_module(door)
# #71 machine law (test_shelf_isolation_71 S3): the env pin alone is NOT a sandbox —
# settings.runs_root outranks WF_RUNS_ROOT in the resolver, so every in-process door
# writer also installs the resolver-level pin beside it.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import wf_test_isolation as _iso71
_iso71.install(door)


class Owner:
    def get_config(self, key, default=None):
        return default


class FakeCtx(Owner):
    pass


import contextlib  # noqa: E402

@contextlib.contextmanager
def door_env():
    home = TMP / "doorhome"
    home.mkdir(exist_ok=True)
    old = os.environ.copy()
    os.environ["WF_RUNS_ROOT"] = str(home / "workflows")
    os.environ["HERMES_HOME"] = str(home)
    # est-3acn re-roll 2026-10-16: main's #439 seat-cap validator bounds
    # WORKFLOW_MAX_SEATS to [4,16]; a wide ambient lane value (64) must never
    # bake-lock the door — the pin is environment-insensitive.
    os.environ["WORKFLOW_MAX_SEATS"] = "16"
    door._CTX = FakeCtx()
    spawned = []
    real = door._spawn_runner
    door._spawn_runner = lambda *a, **k: spawned.append(a)
    try:
        yield spawned
    finally:
        door._spawn_runner = real
        os.environ.clear(); os.environ.update(old)

with door_env() as spawned:
    res_plain = door.act_run({"graph": {"name": "plain3acn",
                                        "nodes": [{"id": "a", "type": "echo", "output": 1}]}})
    check("D4 door accepts a no-budget graph", "run_id" in res_plain, res_plain)
    rj_plain = json.loads((Path(os.environ["WF_RUNS_ROOT"]) / res_plain["run_id"] / "run.json").read_text()) \
        if "run_id" in res_plain else {}
    check("D4 no-budget run.json carries NO budget key (byte-stable meta)",
          "budget" not in rj_plain, sorted(rj_plain))
    res_usd = door.act_run({"graph": {"name": "usd3acn", "budget": {"usd": 9},
                                      "nodes": [{"id": "a", "type": "echo", "output": 1}]}})
    check("D5 door REFUSES budget.usd at admission (loud, named)",
          "error" in res_usd and "tokens" in str(res_usd), res_usd)
    res_cap = door.act_run({"graph": {"name": "cap3acn", "budget": {"tokens": 123456},
                                      "nodes": [{"id": "a", "type": "echo", "output": 1}]}})
    check("D6 door bakes the run cap beside concurrency (meta.budget.tokens)",
          "run_id" in res_cap and json.loads(
              (Path(os.environ["WF_RUNS_ROOT"]) / res_cap["run_id"] / "run.json").read_text())
          .get("budget") == {"tokens": 123456}, res_cap)
    # The no-cap read model: run_state payload key set is the pre-budget golden set.
    if "run_id" in res_plain:
        stD = wfcommon.run_state(Path(os.environ["WF_RUNS_ROOT"]) / res_plain["run_id"]) or {}
        # The graph ran nothing (spawn stubbed); the ledger law is the KEY SET:
        # no budget, no error, exactly the pre-budget surface.
        check("D7 no-cap run_state carries NO budget/error key (golden key set verbatim)",
              "budget" not in stD and "error" not in stD
              and set(stD) == {"run_id", "name", "status", "accounting", "failed_nodes",
                               "held_gate", "nodes", "graph", "runner_live", "runner_exit", "runner_lock",
                               "started", "owner", "done", "skipped", "total"}, sorted(stD))

# ---- LEG E: est-y938s — TWO cap closures, merge-on-write ledger --------------
# The defect (P3 from the PR #529 review): the terminal hook OVERWROTE
# budget_ledger.json at every closure, so a run capped first at a fan-out and
# re-expressed downstream persisted only the LAST cause. The re-expression is
# reachable in the real flow: a capped-with-harvest fan-out whose boundary-gate
# DEMOTE (est-ps8i9) forfeits the graceful closure leaves a record classed by
# the GATE, so the hook's re-scan arrives with a NARROWER set — an overwrite
# loses the first cause by construction.
runE = mkrun("r3acn-e")
close_fn = getattr(wf, "close_budget_ledger", None)
check("E0 the terminal hook is a named closer the run can re-drive (est-y938s)",
      callable(close_fn))
if callable(close_fn):
    # Occurrence 1: a REAL capped fan-out WITH harvest (leg C machinery: item 0
    # rides and bills 1000, item 1 refuses pre-dispatch).
    nodeE1 = {"id": "fo", "type": "agent", "goal": "item {INDEX}",
              "budget": {"tokens": 1000},
              "fanout": {"items": [{"n": 0}, {"n": 1}]}}
    nodeE2 = {"id": "synth", "type": "agent", "goal": "g"}
    byidE = {"fo": nodeE1, "synth": nodeE2}
    metaE = base_meta(runE)
    metaE["item_concurrency"] = 1
    metaE["budget"] = {"tokens": 1000}              # door-baked run-scope shape
    (runE / "graph.json").write_text(json.dumps({"name": "e", "nodes": [nodeE1, nodeE2]}))
    popsE0 = pops()
    wf.run_agent_node(runE, metaE, byidE, nodeE1, {}, [])
    recE = json.loads((runE / "nodes" / "fo.json").read_text())
    check("E1 occurrence 1 is the real cap: fan-out partial + budget class",
          recE.get("status") == "partial"
          and recE.get("error_class") == "budget_exhausted",
          f"status={recE.get('status')} class={recE.get('error_class')}")
    led1 = close_fn(runE, metaE, ["fo"], ["synth"])           # the hook, closure 1
    check("E1 the first closure writes cause+nodes+never and a one-row chain",
          led1.get("cause") == "budget_exhausted"
          and led1.get("exhausted_nodes") == ["fo"]
          and led1.get("never_launched") == ["synth"]
          and led1.get("cap_tokens") == 1000
          and led1.get("occurrences") == [{"at": led1.get("at"),
                                           "exhausted_nodes": ["fo"],
                                           "never_launched": ["synth"]}], led1)
    led1_at = led1.get("at")
    # The est-ps8i9 demote, replayed on the record: the gate's verdict replaces
    # the budget class, so the hook's re-scan no longer sees `fo`.
    (runE / "nodes" / "fo.json").write_text(json.dumps(
        {**recE, "status": "failed", "error_class": "effect_receipt"}))
    # Occurrence 2: the downstream node re-expresses the SAME run cap — and the
    # refusal is REAL, not staged: run-scope spent is the fan-out's 1000.
    rE2 = wf.run_child(metaE, nodeE2, byidE, "g", "", None, skey="wf:r3acn-e:synth")
    check("E2 occurrence 2 refuses at the run cap BEFORE spawn (re-expressed)",
          rE2.get("status") == "failed"
          and rE2.get("error_class") == "budget_exhausted"
          and "run-scope" in (rE2.get("error") or "")
          and pops() == popsE0 + 1,   # the fan-out's one billed item; refusal bills zero
          {"r": rE2, "pops": pops()})
    wf.save_node(runE, nodeE2, byidE,
                 {"status": "failed", "error_class": "budget_exhausted",
                  "error": rE2.get("error")})
    led2 = close_fn(runE, metaE, ["synth"], [])               # the hook, closure 2
    check("E3 merge-on-write: the persisted cause is the FIRST closure's, not the last",
          led2.get("cause") == "budget_exhausted" and led2.get("at") == led1_at
          and led2.get("at") != led2.get("occurrences", [{}])[-1].get("at"),
          led2)
    occ = led2.get("occurrences") or []
    check("E3 the cause chain holds BOTH closures in order — the fan-out cause survives",
          [o.get("exhausted_nodes") for o in occ] == [["fo"], ["synth"]]
          and [o.get("never_launched") for o in occ] == [["synth"], []], occ)
    check("E3 the union view names both cap deaths; the file on disk IS the returned ledger",
          led2.get("exhausted_nodes") == ["fo", "synth"]
          and led2.get("never_launched") == ["synth"]
          and (runE / "budget_ledger.json").read_text()
              == json.dumps(led2, ensure_ascii=False), led2)
    # Idempotence: a benign resume re-passes the hook with the SAME recomputed
    # set (the est-3acn resume law — a later pass sees the same file).
    led3 = close_fn(runE, metaE, ["synth"], [])
    check("E4 a re-closure with the same set mints no duplicate row (resume law)",
          led3.get("occurrences") == occ and led3.get("at") == led1_at, led3)
    # Absorption: a ledger written by the PRE-FIX overwrite closure (no
    # occurrences[]) joins the chain as its first occurrence, cause intact.
    runE2 = mkrun("r3acn-e2")
    legacy = {"cause": "budget_exhausted", "at": "2026-10-15T12:00:00+00:00",
              "exhausted_nodes": ["fo"], "never_launched": ["synth"]}
    (runE2 / "budget_ledger.json").write_text(json.dumps(legacy))
    ledL = close_fn(runE2, base_meta(runE2), ["gate-x"], [])
    check("E5 a pre-fix legacy row joins the chain as the first occurrence",
          ledL.get("cause") == "budget_exhausted" and ledL.get("at") == legacy["at"]
          and [o.get("exhausted_nodes") for o in ledL.get("occurrences") or []]
              == [["fo"], ["gate-x"]], ledL)

print(("ALL PASS " if ok else "FAILURES PRESENT ") + "test_run_budget_3acn")
raise SystemExit(0 if ok else 1)
