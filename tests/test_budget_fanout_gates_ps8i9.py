#!/usr/bin/env python3
"""est-ps8i9 — the budget-capped fan-out PARTIAL is a commit edge: every
declared boundary gate must run over its record before save.

The defect (the est-13z1 class at the budget edge; adversarial R8 sweep of PR
#525 @8fd158ca): the commit-edge census applies the boundary gates —
`_lane_gate`, then `_effect_gate`, then `_bead_close_gate` — at every
committing commit: the agent-solo path (wf.py:8241-8242), the fan-out
aggregate (:8212-8213), the echo boundary, and (since #523/est-ikeon) the
`on_skip:"pass"` and `on_exit` edges. The est-3acn budget-exhausted fan-out
branch (:8141-8153) commits `partial` WITH harvest DIRECTLY — `save_node`
inside its own elif, `merged_rec` assigned only in the else — so a fan-out
node declaring `{effects}` or `{bead_close id:{item}}` that is RunBudget-capped
mid-run banks harvested item outputs with ZERO store read-back and ZERO
receipt proof: the lying-done class at the third commit edge, PRE-EXISTING on
main, not introduced by #525.

The fix mirrors the aggregate edge exactly: build the record, run
_lane_gate -> _effect_gate -> _bead_close_gate over it, save the RESULT, and
log node.finished only while the record is still partial (the demote branch
mirrors the aggregate edge's: node.failed + the demote's own error_class).
A partial IS committed output (#4 law), so it takes the same boundaries; a
gate without its declaration key is a no-op, so undeclared budget partials
stay byte-identical (the golden control below).

Legs (RED proof at 8fd158ca before the fix: A books partial with
effect_receipt_unproven never seen; C books partial with ZERO bd argv in the
probe log; G books partial over a dirty lane; D/E/F already pass — honest
paths must not move):

  A  effects DECLARED, file never written  -> demoted failed, typed
     error_class=effect_receipt, the harvest still rides the record's output.
  B  effects declared, file honestly written -> stays partial, effect_receipts
     proves it, node.finished(harvested, budget_exhausted) stands.
  C  bead_close {item} template, store says open -> demoted failed
     error_class=bead_close; the measured row rides the ITEM record inside
     all_results and the aggregate carries the sparse plural enumeration.
  D  bead_close, store says closed -> stays partial with the plural key.
  E  NO gate declaration -> byte-identical to the pre-fix record: same status,
     same error string, no gate keys minted, and the fake bd is never invoked.
  F  close-pending honest escape -> no probe, stays partial, record pristine.
  G  repo lane DECLARED and dirty -> _lane_gate demotes incomplete_work
     BEFORE the other gates see the record (they no-op on failed).

Harness: test_run_budget_3acn LEG C (in-process run_agent_node, sqlite-billed
fake hermes as the meter, sequential fan-out so the cap boundary is
deterministic) + tests/fake_bd_close_13z1.py (fake `bd` store + argv log).

Run: env -u WF_RUNS_ROOT -u HERMES_HOME PYTHONPATH=/opt/hermes
     /opt/hermes/.venv/bin/python tests/test_budget_fanout_gates_ps8i9.py
  (or plain python3 with the repo root on sys.path — stdlib only)
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import wf  # noqa: E402  (the runner module — loads wfcommon itself)

ok = True


def check(label, cond, detail=""):
    global ok
    print(("PASS " if cond else "FAIL " + label) + (("  " + str(detail)[:400]) if detail else ""))
    ok = ok and bool(cond)


TMP = Path(tempfile.mkdtemp(prefix="wf-ps8i9-"))
os.environ["HERMES_HOME"] = str(TMP / "home")
os.environ["WF_RUNS_ROOT"] = str(TMP / "home" / "workflows")
# env-insensitive law (est-3acn re-roll): an ambient wide seat value must
# never fail-close this pin.
if os.environ.get("WORKFLOW_MAX_SEATS") not in (None, "0") and \
        not (4 <= int(os.environ.get("WORKFLOW_MAX_SEATS", "8")) <= 16):
    os.environ["WORKFLOW_MAX_SEATS"] = "16"

# The fake child IS the meter (test_run_budget_3acn pattern): every spawn
# bills WF3_IN+WF3_OUT tokens into state.db under its --continue session
# title, so a node budget of 1000 lets EXACTLY ONE item through and refuses
# the rest pre-dispatch — the budget partial branch, driven for real.
# Optional per-leg env: WF4_WRITE=<relpath> makes the child write that file
# under the run dir (the honest effect), WF4_PENDING=1 makes it answer with
# the honest non-claim token instead of a close claim.
STUB = TMP / "hermes_stub.py"
STUB.write_text(
    "#!/usr/bin/env python3\n"
    "import sys, os, sqlite3\n"
    "a = sys.argv[1:]\n"
    "title = a[a.index('--continue') + 1] if '--continue' in a else ''\n"
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
    "           '2026-10-21T12:00:00', '2026-10-21T11:00:00'))\n"
    "c.commit(); c.close()\n"
    "w = os.environ.get('WF4_WRITE')\n"
    "if w:\n"
    "    p = os.path.join(os.environ['WF4_RUN'], w)\n"
    "    os.makedirs(os.path.dirname(p), exist_ok=True)\n"
    "    open(p, 'w').write('effect bytes ' * 32 + '\\n')\n"
    "if os.environ.get('WF4_PENDING'):\n"
    "    print('```json\\n{\"result\": \"ok\", \"note\": \"close-pending: assignee guard\"}\\n```')\n"
    "else:\n"
    "    print('```json\\n{\"result\": \"ok\"}\\n```')\n"
)
STUB.chmod(0o755)

# Fake `bd` on PATH (fixture shipped for est-13z1): ground truth is
# $FAKE_BD_STORE, every invocation argv lands in $FAKE_BD_LOG.
BD_PY = str(ROOT / "tests" / "fake_bd_close_13z1.py")
BINDIR = TMP / "bin"
BINDIR.mkdir()
BDP = BINDIR / "bd"
BDP.write_text("#!/bin/sh\nexec /usr/bin/python3 %s \"$@\"\n" % BD_PY)
BDP.chmod(0o755)
os.environ["PATH"] = str(BINDIR) + os.pathsep + os.environ["PATH"]
STORE = TMP / "bd-store.json"
BDLOG = TMP / "bd.log"
os.environ["FAKE_BD_STORE"] = str(STORE)
os.environ["FAKE_BD_LOG"] = str(BDLOG)


def set_store(**beads):
    STORE.write_text(json.dumps({k: {"status": v} for k, v in beads.items()}))


def bd_argv():
    try:
        return BDLOG.read_text().splitlines()
    except OSError:
        return []


def mkrun(rid):
    r = Path(os.environ["WF_RUNS_ROOT"]) / rid
    (r / "nodes").mkdir(parents=True)
    (r / "gates").mkdir()
    return r


def base_meta(run):
    return {"_run": run, "hermes_bin": str(STUB), "_spawn_n": {},
            "_procs_lock": threading.Lock(), "_procs": {},
            "_stop": threading.Event(), "node_timeout": 15,
            "item_concurrency": 1}          # sequential items -> deterministic cap boundary


def events(r):
    try:
        return [json.loads(l) for l in (r / "events.jsonl").read_text().splitlines()]
    except (OSError, ValueError):
        return []


def capped_fanout(rid, **node_over):
    """Drive the budget partial branch for real: node budget 1000, the stub
    bills 1000 per spawn, item_concurrency 1 -> item 0 commits, items 1-2 are
    refused pre-dispatch typed budget_exhausted, the elif at :8141 fires."""
    r = mkrun(rid)
    node = {"id": "fo", "type": "agent", "goal": "item {n}",
            "budget": {"tokens": 1000},
            "fanout": {"items": [{"n": 0}, {"n": 1}, {"n": 2}]}}
    node.update(node_over)
    (r / "graph.json").write_text(json.dumps({"name": rid, "nodes": [node]}))
    os.environ["WF4_RUN"] = str(r)
    wf.run_agent_node(r, base_meta(r), {"fo": node}, node, {}, [])
    rec = json.loads((r / "nodes" / "fo.json").read_text())
    return r, rec


# ---- LEG A: effects declared, never proven -> the budget partial DEMOTES ----
os.environ.pop("WF4_WRITE", None)
rA, recA = capped_fanout("bps8i9-a", effects=[{"file": "lane/out.md", "min_bytes": 1}])
check("A1 budget-partial with an unproven effect file demotes to failed, typed",
      recA.get("status") == "failed" and recA.get("error_class") == "effect_receipt",
      f"status={recA.get('status')} class={recA.get('error_class')}")
check("A2 the demote names the missing effect",
      (recA.get("error") or "").startswith("effect_receipt_unproven:")
      and "lane/out.md" in (recA.get("error") or ""), (recA.get("error") or "")[:200])
check("A2 the harvest still rides the demoted record (partial IS output; the demote kills the claim, not the work)",
      len((recA.get("output") or {}).get("items") or []) == 1
      and len((recA.get("output") or {}).get("all_results") or []) == 3,
      {k: len(v) for k, v in (recA.get("output") or {}).items() if isinstance(v, list)})
check("A3 the measured effect rows ride the record",
      bool(recA.get("effect_receipts")), list(recA.keys()))
evA = events(rA)
check("A4 the event is node.failed with the demote class, never a harvested node.finished",
      any(e.get("event") == "node.failed" and e.get("error_class") == "effect_receipt"
          for e in evA)
      and not any(e.get("event") == "node.finished" for e in evA), evA[-3:])

# ---- LEG B: effect honestly written -> stays partial, receipts prove it -----
os.environ["WF4_WRITE"] = "lane/out.md"
rB, recB = capped_fanout("bps8i9-b", effects=[{"file": "lane/out.md", "min_bytes": 1}])
check("B1 honest effects: budget partial stays partial with its typed class",
      recB.get("status") == "partial" and recB.get("error_class") == "budget_exhausted",
      f"status={recB.get('status')} class={recB.get('error_class')}")
check("B2 the effect receipt proves ok",
      bool(recB.get("effect_receipts")) and recB["effect_receipts"][0].get("ok") is True,
      recB.get("effect_receipts"))
check("B3 harvested node.finished stands (byte-shape of the honest path unchanged)",
      any(e.get("event") == "node.finished" and e.get("harvested") is True
          and e.get("error_class") == "budget_exhausted" for e in events(rB)))
os.environ.pop("WF4_WRITE", None)

# ---- LEG C: bead_close {item} template, store says OPEN -> DEMOTE ----------
set_store(**{"est-ps8i9-0": "in_progress"})
BDLOG.write_text("")
rC, recC = capped_fanout("bps8i9-c", bead_close={"store": str(TMP / "estate"),
                                                 "id": "est-ps8i9-{n}"})
check("C1 budget-partial claiming an unclosed bead demotes, typed bead_close",
      recC.get("status") == "failed" and recC.get("error_class") == "bead_close",
      f"status={recC.get('status')} class={recC.get('error_class')}")
check("C2 the error is the unproven-close sentence, naming the rendered id",
      (recC.get("error") or "").startswith("bead_close_unproven:")
      and "est-ps8i9-0" in (recC.get("error") or ""), (recC.get("error") or "")[:200])
check("C3 the probe actually ran at the RENDERED id (argv log, not prose)",
      any("est-ps8i9-0" in l and "show" in l for l in bd_argv()), bd_argv())
rows0 = [r for r in (recC.get("output") or {}).get("all_results", [])
         if isinstance(r, dict) and r.get("index") == 0 or (r.get("item") == {"n": 0})]
check("C4 the measured row rides the committing ITEM record (per-item law)",
      bool((recC.get("output") or {}).get("all_results", [{}])[0].get("bead_close_receipt")),
      (recC.get("output") or {}).get("all_results", [{}])[0].get("bead_close_receipt"))
check("C4 the aggregate carries the sparse self-indexed plural enumeration",
      isinstance(recC.get("bead_close_receipts"), list)
      and recC["bead_close_receipts"][0].get("index") == 0, recC.get("bead_close_receipts"))
check("C5 non-committing (refused) items stay pristine — no receipt minted on them",
      "bead_close_receipt" not in (recC.get("output") or {}).get("all_results", [{}, {}])[1],
      (recC.get("output") or {}).get("all_results", [{}, {}])[1])

# ---- LEG D: store says CLOSED -> stays partial with the proof ---------------
set_store(**{"est-ps8i9-0": "closed"})
BDLOG.write_text("")
rD, recD = capped_fanout("bps8i9-d", bead_close={"store": str(TMP / "estate"),
                                                 "id": "est-ps8i9-{n}"})
check("D1 a proven close leaves the budget partial partial",
      recD.get("status") == "partial" and recD.get("error_class") == "budget_exhausted",
      f"status={recD.get('status')} class={recD.get('error_class')}")
check("D2 the plural receipts key carries the closed row",
      isinstance(recD.get("bead_close_receipts"), list)
      and recD["bead_close_receipts"][0].get("ok") is True, recD.get("bead_close_receipts"))

# ---- LEG E: golden honest degradation — nothing declared, nothing moves -----
BDLOG.write_text("")
rE, recE = capped_fanout("bps8i9-e")
check("E1 undeclared budget partial commits exactly as before (status/class/error)",
      recE.get("status") == "partial" and recE.get("error_class") == "budget_exhausted"
      and recE.get("error") == "budget_exhausted: 2/3 items refused at the RunBudget cap; harvested 1",
      {k: recE.get(k) for k in ("status", "error_class", "error")})
check("E2 no gate key minted, output shape unchanged",
      "effect_receipts" not in recE and "bead_close_receipt" not in recE
      and "bead_close_receipts" not in recE and "lane_dirty" not in recE
      and len((recE.get("output") or {}).get("items") or []) == 1
      and len((recE.get("output") or {}).get("all_results") or []) == 3, list(recE.keys()))
check("E2 the store was never probed — zero argv for an undeclared node",
      bd_argv() == [], bd_argv())
check("E3 harvested node.finished stands with the budget class",
      any(e.get("event") == "node.finished" and e.get("harvested") is True
          and e.get("error_class") == "budget_exhausted" for e in events(rE)))

# ---- LEG F: per-item honest non-claim -> no probe, stays partial ------------
BDLOG.write_text("")
os.environ["WF4_PENDING"] = "1"
set_store(**{"est-ps8i9-0": "in_progress"})   # closed-or-not must never be consulted
rF, recF = capped_fanout("bps8i9-f", bead_close={"store": str(TMP / "estate"),
                                                 "id": "est-ps8i9-{n}"})
check("F1 close-pending is a NON-claim: no read, no probe argv",
      bd_argv() == [], bd_argv())
check("F2 the record stays partial, pristine, key-free (est-fxh6v shape)",
      recF.get("status") == "partial" and recF.get("error_class") == "budget_exhausted"
      and "bead_close_receipts" not in recF and "bead_close_receipt" not in recF,
      list(recF.keys()))
os.environ.pop("WF4_PENDING", None)

# ---- LEG G: declared repo lane left dirty -> _lane_gate demotes first -------
repo = TMP / "lane-repo"
repo.mkdir()
subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
(repo / "f.txt").write_text("base\n")
subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
subprocess.run(["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t",
                "commit", "-qm", "seed"], check=True)
(repo / "f.txt").write_text("base\nuncommitted child work\n")   # the dad50be0 shape
rG, recG = capped_fanout("bps8i9-g", repo=str(repo))
check("G1 budget-partial over a dirty declared lane demotes, typed incomplete_work",
      recG.get("status") == "failed" and recG.get("error_class") == "incomplete_work",
      f"status={recG.get('status')} class={recG.get('error_class')}")
check("G2 the demote names the lane and the dirty count; harvest survives",
      "lane dirty after commit-time review" in (recG.get("error") or "")
      and len((recG.get("output") or {}).get("items") or []) == 1, (recG.get("error") or "")[:160])

print("\nRESULT:", "ALL PASS" if ok else "FAILURES ABOVE")
sys.exit(0 if ok else 1)
