#!/usr/bin/env python3
"""hw-23c: author-tunable retry — a per-node `retry` key + reachable run knobs.

The gap (bead hw-23c, measured 2026-10-01): both retry ladders were hardcoded.
The transient ladder stopped at 2 re-spawns, the bounded ladder fired exactly
once, the runner READS run.json meta.retry_backoff/retry_budget but the door
never wrote them — the knobs existed only as test injection. Self-healing runs
(councils, wide fan-outs) collapsed one seat at a time where one author knob
would save them.

Law pinned here:
  * `retry: {transient: N, bounded: K}` — agent node key, values integers
    0..8, either half optional; anything else refused by the validator by name;
  * N caps the transient ladder's re-spawns (default 2 = today's law);
  * K re-drives through the bounded ladder (default 1 = today's law), each
    pass under every existing guard (class, tool progress, stop, per-run
    budget, #61 quarantine, #102);
  * a `retry` block also rides graph `defaults` (per-node key wins, like every
    other defaults key);
  * the door bakes run args retry_backoff [a,b] / retry_budget int into
    run.json meta so the runner's existing reader is finally reachable.

Plain script, no pytest: exits non-zero on the first red.
"""
import contextlib
import copy
import importlib.util
import json
import os
import sys
import tempfile
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FAILS = []


def check(name, cond, detail=None):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else "  ::  " + str(detail)[:600]))
    if not cond:
        FAILS.append(name)


common = load("wfcommon", ROOT / "wfcommon.py")
wf = load("wf", ROOT / "wf.py")

# ---------- 1. validator ----------
def graph_errs(node_extra):
    node = {"id": "a", "type": "agent", "goal": "g"}
    node.update(node_extra)
    return common.validate_graph_full({"name": "t", "nodes": [node]})

check("retry absent validates (default law)", graph_errs({}) == [])
check("retry {transient:3} validates", not graph_errs({"retry": {"transient": 3}}))
check("retry {bounded:0} validates (disable)", not graph_errs({"retry": {"bounded": 0}}))
check("retry {transient:8} validates (cap edge)", not graph_errs({"retry": {"transient": 8}}))
e = graph_errs({"retry": {"transient": 9}})
check("retry transient 9 refused by name", any(x["field"] == "retry" for x in e), e)
# est-jzi8a (PR #497 review, finding 2): the node-level `retry` validation block
# was pasted TWICE in wfcommon — one malformed value reported the door error
# twice. This pin is RED against the duplicated block.
check("retry refusal reported exactly once (no duplicated validation block)",
      sum(1 for x in e if x["field"] == "retry") == 1, e)
e = graph_errs({"retry": 3})
check("retry bare int refused", any(x["field"] == "retry" for x in e), e)
e = graph_errs({"retry": {"max": 3}})
check("retry unknown subkey refused", any(x["field"] == "retry" for x in e), e)
e = common.validate_graph_full({"name": "t", "defaults": {"retry": {"bounded": 2}},
                                "nodes": [{"id": "a", "type": "agent", "goal": "g"}]})
check("retry rides graph defaults", not e, e)

# ---------- 2. runner ladders honour the caps ----------
meta_base = {"_run": None, "_stop": threading.Event(), "_procs_lock": threading.Lock(),
             "_retries_left": 50, "retry_backoff": [0, 0],
             # est-9ceqc: the transient schedule is now the persisted exp policy
             # (superseding the fixed [5, 20] ladder); base_seconds=0 keeps this
             # fixture exercising the LOOP without sleeping — door channel only.
             "retry_policy": {"base_seconds": 0}}


def mk_meta(tmp, node):
    run = Path(tmp)
    (run / "nodes").mkdir(exist_ok=True)
    m = dict(meta_base)
    m["_run"] = run
    m["_node_def"] = node
    return m


def death(eclass):
    return {"status": "failed", "error": eclass, "error_class": eclass, "ms": 0,
            "skey": "sk1", "raw": "", "attempts_log": [{"attempt": 0, "error_class": eclass}]}


def fake_bounded(eclass):
    """Respawn always dies the same way; count the calls."""
    calls = {"n": 0}

    def respawn(**kw):
        calls["n"] += 1
        r = copy.deepcopy(death(eclass))
        r["skey"] = f"sk{calls['n']}"
        return r
    return respawn, calls


def node_with(**retry):
    n = {"id": "a", "type": "agent", "goal": "g"}
    if retry:
        n["retry"] = dict(retry)
    return n


with tempfile.TemporaryDirectory() as td:
    orig_tp = wf._tool_progress
    orig_hint = getattr(wf, "_resume_hint_block", None)
    wf._tool_progress = lambda *a, **k: True            # positive progress evidence
    if orig_hint is not None:
        wf._resume_hint_block = lambda *a, **k: ""
    try:
        # bounded: default = exactly one re-drive (today's law, byte-for-behaviour)
        node = node_with()
        meta = mk_meta(td, node)
        r0 = death("transport")
        respawn, calls = fake_bounded("transport")
        r = wf._bounded_retry_tuned(meta, r0, respawn, "node", {"node": "a"}, node=node)
        check("bounded default fires exactly one re-drive", calls["n"] == 1, calls)
        check("bounded default final record is a death", r["status"] == "failed", r)

        # bounded: author K=3 -> three re-drives
        node = node_with(bounded=3)
        meta = mk_meta(td, node)
        respawn, calls = fake_bounded("transport")
        r = wf._bounded_retry_tuned(meta, death("transport"), respawn, "node",
                                    {"node": "a"}, node=node)
        check("bounded 3 fires three re-drives", calls["n"] == 3, calls)

        # bounded: 0 disables the ladder
        node = node_with(bounded=0)
        meta = mk_meta(td, node)
        respawn, calls = fake_bounded("transport")
        r = wf._bounded_retry_tuned(meta, death("transport"), respawn, "node",
                                    {"node": "a"}, node=node)
        check("bounded 0 disables the ladder", calls["n"] == 0 and r["status"] == "failed", calls)

        # est-jzi8a (PR #497 review, finding 1): at the consolidate.batch site the
        # tuned re-drive must run under the PARTNER node (attempt identity and
        # evidence paths — skey, work dir, log stem — are partner-keyed) while
        # the cap keeps reading the CARRIER's retry key via cap_node.
        seen = {}
        orig_br = wf._bounded_retry

        def spy_bounded(meta_, r, respawn, ev, ev_kw, node=None, index=None):
            seen["node"] = node
            return orig_br(meta_, r, respawn, ev, ev_kw, node=node, index=index)
        wf._bounded_retry = spy_bounded
        try:
            carrier = node_with(bounded=3)                     # fan-out carrier owns the knob
            partner = {"id": "cons", "type": "agent", "goal": "g"}   # no retry key
            meta = mk_meta(td, partner)
            respawn, calls = fake_bounded("transport")
            try:
                r = wf._bounded_retry_tuned(meta, death("transport"), respawn, "node",
                                            {"node": "a"}, node=partner, cap_node=carrier)
                check("cap_node reads the carrier's bounded cap (3 re-drives)",
                      calls["n"] == 3, calls)
                check("guard/evidence context stays on the passed node",
                      seen.get("node") is partner, seen)
            except TypeError as exc:
                check("cap_node keyword accepted by _bounded_retry_tuned", False, exc)
        finally:
            wf._bounded_retry = orig_br

        # transient: author N=3 lets the api_calls==0 ladder re-spawn 3 times
        orig_calls = wf._attempt_api_calls
        wf._attempt_api_calls = lambda *a, **k: 0
        try:
            node = node_with()
            meta = mk_meta(td, node)
            respawn, calls = fake_bounded("unknown")
            r = wf._transient_retry(meta, death("unknown"), respawn, "node",
                                    {"node": "a"}, node=node,
                                    defer=False)  # est-9ceqc: the DEFER law moves the wait to the driver drain; the CAP law this check pins is unchanged — pin the in-place re-drive shape
            check("transient default caps at 2 re-spawns", calls["n"] == 2, calls)
            node = node_with(transient=3)
            meta = mk_meta(td, node)
            respawn, calls = fake_bounded("unknown")
            r = wf._transient_retry(meta, death("unknown"), respawn, "node",
                                    {"node": "a"}, node=node,
                                    defer=False)  # est-9ceqc: cap law unchanged; pin the in-place shape
            check("transient 3 caps at 3 re-spawns", calls["n"] == 3, calls)
        finally:
            wf._attempt_api_calls = orig_calls
    finally:
        wf._tool_progress = orig_tp
        if orig_hint is not None:
            wf._resume_hint_block = orig_hint

# ---------- 3. door bakes the run knobs ----------
os.environ["WF_RUNS_ROOT"] = td + "/runs"
os.environ["HERMES_HOME"] = td + "/home"
door = load("wf_door_23c", ROOT / "__init__.py")
import wf_test_isolation as _iso71_23c; _iso71_23c.install(door)  # #71 r5: pin settings.runs_root alongside WF_RUNS_ROOT
G = {"name": "knobs", "nodes": [{"id": "a", "type": "echo", "output": {"ok": True}}]}
res = door.act_run({"graph": copy.deepcopy(G), "retry_budget": 9,
                    "retry_backoff": [3, 15]})
check("door run accepted with retry knobs", "run_id" in res, res)
if "run_id" in res:
    meta = json.loads((Path(td) / "runs" / res["run_id"] / "run.json").read_text())
    check("door bakes retry_budget into meta", meta.get("retry_budget") == 9, meta)
    check("door bakes retry_backoff into meta", meta.get("retry_backoff") == [3, 15], meta)
    door.act_stop({"run_id": res["run_id"]})
res = door.act_run({"graph": copy.deepcopy(G), "retry_budget": -1})
check("door refuses a negative retry_budget", "error" in res, res)
# est-mcwq6 (hw-23c fallout): the refusal runs AT THE DOOR beside
# _concurrency_bake — BEFORE _create_run mkdirs anything (the first #497
# shape baked inside _create_run, after mkdir+graph.json, so a malformed
# arg orphaned a run dir). Pin: a refused submit adds NO directory.
_before = sorted(p.name for p in (Path(td) / "runs").iterdir())
res = door.act_run({"graph": copy.deepcopy(G), "retry_backoff": [3, "x"]})
check("door refuses a malformed retry_backoff", "error" in res, res)
_after = sorted(p.name for p in (Path(td) / "runs").iterdir())
check("refused retry args create no run dir (door parity)", _after == _before,
      (_before, _after))

# ---------- 4. batch-site wiring (est-jzi8a finding 1, source-shape pin) ----------
_wf_src = (ROOT / "wf.py").read_text()
_i = _wf_src.find('_bounded_retry_tuned(meta, rb, _bs, "consolidate.batch"')
check("consolidate.batch tuned call site present in wf.py source", _i != -1)
if _i != -1:
    _seg = _wf_src[_i:_i + 400].splitlines()
    _seg = [l for l in _seg if not l.strip().startswith(("except", "rb = {"))]
    _seg = "\n".join(_seg)
    check("batch tuned re-drive passes partner context (node=cons_partner)",
          "node=cons_partner" in _seg, _seg)
    check("batch tuned re-drive keeps carrier caps (cap_node=node)",
          "cap_node=node" in _seg, _seg)

print("\nchecks failed: %d" % len(FAILS))
sys.exit(1 if FAILS else 0)
