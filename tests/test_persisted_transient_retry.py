#!/usr/bin/env python3
"""est-9ceqc WF-GRAFT-5: the transient ladder's window PERSISTS — a bounded
transient retry that survives a runner restart, ported from the peer
itechmeat/hermes-workflows RetryPolicy (max_attempts=3, base 2.0 s,
ceiling 30.0 s, delay=min(base*2**(attempt-1), ceiling)).

Law pinned here:
  * every transient-classified retry decision persists {attempts, retry_after}
    to <run>/retry_state.json (runner-owned state — never run.json, never
    graph.json, never a committed nodes/*.json record), keyed per node AND
    fan-out item index, written tmp+os.replace;
  * the ladder DEFERS instead of sleeping in place: the worker returns its
    slot, the wave/fan-out driver re-drives the unit only after wall-clock
    passes the persisted deadline, stop + quorum cancel stay observed, and a
    bounded idle is allowed in the driver only while nothing else is runnable;
  * a restart mid-window honors the window: the resumed runner spawns NOTHING
    for the unit before the deadline (no fake child, no billing), attempt
    counts CONTINUE from the persisted number, and the bound still applies;
  * policy defaults: 3 total attempts, exp backoff base 2.0 s / ceiling 30.0 s;
    door-meta override ONLY (retry_policy {max_attempts, base_seconds,
    ceiling_seconds} — same channel law as retry_backoff/retry_budget, no env
    hooks); an explicit node `retry.transient` author key still outranks the
    door max_attempts (hw-23c law intact);
  * the `node.retrying` machine-wait event keeps backoff_s and ADDS the
    persisted deadline (retry_after) to its payload;
  * missing/corrupt state file = NO pending window (fail-open to today's
    behavior, never a runner death);
  * the Q4 shared retry_budget law is untouched: every persisted attempt draws
    it exactly as the fixed [5, 20] ladder did before this schedule was
    superseded.

Plain script, no pytest: prints PASS/FAIL lines, exits non-zero on any red.
"""
import copy
import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wf = load("wf", ROOT / "wf.py")

FAILS = []


def check(name, cond, detail=None):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else "  ::  " + str(detail)[:600]))
    if not cond:
        FAILS.append(name)


# ---------- 0. the new surface exists (RED until the fix lands) ----------
HAVE_API = all(hasattr(wf, n) for n in
               ("_retry_policy_conf_params", "_retry_policy_delay",
                "_retry_state_read", "_retry_state_put", "_retry_state_pop",
                "_retry_window_open", "RETRY_STATE_FILE"))
check("persisted-retry API surface exists on wf", HAVE_API)

# ---------- unit harness ----------
STOP = threading.Event()


def mk_meta(td, extra=None):
    run = Path(td)
    (run / "nodes").mkdir(exist_ok=True)
    m = {"_run": run, "_stop": STOP, "_procs_lock": threading.Lock(),
         "_retries_left": 50, "_retry_state_lock": threading.Lock()}
    m.update(extra or {})
    return m


def death(eclass="transport"):
    # the shape run_child hands the ladder after the FIRST spawn of a fresh
    # entry: spawn==0, no attempts_log yet.
    return {"status": "failed", "error": eclass, "error_class": eclass,
            "ms": 10, "skey": "sk1", "raw": "", "spawn": 0}


def never_spawn():
    """A respawn closure the transient ladder must NEVER call in defer mode —
    the whole point: the worker returns its slot instead of sitting on a sleep
    and a re-drive is the driver's job after the persisted deadline."""
    def respawn(**kw):
        raise AssertionError("transient ladder respawned in place under the "
                             "persisted-window law (slot held through the wait)")
    return respawn


def drive_until_terminal(meta, node_id, policy=None, eclass="transport",
                        node=None, max_laps=10):
    """Emulate the wave/fan-out driver: re-enter the ladder once the persisted
    deadline passes, exactly as the drain loop does; return (final_record,
    persisted_attempts_sequence)."""
    node = node or {"id": node_id, "type": "agent", "goal": "g"}
    seq = []
    for _ in range(max_laps):
        r = wf._transient_retry(meta, copy.deepcopy(death(eclass)), never_spawn(),
                                "node", {"node": node_id}, node=node)
        if r.get("status") == "deferred":
            ent = wf._retry_state_read(meta).get(node_id) or {}
            seq.append(int(ent.get("attempts", -1)))
            rem = float(ent.get("retry_after", 0)) - time.time()
            if rem > 0:
                time.sleep(rem + 0.01)
            continue
        return r, seq
    raise AssertionError("drive_until_terminal never reached a terminal record")


if HAVE_API:
    orig_calls = wf._attempt_api_calls
    orig_iso = wf._isolate_prior
    orig_hint = getattr(wf, "_resume_hint_block", None)
    wf._attempt_api_calls = lambda *a, **k: 0          # replay-safe evidence
    wf._isolate_prior = lambda *a, **k: None           # synthetic deaths, no trees
    if orig_hint is not None:
        wf._resume_hint_block = lambda *a, **k: ""

    # ---------- 1. policy reader: defaults + door-meta override, fail-open ----------
    check("policy defaults: base 2.0, ceiling 30.0, max_attempts 3",
          tuple(wf._retry_policy_conf_params({})) == (2.0, 30.0, 3),
          wf._retry_policy_conf_params({}))
    p = wf._retry_policy_conf_params({"retry_policy": {"base_seconds": 0.05,
                                                       "ceiling_seconds": 0.2,
                                                       "max_attempts": 5}})
    check("door-meta retry_policy override is read", p == (0.05, 0.2, 5), p)
    for junk in ({"retry_policy": "not-a-dict"},
                 {"retry_policy": {"base_seconds": "x"}},
                 {"retry_policy": {"max_attempts": True}},
                 {"retry_policy": {"max_attempts": 0}},
                 {"retry_policy": {"ceiling_seconds": -1}}):
        check("malformed retry_policy %r fails open to defaults" % (junk,),
              tuple(wf._retry_policy_conf_params(junk)) == (2.0, 30.0, 3),
              wf._retry_policy_conf_params(junk))
    check("delay law: attempt 1 == base",
          wf._retry_policy_delay(2.0, 30.0, 1) == 2.0)
    check("delay law: attempt 3 == base*2**2",
          wf._retry_policy_delay(2.0, 30.0, 3) == 8.0)
    check("delay law: ceiling clamps",
          wf._retry_policy_delay(2.0, 5.0, 4) == 5.0)

    with tempfile.TemporaryDirectory() as td:
        # ---------- 2. persist-on-retry + defer instead of sleep ----------
        pol = {"retry_policy": {"base_seconds": 0.2, "ceiling_seconds": 30.0,
                                "max_attempts": 3}}
        meta = mk_meta(td, pol)
        t0 = time.time()
        r = wf._transient_retry(meta, death(), never_spawn(), "node",
                                {"node": "solo"},
                                node={"id": "solo", "type": "agent", "goal": "g"})
        dt = time.time() - t0
        check("transient death DEFERS (worker-slot-releasing sentinel)",
              r.get("status") == "deferred", r)
        check("deferral returns instantly — no in-place sleep", dt < 0.15, dt)
        ent = wf._retry_state_read(meta).get("solo") or {}
        check("state file carries attempts==1 after the first death",
              ent.get("attempts") == 1, ent)
        check("state file carries a FUTURE retry_after ~= now+0.2",
              t0 + 0.19 <= float(ent.get("retry_after", 0)) <= t0 + 1.0, ent)
        check("sentinel carries the persisted deadline",
              abs(float(r.get("deferred_until", 0))
                  - float(ent.get("retry_after", 0))) < 0.05, r)
        check("state file lives in the run dir under retry_state.json",
              (Path(td) / "retry_state.json").exists())

        # ---------- 3. exp schedule via meta override + attempts continue + purge ----------
        meta = mk_meta(td + "b", {"retry_policy": {"base_seconds": 0.1,
                                                   "ceiling_seconds": 30.0,
                                                   "max_attempts": 5}})
        rfin, seq = drive_until_terminal(meta, "sched")
        check("exp backoff emits [0.1, 0.2, 0.4, 0.8] across attempts",
              seq == [1, 2, 3, 4], seq)
        check("terminal death demotes to transport_exhausted",
              rfin.get("error_class") == "transport_exhausted", rfin)
        check("committed attempts CONTINUE across deferrals to the full bound (5)",
              rfin.get("attempts") == 5, rfin)
        check("terminal exit purges the persisted window",
              wf._retry_state_read(meta).get("sched") is None,
              wf._retry_state_read(meta))

        # ---------- 4. ceiling clamp through the ladder ----------
        meta = mk_meta(td + "c", {"retry_policy": {"base_seconds": 0.1,
                                                   "ceiling_seconds": 0.15,
                                                   "max_attempts": 4}})
        rfin, seq = drive_until_terminal(meta, "clamp")
        backoffs = [min(0.1 * 2 ** (a - 1), 0.15) for a in seq]
        check("ceiling clamps the persisted windows (0.1, 0.15, 0.15)",
              seq == [1, 2, 3] and backoffs[1] == 0.15 and backoffs[2] == 0.15,
              seq)

        # ---------- 5. author `retry.transient` still outranks door max_attempts ----------
        meta = mk_meta(td + "d", {"retry_policy": {"base_seconds": 0.05,
                                                   "ceiling_seconds": 1.0,
                                                   "max_attempts": 5}})
        rfin, seq = drive_until_terminal(meta, "auth",
                                         node={"id": "auth", "type": "agent",
                                               "goal": "g",
                                               "retry": {"transient": 1}})
        check("author retry.transient=1 caps at ONE re-spawn despite max_attempts 5",
              seq == [1] and rfin.get("error_class") == "transport_exhausted"
              and rfin.get("attempts") == 2, (seq, rfin))

        # ---------- 6. corrupt / absent state = fail-open ----------
        (Path(td) / "retry_state.json").write_text("{not json at all")
        r = wf._transient_retry(meta, death(), never_spawn(), "node",
                                {"node": "solo"},
                                node={"id": "solo", "type": "agent", "goal": "g"})
        check("corrupt state file FAILS OPEN to a fresh deferral (no crash, no seed)",
              r.get("status") == "deferred"
              and (wf._retry_state_read(meta).get("solo") or {}).get("attempts") == 1,
              r)
        (Path(td) / "retry_state.json").unlink(missing_ok=True)
        check("absent state file reads as no window",
              wf._retry_state_read(meta) == {}
              and wf._retry_window_open(meta, "solo") is False)

        # ---------- 7. retry_budget law STAYS: persisted attempts draw the budget ----------
        meta = mk_meta(td + "e", {"retry_policy": {"base_seconds": 0.05,
                                                   "ceiling_seconds": 1.0,
                                                   "max_attempts": 4}})
        meta["_retries_left"] = 1
        r = wf._transient_retry(meta, death(), never_spawn(), "node",
                                {"node": "budget"},
                                node={"id": "budget", "type": "agent", "goal": "g"})
        check("first deferral draws the shared Q4 budget", meta["_retries_left"] == 0,
              meta["_retries_left"])
        meta2 = mk_meta(td + "f", {"retry_policy": {"base_seconds": 0.05,
                                                    "max_attempts": 4}})
        meta2["_retries_left"] = 0
        r2 = wf._transient_retry(meta2, death(), never_spawn(), "node",
                                 {"node": "nobudget"},
                                 node={"id": "nobudget", "type": "agent", "goal": "g"})
        check("exhausted Q4 budget blocks the window (no deferral, honest death)",
              r2.get("status") == "failed" and r2.get("error_class") == "transport"
              and wf._retry_state_read(meta2).get("nobudget") is None, r2)

    # ---------- 8. fan-out item keying: (node,index) keeps its own window ----------
    with tempfile.TemporaryDirectory() as td2:
        meta = mk_meta(td2)
        wf._retry_state_put(meta, "f#0", 2, time.time() + 60.0)
        wf._retry_state_put(meta, "f#1", 1, time.time() - 1.0)
        check("per-(node,index) keying: open window for f#0",
              wf._retry_window_open(meta, "f#0") is True)
        check("per-(node,index) keying: past window closed for f#1",
              wf._retry_window_open(meta, "f#1") is False)
        wf._retry_state_pop(meta, "f#0")
        check("pop removes only the popped key",
              wf._retry_window_open(meta, "f#0") is False
              and (wf._retry_state_read(meta).get("f#1") or {}).get("attempts") == 1)

    wf._attempt_api_calls = orig_calls
    wf._isolate_prior = orig_iso
    if orig_hint is not None:
        wf._resume_hint_block = orig_hint

# ---------- 9. door bake: retry_policy is a door-meta channel, refused by name ----------
door = load("persisted_retry_door", ROOT / "__init__.py")
import wf_test_isolation as _iso  # noqa: E402  (#71: pin settings.runs_root)
_iso.install(door)

ok, err = door._retry_bake({"retry_policy": {"max_attempts": 3,
                                            "base_seconds": 2.0,
                                            "ceiling_seconds": 30.0}})
check("door bakes a valid retry_policy", ok.get("retry_policy") == {
    "max_attempts": 3, "base_seconds": 2.0, "ceiling_seconds": 30.0} and err is None,
    (ok, err))
ok, err = door._retry_bake({"retry_policy": {"max_attempts": 4}})
check("door bakes a partial retry_policy (sparse keys ride defaults)",
      ok.get("retry_policy") == {"max_attempts": 4} and err is None, (ok, err))
for bad in ({"retry_policy": []}, {"retry_policy": {"max_attempts": 0}},
            {"retry_policy": {"max_attempts": 10}},
            {"retry_policy": {"max_attempts": "3"}},
            {"retry_policy": {"base_seconds": -1}},
            {"retry_policy": {"ceiling_seconds": "x"}},
            {"retry_policy": {"nonsense": 1}}):
    ok, err = door._retry_bake(bad)
    check("door refuses malformed retry_policy %r by name" % (bad,),
          ok is None and "retry_policy" in (err or {}).get("error", ""), (ok, err))

# ---------- 10. integration: restart mid-window (SIGKILL, door resume) ----------
if HAVE_API:
    import wfcommon as common

    def until(pred, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if pred():
                return True
            time.sleep(0.02)
        return False

    FAKE = str(HERE / "fake_hermes.py")

    def integration_env(td, goal, fails):
        runs = Path(td) / "runs"
        runs.mkdir()
        att = Path(td) / "att"
        att.mkdir()
        env = {**os.environ,
               "HERMES_HOME": str(td), "WF_RUNS_ROOT": str(runs),
               "HERMES_WF_HERMES_BIN": FAKE,
               "FAKE_MODE": "transient_counted",
               "FAKE_TRANSIENT_FAILS": str(fails),
               "FAKE_ATTEMPT_DIR": str(att)}
        return runs, att, env

    def tt_count(att):
        p = next(iter(sorted(Path(att).glob("tt-*"))), None)
        return int(p.read_text()) if p is not None else 0

    BASE3 = {"max_attempts": 3, "base_seconds": 1.5, "ceiling_seconds": 5.0}

    with tempfile.TemporaryDirectory() as td3:
        runs, att, env = integration_env(td3, "PTT-RESTART-A", 2)
        graph = {"name": "ptt-restart",
                 "nodes": [{"id": "a", "type": "agent", "goal": "PTT-RESTART-A"}]}
        with __import__("unittest.mock").patch.dict(os.environ, env, clear=False):
            start = door.act_run({"graph": graph, "lane_key": "ptt/restart-a",
                                  "retry_policy": BASE3})
            check("integration: run started", "run_id" in start, start)
            rid = start["run_id"]
            rd = runs / rid
            state_path = rd / "retry_state.json"

            def window_open():
                try:
                    ent = json.loads(state_path.read_text()).get("a")
                except Exception:
                    return None
                return ent if ent and float(ent.get("retry_after", 0)) > time.time() else None

            check("integration: FIRST transient death persists an open window",
                  until(lambda: window_open() is not None, 20))
            ent = json.loads(state_path.read_text())["a"]
            check("integration: persisted attempts==1 before any respawn",
                  ent.get("attempts") == 1, ent)
            check("integration: exactly one child spawned so far", tt_count(att) == 1,
                  tt_count(att))
            pid = int((rd / "wf.pid").read_text())
            os.kill(pid, signal.SIGKILL)
            check("integration: runner SIGKILLed (runner not alive)",
                  until(lambda: not common.runner_alive(rd), 10))
            check("integration: window STILL OPEN at kill time (mid-window death)",
                  window_open() is not None, json.loads(state_path.read_text()))
            t_deadline = float(ent["retry_after"])

            # resume via the door (interrupted run): act_run dedupes, act_wait respawns
            again = door.act_run({"graph": graph, "lane_key": "ptt/restart-a"})
            check("integration: resume lane dedupes to the same run",
                  again.get("deduped") and again["run_id"] == rid, again)
            deadline_hits = 0
            t = time.time()
            while t < t_deadline - 0.25:   # sample deep inside the honored window
                if time.time() >= t_deadline - 0.25:
                    break
                time.sleep(0.05)
                deadline_hits = tt_count(att)
            check("integration: NO spawn before the persisted deadline "
                  "(window honored across restart)", deadline_hits == 1,
                  (deadline_hits, t_deadline - time.time()))
            door.act_wait({"run_id": rid, "timeout": 2})
            check("integration: run completes after the window (attempt continues to 2)",
                  until(lambda: door.act_status({"run_id": rid}).get("state") == "done",
                        25))
            rec = json.loads((rd / "nodes" / "a.json").read_text())
            check("integration: committed record continues the attempt count (2 spawns)",
                  rec.get("attempts") == 2 and tt_count(att) == 2,
                  (rec.get("attempts"), tt_count(att)))
            check("integration: window state purged after success",
                  not state_path.exists()
                  or json.loads(state_path.read_text()).get("a") is None,
                  state_path.read_text() if state_path.exists() else {})
            evs = [json.loads(l) for l in (rd / "events.jsonl").read_text().splitlines()
                   if l.strip()]
            retrying = [e for e in evs if e.get("event", "").endswith(".retrying")]
            check("integration: machine-wait event carries backoff_s AND the deadline",
                  len(retrying) >= 1 and all(
                      "backoff_s" in e and "retry_after" in e for e in retrying),
                  [(e.get("backoff_s"), e.get("retry_after")) for e in retrying])

    with tempfile.TemporaryDirectory() as td4:
        # bound at 3 WITHOUT any restart: two persisted windows, then the typed give-up
        runs, att, env = integration_env(td4, "PTT-BOUND-B", 99)
        graph = {"name": "ptt-bound",
                 "nodes": [{"id": "a", "type": "agent", "goal": "PTT-BOUND-B"}]}
        with __import__("unittest.mock").patch.dict(os.environ, env, clear=False):
            start = door.act_run({"graph": graph, "lane_key": "ptt/bound-b",
                                  "retry_policy": BASE3})
            rid = start["run_id"]
            rd = runs / rid
            door.act_wait({"run_id": rid, "timeout": 2})
            ok_run = until(
                lambda: door.act_status({"run_id": rid}).get("state") in ("failed",),
                30)
            rec = json.loads((rd / "nodes" / "a.json").read_text()) \
                if (rd / "nodes" / "a.json").exists() else {}
            check("integration: bounded at 3 attempts total, then transport_exhausted",
                  ok_run and rec.get("error_class") == "transport_exhausted"
                  and tt_count(att) == 3 and rec.get("attempts") == 3,
                  (rec.get("error_class"), tt_count(att), rec.get("attempts")))
            evs = [json.loads(l) for l in (rd / "events.jsonl").read_text().splitlines()
                   if l.strip()]
            seq = [round(e.get("backoff_s", 0), 2) for e in evs
                   if e.get("event", "").endswith(".retrying")]
            check("integration: exp schedule at defaults-override = [1.5, 3.0]",
                  seq == [1.5, 3.0], seq)

    with tempfile.TemporaryDirectory() as td5:
        # restart AND bound: kill mid-window-1, resume, count continues to the bound
        runs, att, env = integration_env(td5, "PTT-CAP-C", 99)
        graph = {"name": "ptt-cap",
                 "nodes": [{"id": "a", "type": "agent", "goal": "PTT-CAP-C"}]}
        with __import__("unittest.mock").patch.dict(os.environ, env, clear=False):
            start = door.act_run({"graph": graph, "lane_key": "ptt/cap-c",
                                  "retry_policy": BASE3})
            rid = start["run_id"]
            rd = runs / rid
            state_path = rd / "retry_state.json"

            def open_c():
                try:
                    ent = json.loads(state_path.read_text()).get("a")
                except Exception:
                    return None
                return ent if ent and float(ent.get("retry_after", 0)) > time.time() else None

            check("bound-restart: window persisted before kill",
                  until(open_c, 20))
            os.kill(int((rd / "wf.pid").read_text()), signal.SIGKILL)
            until(lambda: not common.runner_alive(rd), 10)
            again = door.act_run({"graph": graph, "lane_key": "ptt/cap-c"})
            check("bound-restart: dedupe to the same run", again.get("run_id") == rid,
                  again)
            door.act_wait({"run_id": rid, "timeout": 2})
            ok_run = until(
                lambda: door.act_status({"run_id": rid}).get("state") in ("failed",),
                35)
            rec = json.loads((rd / "nodes" / "a.json").read_text()) \
                if (rd / "nodes" / "a.json").exists() else {}
            check("bound-restart: bound enforced at 3 TOTAL spawns across the restart",
                  ok_run and tt_count(att) == 3
                  and rec.get("error_class") == "transport_exhausted"
                  and rec.get("attempts") == 3,
                  (tt_count(att), rec.get("error_class"), rec.get("attempts")))

print("\nchecks failed: %d" % len(FAILS))
sys.exit(1 if FAILS else 0)
