#!/usr/bin/env python3
"""est-ja41 — the ratelimit park's attempts history must survive the transport
ladder (PR #159 finding, the REVERSE direction of the wf159c B2 pin).

The reverse-transition shape (unit, deterministic — no fake, no subprocess):

    banner death -> park respawns -> transport death -> transient ladder
    respawns -> success.

Both node.retrying events fire (park's and ladder's), so the event history
holds two deaths. Before the fix the ladder reset attempts_log to its own
bookkeeping — the committed record carried ONLY the transport death and the
parked rate-limit death was silently dropped (the finding's runner-subprocess
repro: attempts=3, attempts_log=[transport] only).

The fix (est-ja41, this pin):
  * the ladder SEEDS from r["attempts_log"] — an inherited park history is
    preserved in record order (ratelimit entry first, ladder entries after);
  * the two-death cap and the transport_exhausted demotion run on
    `ladder_attempts` (THIS ladder's own attempts). An inherited log alone
    never demotes a record the ladder never touched (A1 shape: an
    transport_exhausted laundering would also unlock the fallback ladder for
    a death that was never the ladder's);
  * the retry-sleep schedule indexes ladder_attempts, so an inherited log
    cannot shift the first ladder sleep from 5 s to 20 s (the fake's
    transport deaths are instant — this branch is law-pinned by the schedule
    code path + A4's unit below pins the counter directly).

Byte-identity law: a run that never visited the park has r["attempts_log"]
absent -> seed == [] -> attempts_log grows exactly as before, and
ladder_attempts >= 1 is exactly the old `attempts_log` non-empty condition
(both append sites bump the counter in lockstep). The golden-solo gate is the
referee.

Run: env -u WF_RUNS_ROOT -u HERMES_HOME PYTHONPATH=/opt/hermes /opt/hermes/.venv/bin/python tests/test_ratelimit_history_ja41.py
"""
import json, os, random, shutil, subprocess, sys, tempfile, threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = HERE.parent
HOME = HERE / "home-ja41"
RUNS = HOME / "workflows"
os.environ["HERMES_HOME"] = str(HOME)
os.environ["WF_RUNS_ROOT"] = str(HOME / "workflows")   # est-2ek.1.762 pin: HERMES_HOME alone is not a sandbox
os.environ.pop("WF_RUNS_ROOT", None)   # unset for THIS process; children pin it explicitly
sys.path.insert(0, str(BUILD))
import wf  # noqa: E402
import wf as _wfmod  # noqa: E402

ok = True
def check(label, cond, detail=""):
    global ok
    print(("PASS " if cond else "FAIL ") + label + ("" if cond or not detail else f"  {detail}"))
    ok = ok and bool(cond)

if HOME.exists():
    shutil.rmtree(HOME)
HOME.mkdir(parents=True); RUNS.mkdir()
FAKE = str(HERE / "fake")

BANNER = ("hermes -z: agent failed: Anthropic credentials are rate-limited for "
          "claude-fable-5-1; other Claude models remain available (see `hermes auth list`).")

def banner_r():
    return {"status": "failed", "error_class": "ratelimit", "error": "boom",
            "raw": BANNER, "pid": 4294967, "ms": 1, "attempts": 0}

def transport_r():
    return {"status": "failed", "error_class": "transport", "error": "conn",
            "raw": "hermes -z: agent failed: openai.APIConnectionError. Connection error.",
            "pid": 4294967, "ms": 1, "attempts": 0}

def done_r():
    return {"status": "done", "output": {"result": "ok"}, "pid": 4294967, "ms": 1}

def unit_meta(interval=0.3, jitter=0.0, retries=99):
    d = Path(tempfile.mkdtemp(prefix="rlhist-"))
    return {"_run": d, "_stop": threading.Event(), "_procs_lock": threading.Lock(),
            "_retries_left": retries, "ratelimit_interval": interval,
            "ratelimit_jitter": jitter}

fixed = random.Random(0.5)        # jitter draws at the exact midpoint
_saved_random = _wfmod.random
_saved_calls = _wfmod._attempt_api_calls
_wfmod.random = fixed
_wfmod._attempt_api_calls = lambda *a, **k: 0   # replay-safe evidence (FAKE_API_CALLS=0 shape)

try:
    # ---- A1 (the finding's class): banner -> park -> transport -> ladder -> done.
    # The park produces the banner death first (hand-off INTO the park), then
    # the LADDER sees the transport death on the park's respawn... that is B2.
    # est-ja41 is the reverse: the PARK runs first and hands its history to
    # _transient_retry, which today RESETS it. Drive both stages directly:
    # park(banner -> transport) yields a transport record carrying
    # attempts_log=[ratelimit]; the ladder then re-drives it to done.
    m = unit_meta()
    seq = iter([banner_r(), transport_r()])          # spawn(): banner; respawn(): transport
    def spawn():
        return next(seq)
    resp = transport_r                               # park's respawn -> transport death
    r = _wfmod._ratelimit_park(m, spawn(), resp, "node", {"node": "x"},
                               node={"timeout": 30})
    check("A1-pre: the park hands off a transport record ALREADY carrying its history",
          r.get("status") == "failed" and r.get("error_class") == "transport"
          and [a.get("error_class") for a in (r.get("attempts_log") or [])] == ["ratelimit"],
          json.dumps({k: r.get(k) for k in ("status", "error_class")} |
                     {"log": [a.get("error_class") for a in (r.get("attempts_log") or [])]}))

    seq2 = iter([transport_r(), done_r()])           # ladder: dead spawn, respawn answers
    def spawn2():
        return next(seq2)
    r2 = _wfmod._transient_retry(m, spawn2(), spawn2, "node", {"node": "x"},
                                 node={"timeout": 30}, defer=False)  # est-9ceqc: in-place mode — this test owns the park/history handoff, not the defer law
    # The inherited shape is what the REAL pipeline feeds the ladder: the park's
    # record with attempts_log attached. Drive that exact input:
    r_in = transport_r()
    r_in["attempts_log"] = [{"attempt": 0, "error_class": "ratelimit", "at": "t0"}]
    seq3 = iter([transport_r(), done_r()])       # ladder: dead spawn, respawn answers
    def spawn3():
        return next(seq3)
    r3 = _wfmod._transient_retry(m, r_in, spawn3, "node", {"node": "x"},
                                 node={"timeout": 30}, defer=False)  # est-9ceqc: in-place mode — this test owns the park/history handoff, not the defer law
    log_cls = [a.get("error_class") for a in (r3.get("attempts_log") or [])]
    # The loop logs the death BEFORE each respawn (pre-fix law), so this shape
    # holds the park's death + the entry logged pre-respawn + the entry logged
    # pre-the-successful-respawn: park history FIRST, ladder entries after.
    check("A1: banner->transport->success keeps BOTH deaths (parked ratelimit FIRST, ladder after)",
          r3.get("status") == "done" and log_cls == ["ratelimit", "transport", "transport"],
          f"log={log_cls} status={r3.get('status')}")

    # ---- A2: pre-fix regression witness — the same run WITHOUT inheritance
    # (fresh record, no attempts_log) must log ONLY the transport attempt and
    # still demote on the ladder's own death (byte-identity referee). ----
    seq4 = iter([transport_r(), done_r()])
    def spawn4():
        return next(seq4)
    r4 = _wfmod._transient_retry(m, spawn4(), spawn4, "node", {"node": "x"},
                                 node={"timeout": 30}, defer=False)  # est-9ceqc: in-place mode — this test owns the park/history handoff, not the defer law
    check("A2: no-inherit run logs ONLY its own transport attempt (pre-fix byte shape)",
          [a.get("error_class") for a in (r4.get("attempts_log") or [])] == ["transport"]
          and r4.get("status") == "done",
          json.dumps([a.get("error_class") for a in (r4.get("attempts_log") or [])]))

    # ---- A3: the inherited log NEVER launders a record the ladder never
    # touched to transport_exhausted. Shape: a transport death WITH a parked
    # history whose replay-safety evidence is positive (api_calls != 0) — the
    # loop breaks before its first append, so the ladder NEVER acted and must
    # not re-verdict the death. Pre-fix the record had no inherited log at all;
    # under the fix the inherited log survives untouched and the class holds
    # (pre-fix on this exact input would ALSO demote — but this input cannot
    # exist pre-fix: the log arrives only from the park, which pre-fix dropped
    # it. The guard is what keeps the merged history from speaking for a
    # ladder that never ran). ----
    m_pos = unit_meta(retries=99)
    _saved2 = _wfmod._attempt_api_calls
    _wfmod._attempt_api_calls = lambda *a, **k: 3      # positive evidence: replay unsafe
    try:
        r_in2 = transport_r()
        r_in2["attempts_log"] = [{"attempt": 0, "error_class": "ratelimit", "at": "t0"}]
        r5 = _wfmod._transient_retry(m_pos, r_in2, lambda: transport_r(), "node",
                                     {"node": "x"}, node={"timeout": 30}, defer=False)  # est-9ceqc: in-place mode — this test owns the park/history handoff, not the defer law
        check("A3: transport death, inherited log present, ladder never acted -> class stays transport",
              r5.get("error_class") == "transport", str(r5.get("error_class")))
    finally:
        _wfmod._attempt_api_calls = _saved2

    # ---- A4: the ladder's OWN two-death demotion is intact (the Q4 law,
    # est-2ek.1.164 fallback trigger). Exhausting the ladder (2 respawns + a
    # third death) from a fresh record must demote to transport_exhausted. ----
    m6 = unit_meta(retries=99)
    r_in3 = transport_r()
    seq6 = iter([transport_r(), transport_r(), transport_r(), transport_r()])
    def spawn6():
        return next(seq6)
    r6 = _wfmod._transient_retry(m6, r_in3, spawn6, "node", {"node": "x"},
                                 node={"timeout": 30}, defer=False)  # est-9ceqc: in-place mode — this test owns the park/history handoff, not the defer law
    # Q4 law: AT MOST 2 respawns — the loop logs the death before each respawn,
    # so a fully-spent ladder holds exactly 2 own entries, then the third death
    # exits and the verdict demotes.
    check("A4: ladder spent both respawns on the same dead model -> transport_exhausted (fallback trigger intact)",
          r6.get("error_class") == "transport_exhausted"
          and [a.get("error_class") for a in (r6.get("attempts_log") or [])] == ["transport"] * 2,
          json.dumps({"ec": r6.get("error_class"),
                      "log": [a.get("error_class") for a in (r6.get("attempts_log") or [])]}))

    # ---- A5: demotion with INHERITED history counts only the ladder's own
    # attempts: 2 own respawns + a third transport death WITH a parked history
    # present must still demote (own attempts drive it, not the merged length). ----
    r_in4 = transport_r()
    r_in4["attempts_log"] = [{"attempt": 0, "error_class": "ratelimit", "at": "t0"}]
    seq7 = iter([transport_r(), transport_r(), transport_r()])
    def spawn7():
        return next(seq7)
    r7 = _wfmod._transient_retry(m, r_in4, spawn7, "node", {"node": "x"},
                                 node={"timeout": 30}, defer=False)  # est-9ceqc: in-place mode — this test owns the park/history handoff, not the defer law
    check("A5: inherited history + exhausted own ladder -> transport_exhausted (park history survives the demote)",
          r7.get("error_class") == "transport_exhausted"
          and [a.get("error_class") for a in (r7.get("attempts_log") or [])]
          == ["ratelimit", "transport", "transport"],
          json.dumps([a.get("error_class") for a in (r7.get("attempts_log") or [])]))

    # ---- A6: attempt-numbering is continuous across the seam (0..N over the
    # merged history), so a cold reader can order the deaths machine-side. ----
    nums = [a.get("attempt") for a in (r3.get("attempts_log") or [])]
    check("A6: attempt numbers stay continuous across the inherited seam", nums == list(range(len(nums))),
          str(nums))

    # ---- A7 (runner-subprocess, the finding's actual shape): banner death ->
    # parked respawn dies transport (api_count=0) -> second respawn answers.
    # Committed record must show BOTH deaths and attempts consistent with 3
    # spawns. FAKE_MODE ratelimit_then_transport below drives exactly that. ----
    att = HOME / "att-a7"; shutil.rmtree(att, ignore_errors=True)
    rid = "ja41-a7"
    rdir = RUNS / rid
    shutil.rmtree(rdir, ignore_errors=True)
    (rdir / "nodes").mkdir(parents=True); (rdir / "gates").mkdir()
    (rdir / "graph.json").write_text(json.dumps(
        {"name": rid, "nodes": [{"id": "a", "type": "agent", "goal": "GO ja41-a7"}]}))
    (rdir / "run.json").write_text(json.dumps(
        {"hermes_bin": FAKE, "concurrency": 4, "node_timeout": 30,
         "ratelimit_interval": 0.3, "ratelimit_jitter": 0.0,
         "retry_backoff": [0.05, 0.05], "retry_budget": 6}))
    env = dict(os.environ, HERMES_HOME=str(HOME), FAKE_LOG=str(HOME / "fake.log"),
               FAKE_MODE="ratelimit_then_transport", FAKE_ATTEMPT_DIR=str(att),
               FAKE_API_CALLS="0")
    env.pop("WF_RUNS_ROOT", None)
    (HOME / "fake.log").write_text("")
    out = subprocess.run([sys.executable, str(BUILD / "wf.py"), "run", rid],
                         env=env, capture_output=True, text=True, timeout=120).stdout.strip()
    rec = json.loads((rdir / "nodes" / "a.json").read_text())
    log_cls = [a.get("error_class") for a in (rec.get("attempts_log") or [])]
    nspawns = (HOME / "fake.log").read_text().count("GO ja41-a7")
    check("A7: runner shape banner->transport->success commits BOTH deaths (park first)",
          rec.get("status") == "done" and log_cls == ["ratelimit", "transport"]
          and nspawns == 3 and "WORKFLOW_DONE" in out,
          f"status={rec.get('status')} log={log_cls} spawns={nspawns} out={out[:40]}")
    check("A7: attempts count matches the three spawns", rec.get("attempts") == 3,
          str(rec.get("attempts")))
    evs = [json.loads(l) for l in (rdir / "events.jsonl").read_text().splitlines() if l.strip()]
    retrying = [e for e in evs if str(e.get("event", "")).endswith(".retrying")]
    check("A7: event history and the committed record agree (2 retrying events, both classes named)",
          len(retrying) == 2
          and sorted(e.get("error_class") for e in retrying) == ["ratelimit", "transport"],
          json.dumps([e.get("error_class") for e in retrying]))
finally:
    _wfmod.random = _saved_random
    _wfmod._attempt_api_calls = _saved_calls

print("ALL PASS" if ok else "FAILURES")
sys.exit(0 if ok else 1)
