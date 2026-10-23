#!/usr/bin/env python3
"""Fake hermes chat for wf.py engine tests.
Usage: fake_hermes.py chat --query-file PATH --oneshot -Q ...
Behavior markers inside the prompt:
  'STEER-<word>'  -> returns {"steered": "<word>"}
  'FAILME'        -> returns unparseable prose (forces retry then failure)
  otherwise       -> returns {"result": "ok", "goal": <first line>}
Every invocation appends one line to $FAKE_LOG so tests can count spawns.
"""
import sys, os, json, time

args = sys.argv[1:]
_FAKE_HOME = os.environ.get("HERMES_HOME") or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".tmp-fake-home")
os.makedirs(_FAKE_HOME, exist_ok=True)
if "--query-file" in args:
    q = open(args[args.index("--query-file") + 1]).read()
else:
    q = sys.stdin.read()
with open(os.environ.get("FAKE_LOG", os.path.join(_FAKE_HOME, "fake.log")), "a") as f:
    f.write(q.splitlines()[0] + "\n")
if os.environ.get("FAKE_ARGV_LOG"):          # spawn-contract tests: full argv, one line per child
    with open(os.environ["FAKE_ARGV_LOG"], "a") as f:
        f.write(" ".join(args) + "\n")
if os.environ.get("FAKE_PID_LOG"):            # spawn-record tests: child pid, one per spawn
    with open(os.environ["FAKE_PID_LOG"], "a") as f:
        f.write(str(os.getpid()) + "\n")
if os.environ.get("FAKE_PROMPT_LOG"):         # prompt-content tests: FULL prompt per child,
    with open(os.environ["FAKE_PROMPT_LOG"], "a") as f:   # delimited (first line + full text)
        f.write("\n=====PROMPT=====\n" + q + "\n")
# a2d7f664 quorum-cancel evidence markers (prompt-scoped; no env needed so one
# run mixes silent and had-output stragglers under the SAME spawn conditions).
# BEFORE the shared model-latency sleep so QFLUSH means 'flushed the answer
# immediately' with no race against the quorum kill:
#   QFLUSH     -> flush a valid fenced answer IMMEDIATELY, then keep cooking
#                 (log has bytes at the kill instant)
#   QSLEEP n   -> sleep n seconds printing NOTHING (spawn log stays 0 bytes)
if "QFLUSH" in q:
    print("```json\n" + json.dumps({"result": "flushed-before-kill"}) + "\n```", flush=True)
if "QSLEEP" in q:
    try: time.sleep(float(q.split("QSLEEP")[1].split()[0]))
    except Exception: pass
# est-pygyy (#56): EFFECT-EMIT <rel/path> BYTES=<n> — the child publishes its
# artifact at spawn time (cwd = the child work dir <run>/work/<node>[.<i>],
# A4), so the runner's effect-receipt gate is proven to measure bytes AT the
# commit edge, not at admission. Item rows carry their own EFFECT-EMIT token
# (the fan-out shape: each item publishes its own artifact). Inert without
# the marker: golden paths byte-identical.
if "EFFECT-EMIT" in q:
    _sp = q.split()
    _i = _sp.index("EFFECT-EMIT")
    _tok = _sp[_i + 1]
    _n = int(_sp[_i + 2].split("BYTES=")[1]) if len(_sp) > _i + 2 and \
        _sp[_i + 2].startswith("BYTES=") else 32
    _p = os.path.join(os.getcwd(), _tok)
    os.makedirs(os.path.dirname(_p) or ".", exist_ok=True)
    with open(_p, "wb") as _f:
        _f.write(b"e" * _n)
# ---- #61 process-tree modes (no behavior change without the env) ----
# FAKE_GC=1: background a same-session grandchild that outlives this child —
# the detached-suite shape. Its pid is appended to $FAKE_GC_PIDS so the test
# can prove the runner killed it. start_new_session=False keeps it in the
# spawn's group so killpg can reach it (the pgid-walk proof).
if os.environ.get("FAKE_GC"):
    import subprocess as _sp
    _gc = _sp.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    _gcp = os.environ.get("FAKE_GC_PIDS")
    if _gcp:
        with open(_gcp, "a") as _f:
            _f.write(str(_gc.pid) + "\n")
# est-13z1: CLOSE-CLAIM {<json>} — the child returns EXACTLY that object as its
# fenced answer (the false-close shape: output prose/note claims "est-x closed"
# while the estate store says otherwise). The bead-close gate must anchor to the
# store read-back, never to this reply. Inert without the marker.
if "CLOSE-CLAIM" in q:
    _line = next(l for l in q.splitlines() if "CLOSE-CLAIM" in l)
    _body = _line.split("CLOSE-CLAIM", 1)[1].strip()
    print("Done — posted the verdict.\n```json\n" + _body + "\n```")
    sys.exit(0)
if os.environ.get("FAKE_MODE") == "background" and "BGPROGRESS" in q:   # #61: the 06f57ea9 shape —
    # background the REAL work, print progress chatter, exit 0 with NO fenced
    # block (suite.a0.log verbatim style: "Suite is running ... Waiting").
    _t0 = time.time()
    while time.time() - _t0 < 1.0:    # stay alive so the runner samples the tree
        time.sleep(0.05)              # while our grandchild is attached
    print("Suite is running in the isolated worktree at the exact SHA. "
          "Waiting for the actual exit.")
    sys.exit(0)
time.sleep(0.15)
if "CRASHME" in q:
    print("segfault-ish diagnostic prose, NO json")
    sys.exit(2)
import time as _t
if "SLEEP" in q:
    try: _t.sleep(float(q.split("SLEEP")[1].split()[0]))
    except Exception: pass
# ---- test_failures_0923 env-gated modes (Lane A; no behavior change without the env) ----
def _fake_state_row(n):
    """Register this child's --continue session title in state.db with n api
    calls — the runner's Q4 retry gate reads api_calls via wfcommon.child_metrics."""
    if "--continue" not in args:
        return
    import sqlite3
    title = args[args.index("--continue") + 1]
    c = sqlite3.connect(os.path.join(_FAKE_HOME, "state.db"))
    c.execute("create table if not exists sessions (id text primary key, title text, model text, billing_provider text, input_tokens int, output_tokens int, "
              "cache_read_tokens int, reasoning_tokens int, api_call_count int, tool_call_count int, estimated_cost_usd real, "
              "last_activity_at real, last_activity_description text, ended_at real, started_at real)")
    t = time.time()
    c.execute("insert or replace into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
              ("f-" + title, title, "fake", "fake-provider", 0, 0, 0, 0, int(n), 0, 0.0, t, "", t, t))
    c.commit(); c.close()
if os.environ.get("FAKE_API_CALLS"):
    _fake_state_row(os.environ["FAKE_API_CALLS"])
_FAKE_MODE = os.environ.get("FAKE_MODE")
# ---- #8 item 3 (crash-respawn idempotence): the effect-executing child ----
# FAKE_MODE=effect_once: the child executes an EXTERNAL side effect (one line
# into FAKE_EFFECT_FILE, keyed by `EFFECTKEY=<k>` in its prompt) and journals
# the effect to the runner's env pin HERMES_WF_EFFECTS_FILE (the SIDECAR-pin
# law: the runner names the durable journal, the registrant appends rows). If
# the spawn's prompt names its own key under the engine's committed-effects
# machine preamble, the child HONORS reconcile-don't-redo and skips the effect.
# FAKE_EFFECT_HOLD=<sec> keeps the child alive after committing (crash seam).
if _FAKE_MODE == "effect_once":
    import re as _re_cr8
    _k = _re_cr8.search(r"EFFECTKEY=(\S+)", q)
    _key = _k.group(1) if _k else ""
    _done = ("Already committed side effects" in q and _key
             and _re_cr8.search(r"key=" + _re_cr8.escape(_key) + r"(?![A-Za-z0-9_])", q))
    if _key and not _done:
        _pj = os.environ.get("HERMES_WF_EFFECTS_FILE")
        if _pj:                                   # journal FIRST: a reader that saw
            _d = os.path.dirname(_pj)             # the effect ledger is guaranteed
            if _d: os.makedirs(_d, exist_ok=True) # the journal row already landed
            with open(_pj, "a") as _f:
                _f.write(json.dumps({
                    "node": os.environ.get("HERMES_WF_STEER_NODE", ""),
                    "kind": os.environ.get("FAKE_EFFECT_KIND", "push"),
                    "key": _key,
                    "evidence": os.environ.get("FAKE_EFFECT_EVIDENCE", "committed-once"),
                    "spawn": os.environ.get("HERMES_WF_STEER_SPAWN", "0"),
                    "node_efp": os.environ.get("HERMES_WF_NODE_EFP", "")}) + "\n")
        _ef = os.environ.get("FAKE_EFFECT_FILE")
        if _ef:
            with open(_ef, "a") as _f:
                _f.write(_key + "\n")
    try: time.sleep(float(os.environ.get("FAKE_EFFECT_HOLD") or 0))
    except ValueError: pass
    print("```json\n" + json.dumps({"result": "ok", "goal": q.splitlines()[0]}) + "\n```", flush=True)
    sys.exit(0)
# est-flah (field report, 2026-10-02): the narrow-vocabulary relay enum-gate shape — a child
# whose --reasoning value the RELAY itself rejects (the door's route table missed it)
# must die with the server's "Supported types are ..." 400 so the runner's escape
# hatch can clamp-to-nearest and re-drive. FAKE_SUPPORTED_EFFORTS (csv, default
# 'xhigh,medium,low') = the relay's vocabulary; FAKE_REJECT_EFFORT = the single value
# rejected ('*' = reject whatever is asked whenever it is outside the supported set).
if _FAKE_MODE == "reasoning_gate400":
    _sup = [s.strip() for s in os.environ.get("FAKE_SUPPORTED_EFFORTS", "xhigh,medium,low").split(",") if s.strip()]
    _rej = os.environ.get("FAKE_REJECT_EFFORT", "").strip()
    _got = args[args.index("--reasoning") + 1] if "--reasoning" in args else ""
    if _got:
        _hit = bool(_got) and _got.lower() not in [s.lower() for s in _sup] if _rej == "" or _rej == "*" \
            else _got == _rej
        if _hit:
            print("hermes -z: agent failed: Error code: 400 - {{'error': {{'message': "
                  "\"Unsupported type: {g}. Supported types are {s}\", 'type': 'invalid_request_error'}}}}"
                  .format(g=_got, s=", ".join(_sup)))
            sys.exit(1)
        if os.environ.get("FAKE_GATE_THEN_TRANSPORT"):
            # est-vsgj B1 probe: the value the SERVER declared survives the gate,
            # then the spawn dies the transient shape (transport marker, zero api
            # calls) so the OUTER ladder respawns it. The respawn must re-ask the
            # server's value — never re-clamp the author's against the stale
            # local table. High reappearing after the first accepted medium is
            # the B1-crossed signature (argv high-medium-high-medium).
            print("Warning: Unknown toolsets: bogus")
            print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
            sys.exit(2)
if _FAKE_MODE == "toolset_warn_ok":   # est-flah: the CLI warns 'Unknown toolset', still answers (exit 0)
    print("⚠️  Unknown toolset: bogus")
if _FAKE_MODE == "transport":      # stable marker oneline the runner can pin (oneshot escalation shape)
    print("Warning: Unknown toolsets: bogus")
    print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
    sys.exit(2)
if _FAKE_MODE == "provider400":    # 400 with inherited CLI advice lines that must be stripped
    print("Try re-running with a different model via /new or /model")
    print("hermes -z: agent failed: Error code: 400 - {'error': 'bad request'}")
    sys.exit(2)
if _FAKE_MODE == "fallback_ladder":      # est-2ek.1.164: transport death on any -m that is not $FAKE_OK_MODEL
    _m = args[args.index("-m") + 1] if "-m" in args else None
    if _m == os.environ.get("FAKE_OK_MODEL"):
        print("```json\n" + json.dumps({"result": "answered on " + str(_m)}) + "\n```")
        sys.exit(0)
    print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
    sys.exit(2)
if _FAKE_MODE == "partial_remaining":    # est-2ek.1.165: harvest death WITH a declared Remaining block
    print("did the head of the tail, then the cap took me")
    print("## Remaining")
    print("- roll seat plugin")
    print("- CLI bake")
    print("")
    print("```json\n" + json.dumps({"result": "half done"}) + "\n```")
    sys.exit(1)
if _FAKE_MODE == "partial_noremaining":  # est-2ek.1.165: harvest death with NO declared block
    print("```json\n" + json.dumps({"result": "half done"}) + "\n```")
    sys.exit(1)
if _FAKE_MODE == "unknown":        # dies with prose only — no machine-readable marker
    print("some daemon died unexpectedly, see your provider dashboard")
    sys.exit(7)
if _FAKE_MODE == "cfgtypos":       # est-tmuu: the verified 'Unknown provider <alias>' shape —
    # CLI arg-parse class death: marker on stdout, rc!=0 inside ~0.1s, zero api_calls.
    # The runner must land config_input on ONE spawn (no respawn, no budget burn).
    # FAKE_CFG_SLOW=<sec> adds a post-marker sleep so the same capture dies OUTSIDE
    # the fast window (the slow twin: keeps its existing classification).
    print(f"Warning: Unknown provider '{os.environ.get('FAKE_CFG_ALIAS', 'nope')}'. Check "
          "'hermes model' for available providers, or run 'hermes doctor' to diagnose "
          "config issues. Falling back to auto provider detection.")
    print("Error: Unknown provider 'nope'")
    if os.environ.get("FAKE_CFG_FENCED"):   # est-jam8: a schema-valid fence rides
        # along with the config death — harvest must NOT launder it to partial/unknown.
        print('```json\n{"diagnostic": "invalid provider pin"}\n```')
    if os.environ.get("FAKE_CFG_SLOW"):
        time.sleep(float(os.environ["FAKE_CFG_SLOW"]))
    sys.exit(2)
if _FAKE_MODE == "quota":          # #24: subscription-quota 429 with a reset horizon
    print("Warning: install out of sync")
    print('Provider said: HTTP 429: {"error": {"message": "ChatGPT or Codex '
          'Subscription usage limit reached, resets in ~109 hours"}}')
    sys.exit(1)
# ---- est-imp1z (WF-GRAFT-2): the exit-0-lie family (exit-0-is-not-success) ----
_IMP1Z_LIE = "API call failed after 3 retries: HTTP 429 Too Many Requests"
if _FAKE_MODE == "exit0lie":       # est-imp1z: THE exit-0 lie — provider retries exhausted,
    # failure line as the final message, clean exit 0, NO fenced answer (the shape the
    # harvest-once coercion laundered into {result: prose} -> done on garbage).
    print(_IMP1Z_LIE)
    sys.exit(0)
if _FAKE_MODE == "exit1lie":       # est-imp1z: the same sentinel on a dying exit —
    # generic rc!=0 death whose only machine fact is the transient sentinel line.
    print(_IMP1Z_LIE)
    sys.exit(1)
if _FAKE_MODE == "declared":       # est-imp1z: self-report says FAILURE on a clean exit 0
    # with a schema-valid fence present — the token outranks both the exit code and the
    # answer's parseability; deterministic, never re-driven.
    print("Done — but the acceptance is unmet.\n```json\n"
          + json.dumps({"result": "self-declared-failure"}) + "\n```")
    print('{"node_outcome": "failure"}')
    sys.exit(0)
if _FAKE_MODE == "tokenok":        # est-imp1z: sentinel prose + valid fence + SUCCESS tail
    # token — the token protects finished work from the transient sentinel (token > sentinel).
    print(_IMP1Z_LIE + " (recovered on the final turn)")
    print("```json\n" + json.dumps({"result": "recovered"}) + "\n```")
    print('{"node_outcome": "success"}')
    sys.exit(0)
if _FAKE_MODE == "rlhint130":   # #130: rate-limit park respawn — banks a work file, then the
    # parked respawn answers ONLY when its prompt carried the artifact inventory.
    if "RESUME-HINT-ARTIFACT-130" in q and "size=" in q and "mtime=" in q:
        print("```json\n" + json.dumps({"result": "parked-hint-130"}) + "\n```")
        sys.exit(0)
    with open("hint130.md", "w") as f:
        f.write("RESUME-HINT-ARTIFACT-130 banked before the rate-limit window\n")
    print("hermes -z: agent failed: Anthropic credentials are rate-limited for "
          "claude-fable-5-1; other Claude models remain available (see `hermes auth list`).")
    sys.exit(1)
# ---- committee wf159c pins. Placed ABOVE the est-t0vz block on purpose: both
# ---- mode names start with "ratelimit" and would otherwise be hijacked by its
# ---- startswith() dispatch; they carry their own counter, same file shape. ----
def _rl159_count(key):
    if not os.environ.get("FAKE_ATTEMPT_DIR"):
        return 0
    os.makedirs(os.environ["FAKE_ATTEMPT_DIR"], exist_ok=True)
    pth = os.path.join(os.environ["FAKE_ATTEMPT_DIR"], "rl159-" + key)
    c = int(open(pth).read()) if os.path.exists(pth) else 0
    open(pth, "w").write(str(c + 1))
    return c
if _FAKE_MODE == "ratelimit_then_transport":   # est-ja41 (PR #159 finding): the REVERSE
    # order — banner death, the PARKED respawn dies transport (api_count=0 shape),
    # the ladder respawn answers. The transient ladder must NOT drop the park's
    # parked attempt from the committed attempts_log.
    _k159 = "".join(ch for ch in (q.splitlines()[0] if q.strip() else "x") if ch.isalnum())[:24] or "x"
    if _rl159_count(_k159) == 0:                     # first spawn: banner death
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: Anthropic credentials are rate-limited for "
              "claude-fable-5-1; other Claude models remain available (see `hermes auth list`).")
        sys.exit(1)
    if _rl159_count(_k159 + "t") == 0:               # parked respawn: transport death
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
        sys.exit(2)
    print("```json\n" + json.dumps({"result": "answered-after-both"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "ratelimit_after_transport":
    _k159 = "".join(ch for ch in (q.splitlines()[0] if q.strip() else "x") if ch.isalnum())[:24] or "x"
    if _rl159_count(_k159) == 0:                     # first spawn: transport death
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
        sys.exit(2)
    if _rl159_count(_k159 + "b") < int(os.environ.get("FAKE_RL_FAILS", "9999")):
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: Anthropic credentials are rate-limited for "
              "claude-fable-5-1; other Claude models remain available (see `hermes auth list`).")
        sys.exit(1)
    print("```json\n" + json.dumps({"result": "answered-after-window"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "ratelimit_straggler":
    if "b3-late" in q:
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: Anthropic credentials are rate-limited for "
              "claude-fable-5-1; other Claude models remain available (see `hermes auth list`).")
        sys.exit(1)
    print("```json\n" + json.dumps({"result": "winner-answer"}) + "\n```")
    sys.exit(0)
# ---- est-xodi (M3 pin): first spawn dies on the banner, the PARKED respawn goes
# ---- silent-slow (prints nothing for FAKE_SLEEP s) then answers with a VALID
# ---- payload. Only the consumed remaining-wall cap can kill it before commit. ----
if _FAKE_MODE == "ratelimit_then_slow":
    _k159 = "".join(ch for ch in (q.splitlines()[0] if q.strip() else "x") if ch.isalnum())[:24] or "x"
    if _rl159_count(_k159) == 0:                     # first spawn: banner death
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: Anthropic credentials are rate-limited for "
              "claude-fable-5-1; other Claude models remain available (see `hermes auth list`).")
        sys.exit(1)
    _t.sleep(float(os.environ.get("FAKE_SLEEP", "1.65")))
    print("```json\n" + json.dumps({"result": "answered-way-late"}) + "\n```")
    sys.exit(0)
# ---- est-t0vz (issue #54): credential-window 429 park modes ----
# The banner below is the VERBATIM AuthError text the stock CLI raises at
# /opt/hermes/hermes_cli/runtime_provider.py:358, as it reaches the runner's merged
# capture via the oneshot escalation line (oneshot.py:322) with rc=1.
# FAKE_RL_FAILS = N banner-deaths then success (default: always); FAKE_RL_MODEL keys
# the per-goal attempt counter so one run mixes independent rate-limit nodes.
if _FAKE_MODE and _FAKE_MODE.startswith("ratelimit"):
    def _rl_count(key):
        if not os.environ.get("FAKE_ATTEMPT_DIR"):
            return 0
        os.makedirs(os.environ["FAKE_ATTEMPT_DIR"], exist_ok=True)
        p = os.path.join(os.environ["FAKE_ATTEMPT_DIR"], "rl-" + key)
        c = int(open(p).read()) if os.path.exists(p) else 0
        open(p, "w").write(str(c + 1))
        return c
    _rl_key = "".join(ch for ch in (q.splitlines()[0] if q.strip() else "x") if ch.isalnum())[:24] or "x"
    if _FAKE_MODE == "ratelimit_always" or _rl_count(_rl_key) < int(os.environ.get("FAKE_RL_FAILS", "9999")):
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: Anthropic credentials are rate-limited for "
              "claude-fable-5-1; other Claude models remain available (see `hermes auth list`).")
        sys.exit(1)
    print("```json\n" + json.dumps({"result": "answered-after-window"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "transient_counted":   # est-9ceqc: transport death on the first
    # FAKE_TRANSIENT_FAILS calls (keyed by query), then a clean answer. Each call
    # bumps a durable counter file so the test reads the ACTUAL spawn count —
    # the evidence that a persisted window deferred the respawn, never skipped it.
    def _tt_count(key):
        d = os.environ.get("FAKE_ATTEMPT_DIR") or "."
        os.makedirs(d, exist_ok=True)
        p = os.path.join(d, "tt-" + key)
        c = int(open(p).read()) if os.path.exists(p) else 0
        open(p, "w").write(str(c + 1))
        return c
    _tt_key = "".join(ch for ch in (q.splitlines()[0] if q.strip() else "x") if ch.isalnum())[:24] or "x"
    if _tt_count(_tt_key) < int(os.environ.get("FAKE_TRANSIENT_FAILS", "9999")):
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
        sys.exit(2)
    print("```json\n" + json.dumps({"result": "answered-after-retry"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "maxturns":       # prose mentions the cap — runner must NOT grep it (stays unknown)
    print("Agent reached its max turns budget and stopped.")
    sys.exit(2)
if _FAKE_MODE == "typed_maxturns": # core -Q turn report carries the loop's typed stamp
    _rp = os.environ.get("HERMES_QUIET_TURN_REPORT_FILE")
    if _rp:
        with open(_rp, "w") as f:
            json.dump({"pid": os.getpid(), "exit_code": 2, "error": "budget",
                       "reply": "partial answer before the cap",
                       "turn_exit_reason": "max_iterations_reached(61/60)"}, f)
    print("partial answer before the cap")
    sys.exit(2)
if _FAKE_MODE == "typed_empty_report":  # report exists WITHOUT a typed reason: must stay unknown
    _rp = os.environ.get("HERMES_QUIET_TURN_REPORT_FILE")
    if _rp:
        with open(_rp, "w") as f:
            json.dump({"pid": os.getpid(), "exit_code": 2, "error": "", "reply": "", "turn_exit_reason": ""}, f)
    print("prose death"); sys.exit(2)
if _FAKE_MODE == "untyped_report":   # tier self-report (0924): report exists but the
    _rp = os.environ.get("HERMES_QUIET_TURN_REPORT_FILE")   # typed key is ABSENT -> untyped
    if _rp:
        with open(_rp, "w") as f:
            json.dump({"pid": os.getpid(), "exit_code": 2, "error": "boom", "reply": ""}, f)
    print("prose death without the typed key"); sys.exit(2)
if _FAKE_MODE == "budget_lane":
    # est-bbfy: a lane that burns ONE turn (one api_call + one tool_call) every
    # FAKE_BUDGET_TICK seconds, growing its state.db session row LIVE so the
    # runner can watch consumed turns approach the cap through the same
    # child_metrics join that proves liveness. FAKE_BUDGET_STOP_AT=<n>: the
    # early-finish twin — answer cleanly at n turns (exit 0, no report).
    # Otherwise die at the hard cap with the loop's typed stamp (exit 2).
    import sqlite3 as _sql_bc
    _mt = int(args[args.index("--max-turns") + 1]) if "--max-turns" in args else 60
    _stop_at = int(os.environ.get("FAKE_BUDGET_STOP_AT") or 0)
    _tick = float(os.environ.get("FAKE_BUDGET_TICK") or 0.25)
    _title = args[args.index("--continue") + 1] if "--continue" in args else "bc-x"
    def _bc_row(n):
        c = _sql_bc.connect(os.path.join(_FAKE_HOME, "state.db"))
        c.execute("create table if not exists sessions (id text primary key, title text, model text, billing_provider text, input_tokens int, output_tokens int, "
                  "cache_read_tokens int, reasoning_tokens int, api_call_count int, tool_call_count int, estimated_cost_usd real, "
                  "last_activity_at real, last_activity_description text, ended_at real, started_at real)")
        t = time.time()
        c.execute("insert or replace into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ("f-" + _title, _title, "fake", "fake-provider", 0, 0, 0, 0, n, n, 0.0, t, "", t, t))
        c.commit(); c.close()
    _n = 0
    while True:
        _n += 1
        _bc_row(_n)
        if _stop_at and _n >= _stop_at:
            print("```json\n" + json.dumps({"result": "finished-before-cap"}) + "\n```")
            sys.exit(0)
        if _n > _mt:
            _rp = os.environ.get("HERMES_QUIET_TURN_REPORT_FILE")
            if _rp:
                with open(_rp, "w") as f:
                    json.dump({"pid": os.getpid(), "exit_code": 2, "error": "budget",
                               "reply": "partial answer before the cap",
                               "turn_exit_reason": "max_iterations_reached(%d/%d)" % (_n, _mt)}, f)
            print("partial answer before the cap")
            sys.exit(2)
        time.sleep(_tick)
if _FAKE_MODE == "toolcall_text_541" and "TOOLCALL541" in q:
    # est-2ek.1.541: the agent child whose FINAL REPLY is serialized tool-call markup
    # ('<invoke name=...>'-shape rendered as text, turn_exit_reason unknown, exit 1).
    # The markup is json-quoted so this file stays free of raw open-tag sequences
    # (6st law). FAKE_RC selects the exit code (0 = the exit-0 shape, 1 = the death);
    # FAKE_TERN is the turn report's turn_exit_reason (this shape: "unknown"). The dead
    # attempt registers a state.db row with api_calls/tool_calls > 0 — the shape IS a
    # turn that really called tools and rendered the call as its answer; that is also
    # the positive tool-progress evidence the #5 bounded retry gate reads.
    # FAKE_ATTEMPT_DIR: first spawn dies malformed, later spawns answer valid (the
    # re-drive-success shape); FAKE_ALWAYS=1: every spawn dies malformed (budget pin).
    cnt = 0
    if os.environ.get("FAKE_ATTEMPT_DIR"):
        os.makedirs(os.environ["FAKE_ATTEMPT_DIR"], exist_ok=True)
        p = os.path.join(os.environ["FAKE_ATTEMPT_DIR"], "attempts")
        cnt = int(open(p).read()) if os.path.exists(p) else 0
        open(p, "w").write(str(cnt + 1))
    if cnt == 0 or os.environ.get("FAKE_ALWAYS") == "1":
        if "--continue" in args:   # dead attempt made real tool calls
            import sqlite3
            title = args[args.index("--continue") + 1]
            c = sqlite3.connect(os.path.join(_FAKE_HOME, "state.db"))
            c.execute("create table if not exists sessions (id text primary key, title text, model text, billing_provider text, input_tokens int, output_tokens int, "
                      "cache_read_tokens int, reasoning_tokens int, api_call_count int, tool_call_count int, estimated_cost_usd real, "
                      "last_activity_at real, last_activity_description text, ended_at real, started_at real)")
            t = time.time()
            c.execute("insert or replace into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("f-" + title, title, "fake", "fake-provider", 100, 20, 0, 0, 2, 3, 0.0, t, "", t, t))
            c.commit(); c.close()
        # opt-in hold so a pin can register this fake as a verified LIVE orphan
        # (adoption-path pins); default 0 leaves every existing pin unchanged.
        _hold = float(os.environ.get("FAKE_DELAY", "0") or 0)
        if _hold:
            time.sleep(_hold)
        reply = json.loads('"\\u003cinvoke name=\\"process_manage\\">\\n\\u003cparameter name=\\"action\\">poll\\u003c/parameter>\\n\\u003cparameter name=\\"session_id\\">lane-7\\u003c/parameter>\\n\\u003c/invoke>"')
        rc = int(os.environ.get("FAKE_RC", "1"))
        if rc != 0:
            _rp = os.environ.get("HERMES_QUIET_TURN_REPORT_FILE")
            if _rp:
                with open(_rp, "w") as f:
                    json.dump({"pid": os.getpid(), "exit_code": rc, "error": "",
                               "reply": reply,
                               "turn_exit_reason": os.environ.get("FAKE_TERN", "unknown")}, f)
        print(reply)
        sys.exit(rc)
    print("```json\n" + json.dumps({"result": "redriven"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "cap_harvest_ggqs" and "CAPHARV" in q:
    # est-ggqs (#256 claim 1): the turn-cap death that ALREADY printed a schema-valid
    # fenced answer — typed max_iterations stamp in the -Q turn report, exit 2, so the
    # runner commits partial + error_class=cap_exhausted via _harvest_death.
    # Drive 1 registers a state.db row with tool_call_count > 0 (the bounded-retry
    # progress evidence) unless FAKE_NO_PROGRESS=1. FAKE_ATTEMPT_DIR counts drives:
    # drive 2+ answers clean (exit 0) — unless FAKE_REDRIVE_DIES=1 (drive 2+ dies with
    # NO answer, transport marker). FAKE_REMAINING=1 declares a `## Remaining` tail;
    # FAKE_DECLARED=<s> stamps a child-declared terminal status into the answer.
    cnt = 0
    if os.environ.get("FAKE_ATTEMPT_DIR"):
        os.makedirs(os.environ["FAKE_ATTEMPT_DIR"], exist_ok=True)
        p = os.path.join(os.environ["FAKE_ATTEMPT_DIR"], "attempts")
        cnt = int(open(p).read()) if os.path.exists(p) else 0
        open(p, "w").write(str(cnt + 1))
    if cnt == 0:
        if os.environ.get("FAKE_NO_PROGRESS") != "1" and "--continue" in args:
            import sqlite3
            title = args[args.index("--continue") + 1]
            c = sqlite3.connect(os.path.join(_FAKE_HOME, "state.db"))
            c.execute("create table if not exists sessions (id text primary key, title text, model text, billing_provider text, input_tokens int, output_tokens int, "
                      "cache_read_tokens int, reasoning_tokens int, api_call_count int, tool_call_count int, estimated_cost_usd real, "
                      "last_activity_at real, last_activity_description text, ended_at real, started_at real)")
            t = time.time()
            c.execute("insert or replace into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("f-" + title, title, "fake", "fake", 0, 0, 0, 0, 35, 35, 0.0, t, "", t, t))
            c.commit(); c.close()
        _rp = os.environ.get("HERMES_QUIET_TURN_REPORT_FILE")
        if _rp:
            with open(_rp, "w") as f:
                json.dump({"pid": os.getpid(), "exit_code": 2, "error": "budget",
                           "reply": "census written; turn budget spent",
                           "turn_exit_reason": "max_iterations_reached(35/35)"}, f)
        if os.environ.get("FAKE_REMAINING") == "1":
            print("## Remaining")
            print("- reconcile the last two hosts")
            print("")
        _ans = {"result": "census-complete"}
        if os.environ.get("FAKE_DECLARED"):
            _ans["status"] = os.environ["FAKE_DECLARED"]
        print("```json\n" + json.dumps(_ans) + "\n```")
        sys.exit(2)
    if os.environ.get("FAKE_REDRIVE_DIES") == "1":
        print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
        sys.exit(2)
    print("```json\n" + json.dumps({"result": "redriven-clean"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "die_after_json" and "DIETEST" in q:   # #4: dies AFTER a valid fenced answer
    print("```json\n" + json.dumps({"result": "harvested"}) + "\n```")
    sys.exit(1)
if _FAKE_MODE == "die_after_json_blocked" and "DIETEST" in q:  # #4: dies with a declared terminal status
    print("```json\n" + json.dumps({"status": "BLOCKED", "result": "half done"}) + "\n```")
    sys.exit(1)
if _FAKE_MODE == "retry_progress" and "RESUME" in q:    # #5: transport death WITH tool progress, then resumes
    cnt = 0
    if os.environ.get("FAKE_ATTEMPT_DIR"):
        os.makedirs(os.environ["FAKE_ATTEMPT_DIR"], exist_ok=True)
        p = os.path.join(os.environ["FAKE_ATTEMPT_DIR"], "attempts")
        cnt = int(open(p).read()) if os.path.exists(p) else 0
        open(p, "w").write(str(cnt + 1))
    if cnt == 0:
        if "--continue" in args:   # dead attempt carries tool_call_count > 0 (bounded-retry gate)
            import sqlite3
            title = args[args.index("--continue") + 1]
            c = sqlite3.connect(os.path.join(_FAKE_HOME, "state.db"))
            c.execute("create table if not exists sessions (id text primary key, title text, model text, billing_provider text, input_tokens int, output_tokens int, "
                      "cache_read_tokens int, reasoning_tokens int, api_call_count int, tool_call_count int, estimated_cost_usd real, "
                      "last_activity_at real, last_activity_description text, ended_at real, started_at real)")
            t = time.time()
            c.execute("insert or replace into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("f-" + title, title, "fake", "fake", 0, 0, 0, 0, 1, 1, 0.0, t, "", t, t))
            c.commit(); c.close()
        print("Warning: Unknown toolsets: bogus")
        print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
        sys.exit(2)
    print("```json\n" + json.dumps({"result": "resumed"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "retry_hint":   # #130: transport death (0 api calls) that BANKED a work file;
    # the transient respawn answers ONLY when its prompt carried the artifact inventory.
    if "RESUME-HINT-ARTIFACT-130" in q and "size=" in q and "mtime=" in q:
        print("```json\n" + json.dumps({"result": "resumed-hint-130"}) + "\n```")
        sys.exit(0)
    try:
        with open("hint130.md", "w") as f:
            f.write("RESUME-HINT-ARTIFACT-130 banked before the transport death\n")
    except OSError:
        pass
    print("hermes -z: agent failed: openai.APIConnectionError. Connection error.")
    sys.exit(2)
if _FAKE_MODE == "lease_busy":
    # lease-busy (2026-10-03 field shape): attempt 1 prints the CLI's dead-session
    # banner + the CLI's fail-closed lease notice verbatim and exits 130 (the CLI's
    # lease-wait timeout exit). NO state.db row is written — the tool-progress gate
    # MUST be bypassed for this class. Attempt 2 answers only when its prompt carried
    # the fresh-session harvest preamble (proves the re-drive is a FRESH session,
    # not a resume of the busy one); without it, dies 130 again on the same notice.
    cnt = 0
    if os.environ.get("FAKE_ATTEMPT_DIR"):
        os.makedirs(os.environ["FAKE_ATTEMPT_DIR"], exist_ok=True)
        p = os.path.join(os.environ["FAKE_ATTEMPT_DIR"], "attempts")
        cnt = int(open(p).read()) if os.path.exists(p) else 0
        open(p, "w").write(str(cnt + 1))
    if cnt == 0 or "Dead-session re-drive harvest" not in q:
        print("Session 20261003_000000_fake00 found but has no messages. Starting fresh.", flush=True)
        print("Stopped waiting for another Hermes process on this session. "
              "Your message was not processed.", flush=True)
        sys.exit(130)
    print("```json\n" + json.dumps({"result": "lease-recovered"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "rc130_quiet":
    # control twin: exits 130 WITHOUT the lease notice — must keep the existing
    # `unknown` classification and stay terminal (no lease re-drive).
    print("some unrelated chatter, no lease marker")
    sys.exit(130)
if _FAKE_MODE == "dead_session_102" and "DEADSESS" in q:
    # #102: drive 1 dies on its wall leaving a session row with tool_call_count>0
    # (bounded-retry gate opens) and NO persisted messages when FAKE_MESSAGES=0;
    # its capture is the real burned-lane shape — startup banner + dead-session
    # noise + a discovery log tail. FAKE_MESSAGES=1 persists a message row (the
    # non-empty control: today's resume path must be untouched). Drive 2 only
    # answers when its prompt carried the banked-work harvest it needed.
    cnt = 0
    if os.environ.get("FAKE_ATTEMPT_DIR"):
        os.makedirs(os.environ["FAKE_ATTEMPT_DIR"], exist_ok=True)
        p = os.path.join(os.environ["FAKE_ATTEMPT_DIR"], "attempts")
        cnt = int(open(p).read()) if os.path.exists(p) else 0
        open(p, "w").write(str(cnt + 1))
    if cnt == 0:
        if "--continue" in args:
            import sqlite3
            title = args[args.index("--continue") + 1]
            c = sqlite3.connect(os.path.join(_FAKE_HOME, "state.db"))
            c.execute("create table if not exists sessions (id text primary key, title text, model text, billing_provider text, input_tokens int, output_tokens int, "
                      "cache_read_tokens int, reasoning_tokens int, api_call_count int, tool_call_count int, estimated_cost_usd real, "
                      "last_activity_at real, last_activity_description text, ended_at real, started_at real)")
            c.execute("create table if not exists messages (id integer primary key, session_id text, role text, content text)")
            t = time.time()
            c.execute("insert or replace into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("f-" + title, title, "fake", "fake", 0, 0, 0, 0, 1, 1, 0.0, t, "", None, t))
            if os.environ.get("FAKE_MESSAGES") == "1":
                c.execute("insert into messages (session_id, role, content) values (?,?,?)",
                          ("f-" + title, "user", q[:200]))
            c.commit(); c.close()
        # drive 1 banked real work + a discovery log tail, then died at the wall
        try:
            os.makedirs("work_state", exist_ok=True)
            with open("work_state/progress.md", "w") as f:
                f.write("BANKED-WORKFILE-102: findings banked before the kill\n")
        except OSError:
            pass
        print("  Command helper: applied 2 secrets", flush=True)
        if "--continue" in args:
            print("Session 20261001_000000_fake00 found but has no messages. Starting fresh.", flush=True)
        print("TAIL-LINE-102 discovery finished; about to start the real work", flush=True)
        _t.sleep(float(os.environ.get("FAKE_HANG_SEC", "30")))   # outlives the wall: the runner kills it (timeout)
        sys.exit(137)
    if "BANKED-WORKFILE-102" in q and "progress.md" in q:
        print("```json\n" + json.dumps({"result": "resumed-102"}) + "\n```")
        sys.exit(0)
    if os.environ.get("FAKE_MESSAGES") == "1":
        # non-empty control: today's path re-feeds the prompt with the standard
        # resume preamble (no harvest) — the fake answers, pinning that nothing
        # new rides this branch.
        print("```json\n" + json.dumps({"result": "resumed-102"}) + "\n```")
        sys.exit(0)
    print("no harvest received — redoing all discovery from zero")
    sys.exit(2)
from pathlib import Path
if _FAKE_MODE == "poll_steer":     # B1: child pulls baked steering through the real door
    # and prints what it got — proves the file protocol + cursor, not model compliance.
    ROOT40 = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(ROOT40))
    import importlib.util as _il
    _spec = _il.spec_from_file_location("hw_door_fake", ROOT40 / "__init__.py")
    _door = _il.module_from_spec(_spec); _spec.loader.exec_module(_door)
    _res = _door.act_inbox({})
    _g = _FAKE_POLL_SECS = float(os.environ.get("FAKE_POLL_SLEEP", "0"))
    if _g:
        _t.sleep(_g)
    print("PULLED:" + json.dumps(_res.get("steering", [])))
    # second pull must be empty: the cursor advances exactly-once per spawn
    _res2 = _door.act_inbox({})
    print("REPULL:" + json.dumps(_res2.get("steering", [])))
    print("```json\n" + json.dumps({"pulled": _res.get("steering", []), "repull": _res2.get("steering", [])}) + "\n```")
    sys.exit(0)
# ---- est-g2xx modes (appended; no behavior change without the env) ----
if _FAKE_MODE == "buffered_child":
    # The proven re-fire shape: the child WORKS but block-buffers stdout — its
    # spawn log stays 0 bytes past the silence window while a live tool child
    # runs in its subtree; only later does it answer.
    import subprocess as _sp2
    _tc = _sp2.Popen([sys.executable, "-c",
                      "import time; time.sleep(float(__import__('os').environ.get('FAKE_BUFFERED_SLEEP','5')) * 0.6)"])
    _t.sleep(float(os.environ.get("FAKE_BUFFERED_SLEEP", "5")))
    _tc.wait(timeout=30)
    print("```json\n" + json.dumps({"result": "ok", "buffered": True}) + "\n```", flush=True)
    sys.exit(0)
if _FAKE_MODE == "seat_timing":       # one span row per child: [start, end] of its run
    _s0 = time.time()
    _dur = float(os.environ.get("FAKE_SEAT_HOLD", "2"))
    _t.sleep(_dur)
    _sm = os.environ.get("SEAT_MARK")
    if _sm:
        with open(_sm, "a") as _f:
            _f.write(json.dumps({"start": round(_s0, 6), "end": round(time.time(), 6)}) + "\n")
    print("```json\n" + json.dumps({"result": "ok", "held": _dur}) + "\n```", flush=True)
    sys.exit(0)
if _FAKE_MODE == "hang":
    _t.sleep(float(os.environ.get("FAKE_HANG_SEC", "60")))
    sys.exit(0)
if _FAKE_MODE in ("session_pulse", "session_frozen"):   # est-2ek.1.595: the oneshot -Q shape —
    # nothing ever reaches the spawn log while the child works, but its own sessions row
    # (title = the --continue key) is in state.db. pulse: last_activity_at keeps moving;
    # frozen: the row exists but never moves again. Either way the answer comes at the end.
    import sqlite3 as _sq
    _title = args[args.index("--continue") + 1]
    _db = _sq.connect(os.path.join(_FAKE_HOME, "state.db"), timeout=5)
    _db.execute("create table if not exists sessions (id text primary key, title text, model text, billing_provider text, "
                "input_tokens int, output_tokens int, cache_read_tokens int, reasoning_tokens int, api_call_count int, "
                "tool_call_count int, estimated_cost_usd real, last_activity_at real, last_activity_description text, "
                "ended_at real, started_at real)")
    _t0 = time.time()
    _seen = _t0 - 3600 if _FAKE_MODE == "session_frozen" else _t0
    _db.execute("insert into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (_title, _title, "fake", None, 0, 0, 0, 0, 0, 0, 0.0, _seen, "", None, _seen))
    _db.commit()
    _end = _t0 + float(os.environ.get("FAKE_SESSION_SEC", "3"))
    while time.time() < _end:
        if _FAKE_MODE == "session_pulse":
            _db.execute("update sessions set last_activity_at=? where id=?", (time.time(), _title))
            _db.commit()
        _t.sleep(0.2)
    _db.close()
    print("```json\n" + json.dumps({"result": "ok", "session": _FAKE_MODE}) + "\n```", flush=True)
    sys.exit(0)
if _FAKE_MODE == "early":          # writes stdout, then keeps cooking (mid-run log growth)
    print("partial progress line, flushed early", flush=True)
    _t.sleep(float(os.environ.get("FAKE_EARLY_SLEEP", "3")))
if _FAKE_MODE == "bad_schema":     # valid json fence that fails the node's schema
    print("```json\n" + json.dumps({"wrong": True}) + "\n```")
    sys.exit(0)
# ---- #107 enum-enforcement modes (prompt-scoped; the retry seam is visible
# because the runner's attempt_note lands in the retry's prompt text) ----
if _FAKE_MODE == "enum_out_then_in":  # first answer out-of-set, retry answers in-set
    if "failed schema validation" in q:
        print("```json\n" + json.dumps({"verdict": "ship"}) + "\n```")
    else:
        print("```json\n" + json.dumps({"verdict": "shipp"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "enum_always_bad":   # persistent out-of-set answer
    print("```json\n" + json.dumps({"verdict": "passedd"}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "enum_always_good":  # in-set on the first attempt
    print("```json\n" + json.dumps({"verdict": "hold"}) + "\n```")
    sys.exit(0)
# ---- #96 minItems/minLength-enforcement modes (prompt-scoped like the enum
# ones: the runner's attempt_note lands in the retry's prompt text) ----
if _FAKE_MODE == "min_always_blank":   # persistent whitespace-only verify_list
    print("```json\n" + json.dumps({"verify_list": ["   "]}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "min_blank_then_good":  # first answer blank, retry names a real probe
    if "failed schema validation" in q:
        print("```json\n" + json.dumps({"verify_list": ["pytest -q"]}) + "\n```")
    else:
        print("```json\n" + json.dumps({"verify_list": ["   "]}) + "\n```")
    sys.exit(0)
# ---- #472 maxItems-cap modes (prompt-scoped like the enum/min ones: the
# runner's attempt_note lands in the retry's prompt text) ----
if _FAKE_MODE == "cap_always_bad":     # persistent roster over the declared cap
    print("```json\n" + json.dumps({"candidates": ["a", "b", "c"]}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "cap_over_then_good":  # over-cap first, retry inside the cap
    if "failed schema validation" in q:
        print("```json\n" + json.dumps({"candidates": ["a", "b"]}) + "\n```")
    else:
        print("```json\n" + json.dumps({"candidates": ["a", "b", "c"]}) + "\n```")
    sys.exit(0)
# ---- #113 numeric-type-enforcement modes (prompt-scoped like the enum ones:
# the runner's attempt_note lands in the retry's prompt text) ----
if _FAKE_MODE == "num_out_then_in":    # first answer fractional as integer, retry integral
    if "failed schema validation" in q:
        print("```json\n" + json.dumps({"count": 2}) + "\n```")
    else:
        print("```json\n" + json.dumps({"count": 1.5}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "num_always_bad":     # persistent fractional answer to an integer field
    print("```json\n" + json.dumps({"count": 1.5}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "num_bool_always_bad":  # persistent bool answer to an integer field
    print("```json\n" + json.dumps({"count": True}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "num_always_good":    # valid integer on the first attempt
    print("```json\n" + json.dumps({"count": 3}) + "\n```")
    sys.exit(0)
if _FAKE_MODE == "noisy":          # sprint101 C2 #9: prose around objects, no clean fence
    print("I finished the task. Early draft: {\"ok\": false, \"attempt\": 1}")
    print("```json\n{this fence is broken,,\n```")
    print("Final answer below — trust this one:")
    print("{\"ok\": true, \"attempt\": 2}")
    print("Thanks for reading!")
    sys.exit(0)
if "FAILME" in q:
    print("", end="")  # simulate a crashed/empty child
    sys.exit(0)
if "STEER-" in q:
    import re
    m = re.search(r"STEER-(\w+)", q)
    print("```json\n" + json.dumps({"steered": m.group(1)}) + "\n```")
    sys.exit(0)
if "LIST:" in q:
    print("```json\n" + json.dumps({"result": ["a", "b", "c"]}) + "\n```")
    sys.exit(0)
if "JSON:" in q:   # 'JSON:{...}' on the goal line -> echo that exact object (branch/prune tests)
    line = next(l for l in q.splitlines() if "JSON:" in l)
    print("```json\n" + line.split("JSON:", 1)[1].strip() + "\n```")
    sys.exit(0)
print("```json\n" + json.dumps({"result": "ok", "goal": q.splitlines()[0][:80]}) + "\n```")
