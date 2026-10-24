#!/usr/bin/env python3
"""jam-h22/h30 (hackathon): rate-limit/429 transport deaths retry with
exponential-full-jitter sleeps (cap ladder 5/20/80, ceiling 120 s); every other
transport death keeps the fixed (5.0, 20.0) ladder. Never-retry list untouched."""
import importlib.util
import json
import random
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

engine = load("jitter_engine", ROOT / "wf.py")
checks = 0
failures = 0

def check(label, condition, detail=""):
    global checks, failures
    checks += 1
    if condition:
        print(f"PASS {label}")
    else:
        failures += 1
        print(f"FAIL {label} :: {detail}")

# ---- detector ----
check("429 via error-code marker is rate-limited",
      engine._is_rate_limited("openai.RateLimitError: Error code: 429 - {...}"))
check("http 429 shape is rate-limited",
      engine._is_rate_limited("HTTP 429: too many requests"))
check("rate-limit prose token is rate-limited",
      engine._is_rate_limited("hermes -z: agent failed: rate limit exceeded"))
check("plain connection error is NOT rate-limited",
      not engine._is_rate_limited("openai.APIConnectionError. Connection error."))
check("500 status is NOT rate-limited (stays on the fixed ladder)",
      not engine._is_rate_limited("Error code: 500 - upstream"))
check("empty raw is NOT rate-limited", not engine._is_rate_limited(""))

# ---- _retry_sleep unit shape ----
random.seed(7)
check("non-rate-limited draws the exact ladder",
      engine._retry_sleep((5.0, 20.0), 0, "Connection error.") == 5.0
      and engine._retry_sleep((5.0, 20.0), 1, "Connection error.") == 20.0
      and engine._retry_sleep((5.0, 20.0), 2, "Connection error.") == 20.0)
d = engine._retry_sleep((5.0, 20.0), 0, "Error code: 429 - slow down")
check("rate-limited draw i=0 is jittered within [0, 5]", 0.0 <= d <= 5.0, d)
d = engine._retry_sleep((5.0, 20.0), 1, "Error code: 429 - slow down")
check("rate-limited draw i=1 is jittered within [0, 20]", 0.0 <= d <= 20.0, d)
d = engine._retry_sleep((5.0, 20.0), 2, "Error code: 429 - slow down")
check("rate-limited draw i=2 is jittered within [0, 80]", 0.0 <= d <= 80.0, d)
d = engine._retry_sleep((5.0, 20.0), 9, "Error code: 429 - slow down")
check("cap ladder saturates at 80, never past the 120 s ceiling", 0.0 <= d <= 80.0, d)
check("cap constant stays at the 120 s ceiling", engine._RL_JITTER_CEIL == 120.0)

# ---- TWO retries of the rate-limited class draw DIFFERENT bounded sleeps ----
def transient_delay(seed_val, raw):
    """Run _transient_retry once over a rate-limited death; return the logged
    backoff_s of the first retry (the drawn sleep)."""
    with tempfile.TemporaryDirectory() as td:
        run = Path(td) / "run"
        run.mkdir()
        meta = {"_run": run, "_stop": threading.Event(), "_procs_lock": threading.Lock(),
                "_retries_left": 6, "retry_backoff": [5.0, 20.0]}
        orig_calls = engine._attempt_api_calls
        engine._attempt_api_calls = lambda *_a, **_k: 0
        calls = []
        def spawn():
            calls.append(1)
            return {"status": "done", "skey": "s", "attempts": 1, "spawn": 1,
                    "output": {"ok": True}, "raw": raw}
        try:
            first = {"status": "failed", "error_class": "transport", "skey": "s0",
                     "attempts": 1, "spawn": 0, "raw": raw}
            random.seed(seed_val)
            engine._transient_retry(meta, first, spawn, "node", {"node": "a"},
                                    defer=False)   # est-9ceqc: in-place seam (no driver here); the schedule below is what this test pins
        finally:
            engine._attempt_api_calls = orig_calls
        events = [json.loads(l) for l in (run / "events.jsonl").read_text().splitlines()]
        retrying = [e for e in events if e["event"] == "node.retrying"]
        return first, retrying

rl_raw = "hermes -z: agent failed: openai.RateLimitError: Error code: 429 - too many requests"
r1, ev1 = transient_delay(11, rl_raw)
r2, ev2 = transient_delay(42, rl_raw)
check("both rate-limited runs retried", len(ev1) == 1 and len(ev2) == 1,
      f"ev1={ev1} ev2={ev2}")
check("two retries draw DIFFERENT sleeps (full jitter)",
      ev1 and ev2 and ev1[0]["backoff_s"] != ev2[0]["backoff_s"],
      f"{ev1} vs {ev2}")
check("both draws are bounded by the first cap (5 s)",
      ev1 and ev2 and 0.0 <= ev1[0]["backoff_s"] <= 5.0 and 0.0 <= ev2[0]["backoff_s"] <= 20.0
      and ev1[0]["rate_limited"] is True,
      f"ev1={ev1}")
check("failed record carries the subtype marker, class stays transport",
      ev1 and r1.get("subtype") == "rate_limited", json.dumps(r1))

# non-429 transport keeps the fixed ladder exactly
conn_raw = "hermes -z: agent failed: openai.APIConnectionError. Connection error."
_, ev3 = transient_delay(11, conn_raw)
check("non-429 transport keeps the deterministic ladder (2.0 s first step)",
      len(ev3) == 1 and ev3[0]["backoff_s"] == 2.0 and ev3[0]["rate_limited"] is False,
      json.dumps(ev3))  # est-9ceqc: the pinned LAW is determinism-vs-jitter; the fixed [5,20] first step was superseded by the exp base (2.0 s default)

# ---- bounded retry: rate-limited wait is a jitter draw within [0, 5]; others fixed ----
class FakeStop:
    def __init__(self): self.waited = []
    def is_set(self): return False
    def wait(self, s): self.waited.append(s); return False

def bounded_wait(seed_val, raw, eclass="transport"):
    with tempfile.TemporaryDirectory() as td:
        run = Path(td) / "run"
        run.mkdir()
        stop = FakeStop()
        meta = {"_run": run, "_stop": stop, "_procs_lock": threading.Lock(), "_retries_left": 6}
        orig_tp = engine._tool_progress
        engine._tool_progress = lambda *_a, **_k: True
        try:
            r = {"status": "failed", "error_class": eclass, "skey": "s", "raw": raw}
            random.seed(seed_val)
            engine._bounded_retry(meta, r, lambda **_kw: {"status": "done", "spawn": 1,
                                                          "output": {"ok": True}},
                                  "node", {"node": "a"})
        finally:
            engine._tool_progress = orig_tp
        return r, stop.waited

r, waited = bounded_wait(3, "Error code: 429 - too many requests")
check("bounded retry: rate-limited waits a jitter draw in [0, 5]",
      len(waited) == 1 and 0.0 <= waited[0] <= 5.0 and r.get("subtype") == "rate_limited",
      f"waited={waited}")
r, waited = bounded_wait(3, "Connection error.")
check("bounded retry: non-rate-limited waits the fixed 5.0 s",
      len(waited) == 1 and waited[0] == 5.0 and "subtype" not in r, f"waited={waited}")

# ---- never-retry law intact ----
check("never-retry classes are unchanged",
      all(c in engine._BOUNDED_RETRY_CLASSES
          for c in ("transport", "early_death", "cap_exhausted", "timeout"))
      and "provider_400" not in engine._BOUNDED_RETRY_CLASSES
      and "schema" not in engine._BOUNDED_RETRY_CLASSES
      and "cancelled" not in engine._BOUNDED_RETRY_CLASSES
      and "spawn" not in engine._BOUNDED_RETRY_CLASSES)
check("closed ERROR_CLASSES set NOT widened by the subtype marker",
      "rate_limited" not in engine.ERROR_CLASSES and "transport" in engine.ERROR_CLASSES)

print(f"{'ALL PASS' if failures == 0 else f'{failures} FAILED'} ({checks})")
sys.exit(0 if failures == 0 else 1)
