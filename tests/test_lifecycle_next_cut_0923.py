#!/usr/bin/env python3
"""Lifecycle regressions: fresh exits, truthful steering, retry evidence, final items."""
# Ledger 95d7010295d70102: stamped fixtures verify under their own rule; ambiguous unstamped records fail closed.
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wfcommon = load("lifecycle_wfcommon", ROOT / "wfcommon.py")
door = load("lifecycle_door", ROOT / "__init__.py")
engine = load("lifecycle_engine", ROOT / "wf.py")
FAKE = str(HERE / "fake")
checks = 0
failures = 0


def check(label, condition, detail=""):
    global checks, failures
    checks += 1
    if condition:
        print(f"PASS {label}")
    else:
        failures += 1
        print(f"FAIL {label}: {detail}")


with tempfile.TemporaryDirectory(prefix="lifecycle-next-", dir=HERE) as td:
    home = Path(td) / "home"
    runs = home / "workflows"
    runs.mkdir(parents=True)
    os.environ["HERMES_HOME"] = str(home)
    os.environ["WF_RUNS_ROOT"] = str(Path(os.environ["HERMES_HOME"]) / "workflows")  # est-2ek.1.762 pin: HERMES_HOME alone is not a sandbox
    fake_log = Path(td) / "fake.log"
    os.environ["FAKE_LOG"] = str(fake_log)

    def make_run(run_id, nodes, **meta):
        r = runs / run_id
        (r / "nodes").mkdir(parents=True)
        (r / "gates").mkdir()
        (r / "graph.json").write_text(json.dumps({"name": run_id, "nodes": nodes}))
        config = {"hermes_bin": FAKE, "concurrency": 1, "item_concurrency": 1,
                  "node_timeout": 30, "retry_backoff": [0, 0]}
        config.update(meta)
        (r / "run.json").write_text(json.dumps(config))
        return r

    def run_engine(run_id, extra_env=None):
        env = dict(os.environ, HERMES_HOME=str(home), WF_RUNS_ROOT=str(runs), FAKE_LOG=str(fake_log), **(extra_env or {}))
        return subprocess.run([sys.executable, str(ROOT / "wf.py"), "run", run_id],
                              env=env, capture_output=True, text=True, timeout=90)

    # Missing metrics are not proof of zero API calls; no retry without affirmative evidence.
    fake_log.write_text("")
    r = make_run("retry-missing", [{"id": "a", "type": "agent", "goal": "unknown"}])
    p = run_engine("retry-missing", {"FAKE_MODE": "unknown"})
    rec = json.loads((r / "nodes" / "a.json").read_text())
    check("runner process exit captured", p.returncode == 0, f"rc={p.returncode}; {p.stdout} {p.stderr}")
    check("missing API-call evidence fails closed", rec.get("error_class") == "unknown"
          and not rec.get("attempts_log") and fake_log.read_text().count("unknown") == 1,
          json.dumps(rec)[:400])

    # A real row with an explicit zero is different from no row at all.
    db = home / "state.db"
    c = sqlite3.connect(db)
    c.execute("create table sessions (id text, title text, model text, input_tokens int, output_tokens int, "
              "cache_read_tokens int, reasoning_tokens int, api_call_count int, tool_call_count int, "
              "estimated_cost_usd real, last_activity_at real, last_activity_description text, ended_at real, started_at real)")
    key = f"wf:{r.name}:a:deadbeef.abcdef"
    now = time.time()
    c.execute("insert into sessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
              ("known-zero", key + "#a0", "fake", 0, 0, 0, 0, 0, 0, 0.0, now, "", now, now))
    c.commit()
    c.close()
    m = wfcommon.child_metrics(r.name, home).get(key)
    check("joined zero-call row is affirmative retry evidence",
          m is not None and m.get("api_calls") == 0 and m.get("api_calls_known") is True,
          json.dumps(m))

    # Stale done verdicts must not be presented as current after graph amendment.
    old_graph = {"name": "exit-fresh", "nodes": [{"id": "a", "type": "agent", "goal": "first"}]}
    r = make_run("exit-fresh", old_graph["nodes"])
    p = run_engine("exit-fresh")
    old_exit = json.loads((r / "runner_exit.json").read_text())
    check("runner exit has a current-graph fingerprint",
          bool(old_exit.get("graph_fingerprint")), json.dumps(old_exit))
    amended = json.loads((r / "graph.json").read_text())
    amended["nodes"][0]["goal"] = "amended"
    (r / "graph.json").write_text(json.dumps(amended))
    with (r / "events.jsonl").open("a") as f:
        f.write(json.dumps({"event": "graph.amended", "ts": "later"}) + "\n")
    st = wfcommon.run_state(r)
    check("amended run does not expose old done as its current runner verdict",
          st["status"] == "interrupted" and (st.get("runner_exit") or {}).get("reason") != "done",
          json.dumps({"status": st["status"], "runner_exit": st.get("runner_exit")}))

    # Terminal, failed, pruned, and stopped targets cannot accept stale steering.
    for state in ("done", "failed", "skipped", "stopped"):
        r = make_run("steer-" + state, [{"id": "a", "type": "agent", "goal": "x"}])
        graph = json.loads((r / "graph.json").read_text())
        byid = {n["id"]: n for n in graph["nodes"]}
        if state != "stopped":
            node_rec = {"status": state, "efp": wfcommon.efp(byid, byid["a"]), "fp_rule_version": wfcommon.FP_RULE_VERSION}
            (r / "nodes" / "a.json").write_text(json.dumps(node_rec))
        events = [{"event": "run.stopped" if state == "stopped" else "run.done"}]
        (r / "events.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n")
        ans = door.act_steer({"run_id": r.name, "node": "a", "text": "stale nudge"})
        check(f"steer {state} rejected without queueing",
              ans.get("ok") is False and not (r / "inbox.jsonl").exists(), json.dumps(ans))

    # Every fan-out item reaches a terminal event, even if stop prevents its launch.
    fake_log.write_text("")
    r = make_run("fanout-stop", [{"id": "f", "type": "agent", "fanout": {
        "items": ["one", "two", "three"], "goal": "SLEEP 30 item {item}"}}])
    env = dict(os.environ, HERMES_HOME=str(home), WF_RUNS_ROOT=str(runs), FAKE_LOG=str(fake_log))
    proc = subprocess.Popen([sys.executable, str(ROOT / "wf.py"), "run", r.name], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    deadline = time.time() + 30
    while time.time() < deadline:
        events = [json.loads(line) for line in (r / "events.jsonl").read_text().splitlines()] if (r / "events.jsonl").exists() else []
        if any(e.get("event") == "item.started" for e in events):
            break
        time.sleep(0.05)
    else:
        raise AssertionError("runner never launched the first fan-out item")
    (r / "stop.request").write_text("1")
    out, _ = proc.communicate(timeout=60)
    events = [json.loads(line) for line in (r / "events.jsonl").read_text().splitlines()]
    finished = [e for e in events if e.get("event") == "item.finished"]
    check("fan-out emits one final record per item after stop",
          len(finished) == 3 and {e.get("index") for e in finished} == {0, 1, 2},
          f"rc={proc.returncode}; out={out[-300:]}; events={finished}")
    not_launched = [e for e in finished if e.get("index") in (1, 2)]
    check("unlaunched items are explicitly terminal with zero attempts",
          len(not_launched) == 2 and all(e.get("status") == "failed" and e.get("attempts") == 0
                                         and "before launch" in (e.get("error") or "")
                                         for e in not_launched),
          json.dumps(not_launched))
    final = json.loads((r / "nodes" / "f.json").read_text())
    check("committed all_results exposes item attempts",
          len(final.get("output", {}).get("all_results", [])) == 3
          and all("attempts" in x for x in final["output"]["all_results"]),
          json.dumps(final.get("output"))[:500])

    # The retry wrapper reports total launches, not just the final child process.
    run_stub = Path(td) / "retry-wrapper"
    run_stub.mkdir()
    meta = {"_run": run_stub, "_stop": threading.Event(), "_procs_lock": threading.Lock(),
            "_retries_left": 6, "retry_backoff": [0, 0],
            # est-9ceqc: this wrapper check counts IN-PLACE respawns — pin the
            # defer=False cell shape; base_seconds=0 (door channel) keeps the
            # loop sleeping-free. The deferred shape is pinned elsewhere.
            "retry_policy": {"base_seconds": 0}}
    original = engine._attempt_api_calls
    engine._attempt_api_calls = lambda _run, _key: 0
    calls = []
    def retry_spawn():
        calls.append(1)
        return {"status": "done", "skey": f"spawn-{len(calls)}", "attempts": 1,
                "spawn": len(calls), "output": {"ok": True}}
    try:
        first = {"status": "failed", "error_class": "unknown", "skey": "first", "attempts": 1, "spawn": 0}
        retried = engine._transient_retry(meta, first, retry_spawn, "item", {"node": "f", "index": 0},
                                          defer=False)
    finally:
        engine._attempt_api_calls = original
    check("retry result exposes total attempts across transient respawns",
          retried.get("attempts") == 2 and len(retried.get("attempts_log", [])) == 1,
          json.dumps(retried))

print(f"{'ALL PASS' if failures == 0 else f'{failures} FAILED'} ({checks})")
sys.exit(0 if failures == 0 else 1)
