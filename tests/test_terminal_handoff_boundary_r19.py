#!/usr/bin/env python3
"""Regression: an amend after the final in-loop marker check survives > grace_s.

Fault-injects a 1.3s pause immediately before the failed-run parked verdict. The
door's 0.75s watcher returns while the runner is still alive; the terminal handoff
fence must then release liveness, observe restart.request, re-admit exactly once,
and finish without workflow wait/poll.
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SOURCE = Path(__file__).resolve().parent.parent
work = Path(tempfile.mkdtemp(prefix="wf-terminal-boundary-"))
copy = work / "plugin"
(copy / "tests").mkdir(parents=True)
for f in ("__init__.py", "card_enforcement.py", "card_ledger.py", "telemetry_observers.py", "gate_index.py", "wf.py", "wfcommon.py", "wf_dialect.py", "plugin.yaml"):
    shutil.copy2(SOURCE / f, copy / f)
for f in ("fake", "fake_hermes.py", "wf_test_isolation.py"):
    shutil.copy2(SOURCE / "tests" / f, copy / "tests" / f)

p = copy / "wf.py"
s = p.read_text()
needle = '            return "blocked by failed " + ",".join(n["id"] for n in failed)'
assert s.count(needle) == 1
s = s.replace(needle, '            (run / "after-terminal-check").write_text("1")\n'
                    '            time.sleep(1.3)\n' + needle)
p.write_text(s)

run = work / "workflows" / "handoff-probe"
run.mkdir(parents=True)
old = {"name": "probe", "nodes": [{"id": "n1", "type": "agent", "goal": "CRASHME boom"}]}
new = {"name": "probe", "nodes": [{"id": "n1", "type": "agent", "goal": "R19BOUNDARY step"}]}
(run / "graph.json").write_text(json.dumps(old))
(run / "run.json").write_text(json.dumps({"hermes_bin": str(copy / "tests" / "fake")}))
env = dict(os.environ, HERMES_HOME=str(work), WF_RUNS_ROOT=str(work / "workflows"),
           HERMES_WF_HERMES_BIN=str(copy / "tests" / "fake"), FAKE_LOG=str(work / "fake.log"))
for k in ("API_SERVER_KEY", "API_SERVER_HOST", "API_SERVER_PORT"):
    env.pop(k, None)
proc = subprocess.Popen([sys.executable, str(copy / "wf.py"), "run", run.name],
                        cwd=copy, env=env, stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT, text=True)
try:
    deadline = time.monotonic() + 25
    while not (run / "after-terminal-check").exists() and time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        time.sleep(0.03)
    assert (run / "after-terminal-check").exists(), "runner never reached injected terminal seam"

    os.environ.update({k: env[k] for k in
                       ("HERMES_HOME", "WF_RUNS_ROOT", "HERMES_WF_HERMES_BIN", "FAKE_LOG")})
    sys.path.insert(0, str(copy)); sys.path.insert(0, str(copy / "tests"))
    spec = importlib.util.spec_from_file_location("boundary_door", copy / "__init__.py")
    assert spec is not None and spec.loader is not None
    door = importlib.util.module_from_spec(spec); spec.loader.exec_module(door)
    import wf_test_isolation
    wf_test_isolation.install(door)
    res = door.act_amend({"run_id": run.name, "graph": new})
    output = proc.communicate(timeout=30)[0]
    time.sleep(0.2)
    ev = [json.loads(x)["event"] for x in (run / "events.jsonl").read_text().splitlines()]

    checks = {
        "amend accepted": bool(res.get("ok")),
        "restart marker consumed": not (run / "restart.request").exists(),
        "terminal handoff resumed": ev.count("run.resumed") == 1,
        "amended node ran once": ev.count("node.started") == 2,
        "run.done exactly once": ev.count("run.done") == 1,
        "runner exited done": json.loads((run / "runner_exit.json").read_text()).get("reason") == "done",
        "no wait instruction": "call wait" not in json.dumps(res).lower()
                               and "workflow wait" not in json.dumps(res).lower(),
    }
    for name, ok in checks.items():
        print(("PASS " if ok else "FAIL ") + name)
    if not all(checks.values()):
        print("events", ev)
        print("response", json.dumps(res))
        print("runner", output[-500:])
        raise SystemExit(1)
    print(f"ALL PASS ({len(checks)})")
finally:
    if proc.poll() is None:
        proc.kill(); proc.wait(timeout=5)
