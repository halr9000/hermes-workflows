#!/usr/bin/env python3
"""PR-5 telemetry observers + doctor budgets."""
import importlib.util, io, json, math, os, stat, sys, tempfile, contextlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAILS = []

def check(name, ok):
    print(("PASS " if ok else "FAIL ") + name)
    if not ok:
        FAILS.append(name)

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

tel = load("tel_t", ROOT / "telemetry_observers.py")
db = load("db_t", ROOT / "scripts/doctor_budgets.py")

class Common:
    def __init__(self, root, flag): self.root, self.flag = Path(root), flag
    def owner_setting(self, k): return self.flag if k == "telemetry" else None
    def runs_root(self): return self.root

def rows(root):
    out = []
    for f in sorted((Path(root) / "telemetry").glob("events-*.jsonl")):
        out += [json.loads(l) for l in f.read_text().splitlines()]
    return out

SECRET = "TOP-SECRET-SUMMARY"
with tempfile.TemporaryDirectory() as td:
    # flag off: zero writes, zero dir
    off = Path(td) / "off"; off.mkdir()
    tel.bind(Common(off, False))
    tel.on_subagent_stop(child_summary=SECRET, duration_ms=5)
    tel.on_skill_lifecycle(action="use", skill_name="x")
    check("flag-off: no telemetry dir, no writes", not (off / "telemetry").exists())
    class Ctx:
        def __init__(s): s.hooks = []
        def register_hook(s, n, f): s.hooks.append(n)
    c = Ctx(); tel.install_hooks(c)
    check("flag-off: no hooks registered", c.hooks == [])

    on = Path(td) / "on"; on.mkdir()
    tel.bind(Common(on, True))
    c = Ctx(); tel.install_hooks(c)
    check("flag-on: both hooks registered", sorted(c.hooks) == ["on_skill_lifecycle", "subagent_stop"])
    tel.on_subagent_stop(parent_session_id="p1", parent_turn_id="t", child_session_id="c1", child_role="leaf",
                         child_summary=SECRET, child_status="completed", duration_ms=1234,
                         tool_call_history=[{"tool_name": "write_file", "tool_input": {"targets": {"path": SECRET}}}] * 3)
    tel.on_skill_lifecycle(action="use", skill_name="sk", provenance={"a": SECRET}, task_id="t", session_id="s",
                           use_count=4, reused=True, reuse_after_patch=False)
    r = rows(on)
    check("subagent row keys", set(r[0]) == {"ts", "kind", "parent_session_id", "child_role", "child_status", "duration_ms", "tool_count"})
    check("subagent row values", (r[0]["tool_count"], r[0]["duration_ms"], r[0]["child_status"]) == (3, 1234, "completed"))
    check("skill row keys", set(r[1]) == {"ts", "kind", "action", "skill_name", "use_count", "reused"})
    check("skill row values", (r[1]["use_count"], r[1]["reused"], r[1]["skill_name"]) == (4, True, "sk"))
    raw = "".join(f.read_text() for f in (on / "telemetry").glob("*.jsonl"))
    check("privacy: no summary/history text in corpus", SECRET not in raw and "write_file" not in raw)
    f = next((on / "telemetry").glob("*.jsonl"))
    check("modes 0600 file / 0700 dir", stat.S_IMODE(f.stat().st_mode) == 0o600 and stat.S_IMODE(f.parent.stat().st_mode) == 0o700)

    # missing-field tolerance
    tel.on_subagent_stop(); tel.on_skill_lifecycle(); tel.on_subagent_stop(duration_ms="x", tool_call_history="no")
    r = rows(on)
    check("missing fields -> null, no raise", r[2]["duration_ms"] is None and r[2]["tool_count"] is None and r[3]["skill_name"] is None and r[4]["duration_ms"] is None)

    # size cap
    f.write_text("x" * tel.MAX_BYTES)
    before = f.stat().st_size
    tel.on_subagent_stop(duration_ms=1); tel.on_skill_lifecycle(action="a", skill_name="s")
    check("size cap: no growth, no raise", f.stat().st_size == before)

    # unwritable root: no raise
    tel.bind(Common("/proc/nope/x", True))
    try:
        tel.on_subagent_stop(duration_ms=1); ok = True
    except Exception:
        ok = False
    check("unwritable root: no raise", ok)

    # doctor budgets math
    root = Path(td) / "corpus"; (root / "telemetry").mkdir(parents=True)
    durs = [100 * i for i in range(1, 26)]       # 25 rows
    with open(root / "telemetry/events-20260101.jsonl", "w") as fh:
        for i, d in enumerate(durs):
            fh.write(json.dumps({"kind": "subagent", "child_role": "leaf", "child_status": "completed", "duration_ms": d, "tool_count": i}) + "\n")
        for d in (1, 2, 3):
            fh.write(json.dumps({"kind": "subagent", "child_role": "thin", "duration_ms": d, "tool_count": 1}) + "\n")
        for i in range(20):
            fh.write(json.dumps({"kind": "skill", "action": "use", "skill_name": "sk", "use_count": i, "reused": i % 4 == 0}) + "\n")
    for i in range(21):
        rd = root / f"run{i}"; (rd / "nodes").mkdir(parents=True)
        (rd / "graph.json").write_text(json.dumps({"nodes": [{"id": "a", "shape": "recon", "model": "m1"}]}))
        (rd / "nodes/a.json").write_text(json.dumps({"status": "done", "ms": (i + 1) * 1000}))
    presets = {"recon": {"max_turns": 100, "timeout": 2400}, "build": {"max_turns": 65, "timeout": 1500}}
    rep = db.collect(root, presets)
    exp = lambda vals, p: sorted(vals)[max(1, math.ceil(p / 100 * len(vals))) - 1]
    c = rep["subagents"]["leaf"]["duration_ms"]
    check("p50/p90 math (subagent duration)", (c["n"], c["p50"], c["p90"]) == (25, exp(durs, 50), exp(durs, 90)) == (25, 1300, 2300))
    check("tool_count cell", rep["subagents"]["leaf"]["tool_count"]["p90"] == exp(list(range(25)), 90))
    check("thin cell is insufficient(n)", rep["subagents"]["thin"]["duration_ms"]["p50"] == "insufficient(3)")
    check("skill reuse rate", rep["skills"]["sk"]["reuse_rate"] == round(5 / 20, 3))
    secs = [float(i + 1) for i in range(21)]
    nc = rep["nodes"]["recon/m1"]
    check("node cell p50/p90", (nc["n"], nc["p50"], nc["p90"]) == (21, exp(secs, 50), exp(secs, 90)))
    d = rep["presets_delta"]
    check("presets_delta recon", d["recon"]["n"] == 21 and d["recon"]["observed_p95_s"] == exp(secs, 95) and d["recon"]["delta_s"] == round(exp(secs, 95) - 2400, 1))
    check("presets_delta build insufficient", d["build"]["observed_p95_s"] == "insufficient(0)" and d["build"]["delta_s"] is None)
    txt = db.render(rep)
    check("render prints insufficient(n)", "insufficient(3)" in txt and "NOTHING is applied" in txt)

    # empty corpus exit 0 via CLI main
    emp = Path(td) / "empty"; emp.mkdir()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = db.main(["--runs-root", str(emp)])
    check("empty corpus: exit 0 and says what it needs", rc == 0 and "need:" in buf.getvalue())
    check("doctor is read-only (no dir created)", not (emp / "telemetry").exists())

    # door wiring
    door = load("hw_tel_door", ROOT / "__init__.py")
    # #71 r5: pin BOTH door roots (env + settings.runs_root) before any in-process door call.
    _pin_prev = os.environ.get("WF_RUNS_ROOT")
    os.environ["WF_RUNS_ROOT"] = str(Path(td) / "wf-runs-pin")
    sys.path.insert(0, str(ROOT / "tests"))
    import wf_test_isolation as _iso71
    _iso71.install(door)
    check("door: doctor action registered", "doctor" in door.ACTIONS)
    bad = json.loads(door.handle({"action": "doctor", "sub": "nope"}))
    check("door: unknown sub refused", "error" in bad)
    door._common.runs_root = lambda: emp
    out = json.loads(door.handle({"action": "doctor", "sub": "budgets"}))
    check("door: doctor budgets returns text+report", "text" in out and out.get("report", {}).get("runs_root") == str(emp))
    if _pin_prev is None:
        os.environ.pop("WF_RUNS_ROOT", None)
    else:
        os.environ["WF_RUNS_ROOT"] = _pin_prev

print("FAILED: %s" % FAILS if FAILS else "ALL PASS")
sys.exit(1 if FAILS else 0)
