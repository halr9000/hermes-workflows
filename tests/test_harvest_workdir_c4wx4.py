#!/usr/bin/env python3
"""est-c4wx4 — answer-first closing: adopt <child_work_dir>/answer.json at death.

The disease (ra-work 20261023-142210 fix node 4x, ra-pr 20261023-202025 review
node 2x): a suite-bearing lane commits all its work, has its complete answer
on disk, and dies at the wall before emitting the fenced block — the runner
dead-lettered an answer that was already written. The cure: at the deaths that
consult _harvest_death (wall / cap_exhausted / rc!=0, spawn AND adopted paths;
NEVER cancelled), when stdout carries no fence the runner adopts
<child_work_dir>/answer.json under _harvest_death's EXACT gate (parses to a
dict + validate() passes against the node schema), committing `partial` with
the death class unchanged and `harvest.source: "workdir"` so provenance is
machine-visible. Guards: mtime >= the dead attempt's spawn start (work dirs
persist across attempts — a stale answer.json is never adopted), regular file
only (no symlink/dir), unreadable/oversized/malformed = the death stands
unchanged. Writer half: the reply CONTRACT and WORK_DIR_NOTE carry the
answer-first sentence.

  U1  fresh valid object        -> {output, remaining, harvest.source=workdir}
  U2  fenced file content       -> same adoption (same fence law as stdout)
  U3  mtime < spawn start       -> None (stale across attempts, never adopted)
  U4  mtime == spawn start      -> adopted (the gate is >=)
  U5  schema violation          -> None
  U6  unparseable file          -> None
  U7  symlink                   -> None (regular-file-only)
  U8  a directory squats        -> None
  U9  oversized file            -> None (death unchanged)
  U10 spawn start unknown       -> None (fail-closed)
  U11 no schema                 -> adopted (validate() passes, same as stdout law)
  W1  CONTRACT carries the answer-first writer sentence; WORK_DIR_NOTE names
      {WORK_DIR}/answer.json (the per-item path rides the GOAL half — the
      fan-out byte-identity law forbids per-item interpolation after Inputs)
  W2  fan-out prompt tail after ## Inputs stays byte-identical across items
      (the writer sentence must not break the identity law)
  W3  a live prompt carries the ABSOLUTE <work_dir>/answer.json path
  I1  wall death: stub banks answer.json then sleeps past its wall
      -> nodes/lane.json partial, error_class=timeout, harvest.source=workdir,
         the output committed, the corpse still banked (#131 untouched)
  I2  quorum straggler with a fresh answer.json -> cancelled, never adopted

Run: python3 tests/test_harvest_workdir_c4wx4.py
"""
import json, os, re, subprocess, sys, tempfile, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = Path(os.environ.get("WF_TEST_BUILD") or HERE.parent)

sys.path.insert(0, str(BUILD))
import wf  # noqa: E402

ok, checks = True, []
def check(label, cond, detail=""):
    global ok
    checks.append(bool(cond))
    print(("PASS " if cond else "FAIL " ) + label + (f"  {detail}" if detail and not cond else ""))
    ok = ok and bool(cond)

# ---------- unit: the pure helper ----------
HW = getattr(wf, "_harvest_workdir", None)
check("U0 wf._harvest_workdir exists", HW is not None)

if HW is not None:
    UTMP = Path(tempfile.mkdtemp(prefix="wf-hwdir-"))
    SCHEMA = {"type": "object", "required": ["result"]}
    ANS = {"result": "answer-first", "notes": "written before the wall"}

    def mkfile(nid, content, index=None):
        run = UTMP / f"run-{nid}"
        wd = wf.child_work_dir(run, {"id": nid}, index)
        p = wd / "answer.json"
        if isinstance(content, bytes):
            p.write_bytes(content)
        elif content == "DIR":
            p.mkdir()
        elif content == "SYMLINK":
            target = wd.parent / "elsewhere.json"
            target.write_text(json.dumps(ANS))
            p.symlink_to(target)
        else:
            p.write_text(content)
        return run, p

    def harvest(nid, schema, spawn_started, index=None):
        run = UTMP / f"run-{nid}"
        return HW(run, {"id": nid}, index, schema, spawn_started)

    # U1 fresh valid object
    now_s = time.time()
    run, p = mkfile("u1", json.dumps(ANS))
    os.utime(p, (now_s, now_s))
    hv = harvest("u1", SCHEMA, now_s - 5)
    check("U1 fresh valid object adopts", bool(hv) and hv.get("output") == ANS
          and (hv.get("harvest") or {}).get("source") == "workdir"
          and hv.get("remaining") == [], json.dumps(hv)[:300] if hv else "None")
    check("U1 shape carries the three keys", bool(hv) and set(hv) == {"output", "remaining", "harvest"})

    # U2 fenced file content (the child wrote the fence itself)
    now_s = time.time()
    run, p = mkfile("u2", "prelude prose\n```json\n" + json.dumps(ANS) + "\n```\ntrailing")
    os.utime(p, (now_s, now_s))
    hv = harvest("u2", SCHEMA, now_s - 5)
    check("U2 fenced file content adopts (same fence law as stdout)",
          bool(hv) and hv.get("output") == ANS
          and (hv.get("harvest") or {}).get("source") == "workdir")

    # U3 stale: written 100 s BEFORE the dead attempt's spawn start
    run, p = mkfile("u3", json.dumps(ANS))
    os.utime(p, (now_s - 100, now_s - 100))
    check("U3 stale answer.json (mtime < spawn start) never adopted",
          harvest("u3", SCHEMA, time.time() - 10) is None)

    # U4 equality edge: mtime == spawn start passes (>=)
    run, p = mkfile("u4", json.dumps(ANS))
    mt = p.stat().st_mtime
    check("U4 mtime == spawn start adopts (gate is >=)", harvest("u4", SCHEMA, mt) is not None)

    # U5 schema violation
    run, p = mkfile("u5", json.dumps({"verdict": "no-result-key"}))
    check("U5 schema violation -> death unchanged", harvest("u5", SCHEMA, 0.0) is None)

    # U6 unparseable
    run, p = mkfile("u6", "this is not json at all\nand no fence either")
    check("U6 unparseable file -> death unchanged", harvest("u6", SCHEMA, 0.0) is None)

    # U7 symlink (target is a perfect answer — regular-file-only still refuses)
    run, p = mkfile("u7", "SYMLINK")
    check("U7 symlink never adopted", harvest("u7", SCHEMA, 0.0) is None)

    # U8 directory squat
    run, p = mkfile("u8", "DIR")
    check("U8 a directory squatting answer.json never adopted", harvest("u8", SCHEMA, 0.0) is None)

    # U9 oversized (over the est-gg96 scan cap)
    big = json.dumps({"result": "x" * (wf.HARVEST_SCAN_MAX_BYTES + 4096)})
    run, p = mkfile("u9", big)
    check("U9 oversized file -> death unchanged", harvest("u9", SCHEMA, 0.0) is None)

    # U10 unknown spawn start fails closed
    run, p = mkfile("u10", json.dumps(ANS))
    check("U10 unknown spawn start -> no adoption (fail-closed)", harvest("u10", SCHEMA, None) is None)

    # U11 no schema on the node: validate() passes, adoption rides
    run, p = mkfile("u11", json.dumps(ANS))
    check("U11 schemaless node adopts (same bar as the stdout law)",
          harvest("u11", None, 0.0) is not None)

# ---------- writer half: the prompt law ----------
check("W1 CONTRACT carries the answer-first writer sentence",
      "answer.json" in wf.CONTRACT and "durable" in wf.CONTRACT, wf.CONTRACT[:200])
check("W1b WORK_DIR_NOTE names the absolute answer path token",
      "{WORK_DIR}/answer.json" in wf.WORK_DIR_NOTE, wf.WORK_DIR_NOTE)

# ---------- integration harness (same shape as test_bank_the_corpse_131) ----------
HOME = Path(tempfile.mkdtemp(prefix="wf-hwdir-int-"))
RUNS = HOME / "workflows"
RUNS.mkdir()
STUB = HOME / "stub_answer_first.py"
STUB.write_text(f"#!{sys.executable}\n" + r'''
import json, os, sys, time
a = sys.argv[1:]
if "--query-file" not in a:
    sys.exit(0)
q = open(a[a.index("--query-file") + 1]).read()
ANS = {"result": "answer-first", "status": "done"}
if "AF_SLOW" in q.upper():
    open("answer.json", "w").write(json.dumps(ANS))   # cwd IS the child work dir
    print("progress line: answer banked", flush=True)
    time.sleep(60)
elif "AF_FAST" in q.upper():
    time.sleep(1.0)
    print("```json\n" + json.dumps(ANS) + "\n```", flush=True)
else:
    print("```json\n" + json.dumps({"result": "ok"}) + "\n```", flush=True)
''')
STUB.chmod(0o755)

def mk(run_id, nodes):
    r = RUNS / run_id
    (r / "nodes").mkdir(parents=True); (r / "gates").mkdir()
    (r / "graph.json").write_text(json.dumps({"name": run_id, "nodes": nodes}))
    (r / "run.json").write_text(json.dumps({"hermes_bin": str(STUB), "concurrency": 4}))
    return r

def wfrun(run_id):
    env = dict(os.environ, HERMES_HOME=str(HOME), WF_RUNS_ROOT=str(RUNS))
    return subprocess.run([sys.executable, str(BUILD / "wf.py"), "run", run_id],
                          env=env, text=True, capture_output=True, timeout=120)

# I1 — the disease itself: answer on disk, wall death, no stdout fence.
run = mk("af-i1", [{"id": "lane", "type": "agent", "goal": "AF_SLOW run the whole suite",
                   "schema": {"type": "object", "required": ["result"]}, "timeout": 4}])
wfrun("af-i1")
try:
    rec = json.loads((run / "nodes" / "lane.json").read_text())
except Exception:
    rec = {}
check("I1 wall death with answer.json commits partial (was: failed dead-letter)",
      rec.get("status") == "partial", json.dumps(rec)[:300])
check("I1 the death class stands unchanged", rec.get("error_class") == "timeout", str(rec.get("error_class")))
check("I1 harvest provenance names the workdir",
      (rec.get("harvest") or {}).get("source") == "workdir", json.dumps(rec.get("harvest")))
check("I1 the answer is the committed output", rec.get("output") == {"result": "answer-first", "status": "done"},
      json.dumps(rec.get("output"))[:200])
check("I1 the error names the harvest source", "harvested from workdir" in str(rec.get("error")), str(rec.get("error"))[:200])
check("I1 the corpse is still banked (#131 law untouched)", bool(rec.get("banked")))

# I2 — cancelled never adopts: quorum straggler banks answer.json, gets killed.
run = mk("af-i2", [{"id": "fo", "type": "agent",
                   "fanout": {"items": ["fast", "slow"], "goal": "{item} — AF_{item} the work",
                              "quorum": 1}}])
wfrun("af-i2")
recs = []
for f in (run / "nodes").glob("*.json"):
    try:
        recs.append(json.loads(f.read_text()))
    except Exception:
        pass
canx = [r for r in recs if r.get("error_class") == "cancelled"]
check("I2 the straggler died cancelled (harness alive)", bool(canx), json.dumps(recs)[:300])
check("I2 cancelled NEVER adopts the fresh answer.json",
      all((r.get("harvest") or {}).get("source") != "workdir" for r in recs))

# W2/W3 — live prompts: identity law holds, absolute path lands.
run = mk("af-w3", [{"id": "gen", "type": "agent", "goal": "GEN the list"},
                   {"id": "fw", "type": "agent", "after": ["gen"], "inputs": ["gen.result"],
                    "fanout": {"items": ["p", "q"], "goal": "{item} plain work"}}])
wfrun("af-w3")
pps = sorted((run / "logs").glob("fw.*.a0.prompt.md"))
check("W2 both fan-out prompts exist (harness alive)", len(pps) == 2, str([p.name for p in pps]))
if len(pps) == 2:
    tails = [p.read_text().split("## Inputs")[-1] for p in pps]
    check("W2 fan-out tail after ## Inputs stays byte-identical (identity law)",
          tails[0] == tails[1])
pp = run / "logs" / "gen.a0.prompt.md"
if pp.exists():
    wd = str((run / "work" / "gen").resolve())
    check("W3 the live prompt carries the ABSOLUTE <work_dir>/answer.json path",
          f"{wd}/answer.json" in pp.read_text(), pp.read_text()[-300:])
else:
    check("W3 the live prompt carries the ABSOLUTE <work_dir>/answer.json path", False, "prompt missing")

# W4 — the SITE LAW (deep review on #541, adversary finding #4): the PR may not
# claim parity it didn't implement. _harvest_death is consulted at NINE sites;
# the workdir sibling rides SEVEN; the two cancel-side sites stay unwired by I2
# law. Pinned by census over the real source, not by prose.
_src = (HERE.parent / "wf.py").read_text()
_dead_sites = len(re.findall(r"_harvest_death\(", _src)) - 1          # minus the def
_wd_sites = len(re.findall(r"_harvest_workdir\(", _src)) - 1          # minus the def
check("W4 nine _harvest_death consult sites (census, not prose)", _dead_sites == 9, str(_dead_sites))
check("W4 seven _harvest_workdir sites wired", _wd_sites == 7, str(_wd_sites))
_tree_gate = re.search(r"tree_seen, bool\(hv\)\)", _src)
check("W4 the #61 gate routes on hv AFTER the workdir fallback", _tree_gate is not None)
for _fn in ("_cancel_evidence", "_harvest_cancelled"):
    _m = re.search(rf"def {_fn}\(.*?(?=\ndef )", _src, re.S)
    check(f"W4 {_fn} stays unwired (cancelled NEVER adopts)",
          _m is not None and "_harvest_workdir(" not in _m.group(0))

# W5 — the ANCHOR LAW (adversary finding #5): the adopt path's deadline math has
# always degraded to `now` on an unparseable child start; the est-c4wx4 mtime
# guard must NOT inherit that fallback (a now-anchor would let a previous
# attempt's file pass). It fails closed by passing None instead.
_a = re.search(r"def _adopt_child\(.*?(?=\ndef )", _src, re.S)
check("W5 _adopt_child found (census harness alive)", _a is not None)
_adopt = _a.group(0) if _a else ""
check("W5 unparseable spawn start -> harvest_anchor=None (fail closed)",
      re.search(r"except \(TypeError, ValueError\):\n(?:.*\n)*?\s+started_epoch = time\.time\(\)\n\s+harvest_anchor = None", _adopt) is not None)
check("W5 both adopted-path harvests pass harvest_anchor (never the time.time() fallback)",
      _adopt.count("harvest_schema, harvest_anchor)") == 2,
      str(_adopt.count("harvest_schema, harvest_anchor)")))

print("HARVEST-WORKDIR est-c4wx4:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
