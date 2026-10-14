#!/usr/bin/env python3
"""est-kg3y (case study 20261006, moves 1/2/4): the shelf-to-launch contract.

A library entry is a TEMPLATE when its committed bytes still reference {run.KEY}:
the .572 class is a launch whose ref'd slot was never validly bound (empty
SUITE_CMD inherited as a phantom command). The council decision table:

  (1) DOOR: a LIBRARY-SOURCED run refuses -- before any state write -- any
      surviving {run.X} across the SHARED render surface (_include_text_fields:
      goals/contexts/questions/profiles, fan-out goals/items, echo output,
      gate options[], wait.until_argv[]) AND empty/whitespace/non-string
      VALUES for keys the graph refs. "Required" means nonempty+valid, not
      merely present. Inline graphs keep the documented plain-graph leniency
      (the D19b byte law): the refusal is scoped to from=<library name>.
  (2) LIBRARY META: meta.params {key:{required,desc,default}} rides the
      envelope; the truth is DERIVED from the graph's refs (no second
      manifest); `library` with name=<name> and `/wf show <name>` print the
      required inputs + the exact instantiate command (name-only discovery
      in <=2 calls).
  (3) s12: an entry whose refs and declared params disagree is warned about
      at the save door (refs an uncontracted key => warning; dead declared
      param => named); dag_lint mirrors the predicate (skill copy).

Standalone script (house style): python3 tests/test_runctx_contract_kg3y.py
"""
import importlib.util
import json
import os
import shutil
import sys
from pathlib import Path

BUILD = Path(__file__).resolve().parent
ROOT = BUILD.parent if BUILD.name == "tests" else BUILD
HOME = BUILD / "home_runctx_kg3y"
shutil.rmtree(HOME, ignore_errors=True)
os.environ["HERMES_HOME"] = str(HOME)
os.environ["WF_RUNS_ROOT"] = str(HOME / "workflows")
# house convention (test_steer_live_40 idiom): a door test must never inherit
# the lane spawn env — est-2ek.1.599's shelf guard would judge saves against
# the LANE run dir, not this sandbox.
os.environ.pop("HERMES_WF_RUN_DIR", None)
spec = importlib.util.spec_from_file_location("kg3y_door", str(ROOT / "__init__.py"))
hw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hw)
import wf_test_isolation as _iso71; _iso71.install(hw)  # #71 r5 pin

spawns = []
hw._spawn_runner = lambda r: spawns.append(r)
# The sandbox library dir is NOT under this lane's run dir, so est-2ek.1.599's
# shelf guard would refuse every save here for a reason unrelated to the
# contract under test (the guard has its own suite, test_lane_shelf_guard_1599).
# Neutralise it for this script's saves only.
hw._lane_shelf_guard = lambda p: None

failures = []
def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else " " + str(detail)))
    if not ok:
        failures.append(name)

LIB = HOME / "workflows" / "library"
LIB.mkdir(parents=True, exist_ok=True)

def call(**args):
    return json.loads(hw.handle(args))

def dirs():
    d = HOME / "workflows"
    return sorted(p.name for p in d.iterdir() if p.name != "library") if d.exists() else []

# The template: every shared-surface family carries one ref (goal, context,
# gate question, gate options, wait argv, echo output, fan-out goal).
TEMPLATE = {"name": "tmpl", "nodes": [
    {"id": "a", "type": "agent", "goal": "run {run.SUITE} now",
     "context": "target {run.SUITE}", "shape": "recon"},
    {"id": "f", "type": "agent", "after": ["a"], "shape": "build",
     "fanout": {"items": ["i1"], "goal": "batch {run.JOB} {item}", "quorum": 1}},
    {"id": "g", "type": "gate", "after": ["f"], "question": "ship {run.SUITE}?",
     "options": ["{run.OPTION}", "no"], "wait": {"until_argv": ["echo", "{run.OPTION}"]}},
    {"id": "e", "type": "echo", "after": ["g"], "output": "verdict={run.VERDICT}"},
]}
ALL_KEYS = {"SUITE": "pytest", "JOB": "nightly", "OPTION": "ship", "VERDICT": "go"}

(LIB / "tmpl.json").write_text(json.dumps(TEMPLATE))

# ---------- 1. the .572 wedge: surviving refs on a library launch must refuse
before = (len(spawns), dirs())
r = call(**{"action": "run", "from": "tmpl"})
check("C1 library launch with NO run_context refuses (surviving refs)",
      "error" in r and "SUITE" in r.get("error", "") and (len(spawns), dirs()) == before,
      json.dumps(r)[:200])

# empty / whitespace / non-string VALUES for a ref'd key: refuse, nothing written.
for label, badval in (("empty", ""), ("whitespace", "   "), ("int", 4112),
                      ("bool", True), ("null", None), ("list", ["a"])):
    bad = dict(ALL_KEYS, SUITE=badval)
    before = (len(spawns), dirs())
    r = call(**{"action": "run", "from": "tmpl", "run_context": bad})
    check(f"C2 value for ref'd key must be non-empty string ({label})",
          "error" in r and "run_context" in r.get("error", "")
          and (len(spawns), dirs()) == before, json.dumps(r)[:160])

# missing key for a SHARED-SURFACE ref the map branch never rendered
# (gate options / until_argv / echo output): today these survive as literal
# placeholders in the committed bytes -- the PR#84 F-3 class, library side.
before = (len(spawns), dirs())
r = call(**{"action": "run", "from": "tmpl", "run_context": {"SUITE": "pytest", "JOB": "n"}})
check("C3 missing key for gate-options/argv/echo ref refuses (no survivor)",
      "error" in r and ("OPTION" in r.get("error", "") or "VERDICT" in r.get("error", ""))
      and (len(spawns), dirs()) == before, json.dumps(r)[:200])

# seed form against a ref-bearing library graph stays refused (already true;
# pinned so the library scoping never reopens it).
before = (len(spawns), dirs())
r = call(action="run", **{"from": "tmpl", "run_context": "just some prose"})
check("C4 seed form refuses against ref-bearing library graph",
      "error" in r and (len(spawns), dirs()) == before, json.dumps(r)[:160])

# full binding: LAUNCHES, and the committed bytes are fully rendered on EVERY
# shared surface (options/argv/echo were the survivors).
r = call(**{"action": "run", "from": "tmpl", "run_context": dict(ALL_KEYS)})
rid = r.get("run_id")
check("C5 fully-bound library launch starts", bool(rid), json.dumps(r)[:200])
if rid:
    snap = json.loads((HOME / "workflows" / rid / "graph.json").read_text())
    byid = {n["id"]: n for n in snap["nodes"]}
    check("C5 goal+context rendered",
          byid["a"]["goal"] == "run pytest now" and byid["a"]["context"] == "target pytest")
    check("C5 gate question rendered", byid["g"]["question"] == "ship pytest?")
    check("C5 gate options rendered (was survivor)",
          byid["g"]["options"] == ["ship", "no"], json.dumps(byid["g"])[:200])
    check("C5 wait.until_argv rendered (was survivor)",
          byid["g"]["wait"]["until_argv"] == ["echo", "ship"], json.dumps(byid["g"])[:200])
    check("C5 echo output rendered (was survivor)",
          byid["e"]["output"] == "verdict=go", json.dumps(byid["e"])[:200])
    check("C5 fan-out goal rendered",
          byid["f"]["fanout"]["goal"] == "batch nightly {item}", json.dumps(byid["f"])[:200])
    check("C5 committed bytes carry no surviving {run.*}", "{run." not in json.dumps(snap))
    call(action="stop", run_id=rid)

# dry_run on a ref-bearing library launch: same refusal, nothing written.
before = (len(spawns), dirs())
r = call(**{"action": "run", "from": "tmpl", "dry_run": True})
check("C6 dry_run refuses unbound library refs, touches nothing",
      "error" in r and (len(spawns), dirs()) == before, json.dumps(r)[:160])

# byte law: the leniency stays for INLINE graphs (only from= gets the door).
inline = json.loads(json.dumps(TEMPLATE))
inline["name"] = "inline-tmpl"
# #293 (est-2ek.1.792) refuses a surviving {run.KEY} in wait.until_argv for EVERY
# graph (exec'd argv, not a render surface): pin that, then drop the argv ref so
# C7 measures the leniency that remains on the other surfaces.
before = (len(spawns), dirs())
r = call(action="run", graph=inline)
check("C7a inline argv survivor still refused by the #293 argv law",
      "error" in r and "until_argv" in r.get("error", "") and (len(spawns), dirs()) == before,
      json.dumps(r)[:160])
inline["nodes"][2]["wait"]["until_argv"] = ["echo", "ship"]
r = call(action="run", graph=inline)
rid = r.get("run_id")
check("C7 inline graph keeps documented verbatim leniency", bool(rid), json.dumps(r)[:160])
if rid:
    snap = json.loads((HOME / "workflows" / rid / "graph.json").read_text())
    check("C7 inline committed bytes byte-verbatim (survivors kept, not library)",
          "{run." in json.dumps(snap))
    call(action="stop", run_id=rid)

# ---------- 2. meta.params surfacing + name-only discovery <=2 calls
declared = json.loads(json.dumps(TEMPLATE))
declared["name"] = "declared"
(LIB / "declared.json").write_text(json.dumps({
    "meta": {"description": "declared tmpl", "params": {
        "SUITE": {"desc": "the suite command"}, "JOB": {}, "OPTION": {}, "VERDICT": {},
        "UNUSED": {"desc": "declared, never referenced"}}},
    "graph": declared}))
(LIB / "bare-refs.json").write_text(json.dumps(
    {"name": "bare-refs", "nodes": [{"id": "a", "type": "agent", "goal": "go {run.THING}", "shape": "recon"}]}))
(LIB / "clean.json").write_text(json.dumps(
    {"name": "clean", "nodes": [{"id": "a", "type": "echo", "output": {"ok": True}}]}))

out = call(action="library")
rows = {x["name"]: x for x in out["library"]}
r_decl = rows.get("declared", {})
check("S1 list row surfaces params (refs-derived, required:true)",
      isinstance(r_decl.get("params"), dict)
      and set(r_decl["params"]) == {"SUITE", "JOB", "OPTION", "VERDICT"}
      and all(v.get("required") is True for v in r_decl["params"].values())
      and r_decl["params"]["SUITE"].get("desc") == "the suite command",
      json.dumps(r_decl)[:300])
check("S1b dead declared param named, not silently dropped",
      isinstance(r_decl.get("dead_params"), list) and "UNUSED" in r_decl["dead_params"],
      json.dumps(r_decl)[:300])
r_bare = rows.get("bare-refs", {})
check("S2 bare entry derives params from refs (no meta needed)",
      isinstance(r_bare.get("params"), dict) and set(r_bare["params"]) == {"THING"}
      and r_bare["params"]["THING"].get("required") is True, json.dumps(r_bare)[:250])
check("S3 ref-free BARE entry keeps the exact 1.1 row key-set (golden bytes)",
      set(rows["clean"]) == {"name", "nodes", "gates", "fanouts", "description"},
      json.dumps(rows.get("clean"))[:200])

# name-only detail: `library` action with name=<name> (<=2 calls from name
# to the exact launch command).
out = call(action="library", **{"name": "declared"})
det = out.get("entry") or out
check("S4 library name=<name> answers the required inputs",
      det.get("name") == "declared" and isinstance(det.get("params"), dict)
      and set(det["params"]) == {"SUITE", "JOB", "OPTION", "VERDICT"}, json.dumps(out)[:300])
hint = json.dumps(det)
check("S4b detail carries the exact instantiate command (run from=<name> run_context=...)",
      "run" in hint and "from" in hint and "run_context" in hint
      and all(k in hint for k in ("SUITE", "JOB", "OPTION", "VERDICT")), hint[:300])
r_unknown = call(action="library", **{"name": "no-such-graph"})
check("S4c unknown name refuses honestly", "error" in r_unknown, json.dumps(r_unknown)[:150])

# /wf show <name>: the operator-facing twin of the same discovery.
txt = hw._wf_command("show declared")
check("S5 /wf show <name> prints required inputs + instantiate command",
      "declared" in txt and "SUITE" in txt and "JOB" in txt
      and "OPTION" in txt and "VERDICT" in txt and "run_context" in txt, txt[:300])
txt_unknown = hw._wf_command("show no-such-graph")
check("S5b /wf show unknown names available names", "no-such-graph" in txt_unknown, txt_unknown[:150])

# ---------- 3. s12 at the save door: uncontracted refs => warning (never fatal)
g_uncontracted = {"name": "s12save", "nodes": [
    {"id": "a", "type": "agent", "goal": "run {run.SUITE}", "shape": "recon"}]}
r = call(action="save", graph=g_uncontracted, name="s12save", description="s12 probe")
check("S6 save lands BUT warns on refs an entry does not contract (s12)",
      r.get("saved") == "s12save" and "save_warnings" in r
      and "SUITE" in json.dumps(r.get("save_warnings")), json.dumps(r)[:300])

g2 = json.loads(json.dumps(g_uncontracted)); g2["name"] = "s12save2"
r = call(action="save", graph=g2, name="s12save2",
         description="s12 probe 2", params={"SUITE": {"desc": "suite cmd"}})
check("S6b save with params= declaring the ref'd key: NO s12 warning",
      r.get("saved") == "s12save2" and "error" not in r
      and not [w for w in (r.get("save_warnings") or []) if "SUITE" in str(w)],
      json.dumps(r)[:300])

g3 = json.loads(json.dumps(g_uncontracted)); g3["name"] = "s12save3"
r = call(action="save", graph=g3, name="s12save3",
         description="s13 probe", params={"SUITE": {"desc": "suite cmd"},
                                          "GHOST": {"desc": "never ref'd"}})
check("S6c save with a DEAD declared param: warn names it",
      r.get("saved") == "s12save3"
      and any("GHOST" in str(w) for w in (r.get("save_warnings") or [])),
      json.dumps(r)[:300])

# params persistence law: re-save without params carries the previous
# envelope's params forward (retain-on-overwrite, same #70 law as tags).
g2b = json.loads(json.dumps(g_uncontracted)); g2b["name"] = "s12save2"
r = call(action="save", graph=g2b, name="s12save2")
raw = json.loads((LIB / "s12save2.json").read_text())
check("S7 re-save retains meta.params (no silent discovery wipe)",
      isinstance((raw.get("meta") or {}).get("params"), dict)
      and "SUITE" in raw["meta"]["params"], json.dumps(raw.get("meta"))[:200])

# a clean ref-free save grows NO warning keys (golden-bytes law for save).
r = call(action="save", graph={"name": "s12clean",
                               "nodes": [{"id": "a", "type": "echo", "output": {"ok": True}}]},
         name="s12clean", description="clean")
check("S6d ref-free save keeps byte-identical response (no save_warnings)",
      r.get("saved") == "s12clean" and "save_warnings" not in r, json.dumps(r)[:250])

# malformed meta.params on a hand-edited file: fail-open on the library
# (entry still lists; declared params treated as absent, refs still derived).
(LIB / "malformed.json").write_text(json.dumps({
    "meta": {"description": "bad params", "params": ["not", "a", "map"]},
    "graph": {"name": "malformed", "nodes": [
        {"id": "a", "type": "agent", "goal": "go {run.X}", "shape": "recon"}]}}))
out = call(action="library")
rows = {x["name"]: x for x in out["library"]}
check("S8 malformed meta.params never quarantines the entry; params derive from refs anyway",
      "malformed" in rows and "malformed" not in json.dumps(out.get("skipped", []))
      and "malformed" not in json.dumps(out.get("quarantined", []))
      and set((rows["malformed"].get("params") or {})) == {"X"}, json.dumps(rows.get("malformed"))[:250])

# ---------- 2b. est-kg3y round 2: the instantiate surface must not mint a phantom.
# The printed instantiate command used to carry "<KEY>" for every required input
# with no default. That is a NON-EMPTY string, so the door accepted it and a
# copy-pasted command launched a literal placeholder (the .572 phantom class,
# re-entering through the instantiate surface). Unfilled inputs now print "" (the
# door refuses it by name), and an exact <KEY> placeholder for a ref'd key is
# refused on a library launch even when pasted from an older printout.
det = call(action="library", **{"name": "declared"})
ent = det.get("entry") or {}
ctx = (ent.get("instantiate") or {}).get("run_context") or {}
check("P1 instantiate prints no <KEY> placeholder value (phantom surface)",
      bool(ctx) and not any(isinstance(v, str) and v.startswith("<") and v.endswith(">")
                            for v in ctx.values()), json.dumps(ctx)[:200])
check("P1b entry names the unfilled required inputs (no default)",
      ent.get("unfilled") == ["JOB", "OPTION", "SUITE", "VERDICT"], json.dumps(ent)[:300])
before = (len(spawns), dirs())
legacy = {k: f"<{k}>" for k in ALL_KEYS}
r = call(**{"action": "run", "from": "declared", "run_context": legacy, "dry_run": True})
check("P2 pasted legacy <KEY> placeholders refused on a library launch (no write)",
      "error" in r and "placeholder" in r.get("error", "") and (len(spawns), dirs()) == before,
      json.dumps(r)[:200])
txt = hw._wf_command("show declared")
check("P3 /wf show gives each required input's desc and says to fill before running",
      "the suite command" in txt and "fill" in txt.lower() and "run_context" in txt, txt[:400])

# ---------- 2c. s12 as lint rows: validate reads a library envelope (dag_lint parity)
v = call(action="validate", graph_path=str(LIB / "declared.json"))
check("V1 validate reads an envelope path (no 'unknown graph key') and stays ok",
      v.get("ok") is True and "unknown graph key" not in json.dumps(v), json.dumps(v)[:300])
check("V1b validate names a dead declared param as an s12 warn row",
      any("s12 dead declared params: UNUSED" in str(w) for w in (v.get("warnings") or [])),
      json.dumps(v)[:300])
(HOME / "v-missing.json").write_text(json.dumps({
    "meta": {"description": "missing", "params": {"SUITE": {"desc": "s"}}},
    "graph": {"name": "vmissing", "nodes": [
        {"id": "a", "type": "agent", "goal": "run {run.SUITE} {run.JOB}", "shape": "recon"}]}}))
v2 = call(action="validate", graph_path=str(HOME / "v-missing.json"))
check("V2 validate names an uncontracted ref as an s12 warn row",
      any("s12 uncontracted run_context refs: JOB" in str(w) for w in (v2.get("warnings") or [])),
      json.dumps(v2)[:300])
v3 = call(action="validate", graph={"name": "bare", "nodes": [
    {"id": "a", "type": "agent", "goal": "run {run.SUITE}", "shape": "recon"}]})
check("V3 bare graph validate carries no warnings key (golden bytes)",
      "warnings" not in v3, json.dumps(v3)[:200])

# ---------- 4. engine-truth contract function (single predicate, shared)
check("E1 _common.run_context_contract is the shared predicate",
      callable(getattr(hw._common, "run_context_contract", None)))
if callable(getattr(hw._common, "run_context_contract", None)):
    c = hw._common.run_context_contract(TEMPLATE)
    check("E1b contract: refs derived across the shared surface",
          set(c["refs"]) == {"SUITE", "JOB", "OPTION", "VERDICT"}, json.dumps(c)[:250])
    c2 = hw._common.run_context_contract(
        {"name": "d", "nodes": [{"id": "a", "type": "agent", "goal": "go", "shape": "recon"}]},
        params={"S": {"desc": "x"}})
    check("E1c contract: dead param declared without any ref is named",
          c2["dead"] == ["S"], json.dumps(c2)[:200])

shutil.rmtree(HOME, ignore_errors=True)
print(f"{'ALL PASS' if not failures else 'FAILURES PRESENT'} ({len(failures)} failures: {failures})")
sys.exit(1 if failures else 0)
