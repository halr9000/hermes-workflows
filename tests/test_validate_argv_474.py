#!/usr/bin/env python3
"""#474: `wf.py validate [--json] <graph.json>` — the documented lint entrypoint exists,
is the SAME door pipeline as `workflow {action: "validate"}`, and writes nothing.

The field shape (issue #474, three independent fix crews, 2026-10-19): authoring
instructions point at a `wf.py validate` argv that the shipped build did not have, so
crews either imported the door's `act_validate` or reached the tool — awkward for
headless CI. The ask is one command with NO new behavior, which pins three laws:

  A1 ONE PIPELINE: the argv path spec-loads the sibling door and calls `act_validate`
     verbatim, so its verdict must be DEEP-EQUAL to the tool's for the same bytes —
     a second validator would drift from the door on the first fix, which is the whole
     failure class this issue is about.
  A2 EXIT-CODE CONTRACT: 0 = ok:true, 1 = red verdict (the errors[] envelope prints),
     2 = usage/IO fault (no verdict to report). A CI lane must be able to gate on the
     number alone.
  A3 READ-ONLY: no run dir, no liveness ping, no writes — the runs root and the graph
     file are byte/mtime-identical after a validate, green or red.

Run (suite-style, fresh private root per invocation):
  env HERMES_HOME=$(mktemp -d) WF_RUNS_ROOT=$(mktemp -d) \
     python3 tests/test_validate_argv_474.py
"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
WF = ROOT / "wf.py"

GOOD = {"name": "argv-474", "nodes": [
    {"id": "solo", "type": "agent", "goal": "Say hi.", "shape": "recon"}]}
BAD = {"name": "argv-474-bad", "nodes": [
    {"id": "solo", "type": "agent", "goal": "Say hi.", "after": ["ghost"]}]}


class ValidateArgv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="wf-474-argv-"))
        cls.env = dict(os.environ,
                       HERMES_HOME=str(cls.tmp / "home"),
                       WF_RUNS_ROOT=str(cls.tmp / "runs"),
                       HERMES_WF_HERMES_BIN=str(ROOT / "tests" / "fake"))
        cls.good = cls._write("good.json", GOOD)
        cls.bad = cls._write("bad.json", BAD)
        # In-process door for the A1 parity check, on the SAME private roots.
        spec = importlib.util.spec_from_file_location("argv474_door", ROOT / "__init__.py")
        assert spec is not None and spec.loader is not None
        cls.door = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.door)

    @classmethod
    def tearDownClass(cls):
        import shutil
        shutil.rmtree(cls.tmp, ignore_errors=True)

    @staticmethod
    def _write(name, graph):
        p = Path(tempfile.mkdtemp(prefix="wf-474-graph-")) / name
        p.write_text(json.dumps(graph), encoding="utf-8")
        return p

    def _run(self, *args):
        return subprocess.run([sys.executable, str(WF), *args], capture_output=True,
                              text=True, env=self.env, cwd=str(ROOT), timeout=120)

    # ---- A2: the exit-code contract -------------------------------------------------

    def test_green_exits_zero(self):
        r = self._run("validate", str(self.good))
        self.assertEqual(r.returncode, 0, (r.stdout, r.stderr))
        self.assertIn("VALID ok", r.stdout)

    def test_red_verdict_exits_one_with_error_rows(self):
        r = self._run("validate", str(self.bad))
        self.assertEqual(r.returncode, 1, (r.stdout, r.stderr))
        self.assertIn("VALID fail", r.stdout)
        self.assertIn("ghost", r.stdout)          # the defect is NAMED, not just counted

    def test_io_fault_exits_two_never_a_false_green(self):
        missing = self.tmp / "nope.json"
        cases = [
            ("validate", str(missing)),                 # unreadable file
            ("validate", "--plain", str(self.good)),    # unknown flag
            ("validate", str(self.good), str(self.bad)),  # two graphs
            ("validate", "--json"),                     # no graph at all
        ]
        for args in cases:
            r = self._run(*args)
            self.assertEqual(r.returncode, 2, (args, r.stdout, r.stderr))
            self.assertNotIn("VALID ok", r.stdout)
            self.assertNotIn('"ok": true', r.stdout)

    def test_usage_line_advertises_validate(self):
        r = self._run("nope-verb", "x")
        self.assertEqual(r.returncode, 2, (r.stdout, r.stderr))
        self.assertIn("wf.py validate [--json] <graph.json>", r.stdout)

    def test_json_flag_before_and_after_the_path(self):
        for args in (("--json", str(self.good)), (str(self.good), "--json")):
            r = self._run("validate", *args)
            self.assertEqual(r.returncode, 0, (args, r.stdout, r.stderr))
            self.assertTrue(json.loads(r.stdout)["ok"])

    # ---- A1: same pipeline, byte-for-byte ------------------------------------------

    def test_json_envelope_matches_the_tool_verdict(self):
        for graph_path in (self.good, self.bad):
            with self.subTest(graph=graph_path.name):
                r = self._run("validate", "--json", str(graph_path))
                cli = json.loads(r.stdout)
                tool = self.door.act_validate({"graph_path": str(graph_path)})
                self.assertEqual(cli, tool)   # deep-equal: one pipeline, no second validator

    # ---- A3: read-only --------------------------------------------------------------

    def test_validate_writes_nothing(self):
        runs_root = Path(self.env["WF_RUNS_ROOT"])
        before = sorted(p.name for p in self.tmp.rglob("*") if p.is_file())
        mtime = self.good.stat().st_mtime_ns
        for rc, path in ((0, self.good), (1, self.bad)):
            self.assertEqual(self._run("validate", str(path)).returncode, rc)
        self.assertFalse(runs_root.exists(), "validate created a runs root")
        after = sorted(p.name for p in self.tmp.rglob("*") if p.is_file())
        self.assertEqual(before, after)
        self.assertEqual(self.good.stat().st_mtime_ns, mtime)

    def test_library_envelope_validates_its_graph_body(self):
        env_path = self._write("envelope.json", {"meta": {"name": "env474"}, "graph": GOOD})
        r = self._run("validate", str(env_path))
        self.assertEqual(r.returncode, 0, (r.stdout, r.stderr))


if __name__ == "__main__":
    unittest.main(verbosity=2)
