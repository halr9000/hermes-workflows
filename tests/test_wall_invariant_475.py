#!/usr/bin/env python3
"""#475: hold_timeout vs lane-wall collision is graph-statically decidable — the
admission refuses a gate that can outlive EVERY lane of its own graph (new surfaces
only, R10), and the doors warn when a specific after-consumer's wall sits under a
gate's hold. The receipt: an owners lane (timeout 10800) shepherding human gates
that hold up to 86400 died mid-collection — structurally guaranteed, discovered
only at the wall."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))


def agent(nid, **kw):
    return dict(id=nid, type="agent", goal=f"Inspect {nid}", **kw)


class WallInvariant(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix="wf-wall-475-")
        cls.home = Path(cls.tmp.name)
        cls.env = patch.dict(os.environ, {
            "HERMES_HOME": str(cls.home),
            "WF_RUNS_ROOT": str(cls.home / "runs"),
            "HERMES_WF_HERMES_BIN": str(ROOT / "tests" / "fake"),
        })
        cls.env.start()
        spec = importlib.util.spec_from_file_location("wall_475_door", ROOT / "__init__.py")
        assert spec is not None and spec.loader is not None
        cls.door = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.door)
        import wf_test_isolation as _iso71
        _iso71.install(cls.door)
        warm = json.loads(cls.door.handle({"action": "validate", "graph": {
            "name": "warm", "nodes": [agent("warm")]}}))
        assert warm.get("ok") is True, warm

    @classmethod
    def tearDownClass(cls):
        cls.env.stop()
        cls.tmp.cleanup()

    def validate(self, graph):
        before = copy.deepcopy(graph)
        files = set(self.home.rglob("*"))
        result = json.loads(self.door.handle({"action": "validate", "graph": graph}))
        self.assertEqual(graph, before, "validation must not rewrite the caller's graph")
        self.assertEqual(set(self.home.rglob("*")), files, "validate must not write or spawn")
        return result

    # -- graphs ----------------------------------------------------------------

    def collide(self):
        # The receipt shape: a human gate that can hold 86400 while the widest
        # consumer lane in the graph is 10800 — guaranteed mid-collection death.
        return {"name": "collide", "nodes": [
            agent("prep", timeout=900),
            {"id": "owner_gate", "type": "gate", "after": ["prep"],
             "question": "Proceed?", "hold_timeout": 86400},
            agent("collect", after=["owner_gate"], timeout=10800),
        ]}

    def fixed(self):
        g = self.collide()
        g["nodes"][1]["hold_timeout"] = 10800  # hold <= max lane wall (boundary: equal is legal)
        return g

    def warn_pair(self):
        # Survives the max-wall law (a consumer lane matches the hold) but ONE
        # after-consumer's own wall sits under it.
        return {"name": "warn-pair", "nodes": [
            agent("prep", timeout=900),
            {"id": "owner_gate", "type": "gate", "after": ["prep"],
             "question": "Proceed?", "hold_timeout": 86400},
            agent("guard", after=["owner_gate"], timeout=86400),
            agent("quick_tail", after=["owner_gate"], timeout=3600),
        ]}

    def terminal_hold(self):
        # A gate nobody consumes: the hold blocks no lane — silent.
        return {"name": "terminal", "nodes": [
            agent("prep", timeout=900),
            {"id": "last_gate", "type": "gate", "after": ["prep"],
             "question": "Close?", "hold_timeout": 86400},
        ]}

    def hold_under_defaults_floor(self):
        # A graph-wide defaults.timeout RAISES every unfilled lane above the hold:
        # the resolver must consult defaults (visible on the graph-dict view) and
        # stay silent.
        return {"name": "defaults-floor",
                "defaults": {"timeout": 86400},
                "nodes": [
                    agent("prep"),
                    {"id": "owner_gate", "type": "gate", "after": ["prep"],
                     "question": "Proceed?", "hold_timeout": 86400},
                    agent("collect", after=["owner_gate"]),
                ]}

    # -- (a): the admission REFUSES a hold that outlives every lane --------------

    def test_admission_refuses_hold_over_graph_max_wall(self):
        res = self.validate(self.collide())
        self.assertFalse(res.get("ok"), res)
        rows = [e for e in res.get("errors", []) if e.get("field") == "hold_timeout"]
        self.assertEqual(len(rows), 1, res)
        self.assertEqual(rows[0]["node"], "owner_gate")
        for frag in ("86400", "10800", "max lane wall", "#475"):
            self.assertIn(frag, rows[0]["msg"])
        # One voice per defect: the advisory channel is the ERROR's domain here —
        # the door returned ok=False, so no warning rows surface.
        self.assertNotIn("warnings", res)

    def test_run_door_refuses_before_any_write(self):
        before = set(self.home.rglob("*"))
        res = json.loads(self.door.handle({"action": "run", "graph": self.collide()}))
        self.assertFalse(res.get("ok", "ok" in res and res["ok"]))
        self.assertIn("graph invalid", res.get("error", ""))
        self.assertIn("hold_timeout", json.dumps(res))
        self.assertEqual(set(self.home.rglob("*")), before,
                         "a refused launch writes no run dir")

    def test_persisted_net_admission_false_never_strands(self):
        # R10: the runner/respawn path re-validates persisted bytes WITHOUT the
        # admission flag — the same collision bytes must read clean there, or an
        # already-launched run would be stranded by a rule it was admitted under.
        g = self.collide()
        errs = self.door._common.validate_graph_errors(g["nodes"])
        self.assertEqual([e for e in errs if e.get("field") == "hold_timeout"], [], errs)
        # ...and the full graph-object form WITHOUT admission is equally net.
        errs = self.door._common.validate_graph_errors(copy.deepcopy(g))
        self.assertEqual([e for e in errs if e.get("field") == "hold_timeout"], [], errs)

    def test_equal_hold_and_wall_is_legal(self):
        res = self.validate(self.fixed())
        self.assertTrue(res.get("ok"), res)
        self.assertNotIn("warnings", res)

    def test_terminal_hold_is_silent(self):
        res = self.validate(self.terminal_hold())
        self.assertTrue(res.get("ok"), res)
        self.assertNotIn("warnings", res)

    def test_graph_defaults_raise_the_decidable_wall(self):
        res = self.validate(self.hold_under_defaults_floor())
        self.assertTrue(res.get("ok"), res)
        self.assertNotIn("warnings", res)

    # -- (b): the doors WARN naming both numbers ---------------------------------

    def test_warns_naming_gate_hold_and_underwall_consumer(self):
        res = self.validate(self.warn_pair())
        self.assertTrue(res.get("ok"), res)
        rows = [w for w in res.get("warnings", [])
                if isinstance(w, dict) and w.get("field") == "hold_timeout"]
        self.assertEqual(len(rows), 1, res)
        self.assertEqual(rows[0]["node"], "owner_gate")
        for frag in ("86400", "3600", "quick_tail", "#475"):
            self.assertIn(frag, rows[0]["msg"])
        self.assertNotIn("guard", rows[0]["msg"])  # only the under-walled consumer is named

    def test_clean_graph_stays_warning_free(self):
        # Every consumer lane outlives the hold: byte-silent (a warning key must
        # not appear on a clean validate — the golden-response law).
        res = self.validate(self.warn_pair_fixed())
        self.assertTrue(res.get("ok"), res)
        self.assertNotIn("warnings", res)

    def warn_pair_fixed(self):
        g = self.warn_pair()
        for n in g["nodes"]:
            if n["id"] == "quick_tail":
                n["timeout"] = 86400
        return g


if __name__ == "__main__":
    unittest.main(verbosity=2)
