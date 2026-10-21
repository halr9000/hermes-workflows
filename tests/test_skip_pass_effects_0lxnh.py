#!/usr/bin/env python3
"""est-0lxnh — the on_skip:pass done-commit must ride the _effect_gate boundary (#56 law).

The defect (adversarial probe P2, replayed live at ac5c11ad on main): an agent
node declaring `effects` with a false `when` and `on_skip: pass` committed
{"status": "done", "output": {"agent": "skipped"}, "ms": 0} at the skip site
with ZERO effect_receipts — the run ended run.done, runner_exit reason=done.
A lying done over an unproven postcondition: exactly the class #56 refuses at
every other commit edge (agent solo, fan-out aggregate, echo boundary). The
skip-pass site was the fifth bypass.

The law (#56): a node that declares `effects` commits done ONLY over measured
rows. At the commit edge the runner reads the bytes itself; an unproven row
DEMOTES the commit to status failed, error_class 'effect_receipt', error
'effect_receipt_unproven: ...'; run.done never fires over it. The skip-pass
done-commit is a commit edge — a false predicate guarantees the artifact was
NEVER written, so the demote is deterministic, not probabilistic.

Honest degradation (the gate's own law, asserted as case (b)): a node that
declares NO effects sees byte-identical behavior — the gate returns the rec
untouched, the skip-pass still books done with the {agent: skipped} marker,
run.done still fires. Only effects-declaring skip-pass nodes change.

RED proof (run at base ac5c11ad before the fix): case (a) FAILS — the node
record reads done with no effect_receipts key, WORKFLOW_DONE prints, and the
ledger ends run.done.

Pattern: tests/test_effects_receipt_pygyy.py (drives wf.py with tests/fake
via subprocess + env isolation).

Run: python3 tests/test_skip_pass_effects_0lxnh.py
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import wfcommon  # noqa: E402

FAKE = str(ROOT / "tests" / "fake")


class SkipPassEffectGate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="0lxnh-")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.runs = self.home / "workflows"
        self.runs.mkdir()
        (self.home / "fake.log").write_text("")

    def mk(self, run_id, nodes, **meta):
        r = self.runs / run_id
        (r / "nodes").mkdir(parents=True)
        (r / "gates").mkdir()
        (r / "graph.json").write_text(json.dumps({"name": run_id, "nodes": nodes}))
        m = {"hermes_bin": FAKE, "concurrency": 4, "node_timeout": 30}
        m.update(meta)
        (r / "run.json").write_text(json.dumps(m))
        return r

    def wf(self, run_id, timeout=120):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("WF_", "FAKE_", "HERMES_WF_")) and k != "HERMES_HOME"}
        env.update(HERMES_HOME=str(self.home), WF_RUNS_ROOT=str(self.runs),
                   FAKE_LOG=str(self.home / "fake.log"))
        return subprocess.run([sys.executable, str(ROOT / "wf.py"), "run", run_id],
                              env=env, capture_output=True, text=True,
                              timeout=timeout).stdout.strip()

    def rec(self, r, nid):
        p = r / "nodes" / f"{nid}.json"
        return json.loads(p.read_text()) if p.exists() else {}

    def events(self, r):
        p = r / "events.jsonl"
        return ([json.loads(l) for l in p.read_text().splitlines()]
                if p.exists() else [])

    def graph(self, effects):
        n = {"id": "skipme", "type": "agent", "after": ["seed"], "goal": "never runs",
             "when": "out.seed.v == true", "on_skip": "pass"}
        if effects:
            n["effects"] = [{"file": "work/skipme/never.json"}]
        return [
            {"id": "seed", "type": "echo", "output": {"v": False}},
            n,
            {"id": "tail", "type": "echo", "after": ["skipme"], "output": {"ok": 1}},
        ]

    # ---- (a) THE DEFECT: effects + false when + on_skip:pass must NOT book
    #         a zero-receipt done. Expect the #56 typed demote at this edge.
    def test_a_skip_pass_with_effects_demotes_to_typed_failed(self):
        r = self.mk("sp-a", self.graph(True))
        out = self.wf("sp-a")
        self.assertNotIn("WORKFLOW_DONE", out,
                         "run reported WORKFLOW_DONE over an unproven skip-pass")
        rec = self.rec(r, "skipme")
        self.assertEqual(
            rec.get("status"), "failed",
            f"skip-pass with declared effects booked {rec.get('status')!r} "
            f"with zero effect receipts (the P2 lying-done): {rec}")
        self.assertEqual(rec.get("error_class"), "effect_receipt")
        self.assertIn("effect_receipt_unproven", rec.get("error", ""))
        self.assertIn("work/skipme/never.json", rec.get("error", ""))
        self.assertIsInstance(rec.get("effect_receipts"), list,
                              "gate must record measured rows even on the demote")
        rows = rec["effect_receipts"]
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["ok"])
        self.assertFalse(rows[0]["exists"])
        names = [e.get("event") for e in self.events(r)]
        self.assertNotIn("run.done", names,
                         "ledger ended run.done over the bypass")
        self.assertIn("node.failed", names)

    # ---- (b) honest degradation control: NO effects declared => today's
    #         behavior byte-identical: done + {agent: skipped} + run.done.
    def test_b_skip_pass_without_effects_unchanged(self):
        r = self.mk("sp-b", self.graph(False))
        out = self.wf("sp-b")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "skipme")
        self.assertEqual(rec.get("status"), "done", f"effects-free skip-pass changed: {rec}")
        self.assertEqual(rec.get("output"), {"agent": "skipped"})
        self.assertNotIn("effect_receipts", rec)
        names = [e.get("event") for e in self.events(r)]
        self.assertIn("agent.skipped", names)
        self.assertIn("run.done", names)
        # downstream saw the skip-pass marker: the tail echo committed
        self.assertEqual(self.rec(r, "tail").get("status"), "done")


if __name__ == "__main__":
    unittest.main(verbosity=2)
