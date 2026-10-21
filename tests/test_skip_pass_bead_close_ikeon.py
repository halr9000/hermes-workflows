#!/usr/bin/env python3
"""est-ikeon — the bead_close gate must ride the two remaining done-commit edges.

The defect (the est-13z1 class, R8 sibling sweep of PR #523 @47879ee): the
commit-edge census applies BOTH boundary gates — `_effect_gate` then
`_bead_close_gate` — on the agent-solo path (wf.py:8241-8242), the fan-out
aggregate (:8212-8213) and the echo boundary (:9222-9224). Two done-commits
ride the effect gate ALONE:

  * the `on_skip:"pass"` skip-pass commit (wf.py:9381) — the est-0lxnh edge;
  * the `on_exit` terminal postmortem commit (wf.py:8783, #129).

The door ADMITS the combo: `on_skip` is validated as enum + `when`-presence
only (wfcommon.py:2006-2011), `bead_close` by shape alone (:1722-1742), and
neither key set is consulted between the two gates. Reachable lie: an agent
`{bead_close, when: false, on_skip: "pass"}` with NO effects commits
`done {agent:"skipped"}` with ZERO store read-back — a false predicate
guarantees the node never ran, so it certainly never closed the bead; and an
`on_exit` node declaring `bead_close` books its terminal `done` on prose alone.

The law (est-13z1, extended to these edges): at the commit edge the runner
reads the estate store ITSELF (`bd -C <store> show <id> --json`), records the
measured row as `bead_close_receipt`, and anything but an observed
`status == "closed"` DEMOTES the commit to `failed` `error_class:"bead_close"`
(`error: bead_close_unproven: ...`) — a proven-fail, never retried. At the
skip edge the demote reuses the gate-agnostic branch #523 already landed
(`status != done` + `error` + `error_class` with an `effect_receipt` default),
so no branch change is needed. At the `on_exit` edge the demote lands on the
node record and the receipt WITHOUT moving the run verdict — `write_runner_exit`
commits the verdict before `_run_on_exit` runs (#129 law), which this test
asserts rather than assumes.

Honest degradation (the gate's own law, asserted as the control): a node that
declares NO `bead_close` is byte-identical — no probe argv is even logged.
Join/consolidate/gate commit edges stay exempt: their closed key sets
(wfcommon.py:872/947/1110) carry neither gate key, so the door refuses the
declaration before a runner ever has to honour one.

RED proof (run at head 47879ee, before the fix): (a) the skip-pass node books
`done {agent:"skipped"}` with no `bead_close_receipt` and no probe argv,
WORKFLOW_DONE prints and the ledger ends run.done; (d) the on_exit node books
`done` the same way and `on_exit.finished` reports status=done.

Pattern: tests/test_skip_pass_effects_0lxnh.py (driver shape) +
tests/test_bead_close_claim_13z1.py (fake-bd store fixture).

Run: python3 tests/test_skip_pass_bead_close_ikeon.py
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
BD_PY = str(ROOT / "tests" / "fake_bd_close_13z1.py")


class SkipPassBeadClose(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="ikeon-")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.runs = self.home / "workflows"
        self.runs.mkdir()
        (self.home / "fake.log").write_text("")
        self.store = self.home / "bd-store.json"
        self.bdlog = self.home / "bd.log"
        # fake `bd` on the RUNNER's PATH only (never this process's env)
        bindir = self.home / "bin"
        bindir.mkdir()
        bdp = bindir / "bd"
        bdp.write_text("#!/bin/sh\nexec /usr/bin/python3 %s \"$@\"\n" % BD_PY)
        bdp.chmod(0o755)
        self.bindir = str(bindir)

    def set_store(self, **beads):
        self.store.write_text(json.dumps(beads))

    def decl(self, bid="est-ikeon1", **over):
        d = {"store": str(self.home / "estate"), "id": bid}
        d.update(over)
        return d

    def mk(self, run_id, graph):
        r = self.runs / run_id
        (r / "nodes").mkdir(parents=True)
        (r / "gates").mkdir()
        g = {"name": run_id}
        g.update(graph)
        (r / "graph.json").write_text(json.dumps(g))
        (r / "run.json").write_text(json.dumps({
            "hermes_bin": FAKE, "concurrency": 4, "node_timeout": 30,
            "retry_backoff": [0], "retry_budget": 0}))
        return r

    def wf(self, run_id, timeout=120):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("WF_", "FAKE_", "HERMES_WF_")) and k != "HERMES_HOME"}
        env.update(HERMES_HOME=str(self.home), WF_RUNS_ROOT=str(self.runs),
                   FAKE_LOG=str(self.home / "fake.log"),
                   FAKE_BD_STORE=str(self.store), FAKE_BD_LOG=str(self.bdlog),
                   PATH=self.bindir + os.pathsep + env.get("PATH", ""))
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

    def probes(self):
        return self.bdlog.read_text().splitlines() if self.bdlog.exists() else []

    # ---- graph builders -------------------------------------------------
    def skip_graph(self, with_bc):
        """echo seed(false) -> agent skipme{when false, on_skip:pass} -> echo tail."""
        n = {"id": "skipme", "type": "agent", "after": ["seed"],
             "goal": 'CLOSE-CLAIM {"result":"done","note":"est-ikeon1 closed"} never runs',
             "when": "out.seed.v == true", "on_skip": "pass"}
        if with_bc:
            n["bead_close"] = self.decl("est-ikeon1")
        return {"nodes": [
            {"id": "seed", "type": "echo", "output": {"v": False}},
            n,
            {"id": "tail", "type": "echo", "after": ["skipme"], "output": {"ok": 1}},
        ]}

    def exit_graph(self, with_bc):
        """work (agent, done) with `on_exit` naming `post`, the terminal postmortem."""
        post = {"id": "post", "type": "agent", "after": ["work"], "timeout": 20,
                "goal": 'CLOSE-CLAIM {"result":"done","note":"est-ikeon2 closed"} post the verdict'}
        if with_bc:
            post["bead_close"] = self.decl("est-ikeon2")
        return {"on_exit": "post", "nodes": [
            {"id": "work", "type": "agent", "goal": "plain work", "timeout": 20},
            post,
        ]}

    # ======================================================================
    # EDGE 1 — the on_skip:"pass" done-commit (wf.py:9381)
    # ======================================================================

    # ---- (a) THE DEFECT: bead_close + false when + on_skip:pass must NOT
    #         book a zero-read-back done. Store says in_progress.
    def test_a_skip_pass_with_bead_close_demotes_to_typed_failed(self):
        self.set_store(**{"est-ikeon1": {"status": "in_progress", "assignee": "gh-dispatch"}})
        r = self.mk("bc-sp-a", self.skip_graph(True))
        out = self.wf("bc-sp-a")
        self.assertNotIn("WORKFLOW_DONE", out,
                         "run reported WORKFLOW_DONE over an unread skip-pass close")
        rec = self.rec(r, "skipme")
        self.assertEqual(
            rec.get("status"), "failed",
            f"skip-pass with a declared bead_close booked {rec.get('status')!r} with ZERO "
            f"store read-back (the 13z1 lying-done at the skip edge): {rec}")
        self.assertEqual(rec.get("error_class"), "bead_close")
        self.assertIn("bead_close_unproven", rec.get("error", ""))
        self.assertIn("est-ikeon1", rec.get("error", ""))
        row = rec.get("bead_close_receipt")
        self.assertIsInstance(row, dict, "measured read-back must ride the FAILED record")
        self.assertEqual(row.get("observed_status"), "in_progress")
        self.assertEqual(row.get("assignee"), "gh-dispatch")
        self.assertTrue(row.get("ok") is False)
        self.assertTrue(any(a.startswith("-C ") and a.endswith("show est-ikeon1 --json")
                            for a in self.probes()),
                        "the runner never ran the store read-back at the skip edge")
        names = [e.get("event") for e in self.events(r)]
        self.assertIn("node.failed", names)
        self.assertNotIn("run.done", names, "ledger ended run.done over the bypass")

    # ---- (b) the store really says closed => the skip-pass done stands, with
    #         the measured row on the record and descendants still flowing.
    def test_b_skip_pass_bead_close_proven_stands(self):
        self.set_store(**{"est-ikeon1": {"status": "closed", "assignee": "hermes"}})
        r = self.mk("bc-sp-b", self.skip_graph(True))
        out = self.wf("bc-sp-b")
        self.assertIn("WORKFLOW_DONE", out, "a store-proven close must not demote")
        rec = self.rec(r, "skipme")
        self.assertEqual(rec.get("status"), "done", f"proven close demoted: {rec}")
        self.assertEqual(rec.get("output"), {"agent": "skipped"})
        row = rec.get("bead_close_receipt")
        self.assertIsInstance(row, dict, "the read-back row rides the PASSING record too")
        self.assertEqual(row.get("observed_status"), "closed")
        self.assertTrue(row.get("ok") is True)
        names = [e.get("event") for e in self.events(r)]
        self.assertIn("agent.skipped", names)
        self.assertIn("run.done", names)
        self.assertEqual(self.rec(r, "tail").get("status"), "done",
                         "downstream of a proven skip-pass close must still run")

    # ---- (c) honest degradation control: NO bead_close declared => today's
    #         behavior byte-identical, and NOT ONE probe argv is spent.
    def test_c_skip_pass_without_bead_close_unchanged(self):
        self.set_store(**{"est-ikeon1": {"status": "in_progress", "assignee": "gh-dispatch"}})
        r = self.mk("bc-sp-c", self.skip_graph(False))
        out = self.wf("bc-sp-c")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "skipme")
        self.assertEqual(rec.get("status"), "done", f"bead_close-free skip-pass changed: {rec}")
        self.assertEqual(rec.get("output"), {"agent": "skipped"})
        self.assertNotIn("bead_close_receipt", rec)
        self.assertEqual(self.probes(), [],
                         "an undeclaring graph must never touch the estate store")
        names = [e.get("event") for e in self.events(r)]
        self.assertIn("agent.skipped", names)
        self.assertIn("run.done", names)

    # ======================================================================
    # EDGE 2 — the on_exit terminal commit (wf.py:8783)
    # ======================================================================

    # ---- (d) THE TWIN DEFECT: the terminal postmortem declares bead_close and
    #         the store says otherwise => the NODE record demotes, the verdict
    #         it was written under does not move (#129 law).
    def test_d_on_exit_bead_close_unproven_demotes_the_node(self):
        self.set_store(**{"est-ikeon2": {"status": "in_progress", "assignee": "gh-dispatch"}})
        r = self.mk("bc-ex-d", self.exit_graph(True))
        self.wf("bc-ex-d")
        rec = self.rec(r, "post")
        self.assertEqual(
            rec.get("status"), "failed",
            f"the on_exit commit booked {rec.get('status')!r} on prose with ZERO store "
            f"read-back (the 13z1 lying-done at the terminal edge): {rec}")
        self.assertEqual(rec.get("error_class"), "bead_close")
        self.assertIn("bead_close_unproven", rec.get("error", ""))
        row = rec.get("bead_close_receipt")
        self.assertIsInstance(row, dict, "measured read-back must ride the FAILED record")
        self.assertEqual(row.get("observed_status"), "in_progress")
        self.assertTrue(row.get("ok") is False)
        self.assertTrue(any(a.endswith("show est-ikeon2 --json") for a in self.probes()),
                        "the runner never ran the store read-back at the terminal edge")
        finished = [e for e in self.events(r) if e.get("event") == "on_exit.finished"]
        self.assertTrue(finished, "no on_exit.finished event")
        self.assertEqual(finished[-1].get("status"), "failed")
        self.assertEqual(finished[-1].get("error_class"), "bead_close")
        receipt = (r / "on_exit_receipt.md").read_text()
        self.assertIn("failed", receipt)
        self.assertIn("bead_close", receipt)
        # the postmortem never rewrites the verdict it was called under: the
        # ordinary work node committed done, the ledger says run.done.
        names = [e.get("event") for e in self.events(r)]
        self.assertIn("run.done", names)
        self.assertEqual(self.rec(r, "work").get("status"), "done")

    # ---- (e) store says closed => the terminal commit stands, receipted.
    def test_e_on_exit_bead_close_proven_commits_done(self):
        self.set_store(**{"est-ikeon2": {"status": "closed", "assignee": "gh-dispatch"}})
        r = self.mk("bc-ex-e", self.exit_graph(True))
        self.wf("bc-ex-e")
        rec = self.rec(r, "post")
        self.assertEqual(rec.get("status"), "done", f"proven terminal close demoted: {rec}")
        row = rec.get("bead_close_receipt")
        self.assertIsInstance(row, dict)
        self.assertEqual(row.get("observed_status"), "closed")
        self.assertTrue(row.get("ok") is True)
        finished = [e for e in self.events(r) if e.get("event") == "on_exit.finished"]
        self.assertEqual(finished[-1].get("status"), "done")

    # ---- (f) honest degradation at the terminal edge too: no declaration,
    #         no probe, the postmortem commits exactly as #129 shipped it.
    def test_f_on_exit_without_bead_close_unchanged(self):
        self.set_store(**{"est-ikeon2": {"status": "in_progress", "assignee": "gh-dispatch"}})
        r = self.mk("bc-ex-f", self.exit_graph(False))
        self.wf("bc-ex-f")
        rec = self.rec(r, "post")
        self.assertEqual(rec.get("status"), "done", f"undeclaring on_exit changed: {rec}")
        self.assertNotIn("bead_close_receipt", rec)
        self.assertEqual(self.probes(), [],
                         "an undeclaring graph must never touch the estate store")


if __name__ == "__main__":
    unittest.main(verbosity=2)
