#!/usr/bin/env python3
"""#485 — bead_close id/actor per-item {FIELD} interpolation on fan-out nodes.

The excavation (estate 2026-10-20, E-9/E-11 lanes): a fan-out bead-worker lane
where one item == one estate bead cannot declare per-lane closure proof — one
`bead_close` node serves N beads with different ids, and the gate, node-scoped
and literal, can only probe ONE. Lanes fall back to a prose-level verify node
re-running `bd show`: same law, human-speed, unenforced.

The contract implemented (issue #485):
  * `bead_close.id` (and `.actor`) may carry BARE ITEM TEMPLATES ({FIELD} —
    the exact token law fmt_goal pays for fan-out goals, so {item} and {index}
    resolve too) on a FAN-OUT node; the door REFUSES the tokens elsewhere;
  * every committing (done/partial) item is read back from the store at its
    OWN rendered id; the measured row — the same closed
    {store,id,actor,observed_status,assignee,probe_rc,ok} shape — rides that
    item's record inside all_results as `bead_close_receipt`, and the node
    record carries the full enumeration (with item index) under the PLURAL
    `bead_close_receipts`;
  * the honest escape is per ITEM: an item whose answer carries
    `close-pending` is a non-claim (no probe, pristine record) even while
    other items claim closures the gate must verify;
  * fail-closed semantics UNCHANGED item-for-item: any committing item not
    observed `closed` demotes the whole aggregate to failed
    error_class "bead_close", naming each offender; a template rendering to
    empty, whitespace, or surviving braces fails THAT item typed and is never
    probed (the argv law is checked after interpolation, never assumed);
  * a fan-out node with a LITERAL (token-free) declaration keeps the old
    node-scoped single-probe shape byte-identical — no item rows touched, no
    plural key minted; no declaration at all stays scan-free.

RED proof (run before the fix): (a) the gate probes the literal "{bead}" once
and the run dies with bead_close_unproven over an unresolved brace token;
(b)-(g) fail for the same reason or because the door accepts tokens on a solo
node (e) / the plural enumeration is never minted.

Run: env -u WF_RUNS_ROOT -u HERMES_HOME \
     PYTHONPATH=/opt/hermes /opt/hermes/.venv/bin/python tests/test_bead_close_fanout_485.py
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
import wf  # noqa: E402

FAKE = str(ROOT / "tests" / "fake")
BD_PY = str(ROOT / "tests" / "fake_bd_close_13z1.py")

ROW_KEYS = {"store", "id", "actor", "observed_status", "assignee", "probe_rc", "ok"}


class BeadCloseFanout(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="485-")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.runs = self.home / "workflows"
        self.runs.mkdir()
        (self.home / "fake.log").write_text("")
        self.store = self.home / "bd-store.json"
        self.bdlog = self.home / "bd.log"
        (self.home / "bin").mkdir()
        bdp = self.home / "bin" / "bd"
        bdp.write_text("#!/bin/sh\nexec /usr/bin/python3 %s \"$@\"\n" % BD_PY)
        bdp.chmod(0o755)
        os.environ["PATH"] = str(self.home / "bin") + os.pathsep + os.environ["PATH"]

    def set_store(self, **beads):
        self.store.write_text(json.dumps(beads))

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
        env = dict(os.environ, HERMES_HOME=str(self.home),
                   WF_RUNS_ROOT=str(self.runs),
                   FAKE_LOG=str(self.home / "fake.log"),
                   FAKE_BD_STORE=str(self.store),
                   FAKE_BD_LOG=str(self.bdlog))
        return subprocess.run([sys.executable, str(ROOT / "wf.py"), "run", run_id],
                              env=env, capture_output=True, text=True,
                              timeout=timeout).stdout.strip()

    def rec(self, r, nid):
        p = r / "nodes" / f"{nid}.json"
        return json.loads(p.read_text()) if p.exists() else {}

    def lanes(self, items, decl, goal='CLOSE-CLAIM {"result":"done","note":"{bead} closed"} post verdict'):
        return {"id": "lanes", "type": "agent",
                "fanout": {"items": items, "goal": goal},
                "bead_close": decl}

    def decl(self, **over):
        d = {"store": str(self.home / "estate"), "id": "{bead}", "actor": "{who}"}
        d.update(over)
        return d

    def argvs(self):
        return self.bdlog.read_text().splitlines() if self.bdlog.exists() else []

    # ---- (a) happy path: two lanes, two distinct beads, BOTH closed => the
    #         aggregate commits done with a receipt PER ITEM riding each
    #         all_results row and the plural enumeration on the node record.
    def test_a_per_item_close_commits_with_receipts(self):
        self.set_store(**{"est-a1": {"status": "closed", "assignee": "hermes"},
                          "est-a2": {"status": "closed", "assignee": "hermes"}})
        r = self.mk("fo-a", [self.lanes([{"bead": "est-a1", "who": "hermes"},
                                         {"bead": "est-a2", "who": "hermes"}], self.decl())])
        out = self.wf("fo-a")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "done", f"true per-item closes refused: {rec}")
        rows = rec["output"]["all_results"]
        self.assertEqual(len(rows), 2)
        for i, bid in enumerate(("est-a1", "est-a2")):
            row = rows[i].get("bead_close_receipt")
            self.assertIsInstance(row, dict, f"item {i} carries no receipt")
            self.assertEqual(row["id"], bid)
            self.assertEqual(row["actor"], "hermes", "actor must render per item from {who}")
            self.assertTrue(row["ok"] and row["observed_status"] == "closed")
            self.assertEqual(set(row), ROW_KEYS, "per-item row keeps the closed shape")
        plist = rec.get("bead_close_receipts")
        if not isinstance(plist, list):
            self.fail("node record must carry the plural enumeration")
        self.assertEqual([p["index"] for p in plist], [0, 1])
        self.assertEqual([p["id"] for p in plist], ["est-a1", "est-a2"])
        self.assertTrue(any(a.endswith("show est-a1 --json") for a in self.argvs()),
                        "runner never read the FIRST item's bead itself")
        self.assertTrue(any(a.endswith("show est-a2 --json") for a in self.argvs()),
                        "runner never read the SECOND item's bead itself")
        self.assertIn("run.done", (r / "events.jsonl").read_text())

    # ---- (b) one lane lies: est-a2 still in_progress in the store => the
    #         whole aggregate demotes, naming the offender; the honest
    #         sibling's receipt still rides its row (measured facts, not erasure).
    def test_b_one_unproven_close_demotes_aggregate(self):
        self.set_store(**{"est-a1": {"status": "closed", "assignee": "hermes"},
                          "est-a2": {"status": "in_progress", "assignee": "other-seat"}})
        r = self.mk("fo-b", [self.lanes([{"bead": "est-a1", "who": "hermes"},
                                         {"bead": "est-a2", "who": "hermes"}], self.decl())])
        out = self.wf("fo-b")
        self.assertNotIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "failed")
        self.assertEqual(rec.get("error_class"), "bead_close")
        self.assertIn("bead_close_unproven", rec.get("error", ""))
        self.assertIn("est-a2", rec.get("error", ""))
        self.assertIn("[1]", rec.get("error", ""))
        rows = rec["output"]["all_results"]
        self.assertTrue(rows[0]["bead_close_receipt"]["ok"])
        self.assertFalse(rows[1]["bead_close_receipt"]["ok"])
        self.assertEqual(rows[1]["bead_close_receipt"]["observed_status"], "in_progress")
        self.assertNotIn("run.done", (r / "events.jsonl").read_text())

    # ---- (c) per-item honest escape: item 0 closes for real, item 1 returns
    #         close-pending => done stands, item 1 is NEVER probed (pristine),
    #         item 0 is still verified (one lane's honesty never excuses a skip).
    def test_c_pending_lane_never_probed(self):
        self.set_store(**{"est-a1": {"status": "closed", "assignee": "hermes"},
                          "est-hold2": {"status": "in_progress", "assignee": "other-seat"}})
        items = [{"bead": "est-a1", "who": "hermes",
                  "goal": 'CLOSE-CLAIM {"result":"done","note":"est-a1 closed"} post verdict'},
                 {"bead": "est-hold2", "who": "hermes",
                  "goal": 'CLOSE-CLAIM {"result":"in_progress","close-pending":'
                          '"cannot close: assignee is other-seat"} post verdict'}]
        r = self.mk("fo-c", [self.lanes(items, self.decl())])
        out = self.wf("fo-c")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "done", f"pending lane killed the aggregate: {rec}")
        rows = rec["output"]["all_results"]
        self.assertTrue(rows[0].get("bead_close_receipt", {}).get("ok"),
                        "the committing lane was skipped while a sibling stayed honest")
        self.assertNotIn("bead_close_receipt", rows[1], "pending lane record not pristine")
        self.assertFalse(any("est-hold2" in a for a in self.argvs()),
                         "runner probed the store for a lane that never claimed closure")

    # ---- (d) an item carrying no `bead` field: the template cannot render —
    #         typed failure on THAT item, never probed as literal brace text.
    def test_d_unresolved_token_fails_typed(self):
        self.set_store(**{"est-a1": {"status": "closed", "assignee": "hermes"}})
        r = self.mk("fo-d", [self.lanes([{"bead": "est-a1", "who": "hermes"},
                                         {"who": "hermes",
                                          "goal": 'CLOSE-CLAIM {"result":"done","note":"closed"} post verdict'}],
                                        self.decl())])
        out = self.wf("fo-d")
        self.assertNotIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "failed")
        self.assertEqual(rec.get("error_class"), "bead_close")
        self.assertIn("unresolved", rec.get("error", ""))
        self.assertIn("[1]", rec.get("error", ""))
        row = rec["output"]["all_results"][1]["bead_close_receipt"]
        self.assertIsNone(row["probe_rc"], "an unrenderable id must never reach the argv")
        self.assertEqual(len([a for a in self.argvs() if a.startswith("-C")]), 1)

    # ---- (e) a field value with whitespace violates the read-back argv law —
    #         the check runs AFTER interpolation and fails typed.
    def test_e_whitespace_rendered_id_fails_typed(self):
        self.set_store(**{"est-a1": {"status": "closed", "assignee": "hermes"}})
        r = self.mk("fo-e", [self.lanes([{"bead": "est-a1", "who": "hermes"},
                                         {"bead": "est a2", "who": "hermes",
                                          "goal": 'CLOSE-CLAIM {"result":"done","note":"x"} post verdict'}],
                                        self.decl())])
        out = self.wf("fo-e")
        self.assertNotIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("error_class"), "bead_close")
        self.assertIn("whitespace", rec.get("error", ""))
        row = rec["output"]["all_results"][1]["bead_close_receipt"]
        self.assertIsNone(row["probe_rc"])

    # ---- (f) backward law: a fan-out node whose declaration carries NO token
    #         anywhere keeps the old node-scoped single-probe shape — one
    #         probe, singular key, no receipt touched on any item row. (An
    #         id-only override would leave the actor's {who} token live, which
    #         is the INTERPOLATED shape — that path probes per item too.)
    def test_f_literal_fanout_unchanged(self):
        self.set_store(**{"est-static": {"status": "closed", "assignee": "hermes"}})
        r = self.mk("fo-f", [self.lanes([{"bead": "est-x1", "who": "hermes"},
                                         {"bead": "est-x2", "who": "hermes"}],
                                        self.decl(id="est-static", actor="hermes"))])
        out = self.wf("fo-f")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "done", f"literal fan-out decl broke: {rec}")
        self.assertTrue(rec.get("bead_close_receipt", {}).get("ok"),
                        "node-scoped singular receipt must still be written")
        self.assertNotIn("bead_close_receipts", rec, "plural key only belongs to interpolated decls")
        for row in rec["output"]["all_results"]:
            self.assertNotIn("bead_close_receipt", row, "literal decl must not touch item rows")
        self.assertEqual([a for a in self.argvs() if a.startswith("-C")],
                         [a for a in self.argvs() if a.endswith("show est-static --json")])

    # ---- (g) door: tokens are a FAN-OUT-ONLY law — refuse them anywhere else,
    #         accept them (and plain ids) on a fan-out node.
    def test_g_door_requires_fanout(self):
        def errs(nodes):
            return [e for e in wfcommon.validate_graph_errors(nodes)
                    if "bead_close" in json.dumps(e)]
        tok = {"store": "/srv/beads", "id": "{bead}", "actor": "{who}"}
        lit = {"store": "/srv/beads", "id": "est-x", "actor": "hermes"}
        fo = {"items": [{"bead": "est-a1"}], "goal": "close {bead}"}
        base = [{"id": "a", "type": "agent", "fanout": fo, "bead_close": tok}]
        self.assertEqual(errs(base), [], "fan-out + tokens must pass the door")
        self.assertEqual(errs([{"id": "a", "type": "agent", "fanout": fo, "bead_close": lit}]),
                         [], "fan-out + literal stays legal (backward)")
        for nodes, label in (
            ([{"id": "a", "type": "agent", "bead_close": tok}], "solo agent, token id"),
            ([{"id": "a", "type": "agent", "bead_close":
               {"store": "/s", "id": "est-x", "actor": "{who}"}}], "solo agent, token actor"),
            ([{"id": "a", "type": "agent", "bead_close":
               {"store": "/s", "id": "{item}"}}], "solo agent, {item} token"),
            ([{"id": "a", "type": "echo", "output": {"r": 1}, "bead_close": tok}], "echo, token"),
        ):
            with self.subTest(label):
                self.assertTrue(errs(nodes), f"door accepted {label}")

    # ---- (h) the string-item shape: bare {item} resolves per item, so a
    #         roster of plain bead ids interpolates too.
    def test_h_string_items_interpolate(self):
        self.set_store(**{"est-s1": {"status": "closed", "assignee": "hermes"},
                          "est-s2": {"status": "closed", "assignee": "hermes"}})
        r = self.mk("fo-h", [self.lanes(["est-s1", "est-s2"],
                                        {"store": str(self.home / "estate"), "id": "{item}"},
                                        goal='CLOSE-CLAIM {"result":"done","note":"{item} closed"} post verdict')])
        out = self.wf("fo-h")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "done", f"string-item roster refused: {rec}")
        self.assertEqual([p["id"] for p in rec["bead_close_receipts"]], ["est-s1", "est-s2"])

    # ---- (i) doc pins on the PUBLIC vocabulary + one behavior pin (est-luk28:
    #         this test used to grep wf.py for internal def names — rename-
    #         fragile text that breaks on a refactor which changes no behavior.
    #         The contract lives in the documented keys, not the defs; per-item
    #         rows and the plural enumeration are behavior-pinned in (a)/(f)).
    def test_i_doc_pins(self):
        gram = (ROOT / "references" / "grammar.md").read_text(encoding="utf-8")
        self.assertIn("bead_close_receipts", gram,
                      "grammar.md must document the per-item plural enumeration")
        src = (ROOT / "wf.py").read_text(encoding="utf-8")
        self.assertIn("bead_close_receipts", src,
                      "the shipped gate must still mint the fan-out plural key")
        # behavior pin behind the text pins: the node-scoped (echo) path mints
        # the SINGULAR key with the closed row shape — rename-resilient proof
        # the read-back core is shared, not text.
        self.set_store(**{"est-i0": {"status": "closed", "assignee": "hermes"}})
        r = self.mk("solo-i", [{"id": "solo", "type": "echo", "output": {"r": 1},
                                "bead_close": {"store": str(self.home / "estate"),
                                               "id": "est-i0", "actor": "hermes"}}])
        self.assertIn("WORKFLOW_DONE", self.wf("solo-i"))
        row = self.rec(r, "solo").get("bead_close_receipt")
        self.assertIsInstance(row, dict, "echo path minted no singular receipt")
        self.assertEqual(set(row or {}), ROW_KEYS, "echo receipt keeps the closed row shape")


if __name__ == "__main__":
    unittest.main(verbosity=2)
