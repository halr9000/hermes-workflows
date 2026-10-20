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
    OWN rendered id — ONE bounded 10s read-back probe per committing item,
    serial at the commit edge; the measured row — the same closed
    {store,id,actor,observed_status,assignee,probe_rc,ok} shape — rides that
    item's record inside all_results as `bead_close_receipt`, and the node
    record carries under the PLURAL `bead_close_receipts` a sparse,
    self-indexed enumeration of the PROBED rows only (honest-pending rows
    absent, every row carries index); the key is minted only when at least
    one item was probed — an all-pending aggregate commits key-free, matching
    the token-free node-scoped escape (est-fxh6v shape call);
  * the honest escape is per ITEM: an item whose answer carries
    `close-pending` is a non-claim (no probe, pristine record) even while
    other items claim closures the gate must verify;
  * fail-closed semantics UNCHANGED item-for-item: any committing item not
    observed `closed` demotes the whole aggregate to failed
    error_class "bead_close", naming each offender; a template rendering to
    empty, whitespace, or surviving braces fails THAT item typed and is never
    probed (the argv law is checked after interpolation, never assumed);
  * a fan-out node with a LITERAL (token-free) declaration keeps the node-scoped
    single-probe shape (no item rows touched, no plural key minted, one probe);
    the honest escape is PER ITEM there too (est-nbe2y fold): the whole-blob
    `close-pending` search let one lane's token — or the phrase in any
    non-committing row — exempt the aggregate from read-back while sibling
    committing lanes served the claim, the 13z1 false-green shape at fan-out
    scale; after the fold any committing (done/partial) item without its own
    pending token forces the probe, and only an all-pending aggregate commits
    pristine with zero probes;
  * no declaration at all stays scan-free.

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

    def lanes(self, items, decl, goal='CLOSE-CLAIM {"result":"done","note":"{bead} closed"} post verdict',
              notes=None):
        if notes is not None:      # est-nbe2y: per-item close-pending control
            items = [dict(it, close_note=notes[i]) for i, it in enumerate(items)]
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

    # ---- (j) est-fxh6v (a): an ALL-PENDING fan-out probes nothing, so the
    #         node record must stay key-free — the same pristine shape the
    #         token-free node-scoped escape takes. Minting `[]` here would
    #         fake a "probed, enumerated none" key; the chosen shape is to
    #         skip the plural key when no item was probed.
    def test_j_all_pending_mints_no_plural_key(self):
        self.set_store(**{"est-hold1": {"status": "in_progress", "assignee": "other-seat"},
                          "est-hold2": {"status": "in_progress", "assignee": "other-seat"}})
        pend = ('CLOSE-CLAIM {"result":"in_progress","close-pending":'
                '"cannot close: assignee is other-seat"} post verdict')
        items = [{"bead": "est-hold1", "who": "hermes", "goal": pend},
                 {"bead": "est-hold2", "who": "hermes", "goal": pend}]
        r = self.mk("fo-j", [self.lanes(items, self.decl())])
        out = self.wf("fo-j")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "done", f"all-pending aggregate died: {rec}")
        self.assertNotIn("bead_close_receipts", rec,
                         "no item probed => no plural key (pristine, like the node-scoped escape)")
        for row in rec["output"]["all_results"]:
            self.assertNotIn("bead_close_receipt", row, "pending row record not pristine")
        self.assertFalse(any("est-hold" in a for a in self.argvs()),
                         "runner probed the store for beads that were never claimed closed")

    # ---- (i) doc pins: the grammar line carries the per-item law and the
    #         gate source keys on the shared read-back core.
    def test_i_doc_pins(self):
        gram = (ROOT / "references" / "grammar.md").read_text(encoding="utf-8")
        self.assertIn("bead_close_receipts", gram,
                      "grammar.md must document the per-item plural enumeration")
        # est-fxh6v (b): the plural key is the PROBED enumeration — sparse and
        # self-indexed — never the "full enumeration" of every item.
        self.assertIn("sparse, self-indexed", gram,
                      "grammar.md must describe the plural key as the sparse probed enumeration")
        # est-fxh6v (c): grammar must state the per-item cost against the real core.
        self.assertIn("_bead_close_readback", gram,
                      "grammar.md must name the shared read-back core it costs against")
        self.assertIn("10s", gram,
                      "grammar.md must state the bounded per-item probe cost")
        src = (ROOT / "wf.py").read_text(encoding="utf-8")
        self.assertIn("def _bead_close_readback", src)
        self.assertIn("def _bead_close_gate_items", src)

    # ---- (j) est-nbe2y fold: the per-item escape on a TOKEN-FREE fan-out
    #         decl. Lane 1 honestly returns close-pending; lane 2 claims the
    #         shared literal bead closed and the store says otherwise. The
    #         pre-fold whole-blob search saw lane 1's token and skipped the
    #         probe entirely — the aggregate committed DONE over an unproven
    #         claim with ZERO probes on the wire (RED proof: run this against
    #         the pre-fold gate and it commits done with no receipt). After
    #         the fold the committing lane forces the node-scoped read-back
    #         and the demote names the pending lanes that never excused it.
    def test_j_pending_lane_does_not_exempt_literal_decl(self):
        self.set_store(**{"est-static": {"status": "in_progress", "assignee": "other-seat"}})
        r = self.mk("fo-j", [self.lanes(
            [{"bead": "est-static", "who": "hermes"},
             {"bead": "est-static", "who": "hermes"}],
            self.decl(id="est-static", actor="hermes"),
            goal=('CLOSE-CLAIM {"result":"done","note":"{close_note}"} post verdict'),
            notes=["close-pending: assignee guard is not my seat",
                   "est-static closed"] )])
        out = self.wf("fo-j")
        self.assertNotIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "failed",
                         f"pending straggler still exempts a committing sibling: {rec}")
        self.assertEqual(rec.get("error_class"), "bead_close")
        self.assertIn("bead_close_unproven: est-static", rec.get("error", ""))
        self.assertIn("close-pending lane(s) never excuse", rec.get("error", ""))
        self.assertTrue(rec.get("bead_close_receipt"),
                        "the demoted record carries the measured singular row")
        self.assertFalse(rec["bead_close_receipt"]["ok"])
        self.assertNotIn("run.done", (r / "events.jsonl").read_text())
        shows = [a for a in self.argvs() if "show est-static" in a]
        self.assertEqual(len(shows), 1, "node-scoped single probe kept — exactly one")

    # ---- (k) est-nbe2y fold, the keep side: an ALL-pending token-free
    #         aggregate is still a whole-node non-claim — pristine record,
    #         zero probes, node commits (honesty is never punished).
    def test_k_all_pending_literal_aggregate_stays_pristine(self):
        self.set_store(**{"est-static": {"status": "in_progress", "assignee": "other-seat"}})
        r = self.mk("fo-k", [self.lanes(
            [{"bead": "est-static", "who": "hermes"},
             {"bead": "est-static", "who": "hermes"}],
            self.decl(id="est-static", actor="hermes"),
            goal=('CLOSE-CLAIM {"result":"done","note":"{close_note}"} post verdict'),
            notes=["close-pending: first guard", "close-pending: second guard"])])
        out = self.wf("fo-k")
        self.assertIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "lanes")
        self.assertEqual(rec.get("status"), "done", f"all-pending aggregate broke: {rec}")
        self.assertNotIn("bead_close_receipt", rec, "non-claim: record stays pristine")
        self.assertNotIn("bead_close_receipts", rec)
        self.assertEqual([a for a in self.argvs() if "show est-static" in a], [],
                         "no claim, no read")


if __name__ == "__main__":
    unittest.main(verbosity=2)
