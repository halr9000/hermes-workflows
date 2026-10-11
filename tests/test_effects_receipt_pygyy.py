#!/usr/bin/env python3
"""est-pygyy (issue #56) — effect-receipt postconditions at the commit edge.

The incident class (qm-mine F1): a publication node's child dies after
harvested prose; run.done fires; the artifact was never written. Prose is not
proof. The contract is now EXPLICIT and mechanical:

  * an agent/echo node declares its postconditions with
    `effects: [{file, sha256?, min_bytes?}, ...]` — run-dir-relative paths,
    closed row keys, validated fail-closed at the door;
  * at the COMMIT edge (agent solo, fan-out aggregate, echo), the runner reads
    the bytes ITSELF and records measured facts {file, exists, bytes,
    sha256, ok} on the node record as `effect_receipts`;
  * any unproven row DEMOTES the commit: status failed, error_class
    'effect_receipt', error 'effect_receipt_unproven: <file>: <reason>'; the
    run NEVER reaches run.done over it; the fail is never retried by either
    ladder (a proven-fail: the same graph dies identically until the artifact
    actually lands — recovery is publication-only re-read/complete-write,
    NEVER a re-author of a committed artifact);
  * honest degradation: no `effects` key = today's behavior byte-identical
    (the golden solo graphs declare nothing).

RED proof (run before the fix): case (a) FAILS — the `effects` key is refused
as an unknown key at the door (closed grammar), so no publication lane could
declare a receipt at all; cases (c-e) fail because nothing verified bytes
before run.done.

Run: env -u WF_RUNS_ROOT -u HERMES_HOME \
     PYTHONPATH=/opt/hermes /opt/hermes/.venv/bin/python tests/test_effects_receipt_pygyy.py
"""
import hashlib
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
ART = b"e" * 64
ART_SHA = hashlib.sha256(ART).hexdigest()


class EffectReceipt(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="pygyy-")
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
        env = dict(os.environ, HERMES_HOME=str(self.home),
                   WF_RUNS_ROOT=str(self.runs),
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

    # ---- (a) declared receipt, child writes the artifact => proven done,
    #         measured facts recorded on the node record, run.done fires.
    def test_a_proven_receipt_commits_done(self):
        r = self.mk("er-a", [{
            "id": "pub", "type": "agent",
            "goal": "EFFECT-EMIT out/report.json BYTES=64 publish the report",
            "effects": [{"file": "work/pub/out/report.json"}]}])
        self.wf("er-a")
        rec = self.rec(r, "pub")
        self.assertEqual(rec.get("status"), "done",
                         f"proven publication did not commit done: {rec}")
        rows = rec.get("effect_receipts")
        self.assertIsInstance(rows, list, "no effect_receipts recorded on the node")
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["ok"] and rows[0]["exists"],
                        f"receipt not proven: {rows}")
        self.assertEqual(rows[0]["sha256"], ART_SHA,
                         "runner must record the digest it MEASURED")
        self.assertEqual(rows[0]["bytes"], len(ART))
        self.assertIn("run.done", (r / "events.jsonl").read_text())

    # ---- (b) the incident: child never writes the artifact => done is
    #         DEMOTED to typed failed; run.done must never fire.
    def test_b_missing_artifact_blocks_done(self):
        r = self.mk("er-b", [{
            "id": "pub", "type": "agent", "goal": "publish the report (writes nothing)",
            "effects": [{"file": "work/pub/out/report.json"}]}])
        out = self.wf("er-b")
        self.assertNotIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "pub")
        self.assertEqual(rec.get("status"), "failed",
                         f"unproven publication committed {rec.get('status')}: {rec}")
        self.assertEqual(rec.get("error_class"), "effect_receipt")
        self.assertIn("effect_receipt_unproven", rec.get("error", ""))
        self.assertIn("work/pub/out/report.json", rec.get("error", ""))
        self.assertNotIn("run.done", (r / "events.jsonl").read_text())
        # the fail is a proven-fail: no bounded/transport retry ladder may take it
        self.assertNotIn("effect_receipt", wf._RETRYABLE_CLASSES + wf._BOUNDED_RETRY_CLASSES)

    # ---- (c) bytes exist but the declared digest does not match.
    def test_c_digest_mismatch_refused(self):
        r = self.mk("er-c", [{
            "id": "pub", "type": "agent",
            "goal": "EFFECT-EMIT out/report.json BYTES=64 publish",
            "effects": [{"file": "work/pub/out/report.json", "sha256": "0" * 64}]}])
        self.wf("er-c")
        rec = self.rec(r, "pub")
        self.assertEqual(rec.get("status"), "failed")
        self.assertEqual(rec.get("error_class"), "effect_receipt")
        self.assertIn("sha256 mismatch", rec.get("error", ""))
        rows = rec.get("effect_receipts") or []
        self.assertTrue(rows and rows[0]["ok"] is False
                        and rows[0]["sha256"] == ART_SHA,
                        "measured facts must ride on the FAILED record "
                        "(publication-only recovery re-reads them)")

    # ---- (c2) the TRUE digest + min_bytes stands.
    def test_c2_true_digest_stands(self):
        r = self.mk("er-c2", [{
            "id": "pub", "type": "agent",
            "goal": "EFFECT-EMIT out/report.json BYTES=64 publish",
            "effects": [{"file": "work/pub/out/report.json",
                         "sha256": ART_SHA, "min_bytes": 64}]}])
        self.wf("er-c2")
        self.assertEqual(self.rec(r, "pub").get("status"), "done")

    # ---- (d) declared min_bytes not met => refused, measured count recorded.
    def test_d_min_bytes_enforced(self):
        r = self.mk("er-d", [{
            "id": "pub", "type": "agent", "goal": "EFFECT-EMIT out/small.txt BYTES=5 publish",
            "effects": [{"file": "work/pub/out/small.txt", "min_bytes": 1024}]}])
        self.wf("er-d")
        rec = self.rec(r, "pub")
        self.assertEqual(rec.get("status"), "failed")
        self.assertEqual(rec.get("error_class"), "effect_receipt")
        rows = rec.get("effect_receipts") or []
        self.assertTrue(rows and rows[0]["bytes"] == 5 and rows[0]["ok"] is False)

    # ---- (e) echo commit path: unprovable receipt refused typed; a proven
    #         echo (artifact placed by an ancestor node) commits done.
    def test_e_echo_commit_path_gated(self):
        r = self.mk("er-e1", [
            {"id": "mk", "type": "agent", "goal": "EFFECT-EMIT out/tokens.jsonl BYTES=64 mint",
             "effects": [{"file": "work/mk/out/tokens.jsonl", "min_bytes": 1}]},
            {"id": "say", "type": "echo", "after": ["mk"], "output": "posted",
             "effects": [{"file": "work/mk/out/tokens.jsonl",
                          "sha256": ART_SHA}]},
        ])
        self.wf("er-e1")
        self.assertEqual(self.rec(r, "mk").get("status"), "done")
        say = self.rec(r, "say")
        self.assertEqual(say.get("status"), "done",
                         f"echo with a proven receipt refused: {say}")
        self.assertTrue(all(x["ok"] for x in say.get("effect_receipts") or []))

        r2 = self.mk("er-e2", [{
            "id": "say", "type": "echo", "output": "posted",
            "effects": [{"file": "work/say/never.json"}]}])
        out2 = self.wf("er-e2")
        self.assertNotIn("WORKFLOW_DONE", out2)
        say2 = self.rec(r2, "say")
        self.assertEqual(say2.get("status"), "failed")
        self.assertEqual(say2.get("error_class"), "effect_receipt")
        self.assertNotIn("run.done", (r2 / "events.jsonl").read_text())

    # ---- (f) door: malformed declarations refused fail-closed (closed shape).
    def _effect_errs(self, nodes):
        return [e for e in wfcommon.validate_graph_errors(nodes)
                if "effects" in (e.get("msg") or "") or e.get("field") == "effects"]

    def test_f_door_rejects_malformed(self):
        self.assertEqual(self._effect_errs([{"id": "a", "type": "agent", "effects":
                                             [{"file": "out/x.json"}]}]), [],
                         "well-formed declaration must pass the door")
        for nodes, label in (
            ([{"id": "a", "type": "agent", "effects": []}], "empty list"),
            ([{"id": "a", "type": "agent", "effects": "out.json"}], "bare string"),
            ([{"id": "a", "type": "agent", "effects": [{"file": f"r{i}"} for i in range(9)]}], ">8 rows"),
            ([{"id": "a", "type": "agent", "effects": [{"file": "x", "size": 3}]}], "stray key"),
            ([{"id": "a", "type": "agent", "effects": [{"path": "x"}]}], "missing file key"),
            ([{"id": "a", "type": "agent", "effects": [{"file": "/etc/passwd"}]}], "absolute path"),
            ([{"id": "a", "type": "agent", "effects": [{"file": "../escape"}]}], "parent escape"),
            ([{"id": "a", "type": "agent", "effects": [{"file": "x", "sha256": "beef"}]}], "short sha"),
            ([{"id": "a", "type": "agent", "effects": [{"file": "x", "min_bytes": 0}]}], "min_bytes 0"),
            ([{"id": "a", "type": "agent", "effects": [{"file": "x", "min_bytes": True}]}], "bool min_bytes"),
        ):
            with self.subTest(label):
                self.assertTrue(self._effect_errs(nodes),
                                f"door accepted {label}")
        gate_errs = self._effect_errs([{"id": "g", "type": "gate", "when": "yes",
                                        "after": [], "effects": [{"file": "x"}]}])
        self.assertTrue(gate_errs, "door accepted effects on a gate")

    # ---- (g) honest degradation: undeclared node is byte-identical today.
    def test_g_undeclared_untouched(self):
        r = self.mk("er-g", [{"id": "plain", "type": "agent", "goal": "just answer"}])
        self.wf("er-g")
        rec = self.rec(r, "plain")
        self.assertEqual(rec.get("status"), "done")
        self.assertNotIn("effect_receipts", rec,
                         "undeclared node must carry NO receipt rows")

    # ---- (h) fan-out aggregate: every item's artifact proven => done;
    #         one item without its artifact => the aggregate is refused typed.
    def test_h_fanout_aggregate(self):
        r = self.mk("er-h-ok", [{
            "id": "pub", "type": "agent", "goal": "EFFECT-EMIT out/{item}.json publish {item}",
            "fanout": {"items": ["x", "y"]},
            "effects": [{"file": "work/pub.0/out/x.json"},
                        {"file": "work/pub.1/out/y.json"}]}])
        self.wf("er-h-ok")
        rec = self.rec(r, "pub")
        self.assertEqual(rec.get("status"), "done", f"fan-out proven: {rec}")
        self.assertTrue(all(x["ok"] for x in rec.get("effect_receipts") or []))

        r2 = self.mk("er-h-bad", [{
            "id": "pub", "type": "agent", "goal": "publish {item} (writes nothing)",
            "fanout": {"items": ["x"]},
            "effects": [{"file": "work/pub.0/out/x.json"}]}])
        out2 = self.wf("er-h-bad")
        self.assertNotIn("WORKFLOW_DONE", out2)
        rec2 = self.rec(r2, "pub")
        self.assertEqual(rec2.get("status"), "failed")
        self.assertEqual(rec2.get("error_class"), "effect_receipt")

    # ---- (i) closed set + doc pins.
    def test_i_closed_set_doc_sync(self):
        self.assertIn("effect_receipt", wf.ERROR_CLASSES)
        agents_md = (ROOT / "AGENTS.md").read_text()
        self.assertIn("effect_receipt", agents_md,
                      "AGENTS.md §3d pipe-list must carry the class")
        grammar = (ROOT / "references" / "grammar.md").read_text()
        self.assertIn("effect_receipt_unproven", grammar,
                      "grammar.md must document the refusal copy")


    # External publication receipts are committed evidence, not permission to
    # rerun artifact authors. The finalize barrier must retain a done record.
    TARGET = "https://example.invalid/pull/1#comment-2"
    EXPECT = "report published"

    def external(self):
        return {"target": self.TARGET, "expected-readback": self.EXPECT}

    def receipt(self, **changes):
        row = dict(self.external(), **{"actual-readback": self.EXPECT, "verdict": True})
        row.update(changes)
        return row

    def test_j_external_shape_closed_and_accepted(self):
        for typ in ("agent", "echo"):
            self.assertEqual(self._effect_errs([{
                "id": "pub", "type": typ, "effects": [self.external()]}]), [])
        for row in ({"target": self.TARGET},
                    {"target": "", "expected-readback": self.EXPECT},
                    {"target": self.TARGET, "expected-readback": False},
                    dict(self.external(), file="out/report.json"),
                    dict(self.external(), verdict=True)):
            with self.subTest(row=row):
                self.assertTrue(self._effect_errs([{
                    "id": "pub", "type": "echo", "effects": [row]}]))

    def test_k_missing_external_readback_blocks_without_reauthor(self):
        r = self.mk("er-k", [{"id": "pub", "type": "agent",
            "goal": "EFFECT-EMIT out/report.json BYTES=64 publish the report",
            "effects": [self.external()]}])
        out = self.wf("er-k")
        self.assertNotIn("WORKFLOW_DONE", out)
        rec = self.rec(r, "pub")
        self.assertEqual(rec.get("status"), "done", "bank artifact; do not retry author")
        art = r / "work/pub/out/report.json"
        self.assertEqual(art.read_bytes(), ART)
        before = rec.copy()
        spawn_log = (self.home / "fake.log").read_bytes()
        out2 = self.wf("er-k")
        self.assertNotIn("WORKFLOW_DONE", out2)
        self.assertEqual(self.rec(r, "pub"), before, "re-read must not reauthor the record")
        self.assertEqual((self.home / "fake.log").read_bytes(), spawn_log)
        self.assertEqual(art.read_bytes(), ART)
        events = self.events(r)
        recovery = [e for e in events if e.get("event") == "effect.unreceipted"]
        self.assertTrue(recovery, "no publication-only recovery route")
        self.assertTrue(all(e.get("node") == "pub" and
                            e.get("recovery") == "publication-only" for e in recovery))
        self.assertNotIn("run.done", [e.get("event") for e in events])
        self.assertEqual(wfcommon.run_state(r)["status"], "failed")
        rows = rec["metrics"]["effect_receipts"]
        self.assertEqual(rows, [self.receipt(**{"actual-readback": None, "verdict": False})])

    def test_l_external_false_absent_and_mismatched_evidence_blocks(self):
        for i, receipt in enumerate((
            self.receipt(verdict=False), self.receipt(verdict="true"),
            self.receipt(verdict=1), self.receipt(**{"actual-readback": ""}),
            self.receipt(**{"actual-readback": False}),
            self.receipt(**{"actual-readback": "wrong"}),
            {k: v for k, v in self.receipt().items() if k != "actual-readback"},
            self.receipt(target="wrong"), self.receipt(**{"expected-readback": "wrong"}),
        )):
            with self.subTest(receipt=receipt):
                rid = f"er-l{i}"
                r = self.mk(rid, [{"id": "pub", "type": "echo",
                    "output": {"effect_receipts": [receipt]}, "effects": [self.external()]}])
                self.assertNotIn("WORKFLOW_DONE", self.wf(rid))
                self.assertEqual(self.rec(r, "pub")["status"], "done")
                self.assertEqual(wfcommon.run_state(r)["status"], "failed")
                self.assertTrue(any(e.get("event") == "effect.unreceipted"
                                    for e in self.events(r)))

    def test_m_external_fixed_readback_done_and_metrics_visible(self):
        r = self.mk("er-m", [{"id": "pub", "type": "echo",
            "output": {"artifact": "report.json", "effect_receipts": [self.receipt()]},
            "effects": [self.external()]}])
        self.assertIn("WORKFLOW_DONE", self.wf("er-m"))
        rec = self.rec(r, "pub")
        self.assertEqual(rec["metrics"]["effect_receipts"], [self.receipt()])
        self.assertEqual(wfcommon.run_state(r)["status"], "done")
        # Exercise the actual door view; spending metrics must not hide receipts.
        import importlib.util
        from unittest.mock import patch
        spec = importlib.util.spec_from_file_location("effects_door", ROOT / "__init__.py")
        door = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(door)
        with patch.object(door._common, "run_child_metrics", return_value={}):
            view = door._view(r)
        self.assertEqual(view["nodes"]["pub"]["metrics"]["effect_receipts"], [self.receipt()])

    def test_n_publication_only_echo_recovery_preserves_committed_artifact(self):
        # Public amend accepts the echo-bank replacement; only the receipt is
        # completed, the already-written report never needs an author spawn.
        r = self.mk("er-n", [{"id": "pub", "type": "agent",
            "goal": "EFFECT-EMIT out/report.json BYTES=64 publish the report",
            "effects": [self.external()]}])
        self.assertNotIn("WORKFLOW_DONE", self.wf("er-n"))
        art = r / "work/pub/out/report.json"
        self.assertEqual(art.read_bytes(), ART)
        before = (art.read_bytes(), art.stat().st_mtime_ns)
        spawn_log = (self.home / "fake.log").read_bytes()
        output = self.rec(r, "pub")["output"]
        output = dict(output, effect_receipts=[self.receipt()])
        graph = {"name": "er-n", "nodes": [{"id": "pub", "type": "echo",
            "output": output, "effects": [self.external()]}]}
        self.assertEqual(wfcommon.validate_graph_errors(graph["nodes"]), [])
        (r / "graph.json").write_text(json.dumps(graph))
        self.assertIn("WORKFLOW_DONE", self.wf("er-n"))
        self.assertEqual((art.read_bytes(), art.stat().st_mtime_ns), before)
        self.assertEqual((self.home / "fake.log").read_bytes(), spawn_log)


if __name__ == "__main__":
    unittest.main(verbosity=2)
