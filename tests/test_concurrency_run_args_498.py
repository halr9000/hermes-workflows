#!/usr/bin/env python3
"""#498: launch-time concurrency override ARGS on the run verb obey the #100 law.

The graph-KEY channel (#100, plugin 1.3.8) shipped clamp-at-door + run.json bake;
this suite pins the launch-time ARG channel: shape validation, owner clamp, bake
under the existing keys, fingerprint exclusion, co-declaration refusal (one
channel per key), byte-identical absence, and `from=` library replay — the
shelved-entry-launches-at-a-different-width case that motivated the issue.

Run: env -u WF_RUNS_ROOT -u HERMES_HOME PYTHONPATH=/opt/hermes
     /opt/hermes/.venv/bin/python tests/test_concurrency_run_args_498.py
"""
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("wf_door_498", ROOT / "__init__.py")
door = importlib.util.module_from_spec(spec)
spec.loader.exec_module(door)
import wf_test_isolation
wf_test_isolation.install(door)

BASE = {"name": "args498", "nodes": [{"id": "a", "type": "echo", "output": 1}]}


class Owner:
    def __init__(self):
        self.settings = {}

    def get_config(self, key, default=None):
        return self.settings.get(key, default)


class ConcurrencyRunArgs498(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="wf498-args-")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        (self.root / "home").mkdir()
        env = patch.dict(os.environ, {"WF_RUNS_ROOT": str(self.root / "runs"),
                                      "HERMES_HOME": str(self.root / "home")})
        env.start()
        self.addCleanup(env.stop)
        wf_test_isolation.install(door)  # resolver-level pin, not a _CTX substitute
        self.owner = Owner()
        ctx = patch.object(door, "_CTX", self.owner)
        ctx.start()
        self.addCleanup(ctx.stop)
        spawn = patch.object(door, "_spawn_runner")
        self.spawn = spawn.start()
        self.addCleanup(spawn.stop)

    def _write_run(self, run_args, graph_keys=None):
        return door.act_run({"graph": {**BASE, **(graph_keys or {})}, **run_args})

    def launch(self, run_args, graph_keys=None):
        result = self._write_run(run_args, graph_keys)
        self.assertIn("run_id", result, result)
        path = self.root / "runs" / result["run_id"]
        self.assertTrue(path.is_dir(), str(path))
        return json.loads((path / "run.json").read_text()), path

    def refused(self, run_args, graph_keys=None):
        result = self._write_run(run_args, graph_keys)
        self.assertNotIn("run_id", result, result)
        # Refusal law: BEFORE any write — a refused submit leaves no run dir.
        self.assertFalse(list((self.root / "runs").glob("*/run.json")),
                         "refused submit must not write")
        return result.get("error", "")

    def test_args_clamped_to_default_caps_and_owner_caps(self):
        # No owner settings: the runner's historical 4/8 ceilings apply to args,
        # exactly as they apply to graph keys (#100 law, unchanged).
        meta, _ = self.launch({"concurrency": 20, "item_concurrency": 30})
        self.assertEqual((meta["concurrency"], meta["item_concurrency"]), (4, 8))
        self.owner.settings.update(max_concurrency=6, max_item_concurrency=3)
        meta, _ = self.launch({"concurrency": 16, "item_concurrency": 12})
        self.assertEqual((meta["concurrency"], meta["item_concurrency"]), (6, 3))
        meta, _ = self.launch({"concurrency": 2, "item_concurrency": 1})
        self.assertEqual((meta["concurrency"], meta["item_concurrency"]), (2, 1))

    def test_co_decl_refused_naming_key_even_when_equal(self):
        # One channel per key (params-vs-#126 law): the arg and the graph key of
        # the same name are a co-declaration EVEN when the values agree.
        for key in ("concurrency", "item_concurrency"):
            for value in (2, 4):
                err = self.refused({key: value}, graph_keys={key: value})
                self.assertIn(key, err, err)
                self.assertIn("co-declared", err, err)

    def test_mixed_channels_are_not_a_co_declaration(self):
        # A graph key on one name and an arg on the other is two DIFFERENT keys,
        # each single-channel: both ride their own law.
        meta, _ = self.launch({"item_concurrency": 2}, graph_keys={"concurrency": 3})
        self.assertEqual((meta["concurrency"], meta["item_concurrency"]), (3, 2))

    def test_bad_arg_shape_refused_before_write(self):
        # The positive-int graph-key law verbatim: booleans, floats, strings,
        # null, zero and negatives all refuse at submit, naming the key.
        for key in ("concurrency", "item_concurrency"):
            for value in (0, -1, True, 1.5, "2", None, []):
                with self.subTest(key=key, value=value):
                    err = self.refused({key: value})
                    self.assertIn(key, err, err)

    def test_absent_args_byte_identical(self):
        meta, _ = self.launch({})
        self.assertEqual(set(meta), {"name", "hermes_bin", "started", "fp_rule_version",
                                     "owner", "launch_root", "plugin_version"}, meta)
        self.assertNotIn("concurrency", meta)
        self.assertNotIn("item_concurrency", meta)

    def test_arg_channel_bakes_exactly_like_the_graph_key_channel(self):
        # The two channels converge: same effective meta (modulo the start clock)
        # and byte-identical graph.json — the arg IS the #100 law fed from the run.
        meta_arg, path_arg = self.launch({"concurrency": 2, "item_concurrency": 1})
        meta_key, path_key = self.launch({}, graph_keys={"concurrency": 2,
                                                         "item_concurrency": 1})
        strip = lambda m: {k: v for k, v in m.items() if k != "started"}
        self.assertEqual(strip(meta_arg), strip(meta_key))
        self.assertEqual((path_arg / "graph.json").read_bytes(),
                         (path_key / "graph.json").read_bytes())

    def test_arg_keys_stay_out_of_the_fingerprint(self):
        # efP law, mirrored from #100: the keys never participate in
        # graph_fingerprint, so an arg-baked width never un-freezes committed
        # nodes. graph.json carries the REQUESTED width (author-channel parity —
        # the clamp lives in run.json meta, exactly like a graph-key launch), and
        # the fingerprint ignores it either way.
        _, path = self.launch({"concurrency": 16, "item_concurrency": 2})
        common = door._common
        written = json.loads((path / "graph.json").read_text())
        self.assertEqual(written.get("concurrency"), 16)
        self.assertEqual(common.graph_fingerprint(written),
                         common.graph_fingerprint({k: v for k, v in written.items()
                                                   if k not in ("concurrency",
                                                                "item_concurrency")}))

    def test_library_replay_takes_the_arg_channel(self):
        # The motivating case: a shelved entry launched at a different width than
        # it was authored. The shelf bytes must not move.
        entry = self.root / "runs" / "library"
        entry.mkdir(parents=True)
        shelf = entry / "wide498.json"
        shelf.write_text(json.dumps(BASE))
        result = door.act_run({"from": "wide498", "concurrency": 2})
        self.assertIn("run_id", result, result)
        meta = json.loads((self.root / "runs" / result["run_id"] / "run.json").read_text())
        self.assertEqual(meta["concurrency"], 2)
        self.assertEqual(shelf.read_bytes(), json.dumps(BASE).encode())

    def test_library_replay_co_decl_refused(self):
        # A shelved entry that already carries the graph key takes the ARG as a
        # co-declaration — refused, same law, before any write.
        entry = self.root / "runs" / "library"
        entry.mkdir(parents=True)
        shelf = entry / "widekey498.json"
        shelf.write_text(json.dumps({**BASE, "concurrency": 3}))
        result = door.act_run({"from": "widekey498", "concurrency": 3})
        self.assertNotIn("run_id", result, result)
        self.assertIn("concurrency", result.get("error", ""))
        self.assertFalse(list((self.root / "runs").glob("*/run.json")),
                         "refused submit must not write")

    def test_dry_run_with_args_passes_gates_without_writes(self):
        result = door.act_run({"graph": BASE, "concurrency": 3, "dry_run": True})
        self.assertTrue(result.get("ok"), result)
        self.assertTrue(result.get("dry_run"), result)
        self.assertFalse(list((self.root / "runs").glob("*/run.json")),
                         "dry_run writes nothing")


if __name__ == "__main__":
    unittest.main()
