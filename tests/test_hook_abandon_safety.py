#!/usr/bin/env python3
"""Upstream v0.21.5 plugin contract: abandoned hook workers + load deadline.

Core v2026.9.24 (hermes_cli/plugins_dispatch.py) bounds ``transform_llm_output``
callbacks by ``plugins.hook_callback_timeout``: on timeout the worker is
ABANDONED, not joined (``if not done.wait(timeout=timeout):  # do not join``),
its result is discarded, and once the 60s suppression window passes "a fresh
call id may start a new worker" while the abandoned one still runs. Python
cannot kill that thread, so it RESUMES later, concurrently with the fresh call.
These cases pin that card_enforcement never double-ships across that
interleaving, and that register() stays far inside ``plugins.load_timeout_seconds``
(plugins_loader.py: default 10s; overrun -> ctx._abandon_load(), every later
register_* silently ignored).
"""
import gc
import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))


def _load_door():
    spec = importlib.util.spec_from_file_location("hw_abandon", ROOT / "__init__.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Ctx:
    def __init__(self):
        self.hooks, self.tools, self.config_reads = {}, [], 0

    def register_hook(self, name, fn):
        self.hooks[name] = fn

    def register_tool(self, **kw):
        self.tools.append(kw)

    def register_command(self, *_, **__):
        pass

    def register_skill(self, *_, **__):
        pass

    def get_config(self, key, default):
        self.config_reads += 1
        return default


class HookAbandonSafety(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory(dir=ROOT / "tests")
        self.env = mock.patch.dict(os.environ, {
            "HERMES_HOME": self.home.name,
            "WF_RUNS_ROOT": str(Path(self.home.name) / "workflows"),
            "HERMES_WF_HERMES_BIN": "offline",
        })
        self.env.start()
        os.environ.pop("HERMES_SESSION_ID", None)
        self.hw = _load_door()
        self.ce = self.hw._card_enforcement
        import wf_test_isolation
        wf_test_isolation.install(self.hw)
        self.ctx = _Ctx()
        t0 = time.perf_counter()
        self.hw.register(self.ctx)
        self.register_secs = time.perf_counter() - t0
        self.hook = self.ctx.hooks["transform_llm_output"]
        self.runs = Path(os.environ["WF_RUNS_ROOT"])
        self.runs.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.env.stop()
        self.home.cleanup()

    def mk_run(self, rid, sid="S"):
        """Owe a card: the launch hook's ledger row (no run dir is ever scanned)."""
        self.hw._card_ledger.add(sid, rid, "t")
        return rid

    def markers(self, rid):
        """1 once the card's ledger entry is dropped (shipped), 0 while still owed."""
        owed = [r for r, _ in self.hw._card_ledger.entries("S")]
        return 0 if rid in owed else 1

    def fire(self, turn, text="All done."):
        return self.hook(response_text=text, session_id="S", turn_id=turn,
                         platform="desktop", model="m")

    @staticmethod
    def select(result):
        # turn_finalizer.apply_llm_output_transform: first `isinstance(r, str) and r` wins.
        return result if isinstance(result, str) and result else None

    # ---- (b) abandoned worker resumes after the re-fire: no double-ship ------
    def test_abandoned_worker_resuming_after_refire_never_double_ships(self):
        # Interleaving the core permits: t1's worker is abandoned mid-_propose;
        # the fresh t2 call proposes the same owed card; the abandoned worker
        # then resumes and lands ITS proposal before the core selects t2's text;
        # finally the core drops t1's discarded result. The card must ship
        # exactly once (t2) and never again.
        r = self.mk_run("r-ab")
        card = '::workflow{id="r-ab"}'
        real_card = self.ce._CARD
        t1_parked, t1_release = threading.Event(), threading.Event()

        def gated_card(rid):
            if threading.current_thread().name == "hermes-hook-t1":
                t1_parked.set()                    # killed mid-flight: parked in _propose
                t1_release.wait(5)
            return real_card(rid)

        outcome = {}

        def runner():                              # mirror of _run_hook_with_timeout._runner
            outcome["value"] = self.fire("t1")

        with mock.patch.object(self.ce, "_CARD", gated_card):
            worker = threading.Thread(target=runner, name="hermes-hook-t1", daemon=True)
            worker.start()
            self.assertTrue(t1_parked.wait(5))
            self.assertFalse(threading.Event().wait(0.05))  # deadline passes -> abandoned
            out2 = self.fire("t2")                 # fresh call id after suppression window
            t1_release.set()                       # abandoned worker resumes...
            worker.join(5)                         # (test-only: the core never joins)
            self.assertFalse(worker.is_alive())
        outcome.clear()                            # core never reads the abandoned result
        gc.collect()                               # its text dies -> weakref callback fires
        winner = self.select(out2)                 # ...then core selects t2's text
        self.assertIsNotNone(winner)
        self.assertTrue(winner.rstrip().endswith(card), winner)
        self.assertGreaterEqual(self.markers(r), 1, "delivered card must be ledgered")
        for turn in ("t3", "t4"):                  # re-fire: shown once, never again
            again = self.fire(turn)
            self.assertIsNone(again, f"card double-shipped on {turn}: {again!r}")

    # ---- (b) ordering: the claim outlives the ledger drop --------------------
    def test_ledger_drop_is_durable_before_the_inmemory_claim_drops(self):
        r = self.mk_run("r-ord")
        seen = {}
        led = self.ce._LEDGER
        real_drop = led.drop

        def spying_drop(sid, rid):
            # A concurrent (abandoned) worker reading the ledger at this instant must
            # find the run still covered by the in-memory claim: nothing re-offered.
            seen["offered"] = self.ce._owed("S")
            return real_drop(sid, rid)

        with mock.patch.object(led, "drop", spying_drop):
            self.assertIsNotNone(self.select(self.fire("t1")))
        self.assertEqual(seen.get("offered"), [], "run re-offered mid-confirm")
        self.assertEqual(self.markers(r), 1)
        self.assertEqual(self.ce._owed("S"), [])

    # ---- (b) a concurrent reconcile racing on the same key stays quiet --------
    def test_reconcile_tolerates_entry_vanishing_under_it(self):
        self.mk_run("r-rc")
        held = self.fire("t1")                     # unselected proposal, still alive
        self.assertTrue([k for k in self.ce._PENDING if k[0] == "S"])
        real_pending = self.ce._PENDING

        class Racing(dict):                        # the other worker pops it first
            def __getitem__(self, k):
                real_pending.pop(k, None)
                raise KeyError(k)

            def get(self, k, d=None):
                real_pending.pop(k, None)
                return d

        with mock.patch.object(self.ce, "_PENDING", Racing(real_pending)):
            self.ce._reconcile("S", "t2")          # must not raise
        del held

    # ---- (a) register() is cheap; never requests an operator-gated override ----
    def test_register_is_cheap_and_never_requests_tool_override(self):
        self.assertLess(self.register_secs, 1.0, "register() must stay far inside "
                        "plugins.load_timeout_seconds (default 10s)")
        self.assertEqual(self.ctx.config_reads, 2, "two cheap config reads (model_tiers + the PR-5 telemetry flag)")
        self.assertEqual([t["name"] for t in self.ctx.tools], ["workflow"])
        self.assertNotIn("override", self.ctx.tools[0],
                         "override=True is operator-gated (PluginToolOverrideError)")


if __name__ == "__main__":
    unittest.main(verbosity=2)
