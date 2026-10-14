#!/usr/bin/env python3
"""#114 (WF-04): one shared metrics fold — unknown api_calls must stay UNKNOWN on
every aggregation level of BOTH projection surfaces, never advertised as a number.

Reproduces the issue's divergence fixture through the two production adapters
(dual-adapter pattern of tests/test_current_attempt_metrics.py — exec-loads
__init__.py + dashboard/plugin_api.py, stubs run_state/child metrics/current
attempt facts) over ONE fixture grid:

  issue fixture  {api_calls: 0, api_calls_known: False}
      -> api_calls ABSENT at node AND run level on BOTH surfaces
         (RED on main: door printed 0 in the run total, dashboard printed 0
          on the node line — the divergence table's bold cells)
  mixed known/unknown
      -> omitted at node, at the unknown fanout item, and at run level on both
         surfaces; the known fanout item keeps its exact numeric
  all-known / genuine known-zero {api_calls: 0, api_calls_known: True}
      -> today's exact numbers and shapes on every level (regression pin)
  fanout item_metrics, retries and heartbeat live/idle_s/last/last_tool_at
      -> unchanged

Asserts returned dicts only, never helper names in source (R6).
R8 affected set (graphify, at 2d97807): _fold_metrics 3 callers
(plugin_api.py:159/165/171); act_status callers act_wait/_wait_foreign — no
third fold exists (scripts/ grep api_calls|child_metrics|_fold is empty).

Standalone (NOT pytest): python3 tests/test_metrics_unknown_fold_114.py
"""
import importlib.util
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import wfcommon as WFC  # noqa: E402  (real read model; runs nothing)

RUN = "metrics114"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


door = _load("wf114_door", ROOT / "__init__.py")
api = _load("wf114_dashboard", ROOT / "dashboard" / "plugin_api.py")


def row(tokens_in=10, tokens_out=2, api_calls=1, api_calls_known=True,
        tool_calls=1, cost=0.0, model="fake"):
    """One child_metrics() row exactly as wfcommon.child_metrics folds it."""
    return {"tokens_in": tokens_in, "tokens_out": tokens_out, "cache_read": 0,
            "reasoning": 0, "api_calls": api_calls, "api_calls_known": api_calls_known,
            "tool_calls": tool_calls, "cost": cost, "attempts": 1, "model": model,
            "billing_provider": None, "last_activity": None, "last_desc": None,
            "ended": None, "started": None, "sessions": {}}


def st_for(nodes):
    """run_state-shaped state for node ids in `nodes` (both adapters read ONE)."""
    return {
        "run_id": RUN, "name": "metrics114", "status": "done", "runner_live": False,
        "failed_nodes": [],   # sys-5lnm17: run_state() always publishes it; nothing failed here
        "nodes": {nid: {"type": "agent", "status": "done", "after": [],
                        "fanout": nid == "a", "stale_of_amend": None} for nid in nodes},
        "done": len(nodes), "skipped": 0, "total": len(nodes),
        "held_gate": None, "started": None, "owner": None,
        "graph": {"name": "metrics114",
                  "nodes": [{"id": nid, "type": "agent", "goal": "g"} for nid in nodes]},
    }


class MetricsUnknownFold114(unittest.TestCase):
    """Both surfaces driven over one fixture grid; asserts returned dicts only."""

    def drive(self, cm, nodes=("a",)):
        """Return (door_out, dashboard_view) for the stubbed read model."""
        st = st_for(nodes)
        tmp = Path(tempfile.mkdtemp(prefix="wf114-")) / RUN
        tmp.mkdir(parents=True)
        st["run_dir"] = tmp
        fake_common = types.SimpleNamespace(
            # (#44 keeps the forge honest: plugin_api._view threads a lock_state
            # kwarg into run_state — the forged read ignores it, est-lf0j pattern.)
            run_state=lambda r, lock_state=None: dict(st),
            run_child_metrics=lambda r: dict(cm),
            current_attempt=WFC.current_attempt,        # the REAL heartbeat fn
            node_facts=lambda r, nid, index=None: None,
            fold_child_metrics=lambda rows: WFC.fold_child_metrics(rows),
            profiles_by_session=lambda: {})
        with patch.object(door, "_resolve_read_run", lambda rid: (tmp, dict(st), None)), \
             patch.object(door._common, "run_child_metrics", lambda r: dict(cm)), \
             patch.object(api, "_workflow_common", lambda: fake_common):
            return door.act_status({"run_id": RUN, "detail": "full"}), api._view(tmp, full=True)

    # ---- the issue fixture: unknown is unknown on every level, both surfaces ----

    def test_issue_fixture_unknown_zero_absent_node_and_run_both_surfaces(self):
        cm = {f"wf:{RUN}:a:abcd.1": row(12, 3, api_calls=0, api_calls_known=False,
                                        tool_calls=1)}
        tool, dash = self.drive(cm)
        self.assertNotIn("api_calls", tool["nodes"]["a"]["metrics"])   # door node (green on main)
        self.assertNotIn("api_calls", tool["metrics"])                  # door run  (RED on main)
        self.assertNotIn("api_calls", dash["nodes"]["a"]["metrics"])    # dash node (RED on main)
        self.assertNotIn("api_calls", dash["metrics"])                  # dash rollup (green on main)
        # presentation untouched: tokens string, children, tool_calls, scope
        self.assertEqual(tool["nodes"]["a"]["metrics"]["tokens"], "12▸3")
        self.assertEqual(tool["nodes"]["a"]["metrics"]["tool_calls"], 1)
        self.assertEqual(tool["nodes"]["a"]["metrics"]["children"], 1)
        self.assertEqual(dash["nodes"]["a"]["metrics"]["tokens_in"], 12)
        self.assertEqual(dash["nodes"]["a"]["metrics"]["scope"], "cumulative")

    # ---- mixed known/unknown: no partial sum advertised as the exact total ----

    def test_mixed_known_unknown_omits_node_item_and_run_on_both_surfaces(self):
        cm = {
            f"wf:{RUN}:a:0:abcd.1": row(10, 2, api_calls=5),                       # known item
            f"wf:{RUN}:a:1:abcd.2": row(20, 4, api_calls=0, api_calls_known=False),  # unknown item
            f"wf:{RUN}:b:beef.1": row(7, 1, api_calls=2, cost=0.5),               # known node
        }
        tool, dash = self.drive(cm, nodes=("a", "b"))
        # node level: node a's fold mixes an unknown row -> absent on both surfaces
        self.assertNotIn("api_calls", tool["nodes"]["a"]["metrics"])
        self.assertNotIn("api_calls", dash["nodes"]["a"]["metrics"])
        # fanout item level: the KNOWN item keeps its exact number, the unknown
        # item is absent (dashboard today keeps 0 — RED on main)
        items = dash["nodes"]["a"]["item_metrics"]
        self.assertEqual(items[0]["api_calls"], 5)
        self.assertEqual(items[0]["tokens_in"], 10)
        self.assertNotIn("api_calls", items[1])
        self.assertEqual(items[1]["tokens_in"], 20)          # spend persists
        self.assertEqual(items[1]["live"], 0)                # heartbeat law unchanged
        # run level: any unknown contribution -> absent on both surfaces
        self.assertNotIn("api_calls", tool["metrics"])       # RED on main (partial sum 7)
        self.assertNotIn("api_calls", dash["metrics"])
        # the fully-known sibling node still reports its exact number
        self.assertEqual(tool["nodes"]["b"]["metrics"]["api_calls"], 2)
        self.assertEqual(dash["nodes"]["b"]["metrics"]["api_calls"], 2)
        self.assertEqual(tool["nodes"]["b"]["metrics"]["cost_usd"], 0.5)
        self.assertEqual(tool["metrics"]["tool_calls"], 3)   # counters still sum exactly
        self.assertEqual(dash["metrics"]["tool_calls"], 3)
        self.assertEqual(tool["metrics"]["tokens"], "37▸7")
        self.assertEqual(dash["metrics"]["tokens_in"], 37)

    # ---- all-known: today's exact numbers and shapes everywhere (regression pin) ----

    def test_all_known_keeps_exact_numbers_and_shapes(self):
        cm = {
            f"wf:{RUN}:a:0:abcd.1": row(10, 2, api_calls=5, cost=0.25),
            f"wf:{RUN}:a:1:abcd.2": row(20, 4, api_calls=3, cost=0.25),
            f"wf:{RUN}:b:beef.1": row(7, 1, api_calls=2),
        }
        tool, dash = self.drive(cm, nodes=("a", "b"))
        self.assertEqual(tool["nodes"]["a"]["metrics"]["api_calls"], 8)
        self.assertEqual(tool["nodes"]["a"]["metrics"]["tokens"], "30▸6")
        self.assertEqual(tool["nodes"]["a"]["metrics"]["cost_usd"], 0.5)
        self.assertEqual(dash["nodes"]["a"]["metrics"]["api_calls"], 8)
        self.assertEqual(dash["nodes"]["a"]["item_metrics"][0]["api_calls"], 5)
        self.assertEqual(dash["nodes"]["a"]["item_metrics"][1]["api_calls"], 3)
        self.assertEqual(tool["metrics"]["api_calls"], 10)
        self.assertEqual(tool["metrics"]["cost_usd"], 0.5)
        self.assertEqual(dash["metrics"]["api_calls"], 10)

    def test_genuine_known_zero_stays_an_exact_zero_everywhere(self):
        cm = {f"wf:{RUN}:a:abcd.1": row(4, 1, api_calls=0, api_calls_known=True)}
        tool, dash = self.drive(cm)
        self.assertEqual(tool["nodes"]["a"]["metrics"]["api_calls"], 0)
        self.assertEqual(dash["nodes"]["a"]["metrics"]["api_calls"], 0)
        self.assertEqual(tool["metrics"]["api_calls"], 0)
        self.assertEqual(dash["metrics"]["api_calls"], 0)
        # heartbeat law unchanged: no verified spawn -> not live, no idle invented
        self.assertNotIn("live", tool["nodes"]["a"]["metrics"])
        self.assertEqual(dash["nodes"]["a"]["metrics"]["live"], 0)
        self.assertIsNone(dash["nodes"]["a"]["metrics"]["last_activity"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
