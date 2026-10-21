#!/usr/bin/env python3
"""#499 — the quota cost gate matches by ROUTE, not by raw model name.

A subscription is a route, not a name. The #24 gate stamped `node["model"]` and looked
that string up raw, which got both directions wrong:

  FALSE REFUSAL — one `fatal_quota` death on provider-A/x refused at admission ANY node
      pinning provider-B/x (a live route on a different bill), killing the whole submit
      with `route_unavailable at submit` — the resubmit-churn class #296 removed from
      the forbidden-model gate.
  FALSE ADMIT — a mark stamped as `anthropic/claude-opus-4.1` did not bind a node
      pinning the folded `claude-opus-4-1` that core actually bills: the gate missed its
      own mark, one name-punctuation apart.

Pinned here (the law, and everything that must NOT relax):
  (1) a provider-blank (bare) mark cannot name an explicit-provider subscription: it
      binds seat-default pins only;
  (2) a provider-qualified mark binds its route by IDENTITY (wfcommon.route_ids_equal),
      so dotted == folded, and never a different provider's subscription;
  (3) the stamp itself rides the qualified id when the dead node pinned a provider, and
      the record carries `provider` (keys gain an optional `provider/` prefix);
  (4) intact: same-subscription refusals, the A1 frozen-skip law, expiry, and the #291
      F3 legacy full-id key (pre-#499 entries keep exactly the binding they had).

Run (stdlib only): python3 tests/test_quota_route_identity_499.py
"""
import importlib.util
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
spec = importlib.util.spec_from_file_location("wf_door_499", ROOT / "__init__.py")
door = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(door)
except SystemExit:
    pass
import wf as wfmod                      # noqa: E402  (the runner's stamp side)
import wfcommon as wc                   # noqa: E402

FUTURE = time.time() + 3600
PAST = time.time() - 60


def entry(provider=None, resets=FUTURE, marker="resets in ~2 hours"):
    e = {"resets_epoch": resets, "at": time.time(), "marker": marker}
    if provider is not None:
        e["provider"] = provider
    return e


def node(nid, **kw):
    return {"id": nid, "type": "agent", "goal": "GO", **kw}


class QuotaRouteIdentity499(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="wf499-")
        self.addCleanup(tmp.cleanup)
        self.cache = Path(tmp.name) / "quota.json"
        # A test-seated cache NEVER pings (the A5 recovery law stays honest in CI).
        env = patch.dict(os.environ, {"HERMES_HOME": tmp.name,
                                      "WF_QUOTA_CACHE": str(self.cache)})
        env.start()
        self.addCleanup(env.stop)

    def write(self, cache):
        self.cache.write_text(json.dumps(cache))

    def refuse(self, nodes, routes=None, skip=()):
        return door._quota_refusal({"name": "q499", "nodes": list(nodes)},
                                   routes=routes, cache_path=self.cache, skip=skip)

    # ---- (1) the false refusal is gone -------------------------------------------
    def test_repro1_bare_mark_does_not_refuse_a_cross_provider_pin(self):
        # issue #499 repro, verbatim shape
        self.write({"gpt-test-model": entry()})
        out = self.refuse([node("lane", provider="openrouter", model="gpt-test-model")])
        self.assertIsNone(out, out)

    def test_bare_mark_with_an_empty_provider_field_is_still_bare(self):
        self.write({"gpt-test-model": entry(provider="")})
        self.assertIsNone(self.refuse([node("lane", provider="other", model="gpt-test-model")]))

    def test_bare_mark_still_refuses_the_seat_default_pin_it_came_from(self):
        self.write({"sol": entry()})
        out = self.refuse([node("lane", model="sol")])
        self.assertIsNotNone(out)
        self.assertIn("quota-exhausted", out)

    # ---- (2) the false admit is closed -------------------------------------------
    def test_repro2_qualified_mark_binds_the_folded_spelling(self):
        # issue #499 repro, vice versa: the gate's own Anthropic mark, hyphenated pin
        self.write({"anthropic/claude-opus-4.1": entry(provider="anthropic")})
        out = self.refuse([node("lane", provider="anthropic", model="claude-opus-4-1")])
        self.assertIsNotNone(out, "same subscription, one punctuation apart: must refuse")
        self.assertIn("lane", out)

    def test_qualified_mark_refuses_its_own_spelling_too(self):
        self.write({"anthropic/claude-opus-4.1": entry(provider="anthropic")})
        self.assertIsNotNone(self.refuse([node("lane", provider="anthropic",
                                               model="anthropic/claude-opus-4.1")]))

    def test_qualified_mark_never_names_another_providers_subscription(self):
        # a dead Anthropic route must not refuse a live OpenRouter pin of the same id
        self.write({"anthropic/claude-opus-4.1": entry(provider="anthropic")})
        self.assertIsNone(self.refuse([node("lane", provider="openrouter",
                                            model="anthropic/claude-opus-4.1")]))

    def test_aggregator_qualified_mark_binds_its_own_namespace(self):
        self.write({"openrouter/anthropic/claude-opus-4.1": entry(provider="openrouter")})
        self.assertIsNotNone(self.refuse([node("lane", provider="openrouter",
                                               model="anthropic/claude-opus-4.1")]))

    def test_resolved_alias_pin_binds_the_mark_of_the_route_it_resolved_to(self):
        # the node pins an alias; the mark names the resolved route (the #24 cache is
        # keyed on what DIED, not on the author's spelling)
        self.write({"anthropic/claude-opus-4.1": entry(provider="anthropic")})
        routes = {"lane": {"resolved": {"provider": "anthropic", "model": "claude-opus-4.1"}}}
        self.assertIsNotNone(self.refuse([node("lane", provider="anthropic", model="opus")],
                                         routes=routes))

    # ---- (4) laws that must NOT relax --------------------------------------------
    def test_legacy_full_id_key_still_binds_the_baked_node_291f3(self):
        self.write({"openai-codex/gpt-6-sol": entry()})            # no provider field
        out = self.refuse([node("lane", provider="openai-codex", model="gpt-6-sol")])
        self.assertIsNotNone(out, "#291 F3: a pre-#499 full-id key binds its own route")

    def test_legacy_prefixed_key_still_binds_the_verbatim_pin(self):
        # an aggregator's vendor namespace is part of the id: the mark IS the pin
        self.write({"anthropic/claude-opus-4.1": entry()})         # no provider field
        self.assertIsNotNone(self.refuse([node("lane", provider="openrouter",
                                               model="anthropic/claude-opus-4.1")]))

    def test_a1_frozen_skip_law_intact(self):
        self.write({"sol": entry()})
        nodes = [node("done1", model="sol"), node("live1", model="m-live")]
        self.assertIsNone(self.refuse(nodes, skip={"done1"}))
        self.assertIsNotNone(self.refuse(nodes))                  # no skip: still refuses

    def test_expired_mark_never_refuses(self):
        self.write({"sol": entry(resets=PAST)})
        self.assertIsNone(self.refuse([node("lane", model="sol")]))

    def test_baked_alias_pin_is_still_the_seats_own_subscription(self):
        # the A1-e2e shape: an ALIAS death wrote a bare mark, and the door later baked
        # a provider onto that same pin. The AUTHOR named no subscription, so the mark
        # still holds — else the gate forgets its own mark on every alias seat.
        self.write({"sol": entry()})
        routes = {"lane": {"requested": {"provider": None, "model": "sol"},
                           "resolved": {"provider": "openai-codex", "model": "gpt-6-sol"}}}
        out = self.refuse([node("lane", provider="openai-codex", model="gpt-6-sol")],
                          routes=routes)
        self.assertIsNotNone(out, "a baked provider is derived, not claimed")

    def test_unpinned_node_never_refuses(self):
        self.write({"sol": entry()})
        self.assertIsNone(self.refuse([node("lane", goal="GO")]))

    # ---- (3) the stamp names the subscription that died ---------------------------
    def test_stamp_side_key_and_provider(self):
        self.assertEqual(wc.quota_stamp(node("a", provider="anthropic",
                                             model="claude-opus-4-1")),
                         ("anthropic/claude-opus-4-1", "anthropic"))
        self.assertEqual(wc.quota_stamp(node("a", model="sol")), ("sol", ""))
        # the baked receipt wins: the author pinned an alias, the route that DIED is proven
        self.assertEqual(wc.quota_stamp({"id": "a", "type": "agent", "model": "opus",
                                         "route_verified": "anthropic/claude-opus-4.1"}),
                         ("anthropic/claude-opus-4.1", "anthropic"))
        # a receipt that contradicts the pin claims no provider: the mark rides the
        # node's OWN subscription, so the same pin still gets refused
        self.assertEqual(wc.quota_stamp({"id": "a", "type": "agent", "provider": "openrouter",
                                         "model": "anthropic/claude-opus-4.1",
                                         "route_verified": "anthropic/claude-opus-4.1"}),
                         ("openrouter/anthropic/claude-opus-4.1", "openrouter"))
        # no receipt at all: the pin is the claim
        self.assertEqual(wc.quota_stamp(node("a", provider="openrouter",
                                             model="gpt-test-model")),
                         ("openrouter/gpt-test-model", "openrouter"))

    def test_note_writes_the_qualified_key_and_the_provider_field(self):
        p = Path(os.environ["WF_QUOTA_CACHE"])
        wfmod._quota_note("anthropic/claude-opus-4.1", "resets in ~2 hours",
                          provider="anthropic")
        data = json.loads(p.read_text())
        self.assertIn("anthropic/claude-opus-4.1", list(data))
        self.assertEqual(data["anthropic/claude-opus-4.1"]["provider"], "anthropic")
        # a seat-default death stays bare and names no subscription
        wfmod._quota_note("sol", "resets in ~2 hours")
        data = json.loads(p.read_text())
        self.assertIn("sol", data)
        self.assertNotIn("provider", data["sol"])

    def test_death_then_refusal_round_trip(self):
        # the seam end-to-end at the unit level: the runner's stamp, the door's bind
        p = Path(os.environ["WF_QUOTA_CACHE"])
        dead = node("a", provider="anthropic", model="claude-opus-4.1")
        key, prov = wc.quota_stamp(dead)
        wfmod._quota_note(key, "HTTP 429: usage limit reached, resets in ~109 hours",
                          provider=prov)
        self.assertIsNone(self.refuse([node("lane", provider="openrouter",
                                            model="claude-opus-4.1")]))   # different bill
        self.assertIsNotNone(self.refuse([node("lane", provider="anthropic",
                                               model="claude-opus-4-1")]))  # its own bill


class StampBindsTable(unittest.TestCase):
    """The contract in one table, without the door in the way."""

    def test_table(self):
        q, dot = "gpt-test-model", "anthropic/claude-opus-4.1"
        self.assertFalse(wc.quota_stamp_binds(q, entry(), "gpt-test-model", "openrouter"))
        self.assertTrue(wc.quota_stamp_binds(q, entry(), "gpt-test-model", None))
        self.assertFalse(wc.quota_stamp_binds(q, entry(), "sol", None))     # another name
        self.assertTrue(wc.quota_stamp_binds(dot, entry("anthropic"), "claude-opus-4-1",
                                             "anthropic"))
        self.assertFalse(wc.quota_stamp_binds(dot, entry("anthropic"), "anthropic/claude-opus-4.1",
                                              "openrouter"))
        self.assertTrue(wc.quota_stamp_binds(dot, entry(), "anthropic/claude-opus-4.1",
                                             "openrouter"))          # legacy verbatim
        self.assertFalse(wc.quota_stamp_binds(dot, entry("anthropic"), "claude-opus-4.1",
                                              None))                 # claims ≠ unclaimed
        self.assertFalse(wc.quota_stamp_binds(q, entry(), "", None))  # absence proves nothing
        self.assertTrue(wc.quota_stamp_binds("SOL", entry(), "sol", None))   # case-folded


if __name__ == "__main__":
    unittest.main(verbosity=2)
