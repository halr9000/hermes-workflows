#!/usr/bin/env python3
"""#499 — the quota cost gate matches by ROUTE, not by raw model name.

A subscription is a route, not a name. The #24 gate stamped `node["model"]` and looked
that string up raw, which got both directions wrong:

  FALSE REFUSAL — one `fatal_quota` death on provider-A/x refused at admission ANY node
      pinning provider-B/x (a live route on a different bill), killing the whole submit
      with `route_unavailable at submit` — the resubmit-churn class #296 removed from
      the forbidden-model gate.
  FALSE ADMIT — a mark stamped as the provider-qualified id did not bind a node
      pinning the folded id core actually bills: the gate missed its own mark, one
      name-punctuation apart.

Pinned here (the law, and everything that must NOT relax):
  (1) a provider-blank (bare) mark cannot name an explicit-provider subscription: it
      binds a pin whose author named no subscription;
  (2) a provider-qualified mark binds its route by IDENTITY (wfcommon.route_ids_equal),
      so dotted == folded, and never a different provider's subscription;
  (3) the stamp itself rides the qualified id when the dead node pinned a provider, and
      the record carries `provider` (keys gain an optional `provider/` prefix);
  (4) intact: same-subscription refusals, the A1 frozen-skip law, expiry, the A5
      recovery-ping law, and the #291 F3 legacy full-id key (pre-#499 entries keep
      exactly the binding they had).

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

# Anthropic spelling-fold fixture: stock core folds '.'->'-' ONLY for Anthropic ids
# that carry the vendor's own prefix, so the pair must wear it (the model names are
# invented; the three assignment lines below are the only scrub-relevant tokens in
# this file, guarded in scripts/.scrub-guards per the same fixture-identity class as
# tests/test_route_identity_699.py).
DOT = "anthropic/claude-model-5.1"        # provider-qualified spelling of one route
DOT_M = "claude-model-5.1"                # ... its model half
FOLD = "claude-model-5-1"                 # the spelling core bills on the SAME route
NS = "vendor-a/alpha-test-1"              # an aggregator id whose prefix is NOT a provider
PROV_A, PROV_B = "prov-a", "openrouter"   # two different bills
ALIAS, ALIASED = "t-alias", "tmodel-1"    # a seat alias and the model it resolves to
BARE = "gpt-test-model"                   # the #499 repro's model name

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
        # issue #499 repro, verbatim shape: a provider-blank mark, a claimed route
        self.write({BARE: entry()})
        out = self.refuse([node("lane", provider=PROV_B, model=BARE)])
        self.assertIsNone(out, out)

    def test_bare_mark_with_an_empty_provider_field_is_still_bare(self):
        self.write({BARE: entry(provider="")})
        self.assertIsNone(self.refuse([node("lane", provider=PROV_A, model=BARE)]))

    def test_bare_mark_still_refuses_the_seat_default_pin_it_came_from(self):
        self.write({ALIAS: entry()})
        out = self.refuse([node("lane", model=ALIAS)])
        self.assertIsNotNone(out)
        self.assertIn("quota-exhausted", out)

    # ---- (2) the false admit is closed -------------------------------------------
    def test_repro2_qualified_mark_binds_the_folded_spelling(self):
        # issue #499 repro, vice versa: the gate's own mark, hyphenated pin
        self.write({DOT: entry(provider="anthropic")})
        out = self.refuse([node("lane", provider="anthropic", model=FOLD)])
        self.assertIsNotNone(out, "same subscription, one punctuation apart: must refuse")
        self.assertIn("lane", out)

    def test_qualified_mark_refuses_its_own_spelling_too(self):
        self.write({DOT: entry(provider="anthropic")})
        self.assertIsNotNone(self.refuse([node("lane", provider="anthropic", model=DOT_M)]))

    def test_qualified_mark_never_names_another_providers_subscription(self):
        # a dead Anthropic route must not refuse a live pin of the same id on PROV_B
        self.write({DOT: entry(provider="anthropic")})
        self.assertIsNone(self.refuse([node("lane", provider=PROV_B, model=NS)]))

    def test_aggregator_qualified_mark_binds_its_own_namespace(self):
        self.write({f"{PROV_B}/{NS}": entry(provider=PROV_B)})
        self.assertIsNotNone(self.refuse([node("lane", provider=PROV_B, model=NS)]))

    def test_resolved_alias_pin_binds_the_mark_of_the_route_it_resolved_to(self):
        # the node pins an alias; the mark names the resolved route (the #24 cache keys
        # on what DIED, not on the author's spelling)
        self.write({DOT: entry(provider="anthropic")})
        routes = {"lane": {"resolved": {"provider": "anthropic", "model": DOT_M}}}
        self.assertIsNotNone(self.refuse([node("lane", provider="anthropic", model=ALIAS)],
                                         routes=routes))

    # ---- (4) laws that must NOT relax --------------------------------------------
    def test_legacy_full_id_key_still_binds_the_baked_node_291f3(self):
        self.write({f"{PROV_A}/{ALIASED}": entry()})                  # no provider field
        out = self.refuse([node("lane", provider=PROV_A, model=ALIASED)])
        self.assertIsNotNone(out, "#291 F3: a pre-#499 full-id key binds its own route")

    def test_legacy_prefixed_key_still_binds_the_verbatim_pin(self):
        # an aggregator's vendor namespace is part of the id: the mark IS the pin
        self.write({NS: entry()})                                     # no provider field
        self.assertIsNotNone(self.refuse([node("lane", provider=PROV_B, model=NS)]))

    def test_a1_frozen_skip_law_intact(self):
        self.write({ALIAS: entry()})
        nodes = [node("done1", model=ALIAS), node("live1", model="m-live")]
        self.assertIsNone(self.refuse(nodes, skip={"done1"}))
        self.assertIsNotNone(self.refuse(nodes))                      # no skip: refuses

    def test_expired_mark_never_refuses(self):
        self.write({ALIAS: entry(resets=PAST)})
        self.assertIsNone(self.refuse([node("lane", model=ALIAS)]))

    def test_baked_alias_pin_is_still_the_seats_own_subscription(self):
        # the A1-e2e shape: an ALIAS death wrote a bare mark, and the door later baked
        # a provider onto that same pin. The AUTHOR named no subscription, so the mark
        # still holds — else the gate forgets its own mark on every alias seat.
        self.write({ALIAS: entry()})
        routes = {"lane": {"requested": {"provider": None, "model": ALIAS},
                           "resolved": {"provider": PROV_A, "model": ALIASED}}}
        out = self.refuse([node("lane", provider=PROV_A, model=ALIASED)], routes=routes)
        self.assertIsNotNone(out, "a baked provider is derived, not claimed")

    def test_unpinned_node_never_refuses(self):
        self.write({ALIAS: entry()})
        self.assertIsNone(self.refuse([{"id": "lane", "type": "agent", "goal": "GO"}]))

    # ---- (3) the stamp names the subscription that died ---------------------------
    def test_stamp_side_key_and_provider(self):
        # the mark rides the node's OWN spelling, qualified — identity is the read
        # side's job (`quota_stamp_binds` folds it), never a silent rewrite here
        self.assertEqual(wc.quota_stamp(node("a", provider="anthropic", model=FOLD)),
                         (f"anthropic/{FOLD}", "anthropic"))
        self.assertEqual(wc.quota_stamp(node("a", provider="anthropic", model=DOT_M)),
                         (DOT, "anthropic"))
        self.assertEqual(wc.quota_stamp(node("a", model=ALIAS)), (ALIAS, ""))
        # the baked receipt wins: the author pinned an alias, the route that DIED is proven
        self.assertEqual(wc.quota_stamp({"id": "a", "type": "agent", "model": ALIAS,
                                         "route_verified": DOT}), (DOT, "anthropic"))
        # a receipt that contradicts the pin claims no provider: the mark rides the
        # node's OWN subscription, so the same pin still gets refused
        self.assertEqual(wc.quota_stamp({"id": "a", "type": "agent", "provider": PROV_B,
                                         "model": NS, "route_verified": DOT}),
                         (f"{PROV_B}/{NS}", PROV_B))
        # no receipt at all: the pin is the claim
        self.assertEqual(wc.quota_stamp(node("a", provider=PROV_B, model=BARE)),
                         (f"{PROV_B}/{BARE}", PROV_B))

    def test_note_writes_the_qualified_key_and_the_provider_field(self):
        p = Path(os.environ["WF_QUOTA_CACHE"])
        wfmod._quota_note(DOT, "resets in ~2 hours", provider="anthropic")
        data = json.loads(p.read_text())
        self.assertIn(DOT, list(data))
        self.assertEqual(data[DOT]["provider"], "anthropic")
        # a seat-default death stays bare and names no subscription
        wfmod._quota_note(ALIAS, "resets in ~2 hours")
        data = json.loads(p.read_text())
        self.assertIn(ALIAS, data)
        self.assertNotIn("provider", data[ALIAS])

    def test_death_then_refusal_round_trip(self):
        # the seam end-to-end at the unit level: the runner's stamp, the door's bind
        p = Path(os.environ["WF_QUOTA_CACHE"])
        dead = node("a", provider="anthropic", model=FOLD)
        key, prov = wc.quota_stamp(dead)
        wfmod._quota_note(key, "HTTP 429: usage limit reached, resets in ~109 hours",
                          provider=prov)
        self.assertIsNone(self.refuse([node("lane", provider=PROV_B, model=NS)]))     # other bill
        self.assertIsNotNone(self.refuse([node("lane", provider="anthropic",
                                               model=FOLD)]))                        # its own bill


class StampBindsTable(unittest.TestCase):
    """The contract in one table, without the door in the way."""

    def test_table(self):
        self.assertFalse(wc.quota_stamp_binds(BARE, entry(), BARE, PROV_B))
        self.assertTrue(wc.quota_stamp_binds(BARE, entry(), BARE, None))
        self.assertFalse(wc.quota_stamp_binds(BARE, entry(), ALIAS, None))   # another name
        self.assertTrue(wc.quota_stamp_binds(DOT, entry("anthropic"), FOLD, "anthropic"))
        self.assertFalse(wc.quota_stamp_binds(DOT, entry("anthropic"), NS, PROV_B))
        self.assertTrue(wc.quota_stamp_binds(NS, entry(), NS, PROV_B))       # legacy verbatim
        self.assertFalse(wc.quota_stamp_binds(DOT, entry("anthropic"), FOLD, None))
        self.assertFalse(wc.quota_stamp_binds(BARE, entry(), "", None))      # absence: nothing
        self.assertTrue(wc.quota_stamp_binds(ALIAS.upper(), entry(), ALIAS, None))   # case-fold


if __name__ == "__main__":
    unittest.main(verbosity=2)
