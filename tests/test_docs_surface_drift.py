"""Docs-surface drift guard: the README's tool-action table must name exactly
the actions the door ACTUALLY dispatches, in both directions.

  * every dispatched action has a table row (a silent new action is undocumented);
  * a table row without code is an unshipped claim and must carry an
    `(open PR #NN)` tag (the doc-only direction);
  * INVERSE: a row for an action that IS dispatched must NOT carry an
    `(open PR #NN)` tag — even while that PR is open. A shipped action tagged as
    unshipped is the same lie in the other direction; the tag says "only a
    human knows whether this is real", and for a dispatched action the code
    already answered. (Reconcile est-4vnq finding 2.)

Why actual dispatch and not source text (finding 1 / R6): an earlier revision
of this pin regex-scanned `__init__.py` source for `"name": act_` pairs, so a
callable added to ACTIONS by any edit shape the regex missed stayed invisible:
a mutation adding `"probe": lambda a: True` to ACTIONS with no README row left
the old pin printing `ALL PASS: docs surface matches code` (exit 0) while the
imported door demonstrably had a 13th dispatched key. The pin must execute the
isolated door and compare ACTUAL dispatch keys. Checks 6a-6d below are the
mutation self-proof, run every time: they re-assert each direction bites, so
this pin can never again silently degrade into theater.

Shipped/unshipped tag STATES (merged? closed?) are a separate half of the same
law, audited against GitHub by scripts/pr_tag_audit.py — which now enforces
this inverse direction too, independently of this file.
"""
import importlib.util
import re
import sys
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]

ok = True
count_pass = 0
count_fail = 0


def check(label, cond, detail: object = ""):
    global ok, count_pass, count_fail
    if cond:
        count_pass += 1
    else:
        count_fail += 1
    print(("PASS " if cond else "FAIL ") + label + ("" if cond or not detail else f"  {detail}"))
    ok = ok and cond


def load_door(door_dir: Path):
    """Execute the isolated door by path and return its ACTUAL ACTIONS dict.

    Import is cheap: the door binds wfcommon by path and every host import is
    lazy (measured ~0.06 s). A door that cannot be executed is a hard failure,
    never a silent skip to the old regex path."""
    name = f"_hw_door_surface_probe_{abs(hash(str(door_dir))) % 10**8}"
    spec = importlib.util.spec_from_file_location(name, door_dir / "__init__.py")
    assert spec is not None and spec.loader is not None, f"door not loadable from {door_dir}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    actions = getattr(mod, "ACTIONS", None)
    assert isinstance(actions, dict) and actions, f"{door_dir}: no non-empty ACTIONS dict"
    return actions


TABLE_ANCHOR = "## The `workflow` tool"
TABLE_END = "Plus a `/wf`"
TAG_RE = re.compile(r"open PRs?\s+#\d+")


def table_rows(readme_text: str) -> dict:
    """action -> full row line, from the rows under the tool-table anchor.

    wf165c/B3: a duplicate row for the same action used to SILENTLY OVERWRITE —
    a tagged shipped row followed by an untagged copy read as untagged and the
    inverse check went blind. Occurrences are counted; check 6b holds the line.
    """
    section = readme_text.split(TABLE_ANCHOR, 1)
    assert len(section) == 2, f"README lost the {TABLE_ANCHOR!r} section the action table lives in"
    table = section[1].split(TABLE_END, 1)[0]
    rows = {}
    seen: dict = {}
    for line in table.splitlines():
        m = re.match(r"^\|\s*`([a-z_]+)`\s*\|", line.strip())
        if m:
            action = m.group(1)
            seen[action] = seen.get(action, 0) + 1
            rows[action] = line.strip()
    rows["__duplicates__"] = sorted(a for a, n in seen.items() if n > 1)
    assert rows, "README action table parsed zero rows — did the table shape change? Fix this test WITH the README."
    return rows


def compare(code_actions: set, rows: dict) -> dict:
    """Pure bidirectional comparison — the self-proof mutates inputs, never the tree."""
    real = {a: r for a, r in rows.items() if a != "__duplicates__"}
    return {
        "missing": sorted(code_actions - set(real)),
        "extra_untagged": sorted(a for a in set(real) - code_actions if not TAG_RE.search(real[a])),
        "shipped_tagged": sorted(a for a in code_actions & set(real) if TAG_RE.search(real[a])),
    }


# ---- 1: the door executes and exposes a real dispatch dict (the single source of truth)
try:
    door_actions = load_door(root)
    check("1 door executes isolated and exposes ACTIONS", True)
except Exception as exc:  # noqa: BLE001 — any load failure is a hard red, never a skip
    door_actions = {}
    check("1 door executes isolated and exposes ACTIONS", False, f"{type(exc).__name__}: {exc}")

check("2 every ACTIONS value is callable (dispatch would execute it)",
      bool(door_actions) and all(callable(v) for v in door_actions.values()),
      sorted(k for k, v in door_actions.items() if not callable(v)))
code_actions = set(door_actions)

# ---- core sanity: the verbs the README and every doc promise
check("3 core verbs present in ACTUAL dispatch",
      {"run", "status", "wait", "library"} <= code_actions and len(code_actions) >= 11,
      sorted(code_actions))

readme = (root / "README.md").read_text(encoding="utf-8")
rows = table_rows(readme)

# ---- 4: forward direction — every dispatched action has a row
res = compare(code_actions, rows)
check("4 README action table names every code action",
      not res["missing"], f"README action table missing code actions: {res['missing']}")

# ---- 5: doc-only direction — an unshipped row must carry an open-PR tag
check("5 unshipped rows are PR-tagged",
      not res["extra_untagged"],
      f"README documents action(s) not in code and not PR-tagged: {res['extra_untagged']}")

# ---- 6: INVERSE direction — a dispatched action's row must not claim unshipped
check("6 no shipped action carries an open-PR tag (inverse assertion)",
      not res["shipped_tagged"],
      f"shipped action row(s) tagged (open PR #NN) while dispatched: {res['shipped_tagged']} "
      f"— remove the tag or un-shim the code")

# ---- 6b: duplicate rows for one action are a drift-hiding hazard (wf165c/B3):
# a tagged shipped row followed by an untagged copy used to overwrite silently
# and blind checks 4-6. One row per action, full stop.
check("6b no action appears twice in the README action table",
      not rows["__duplicates__"],
      f"duplicate row(s) for action(s): {rows['__duplicates__']} — last-write-wins hid drift")


# ---- 7: mutation self-proof — every direction bites, asserted every run.
# The observed theater this replaces (finding 1, R6): adding
# `"probe": lambda a: True` to ACTIONS with no README row left the old
# source-regex pin printing `ALL PASS: docs surface matches code` / exit 0.
# The actual-dispatch comparison below must instead fail 7a naming `probe`.
def _mutated_door(extra_key: str):
    tmp = Path(tempfile.mkdtemp(prefix=".tmp-door-mut-"))
    for f in ("__init__.py", "wfcommon.py", "card_enforcement.py", "card_ledger.py", "telemetry_observers.py", "gate_index.py"):
        (tmp / f).write_bytes((root / f).read_bytes())
    p = tmp / "__init__.py"
    src = p.read_text(encoding="utf-8")
    m = re.search(r"\bACTIONS\s*=\s*\{\s*", src)
    assert m, "ACTIONS literal shape changed — update _mutated_door WITH the code"
    src = src[:m.end()] + f'"{extra_key}": lambda a: True,\n           ' + src[m.end():]
    p.write_text(src, encoding="utf-8")
    return load_door(tmp)


def _untagged_row(row: str) -> str:
    return TAG_RE.sub("(nobody remembers)", row)


with tempfile.TemporaryDirectory(prefix=".tmp-table-mut-") as td:
    # Synthetic baseline: one clean untagged row per dispatched action — the
    # self-proofs must be pure logic, independent of whatever state the live
    # table happens to be in (that state is checks 4-6's job).
    synth = {a: f"| `{a}` | does the thing |" for a in sorted(code_actions)}
    # 7a: an action that exists ONLY in actual dispatch (invisible to a source regex)
    mut = compare(set(_mutated_door("probe_probe")) | code_actions, synth)
    check("7a actual-dispatch addition with no row goes RED naming it",
          mut["missing"] == ["probe_probe"], mut)
    # 7b: a dropped row goes RED naming it
    victim = sorted(code_actions)[0]
    dropped = dict(synth)
    dropped.pop(victim)
    check("7b dropped table row goes RED naming it",
          compare(code_actions, dropped)["missing"] == [victim], victim)
    # 7c: a shipped row tagged as unshipped goes RED naming it (inverse bite)
    shipped_row = sorted(code_actions)[0]
    tagged = dict(synth)
    tagged[shipped_row] = synth[shipped_row] + " (open PR #0)"
    check("7c shipped row tagged (open PR) goes RED naming it",
          compare(code_actions, tagged)["shipped_tagged"] == [shipped_row], shipped_row)
    # 7d: an untagged doc-only row goes RED naming it
    doc_key = "doc_only_probe"
    good = dict(synth, **{doc_key: synth[sorted(code_actions)[0]] + " (open PR #0)"})
    check("7d tagged doc-only row is the sanctioned shape",
          compare(code_actions, good)["extra_untagged"] == [], good)
    stripped = dict(synth, **{doc_key: _untagged_row(good[doc_key])})
    check("7d untagged doc-only row goes RED naming it",
          compare(code_actions, stripped)["extra_untagged"] == [doc_key], doc_key)
    # 7e: the wf165c/B3 attack, at the REAL parser — replay the README with a
    # TAGGED copy of one shipped action's row injected immediately BEFORE the
    # real (untagged) row: the exact pair the old last-write-wins read as a
    # clean untagged shipped row, blinding checks 4-6. table_rows now counts
    # occurrences, so the duplicate itself is named RED whatever the tags say.
    dupe = sorted(code_actions)[0]
    lines = readme.splitlines()
    for i, ln in enumerate(lines):
        if re.match(r"^\|\s*`" + re.escape(dupe) + r"`\s*\|", ln.strip()):
            injected = lines[:i] + [ln.strip() + " (open PR #0)", ln.strip()] + lines[i + 1 :]
            break
    else:
        injected = None
    reparsed = table_rows("\n".join(injected)) if injected is not None else {}
    check("7e duplicate-row pair (tagged then untagged) REDs the real parse",
          injected is not None and reparsed.get("__duplicates__") == [dupe],
          f"replay-injected duplicate row for {dupe!r} not named by table_rows")

print(f"TOTAL {count_pass} PASS {count_fail} FAIL")
sys.exit(0 if ok else 1)
