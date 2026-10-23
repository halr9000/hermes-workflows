#!/usr/bin/env python3
"""WF-GRAFT-6 proof — the sandbox binds, the resolver cannot leak, writes land
inside, and the operator's real home comes out untouched.

Runnable two ways (repo convention): `python3 tests/test_sandbox_conftest_z2heo.py`
prints PASS/FAIL lines, exit 0 = green; pytest collects the `def test_` trio.

The verdict depends on the runner, by design — three classes, in precedence order:

  A. CONFTEST (the graft): WF_TEST_SANDBOX_ROOT exported by tests/conftest.py —
     canonical floor re-asserted, poison reader repelled, write probed into the
     sandbox, live home mtimes diffed. All three tests green.
  B. RUNNER PIN (merge-gate parity): WF_RUNS_ROOT exported by the launcher
     (scripts/suite.py pins a tmp root + a scratch HERMES_HOME). This file treats
     the pin as the floor and, lacking the conftest's publish-seam wrap, installs
     the same resolver wrap itself (tests/wf_test_isolation.py) before poisoning —
     the merge gate runs this file as a script and must stay green, so the pin
     class self-defends and proves containment against the pin.
  C. BARE (no marker, no pin — a fresh clone without the conftest): the binding
     test FAILS, the poisoned resolver answers the decoy (the leak, demonstrated
     without touching it), and the write test REFUSES — an unprotected process is
     never handed a real store to write into. This is the RED this file exists to
     show; the hazard write is only demonstrated when the operator explicitly
     nominates a decoy store:
        HERMES_HOME=$DECOY WF_TEST_LIVE_DECOY=$DECOY \
            python3 tests/test_sandbox_conftest_z2heo.py   # red, write lands in $DECOY

Red on base / green with the conftest, exact commands in the PR body.
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for _p in (str(ROOT), str(HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

MARKER = os.environ.get("WF_TEST_SANDBOX_ROOT", "")
PIN = os.environ.get("WF_RUNS_ROOT", "")
CLASS = "A" if MARKER else ("B" if PIN else "C")
SANDBOX = Path(MARKER or PIN) if CLASS in "AB" else None
# Under the conftest (A) the floor is <sandbox>/workflows; a launcher pin (B) IS
# the runs root verbatim — the resolver's precedence honours it without suffix.
EXPECTED_RUNS = (SANDBOX / "workflows") if CLASS == "A" else SANDBOX


def _live_homes():
    """Operator-state candidates this process would fall back to unprotected:
    the platform default home plus whatever home the launcher actually exported
    (the conftest preserves the launch home as WF_TEST_LIVE_HOME)."""
    out = [Path.home() / ".hermes"]
    lh = os.environ.get("WF_TEST_LIVE_HOME", "")
    if lh and not MARKER:  # under the conftest this names the same home, pre-rebind
        p = Path(lh)
        if p not in out:
            out.append(p)
    return out


def _snapshot():
    """Existence + mtime + size for each live home, its plugin/run/cache faces,
    and the legacy runs root + quota file under it. Absent is a state too."""
    snap = {}
    for home in _live_homes():
        for p in [home, home / "workflows", home / "workflows" / "library",
                  home / "cache", home / "cache" / "workflow-quota-cache.json",
                  home / "plugins"]:
            try:
                st = p.stat()
                snap[str(p)] = (True, st.st_mtime_ns, st.st_size)
            except OSError:
                snap[str(p)] = (False, None, None)
    return snap


def _rebind_canonical():
    """Class A only: other files may have re-pinned the inherited env at import;
    this proof stands on the floor, so re-assert it (the autouse guard restores
    whatever the snapshot was afterwards, leaving neighbours' pins intact)."""
    os.environ["HERMES_HOME"] = str(SANDBOX)
    os.environ["WF_RUNS_ROOT"] = str(SANDBOX / "workflows")
    os.environ["WF_QUOTA_CACHE"] = str(SANDBOX / "cache" / "workflow-quota-cache.json")


def _ensure_seam_wrap(wfcommon):
    """Classes A/B: guarantee the publish seam repels the owner-settings class.
    Under the conftest this is already live (the wrap is idempotent by the
    _iso71_pinned protocol); class B installs it itself — merge-gate parity."""
    if getattr(wfcommon.set_owner_setting_reader, "_z2heo_seam_wrapped", False):
        return
    import wf_test_isolation as iso
    orig = wfcommon.set_owner_setting_reader

    def seam(fn):
        if fn is not None and not getattr(fn, "_iso71_pinned", False):
            fn = iso._wrap_resolver(fn)
        return orig(fn)

    seam._z2heo_seam_wrapped = True
    wfcommon.set_owner_setting_reader = seam
    if getattr(wfcommon, "_OWNER_SETTING_READER", None) is not None:
        seam(wfcommon._OWNER_SETTING_READER)


def _boot_door():
    import wfcommon
    spec = importlib.util.spec_from_file_location("doorz2heo", ROOT / "__init__.py")
    door = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(door)
    return door, wfcommon


def _poison_runs_root(wfcommon, decoy):
    """Publish a stand-in owner-settings reader whose `runs_root` answer is DECOY.
    Containment here IS the resolver-leak proof: a raw publish without the seam
    wrap is exactly the pre-graft shape (last-writer-wins, settings outranks env)."""
    prev = wfcommon._OWNER_SETTING_READER

    def fake(key):
        return str(decoy) if key == "runs_root" else wfcommon.NO_READER

    wfcommon.set_owner_setting_reader(fake)

    def restore():
        wfcommon.set_owner_setting_reader(prev if prev is not None else (lambda k: wfcommon.NO_READER))
    return restore


GRAPH = {"name": "z2heo-sandbox-proof", "nodes": [
    {"id": "solo", "type": "agent", "goal": "proof of containment"}]}
ENTRY = "z2heo-sandbox-proof"


def test_sandbox_binding():
    """The floor is bound: a marker (class A) names the sandbox and the resolver
    answers it; a launcher pin (class B) is honoured as the floor; bare (class C)
    is RED — an unbound process must not proceed to a store."""
    if CLASS == "C":
        raise AssertionError(
            "no sandbox bound (no WF_TEST_SANDBOX_ROOT, no WF_RUNS_ROOT): the "
            "hazard class is live — writes here ride the operator's home. Run "
            "with tests/conftest.py present, or a launcher pin like the merge gate.")
    _rebind_canonical() if MARKER else None
    import wfcommon
    assert wfcommon.runs_root() == EXPECTED_RUNS, \
        f"resolver answers {wfcommon.runs_root()}, floor says {EXPECTED_RUNS}"


def test_resolver_cannot_leak():
    """Poison the owner-settings reader with a decoy runs_root: the resolver must
    still answer the floor. On bare (class C) the decoy answer escapes — the
    leak, reproduced against a stand-in, never against the operator's store."""
    if CLASS == "C":
        decoy = Path(tempfile.mkdtemp(prefix="wf-z2heo-decoy-"))
        try:
            import wfcommon
            if CLASS == "A":
                _rebind_canonical()  # in-suite, an earlier module's import pin may own the env
            _ensure_seam_wrap(wfcommon)
            restore = _poison_runs_root(wfcommon, decoy)
            try:
                got = wfcommon.runs_root()
            finally:
                restore()
            assert got != decoy, (
                f"unbound process: poisoned settings.runs_root outranks the (absent) "
                f"env pin — resolver answers {got}, the decoy leaked through")
        finally:
            shutil.rmtree(decoy, ignore_errors=True)
        return
    decoy = Path(tempfile.mkdtemp(prefix="wf-z2heo-decoy-"))
    try:
        import wfcommon
        _ensure_seam_wrap(wfcommon)
        restore = _poison_runs_root(wfcommon, decoy)
        try:
            got = wfcommon.runs_root()
        finally:
            restore()
        assert got == EXPECTED_RUNS, (
            f"poisoned settings.runs_root escaped: resolver answers {got}, "
            f"floor says {EXPECTED_RUNS}")
    finally:
        shutil.rmtree(decoy, ignore_errors=True)


def test_write_lands_in_sandbox_live_untouched():
    """A real door save through the booted door: the library entry must exist
    inside the sandbox and the live home's mtimes must not move. Class C refuses
    the write (never hands a bare process the real store) and is RED unless the
    operator nominated a decoy home — then the write goes to the decoy and the
    FAIL detail names exactly where it landed."""
    if CLASS == "C":
        decoy_home = os.environ.get("WF_TEST_LIVE_DECOY", "")
        if decoy_home and os.environ.get("HERMES_HOME", "") == decoy_home:
            door, wfcommon = _boot_door()
            before = _snapshot()
            res = door.handle({"action": "save", "graph": json.loads(json.dumps(GRAPH)),
                               "name": ENTRY})
            landed = wfcommon.runs_root() / "library" / f"{ENTRY}.json"
            assert not landed.exists() or str(landed).startswith(decoy_home)
            raise AssertionError(
                f"unbound process: door save LANDED IN THE STORE at {landed} "
                f"(result: {str(res)[:120]}) — that is the incident class; "
                f"live-home mtimes diffed below: "
                f"{'changed' if before != _snapshot() else 'clean'}")
        raise AssertionError(
            "no sandbox bound — write REFUSED (a bare process is never allowed to "
            "write the operator's store; that refusal is the proof's red face)")
    _rebind_canonical() if MARKER else None
    door, wfcommon = _boot_door()
    _ensure_seam_wrap(wfcommon)
    before = _snapshot()
    res = door.handle({"action": "save", "graph": json.loads(json.dumps(GRAPH)),
                       "name": ENTRY})
    assert not (isinstance(res, dict) and res.get("error")), f"save refused: {res}"
    entry = wfcommon.runs_root() / "library" / f"{ENTRY}.json"
    assert entry.is_file(), f"entry not under the floor: {entry}"
    assert str(entry).startswith(str(SANDBOX)), f"entry escaped the sandbox: {entry}"
    after = _snapshot()
    assert before == after, "LIVE STATE MOVED: " + "; ".join(
        f"{k}: {before[k]} -> {after[k]}" for k in before if before[k] != after[k])
    # every live runs root must show zero trace of the entry name
    for home in _live_homes():
        assert not (home / "workflows" / "library" / f"{ENTRY}.json").exists(), \
            f"live runs root carries the entry: {home}"
    entry.unlink()  # sandbox hygiene; the session teardown sweeps the rest


# ---------- script-mode (merge-gate shape) ----------

fails = 0


def _run(fn):
    global fails
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 — script-mode verdict capture
        fails += 1
        print(f"FAIL {fn.__name__} << {exc}")
    else:
        print(f"PASS {fn.__name__}")


if __name__ == "__main__":
    print(f"class {CLASS} (marker={MARKER or '-'} pin={PIN or '-'})")
    _run(test_sandbox_binding)
    _run(test_resolver_cannot_leak)
    _run(test_write_lands_in_sandbox_live_untouched)
    print(f"\n{'ALL PASS' if not fails else 'FAILURES'} ({fails} failures)")
    sys.exit(1 if fails else 0)
