"""WF-GRAFT-6 — the autouse test sandbox: ONE binding so no test path reaches
operator state. Born from the class of incident a peer estate documented at their
0.7.6: running the suite armed a job in the operator's REAL store and shadow-broke
the live tick. Our self-recorded hazard of the same class is the comment at
tests/test_admission_ledger_85.py:37 — "WF_RUNS_ROOT alone is not a sandbox — pin
the resolver too, or saves ride the owner's settings.runs_root into the estate".

What this file does:

  1. ENV FLOOR (at import, before any test file or child process runs) — a fresh
     tmp sandbox becomes HERMES_HOME / WF_RUNS_ROOT / WF_QUOTA_CACHE, and the
     launch environment's session identity (HERMES_SESSION_* / HERMES_UI_SESSION_*)
     plus stray HERMES_WF_* seams are stripped exactly like scripts/suite.py
     strips them (frozen-golden law: a test process must never wear the
     launching agent's identity). Whatever the operator's shell exported is
     shadowed, never honoured: an exported WF_RUNS_ROOT pointing at the live
     estate is precisely the incident. The launch home is preserved for auditing
     as WF_TEST_LIVE_HOME; the sandbox path is exported as WF_TEST_SANDBOX_ROOT
     (the proof test reads it).
  2. RESOLVER WRAP — the #42 precedence makes `settings.runs_root` outrank the
     env pin, so the env floor alone leaves the owner-settings class open. This
     file wraps `wfcommon.set_owner_setting_reader` at the PUBLISH seam using
     tests/wf_test_isolation.py's own wrapper: every reader any door publishes
     (now or later, in this process) answers `runs_root` from the live env pin,
     and the existing `wf_test_isolation.install(door)` call sites stay exact
     no-ops (same _iso71_pinned / _iso71_inner protocol).
  3. AUTOUSE FIXTURES — session scope owns teardown (sandbox removal, launch env
     restore); function scope re-asserts the floor around every item and restores
     the parent environment after it, so an in-process consumer never resolves to
     a leftover.
  4. EVERY TEST MODULE IS A MERGE-GATE SUBPROCESS ITEM. The repo's verdict unit
     is the FILE (PASS/FAIL lines + exit code is the contract the serial merge
     gate enforces); the files were authored for fresh-process execution, they
     pin env at import and share sys.modules, so in-process collection at
     whole-suite scale reorders state they assume. Instead each test_*.py runs
     exactly like scripts/suite.py runs it: fresh interpreter, cwd=repo root,
     the canonical floor re-applied to the child env, 90 s wall, exit 0 green —
     and none of them can take the pytest mainloop down with a top-level
     sys.exit (importing them is bypassed, which is also what makes
     `pytest tests/` finish at all today: plain pytest dies as an INTERNALERROR
     SystemExit on the first script-style module). Opt-in native granularity
     (import + collect the pytest-shaped files in-process, floor and wrap
     active) is available via WF_PYTEST_NATIVE=1 for single-file iteration;
     file-subprocess stays the default contract.

Proof that the floor holds: tests/test_sandbox_conftest_z2heo.py (red on a tree
without this file, green with it, green under the merge gate's own pins).
"""
import ast
import atexit
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for _p in (str(ROOT), str(HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import wfcommon  # noqa: E402
import wf_test_isolation as _iso71  # noqa: E402

# ---------- 1. env floor (launch state captured before anything is touched) ----------

_ORIG_ENV = dict(os.environ)
_LIVE_HOME = str(Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes")))

# suite.py parity: keep only the fake-binary seam, strip session identity and the
# rest of the HERMES_WF_* run seams (each test that wants one sets it itself).
_KEEP_WF_ENV = {"HERMES_WF_HERMES_BIN"}
for _k in [k for k in os.environ
           if k.startswith("HERMES_SESSION_") or k.startswith("HERMES_UI_SESSION_")
           or (k.startswith("HERMES_WF_") and k not in _KEEP_WF_ENV)]:
    del os.environ[_k]

_SANDBOX = Path(tempfile.mkdtemp(prefix="wf-conftest-sandbox-"))
atexit.register(shutil.rmtree, _SANDBOX, ignore_errors=True)

os.environ["HERMES_HOME"] = str(_SANDBOX)
os.environ["WF_RUNS_ROOT"] = str(_SANDBOX / "workflows")
os.environ["WF_QUOTA_CACHE"] = str(_SANDBOX / "cache" / "workflow-quota-cache.json")
os.environ["WF_TEST_SANDBOX_ROOT"] = str(_SANDBOX)
os.environ["WF_TEST_LIVE_HOME"] = _LIVE_HOME

# Canonical child binding: items re-apply exactly these five keys so a parent-env
# mutation (any kind, from any source) never leaks into a child — merge-gate
# launcher parity, and the reason no cross-file pin can ever poison a run.
_STATE_KEYS = ("HERMES_HOME", "WF_RUNS_ROOT", "WF_QUOTA_CACHE")
_CANON = {k: os.environ[k] for k in _STATE_KEYS}
_CANON.update(WF_TEST_SANDBOX_ROOT=str(_SANDBOX), WF_TEST_LIVE_HOME=_LIVE_HOME)

# ---------- 2. resolver wrap (settings.runs_root can no longer outrank the floor) ----------

_orig_publish = wfcommon.set_owner_setting_reader


def _pinned_publish(fn):
    if fn is not None and not getattr(fn, "_iso71_pinned", False):
        fn = _iso71._wrap_resolver(fn)
    return _orig_publish(fn)


_pinned_publish._z2heo_seam_wrapped = True  # idempotence marker shared with the proof test
wfcommon.set_owner_setting_reader = _pinned_publish
if getattr(wfcommon, "_OWNER_SETTING_READER", None) is not None:
    _pinned_publish(wfcommon._OWNER_SETTING_READER)

# ---------- 3. autouse fixtures ----------


def _restore_env(snap):
    """Diff-restore: re-apply changed keys, drop keys the test added — never
    PYTEST_* (pytest owns that bookkeeping mid-run; clearing it races its own
    pop and dies KeyError at teardown)."""
    for k in [k for k in os.environ if k not in snap and not k.startswith("PYTEST_")]:
        del os.environ[k]
    for k, v in snap.items():
        if k.startswith("PYTEST_"):
            continue
        if os.environ.get(k) != v:
            os.environ[k] = v


@pytest.fixture(scope="session", autouse=True)
def _wf_sandbox_session():
    """Session ownership only: teardown restores the launch environment (the sandbox
    directory itself is removed by the atexit hook, which also covers --collect-only
    and crash paths that skip fixture teardown)."""
    yield
    _restore_env(_ORIG_ENV)


@pytest.fixture(autouse=True)
def _wf_env_guard():
    """Function scope: re-assert the floor before the item (anything an in-process
    consumer reads resolves to the sandbox), restore the parent environment after."""
    snap = dict(os.environ)
    for k in _STATE_KEYS:
        os.environ[k] = _CANON[k]
    yield
    _restore_env(snap)


# ---------- 4. merge-gate subprocess items ----------

SCRIPT_WALL_S = 90  # scripts/suite.py rc=124 parity
NATIVE_MODE = (os.environ.get("WF_PYTEST_NATIVE", "") or "").strip() not in ("", "0")


class ScriptItem(pytest.Item):
    """One merge-gate script as one item: fresh subprocess, canonical floor, 90 s
    wall; exit 0 green, anything else red with the script's own tail attached."""

    def __init__(self, *args, script_path, **kw):
        super().__init__(*args, **kw)
        self.script_path = Path(script_path)

    def runtest(self):
        env = dict(os.environ)
        env.update(_CANON)
        try:
            proc = subprocess.run([sys.executable, str(self.script_path)],
                                  cwd=str(ROOT), env=env,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, timeout=SCRIPT_WALL_S)
        except subprocess.TimeoutExpired as exc:
            out = (exc.stdout or b"").decode(errors="replace") if isinstance(exc.stdout, bytes) \
                else (exc.stdout or "")
            pytest.fail(f"wall {SCRIPT_WALL_S}s exceeded (merge-gate rc=124 parity)\n"
                        + "\n".join(out.splitlines()[-40:]))
        if proc.returncode != 0:
            pytest.fail(f"{self.script_path.name}: exit {proc.returncode} (merge-gate red)\n"
                        + "\n".join((proc.stdout or "").splitlines()[-40:]))

    def reportinfo(self):
        return self.script_path, None, self.name


class ScriptModule(pytest.Module):
    """Collector for a merge-gate script: NEVER imports it (its top-level
    sys.exit would take the mainloop down; its import-time pins would poison
    other files); yields one subprocess item running its script contract."""

    def collect(self):
        return [ScriptItem.from_parent(self, name=self.name,
                                       script_path=str(self.path))]


def _is_pytest_shaped(src):
    """Native = has `def test_*` AND no import-time exit path (a `sys.exit` /
    `SystemExit` reachable at module scope outside the `__main__` guard takes
    the whole pytest mainloop down). Only consulted under WF_PYTEST_NATIVE=1."""
    try:
        tree = ast.parse(src)
    except (SyntaxError, ValueError):
        return True  # let the default collector surface the import error honestly
    has_testdef = any(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                      and n.name.startswith("test_") for n in ast.walk(tree))
    for stmt in tree.body:
        if (isinstance(stmt, ast.If)
                and "__main__" in ast.dump(getattr(stmt, "test", None) or ast.Constant(None))):
            continue
        for x in ast.walk(stmt):
            if (isinstance(x, ast.Attribute) and x.attr == "exit"
                    or isinstance(x, ast.Name) and x.id == "SystemExit"):
                return False
    return has_testdef


@pytest.hookimpl(tryfirst=True)
def pytest_pycollect_makemodule(module_path, parent):
    """The firstresult seam for .py modules (pytest_collect_file is NOT firstresult
    in this pytest — the built-in Module would be built alongside anything returned
    from there). Default: every module becomes a ScriptModule (never imported,
    merge-gate subprocess verdict). Under WF_PYTEST_NATIVE=1, pytest-shaped files
    fall through to the default in-process collector (floor and wrap active)."""
    try:
        src = module_path.read_text(encoding="utf-8")
    except OSError:
        return None
    if NATIVE_MODE and _is_pytest_shaped(src):
        return None
    return ScriptModule.from_parent(parent, path=module_path)
