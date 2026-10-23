"""Tick bridge: the native no_agent tick job lifecycle (est-ip975, PR-1).

WF-GRAFT-3 grafts the scion cron bridge's tick singleton onto this plugin in
three serial PRs. This module is the lifecycle HALF and is DORMANT at runtime:
no production caller exists until the arm-on-launch graft (PR-3), so importing
it does nothing. The test IS the behavior (tests/test_tick_bridge.py).

The lifecycle law (pinned by that test):
  * the tick job is a SINGLETON keyed BY NAME — every launch may arm it, a
    double-arm returns the incumbent id, and the store never holds two;
  * teardown DELETES the job, never disables it, so tick jobs never accumulate
    and an idle host holds zero rows and takes zero wakes;
  * the job is ``no_agent=True``: its stdout IS the job output, no agent wakes;
  * core runs cron scripts ONLY from HERMES_HOME/scripts (the traversal guard
    in core's cron/scheduler_script.py), so the job script is a generated shim
    written under that dir, execing this plugin's absolute wf.py carrying the
    ``tick`` verb (the tick verb itself lands in PR-2 — the shim verb is the
    contract the arm writes toward).

Host imports are lazy and guarded (AGENTS.md 4c rule 7): ``_jobs()`` returns
core's cron.jobs module or None on a bare system-Python host, and every entry
point degrades honestly (return None/False) instead of raising — a launch must
never fail because of the tick (the PR-3 arm call site wraps this in
best-effort anyway; the module stands on its own too).
"""
import os
import shlex
import sys
from pathlib import Path

TICK_NAME = "hermes-workflows-tick"
DEFAULT_TICK_SCHEDULE = "every 2m"
_ROOT = Path(__file__).resolve().parent


def _jobs():
    """Core's cron.jobs module, or None when absent (THE test seam: a lane
    test injects a fake jobs module at this attribute)."""
    try:
        import importlib
        return importlib.import_module("cron.jobs")
    except Exception:
        return None


def _home():
    """Home resolution mirrors wfcommon.hermes_home(): core's context-aware
    getter when importable, else the raw HERMES_HOME env, else ~/.hermes."""
    try:
        from hermes_constants import get_hermes_home
        return Path(get_hermes_home())
    except Exception:
        return Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))


def _config_tick_schedule():
    """Config value ``workflows.tick_schedule``, resolved core-first then by a
    lite top-level scan of <home>/config.yaml for bare hosts without hermes_cli
    (the same precedence shape wfcommon.seat_max_seats uses for max_seats).
    Returns None when the config layer is absent or names nothing."""
    try:
        from hermes_cli.config import load_config_readonly
        cfg = load_config_readonly()
        if cfg:
            w = cfg.get("workflows")
            if isinstance(w, dict) and w.get("tick_schedule"):
                return str(w["tick_schedule"])
    except Exception:
        pass
    try:
        lines = (_home() / "config.yaml").read_text().splitlines()
    except OSError:
        return None
    in_workflows = False
    for line in lines:
        line = line.split(" #", 1)[0]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line[0].isspace():
            in_workflows = stripped.split(":", 1)[0] == "workflows"
            continue
        if in_workflows and stripped.startswith("tick_schedule:"):
            val = stripped.split(":", 1)[1].strip().strip("'\"")
            return val or None
    return None


def tick_schedule(env=None, config_reader=None):
    """Effective tick cadence, resolved at every call (config is re-read,
    never baked): WF_TICK_SCHEDULE env > config workflows.tick_schedule >
    DEFAULT_TICK_SCHEDULE (the same resolution shape as wfcommon.seat_max_seats
    — knob parameters are the test seam, mirroring its dotenv_reader)."""
    e = (env if env is not None else os.environ.get("WF_TICK_SCHEDULE") or "").strip()
    if e:
        return e
    reader = config_reader or _config_tick_schedule
    try:
        raw = reader()
    except Exception:
        raw = None
    return str(raw) if raw else DEFAULT_TICK_SCHEDULE


def write_shim(name, *args):
    """Write an executable bash shim under <resolved HERMES_HOME>/scripts that
    execs this plugin's absolute wf.py with `*args`. Core validates cron script
    files stay under HERMES_HOME/scripts (cron/scheduler_script.py guard) and
    invokes them with no arguments, so a per-purpose shim is how the tick
    carries its verb. Returns the shim Path."""
    scripts = _home() / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    python = sys.executable or "python3"
    parts = [python, str(_ROOT / "wf.py")] + [str(a) for a in args]
    path = scripts / ("%s.sh" % name)
    path.write_text("#!/usr/bin/env bash\nexec %s\n" % " ".join(shlex.quote(p) for p in parts))
    path.chmod(0o755)
    return path


def find_tick():
    """The incumbent tick job record (a disabled tick still counts — it is the
    same singleton), or None when absent or cron is unavailable."""
    jobs = _jobs()
    if jobs is None:
        return None
    for job in jobs.list_jobs(include_disabled=True):
        if job.get("name") == TICK_NAME:
            return job
    return None


def ensure_tick():
    """Ensure the singleton tick job exists; returns its id, or None when core
    cron is unavailable. Idempotent by NAME: an incumbent (even a disabled one)
    is returned untouched — no second create call, no second job, ever."""
    existing = find_tick()
    if existing is not None:
        return existing["id"]
    jobs = _jobs()
    if jobs is None:
        return None
    shim = write_shim(TICK_NAME, "tick")
    job = jobs.create_job(prompt=None, schedule=tick_schedule(), name=TICK_NAME,
                          script=str(shim), no_agent=True)
    return job["id"]


def teardown_tick():
    """Delete the tick job (deletion, never disable) — True when a job was
    removed, False when none existed. The store never accumulates tick rows."""
    existing = find_tick()
    if existing is None:
        return False
    jobs = _jobs()
    if jobs is None:
        return False
    return bool(jobs.remove_job(existing["id"]))
