#!/usr/bin/env python3
"""est-ip975 (WF-GRAFT-3, PR-1): the tick bridge is a name-keyed SINGLETON
no_agent job lifecycle — arm idempotent, teardown by deletion, dormant module.

The law pinned here:
  * ensure_tick() creates exactly one job named `hermes-workflows-tick` with
    no_agent=True, and its script is a generated shim under HERMES_HOME/scripts
    (core runs cron scripts ONLY from that dir — the traversal guard in
    cron/scheduler_script.py), and the shim execs this plugin's wf.py carrying
    the `tick` verb;
  * a second ensure_tick() returns the SAME id, the store still holds ONE job,
    and create_job is never called a second time (the singleton is keyed BY
    NAME, so double-arm is idempotence, never duplication);
  * teardown_tick() DELETES the job (never disable) and returns True; teardown
    with no job present returns False — the store never accumulates tick rows;
  * the cadence resolves env (WF_TICK_SCHEDULE) > config (workflows.
    tick_schedule) > DEFAULT_TICK_SCHEDULE, at call time, never baked;
  * a host without core cron (_jobs() None) degrades honestly: ensure_tick
    returns None and teardown_tick returns False, neither ever raises.

Hermetic per the estate cron-store-pin law: the core module seam is `_jobs()`
and every case injects a fake jobs module; HERMES_HOME is repointed to a tmp
dir — the live cron store is never touched, and this test never arms a real
job. The module under test is DORMANT: no production caller exists until the
arm-on-launch graft (PR-3); the test IS the behavior.

Plain script, no pytest: exits non-zero on the first red.
"""
import importlib.util
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

FAILS = []


def check(name, cond, detail=None):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else "  ::  " + str(detail)[:600]))
    if not cond:
        FAILS.append(name)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeJobs:
    """Stand-in for core cron.jobs — records every call, holds an in-memory
    store. Same call surface the bridge uses: list_jobs/create_job/remove_job."""

    def __init__(self):
        self.jobs = []
        self.create_calls = []
        self.remove_calls = []
        self.list_kwargs = []

    def list_jobs(self, include_disabled=False):
        self.list_kwargs.append({"include_disabled": include_disabled})
        return [dict(j) for j in self.jobs]

    def create_job(self, **kw):
        self.create_calls.append(kw)
        job = {"id": "job-%02d" % (len(self.jobs) + 1)}
        job.update(kw)
        self.jobs.append(job)
        return dict(job)

    def remove_job(self, ref):
        self.remove_calls.append(ref)
        before = len(self.jobs)
        self.jobs = [j for j in self.jobs if j["id"] != ref]
        return len(self.jobs) < before


def main():
    cb = load("cron_bridge_under_test", ROOT / "cron_bridge.py")

    with tempfile.TemporaryDirectory(prefix="tick-bridge-") as td:
        home = Path(td)
        os.environ["HERMES_HOME"] = str(home)
        os.environ.pop("WF_TICK_SCHEDULE", None)
        scripts = home / "scripts"

        # A. first arm: exactly one job, no_agent=True, named singleton,
        #    shim under the tmp scripts dir, carrying wf.py + the tick verb.
        fake = FakeJobs()
        cb._jobs = lambda: fake
        job_id = cb.ensure_tick()
        check("A1: ensure_tick returns a job id", bool(job_id), job_id)
        check("A2: exactly one job created", len(fake.jobs) == 1, fake.jobs)
        job = fake.jobs[0] if fake.jobs else {}
        check("A3: no_agent=True", job.get("no_agent") is True, job)
        check("A4: prompt=None", job.get("prompt") is None, job)
        check("A5: name is the singleton key",
              job.get("name") == cb.TICK_NAME == "hermes-workflows-tick", job)
        shim = Path(str(job.get("script")))
        check("A6: script is the generated shim under HERMES_HOME/scripts",
              shim.parent == scripts and shim.exists(), str(shim))
        text = shim.read_text() if shim.exists() else ""
        check("A7: shim execs the plugin's absolute wf.py", str(ROOT / "wf.py") in text, text[:300])
        check("A8: shim carries the tick verb", "tick" in text, text[:300])
        check("A9: shim is executable", shim.exists() and bool(shim.stat().st_mode & 0o111),
              oct(shim.stat().st_mode) if shim.exists() else "missing")
        check("A10: list_jobs asked include_disabled",
              any(k.get("include_disabled") for k in fake.list_kwargs), fake.list_kwargs)

        # B. double-arm idempotence: same id, still one job, zero extra creates.
        again = cb.ensure_tick()
        check("B1: second ensure_tick returns the SAME id", again == job_id, (again, job_id))
        check("B2: store still holds exactly one job", len(fake.jobs) == 1, fake.jobs)
        check("B3: create_job called exactly once overall", len(fake.create_calls) == 1,
              fake.create_calls)

        # C. teardown deletes (never disables) and returns True.
        down = cb.teardown_tick()
        check("C1: teardown returns True", down is True, down)
        check("C2: store empty after teardown", fake.jobs == [], fake.jobs)
        check("C3: removal went through remove_job", fake.remove_calls == [job_id],
              fake.remove_calls)

        # D. teardown with no job: False — deletion, never accumulation.
        check("D1: teardown with nothing to remove returns False",
              cb.teardown_tick() is False, cb.teardown_tick())

        # E. re-arm after teardown is a fresh create (name-keyed, store-truth).
        again2 = cb.ensure_tick()
        check("E1: re-arm after teardown creates again", len(fake.create_calls) == 2
              and again2 and again2 != job_id, (again2, fake.create_calls))
        cb.teardown_tick()

        # F. schedule resolution: env > config > default, at call time.
        check("F1: default cadence", cb.tick_schedule(config_reader=lambda: None)
              == cb.DEFAULT_TICK_SCHEDULE == "every 2m", cb.tick_schedule(config_reader=lambda: None))
        check("F2: config beats default", cb.tick_schedule(config_reader=lambda: "every 5m")
              == "every 5m", cb.tick_schedule(config_reader=lambda: "every 5m"))
        os.environ["WF_TICK_SCHEDULE"] = "every 7m"
        try:
            check("F3: env beats config", cb.tick_schedule(config_reader=lambda: "every 5m")
                  == "every 7m", cb.tick_schedule(config_reader=lambda: "every 5m"))
            created = cb.ensure_tick()
            sched = fake.jobs[0].get("schedule") if fake.jobs else None
            check("F4: the created job carries the env cadence",
                  created and sched == "every 7m", sched)
            cb.teardown_tick()
        finally:
            os.environ.pop("WF_TICK_SCHEDULE", None)

        # G. a disabled tick still counts as the singleton (never a second job).
        disabled = {"id": "job-dis", "name": cb.TICK_NAME, "disabled": True}
        fake.jobs = [disabled]
        check("G1: find_tick sees a disabled job", cb.find_tick() is not None
              and cb.find_tick()["id"] == "job-dis", cb.find_tick())
        creates_before = len(fake.create_calls)
        check("G2: ensure_tick on a disabled incumbent returns its id, no create",
              cb.ensure_tick() == "job-dis" and len(fake.create_calls) == creates_before,
              (cb.find_tick(), fake.create_calls))

        # H. no core cron: honest degradation, never a raise.
        cb._jobs = lambda: None
        try:
            check("H1: ensure_tick without core cron returns None", cb.ensure_tick() is None,
                  "raised or returned")
            check("H2: teardown without core cron returns False",
                  cb.teardown_tick() is False, "raised or returned")
            check("H3: find_tick without core cron returns None", cb.find_tick() is None,
                  "raised or returned")
        except Exception as exc:  # the honest-degradation law forbids any raise
            check("H0: cron-unavailable path never raises", False, repr(exc))

    print(("ALL PASS" if not FAILS else f"FAILED: {FAILS}"))
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
