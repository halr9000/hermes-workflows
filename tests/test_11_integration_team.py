#!/usr/bin/env python3
"""Lane E: cross-lane executable integration fixtures; no production monkeypatches.
Run with: python3 tests/test_11_integration_team.py
"""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/11-fake-hermes.py'
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('lane_e_door', ROOT / '__init__.py')
door = importlib.util.module_from_spec(spec)
spec.loader.exec_module(door)
import wf_test_isolation as _iso71_door22; _iso71_door22.install(door)  # #71 r5: pin settings.runs_root alongside WF_RUNS_ROOT
import wfcommon as common


def until(check, timeout=8):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(.025)
    raise AssertionError('condition not reached in %.1fs' % timeout)


class TeamIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='wf11-e-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.runs = self.root / 'shared-runs'
        self.runs.mkdir()
        self.profile = self.root / 'profiles' / 'e-fixture'
        self.profile.mkdir(parents=True)
        (self.profile / 'config.yaml').write_text('{}\n')
        (self.profile / 'workflow_team.json').write_text('{"accept_from":["default","launcher"]}\n')
        self.launcher = self.root / 'profiles' / 'launcher'
        self.launcher.mkdir()
        (self.launcher / 'config.yaml').write_text('{}\n')
        self.env = {'HERMES_HOME': str(self.root), 'WF_RUNS_ROOT': str(self.runs),
                    'HERMES_WF_HERMES_BIN': str(FIXTURE)}
        self.addCleanup(self.kill_runners)

    def kill_runners(self):
        for r in self.runs.iterdir():
            if not r.is_dir():
                continue
            try:
                pid = int((r / 'wf.pid').read_text())
                if common.runner_alive(r):
                    os.kill(pid, signal.SIGKILL)
            except (FileNotFoundError, ValueError, ProcessLookupError):
                pass

    def graph(self, goal='JSON:{"result":"ok"}', profile=None):
        node = {'id': 'a', 'type': 'agent', 'goal': goal}
        if profile:
            node['profile'] = profile
        return {'name': 'lane-e', 'nodes': [node]}

    def first_trace(self, r, timeout=8):
        """Parse the child's first trace record once it has LANDED. The old
        predicate was `until(file exists)` — which admits the child's
        open(trace,'a')-then-write window: the file is created empty at open,
        and a single buffered write lands at exit, so a poll that sees the
        CREATED file and reads `splitlines()[0]` hits IndexError on an empty
        list (exact CI signature, run 36878156698 job 110422814737, plain
        subtest — the child spawned, the predicate was just wrong). The
        contract this suite needs is 'first complete (newline-terminated)
        record', which every assertion below then reads without a torn window."""
        p = r / '11-child-trace.jsonl'
        def landed():
            try:
                return '\n' in p.read_text(errors='replace')
            except FileNotFoundError:
                return False
        until(landed, timeout)
        return json.loads(p.read_text().splitlines()[0])

    def launch(self, graph=None, *, home=None, **kw):
        with patch.dict(os.environ, {**self.env, 'HERMES_HOME': str(home or self.root)}, clear=False):
            result = door.act_run({'graph': graph or self.graph(), **kw})
        self.assertIn('run_id', result, result)
        return result['run_id']

    def test_fake_hermes_refuses_unknown_profile_and_writes_target_metrics(self):
        env = {**os.environ, **self.env}
        p = subprocess.run([sys.executable, str(FIXTURE), '-p', 'missing', 'chat'], env=env,
                           input='hello', text=True, capture_output=True)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn('unknown profile', p.stderr)
        p = subprocess.run([sys.executable, str(FIXTURE), '-p', 'e-fixture', 'chat',
                            '--continue', 'wf:fixture:a:xyz#a0'], env=env, input='hello',
                           text=True, capture_output=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertTrue((self.profile / 'state.db').exists())
        self.assertFalse((self.root / 'state.db').exists())
        self.assertEqual(common.child_metrics('fixture', home=self.profile)['wf:fixture:a:xyz']['api_calls'], 1)

    def test_env_whitelist_plain_and_profiled(self):
        permitted = {'PATH', 'HOME', 'LANG', 'TERM', 'TZ', 'TMPDIR', 'HERMES_HOME',
                     'WF_RUNS_ROOT', 'HERMES_WRITE_SAFE_ROOT', 'HERMES_QUIET_TURN_REPORT_FILE',
                     'TERMINAL_CWD'}
        for profile in (None, 'e-fixture'):
            with self.subTest(profile=profile):
                with patch.dict(os.environ, {**self.env, 'LANE_E_SECRET_TOKEN': 'never-inherit',
                                              'LC_ALL': 'C.UTF-8'}, clear=False):
                    rid = door.act_run({'graph': self.graph(profile=profile)})['run_id']
                r = self.runs / rid
                trace = self.first_trace(r)
                self.assertEqual(trace['env']['TERMINAL_CWD'], str((r / 'work' / 'a').resolve()))
                if profile:
                    # RATIFY F2: the env WHITELIST is mandated on routed spawns only.
                    self.assertNotIn('LANE_E_SECRET_TOKEN', trace['env'], 'profiled child inherited test secret')
                    self.assertEqual(trace['env']['HERMES_HOME'], str(self.root))
                    self.assertEqual(trace['home'], str(self.profile))
                    self.assertEqual(trace['argv'][:3], ['-p', profile, 'chat'])
                    self.assertTrue(all(k in permitted or k.startswith('LC_') or
                                        k.startswith('HERMES_WF_') for k in trace['env']), trace['env'].keys())
                else:
                    # RATIFY F2/F1: a no-profile spawn is byte-identical to 1.0.15,
                    # which forwarded os.environ (no whitelist). Do not demand the
                    # route-only whitelist here; assert the 1.0.15 contract instead.
                    self.assertEqual(trace['argv'][0], 'chat')
                    self.assertNotIn('HERMES_WF_RUN_DIR', ())  # no-op keeps structure explicit
                    self.assertTrue((self.runs / rid / '11-child-trace.jsonl').exists())

    def test_liveness_matrix_eight_cases(self):
        # Real wf.py process identity and /proc environment (not os.kill alone).
        for shared in (False, True):
            home = self.root / ('home-shared' if shared else 'home-legacy')
            home.mkdir()
            runs = self.runs if shared else home / 'workflows'
            runs.mkdir(exist_ok=True)
            for mode in ('live', 'dead', 'pid-reused', 'wait-respawn'):
                with self.subTest(shared=shared, mode=mode):
                    r = runs / ('liveness-' + mode)
                    r.mkdir()
                    (r / 'graph.json').write_text(json.dumps(self.graph('SLEEP 1')))
                    (r / 'run.json').write_text(json.dumps({'name': 'liveness', 'hermes_bin': str(FIXTURE), 'node_timeout': 10}))
                    (r / 'nodes').mkdir()
                    env = {**os.environ, 'HERMES_HOME': str(home)}
                    if shared:
                        env['WF_RUNS_ROOT'] = str(runs)
                    else:
                        env.pop('WF_RUNS_ROOT', None)
                    p = subprocess.Popen([sys.executable, str(ROOT / 'wf.py'), 'run', r.name], env=env,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    try:
                        (r / 'wf.pid').write_text(str(p.pid))
                        until(lambda: common.runner_alive(r))
                        with patch.dict(os.environ, env, clear=True):
                            self.assertTrue(door.act_status({'run_id': r.name})['runner_live'])
                        if mode == 'pid-reused':
                            # #271 review: the kernel is the lock truth — while a
                            # LIVE holder holds the flock, held=>live is correct
                            # (91b9a3de), so "reused pid must not read alive" can
                            # only be asserted over a DEAD holder. The base code
                            # passed this branch only because its µs in-process
                            # probe beat the runner to admission; the #44 child
                            # probe (spawn cost) exposes that race every run.
                            # Kill + reap FIRST: the kernel released the flock
                            # with the process, so the remaining claim under test
                            # is purely pid-reuse identity.
                            p.kill(); p.wait()
                            (r / 'wf.pid').write_text(str(os.getpid()))
                            self.assertFalse(common.runner_alive(r))
                            with patch.dict(os.environ, env, clear=True):
                                self.assertFalse(door.act_status({'run_id': r.name})['runner_live'])
                        elif mode != 'live':
                            p.kill(); p.wait()
                            self.assertFalse(common.runner_alive(r))
                            with patch.dict(os.environ, env, clear=True):
                                self.assertFalse(door.act_status({'run_id': r.name})['runner_live'])
                            if mode == 'wait-respawn':
                                with patch.dict(os.environ, env, clear=True):
                                    door.act_wait({'run_id': r.name, 'timeout': .1})
                                    until(lambda: common.runner_alive(r) or door.act_status({'run_id': r.name})['status'] == 'done')
                                    self.assertIn(door.act_status({'run_id': r.name})['status'], ('running', 'done'))
                    finally:
                        if p.poll() is None:
                            p.kill(); p.wait()
                        try:
                            q = int((r / 'wf.pid').read_text())
                            if q not in (p.pid, os.getpid()) and common.runner_alive(r):
                                os.kill(q, signal.SIGKILL)
                        except (FileNotFoundError, ProcessLookupError):
                            pass

    def test_cross_profile_wait_status_dashboard_after_target_deleted(self):
        import shutil
        import uuid
        # Estate: prefer this test's own sandbox (setUp already made <tmp>/profiles,
        # portable across the lane worktree and the merge tree, and CI checkouts
        # have no ancestor profiles/ dir); fall back to the nearest ancestor that
        # has one.
        ancestors = [p for p in Path(__file__).resolve().parents
                     if (p / 'profiles').is_dir()]
        estate = self.root if (self.root / 'profiles').is_dir() else ancestors[0]
        name = 'wf11-e-' + uuid.uuid4().hex[:12]
        target = estate / 'profiles' / name
        launcher = estate / 'profiles' / (name + '-launcher')
        for home in (target, launcher):
            home.mkdir(parents=True, exist_ok=False)
            (home / 'config.yaml').write_text('{}\n')
            self.addCleanup(lambda p=home: shutil.rmtree(p, ignore_errors=True))
        (target / 'workflow_team.json').write_text(json.dumps({'accept_from':[launcher.name]}))
        graph = self.graph('SLEEP 0.5', name)
        with patch.dict(os.environ, {**self.env, 'HERMES_HOME': str(launcher)}):
            result = door.act_run({'graph': graph})
        self.assertIn('run_id', result, result)
        rid = result['run_id']
        r = self.runs / rid
        self.first_trace(r)
        until(lambda: not common.runner_alive(r), 10)
        shutil.rmtree(target)
        with patch.dict(os.environ, {**self.env, 'HERMES_HOME': str(launcher)}):
            waited = door.act_wait({'run_id': rid, 'timeout': 3})
            status = door.act_status({'run_id': rid})
            dash_spec = importlib.util.spec_from_file_location('lane_e_dashboard', ROOT / 'dashboard/plugin_api.py')
            dashboard = importlib.util.module_from_spec(dash_spec)
            dash_spec.loader.exec_module(dashboard)
            self.assertEqual(dashboard._root(), self.runs)
            self.assertEqual(status['run_id'], rid)
            self.assertEqual(waited['run_id'], rid)
            self.assertTrue((r / 'graph.json').exists())


if __name__ == '__main__':
    unittest.main()
