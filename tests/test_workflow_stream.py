#!/usr/bin/env python3
"""workflow-stream daemon tests against a recording fake Transport — lifecycle,
edge-triggered edits, pin/unpin, WATCH filtering, throttle backoff, restart
recovery, deleted-message repost — plus read-model text rendering over
scratch run dirs via wfcommon.run_state (no network anywhere)."""
import importlib.util
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import wfcommon as common

_spec = importlib.util.spec_from_file_location(
    "workflow_stream", ROOT / "scripts" / "workflow_stream.py")
ws = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ws)

STARTED = "2026-10-25T12:00:00+00:00"


def write(path, obj):
    path.write_text(json.dumps(obj))


def stamp(byid, node):
    return {"efp": common.efp(byid, node), "fp_rule_version": common.FP_RULE_VERSION}


class FakeTransport(ws.Transport):
    """In-memory recording fake. message ids increment; failures scripted."""

    def __init__(self):
        self.calls = []
        self._next = 100
        self.edit_errors = []

    def _id(self):
        self._next += 1
        return self._next

    def send_photo(self, png, caption=""):
        self.calls.append(("send_photo", len(png)))
        return self._id()

    def send_text(self, text, reply_to=None):
        self.calls.append(("send_text", text, reply_to))
        return self._id()

    def edit_text(self, message_id, text):
        if self.edit_errors:
            err = self.edit_errors.pop(0)
            self.calls.append(("edit_text_error", message_id, err))
            raise err
        self.calls.append(("edit_text", message_id, text))

    def pin(self, message_id):
        self.calls.append(("pin", message_id))

    def unpin(self, message_id):
        self.calls.append(("unpin", message_id))


def calls_of(transport, kind):
    return [c for c in transport.calls if c[0] == kind]


def make_run(root, name, nodes, node_states=None, run_meta=None):
    run = Path(root) / name
    (run / "nodes").mkdir(parents=True)
    graph = {"name": name, "nodes": nodes}
    byid = {n["id"]: n for n in nodes}
    write(run / "graph.json", graph)
    meta = {"name": name, "started": STARTED}
    meta.update(run_meta or {})
    write(run / "run.json", meta)
    for nid, rec in (node_states or {}).items():
        write(run / "nodes" / f"{nid}.json", {**stamp(byid, byid[nid]), **rec})
    return run


AGENT_GATE = [
    {"id": "a", "type": "agent", "goal": "do"},
    {"id": "g", "type": "gate", "after": ["a"], "question": "Ship it?",
     "options": ["ship", "hold"]},  # human gate: no `wait` key (that's machine gates)
    {"id": "b", "type": "agent", "after": ["g"], "goal": "finish"},
]

NOW = 1799999999.0


def watcher_with(tmp, transport, **kw):
    kw.setdefault("graphic", True)
    kw.setdefault("watch", "all")
    return ws.Watcher(transport, roots=[tmp], now=lambda: NOW, **kw)


def test_adopt_sends_photo_once_then_text_reply_and_pins_text():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        make_run(tmp, "adopt", AGENT_GATE,
                 node_states={"a": {"status": "done", "output": {"ok": True}}})
        transport = FakeTransport()
        stats = watcher_with(tmp, transport).poll()
        assert stats["adopted"] == 1
        photos, texts, pins = (calls_of(transport, "send_photo"),
                               calls_of(transport, "send_text"), calls_of(transport, "pin"))
        assert len(photos) == len(texts) == len(pins) == 1, transport.calls
        photo_id, text_id = 101, 102  # fake counter: photo adopted first
        assert texts[0][2] == photo_id          # the text replies to the photo
        assert pins[0][1] == text_id            # the TEXT message is pinned, not the photo
        text = texts[0][1]
        assert "🔀 adopt — held · 1/3 · ⏱" in text
        assert "[2] ⏸️ g — gate · held — waiting on you (→ a)" in text
        assert "❓ Ship it?" in text and "⚙️ ship / hold" in text


def test_edits_are_edge_triggered_only_on_state_change():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        run = make_run(tmp, "edge", AGENT_GATE,
                       node_states={"a": {"status": "running", "pid": 0}})
        transport = FakeTransport()
        watcher = watcher_with(tmp, transport)
        watcher.poll()
        watcher.poll()  # identical state: no edit
        assert calls_of(transport, "edit_text") == []
        rec = json.loads((run / "nodes" / "a.json").read_text())
        rec["status"] = "done"
        write(run / "nodes" / "a.json", rec)
        stats = watcher.poll()
        assert stats["edited"] == 1 and len(calls_of(transport, "edit_text")) == 1
        assert "[1] ✅ a" in calls_of(transport, "edit_text")[0][2]
        watcher.poll()  # nothing changed again
        assert len(calls_of(transport, "edit_text")) == 1


def test_terminal_state_edits_unpins_and_closes():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        run = make_run(tmp, "term", AGENT_GATE,
                       node_states={"a": {"status": "done"}})
        transport = FakeTransport()
        watcher = watcher_with(tmp, transport)
        watcher.poll()
        graph = json.loads((run / "graph.json").read_text())
        byid = {n["id"]: n for n in graph["nodes"]}
        write(run / "nodes" / "g.json", {**stamp(byid, byid["g"]), "status": "done"})
        write(run / "nodes" / "b.json", {**stamp(byid, byid["b"]), "status": "done"})
        stats = watcher.poll()
        assert stats["closed"] == 1, stats
        assert len(calls_of(transport, "unpin")) == 1
        book = ws.load_book(run)
        assert book["closed"] is True and book["pinned"] is False
        assert "🏁 done" in calls_of(transport, "edit_text")[-1][2]
        # closed runs are never touched again
        transport.calls.clear()
        assert watcher.poll()["seen"] == 1 and not transport.calls


def test_restart_recovers_bookkeeping_without_duplicate_photo():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        run = make_run(tmp, "crash", AGENT_GATE,
                       node_states={"a": {"status": "running", "pid": 0}})
        first = FakeTransport()
        watcher_with(tmp, first).poll()
        assert len(calls_of(first, "send_photo")) == 1
        # restart: a brand-new watcher recovers the on-disk book
        second = FakeTransport()
        watcher_with(tmp, second).poll()
        assert calls_of(second, "send_photo") == [] and calls_of(second, "send_text") == []
        assert calls_of(second, "pin") == []
        rec = json.loads((run / "nodes" / "a.json").read_text())
        rec["status"] = "done"
        write(run / "nodes" / "a.json", rec)
        stats = watcher_with(tmp, second).poll()
        assert stats["edited"] == 1
        assert ws.load_book(run)["text_message_id"] == 102  # first watcher's text id, resumed


def test_watch_filter_gates_adoption():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        make_run(tmp, "tg", AGENT_GATE,
                 node_states={"a": {"status": "done"}},
                 run_meta={"owner": {"session": "s1", "platform": "telegram"}})
        make_run(tmp, "desk", AGENT_GATE,
                 node_states={"a": {"status": "done"}},
                 run_meta={"owner": {"session": "s2", "platform": "desktop"}})
        transport = FakeTransport()
        watcher_with(tmp, transport, watch="telegram").poll()
        adopted = calls_of(transport, "send_text")
        assert len(adopted) == 1 and "tg" in adopted[0][1]
        # WATCH=off adopts nothing
        off = FakeTransport()
        watcher_with(tmp, off, watch="off").poll()
        assert not off.calls


def test_throttle_backoff_defers_then_delivers():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        run = make_run(tmp, "throttle", AGENT_GATE,
                       node_states={"a": {"status": "running", "pid": 0}})
        transport = FakeTransport()
        clock = [1000.0]
        watcher = ws.Watcher(transport, roots=[tmp], now=lambda: clock[0])
        watcher.poll()
        transport.edit_errors = [ws.TelegramError(429, "Too Many Requests: retry after 30",
                                                  retry_after=30)]
        rec = json.loads((run / "nodes" / "a.json").read_text())
        rec["status"] = "done"
        write(run / "nodes" / "a.json", rec)
        stats = watcher.poll()
        assert stats["throttled"] == 1 and not calls_of(transport, "edit_text")
        assert ws.load_book(run)["edit_ok_at"] >= 1000.0 + 30.0
        watcher.poll()  # still inside the backoff window: the pending edge keeps waiting
        assert not calls_of(transport, "edit_text")
        clock[0] += 31.0
        stats = watcher.poll()
        assert stats["edited"] == 1 and len(calls_of(transport, "edit_text")) == 1


def test_deleted_message_is_reposted_once():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        run = make_run(tmp, "repost", AGENT_GATE,
                       node_states={"a": {"status": "running", "pid": 0}})
        transport = FakeTransport()
        watcher = watcher_with(tmp, transport)
        watcher.poll()
        old_text_id = ws.load_book(run)["text_message_id"]
        transport.edit_errors = [ws.TelegramError(400, "Bad Request: message to edit not found")]
        rec = json.loads((run / "nodes" / "a.json").read_text())
        rec["status"] = "done"
        write(run / "nodes" / "a.json", rec)
        stats = watcher.poll()
        assert stats["edited"] == 1
        assert len(calls_of(transport, "send_photo")) == 2  # repost carries a fresh photo
        assert len(calls_of(transport, "pin")) == 2
        book = ws.load_book(run)
        assert book["text_message_id"] != old_text_id
        assert book["last_text_sha"] == ws.text_sha(
            ws.render_text(common.run_state(run), now=NOW))


def test_read_model_text_renders_fanout_and_gate_from_scratch_run():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        run = make_run(tmp, "model", [
            {"id": "fan", "type": "agent",
             "fanout": {"items": ["first", "second"], "goal": "read {item}"}},
            {"id": "g", "type": "gate", "after": ["fan"], "question": "Publish?",
             "options": ["yes", "no"]},
        ], node_states={"g": {"status": "pending"}})
        graph = json.loads((run / "graph.json").read_text())
        byid = {n["id"]: n for n in graph["nodes"]}
        rows = [{"status": "done", "output": {"item": i}, "ms": 1} for i in range(2)]
        write(run / "nodes" / "fan.json",
              {**stamp(byid, byid["fan"]), "status": "done",
               "output": {"items": [{"item": i} for i in range(2)], "all_results": rows}})
        for i in range(2):  # settled per-child item records
            write(run / "nodes" / f"fan.{i}.json",
                  {**stamp(byid, byid["fan"]), "status": "done", "output": {"item": i}})
        state = common.run_state(run)
        text = ws.render_text(state, now=NOW)
        counts = state["nodes"]["fan"].get("item_counts") or {}
        assert counts, f"item_counts missing from read model: {state['nodes']['fan']}"
        total = counts.get("total") if isinstance(counts.get("total"), int) \
            else sum(v for k, v in counts.items() if isinstance(v, int) and k != "total")
        settled = sum(counts.get(k, 0) or 0
                      for k in ("done", "partial", "failed", "cancelled"))
        assert f"x{total}" in text and f"{settled}/{total} settled" in text
        assert "❓ Publish?" in text and "⚙️ yes / no" in text
        assert "🔀 model — held" in text
        # terminal summary renders the outcome line
        summary = ws.render_text(dict(state, status="done", done=2), now=NOW)
        assert "🏁 done — 2/2 settled" in summary


def test_startup_hard_fails_without_pillow():
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "PIL" or name.startswith("PIL."):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        (Path(tmp) / "runs").mkdir()
        with patch.dict(os.environ, {
                "WORKFLOW_STREAM_BOT_TOKEN": "t", "WORKFLOW_STREAM_CHAT_ID": "1",
                "WF_RUNS_ROOT": str(Path(tmp) / "runs")}), \
             patch.object(builtins, "__import__", fake_import):
            code = ws.main(["--once"])
        assert code == 2
        with patch.object(builtins, "__import__", fake_import):
            try:
                ws.require_graphic_deps()
                raised = None
            except ws.MissingGraphicDependency as exc:
                raised = exc.instruction
        assert raised and 'pip install "Pillow>=10.0"' in raised


def test_startup_hard_fails_without_vendored_font():
    with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
        try:
            ws.font_path(root=tmp)
            raised = None
        except ws.MissingGraphicDependency as exc:
            raised = exc.instruction
        assert raised and "LiberationMono-Regular.ttf" in raised


def test_config_requires_token_chat_and_valid_enum():
    saved = {k: os.environ.get(k) for k in
             ("WORKFLOW_STREAM_BOT_TOKEN", "WORKFLOW_STREAM_CHAT_ID", "WORKFLOW_STREAM_WATCH")}
    try:
        for key in saved:
            os.environ.pop(key, None)
        problems = ws._config()[5]
        assert any("BOT_TOKEN" in p for p in problems)
        assert any("CHAT_ID" in p for p in problems)
        assert any("dedicated" in p.lower() for p in problems)  # token doc nudge
        os.environ["WORKFLOW_STREAM_WATCH"] = "bogus"
        assert any("WATCH" in p for p in ws._config()[5])
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class StubTransport:
    """Offline stand-in for the Bot API in run_check / readme tests."""

    def __init__(self, results=None, error=None):
        self.results = results or {}
        self.error = error
        self.calls = []
        self._next = 300

    def _post(self, method, payload):
        self.calls.append(method)
        if self.error:
            raise self.error
        return self.results.get(method, {})

    def send_text(self, text, reply_to=None):
        self.calls.append("send_text")
        self._next += 1
        return self._next

    def pin(self, message_id):
        self.calls.append("pin")


def test_hermes_env_file_fills_missing_stream_vars_only():
    saved = {k: os.environ.get(k) for k in
             ("WORKFLOW_STREAM_CHAT_ID", "WORKFLOW_STREAM_POLL_S", "WORKFLOW_STREAM_WATCH")}
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        (home / ".env").write_text(
            "# comment\n"
            "export WORKFLOW_STREAM_CHAT_ID=\"999\"\n"
            "WORKFLOW_STREAM_POLL_S=2.5\n"
            "WORKFLOW_STREAM_WATCH=all\n"
            "UNRELATED=keep\n")
        for key in ("WORKFLOW_STREAM_CHAT_ID", "WORKFLOW_STREAM_POLL_S"):
            os.environ.pop(key, None)
        os.environ["WORKFLOW_STREAM_WATCH"] = "telegram"  # preexisting wins
        try:
            with patch.object(common, "hermes_home", lambda: home):
                ws.load_hermes_env_file()
            assert os.environ["WORKFLOW_STREAM_CHAT_ID"] == "999"       # quotes stripped
            assert os.environ["WORKFLOW_STREAM_POLL_S"] == "2.5"
            assert os.environ["WORKFLOW_STREAM_WATCH"] == "telegram"    # environ wins
            assert "UNRELATED" not in os.environ                        # prefix-scoped
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def test_run_check_off_watch_needs_no_credentials():
    stub = StubTransport()
    ok, problems = ws.run_check("tok", "123", "off", False, 5.0)
    assert not problems and not stub.calls


def test_run_check_reports_dead_token_cleanly():
    stub_cls = lambda *a, **k: StubTransport(error=ws.TelegramError(404, "Not Found"))
    with patch.object(ws, "TelegramTransport", stub_cls):
        ok, problems = ws.run_check("dead", "123", "all", False, 5.0)
    assert any("BOT_TOKEN rejected" in p and "404" in p for p in problems)


def test_run_check_happy_path_verifies_bot_and_chat():
    results = {"getMe": {"username": "stream_bot", "id": 7},
               "getChat": {"type": "private", "username": "hal"}}
    stub_cls = lambda *a, **k: StubTransport(results=results)
    with patch.object(ws, "TelegramTransport", stub_cls):
        ok, problems = ws.run_check("tok", "123", "all", False, 5.0)
    assert not problems
    assert any("@stream_bot" in line for line in ok)
    assert any("chat verified" in line for line in ok)


def test_readme_pins_once_then_skips_and_repins_on_version_bump():
    with tempfile.TemporaryDirectory() as tmp:
        marker = Path(tmp) / "stream-readme.json"
        stub = StubTransport()
        ws.send_readme_once(stub, marker)
        ws.send_readme_once(stub, marker)  # marker present -> no-op
        assert stub.calls.count("send_text") == 1
        assert stub.calls.count("pin") == 1
        assert json.loads(marker.read_text())["version"] == ws.README_VERSION
        ws.README_VERSION += 1
        try:
            ws.send_readme_once(stub, marker)  # version bump -> re-send
        finally:
            ws.README_VERSION -= 1
        assert stub.calls.count("send_text") == 2


def test_env_file_watch_helpers_roundtrip_and_scoping():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        with patch.object(common, "hermes_home", lambda: home):
            assert ws.read_env_file_watch("all") == "all"  # missing file: default
            ws.set_env_file_value("WORKFLOW_STREAM_BOT_TOKEN", "t0k")
            ws.set_env_file_value("WORKFLOW_STREAM_WATCH", "telegram")
            text = (home / ".env").read_text()
            assert "WORKFLOW_STREAM_BOT_TOKEN=t0k" in text
            assert "WORKFLOW_STREAM_WATCH=telegram" in text
            assert ws.read_env_file_watch("all") == "telegram"
            ws.set_env_file_value("WORKFLOW_STREAM_WATCH", "off")  # rewrite, not duplicate
            text = (home / ".env").read_text()
            assert text.count("WORKFLOW_STREAM_WATCH=") == 1
            assert "WORKFLOW_STREAM_BOT_TOKEN=t0k" in text  # unrelated line untouched
            assert ws.read_env_file_watch("all") == "off"
            ws.set_env_file_value("WORKFLOW_STREAM_WATCH", "bogus")
            assert ws.read_env_file_watch("all") == "all"  # invalid: fall back, never wedge


def test_workflow_stream_command_toggles_and_reports_offline():
    door_spec = importlib.util.spec_from_file_location("stream_cmd_door", ROOT / "__init__.py")
    plugin = importlib.util.module_from_spec(door_spec)
    door_spec.loader.exec_module(plugin)
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        saved = {k: os.environ.get(k) for k in
                 ("WORKFLOW_STREAM_BOT_TOKEN", "WORKFLOW_STREAM_CHAT_ID")}
        try:
            for key in saved:  # a dev shell may export real values; keep the test offline
                os.environ.pop(key, None)
            with patch.object(common, "hermes_home", lambda: home):
                sys.modules["workflow_stream"] = ws
                off = plugin._workflow_stream_command("off")
                assert "WATCH=off" in off and "no restart" in off
                assert "WORKFLOW_STREAM_WATCH=off" in (home / ".env").read_text()
                on = plugin._workflow_stream_command("on")
                assert "WATCH=all" in on
                assert "WORKFLOW_STREAM_WATCH=all" in (home / ".env").read_text()
                unknown = plugin._workflow_stream_command("wat")
                assert "Usage:" in unknown
                # status is local-only: no network even with zero config
                status = plugin._workflow_stream_command("")
                assert "watch=all" in status and "token: NOT SET" in status
                assert "daemon:" in status
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


class _CredsFakeTransport(FakeTransport):
    def __init__(self, *a):  # TelegramTransport is constructed with (token, chat)
        super().__init__()
        _MADE.append(self)

    def _post(self, method, payload):
        self.calls.append((method, payload))
        return {}  # getMe/getChat both OK: preflight passes, main reaches the loop


_MADE = []


def test_daemon_reads_watch_from_env_file_each_cycle():
    """on/off toggle works without a restart: main() re-reads WATCH from the
    env file every cycle — unless the operator pinned it via process env."""
    saved = {k: os.environ.get(k) for k in
             ("WORKFLOW_STREAM_BOT_TOKEN", "WORKFLOW_STREAM_CHAT_ID",
              "WORKFLOW_STREAM_WATCH", "WF_RUNS_ROOT")}
    try:
        for key in saved:
            os.environ.pop(key, None)
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            home = Path(tmp) / "home"
            home.mkdir()
            (home / ".env").write_text(
                "WORKFLOW_STREAM_BOT_TOKEN=123456789:t\n"
                "WORKFLOW_STREAM_CHAT_ID=1\n"
                "WORKFLOW_STREAM_WATCH=off\n")
            runs = Path(tmp) / "runs"
            runs.mkdir()
            with patch.object(common, "hermes_home", lambda: home), \
                 patch.dict(os.environ, {"WF_RUNS_ROOT": str(runs)}), \
                 patch.object(ws, "TelegramTransport", _CredsFakeTransport):
                sys.modules["workflow_stream"] = ws
                assert ws.main(["--once"]) == 0
                assert not _MADE[-1].calls  # watch=off from FILE: feature idle
            # pinned: an explicit process-env WATCH beats the file (still all → readme sent)
            (home / ".env").write_text(
                "WORKFLOW_STREAM_BOT_TOKEN=123456789:t\n"
                "WORKFLOW_STREAM_CHAT_ID=1\n"
                "WORKFLOW_STREAM_WATCH=off\n")
            with patch.object(common, "hermes_home", lambda: home), \
                 patch.dict(os.environ, {"WF_RUNS_ROOT": str(runs),
                                         "WORKFLOW_STREAM_WATCH": "all"}), \
                 patch.object(ws, "TelegramTransport", _CredsFakeTransport):
                assert ws.main(["--once"]) == 0
                assert any(c[0] == "pin" for c in _MADE[-1].calls)
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_act_stream_check_preflight_offline():
    # House convention: load the plugin door by path, never `import __init__`.
    door_spec = importlib.util.spec_from_file_location("stream_check_door", ROOT / "__init__.py")
    plugin = importlib.util.module_from_spec(door_spec)
    door_spec.loader.exec_module(plugin)
    stub_cls = lambda *a, **k: StubTransport(error=ws.TelegramError(404, "Not Found"))
    saved = {k: os.environ.get(k) for k in ("WORKFLOW_STREAM_BOT_TOKEN", "WORKFLOW_STREAM_CHAT_ID")}
    try:
        os.environ["WORKFLOW_STREAM_BOT_TOKEN"] = "123456789:deadbeef"
        os.environ["WORKFLOW_STREAM_CHAT_ID"] = "123"
        with patch.object(ws, "TelegramTransport", stub_cls):
            sys.modules["workflow_stream"] = ws  # plugin reuses the patched module
            out = json.loads(plugin.handle({"action": "stream_check"}))
        assert out["ok"] is False
        assert any("BOT_TOKEN rejected" in p for p in out["problems"])
        assert any("BotFather" in step for step in out["next_steps"])
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


if __name__ == "__main__":
    tests = sorted((name, fn) for name, fn in globals().items()
                   if name.startswith("test_") and callable(fn))
    failures = 0
    for name, fn in tests:
        try:
            fn()
            print("PASS", name)
        except Exception as exc:
            failures += 1
            print("FAIL", name, type(exc).__name__, str(exc))
    print(f"TOTAL {len(tests)} FAIL {failures}")
    sys.exit(bool(failures))