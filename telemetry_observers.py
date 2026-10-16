"""Default-off telemetry observers: subagent_stop + on_skill_lifecycle -> one bounded JSONL corpus.

PRIVACY LAW: rows carry sizes, counts, and statuses ONLY. Never a summary, a tool
history, a tool input, a prompt, or any model text. Observer law: payload-tolerant
(``**kwargs``, missing field -> null) and NEVER raises into the host.

Flag: ``plugins.entries.hermes-workflows.settings.telemetry`` (default false).
Flag off = zero writes, no directory. Corpus: ``<runs_root>/telemetry/events-YYYYMMDD.jsonl``
(0600 files, 0700 dir). Size cap: once the day file is >= MAX_BYTES no new row is
appended (bounded corpus; silent, no exception).
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

MAX_BYTES = 4 * 1024 * 1024
_common = None   # bound by the door: wfcommon module (runs_root / owner_setting)


def bind(common):
    global _common
    _common = common


def enabled():
    try:
        v = _common.owner_setting("telemetry")
    except Exception:
        return False
    return v is True


def _int_or_none(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v == v:
        return int(v)
    return None


def _str_or_none(v):
    return v if isinstance(v, str) else None


def _append(row, root=None):
    """Append one row; True when written. Never raises."""
    try:
        base = Path(root) if root is not None else Path(_common.runs_root())
        now = datetime.now(timezone.utc)
        d = base / "telemetry"
        p = d / f"events-{now.strftime('%Y%m%d')}.jsonl"
        try:
            if p.stat().st_size >= MAX_BYTES:
                return False
        except FileNotFoundError:
            pass
        d.mkdir(mode=0o700, parents=True, exist_ok=True)
        row = dict(row)
        row["ts"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        line = json.dumps(row, separators=(",", ":")) + "\n"
        fd = os.open(str(p), os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, line.encode("utf-8"))
        finally:
            os.close(fd)
        return True
    except Exception:
        return False


def on_subagent_stop(**kwargs):
    try:
        if not enabled():
            return None
        hist = kwargs.get("tool_call_history")
        _append({"kind": "subagent",
                 "parent_session_id": _str_or_none(kwargs.get("parent_session_id")),
                 "child_role": _str_or_none(kwargs.get("child_role")),
                 "child_status": _str_or_none(kwargs.get("child_status")),
                 "duration_ms": _int_or_none(kwargs.get("duration_ms")),
                 "tool_count": len(hist) if isinstance(hist, (list, tuple)) else None})
    except Exception:
        pass
    return None


def on_skill_lifecycle(**kwargs):
    try:
        if not enabled():
            return None
        reused = kwargs.get("reused")
        _append({"kind": "skill",
                 "action": _str_or_none(kwargs.get("action")),
                 "skill_name": _str_or_none(kwargs.get("skill_name")),
                 "use_count": _int_or_none(kwargs.get("use_count")),
                 "reused": reused if isinstance(reused, bool) else None})
    except Exception:
        pass
    return None


def install_hooks(ctx):
    """Register both observers only when the flag is on (flag off = nothing registered)."""
    if not enabled():
        return False
    ctx.register_hook("subagent_stop", on_subagent_stop)
    ctx.register_hook("on_skill_lifecycle", on_skill_lifecycle)
    return True
