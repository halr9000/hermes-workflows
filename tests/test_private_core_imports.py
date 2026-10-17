#!/usr/bin/env python3
"""Private-core-import contract (#456): the reasoned allow-list.

hermes-agent >= v0.22.0 `hermes plugins validate` warns on every import of a
private Hermes core module (hermes_cli.plugin_validate_private_imports). This
test runs that same lint over the repo and pins its findings to ALLOWED: one
reasoned row per (file, target). Line numbers are not part of the key.

  NEW   (reported, not allowed) -> FAIL: soft-guard it per R2 and add a row,
                                   or use a public surface
  STALE (allowed, not reported) -> FAIL: delete the row
  core without the lint (< v0.22.0) -> SKIP + exit 0

Conventions: stdlib only, PASS/FAIL/SKIP lines, exit 0 = green. Same
contract-probe class as tests/test_upstream_hook_contract.py; source is read
only through upstream's own lint.

Run:  PYTHONPATH=<hermes-agent >= v0.22.0 checkout> python3 tests/test_private_core_imports.py
"""
import importlib
import importlib.util
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LINT = "hermes_cli.plugin_validate_private_imports"
FINDING_RE = re.compile(r"^(?P<rel>\S+):(?P<line>\d+) (?P<target>[A-Za-z_][\w.]*)$")
_NOPUB = " No public equivalent in v0.22.2 _PUBLIC."

ALLOWED = {
    ("__init__.py", "agent.reasoning_effort.route_supported_efforts"):
        "_route_efforts: `except Exception: pass` -> codex vocabulary for openai-codex, "
        "else wfcommon.reasoning_levels()." + _NOPUB,
    ("__init__.py", "agent.reasoning_effort.codex_supported_efforts"):
        "_route_efforts (openai-codex branch): `except Exception: pass` -> "
        "wfcommon.reasoning_levels()." + _NOPUB,
    ("__init__.py", "agent.reasoning_effort.EFFORT_LADDER"):
        "_nearest_effort: `except Exception` -> None (no clamp target)." + _NOPUB,
    ("__init__.py", "hermes_cli.model_switch.DIRECT_ALIASES"):
        "_alias_provider_pair: `except Exception: pass` -> (None, None); the seat "
        "default route stays authoritative." + _NOPUB,
    ("__init__.py", "hermes_cli.model_switch._ensure_direct_aliases"):
        "_alias_provider_pair: `except Exception: pass` -> (None, None); the seat "
        "default route stays authoritative." + _NOPUB,
    ("__init__.py", "agent.auxiliary_client.call_llm"):
        "_import_call_llm raises by design; callers map it: _ping_route_once "
        "`except ModuleNotFoundError` -> _ping_subprocess, `except Exception` -> "
        "liveness 'unknown'; _ping_reachable `except Exception` -> False." + _NOPUB +
        " agent.plugin_llm has no route_info, so it cannot keep the #25 same-route law."
        " _PING_SUBPROCESS (__init__.py ~1191) imports the same symbol inside a string"
        " run in a subprocess, which the AST lint cannot see.",
    ("__init__.py", "agent.retry_utils.parse_retry_after_seconds"):
        "_ping_retry_after: `except Exception` -> None (Retry-After never "
        "fabricated)." + _NOPUB,
    ("__init__.py", "gateway.session_context.get_session_env"):
        "_session_env: `except ImportError` -> os.environ.get(name, '')." + _NOPUB,
    ("wf.py", "agent.reasoning_effort.route_supported_efforts"):
        "_resolve_child_reasoning (openai-codex branch): `except Exception` -> "
        "supported=None, then the wfcommon.reasoning_levels() table." + _NOPUB,
    ("wf.py", "toolsets.validate_toolset"):
        "_filter_child_toolsets: `except Exception` -> names pass through "
        "unjudged." + _NOPUB,
    ("wf.py", "hermes_cli.plugins.get_plugin_toolset_keys_nowait"):
        "_filter_child_toolsets: `except Exception` -> plugin_keys = set() (names "
        "still judged by validate_toolset + MCP keys)." + _NOPUB,
    ("wfcommon.py", "hermes_yaml.safe_load"):
        "_yaml_load: `except ImportError: pass` -> ruamel.yaml (YAML 1.1), then "
        "PyYAML." + _NOPUB,
}


def load_lint():
    """The lint module, or None when core predates it (SKIP). A present module
    that fails to import is a FAIL, never a skip."""
    try:
        if importlib.util.find_spec("hermes_cli") is None or importlib.util.find_spec(LINT) is None:
            return None
    except ModuleNotFoundError as exc:
        if exc.name in ("hermes_cli", LINT):
            return None
        raise
    return importlib.import_module(LINT)


def main():
    try:
        m = load_lint()
    except Exception as exc:
        print(f"FAIL: {LINT} is present but does not import: {type(exc).__name__}: {exc}")
        return 1
    if m is None:
        print(f"SKIP private-import contract: {LINT} not importable (core < v0.22.0)")
        return 0
    try:
        lines = list(m.private_import_findings(REPO_ROOT))
    except Exception as exc:
        print(f"FAIL: {LINT}.private_import_findings raised: {type(exc).__name__}: {exc}")
        return 1
    fails = 0
    found = {}
    for raw in lines:
        hit = FINDING_RE.match(str(raw))
        if hit is None:
            print(f"FAIL: unparseable lint finding {raw!r} (upstream output format changed?)")
            fails += 1
            continue
        found.setdefault((hit["rel"], hit["target"]), []).append(int(hit["line"]))
    for rel, target in sorted(set(found) - set(ALLOWED)):
        for line in found[(rel, target)]:
            print(f"FAIL: NEW private core import {rel}:{line} {target} — soft-guard it "
                  f"per R2 and add a reasoned ALLOWED row, or use a public surface")
            fails += 1
    for rel, target in sorted(set(ALLOWED) - set(found)):
        print(f"FAIL: STALE allow-list row ({rel}, {target}) — delete the row: the "
              f"import is gone or core made it public")
        fails += 1
    for key in sorted(set(ALLOWED) & set(found)):
        rel, target = key
        at = ",".join(str(n) for n in found[key])
        print(f"PASS: allowed {rel}:{at} {target} — {ALLOWED[key]}")
    print(f"{'FAIL' if fails else 'PASS'}: {len(lines)} lint finding(s), "
          f"{len(ALLOWED)} allow-list row(s), {fails} failure(s)")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
