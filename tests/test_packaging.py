"""Packaging-specific reproducibility, manifest, and import-isolation checks."""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import types
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
VERSION = re.search(r"^version:\s*([\d.]+)", (ROOT / "plugin.yaml").read_text(), re.M).group(1)


def check(label: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(label)
    print("PASS", label)


def main() -> None:
    manifest = json.loads((ROOT / "dashboard/manifest.json").read_text(encoding="utf-8"))
    check("dashboard backend keeps its API and hidden tab with a loadable web entry",
          manifest.get("api") == "plugin_api.py" and manifest.get("tab", {}).get("hidden") is True
          and manifest.get("name") == "hermes-workflows" and manifest.get("version") == VERSION
          and manifest.get("entry") == "index.js" and (ROOT / "dashboard/index.js").is_file())

    collision = types.ModuleType("wfcommon")
    sys.modules["wfcommon"] = collision
    api_path = ROOT / "dashboard/plugin_api.py"
    before_path = list(sys.path)
    spec = importlib.util.spec_from_file_location("packaging_dashboard_probe", api_path)
    if spec is None or spec.loader is None:
        raise AssertionError("dashboard plugin API cannot be loaded")
    api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(api)
    scoped = api._workflow_common()
    check("dashboard resolves its sibling wfcommon under a private module name",
          Path(scoped.__file__).resolve() == (ROOT / "wfcommon.py").resolve()
          and scoped is not collision and sys.modules["wfcommon"] is collision
          and list(sys.path) == before_path)
    sys.modules.pop("wfcommon", None)

    with tempfile.TemporaryDirectory(prefix=".tmp-package-", dir=HERE) as temp_dir:
        temp = Path(temp_dir)
        first = temp / "first.zip"
        second = temp / "second.zip"
        for target in (first, second):
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/pack.py"), "--output", str(target)],
                cwd=ROOT, capture_output=True, text=True,
            )
            if result.returncode != 0:
                raise AssertionError(
                    f"pack.py exit {result.returncode}:\n{result.stdout}\n{result.stderr}"
                )
        first_bytes, second_bytes = first.read_bytes(), second.read_bytes()
        check("identical source trees produce byte-identical archives", first_bytes == second_bytes)
        check("sidecar pins the archive bytes",
              first.with_suffix(".zip.sha256").read_text(encoding="utf-8")
              == f"{hashlib.sha256(first_bytes).hexdigest()}  first.zip\n")

        with zipfile.ZipFile(first) as archive:
            members = set(archive.namelist())
            root = f"hermes-workflows-{VERSION}/"
            check("archive includes runtime, docs, skill, examples, tests, and checksum manifest",
                  all(root + rel in members for rel in (
                      "plugin.yaml", "__init__.py", "wf.py", "wfcommon.py",
                      "dashboard/manifest.json", "dashboard/index.js", "dashboard/plugin_api.py", "desktop/plugin.js",
                      "README.md", "INSTALL.md", "SKILL.md", "examples/README.md", "examples/basics/smoke.json",
                      "examples/basics/approve-publish.json", "examples/review/blind-council.workflow.json",
                      "examples/basics/branch-on-verdict.json",
                      "examples/build/triage-route.workflow.json",
                      "examples/review/portable-review.workflow.json",
                      "examples/ops/incident-response.json",
                      "examples/release/gated-publish.workflow.json",
                      "examples/release/machine-watch.workflow.json",
                      "examples/basics/exchange-run.workflow.json",
                      "examples/build/quorum-probe.workflow.json",
                      "examples/build/census-fanout.workflow.json", "examples/review/escalation-ladder.workflow.json", "examples/build/bulk-transform.workflow.json",
                      "examples/release/release-lifecycle.workflow.json", "examples/release/issue-to-pr.workflow.json", "examples/release/submit-pr.workflow.json", "references/portable.md",
                      "tests/test_packaging.py", "tests/test_fanout_ui.mjs", "tests/test_card_frontend_contract.mjs",
                      "tests/test_inline_header.mjs", "tests/fixtures/mac-source.txt",
                      "references/grammar.md", "references/operations.md",
                      # est-k75i: the upstream-shaped tool-result-card proposal doc is
                      # part of the shipped surface — desktop contributors read it from
                      # the package. Mandatory new-file pack-surface gate: an unshipped
                      # doc is invisible to every install.
                      "docs/card-toolresult-hook.md", "docs/orphan-held.md", "orphan_watch.py", "SHA256SUMS")))
            check("fake child retains executable mode in ZIP",
                  (archive.getinfo(root + "tests/fake").external_attr >> 16) & 0o111 == 0o111)
            # est-4vnq finding 4 (pack-list omission, made explicit — pack_choice=INCLUDED):
            # scripts/pr_tag_audit.py was outside INCLUDE_FILES/INCLUDE_PATTERNS while
            # tests/test_pr_tag_audit.py (shipped via the tests/test_*.py glob) executes
            # it, and test_packaging stayed green (observed red-proof on base f83e5d0:
            # pack.collect_sources() scripts members == ['scripts/graph_check.py',
            # 'scripts/pack.py', 'scripts/suite.py'] — helper absent, ALL PASS).
            # The helper SHIPS because its shipped test subprocess-executes it from the
            # package root; the four checks below are the mechanical contract.
            packed_scripts = {m[len(root):] for m in members if m.startswith(root + "scripts/")}
            check("audit helper ships: scripts/pr_tag_audit.py is in the ZIP (explicit include decision)",
                  "scripts/pr_tag_audit.py" in packed_scripts)
            check("pack-list contract: ZIP scripts/ set equals the exact declared packed set",
                  packed_scripts == {"scripts/pack.py",
                                      "scripts/pr_tag_audit.py", "scripts/suite.py",
                                      # est-zee7e: the pin-delta re-pin gate ships
                                      # because its shipped test exec-loads it from
                                      # the package root (est-4vnq finding-4 shape).
                                      "scripts/pin_delta_check.py",
                                      # est-5p7x: the diagram-law generators — shipped
                                      # because tests/test_diagram_law.py executes both
                                      # from the package root (same rationale as the
                                      # pr_tag_audit include above).
                                      "scripts/diagram_readme.py",
                                      "scripts/graph_diagram.py",
                                      # est-2ek.1.866 (PR #323): tests/test_self_review_422_1866.py
                                      # exec-modules this at import — packs per the
                                      # est-4vnq finding-4 rationale in pack.py.
                                      "scripts/pr_formal_review.py", "scripts/drift_check.py",
                                      # est-h0huv: tests/test_wofify.py subprocess-executes
                                      # this CLI end to end — the est-4vnq finding-4
                                      # rationale again; the tool ships beside its test.
                                      "scripts/wofify.py",
                                      # workflow-stream (#3): the standalone Telegram
                                      # mirror daemon ships beside its vendored font;
                                      # the shipped tests/test_workflow_stream*.py
                                      # exec-load it from the package root.
                                      "scripts/workflow_stream.py"})
            # wf165d: the no-silent-middle half only has a premise in a REPO
            # checkout — the source-only scripts (make_public.py,
            # lane_recover.py) never travel inside the ZIP, so from the
            # unpacked root the equality is unsatisfiable by construction
            # (the committee's repro: exit 1 at the unpacked root while CI
            # is green). Premise is .git, NOT the source-only files
            # themselves: an adversary who deletes make_public.py +
            # lane_recover.py and smuggles a silent-middle script must not
            # flip the check into a silent skip — inside a git clone it still
            # runs and REDs. The ZIP-side equality above stays unconditional
            # and still bites a smuggled extra script INTO the package.
            if (ROOT / ".git").exists():
                check("pack-list contract: every repo scripts/*.py is packed or declared source-only (no silent middle)",
                      {p.name for p in (ROOT / "scripts").glob("*.py")} ==
                      {Path(s).name for s in packed_scripts}
                      | {"make_public.py", "lane_recover.py",
  # est-xwvu9 (upstream #133387 accepted ask): the post-exit
  # hook is stripped from the shipped build; kept here for
  # our own distribution path, executed by the source-only
  # tests/test_post_exit_hook.py.
  "post_exit_hook.py",
  # est-a5obt (PR #414): estate horde generator — speaks bd-store takeover law
  # at an estate-private store; source-only like make_public.py, declared in
  # pack.py SOURCE_ONLY_SCRIPTS.
  "horde_gen.py",
  # est-d2eyc (PR #467, SH-07): estate shelf census — scans the shelf library,
  # run-dir roots, and seat profiles; source-only, declared in pack.py
  # SOURCE_ONLY_SCRIPTS.
  "shelf-census.py",
  # ci-P1 (NEXTGEN-CI (e)): estate-ci observe instruments (timing/registry/
  # md-input census + preflight observer); source-only, declared in pack.py
  # SOURCE_ONLY_SCRIPTS.
  "timing_observe.py", "registry_observe.py", "md_inputs_observe.py",
  "ci_preflight_observe.py"})
            else:
                print("SKIP pack-list repo-side half: no .git — running from an "
                      "unpacked ZIP/archive, where the premise does not exist")
            check("the test that executes the audit helper ships beside it",
                  root + "tests/test_pr_tag_audit.py" in members)
            unsafe = ("/.git/", "/home", "/workflows/", "/state.db", "/runner.log",
                      "/pre-lanes", "/home1/", "/.tmp-")
            check("archive excludes generated homes, owner-local examples, state, backups, logs, and Git metadata",
                  not any(part.endswith("/.git") or any(token in part for token in unsafe)
                          for part in members)
                  and {m[len(root):] for m in members if m.startswith(root + "examples/")}
                      == {"examples/README.md", "examples/basics/smoke.json", "examples/basics/approve-publish.json",
                          "examples/review/blind-council.workflow.json",
                          "examples/basics/branch-on-verdict.json",
                          "examples/build/triage-route.workflow.json",
                          "examples/ops/incident-response.json",
                          "examples/review/portable-review.workflow.json",
                          "examples/release/gated-publish.workflow.json",
                          "examples/release/machine-watch.workflow.json",
                          "examples/build/quorum-probe.workflow.json",
                          "examples/build/census-fanout.workflow.json", "examples/review/escalation-ladder.workflow.json", "examples/build/bulk-transform.workflow.json",
                          "examples/release/release-lifecycle.workflow.json", "examples/release/issue-to-pr.workflow.json",
                          "examples/release/submit-pr.workflow.json",
                          "examples/basics/exchange-run.workflow.json"})
            # P1 (PR #432): wf.py loads hooks_outbound.py by FILE PATH, so the import-closure
            # probe below cannot see it — pin the ZIP membership directly.
            check("hooks_outbound.py ships in the ZIP (wf.py spec-loads it; the shipped bridge test needs it)",
                  root + "hooks_outbound.py" in members)
            # P3 (#430, est-w86l0 lane): __init__.py spec-loads subagent_accounting.py by
            # FILE PATH when the token is allowed — same est-4vnq finding-4 shape.
            check("subagent_accounting.py ships in the ZIP (__init__.py spec-loads it)",
                  root + "subagent_accounting.py" in members)
            # P5 (#431, est-431 lane): approval_bridge.py + its switch-board doc — the
            # same finding-4 shape; an absent bridge armed per docs/native-hooks.md is a
            # silent no-op on every ZIP install.
            check("approval_bridge.py ships in the ZIP (__init__.py spec-loads it)",
                  root + "approval_bridge.py" in members)
            check("docs/native-hooks.md ships in the ZIP (the p3/p5 discoverability doc)",
                  root + "docs/native-hooks.md" in members)
            checksum_text = archive.read(root + "SHA256SUMS").decode("utf-8")
            rows = [line.split("  ", 1) for line in checksum_text.splitlines()]
            check("SHA256SUMS verifies every pinned source entry",
                  bool(rows) and all(len(row) == 2 and root + row[1] in members
                                     and hashlib.sha256(archive.read(root + row[1])).hexdigest() == row[0]
                                     for row in rows))
            manifest = archive.read(root + "plugin.yaml").lower()
            check("public manifest declares license, homepage and requires_hermes",
                  b"license: mit" in manifest and b"homepage: https://github.com/" in manifest
                  and b"requires_hermes:" in manifest)

        # #105 discharge: membership-by-glob passed while the shipped test_*.py died
        # ModuleNotFoundError on wf_test_isolation — assert the import CLOSURE from the
        # unpacked package root instead of just listing names.
        with tempfile.TemporaryDirectory(prefix=".tmp-unpack-") as unpack_dir:
            unpacked = Path(unpack_dir)
            with zipfile.ZipFile(first) as archive:
                archive.extractall(unpacked)
            pkg = unpacked / root.rstrip("/")
            core_allowlist = {"hermes_constants", "fastapi"}  # supplied by the pinned /opt/hermes env
            missing: dict[str, list[str]] = {}
            for test_file in sorted(pkg.glob("tests/test_*.py")):
                tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=str(test_file))
                names: set[str] = set()
                for node in tree.body:  # top-level imports only: the shipped-entry contract
                    if isinstance(node, ast.Import):
                        names.update(alias.name.split(".")[0] for alias in node.names)
                    elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                        names.add(node.module.split(".")[0])
                # Resolution mirrors the shipped-entry reality: running tests/test_X.py puts
                # tests/ (script dir) on sys.path; root and dashboard/ ride along as the
                # tests themselves insert them. A top-level import that resolves in NONE of
                # these is the #105 dead-package shape.
                probe = (
                    "import importlib.util, json, sys\n"
                    "root = sys.argv[1]\n"
                    "for p in (root, root + '/tests', root + '/dashboard'):\n"
                    "    sys.path.insert(0, p)\n"
                    "mods = json.loads(sys.argv[2])\n"
                    "bad = []\n"
                    "for m in mods:\n"
                    "    try:\n"
                    "        importlib.import_module(m)\n"
                    "    except Exception as exc:\n"
                    "        bad.append(f'{m}: {type(exc).__name__}: {exc}')\n"
                    "print('MISSING:' + json.dumps(bad))\n"
                )
                candidates = sorted(
                    n for n in names
                    if n not in sys.stdlib_module_names and n not in core_allowlist
                    and n != test_file.stem
                )
                env = {k: v for k, v in os.environ.items()
                       if k not in {"WF_RUNS_ROOT", "HERMES_HOME"}}
                env.setdefault("PYTHONPATH", "/opt/hermes")
                result = subprocess.run(
                    [sys.executable, "-c", probe, str(pkg), json.dumps(candidates)],
                    capture_output=True, text=True, env=env,
                )
                line = [l for l in result.stdout.splitlines() if l.startswith("MISSING:")]
                bad = json.loads(line[-1][len("MISSING:"):]) if line else [result.stderr[-200:]]
                for failure in bad:
                    missing.setdefault(test_file.name, []).append(failure)
            check("every shipped tests/test_*.py import-closure resolves from the unpacked package root",
                  not missing and repr(missing) == "{}")

    print("ALL PASS")


if __name__ == "__main__":
    main()
