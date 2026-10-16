#!/usr/bin/env python3
"""Build the private, reproducible Hermes Workflows source ZIP (stdlib only)."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.search(r"^version:\s*([\d.]+)", (ROOT / "plugin.yaml").read_text(), re.M).group(1)  # single source: the manifest
PACKAGE_NAME = f"hermes-workflows-{VERSION}"


# est-2ek.1.159 version truth: install.json provenance rides the ZIP root so an
# installed seat can tell "still broken" from "fixed, not deployed". Test seam:
# HERMES_WF_PACK_COMMIT pins the commit (sanitized: 40-hex only, never raw);
# SOURCE_DATE_EPOCH pins packaged_at for reproducibility.
_COMMIT_PIN = re.compile(r"^[0-9a-f]{40}$")


def _source_commit() -> str:
    pin = os.environ.get("HERMES_WF_PACK_COMMIT", "").strip()
    if _COMMIT_PIN.fullmatch(pin):
        return pin
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
            text=True, timeout=10,
        )
        if sha.returncode == 0 and _COMMIT_PIN.fullmatch(sha.stdout.strip()):
            return sha.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "unknown"


def _packaged_at() -> str:
    # Reproducibility law: no wall-clock byte may ever enter the ZIP. Pinned by
    # SOURCE_DATE_EPOCH, else the HEAD commit's own committer date (deterministic
    # per tree — two packs of one tree stay byte-identical), else "unknown".
    epoch = os.environ.get("SOURCE_DATE_EPOCH", "").strip()
    if epoch.isdigit():
        # Explicit UTC: astimezone()/local conversion lets the HOST timezone leak
        # into the bytes — TZ=UTC vs TZ=America/Chicago gave different packaged_at
        # and different ZIP hashes for one HEAD (zap review, issue #198).
        return datetime.fromtimestamp(int(epoch), timezone.utc).isoformat()
    try:
        when = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "HEAD"], cwd=ROOT,
            capture_output=True, text=True, timeout=10,
        )
        if when.returncode == 0 and when.stdout.strip():
            return datetime.fromisoformat(when.stdout.strip()).isoformat()
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return "unknown"


def install_provenance() -> bytes:
    return (json.dumps(
        {"version": VERSION, "source_commit": _source_commit(),
         "packaged_at": _packaged_at()},
        separators=(",", ":"),
    ) + "\n").encode("utf-8")

# Exact files plus deliberately narrow source patterns. Never package a worktree wholesale.
INCLUDE_FILES = (
    ".gitignore",
    "AGENTS.md",
    "INSTALL.md",
    "LICENSE",
    "README.md",
    "SKILL.md",
    "__init__.py",
    "telemetry_observers.py",  # PR-5: imported by __init__.py at bind time
    "card_ledger.py",  # imported by __init__.py at bind time (hook-led card ledger)
    "gate_index.py",  # PR-3: imported by __init__.py at bind time
    "docs/admission-gates.md",
    "card_enforcement.py",  # #157: imported by __init__.py at bind time — a packaged plugin without it dies at import
    "dashboard/manifest.json",
    "dashboard/index.js",  # hidden API plugin still needs the web host's script/registration contract
    "dashboard/plugin_api.py",
    "desktop/plugin.js",
    # est-k75i: the tool-result-card proposal doc ships with the package (the
    # mandatory new-file pack-surface gate). Pinned by tests/test_packaging.py's
    # ZIP-membership assertion — an unshipped doc is invisible to every install.
    "docs/card-toolresult-hook.md",
    # #51 (est-6cym): the PACKAGED curated starter set ships with the plugin —
    # the door's `_packaged_general_root()` resolves next to __init__.py, and
    # tests/test_door_general_51.py (shipped via the tests glob) asserts the
    # set loads from that address. Unshipped = every fresh install loses the
    # cold-shelf `from: general/<name>` fallback AND the suite dies (the exact
    # est-4vnq finding-4 shape). Explicit listing, per pack.py's no-wholesale law.
    "library/general/README.md",
    "library/general/pr-review-light.json",
    "library/general/issue-triage.json",
    "library/general/test-first-feature-slice.json",
    "library/general/fan-out-audit-census.json",
    "library/general/report-and-verify.json",
    "examples/basics/approve-publish.json",
    "examples/review/blind-council.workflow.json",
    "examples/basics/branch-on-verdict.json",
    "examples/basics/exchange-run.workflow.json",
    "examples/release/gated-publish.workflow.json",
    "examples/build/triage-route.workflow.json",
    "examples/ops/incident-response.json",
    "examples/release/machine-watch.workflow.json",
    "examples/review/portable-review.workflow.json",
    "examples/build/quorum-probe.workflow.json",
    "examples/build/census-fanout.workflow.json",
    "examples/review/escalation-ladder.workflow.json",
    "examples/build/bulk-transform.workflow.json",
    "examples/release/release-lifecycle.workflow.json",
    "examples/release/issue-to-pr.workflow.json",
    "examples/release/submit-pr.workflow.json",
    "examples/README.md",
    "examples/basics/smoke.json",
    "plugin.yaml",
    "CHANGELOG.md",
    "scripts/graph_diagram.py",
    "scripts/diagram_readme.py",
    "scripts/pack.py",
    "scripts/pr_tag_audit.py",  # est-4vnq: tests/test_pr_tag_audit.py executes it; the shipped suite must not die on a missing helper
    # est-5p7x (PR #165 CI blocker, run 37440782151): tests/test_diagram_law.py
    # (shipped via the tests/test_*.py glob) subprocess-executes BOTH generators
    # from the package root — the exact est-4vnq finding-4 shape. The merge ref
    # gained these files from main (#232/#236 diagram law) while pack.py never
    # declared them, so the no-silent-middle contract REDed there while the head
    # tree (predating the files) stayed vacuously green. Stdlib-only, like the
    # graph helpers above, so the ZIP side stays honest.
    "scripts/diagram_readme.py",
    "scripts/graph_diagram.py",
    # est-2ek.1.866 (PR #323 CI red, run 37842073930 admission introduced=1):
    # tests/test_self_review_422_1866.py exec-modules this at import
    # (spec_from_file_location) — the exact est-4vnq finding-4 shape. Stdlib-only,
    # so the ZIP side stays honest.
    "scripts/pr_formal_review.py",
    "scripts/doctor_budgets.py",  # PR-5: loaded by the doctor verb (sub=budgets)
    "scripts/suite.py",
    ".graphifyignore",
    "tests/fake",
    "tests/fake_hermes.py",
    "tests/wf_test_isolation.py",
    "tests/wf_test_markers.py",  # est-jue3: shared parseable-content marker waits, imported by the shipped tests (est-4vnq finding-4 shape: an unshipped helper kills the suite)
    "tests/fixtures/mac-source.txt",
    "tests/fixtures/seat_acquire_driver.py",   # est-g2xx: seat-semaphore driver
    # est-2ek.1.762: the spawn-isolation law's shared helpers — SHIPPED because
    # the shipped tests exec-load them by file path from the package root
    # (test_suite_runs_root_762 imports runs_root_probe; the pinned spawned-
    # runner tests load wf_spawn_isolation_762), the exact est-4vnq finding-4
    # shape: a shipped test that executes an unpackaged helper dies there.
    "tests/fixtures/runs_root_probe.py",
    "tests/fixtures/wf_spawn_isolation_762.py",
    "tests/test_fanout_ui.mjs",
    "wf.py",
    "wf_dialect.py",
    "wfcommon.py",
)
INCLUDE_PATTERNS = ("tests/test_*.py", "tests/test_*.mjs", "references/*.md",
                    "tests/fixtures/dialect/*")   # #33: the corpus test_dialect_js_33 asserts against
EXECUTABLE_FILES = {"scripts/pack.py", "tests/fake"}
# Checkout-only tests stay in repository CI, not the release's test glob.
SOURCE_ONLY_TESTS = {
    "tests/test_diagram_law.py": "needs diagrams/README.md and the source-only renderer",
    "tests/test_example_census_fanout.py": "imports source-only scripts/make_public.py",
    "tests/test_example_issue_to_pr.py": "imports source-only scripts/make_public.py",
    "tests/test_example_release_lifecycle.py": "imports source-only scripts/make_public.py",
    "tests/test_lane_recover_8edcc9bf.py": "executes source-only scripts/lane_recover.py",
    "tests/test_example_exchange_run.py": "reads source-only scripts/scrub-list.txt",
    "tests/test_lane_recover_legacy_amend_237r4.py": "loads source-only scripts/lane_recover.py",
    "tests/test_respawn_hardening_723.py": "loads source-only scripts/lane_recover.py",
    "tests/test_crash_no_exit_respawn_718.py": "loads source-only scripts/lane_recover.py",
    "tests/test_lane_recover_cli_237r3.py": "executes source-only scripts/lane_recover.py",
    "tests/test_plugin_resolution_live_not_old.py": "loads source-only scripts/lane_recover.py",
    "tests/test_scrub_yml_166b.py": "executes source-only scripts/make_public.py",
    "tests/test_stuck_node_finalize_733.py": "executes source-only scripts/lane_recover.py (main's #235)",
    # #248 r3 (rebase onto main's #298): executes source-only make_public.py + reads scripts/scrub-list.txt
    "tests/test_personal_path_scrub_867.py": "executes source-only scripts/make_public.py, reads scripts/scrub-list.txt",
    # est-xwvu9 (upstream #133387 accepted ask): the post-exit hook is NOT shipped;
    # this test executes the source-only scripts/post_exit_hook.py kept for our own
    # distribution path only.
    "tests/test_post_exit_hook.py": "executes source-only scripts/post_exit_hook.py (stripped from the shipped build, est-xwvu9)",
}


def collect_sources() -> list[tuple[str, bytes]]:
    paths = {Path(name) for name in INCLUDE_FILES}
    for pattern in INCLUDE_PATTERNS:
        matches = sorted(ROOT.glob(pattern))
        if not matches:
            raise RuntimeError(f"include pattern matched no files: {pattern}")
        paths.update(path.relative_to(ROOT) for path in matches)

    paths.difference_update(Path(name) for name in SOURCE_ONLY_TESTS)
    sources: list[tuple[str, bytes]] = []
    for rel in sorted(paths, key=lambda p: p.as_posix()):
        path = ROOT / rel
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"included path is not a regular file: {rel.as_posix()}")
        try:
            path.resolve().relative_to(ROOT.resolve())
        except ValueError as exc:
            raise RuntimeError(f"included path escapes source root: {rel.as_posix()}") from exc
        sources.append((rel.as_posix(), path.read_bytes()))

    manifest = dict(sources).get("plugin.yaml", b"").decode("utf-8")
    if not re.search(rf"(?m)^version:\s*{re.escape(VERSION)}\s*$", manifest):
        raise RuntimeError(f"plugin.yaml must declare version {VERSION}")
    return sources


def _zip_info(name: str, executable: bool = False) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    mode = 0o755 if executable else 0o644
    info.external_attr = (stat.S_IFREG | mode) << 16
    info.extra = b""
    info.comment = b""
    return info


def build(output: Path) -> tuple[str, Path]:
    sources = collect_sources()
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    checksums = "".join(
        f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in sources
    ).encode("utf-8")

    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=9, strict_timestamps=True) as archive:
            for name, data in sources:
                archive.writestr(
                    _zip_info(f"{PACKAGE_NAME}/{name}", name in EXECUTABLE_FILES),
                    data,
                    compress_type=zipfile.ZIP_DEFLATED,
                    compresslevel=9,
                )
            archive.writestr(
                _zip_info(f"{PACKAGE_NAME}/SHA256SUMS"), checksums,
                compress_type=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            )
            # Provenance rides AFTER the ledger: SHA256SUMS pins sources only,
            # install.json is generated (no self-referential row).
            archive.writestr(
                _zip_info(f"{PACKAGE_NAME}/install.json"), install_provenance(),
                compress_type=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            )
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)

    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    sidecar = output.with_suffix(output.suffix + ".sha256")
    sidecar.write_text(f"{digest}  {output.name}\n", encoding="utf-8")
    return digest, sidecar


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "artifacts" / f"{PACKAGE_NAME}.zip",
        help="output ZIP path (default: artifacts/<package>.zip)",
    )
    args = parser.parse_args()
    digest, sidecar = build(args.output)
    print(f"archive: {args.output.expanduser().resolve()}")
    print(f"sha256: {digest}")
    print(f"sha256 sidecar: {sidecar}")
    print(f"source files: {len(collect_sources())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
