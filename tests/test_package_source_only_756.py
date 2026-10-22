"""Release ZIP omits tests whose fixtures only exist in the source checkout."""
from __future__ import annotations

import importlib.util
import re
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ONLY_TESTS = {
    "tests/test_diagram_law.py", "tests/test_example_census_fanout.py",
    "tests/test_example_issue_to_pr.py", "tests/test_example_release_lifecycle.py",
    "tests/test_lane_recover_8edcc9bf.py", "tests/test_example_exchange_run.py",
    # merge-assist queue (est-0izi7): changelog.d pin, source-only scripts/changelog_assemble.py
    "tests/test_changelog_fragments_izi7.py",
    # #298 review (R8 sibling sweep): same shape, source-only lane_recover.py / make_public.py
    "tests/test_lane_recover_legacy_amend_237r4.py", "tests/test_respawn_hardening_723.py",
    "tests/test_crash_no_exit_respawn_718.py", "tests/test_lane_recover_cli_237r3.py",
    "tests/test_plugin_resolution_live_not_old.py", "tests/test_scrub_yml_166b.py",
    # rebased onto main's #235: same shape, executes source-only lane_recover.py
    "tests/test_stuck_node_finalize_733.py",
    # #248 r3 rebase onto main's #298: executes source-only make_public.py, reads scripts/scrub-list.txt
    "tests/test_personal_path_scrub_867.py",
    # est-xwvu9: the post-exit hook is stripped from the shipped build; this test
    # executes the source-only scripts/post_exit_hook.py (kept for our own dist path)
    "tests/test_post_exit_hook.py",
}
# A shipped test that loads/executes `<root> / "scripts" / "<file>"` needs that file in the ZIP.
SCRIPT_REF = re.compile(r'(?:ROOT|BUILD)\s*/\s*"scripts"\s*/\s*"([^"]+)"|ROOT\s*/\s*"scripts/([^"]+)"')


def main():
    spec = importlib.util.spec_from_file_location("pack_756", ROOT / "scripts/pack.py")
    assert spec is not None and spec.loader is not None
    pack = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pack)
    with tempfile.TemporaryDirectory(prefix="pack-756-") as tmp:
        archive = Path(tmp) / "release.zip"
        pack.build(archive)
        with zipfile.ZipFile(archive) as z:
            prefix = pack.PACKAGE_NAME + "/"
            members = {name.removeprefix(prefix) for name in z.namelist()}
        for path in sorted(SOURCE_ONLY_TESTS):
            assert path not in members, f"source-only test shipped without its inputs: {path}"
            print("PASS source-only test excluded:", path)
        for path in ("tests/test_packaging.py", "tests/test_package_source_only_756.py",):
            assert path in members, f"package regression gate omitted: {path}"
            print("PASS package gate still ships:", path)
        # Derived sweep: no shipped test may reference an unshipped scripts/ file.
        broken = []
        for path in sorted(m for m in members if m.startswith("tests/test_") and m.endswith(".py")):
            text = (ROOT / path).read_text(encoding="utf-8")
            for m in SCRIPT_REF.finditer(text):
                dep = "scripts/" + (m.group(1) or m.group(2))
                if dep not in members:
                    broken.append(f"{path} -> {dep}")
        assert not broken, "shipped tests need unshipped scripts:\n  " + "\n  ".join(broken)
        print("PASS no shipped test references an unshipped scripts/ file")
        reasons = getattr(pack, "SOURCE_ONLY_TESTS", {})
        assert set(reasons) == SOURCE_ONLY_TESTS, "each exclusion needs an explicit include decision"
        assert all(isinstance(reason, str) and reason.strip() for reason in reasons.values())
        print("PASS each source-only exclusion has a reason")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
