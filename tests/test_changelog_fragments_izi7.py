#!/usr/bin/env python3
"""changelog.d fragments: the disjoint-write-surface contract (merge-assist queue, est-0izi7).

Why this pin exists: `CHANGELOG.md` was the single hunk every open lane edited, which
made concurrent PRs conflict on prose alone (2026-10-22 queue read: 12 of 15 open PRs
CONFLICTING, `CHANGELOG.md` conflicted in all 12, sole conflict in 8). The cure is that
a lane writes `changelog.d/<key>.md` instead, so the collision is unconstructible. This
file pins the mechanics the cure rests on — determinism, the refusals, and the one
property that makes the fold safe to run at 03:00Z by anyone: **it cannot double-post**.

Run: python3 tests/test_changelog_fragments_izi7.py
"""
from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "changelog_assemble.py"

spec = importlib.util.spec_from_file_location("clgwd_under_test", SCRIPT)
assert spec is not None and spec.loader is not None
clg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(clg)

fails = total = 0


def check(label: str, cond: bool, detail: object = "") -> None:
    global total, fails
    total += 1
    print(("PASS " if cond else "FAIL ") + label + ("" if cond else f"  {detail}"))
    fails += 0 if cond else 1


def tree(files: dict[str, str], changelog: str | None) -> Path:
    """A scratch dir holding changelog.d/<files> and an optional CHANGELOG.md."""
    root = Path(tempfile.mkdtemp(prefix="clgwd-"))
    (root / "changelog.d").mkdir()
    for name, body in files.items():
        (root / "changelog.d" / name).write_text(body, encoding="utf-8")
    if changelog is not None:
        (root / "CHANGELOG.md").write_text(changelog, encoding="utf-8")
    return root


CHANGELOG = ("# Changelog\n\n"
             "## Unreleased\n\n"
             "- old entry, hand-written, must survive verbatim\n"
             "  with its continuation line\n\n"
             "## 1.4.0\n\n"
             "- shipped entry\n")

# --- 1. ordering is a function of the directory, not of walk order ---------------
d = tree({"531.md": "feat: five hundred thirty one\n",
          "99.md": "fix: ninety nine\n",
          "clgwd.md": "chore: lane token key\n",
          "ab1.md": "docs: earlier token\n",
          "README.md": "# the law, not a fragment\n"}, CHANGELOG)
keys = [k for k, _p in clg.fragments(d / "changelog.d")]
check("order: numeric ascending first, then lane tokens by key",
      keys == ["99", "531", "ab1", "clgwd"], keys)
check("README.md is documentation, never a fragment", "README" not in keys)
check("check() passes on a clean directory", clg.violations(d / "changelog.d") == [],
      clg.violations(d / "changelog.d"))
shutil.rmtree(d)

# --- 2. rendering owns the bullet and the continuation ---------------------------
one = clg.render("7", "- fix(door): the thing\n    wrapped at four spaces\n")
check("render: one bullet, two-space continuation, author indentation normalized",
      one == "- fix(door): the thing\n  wrapped at four spaces", repr(one))
check("render: an empty fragment renders to nothing", clg.render("7", "\n  \n") == "")

# --- 3. refusals -----------------------------------------------------------------
BAD = {
    "empty.md": ("\n   \n", "empty"),
    "heading.md": ("feat: x\n\n## Unreleased\n\n- no\n", "heading"),
    "clash.md": ("feat: y\n<<<<<<< HEAD\nours\n", "conflict marker"),
    "-nope.md": ("feat: z\n", "illegal fragment key"),
}
for name, (body, why) in BAD.items():
    d = tree({name: body}, CHANGELOG)
    v = clg.violations(d / "changelog.d")
    check(f"check() refuses {name} ({why})", any(why in line and name in line for line in v), v)
    shutil.rmtree(d)
d = tree({"dup-a.md": "feat: same words twice\n", "dup-b.md": "feat: same words twice\n"}, CHANGELOG)
v = clg.violations(d / "changelog.d")
check("check() refuses two fragments rendering to one entry",
      any("identical entry" in line for line in v), v)
shutil.rmtree(d)

# --- 4. fold: keep-both, region-scoped, consuming --------------------------------
d = tree({"501.md": "feat: first\n  detail line\n", "502.md": "fix: second\n"}, CHANGELOG)
consumed = clg.fold(d / "changelog.d", d / "CHANGELOG.md")
text = (d / "CHANGELOG.md").read_text()
check("fold: reports both keys consumed", consumed == ["501", "502"], consumed)
check("fold: fragments are deleted (that deletion is the idempotence guard)",
      sorted(p.name for p in (d / "changelog.d").glob("*.md")) == [],
      sorted(p.name for p in (d / "changelog.d").glob("*.md")))
check("fold: assembled entries land above the previous entries",
      text.index("- feat: first") < text.index("- old entry"), text[:400])
check("fold: hand-written history below the region is byte-identical",
      "- old entry, hand-written, must survive verbatim\n  with its continuation line" in text
      and "## 1.4.0\n\n- shipped entry\n" in text, text)
check("fold: heading keeps exactly one blank line after it", "## Unreleased\n\n- feat: first" in text)
check("fold: nothing above the heading moved", text.startswith("# Changelog\n\n## Unreleased"))
shutil.rmtree(d)

# --- 5. the safety properties ----------------------------------------------------
d = tree({}, CHANGELOG)
before = (d / "CHANGELOG.md").read_text()
check("fold over an empty directory is a true no-op",
      clg.fold(d / "changelog.d", d / "CHANGELOG.md") == []
      and (d / "CHANGELOG.md").read_text() == before)
shutil.rmtree(d)

d = tree({"503.md": "feat: fold me once\n"}, CHANGELOG)
clg.fold(d / "changelog.d", d / "CHANGELOG.md")
(d / "changelog.d" / "503.md").write_text("feat: fold me once\n")   # e.g. restored by a bad rebase
folded_once = (d / "CHANGELOG.md").read_text()
try:
    clg.fold(d / "changelog.d", d / "CHANGELOG.md")
    refused = False
except clg.Violation as exc:
    refused = "already folded" in str(exc)
check("fold refuses to post an entry that is already in the changelog", refused)
check("the refusal wrote nothing", (d / "CHANGELOG.md").read_text() == folded_once)
shutil.rmtree(d)

d = tree({"504.md": "feat: nowhere to go\n"}, "# Changelog\n\n## 1.4.0\n\n- shipped\n")
try:
    clg.fold(d / "changelog.d", d / "CHANGELOG.md")
    refused = False
except clg.Violation as exc:
    refused = "refusing to invent" in str(exc)
check("fold refuses a changelog with no Unreleased section rather than inventing one", refused)
check("that refusal wrote nothing either",
      (d / "CHANGELOG.md").read_text() == "# Changelog\n\n## 1.4.0\n\n- shipped\n")
shutil.rmtree(d)

d = tree({"505.md": "feat: kept\n"}, CHANGELOG)
clg.fold(d / "changelog.d", d / "CHANGELOG.md", keep=True)
check("fold --keep leaves the fragment for the next run", (d / "changelog.d" / "505.md").is_file())
shutil.rmtree(d)

# --- 6. the CLI is the seam a lane and a release both use ------------------------
d = tree({"506.md": "feat: via cli\n"}, CHANGELOG)
r = subprocess.run([sys.executable, str(SCRIPT), "check", "--dir", str(d / "changelog.d")],
                   capture_output=True, text=True)
check("CLI check exits 0 and names the assembled order",
      r.returncode == 0 and "506" in r.stdout, r.stdout + r.stderr)
r = subprocess.run([sys.executable, str(SCRIPT), "fold", "--dir", str(d / "changelog.d"),
                    "--changelog", str(d / "CHANGELOG.md"), "--json"],
                   capture_output=True, text=True)
check("CLI fold reports the keys it consumed",
      r.returncode == 0 and '"506"' in r.stdout, r.stdout + r.stderr)
shutil.rmtree(d)

# --- 7. mutation self-proof: every direction above actually bites ---------------
d = tree({"507.md": "feat: mutation target\n"}, CHANGELOG)
(d / "changelog.d" / "507.md").write_text("feat: mutation target\n>>>>>>> theirs\n")
check("self-proof: a conflict marker flips check() to nonzero",
      subprocess.run([sys.executable, str(SCRIPT), "check", "--dir", str(d / "changelog.d")],
                     capture_output=True, text=True).returncode != 0)
(d / "changelog.d" / "507.md").write_text("feat: mutation target\n")
clg.fold(d / "changelog.d", d / "CHANGELOG.md")
(d / "changelog.d" / "507.md").write_text("feat: mutation target\n")
check("self-proof: a re-fold of the same text is refused, not duplicated",
      subprocess.run([sys.executable, str(SCRIPT), "fold", "--dir", str(d / "changelog.d"),
                      "--changelog", str(d / "CHANGELOG.md")],
                     capture_output=True, text=True).returncode != 0)
check("self-proof: the tree under test ships clean today",
      subprocess.run([sys.executable, str(SCRIPT), "check"], capture_output=True,
                     text=True).returncode == 0)
shutil.rmtree(d)

print(("ALL PASS" if fails == 0 else f"{fails} FAILED") + f": changelog.d fragments ({total} checks)")
sys.exit(1 if fails else 0)
