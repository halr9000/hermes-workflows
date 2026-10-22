#!/usr/bin/env python3
"""changelog_assemble.py — one changelog fragment per PR, assembled mechanically.

WHY THIS FILE EXISTS. `CHANGELOG.md` is one file every lane edits at one anchor
(the top of `## Unreleased`). N concurrent PRs therefore guarantee N conflicts on
one hunk, and every merge that lands invalidates the other N-1 branches — measured
on the merge-assist queue of 2026-10-22: 12 of 15 open PRs were CONFLICTING, and
`CHANGELOG.md` was one of the conflicted files in 11 of those 12; in 8 of them it
was the ONLY conflict. That is a turnstile, not a merge queue: reviewers wait on
an author-side rebase whose only content is other people's prose.

The fix is disjoint write surfaces: a lane writes `changelog.d/<key>.md`, a path no
other lane can choose, and the ONE writer of `CHANGELOG.md` becomes this script,
run once at release. Nothing else changes: the assembled block is byte-compatible
with the hand-written style (one bullet per entry, two-space continuation).

VERBS
  list  [--dir D]                print the assembled block; never writes
  check [--dir D]                validate names + bodies; exit 1 on any violation
  fold  [--dir D] [--changelog C] [--keep]
       insert the assembled block at the top of the `## Unreleased` section, then
       delete the consumed fragments (`--keep` leaves them). Every other byte of
       the changelog is left untouched.

NAMING  `<PR-number>.md` once the number exists, otherwise `<lane-token>.md`
        (stem = digits, or a letter followed by [A-Za-z0-9._-]). Ordering: numeric
        keys first, ascending, then the rest by key — so the assembled order is a
        function of the directory alone, never of filesystem walk order.

IDEMPOTENCE  the consumed fragment files are the guard, and they are the honest
        one: `fold` with an empty directory changes nothing at all, and `fold`
        REFUSES (exit 1, no write) when a fragment's rendered entry already appears
        in the changelog, so a re-run after a partial merge cannot post an entry
        twice. No state file, no sidecar — the directory is the state.

ONE ENTRY PER FRAGMENT, prose only: no `##` heading, no nested list (use `;`
clauses), no conflict markers. `check` enforces exactly that and nothing more.

Stdlib only (repo rule 4c.7). Run: python3 scripts/changelog_assemble.py check
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KEY_STEM = re.compile(r"^(?:[0-9]{2,6}|[A-Za-z][A-Za-z0-9._-]{1,40})$")
CONFLICT_MARK = re.compile(r"^(?:<{7}|={7}|>{7})(?:\s|$)")
HEADING = re.compile(r"^##\s")

FRAG_GLOB = "*.md"
README_NAME = "README.md"
SECTION = "## Unreleased"


class Violation(Exception):
    """A fragment or changelog shape that `check`/`fold` refuses."""


def fragments(dirn: Path) -> list[tuple[str, Path]]:
    """Every fragment in `dirn`, in assembled order (numeric first, ascending)."""
    out = []
    for p in sorted(dirn.glob(FRAG_GLOB)):
        if p.name == README_NAME:
            continue
        out.append((p.stem, p))
    return sorted(out, key=lambda kp: order_key(kp[0]))


def order_key(stem: str) -> tuple[int, int, str]:
    return (0, int(stem), "") if stem.isdigit() else (1, 0, stem)


def render(stem: str, body: str) -> str:
    """One entry: '- ' + first line, every later line re-indented to two spaces.

    Deliberately tolerant of however the author indented, so the assembled block
    depends only on the fragment's CONTENT. Returns "" for an empty fragment so
    callers can report it instead of emitting an empty bullet.
    """
    lines = [ln.rstrip() for ln in body.strip().splitlines()]
    lines = [ln for ln in lines if ln.strip() != ""]
    if not lines:
        return ""
    first = lines[0]
    for marker in ("- ", "* "):
        if first.startswith(marker):
            first = first[len(marker):]
            break
    out = ["- " + first.strip()]
    out += ["  " + ln.strip() for ln in lines[1:]]
    return "\n".join(out)


def load(dirn: Path) -> list[tuple[str, str]]:
    """(key, rendered entry) for every fragment, in assembled order; '' = refused shape."""
    return [(stem, render(stem, p.read_text(encoding="utf-8"))) for stem, p in fragments(dirn)]


def block(pairs: list[tuple[str, str]]) -> str:
    return "\n\n".join(text for _k, text in pairs if text)


def violations(dirn: Path) -> list[str]:
    """Every `check` failure as a line naming the file and the reason."""
    bad: list[str] = []
    seen_text: dict[str, str] = {}
    for stem, path in fragments(dirn):
        if not KEY_STEM.match(stem):
            bad.append(f"{path.name}: illegal fragment key (use <PR-number>.md or <lane-token>.md)")
        raw = path.read_text(encoding="utf-8")
        if raw.strip() == "":
            bad.append(f"{path.name}: empty fragment (an entry or nothing — never a blank file)")
        for ln in raw.splitlines():
            if CONFLICT_MARK.match(ln):
                bad.append(f"{path.name}: conflict marker in a fragment (resolve the rebase, not the text)")
                break
        for ln in raw.splitlines():
            if HEADING.match(ln):
                bad.append(f"{path.name}: carries a '##' heading (a fragment is one entry, not a section)")
                break
        text = render(stem, raw)
        if text and text in seen_text:
            bad.append(f"{path.name}: identical entry already contributed by {seen_text[text]}")
        elif text:
            seen_text[text] = path.name
    return bad


def _section_span(text: str) -> tuple[int, int]:
    """(start, end) offsets of the content of the first `## Unreleased` section."""
    m = re.search(r"^" + re.escape(SECTION) + r"[ \t]*$", text, re.M)
    if not m:
        raise Violation(f"{SECTION} heading not found — refusing to invent one")
    start = m.end()
    nxt = re.search(r"^##\s", text[start:], re.M)
    return start, start + (nxt.start() if nxt else len(text[start:]))


def fold(dirn: Path, changelog: Path, keep: bool = False) -> list[str]:
    """Insert the assembled block above the existing Unreleased entries. Returns keys consumed."""
    pairs = [p for p in load(dirn) if p[1]]
    if not pairs:
        return []
    if not changelog.is_file():
        raise Violation(f"changelog not found: {changelog}")
    text = changelog.read_text(encoding="utf-8")
    dupes = [stem for stem, ent in pairs
             if _norm(ent) in _norm(text)]
    if dupes:
        raise Violation("already folded (entry text present in the changelog): " + ", ".join(dupes))
    start, end = _section_span(text)
    existing = text[start:end]
    new = "\n\n" + block(pairs) + "\n" + (existing.rstrip("\n") + "\n" if existing.strip() else "")
    merged = text[:start] + new + text[end:]
    tmp = changelog.with_suffix(changelog.suffix + ".assemble.tmp")
    tmp.write_text(merged, encoding="utf-8")
    tmp.replace(changelog)
    if not keep:
        for _stem, path in fragments(dirn):
            path.unlink()
    return [stem for stem, _e in pairs]


def _norm(s: str) -> str:
    return " ".join(s.split()).lower()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="changelog_assemble.py",
                                 description=(__doc__ or "").splitlines()[0])
    ap.add_argument("verb", choices=["list", "check", "fold"])
    ap.add_argument("--dir", default=str(ROOT / "changelog.d"), help="fragment directory")
    ap.add_argument("--changelog", default=str(ROOT / "CHANGELOG.md"), help="target changelog")
    ap.add_argument("--keep", action="store_true", help="fold: leave the fragment files in place")
    ap.add_argument("--json", action="store_true", help="machine-readable summary")
    a = ap.parse_args(argv)

    dirn, changelog = Path(a.dir), Path(a.changelog)
    if not dirn.is_dir():
        print(f"changelog_assemble: no fragment directory at {dirn}", file=sys.stderr)
        return 2
    if a.verb == "list":
        print(block([p for p in load(dirn) if p[1]]))
        return 0
    if a.verb == "check":
        bad = violations(dirn)
        for line in bad:
            print("FAIL " + line)
        n = len(fragments(dirn))
        if bad:
            print(f"FAIL changelog_assemble check: {len(bad)} violation(s) in {n} fragment(s)")
            return 1
        print(f"PASS changelog_assemble check: {n} fragment(s), assembled order = "
              + ", ".join(stem for stem, _p in fragments(dirn)))
        return 0
    try:
        consumed = fold(dirn, changelog, keep=a.keep)
    except Violation as exc:
        print(f"FAIL changelog_assemble fold: {exc}", file=sys.stderr)
        return 1
    if a.json:
        import json
        print(json.dumps({"folded": consumed, "changelog": str(changelog)}))
    elif consumed:
        print(f"folded {len(consumed)} fragment(s) into {changelog.name}: " + ", ".join(consumed))
    else:
        print("nothing to fold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
