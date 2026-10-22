# `changelog.d/` — one changelog fragment per pull request

**The law:** a contribution never edits `CHANGELOG.md`. It adds one file here.
The release commit is the only one that touches `CHANGELOG.md`, and it does so by
running the assembler, not by hand.

```sh
python3 scripts/changelog_assemble.py check          # what a lane runs before it pushes
python3 scripts/changelog_assemble.py list           # preview the assembled block
python3 scripts/changelog_assemble.py --json fold    # release commit: insert + consume
```

## Why this shape, and not a bigger changelog

`CHANGELOG.md` is a single file whose newest entry always goes in the same place.
Concurrent lanes therefore collide on one hunk no matter what they change, and each
merge invalidates every other open branch. Measured on the merge-assist queue of
2026-10-22 (`git merge-tree --write-tree --name-only origin/main <head>` per PR):
**12 of 15 open PRs read CONFLICTING; `CHANGELOG.md` was among the conflicted files in
11 of those 12; in 8 of them it was the only conflicted file.** The twelfth
(`#490`, one test file) is the counterexample that keeps the claim honest — fragments
fix the changelog collision, not every collision. A PR that merged at 11:55Z flipped a
clean sibling to CONFLICTING by 11:59Z. Every one of those conflicts was rebased away
by an agent re-copying prose that another agent had already written.

Per-PR fragments make the write surface **disjoint**: two lanes cannot choose the same
path, so the collision is not mediated better — it is unconstructible. The cost is one
file per PR and one mechanical fold at release. The `## Unreleased` block a reader sees
is byte-compatible with the hand-written style it replaces.

## Naming

| File | Use it when |
|---|---|
| `changelog.d/531.md` | the PR number exists — number the fragment after your own PR |
| `changelog.d/clgwd.md` | the branch is cut before a number exists — use the lane token |

The number is only known after the PR is opened, so the expected sequence is: push the
lane, open the PR, then add or `git mv` the fragment to the number in a second commit.
Renaming a fragment is free — it is the file's key, not its content.

`README.md` in this directory is documentation, not a fragment: the assembler skips it.

## Ordering

Numeric keys sort first, ascending; lane-token keys follow, by key. The assembled order
is a function of the directory alone, so two runs over the same tree agree byte-for-byte.

## Entry shape

One entry per fragment. Prose only:

* no `##` heading (a fragment is an entry, not a section);
* no nested list — use `;` clauses, the assembler owns the bullet and the two-space
  continuation, and it normalizes either indentation style you happen to write;
* no conflict markers (if you are resolving a rebase, resolve the rebase).

`check` refuses the above, refuses an empty fragment, and refuses two fragments that
render to the same entry.

## At release

The release commit runs `fold`: entries land above the existing `## Unreleased` content,
the consumed fragments are deleted (that deletion is the idempotence guard — a second
`fold` over an empty directory changes nothing, and a fragment whose text is already in
the changelog is refused rather than duplicated), and the section heading is then renamed
to the version being cut, per the release recipe.

`fold` touches the `## Unreleased` region and no other byte of the changelog. History
above it, including every entry written by hand before this directory existed, is left
as found.
