feat(changelog): `changelog.d/` fragments — one file per PR, assembled at release

Contributors no longer edit `CHANGELOG.md`; a lane adds `changelog.d/<PR-number>.md`
(or `changelog.d/<lane-token>.md` before a number exists) and the release commit folds
the directory into `CHANGELOG.md` with `python3 scripts/changelog_assemble.py fold`
(verbs: `list` preview, `check` validate, `fold` insert-and-consume; stdlib only, no
state file — the fragments themselves are the state, and their deletion is what makes
the fold non-repeatable). Why: `CHANGELOG.md` was one hunk every open lane edited, so
concurrent PRs conflicted on prose alone — on 2026-10-22 the queue read 12 of 15 open
PRs CONFLICTING, `CHANGELOG.md` among the conflicted files in 11 of those 12 and the
sole conflict in 8, and one merge at 11:55Z flipped a clean sibling dirty by 11:59Z.
Per-PR fragments make the
write surface disjoint, which removes that class of conflict instead of mediating it.
`CHANGELOG.md` is untouched by this PR, deliberately: it is the demonstration. Pinned
by tests/test_changelog_fragments_izi7.py (ordering determinism, the four refusals,
region-scoped keep-both fold, and the double-post refusal with a mutation self-proof).
