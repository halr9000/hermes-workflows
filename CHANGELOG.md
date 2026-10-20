# Changelog

## Unreleased

- chore(bead_close): per-item `close-pending` escape folded into TOKEN-FREE
  fan-out decls (est-nbe2y, pre-existing from est-13z1; found reviewing #486).
  The node-scoped whole-blob search let ONE lane's honest `close-pending` —
  or the phrase appearing in any non-committing row — exempt the whole
  aggregate from store read-back while sibling done/partial lanes served the
  very claim the gate exists to verify: the 13z1 false-green shape at fan-out
  scale. The escape is now per item: any committing item without its own
  pending token forces the (still single, node-scoped) probe, and only an
  ALL-pending aggregate commits pristine with zero probes. The single-probe
  shape is kept (singular receipt key, item rows untouched, one probe) —
  `test_f` stands; pins in `tests/test_bead_close_fanout_485.py` (j, k) and
  `references/grammar.md` (bead_close).

- fix(contract): between-turns wake is the delivery DEFAULT; the wait loop was
  demoted to what it always mechanically was — optional blocking observation and
  the revive verb. Operator-facing truth audit (host ledger, ~40 runs): the
  owner-session wake self-post lands (verified injected as a user turn in the
  owner session dump), yet SKILL.md ordered agents to sit in the `wait` loop
  ("ending your turn right after run is how runs stall") and every door hint
  called wake "optional" — so agents blocked the originating session while
  delegate_task's children re-entered. Changes: `run` hint states the wake
  contract and says END YOUR TURN; `release`/`amend` hints stop pointing at
  blocking first; the action schema marks `wait` OPTIONAL; derived `next` rows
  for a live run carry a `wake` note when (and only when) run.json stamps an
  owner.session_id; SKILL.md Smallest-working-graph, next-law and delivery
  bullets rewritten async-first (fail-open honesty kept: `wake.jsonl` ledger,
  status-on-resume after a host restart). Follow-up issue: wake retry + silent
  `interrupted` class.

- feat(bead_close): per-item `{FIELD}` interpolation on fan-out nodes (#485,
  estate excavation E-9/E-11). A fan-out bead-worker lane — one item = one
  estate bead — can now declare `bead_close.id` (and `.actor`) as ITEM
  TEMPLATES: bare `{FIELD}` tokens (the exact law fan-out goals pay; `{item}`
  and `{index}` resolve too) render per item at the commit edge and EVERY
  committing item is read back from the store at its own rendered id. The
  measured row rides each item's record inside `all_results` as
  `bead_close_receipt`; the full indexed enumeration rides the node record
  under the plural `bead_close_receipts`. The `close-pending` honest escape
  is per item; unrenderable templates (empty / whitespace / surviving braces)
  fail that item typed and are never probed. The door refuses the tokens on
  any non-fan-out node, and a token-free declaration keeps the node-scoped
  single-probe shape byte-identical. Tests: `tests/test_bead_close_fanout_485.py`.

- feat(door): `maxItems` joins the admitted schema subset as `minItems`' mirror
  and `enum` widens onto `boolean`/`integer` (#472, est-v2fr6). Fan-out rosters
  (arena candidates 2-6, swarm items 2-8) and bug-fix gate-keys
  (`reproduced: enum[true,false]`) can now self-enforce instead of degrading to
  prose. `wf.validate()` enforces the cap (`"{path}: expected at most N
  item(s)"`) and typed membership — bool never satisfies an integer enum, an
  integral float may hit its integer rung; an unsatisfiable `minItems >
  maxItems` pair is refused at the door; the unsupported-keyword refusal names
  the supported set AND the `description`-prose fallback for still-unenforceable
  constraints (integer bounds, patterns). Tests:
  `tests/test_schema_max_472.py` (+ re-pinned #96/#107 canaries).

- docs(v1.4.0): revitalization pass — README drift fixes (`/wf <name>` returns
  a run instruction, `/wf gates` behind `admission_gates`, `library name:` /
  `amend patch` forms documented), four spec-backed showcase diagrams under
  `docs/pr-diagrams/` with `docs/showcase.md` glosses; removed
  `docs/event-driven-wake.md` (unreferenced pattern essay; history in git).
  (est-7m6wv)
- feat(spawn): marker-gated creation-override flag strip (est-yq2xq). On seats
  whose gateway freezes the session route at create, `--continue <skey>
  --create-if-missing` + any creation override is refused (child dies rc=2,
  whole waves die). The runner now strips the frozen-route flag pairs
  (`--max-turns`, `--run-budget`, `--reasoning`, `-t`, `-m`, `--provider`)
  when `WF_OMIT_POLICY_FLAGS=1` or the marker
  `<hermes_home>/workflows/omit-policy-flags.on` exists — children ride seat
  defaults and per-node pinning is unavailable while on; delete the marker /
  unset the env to return byte-identical stock argv. This replaces the
  estate-side monkey-patch that every plugin update wiped. Contract in
  `references/operations.md`.
- `tests/test_mechanic_admission_gate_104.py`: the issue-#104 admission gate as
  an executable check — L1 drives a mechanic-shaped probe graph through the
  real runner with the recording fake door and asserts deny-default
  auto-release on `hold_timeout`, arm self-no-op, and audit-shape gate skips
  with zero arm spawns; L2 pins the blind-holdout ledger (run.json.status
  blinded, sealed-key digests, battery counts as measured ints); L3 becomes
  binding on any vendoring PR via `WF_MECHANIC_CANDIDATE` (engine validation
  in admission mode, deny-named defaults, single-gate arm dependencies).
  Battle cards in `references/mechanic-battle-cards.md`; ledger in
  `receipts/workflow-mechanic/admission-104.json` (est-t3m6).

- `scripts/wofify.py`: the WOFification authoring-time seam — `inventory`
  (path-shape census of a way-of-working repo), `scaffold` (never-clobber pack
  skeleton), `validate` (pack graphs compiled and measured by the door's own
  validator; card sidecars skipped; zero graphs is never green), `shelf`
  (adoption script with sha256 drift refusal). Not a door action, like the
  vaporized `wf_dialect.py` it mirrors; contract in `references/wofification.md`
  (est-h0huv).

## 1.3.9 — 2026-10-17

Patch release: 43 merged PRs since v1.3.8 — runner hardening (zombie-reap
settle, attempt history, launcher pins, workdir grant), seat admission
(FIFO queue, pool-recorded cap), wake catch-all, node-targeted kill, token
run-budget cap, the session workflow tray and the public starter library.

### Merged PRs (all via the maintainer account)

- fix(door): no placeholder mints a phantom run_context; s12 warn rows on validate; library show (est-kg3y) (#421)
- test(ratelimit): spawn-proportional W1 wall bound + per-pid fixture (est-qp8dj) (#420)
- fix(process-tree): report unreadable quiescence as unknown (est-cbbc) (#384)
- docs: harden amend, read-model and lane rules from estate operation history (#426)
- fix(post-exit): write launching receipt before first launch attempt (est-fdulo b) (#434)
- feat(gates): zero-turn gate answers on the admission lane (+dispatch witness observer) (#429)
- feat(library): continuous discovery-time structural admission (est-2ek.1.828) (#415)
- fix(wf): harvested max_turns partial reaches the bounded layer-2 re-drive; per-node harvest_policy clamp (est-ggqs, #256 claim 1) (#422)
- feat(library): curated public library/general/ starter set + door packaged fallback (#51) (#423)
- feat(validate): warn on below-shape turn budgets and document lane exits (#256) (#381)
- feat(door): s12 advisory nudge for single-char/vowel-only node ids (est-6ihr6) (#405)
- feat(seats): FIFO queue admission + validated operator cap knobs (est-2ek.1.856/857) (#324)
- fix(#44): child-probe tri-state runner.lock liveness + safe heal of the wedged runner.lock (N30 re-port; supersedes #271) (#439)
- feat(runner): declared bead_close read-back gate at the commit edge (est-13z1) (#360)
- feat(cards): hook-led card ledger replaces the per-turn runs-root scan (lane 2, est-w86l0) (#440)
- fix(scrub): re-anchor stale est-13z1/est-q2nwx guards by feature token (main red at ee5018d) (#442)
- feat(orphan-held): session-lifecycle observers name runs whose owner died at a gate (lane 4, est-w86l0) (#441)
- fix: distinguish external signal death from quorum cancellation (#380)
- feat(hooks): p1 / outbound (config-gated, default-off) (#432)
- test(833): seat_wait pass-through assertion is wall-clock-tick immune (est-b9dtf) (#444)
- fix(toolsets): refuse declared-empty selection at graph load; warned fail-closed at the seam (#427) (#446)
- feat(seats): pool-recorded cap arbitrates admission; two-runner combined cap (est-mg5o2) (#425)
- feat(desktop): session workflow tray above the composer (#233)
- fix(door): explicit-provider seat alias pings the alias target id (est-55z04) (#437)
- fix(read-model): supersede stale fan-out spawn facts after aggregate done (est-zvc7u) (#376)
- feat(validator): admit agent when/on_skip through the bounded grammar (#132 slice 2) (#383)
- perf(fingerprint): memoize the efp DAG walk — one def_hash per node, not per path (#433)
- fix(runner): timed-out attempt's corpus+log survive verbatim into next attempt (est-hls8y) (#436)
- feat(workflow): node-targeted lane kill — action=kill (est-cl7t) (#438)
- fix(runner): run_child grants the child workdir on HERMES_WRITE_SAFE_ROOT_EXTRA, a channel core cannot replace (est-2mhl) (#445)
- feat(wake): class-coverage registry + catch-all waker (est-jbcrw) (#449)
- fix(seats): seat cap authoritative at spawn — stale inherited WORKFLOW_MAX_SEATS loses to the fresh .env read (est-mg5o2) (#451)
- fix(read-model): live runner's foreign-pid-namespace child reads running, not pending (est-2ek.1.818) (#297)
- feat(pin-delta): fail-closed host-effects gate for the vendor/catalog re-pin lane (est-zee7e) (#329)
- fix(runner): park's attempts history survives the transport ladder (est-ja41) (#353)
- fix(pack): the release ZIP runs the tests it ships — ship fake-b1, git-free fallback for the 954r pin (est-1nkx) (#358)
- fix(policy): forbid equivalent Anthropic route spellings (est-e0yqy) (#375)
- scripts: horde_gen.py — wave-generator successor, W19 defect fixes 2-5 (est-a5obt) (#414)
- fix(runner): zombie/empty-tree counts as reaped before the #61 fail-close; drained-during-check never dead-letters (#448) (#450)
- fix(runner): recover missing installation launcher pins (#379)
- fix(amend): name every RUNNING node an amend resets (#366) (#390)
- feat(drift): offline checkout probe (#125 item 3, est-b994) (#382)
- feat(run-budget): tokens-only pre-dispatch cap + graceful budget_exhausted partial (est-3acn) (#435)

### Notes

- **Wake-router class coverage + catch-all (est-jbcrw, dead-landing census
  verdict 4):** `notify()` can no longer return silently for an event outside
  `_WAKE_TEMPLATES`. A closed registry `_WAKE_EVENTS` names every event a call
  site may pass; an unmapped class wakes on a fixed runner-authored catch-all
  body (AUTHORITY LAW r5 unchanged — the text table is still the only author,
  the key rides only as the Idempotency-Key), the probe row is typed
  `catchall`, and the runner prints `WORKFLOW_WAKE_UNMAPPED_EVENT` on stdout so
  a class gap is loud in the field. The old fail-silent rule let scripted wakes
  for unmapped classes rot (5 dead rows attributed in the census). Pinned by
  `tests/test_wake_catchall_jbcrw.py`.

- **Zombie-reap settle at the #61 gate (#448):** a spawn caught mid-reap no
  longer reads as a live descendant. The commit-edge quiesce honors its own
  dead-or-empty contract (`proof == ""` returns the record untouched — a
  banked-complete node commits through harvest-on-death instead of dead-lettering
  under a manufactured `left_live_descendants`; the nightly #427 lane lost a
  complete answer to exactly this race, where the guard quoted the live tree as
  `[]`). Every stuck verdict now rides a bounded settle (2 re-checks x 0.5 s,
  waitpid first): a /proc row that vanishes with an ESRCH re-ask, or an empty
  `ps` row, is reaped-class death; fail-closed stands only for pids still
  non-zombie live after the settle, and the g255 unknown-state law is untouched.
- **Operator scar-laws port (docs-only):** fifteen generalizable lessons from a
  retiring usage skill fold into the docs they belong to, one hardened line
  each — twelve authoring and recovery laws in `references/operator-playbook.md`
  (machine-gate probe polarity, error-but-launched dedupe, amend re-queue
  fingerprint law, cold-start re-runs, index-not-proof premises, no
  maker-authored green, seat-wait disposition, census-never-quorum, closed
  verdict enums, decorative-gate ban, control-plane credential containment,
  parser-first recount diagnosis), two run-lifecycle laws in
  `references/operations.md` (frozen launch meta, ephemeral run dirs), and the
  launch-agnostic reusable-graph law in `references/grammar.md`. No engine
  change.

Desktop:
- feat(desktop): est-z717 — the live-run TRAY is an opt-in surface: it
  renders ONLY when `plugins.entries.hermes-workflows.settings.tray` is
  literal `true` (GET /settings, `wfcommon.owner_setting` — fail-closed
  default OFF while the native tray SDK area hermes-agent#133724 is
  unlanded; when it lands, default-on rides that mount). The PillRail stays
  the always-on surface, untouched by the gate. Door side: `_owner_stamp()`
  honors the estate-shape law — an estate/dispatcher launcher sets
  `WF_OWNER_SESSION=''` to stamp a NULL owner (pane-only, blank-owner honest
  absent — a lane never impersonates the dispatcher's chat); a non-empty
  override stamps that session with ui/platform null; unset keeps interactive
  chats byte-identical. Zero-directive capture pinned end-to-end: a door
  run-create under `HERMES_SESSION_ID` joins that chat's rail with NO
  `::workflow` output. Tests: `tests/test_settings_route_z717.py`,
  `tests/test_owner_session_override_z717.py`,
  `tests/test_z717_zero_directive_capture.mjs`, gate pins in
  `test_run_tray`/`test_session_tray`.

- feat(desktop): session workflow tray above the composer (#230) — the
  running-only accumulator stacks beside the pill rail on the
  feature-detected composer mount (tray ?? underside ?? top). Collapsed
  default is ONE slim row `[chevron] N running workflows` (pictured density,
  inline styles over the CSS-var tone table, in-memory `$trayOpen` atom);
  expand-in-place renders one THICK row per live run (name, status dot,
  nodes done/total + mini progress bar, current node, gate-question snippet
  while held, wall-clock elapsed); a row click accordion-opens the SHARED
  MiniGraph for THAT run and `open ↗` escalates to the pane. Terminal runs
  leave the tray at once — no ack ledger, no 60 s recap. The pill rail stays
  mounted with `suppressGates` (exactly ONE GateActions per held run). When
  neither focused-chat atom answers, the tray derives its sid from the first
  live run's own owner stamp (honest absence preserved). Mirror
  `desktop/src/components/runtray.mjs` grew real bodies; the parity test is
  the leash. Spec: `tests/test_session_tray.mjs` (red-on-base, green-on-head).

## 1.3.8 — 2026-10-14

Patch release for P01/P02 (#418), preserving the v1.3.7 runner, patch/diff,
card-hook safety and retired baseline/navigation behavior.

- **Factual workflow guidance:** launch, wait and completion messages describe
  observed state without granting new authority. Cards and curation are optional;
  waiting can resume interrupted work. Static validation remains distinct from
  route-probing launch dry runs.
- **Read-only execution clocks:** a shared snapshot and bounded ledger index
  project observed timing into status and dashboard responses using existing
  spawn/item records. Missing evidence and dynamic totals stay explicitly unknown.
- **Best-effort boundary:** equal-timestamp PID reuse is not complete attempt
  provenance. No new scheduling, cancellation, authorization or persisted producer
  contract is introduced.
- Regression coverage includes malformed/missing evidence, stale identity,
  fractional timestamps, retries and fan-out joins. S01 is not included.

## 1.3.7 — 2026-10-14

Patch line, sixteen merged PRs since v1.3.6 — the runner's deepest hardening
wave yet (zombie hygiene, typed silent-death receipts, fail-safe wall events),
a door amend/diff surface, effect-gated publication, and two audit-mandated
regimes retired. Every `git log v1.3.6..HEAD` row re-derives from the merge
commits; authorship of record is listed under **Attribution** below.

**Runner and engine**

- Continuous zombie hygiene for the subreaper pool (#283, @atbrace)
- External jobs are registered and silent deaths reap typed receipts (#394)
- Once-only graph `on_exit` postmortem (#129 → #391)
- `resume_hint`: dead-attempt artifact inventory on every re-run path (#130 → #284)
- Read model: fail-safe wall events and one snapshot per poll (#385)
- Null-quorum fan-out death now names the real cause (#410)
- Publication nodes gate `run.done` on external-effect receipts (#56 → #403)

**Door and graph**

- Patch surface for amend plus `action=diff` confirm surface (est-2del → #412)
- PR-owned source deletions permitted without graph rewrites (est-i24i → #378)
- Regen normalizes `.graphify_root` to contract `'.'` (est-0an45 → #409)

**Hardening and tests**

- Card-hook abandon-safety and cheap `register()` for the upstream v0.21.5
  plugin contract (#304)
- Behavioral error-class coverage and dead-fixture cleanup (est-pbwn → #377)

**Retirements (audit)**

- The mandatory all-green `ci-baseline` registry is retired (audit M01 → #351);
  admission now compares against exits measured on the merge-base
- The committed code-navigation regime is retired (audit M02 → #319, @atbrace)

Routine graph refreshes ride along (#408, #404).

Attribution — all sixteen merges are `Author: agent` trailers on the squash
commits; the GitHub actor of record for every one is the org owner account,
with @atbrace co-authoring the two peer-estate PRs (#283, #319).

- #283 — @atbrace.
- #319 — @atbrace.

## 1.3.6 — 2026-10-11

Patch line, four merged PRs since v1.3.5 — #396 (the ponytail truth pass over
the docs and the comments), #399 (the child-prompt line the upstream catalog
review asked for), #354 (the stale-graph spawn guard), #395 (the compliance
standard's C8 teeth). The docs are the contract agents code against, and
every rotted pointer is an agent lap burned on a wrong map:

- **Node-type count corrected** — README and `references/grammar.md` said three
  (`agent|gate|echo`); `join` is the shipped fourth since the 1.3.0 wave (#396).
- **`reasoning` runtime-subset law updated to est-flah** — a relay's own enum-400
  list triggers ONE logged clamp to the nearest supported level (never escalates)
  and a re-drive; `provider_400` stands only when nothing clamps (#396).
- **Grammar made complete** — `defaults` and the agent-field lists gain
  `fallback_models` and `tier`; the `when` literal set gains `None`; the door's
  run/amend description matches `DEFAULTS_KEYS` (#396).
- **Dead pointers swept** — five line cites into core re-pointed (runtime_provider,
  api_error_summary, main.py marker, GRAPH_MAX_BYTES, steer-var bake/reader sites),
  the GRAPH_REPORT census pointer aimed at its `## Summary` block, four rot-prone
  line cites replaced by symbol names, a dead doc anchor and a duplicated
  `{runs,total,counts}` removed (#396).
- **Child prompt carries no repackaging instruction** (#399, upstream catalog
  ask 2) — `WORK_DIR_NOTE` no longer tells `-q` children to write refused
  inline-interpreter payloads into script files; the refusal is core's
  (`approvals.single_query_mode`), now documented in SECURITY.md as the
  operator's choice. Guard test inverted to assert ABSENCE; golden re-captured,
  prompts differ ONLY by the deleted sentence.
- **A queued spawn never launches a stale definition** (#354, est-byft8) — a
  spawn that queued on the global agent seat and reaches the Popen instant
  AFTER an amend moved its effective fingerprint is refused typed
  `stale_graph`; the comparison is the replay-skip efp law itself (a
  timeout/max_turns-only amend never refuses), the refusal releases its seat
  ticket, and the wave boundary re-drives the node under the CURRENT
  definition — the replay-skip promise held across a queueing window.
- **pr-compliance v0.3 C8** (#395) — a PR carrying a diagram must now lead its
  body with a prose `## Problem` (or User story/Why/Context) section, RED
  without it; the exemplar carrier `docs/pr-diagrams/est-g43qp` draws the
  broken lane above the fixed lane so the picture alone reviews 90/90.
- **Comments that lied, told true** — `_typed_error_class` returns `cap_exhausted`
  (the docstring predated the rename); the rate-limit cap quadruples, it never
  "doubles-ish"; the settings-cache shape comment matches the stored tuple;
  `post_exit_hook` docstrings now say the runner stopped dispatching it at the
  est-xwvu9 strip — tests exercise it, the shipped build never does (#396).
- **Prompt honesty** — `WORK_DIR_NOTE` drops the single-query repackaging
  instruction that the engine never honored (catalog review 2, #399).

## 1.3.5 — 2026-10-11

Patch line, sixteen merged PRs since v1.3.3 (supersedes the untagged 1.3.4 prep
#364 — everything below ships in this single cut):

- **Heartbeat verdict per running node** — the read model now reports each live
  node's verdict, plus a one-line `run explain` (est-zsws) (#352).
- **CI diagram-check diff-scoped** — the check now fires only when a PR ADDS the
  carrier PNG, so body-only PRs stop burning red compliance cycles (#373).
- **Core-contract drift probe** — a test asserts plugin-registered hooks against
  core `VALID_HOOKS` (nightly drift probe) (#365).
- **Chore:** dropped the stale lowercase `.github/pull_request_template.md`
  shadowing the real template (#368).

- **Unconditional `plugin_version` stamp** — every new `run.json` reports the arming
  door's own truth (plugin.yaml via `wfcommon.plugin_version()`), not only runs that
  declared `requires_plugin`; old dirs without the key still load (derive-only, honest
  absence) (#356).
- **Agent nodes honor `when` + `on_skip`** — #132 slice 1: a node whose `when` predicate
  is false skips with its declared `on_skip` outcome instead of running (#355).
- **Declared effect receipts gate publication-node `run.done`** — a publication node
  cannot report done without its declared receipts (#346).
- **Opt-in graph-revision CAS** on door `amend`/`release`/`steer` — stale revisions
  refuse instead of clobbering (#344).
- **Security:** the `post_exit_hook` mechanism is stripped from the shipped build
  (upstream NousResearch/hermes-agent#133387 accepted ask) (#334).
- **`graph_regen` fix:** shared `\0`-separated `--name-status` parser for the regen
  diff (est-h3yi) (#314).
- **Rate-limit-park wall pins** — M2/M3 fanout-respawn wall assertions plus mutation
  proofs (est-xodi) (#339).
- **Docs:** portable pre-share validation must pass with `admission=True` (closes
  est-73vk) (#341); pack card-toolresult-hook ship card + truthful result-contract
  doc (est-k75i) (#359).
- **Suite:** the golden seat never wears the launching agent's session identity
  (#325); graph refresh at 7ae1f6f (#350).

## 1.3.3 — 2026-10-09

Patch line: a child that dies rc=130 carrying the CLI's session-lease notice
(`Stopped waiting for another Hermes process on this session. Your message was
not processed.`) is now typed `lease_busy` instead of falling to `unknown` and
failing the node: the class joins the closed `ERROR_CLASSES` set and
`_BOUNDED_RETRY_CLASSES`, so the node gets exactly ONE bounded re-drive as a
FRESH session under the next attempt key (never `--continue` of the still-busy
session) after `node.retrying error_class=lease_busy`. The Oct-3 field shape:
two runs where a freshly spawned review child printed the CLI's lease notice
after ~1800 s of lease waiting and exited 130 — the runner knew only SIGKILL
deaths, so the collision read as an untyped terminal failure. The tool-progress
gate is bypassed for this class alone (the notice proves the attempt never
ran); a rc=130 death without the notice keeps its existing classification,
and a harvestable fenced answer still outranks the class (#4 law). Pin:
`tests/test_lease_busy_retry.py`.

## 1.3.2 — 2026-10-09

Patch line, 37 commits since v1.3.1 (merge order #274 → #272 → #275 → #285
→ #300 → #289 → #294 → #293 → #299 → #290 → #292 → #303 → #280 → #301 → #286
→ #309 → #311 → #318 → #313 → #315 → #305 → #235 → #281 → #253 → #282 → #307
→ #296 → #298 → #287 → #312 → #248 → #306 → #327 → #328 → #322 → #291 → #295): the
wall-meter/read-model wave (per-running-node wall seconds, artifact-mtime
progress channel, failed_nodes[] surfacing, bank-the-corpse on wall-kill),
the provider/route-identity fixes (seat-routed provider/model literals,
alias-pinned ping, billed-model ledger), the door admission hardenings
(schema minItems/minLength, baked-empty fanout refusal, fsync+verify run
dir, surviving-template refusal at amend), the shelf-to-launch contract,
and the graph-level `result:` verdict node. No new tool surface.

Merges since the tag, in brief:

- 851174d #274 — docs(runs): keep find_run docstring to the operative contract
- 4647aec #272 — test(proctree): parseable-content pid/record waits in sibling fixtures
- 0a44455 #275 — fix(ci): graph_path_ban sees renames + quoted paths — -z name-status
- d9a53dc #285 — ci(baseline): ledger row for test_library_tag_read_qeul.py — heals main admission guard
- b172aab #300 — fix(runner): disclose single-query tool approval limits
- 10c56d9 #289 — fix(runner): pin child TERMINAL_CWD to its work dir
- a884eb9 #294 — docs(tool): workflow tool description carries the use/avoid sentence and names delegate_task
- 7f04d95 #293 — fix(door): refuse a surviving {run.KEY} in gate wait.until_argv at run/amend
- 6d181fd #299 — fix(suite): isolate inherited workflow identity
- cde1530 #290 — fix(runner): node record lists every billed model; route hold names final vs mid-session
- 8073d93 #292 — feat(grammar): optional graph-level result: <node id> — status/wait name the verdict node
- 8152edb #303 — docs(grammar): scope the result: contract
- 0d91f80 #280 — wfcommon: reject baked-empty fanout.items:[] at the door
- aca313d #301 — fix(dashboard): loadable hidden API entry
- 869c401 #286 — feat(runner): bank-the-corpse on wall-kill — deterministic <node>.corpus/ + node.banked event
- a11a427 #309 @atbrace — fix(suite): refused invocation invalidates stale green receipts on the --baseline fail path (audit M11)
- 5801d11 #311 @atbrace — test: drop the self-proving stray-file check from the partial-rescue law test (audit M08)
- f3801c9 #318 — docs: one owner per check truth — contributor guide halves, skill sheds its second copy (M05/M06)
- 2f63070 #313 — fix(runner): truthful corpse sizes, protected opens, measured provenance
- 1d6a66f #315 — docs(wfcommon): cut the dead injectable-hook claim from the tag READ fold
- 0abd168 #305 — fix: status/next expose global seat back-pressure
- 8bc661c #235 — feat(#733): dead-on-arrival finalize for stuck running node records
- bb7e163 #281 — test(parity): dual-surface fold parity contract test, test-only
- f45b79a #253 — feat(door): est-kg3y — shelf-to-launch contract: library run_context validation + meta.params surfacing + s12
- dfc388e #282 — read model: artifact-mtime progress channel — run_state publishes {artifact, size, mtime_age_s, last_line} for running nodes
- f549709 #307 — fix(runner): summary.md lists every node's status + output
- e48a3e8 #296 — fix: provider-aware route-identity contract
- 703ccb0 #298 — fix(pack): exclude checkout-only tests from release ZIP
- 19b43d6 #287 — fix(door): alias-pinned nodes ping and report the alias target id, not the bare alias
- 3b45625 #312 — fix(door,harvest): admit + enforce schema minItems/minLength, fail closed on blank lists
- 69ed113 #248 — fix(door): est-2ek.1.199 — fsync+verify the run dir before run_id returns; orphaned-run lane status
- 7d8e6bb #306 — fix: defaults.context bake is idempotent across a run_context-bound amend
- 96ee587 #327 — fix(amend): preview flags def drift on RUNNING never-committed nodes (issue #18)
- 94fd074 #328 — feat(read-model,door,desktop): #133 wall meter — per-running-node wall seconds
- 92b8f67 #322 @atbrace-hermes — feat(read model): populate failed_nodes[] in run_state + status/wait (sys-5lnm17)
- 8632839 #291 — fix(door): bake the provider of a seat-routed provider/model literal at submit
- aeb89ff #295 — fix(runner): wall extension accepts the child's own session row as proof of life (est-2ek.1.595)

## 1.3.1 — 2026-10-07

Patch line, 46 commits since v1.3.0 (merge order #214 → #215 → #218 → #217 → #219
→ #220 → #223 → #221 → #216 → #213 → #226 → #225 → #228 → #229 → #79 → #166 → #177
→ #180 → #232 → #236 → #240 → #242 → #247 → #165 → #234 → #244 → #246 → #250 → #249
→ #237 → #254 → #255 → #251 → #260 → #243 → #258 → #252 → #261 → #262 → #263 → #259
→ #264 → #267 → #265 → #266 → #269): the respawn/SIGTERM-wave hardening batch (runner spawn detach,
liveness past buffered child, crashed-no-exit watchdog), the DIAGRAM LAW wave
(diagrams derive from shipped graph bytes), the hermetic-suite contract sweep
(isolated WF_RUNS_ROOT everywhere), asks 164–166, the partial-rescue law, and the
relay-only gate.held wake (upstream 133387 ask 2).
No new tool surface.

Earlier merges since the tag, in brief:
- 4e39f23 #214 — fix(runner,door): gw-restart-window reap reason + runner-owned spawn ledger (#8 half, WF bugq b2)
- 98e209b #215 — docs(contributing): R5 PUBLIC-release-only carve-out (skills house 58ff35f)
- 399d9ea #218 @atbrace — test(gates): fail closed on missing declared test deps — no silent graph-gate skips
- 91f4f48 #217 — triage(runner-lifecycle): docs name the serve unit that must restart after enable (est-2ek.1.279)
- b35f26e #219 — triage(docs-ux-misc): list surfaces name their scanned roots on empty (est-2ek.1.280)
- 4712d4f #220 — triage(suite-contract): save warns on stale launch-literals in unbound library graphs (est-2ek.1.245 wave-2)
- 39d50d8 #223 — feat(desktop): collapsible live-run tray above the chat window (#22, est-8ppm)
- fabcedf #221 — fix(door,runner): artifact-admission guard enforces 1:1 ledger-to-source mapping (#85, est-mmx0)
- 836bd1e #216 — fix(door): sibling-root scan for status/wait with resolved_version (#58, est-fwdx)
- 4cbcad3 #213 — fix(door,runner): gate answer never consumed by an un-honored resume + provenance-keyed duplicate-consumer gate (#152, est-pzuz)
- 4d6233b #226 — chore(graph): refresh at 4cbcad3362d (est-t4w5, closes #224)
- b161274 #225 — docs(events): event-driven wake pattern — push spine replaces cron polls
- 141b555 #228 — test(dialect-33): courtesy node gate never holds the refuse verdict — tolerant across node minors (#227, est-exmo)
- ac9bbb7 #229 — chore(graph): refresh at 141b5555d96 (est-t4w5, closes #224)
- c648bfe #79 — fix(door): status/wait fold child metrics through the routed channel (est-6b65)
- 0f17cf6 #166 — fix(runner,scrub): adopted-child malformed_turn class
- d36760b #177 — docs: close conformance gaps — SECURITY.md + issue templates (est-rdjq)
- d4e68b4 #180 — docs: point at ratified joint-eng-protocol SSOT (1-line pointer) (est-nztk)
- 66857e8 #232 — DIAGRAM LAW v1: example diagrams derive from shipped graph bytes (diagrams half of owner split)
- 8f6a92b #236 — DIAGRAM LAW PR-3: vendored archify + spelled-numeral ban (A3 closes) + QA receipts derive the gates column
- fa9e900 #240 @atbrace — docs(diagrams): AUSTIN half — cards + derived candidates + receipted shots for 9 examples (PR-2 of owner split)
- a1a1eec #242 — fix(diagrams): one shared walker — qa receipts can no longer pose as graphs
- 2d024da #247 — chore(graph): refresh at a1a1eec
- e68b3d5 #165 — fix(guards): actual-dispatch docs pin, inverse PR-tag audit, packaging pins
- 045cf62 #234 — chore(receipts): drop stale peer-clean-run receipts
- d41f004 #244 — fix: amend-preview def drift + b2b exit contract + bounded harvest scan
- 63b37bc #246 — fix(est-6226): gateway SIGTERM wave — runner spawn detach pin, honest kill attribution
- 8d42313 #250 @atbrace — test(wake): make wake_101 hermetic against inherited WF_RUNS_ROOT
- 743afd2 #249 — chore(cleanup): zap-verified dead-code cut, complete (est-2ek.1.730)
- 14bc40c #237 — fix(respawn): live-plugin resolution + crashed-no-exit watchdog respawn (est-ujtf, est-2ek.1.718, est-c481)
- 3c0b401 #254 — docs: codify the partial-rescue law — done-set against the CURRENT graph is the only saved state (#134)
- 6f1f0b9 #255 — fix(runner): heartbeat liveness past buffered child + global seat cap (est-g2xx)
- 29eb0a6 #251 — fix(library): est-bvg0 — filtered query on empty/tagless shelf returns the promised tag_match_counts diagnosis
- d3fd5f7 #260 — test(seat): scrub WF_* from child spawn env — test_seat_live hermetic (est-aywd)
- d0a0017 #243 — fix(133387): machine-gate probes bind to the run that owns the gate; import hygiene (est-vuqr)
- 8502536 #258 — fix(runner): fanout aggregate derives from committed per-item records with status-compatibility certification (est-2ek.1.765)
- 66142b7 #252 — fix(library): est-yzoy — corrupt-prev retain protection + empty-result cause naming + erase-path pins (#146)
- a4dbd51 #261 — feat(plugin-asks): asks 164-166 + toolsets slice — est-rqmn nightly landing
- 0497444 #262 — fix(asks166): padded version handshake compare + amend-time enforcement (#261 follow-ups)
- a690c04 #263 — test(integration_080): hermetic runs-root pin — passes cold, not just in serial order (est-hk0z)
- b413983 #259 — test(suite): pin every spawned-runner test to an isolated WF_RUNS_ROOT (est-2ek.1.762)
- f2df20a #264 — test(7ps8): realpath dedupe, executed landing-rows accounting, suite temp cleanup
- d58e9b2 #267 — test(proctree): wait for parseable pid markers, not mere existence (est-gzmm)
- db26151 #265 — fix(runs): find_run resolves the listed VALID twin, not the torn first-root dir (est-t1kk)
- 9fc82fe #266 — docs(test): trim routing narration from measured-zero docstring, fix provenance (est-2ek.1.797)
- f0e6935 #269 — fix(runner): gate.held wake is relay-only — agent relays the human gate, never answers it (upstream 133387 ask 2)

Fixes:
- fix(runner,door): #44 — the stranded `runner.lock` wedge (Errno 11 forever with the
  kernel ledger knowing zero holders) no longer makes `wait`-resume permanently
  impossible. The door's liveness read is now a TRI-STATE CHILD-PROBE
  (`runner_lock_state` → `free|busy|eacces|wedged`): every flock probe runs in a
  short-lived child whose death is the release (the door never opens `runner.lock`,
  so a SIGKILLed prober can never strand the fd — issue ask 1); `busy` is resolved
  against `/proc/locks` (seam `WF_PROC_LOCKS_PATH`) and the new heartbeated admission
  lease (`runner.lease {pid,boottime,at}`, written/beat by `wf.py acquire_lock`);
  EACCES on an existing lock is its own fail-closed state, NEVER free (PR #47 block
  B1); a lease-less legacy dir stays fail-closed busy (91b9a3de law verbatim).
  `wedged` = probe EAGAIN + zero ledger rows + proven-dead lease — the exact field
  shape — and is the ONLY shape `wait` auto-heals. The heal
  (`wfcommon.heal_wedged_runner_lock`, single safety authority) renames the stranded
  inode aside under the admission heal-barrier (`runner.lock.heal`, which
  `acquire_lock` honors before opening the path) and RE-VERIFIES probe+ledger+lease
  immediately before the rename, so a holder that wakes between check and commit
  wins (PR #47 block B2); the stranded-replace race closes atomically at admission
  via an fstat-vs-path inode compare with bounded retry, never by displacing a
  holder. New escape hatch (issue ask 2): door action `release_lock` +
  `wf.py release-lock <run_id>` — refuses unless zero live holders are proven.
  `list`/dashboard census batch the probe into ONE child per call (PR #47 C2 law).
  Read-model surfaces `runner_lock`/`runner_lock_detail` only on non-ordinary states
  (golden key sets byte-unchanged for busy/free).
  Test: `tests/test_lock_wedge_44b.py` (35 contracts: child-probe + kill-the-probe
  mid-flight, B1 EACCES-never-free + heal-refuses, B2 waking-holder-wins, wedge
  classification + heal re-acquire + idempotence, door + CLI verbs);
  `tests/test_cross_container_liveness_91b9a3de.py` re-pinned to the tri-state shape
  (19 contracts, cross-container law byte-preserved).

Discipline / docs:
- docs: #134 partial-rescue law (jam-g1, epic #40) — AGENTS.md 3d states the
  recovery discipline explicitly: the committed `nodes/*.json` done-set parsed
  against the CURRENT graph is the ONLY saved recovery state (efp re-validation
  makes amend-via-efp the sanctioned path; a rerun follows the edited graph,
  contrasting DAGMan's rescue-DAG pattern), and the plugin carries no
  rescue-snapshot file BY DESIGN — none may be added. No behavior change to
  wf.py/wfcommon.py. Test: `tests/test_partial_rescue_law_134.py` (4 contracts;
  pins amended-ancestor demotion of untouched descendants + by-design absence
  of any graph-snapshot file by name).

Read-model / DX:
- feat(door,dashboard): est-2ek.1.280 — an EMPTY `list`/`_list_runs` scan now
  emits `roots:` the resolved runs_root first, then the legacy launch root when
  different. The multi-profile papercut was a profile-scoped tool writing
  `profiles/<p>/workflows/` while the API globbed the canonical dir: BOTH
  answered a silent `[]` and a peer lost ~10h chasing auth instead of the
  stale root. Non-empty payloads keep the golden-solo key set
  `{runs, total, counts(, provenance)}` byte-identical (F1 identity law).
  Test: `tests/test_list_root_280.py` (6 contracts, RED first).
- #58 door sibling-root scan (status/wait): a run dispatched under one home (profile
  seat) and read from a consumer whose env resolves another (estate shared root,
  sibling profile) answered `unknown run_id` while its runner and events were alive
  next door — find_run covers only resolved-root + legacy launch root. The READ verbs
  now scan the sibling known roots (estate + every profile under it, both directions
  from the resolved and launch homes) before answering unknown, and the answer from a
  hit carries a `resolved_via` warning naming the foreign dir. Read-only convenience:
  foreign-root `wait` never spawns a runner (the owning root does; the root-compare in
  `_runner_pid_alive` would reject a foreign spawn anyway), and write verbs
  (amend/release/steer/stop/save) stay fail-closed on the resolved root. Same-root
  behavior is byte-identical (no `resolved_via` field on a resolved-root read).
  Test: `tests/test_door_sibling_root_58.py` (5 red at base → all green at head;
  hermetic two-root sandbox, fake launcher).
- #est-tmuu — deterministic provider/alias config deaths are never respawned. A
  node pinning a provider the seat does not define made the CLI exit rc!=0 in
  ~0.1s (`Unknown provider 'x'. Check 'hermes model' …`); the runner surfaced
  that death as the transient classes and the Q4 ladder respawned the WHOLE
  recovery sequence on a deterministic input error, landing
  `transport_exhausted` with the budget burned (operator-verified report,
  2026-10-02). The runner now types it `config_input`: a child that dies rc!=0
  within ~1 s of spawn AND whose capture carries the config-error marker line
  fails on the FIRST attempt — the class sits outside both retry ladders (the
  #24 fatal_quota law), the respawn budget is never decremented, and the error
  names the fix (the node's provider pin or the seat config). The window is the
  precision guard: the same capture dying late keeps its existing
  classification; `unknown model` stays `unresolved_model` territory. Pin:
  `tests/test_config_input_tmuu.py` (fake mode `cfgtypos`).
- Docs-surface guard hardening (PR #155 follow-up) — four adversary-confirmed
  blind spots closed, test-first. The drift pin now executes the door and
  compares ACTUAL `ACTIONS` dispatch keys against the README action table
  (regex-scanning source stayed green while an unlisted callable lived in the
  dispatch dict; mutation self-proofs 7a-7d run every pass). `pr_tag_audit.py`
  gained the INVERSE assertion — an `(open PR #NN)` tag on a row whose action
  is dispatched fails even while the PR is open — and missing `gh` now exits 2
  with file:line diagnostics instead of an uncaught traceback. The audit helper
  joins the pack list beside `graph_path_ban.py`/`graph_regen.py`
  (`scripts/pack.py`), and `test_packaging.py` pins the
  exact packed `scripts/` set plus a packed-or-declared-source-only contract,
  so a helper can never again sit outside the ZIP while its test ships green.
  Stale doc tags aged in the same commit (red-on-base rule): the #121 tags in
  CONTRIBUTING.md + references/development.md → `(shipped in v1.2.1)`, the #84
  tags in README + references/portable.md → `(shipped in v1.3.0)`, and the
  three #47 tag sites in README rewritten to current truth — #47 closed
  unmerged, so the `release_lock` row and the `liveness-unknown` run state are
  removed and wedged-lock recovery describes the shipped kernel-flock
  admission. `pr_tag_audit.py --repo <owner>/hermes-workflows` exits 0.

## 1.3.0 — 2026-10-05

Minor line, 29 commits since v1.2.1: the join/on_fail wave (deterministic merged
join objects at the wave boundary, caught agent death becomes join-tolerant), the
read-model honesty batch (honest status, `stale_because`), and the contributor-DX
sweep. Merge order #168 → #178 → #184 → #185 → #188 → #179 → #193 → #195 → #197 →
#198 → #199 → #200 → #202 → #203 → #204 → #205 → #206 → #207 → #208 → #209 → #210
(+ composite include #84; graph refreshes in between).

Runner features — join / on_fail / join-object semantics:
- 4f5f359 #209 — feat(runner): join nodes — deterministic merged object of named
  parent outputs at the wave boundary.
- f8a5956 #207 — feat(runner): on_fail — a caught agent death becomes join-tolerant
  skipped or a fallback agent.
- e13a347 #208 — refactor(runner,door): NODE_TYPES is the ONE node-kind table —
  schedule + validation consult kind(n).
- 16e705f #206 — feat(runner): full-jitter retry sleeps for rate-limited transport
  deaths.
- acc5bc0 #202 — feat(runner): pre-cap persist/finish budget cue + stop_reason
  kind=budget.
- 5d89502 #178 — feat(runner): convoy splice (order_only) + run.blocked residue
  classes + dead-letter ledger.
- 7bec849 #184 — feat(runner): boot-time clean-lane assert for re-drives.
- ca456f6 #185 — feat(runner): attempt-N preamble + reconcile-don't-redo on
  crash-respawn.
- 9bf28de #168 — feat(plugin): card enforcement — transform_llm_output ships the
  ::workflow card unasked.

Registry & read-model:
- 27b2ec4 #210 — feat(read-model): stale_because — the read model says WHY a
  committed node will rerun.
- 87306e4 #203 — feat(read-model): honest status — harvest-proven records read
  done; runner_exit verdict outranks pid-liveness.
- 967b486 #205 — feat(door): validate action — dry-run the door's whole validation
  with zero writes and no liveness ping.
- afcba7f #198 — fix(pack,door): install.json provenance in the ZIP +
  doctor_version drift check.

DX & fixes:
- c01b4a0 #204 — fix(dx): error-polish + grammar-pointer — errors name their
  offender; one-pass defect reporting; when-grammar quoted.
- d93eef0 #195 — fix(runner): never orphan the agent child; never bill a
  substituted route.
- e7f416f #200 — fix(schema): validate() names the fuzzy sibling-key rename on
  missing-required.
- 3ed07ed #199 — fix(door): status/wait surface a structured stop_reason for
  budget deaths.
- 50234d8 #197 — fix(lane-hygiene): the ONE verified reload form per deployment
  joins LANE_HYGIENE_LINES.
- e14f85e #193 — fix(include): malformed parent node id + graph-level include ->
  named errors[] envelope (issue #192).
- e7cf7e3 #84 — feat(include): composite graphs — expand shelved library graphs
  into a run at materialize time.
- 45e1228 #188 — feat(runner): publisher capability gate — typed refusal until
  verified suite proof.
- 1c8ddad #179 — fix(door): lane children cannot publish run graphs onto the
  shared shelf.

Graph refresh: graphify-out rebuilt at each merge; current refresh at 4f5f359 (#211),
settled GREEN at tip e1c16ae.

## 1.2.1 — 2026-10-04

Patch line, 26 commits since v1.2.0 (merge order #136 → #147 → #97 → #148 → #149 →
#135 → #118 → #140 → #120 → #150 → #155 → #151 → #158 → #161 → #164 → #160 → #121 →
#167 → #122 → #162 → #170 → #156 → #159 → #169 → #163 → #176): the P1 runner-error
batch (typed classes + credential-window park + resolve-time clamps), door/desktop
legibility fixes, the examples rework, and the CI single-writer gate for the
knowledge graph. No new tool surface.

Earlier merges since the tag, in brief:
- af2c2f5 #136 @atbrace — feat(door): faceted library tags (`facet:value` save +
  filter), `tag_vocab` echo, self-diagnosing empty results.
- 60a4318 #147 @atbrace — docs(grammar): `risk:` seed values + co-occurrence-keyed
  vocab trigger.
- 7c7f0d9 #97 @atbrace — wake the owning session on lifecycle transitions
  (`gate.held` / `run.done` / `run.failed`).
- 8c1231b #148 — test(door): #146 item 2 — multi-term empty-result diagnosis
  regression.
- 87826ef #149 @atbrace — fix(door): erase-by-file-edit is a state — retain keys on
  presence, garbage fails closed.
- ce47f63 #135 — fix(validate): split integer/number — reject booleans and
  fractional values as integer.
- 07a6786 #140 — feat(desktop): pane rework — RUNNING on top, husks unrendered,
  RECENTLY FINISHED.
- e1281e7 #120 — fix(runner): typed tool-call-as-text classifier blocks
  prose-coercion false green.
- e253f9a #150 — fix(desktop): directive card keeps the last good pill through
  refetch errors; error branch retries.
- 4c5a3a3 #155 @atbrace — docs: refresh against 1.2.0 live surface — stale claims
  fixed, jargon sweep, docs-surface drift guard.
- f83e5d0 #151 @atbrace — feat(examples): release-lifecycle + issue-to-pr distilled
  lifecycle templates.
- e14978d #158 — fix(tests): est-954r — fake-b1 copy never mutates tracked files on
  raw runs.
- 36cb02d #161 — feat(door): run-result `lifecycle_notice` — card delivery as tool
  RESULT, not just hint.
- e18eca5 #164 — fix(runner): ERROR_CLASSES gains `forbidden_model` +
  `precondition` — closed set is now provably exhaustive.
- ab78114 #160 — fix(runner): malformed turn-shape gets typed `malformed_turn`
  class + bounded re-drive (#541).
- 6c67789 #121 — fix(suite): zero-discovery is a failed admission, never green.
- e75a72e #167 — feat(ci): knowledge graph single-writer — path-ban + regen scripts
  + `.gitattributes` (closes #153).
- 3fdff85 #122 — fix(wf): prose-JSON fallback uses `json` raw_decode; no silent
  nested promotion at 200 items.
- ae2e8b7 #170 — ci: knowledge-graph single-writer gate swap (2/2 of #153).
- 6311602 #156 — feat(desktop): agent-first pane — index by originator, no
  ownership framing.
- d6dfa27 #163 — fix(runner): resolve-time clamps — an unknown reasoning effort or
  toolset at resolve time is clamped to the nearest valid value with a
  `node.clamped` notice, never a hard child death (est-flah).
- 28aed8e #176 @- — fix(examples): portable-review runs as README documents (#172).

Detailed entries (Unreleased work folded in):

- #157 — card enforcement: the `::workflow` card ships unasked. A new
  `transform_llm_output` hook (`card_enforcement.py`, registered by `register()`)
  is the last-resort shipper behind the model's paste: when the turn's final
  text carries NO unfenced `::workflow{id="<run_id>"}` directive (fences and
  `inline code` are dead text to the directive renderer and do not count), the
  cards for runs THIS session launched inside the recency window (default
  30 min, owner-setting `card_window_minutes`) are appended as new lines before
  the assistant row persists. Ledger is the run's own `events.jsonl`: a
  `card.echoed` marker (appended after the text is computed — a failed write
  costs a re-ship, never a lost turn) keeps shipping one-shot across turns
  without touching `run.json`; an in-turn `(turn_id, run_id)` table dedupes
  retries. Rails: ≤3 cards/turn (newest first; the ledger ships the rest next
  turn), terminal-stale runs (run.done > 10 min) never spam, falsy `platform`
  (unknown surface) never guesses, discovery runs ONLY through the existing
  wfcommon resolvers (runs root, per-profile roots, legacy launch root), and
  every path is fail-open — any exception returns None, the user's reply is
  never eaten. The model's paste stays the primary path; `desktop/plugin.js`
  is untouched (no tool-result render slot exists in the plugin SDK — the
  render-from-record half of #157-A is desktop/core work).
  Discovery half superseded: the runs-root scan, the dedupe census and the `card_window_minutes`
  knob are replaced by the hook-led pending ledger (`card_ledger.py`; see references/operations.md).

- #166c — the scrub audit's suffix set covers every shipped text shape: .yml and
  .txt joined AUDIT_SUFFIXES (the shipped ci.yml exported forbidden lines with
  '0 scrub hits' under the old set, NUL or not). New text types join the set,
  never the skip path.
- est-n58i — the adopted-child death path shares the malformed-turn law: an
  adopted orphan whose reply IS serialized tool-call markup now classifies
  `malformed_turn` (verbatim diagnostic carried), not generic `unknown` — the
  class no longer varies by which path (fresh vs adopted) reached the same
  death; the bounded ladder still may not re-drive the adopted path (its spend
  is committed). Pin: `tests/test_malformed_turn_adopted_541.py` drives the
  REAL runner on both paths and asserts `item.adopted` actually happened.
  The scrub audit's text-ness is content-based (null-byte sniff, the git
  heuristic): the shipped `.sig` and extensionless text (`tests/fake-b1`) are
  audited too — a file name can no longer hide text from the gate; the
  LICENSE copyright hit is guarded by an anchored exemption.
- #116 — confidence_substrate: engine-stamped fallback when a pinned confidence
  route is quota-dead. The owner declares a sanctioned fallback substrate once
  (`plugins.entries.hermes-workflows.settings.confidence_substrate`, top-level
  `workflows: confidence_substrate:` config, or `WF_CONFIDENCE_SUBSTRATE` env —
  a `"provider/model"`, comma list, or ladder list; ORDER is the ladder). The
  door consults it ONLY on the #25 dead/fallback-ladder-surprise branch: the
  first rung whose own submit ping proves alive serves, the node def is
  re-routed and engine-stamped `substrate_substituted` {from, to, reason,
  source}, `route_verified` re-bakes to the served rung, and the node's result
  schema machine-gains the required `substrate_disclosure` clause — the runner
  stamps the honest label into `nodes/<n>.json` at commit; a child can neither
  author it away nor be killed for omitting it. Author-written stamps are
  stripped at the door; the runner re-verifies the stamp against the estate
  config at commit and fails closed (`route_unavailable`) on a forged one.
  Absent config = today's fail-closed refusal byte-identical (golden-solo +
  no-config EMPTY-diff gate); `require_route: false` stays a pure opt-out and
  an explicit live pin always beats the config. Rides the existing #25
  route-hold path (R6), no parallel gate.

- #est-tmuu — deterministic provider/alias config deaths are never respawned. A
  node pinning a provider the seat does not define made the CLI exit rc!=0 in
  ~0.1s (`Unknown provider 'x'. Check 'hermes model' …`); the runner surfaced
  that death as the transient classes and the Q4 ladder respawned the WHOLE
  recovery sequence on a deterministic input error, landing
  `transport_exhausted` with the budget burned (operator-verified report,
  2026-10-02). The runner now types it `config_input`: a child that dies rc!=0
  within ~1 s of spawn AND whose capture carries the config-error marker line
  fails on the FIRST attempt — the class sits outside both retry ladders (the
  #24 fatal_quota law), the respawn budget is never decremented, and the error
  names the fix (the node's provider pin or the seat config). The window is the
  precision guard: the same capture dying late keeps its existing
  classification; `unknown model` stays `unresolved_model` territory. Pin:
  `tests/test_config_input_tmuu.py` (fake mode `cfgtypos`).
- examples/ reorganized into role subdirectories (`basics/`, `build/`,
  `review/`, `release/`, `ops/`) with a map README describing each template,
  what it teaches, and the receipts proving its arms; packaging and structural
  tests follow the new paths.
- #54 — credential-window 429s get their own `ratelimit` error class and a bounded
  park. The child CLI's `Anthropic credentials are rate-limited for <model>` banner
  (hermes_cli/runtime_provider.py) used to classify as transport/unknown and burn
  the 5 s/20 s ladder in ~10 s against a 36+ minute provider window; the runner now
  parks ~5 minutes (jittered; `run.json` `ratelimit_interval`/`ratelimit_jitter`,
  meta-only like `retry_backoff`) and re-spawns while the node's wall budget still
  fits a park, emitting `node.retrying error_class=ratelimit backoff_s=…` per park.
  On give-up — budget full, stop set, or the shared per-run retry budget exhausted —
  the node fails with the verbatim `credential rate-limited for <model>` as its
  error text so the dispatcher can switch model instead of requeue. Plain 429s stay
  transport, quota-horizon 429s stay `fatal_quota` (banner wins when both match);
  every existing classification pin is green.
- wf159c — the ratelimit WALL is a REAL bound + late banners reach the park +
  the park sees the quorum cancel. A park may fire only when the worst-case
  jittered wait plus a real respawn window (>=15% wall, floor 0.25 s) fit the
  wall; the deadline is rechecked after the wait and the parked respawn runs
  CLAMPED to the remaining wall (a park can never buy a fresh full timeout).
  A banner discovered after a ladder respawn is handed to the same park by
  the transient ladder (dispatcher contract identical for late deaths). The
  park's wait observes the fan-out quorum cancel — a satisfied quorum never
  waits out a parked straggler (the 300 s default was the last held minute).
  Pin: `tests/test_ratelimit_park_walls_159c.py` (mutation-proved on all three
  defects; test_ratelimit_54 pins undrifted).

- #163c — gate-400 quarantine FAIL path commits its typed record. The fail-closed
  branch of the gate-400 re-drive read a local the transient ladder owns
  (UnboundLocalError: no record, no stamp); the dead attempt's evidence now rides
  in via the dict handed to _isolate_prior, and both quarantine record builders
  carry attempts_log forward like the log paths.
- #116 — confidence_substrate: engine-stamped fallback when a pinned confidence
  route is quota-dead. The owner declares a sanctioned fallback substrate once
  (`plugins.entries.hermes-workflows.settings.confidence_substrate`, top-level
  `workflows: confidence_substrate:` config, or `WF_CONFIDENCE_SUBSTRATE` env —
  a `"provider/model"`, comma list, or ladder list; ORDER is the ladder). The
  door consults it ONLY on the #25 dead/fallback-ladder-surprise branch: the
  first rung whose own submit ping proves alive serves, the node def is
  re-routed and engine-stamped `substrate_substituted` {from, to, reason,
  source}, `route_verified` re-bakes to the served rung, and the node's result
  schema machine-gains the required `substrate_disclosure` clause — the runner
  stamps the honest label into `nodes/<n>.json` at commit; a child can neither
  author it away nor be killed for omitting it. Author-written stamps are
  stripped at the door; the runner re-verifies the stamp against the estate
  config at commit and fails closed (`route_unavailable`) on a forged one.
  Absent config = today's fail-closed refusal byte-identical (golden-solo +
  no-config EMPTY-diff gate); `require_route: false` stays a pure opt-out and
  an explicit live pin always beats the config. Rides the existing #25
  route-hold path (R6), no parallel gate.
- door: composite graphs — a top-level `include:[{as, use, seeds?, exports?}]` annotation
  expands shelved library DAGs into the parent graph at MATERIALIZE time
  (`wfcommon.expand_includes` core + door wiring; design 2026-09-30). Composition happens
  before validation, before `defaults` baking and before `{run.KEY}` binding on run,
  amend and save: the runner, read model, gates and desktop never learn includes
  exist, so every load-bearing invariant holds by construction (efp replay-skip,
  `on_skip:prune` priceability, closed-set node validator — zero new node kinds —
  and static arm-drawing). Deterministic `alias__<inner-id>` namespacing rewrites
  all five id-ref surfaces in one pass (`after`, `inputs` heads, `requires` keys,
  `fanout.items_from` head + its `after` entry in lockstep, `when` `out.<id>.`
  paths whose heads validate never existence-checks); seeds are a closed map that
  renders `{run.KEY}` inside the included subtree only; the parent reaches an
  include only through an exported public name or a literal `alias__id` — a bare
  inner id is refused. Every guard (unknown library entry, standalone-invalid
  child, alias/id collision, cross-include cycle, depth-4 cap, merged node/byte
  caps, unbound seed, dot/overflow id) refuses with the existing
  `errors:[{node:'include:<alias>',field,msg}]` envelope before any write or spawn;
  non-fatal resolver warnings (shared fixed scratch paths — detect-and-warn, never
  rewrite) echo as `include_notes` on run/status and land in run.json.
  Strip-on-expand is the storage contract: the committed `graph.json` is the
  expanded, include-stripped truth (amend edits the expanded form; amending an
  already-expanded run is an identity no-op), while library `save` keeps the
  AUTHOR form so a shelved composite tracks shelf updates — resolution stays at
  run time — and save runs the resolver once purely to validate guards.
  `run.json` gains `includes:[{alias,name,source_digest}]` provenance (empty
  omitted; a plain run's key set is byte-unchanged, golden-solo stays an empty
  diff). `GRAPH_KEYS` admits `include` (grammar/provenance precedent); the
  WORKFLOW_PARAMS graph description documents the surface.
  Hardening pass (blind-review findings, 2026-09-30): an included graph's
  `model_policy.forbidden_models` UNIONS into the parent's policy (deterministic
  sorted order, `include_note` names the alias — a shelved safety floor survives
  composition; no other child top-level key is carried); the alias grammar is
  tightened to `[A-Za-z0-9_]` (no hyphen — a hyphenated alias + a child `when`
  gate fell through the when-token grammar as a confusing when-syntax error, now
  a named include-guard refusal at declaration); an explicit `include: null` (or
  any present-but-not-non-empty-list) refuses instead of retaining the key past
  the include-stripped contract; the door memoizes its library reader so the
  expansion and the provenance digests are stamped from ONE view of the shelf
  bytes (no torn read between the two passes); an author-form amend restamps
  run.json's `includes`/`include_notes` (provenance never describes the previous
  graph); `save(run_id=…)` REFUSES for composite runs (their graph.json is the
  expanded form — save the author graph inline); and the merged node/byte caps
  re-check the FINAL fused graph after the parent-ref rewrite, not just the
  pre-rewrite candidate.
  Round-2 P1 follow-up: gate `options[]` and `wait.until_argv[]` joined the
  shared `_include_text_fields` traversal — an included gate's option labels
  (verbatim on the human release card) and fixed argv (exec'd by the wait pass)
  are now seed-rendered AND survivor-swept like every other text surface; the
  nested write-back was generalized to a copy-on-write path setter (the old one
  assumed every nested field was `fanout`, so a seeded value could not land in
  options/argv at all). Unseeded placeholders refuse through the errors
  envelope; include-free gate bytes keep their verbatim leniency.
  Tests: `tests/test_include_door.py` (100 door contracts — expand-before-validate,
  refusal envelopes with zero run dirs, dry_run side-effect-free lint,
  amend identity-stability, save author-form, from= replay, provenance + notes
  round-trip) and `tests/test_include_expansion_core.py` (core-resolver contracts).
- #59 validator: string-typed keys are TYPE-checked at submit, never discovered at the
  wall (fb-fix ledger 97e90c2205f17fb0 — run `20260930-051209-fb-fix-436f89c3-rem`
  authored an agent `context` as a LIST; it passed the truthy-only checks and died at
  FIRST spawn in `run_child`'s prompt concat, `node crashed: TypeError: can only
  concatenate str`, `error_class:'crashed'`, burning an already-answered gate release).
  `validate_graph_errors` now rejects, with named-node `{node, field, msg}` errors
  mirroring the fan-out item-goal law: agent `goal` — a PRESENT non-str (`[]`, `{}`,
  `0`, `False`, `null` included; truthiness-independent, ra-59 review finding 1) — and
  plain-agent whitespace-only str; agent AND gate `context` and gate `question` —
  present-key-must-be-str, so explicit null is rejected like a list/dict (string-when-
  present contract, ra-59 review finding 2; absent and `''` stay legal optional keys);
  `fanout.goal` template present-non-str (same first-spawn crash class — `fmt_goal`
  re.sub + node-goal concat); and echo `output` non-JSON-serialisable (the door's own
  `graph.json`/node commit write is where a set or custom object used to explode;
  dict/list/str/num/bool/null stay the documented verbatim-commit shapes — the echo
  JSON-verbatim contract is RETAINED, per the ra-59 compatibility clarification).
  Absent/`''` goals keep the exact legacy "agent node has no goal" message. NO coercion
  at resolve — strict-at-submit is the engine law (closed grammar: an un-validatable
  graph must never be accepted). Additive validation: well-typed graphs validate
  identically, golden-solo stays EMPTY, zero run-dir writes and zero spawns on a
  rejected submit.
  Test: `tests/test_string_type_validation_59.py` (issue repro, every key × good/bad
  shape incl. falsy/null rows and the review's exact case set, legacy-message pin,
  door-level zero-writes/zero-spawns rows per rejected case, valid-graph zero-errors
  control).
- door: `run_context` transport guards in `_bind_run_context` (string branch). Two silent
  routes to a launched run full of unsubstituted `{run.KEY}` refs, both now rejecting
  before any run write, same fail-closed style as the #7 brace guard: (1) a JSON object
  handed over as a STRING (a caller that meant the map form — tool transports routinely
  stringify objects) was routed to SEED mode and bound nothing; it now raises and names
  the mistake. (2) a seed string against a graph holding `{run.KEY}` refs appended to
  context and launched anyway, leaving the refs literal in the persisted graph; it now
  raises naming the offending node id and key. The map branch is untouched; prose and
  `k=v` seeds behave exactly as before. The `run_context` schema entry declares
  `["string","object"]` and states both rejections.
  Test: `tests/test_run_context_seed_guard.py` (encoded-map rejects atomically — no run
  written, no spawn; seed-with-refs rejects naming node+key; dict binding still
  substitutes; prose seeds still launch).
- runner: pre-cap persist/finish budget cue (est-bbfy — residual half of
  est-2ek.1.95). When a capped agent lane's consumed turns (the same
  `wfcommon.child_metrics` state.db join that already proves liveness) reach
  `max_turns - margin` — margin from run.json meta `budget_cue_margin`, the door's
  channel (same law as `retry_backoff`), default 5 — the runner injects ONE steer
  line on the existing steer channel: appended to the run's `inbox.jsonl` (the next
  spawn's bake) AND to the live spawn's bake file with `i=-1` (pullable at its next
  inbox seam), reading `turn budget: N turns left — persist your work now
  (commit/push per checkpoint law) and prepare your final fenced-json answer`. An
  `O_EXCL` marker under `<run>/budget_cue/<node>` is the claim — one cue per
  (node,index) for the life of the run, surviving respawn/re-drive. Zero behavior
  change under the cap: no cap, unknown counter, or turns still above the soft-cap
  => no file, no line, no event (honest absence). `act_status` `stop_reason` gains
  `kind: "budget"` — derive-ONLY off the marker file, and only when #199's tier
  note already produced a `stop_reason` (plain cap deaths stay class-only,
  successes stay key-free). Wall kills (`node_timeout`) are an independent clock;
  `budget_cue_margin` is the lever for turn-cap deaths.
  Test: `tests/test_budget_cue_bbfy.py` (22 contracts — cue fires inside the 1 s
  liveness loop for both spawn generations, O_EXCL respawn-idempotence, honest
  absence under the cap, `kind=budget` derivation; fake modes `budget_lane` /
  `typed_maxturns`).

## 1.2.0 — 2026-10-03

Minor line, 12 commits since v1.1.4 (enumerated below in merge order
#80 → #108 → #89 → #119 → #137 → #90 → #139 → #138 → #142 → #141 → #143 → #144):
seven teaching templates (Core-10 #2/#3/#6/#7/#8/#9/#10) with dual receipts under
`receipts/` — author run + independent peer clean-run, digests recomputable from
shipped bytes — forged across two estates (the Core-10 mill), plus the #61b
process-tree runner close and one door fix.

- a4a5266 #80 — feat(runner): process-tree accounting closes the false-green-suite
  hole (#61b). An exit-0 spawn is believed only when its own process group is
  provably empty (recursive /proc walk, pid-set persists across retries); an
  adopted orphan whose tree outlives it commits `partial`, never a clean `done`.
- 94fadf4 #108 — feat(examples): quorum-probe — folded speed/coverage contrast
  (#8): quorum-raced probe fan-out beside a no-quorum barrier reconciling against
  a master catalog; dead lanes print as loud coverage gaps.
- 712b7bc #89 — feat(examples): blind-council review template (#2): isolated seats
  bound to capability classes via settings.models; synthesis with mandatory
  verify-list; unbound class fails launch closed.
- cccec53 #119 — fix(door): run hint forbids code-blocking the card line.
- 4656098 #137 — feat(examples): bulk-transform barrier-audit template (#9):
  manifest fan-out, audit reconciles all_results against the manifest and
  re-checks the filesystem itself.
- 282a5c1 #90 — feat(examples): triage-route queue template (#3): classifier +
  complementary when-gate pair, exactly one arm, empty queue closes not fails.
- 7c89a1a #139 — docs(AGENTS): quorum-probe lap law — both folded modes walk
  every launch; laps exist per-engine-side, not per-mode.
- 425ac7c #138 — feat(examples): escalation-ladder verify-then-branch (#10):
  builder → fresh verifier → verified lands behind a human gate, failed escalates
  to a human hold; both arms smoke-run.
- 6e96af1 #142 — feat(examples): census-fanout (#7): deterministic tally — every
  item answered or NAMED missing, counts read from the record, never prose;
  designed dead gauge exercises the loud-row law every run.
- 33aac84 #141 — feat(examples): exchange-run (#6): byte-portability teaching
  pair — the file itself travels save → library → re-run; leaf proves hand-off
  only from committed records.
- 6f6ecd4 #143 — receipts(exchange-run): peer clean-run signature — four laps at
  the peer door on the shipped bytes; completes the dual-receipt dir.
- fde3e23 #144 — docs(AGENTS): receipt arithmetic + fingerprint/shelf-provenance
  laws (rules 9–10): totals are pointers into the enumeration, never
  hand-maintained headlines; fingerprint lives in runner_exit.json.

## 1.1.3 — 2026-10-01

Three merged PRs since the previous tag, in merge order (#91 → #62 → #82).
#88's 61c content is not on `main`: it stack-merged onto `fix/proc-tree-61b`
and rides #80. The later CI-only workflow_dispatch commit (#99) and the examples-only
incident-response fix (#93) change no plugin code.

- #70 faceted library tags, ported onto #62's discovery-first library: `tags` tokens
  are now legacy-flat OR `facet:value` with a CLOSED facet set
  (`use_case|repo|domain|risk|note`; hard namespace, soft values — the k8s
  well-known-labels split), validated on every save path with case-normalization and
  dup collapse; `library` gains an ALL-match `tags` filter (normalized queries),
  a whole-library `tag_vocab` echo once the library carries tags (the anti-drift
  loop: reuse what you see), and `tag_match_counts` so an empty filtered result
  diagnoses itself (spelling vs sparse co-occurrence). RETAIN-ON-OVERWRITE: a resave
  that omits tags/description carries the previous envelope's values; library writes
  are atomic (tmp+os.replace). `/wf` lists tags inline. Council/ponytail/folksonomy
  adjudicated cuts with named triggers: did-you-mean (observed bad save), aliases /
  OR grammar / facet-grouped vocab (100+ entries), desktop view (separate feature).

- #91 — @jacobhausler.
  fix(runner,validator): a partial ancestor no longer releases plain after-edges (#87).
  A harvest-on-death `partial` no longer silently satisfies a plain after-edge.
  A child that dies mid-work (cap/rc≠0 with a valid fenced answer) still commits
  `partial` with its harvest (#4 law untouched), but releasing `verify`/`suite`
  onto the incomplete candidate was the false-green class. Now: `dep_satisfied`
  is STRICT for after-edges (partial satisfies only via the per-node opt-in
  `after_partial: true`, agent/gate keys, bool only, echo rejected by name);
  a pending agent/gate with a plain partial ancestor FAILS TYPED at the wave
  boundary — `error_class:"precondition"`, `error: blocked_by_partial_ancestor:
  <nid>`, ZERO spawns, never a hang (`deps_res` keeps partial RESOLVED so the
  verdict always lands); `requires` gains provenance — a ref resolving from an
  ancestor whose committed record carries `harvest` is UNMET
  (`precondition unmet: <ancestor>.harvested`) without the opt-in, satisfied
  with it. PRESERVED deliberately (#4): a node's OWN fanout partial-credit
  merge still commits done; a LEAF partial still closes the run green with its
  harvested output in the summary; golden-solo bytes unchanged. Read model
  (`blocked_by`, `run_state.deps_ok`) mirrors the runner law.
  Test: `tests/test_partial_block_87.py` (C1–C7: typed block + spawn-control,
  opt-in release, leaf green, fanout merge regression guard, requires
  provenance, validator grammar, gate same-law). Law-tightening update:
  `tests/test_sprint101w2_B2-retry.py` #4a/#4-read-model rows encoded
  partial-satisfies-downstream for an after-edge; they now pin the opt-in
  (same ledger row e68544a37be37657).

- #62 — @jacobhausler.
  feat(door): discovery-first library — rich list, submit-for-study inbox, fuzzy from= nudge (#50).
  `list` rows carry rich metadata (description/triggers/tags/roles) so an agent
  picks a proven graph instead of hand-rolling; the new `submit` action quarantines
  hand-rolled graphs for the quartermaster's study loop (`why_not_library` ≥ 80
  chars required, never joins the library), and `inbox` lists submissions newest-first.
  The discovery-first law is stated in SKILL.md alongside the `after_partial` law.
  Test: `tests/test_door_lib_50.py`, `tests/test_library.py`.

- #82 — @atbrace.
  fix(door): reap the silent runner death loudly before the respawn (#8 item 2).
  Crash-visibility for silent runner deaths: when a door RESPAWN path finds `wf.pid` dead with no valid
  `runner_exit.json` verdict, it appends `runner.reaped` + one `node.interrupted`
  per falsely-claimed running child (live children are adopted, never interrupted)
  BEFORE replacing the runner, so a SIGKILLed run is never mistaken for liveness.
  Read paths stay pure observers; node records are append-only; fresh launches and
  clean parked/held exits write nothing.
  Test: `tests/test_silent_death_reaper_8.py`.

## 1.1.2 — 2026-10-01

Includes all 12 merged PRs after v1.1.1, in merge order. Author handles are verified from the merged PR records.

- #39 — @jacobhausler.
  Add build-lane hygiene prompts that forbid testing a base revision over a dirty worktree, plus read-only journal replay with `scripts/lane_recover.py` to recover unfinished lane edits.
- #36 — @jacobhausler.
  Add the standalone `wf_dialect.py` JavaScript workflow exporter and constrained-subset importer; unsupported constructs are refused by name and lossy exports disclose dropped semantics.
- #45 — @jacobhausler.
  Make the knowledge graph canonical: one node per ID and one edge per source/target/relation, deterministic clean regeneration, and idempotent repair instead of duplicate accumulation.
- #60 — @jacobhausler.
  Add `list` provenance counters by folding the run records already being read; unstamped installations retain the prior payload shape.
- #46 — @jacobhausler.
  Resolve owner-configured `runs_root` consistently across the tool, runner and dashboard, and support a validated owner-configured profile fallback when the launch environment has no profile identity.
- #69 — @atbrace. Reject JSON-encoded `run_context` maps and seed strings that would leave `{run.KEY}` placeholders unbound, before creating a run or spawning a runner.
- #67 — @jacobhausler.
  Validate string-typed goals, contexts, gate questions and fan-out goal templates at submission, including falsy and explicit-null values; reject non-JSON echo output without coercing valid values.
- #72 — @atbrace. Honor `run` with `dry_run:true` as a write-free static graph preflight returning resolved models and routes; it does not ping providers, inspect quota or prove route liveness.
- #65 — @jacobhausler.
  Polish desktop node tones, edge flow and run headers through shared presentation models, with terminal animations stopped and per-instance SVG markers isolated.
- #86 — @atbrace. Add a validated incident-response lifecycle template combining verdict branches, a machine recovery probe and human escalation; infrastructure-specific lanes require adaptation and are not smoke-run.
- #64 — Hermes Agent; @jacobhausler.
  Daemonize POSIX runner admission so caller-tree cleanup sweeps cannot reap the run, and make the admitted runner the sole writer of `wf.pid`. This addresses only the caller-tree portion of #8; enclosing service/cgroup survival, crash visibility and idempotence remain open. Release validation also repairs the claim fixture's teardown race: wait for the killed detached runner to release ownership before deleting its files; keep all claim assertions unchanged.
- #68 — @atbrace. Validate gate `when` reference heads against the gate's direct/transitive `after` ancestry, matching the existing `inputs` and `fanout.items_from` rules. Sibling, ghost and self references are rejected at submission instead of silently skipping or holding a gate at fire time; valid ancestor references and parse-error reporting remain unchanged.

## 1.1.1 — 2026-09-29 — runner correctness (cross-container liveness, ancestor gate answers), profile-home fix, lane-clean gate, portable files, pill rail

Patch release: every merged PR since v1.1.0, in merge order. Solo default-profile runs stay byte-identical to 1.0.15 (golden-solo EMPTY diff re-run on this tree). Stdlib-only backend; desktop imports frozen to `@hermes/plugin-sdk`, `react`, `react/jsx-runtime`.

- #16 docs(skill) — Hermes Agent: the operator playbook is restored as `references/operator-playbook.md` (17 measured lessons: one-runner-per-worktree, wall-vs-turns sizing, dependency-ordered merges with per-merge suite, stall detection, 420 s host deadline / 330 s wait segments, alias-vs-literal route law, quorum-has-no-default, …); INSTALL.md links the bundled skill by symlink instead of forking a copy (the copy-freeze hazard behind issue #15).
- #20 test: exercise the crash net for real — contribution by @atbrace (#12, cherry-picked with authorship preserved; landed by Hermes Agent): `tests/test_systemexit_stamp.py` drives a genuine BaseException through the crash net (`concurrency:'abc'` → TypeError inside the executor) and asserts `crashed: TypeError`, closing #11's demand for an anti-false-green test. Code diff byte-identical to the contribution; `graphify-out/` regenerated under the CI-pinned extractor.
- #21 fix(suite): pass-gate admission (#17) — Hermes Agent: `scripts/suite.py --baseline <prior exits.json>` writes `admission.json` with exact red identities (test name + exit code) split `introduced` / `pre_existing` / `fixed`; exit 0 requires ZERO reds base or fix; a base red is never auto-waived (`blocking_base_reds` needs its own linked fix); a deleted base red reports `missing` and blocks (rm can't launder a red); malformed baseline = hard exit 2. Without `--baseline` the exit contract is byte-compatible. `tests/test_suite_admission_17.py` (21 checks, 8 red against the old suite.py).
- #26 → #24 fatal_quota — Hermes Agent; from a partner estate's report): a 429 whose own text carries a
  reset horizon (`resets in ~109h`) is classed `fatal_quota` — the node fails on the
  FIRST attempt (the bounded retry ladder can never beat a multi-day reset; the
  field case burned 3 spawns in 60s), and the model + horizon land in the seat quota
  cache (`$HERMES_HOME/cache/workflow-quota-cache.json`). The door refuses a launch
  whose node pins a cache-marked model — one recovery ping first, so a stale stamp
  can't wedge a model that recovered early. Minute-granularity horizons
  (`resets in 30 minutes`) are honored as minutes, not folded into the 6h default;
  a provider-less (seat-default) stamp recovers on any answered ping, an
  explicit-provider stamp only on a same-route alive ping (attribution law).
- #26 → #25 fail-closed pinned routes — Hermes Agent; field report fb-fix-9c575645): a node that pins an
  explicit model now gets `require_route` default TRUE — if the door's submit ping
  AFFIRMATIVELY proves the pinned route dead, or answered from the fallback ladder
  (recorded route != pinned route), the launch is REFUSED with
  `error_class=route_unavailable` instead of silently billing the seat's fallback.
  `require_route: false` (node or `defaults`) opts into the ladder explicitly;
  infra-absence (ping impossible) stays unknown = warn-only, never a false block.
  A ping that PROVES the route alive bakes `route_verified` (door-only proof — the
  validator carries the key for the runner; `_resolve_models` drops any author or
  stale value) and the runner's commit hold fails a node whose committed
  served_model contradicts the proof. Deep-review pass 2 hardened: policy keys
  (`require_route`/`route_verified`) are fingerprint-ignored like budgets, frozen
  replay comparisons exclude the proof annotation, quota refusal skips replay-skip
  nodes on amend, and a profile-routed hold reads the TARGET's aliases, never the
  runner seat's. Tests: `test_fatal_quota_24` / `test_require_route_25` (+ runner
  validator + forged-proof + def_hash + un-bake regression rows), golden-solo EMPTY
  diff holds (solo graphs pin no models → no new bytes).
- #29 runner lane-clean gate — Hermes Agent; feedback digest 29d, ledger 64c6772b): an agent node may declare `repo: <path>` — the git lane it owns. At commit the runner runs `git status --porcelain --untracked-files=no` on the lane; a done/partial over a lane with uncommitted TRACKED changes commits `failed` `error_class:"incomplete_work"` carrying `lane_dirty` (the porcelain), instead of the dad50be0 false-green where the fix died uncommitted in a capped child and downstream verified a HEAD equal to the mutant. Refusal, never auto-commit (no runner author identity); untracked never dirties; git-unable fails open; default-off keeps every existing graph byte-identical (golden-solo EMPTY). Door description, AGENTS closed set, and grammar.md document the field; `tests/test_lane_gate_64c6772b.py` covers clean/dirty/untracked/fail-open/mutation.
- #29 door schema legibility — Hermes Agent; feedback digest 29d, ledger 0b680871): the registered `graph` param description is built from adjacent short string literals (each < 400 chars) instead of one 3.4k-char line — the runtime string is byte-identical (pinned by `test_validator_caps` ROW 1 + a segment-boundary test); the "schema is truncated" report was core `search_files`' 500-char per-match clamp on that single line, not a core defect. Comment at `tests/test_validator_caps.py` fixed to stop blaming the schema validator.
- #28 desktop — pill rail for the session's live runs + expansion — Hermes Agent; #22 rail half; the
  auto-card half stays backlog): `railModel(ownedRuns)` (exported pure) folds the live
  set into pills — held first, then running by `started` desc, capped at 3 + overflow
  (same policy as `splitRuns`, which stays for the pane); `PillRail` renders one
  horizontal row of compact pills `[Dot][short name][done/total]` and gate-held pills
  keep `GateActions`; progress is `${nodes_done}/${nodes_total}` and `?` when either
  count is absent — never fabricated. Clicking a pill expands the run's existing
  `MiniGraph` (via `runQuery`) in a mini node-strip ABOVE the rail; clicking the open
  pill, Esc, a click outside the rail, or switching the focused chat collapses it.
  Open state is the in-memory `$railOpen` atom (`{sid, runId}`, never localStorage);
  `toggleRail` is the exported pure transition core. Theme via
  `var(--ui-sidebar-surface-background, var(--card))` + `--ui-stroke-secondary`, inline
  style only. Tests: `tests/test_pill_rail.mjs`, `tests/test_pill_rail_expand.mjs`.
- #28 desktop — the session strip mounts in `composer.underside` when the SDK offers it
  (#22): `register()` uses `COMPOSER_AREAS.underside ?? COMPOSER_AREAS.top`. On core
  >= v2026.7.30 the strip is the floating strip BELOW the composer dock —
  bottom-anchored, grows upward over the thread, and does not dim on scroll-up
  (composer/index.tsx:1558-1560). `COMPOSER_AREAS` is an SDK const map, so a missing
  key means the core doesn't mount the area at all; `??` then keeps today's
  `composer.top` slot on older shells. One mount, never both.
  `tests/test_register_surface.mjs` loads the module against both stub SDK shapes
  (area-set assertions per shape) and asserts hook-order safety: `SessionStrip` calls
  `useValue`/`useQuery` before its null return.
- #28 fix #23 (desktop): `SessionStrip` and the pane's `this chat` pill read the focused-chat
  atoms from `host.state.focusedSessionId` / `host.state.focusedStoredSessionId` — the SDK
  exposes them ONLY under `host.state` (sdk/index.ts:665-697). Reading `host.focusedSessionId`
  left `focusAtom(undefined)` → null sid → the strip never rendered and `owned` stayed empty.
  The test stubs previously placed the atoms at the SDK `host` top level — the stub encoded
  the bug; they now live under `host.state` with a red-on-base render assertion.
- #14 fix(door): resolve profile home through core; spawn runner with the OWNER's home — @atbrace: on a profile-scoped host `os.environ["HERMES_HOME"]` is the LAUNCH root, while core serves the actual profile through its context-local override (`hermes_constants.set_hermes_home_override`, installed per turn by the gateway's profile runtime scope). Door, runner and children silently used the wrong profile — seat config invisible, custom providers dead (`Unknown provider` on every node), run dirs under the wrong home. The plugin now resolves its home through core's documented contract and stamps the OWNER's home onto the spawned runner. Measured on both host shapes (`gateway run` under a systemd unit pinning the base root; `dashboard --open-profile <profile>`).
- #34 grammar-dialect-1 (#31) — Hermes Agent; review nits N1–N3 in b661496): `references/dialect.md` maps Anthropic's Claude Code dynamic-workflow JS grammar (agent/parallel/pipeline/phase/args/plain-JS glue/caps/worktrees) to our JSON graph with a filled DEVIATION JUSTIFICATION column per row, and `tests/fixtures/dialect/` (13 `.js` scripts + `.expected.json` verdicts) pins the constrained-subset import contract that #33 implements. Docs + fixtures only; no importer/exporter code.
- #35 publish-as-file (#32) — Hermes Agent: top-level `grammar: "wf/1"` accepted (absent = wf/1; unknown value refused listing the supported values), def_hash-neutral like `provenance`; `references/portable.md` convention + `examples/portable-review.workflow.json` walk-in with its digests pinned in the doc (`tests/test_portable_32.py`).
- #30 fix(runner,door): cross-container runner liveness + ancestor gate answers flow downstream — Hermes Agent. Runner correctness pair from feedback digest 20260929e (rows 91b9a3de, 00e46adb); both runner-internal, no desktop change, golden-solo EMPTY by construction. Companion door law (the probe-window admission retry): `run_state` publishes THE ONE liveness read (`runner_live`) and `act_wait` consumes it — two probes microseconds apart flip across a dying runner's kernel-released flock, which is what made `status` and `alive` disagree; the wait loop now re-attempts the respawn exactly for that shape (top said alive, now interrupted; 3 tries, 1 s throttle — the runner's own flock admission makes a double-spawn harmless). Rows:
  - 91b9a3de cross-container runner liveness (feedback digest 20260929e, recurrence
    of baa0088f19452326): our fleet runs runners in a sibling container on the SHARED
    `~/.hermes` volume with a separate pid namespace, so `os.kill(pid, 0)` in
    `runner_alive` reads a LIVE runner as dead and `status` reports `interrupted` /
    `next:[wait]` → the next `wait` spawns a SECOND runner over live children
    (the fb-squad false-interrupted incident, 20260929-131003). The kernel-enforced
    flock the runner already holds on `<run>/runner.lock` for its whole life is the
    ownership proof that survives pid namespaces: `runner_alive` is now
    `runner_lock_held(r) or _runner_pid_alive(r, pid_path)` — HELD ⇒ live, at every
    one of the door's call sites at once (status/next, the wait/stop/amend respawn
    guards, `list`), and the ORIGINAL pid-identity law (argv `wf.py run <id>` +
    effective-root match) is preserved verbatim as the fallback so a free lock or a
    foreign/crashed pid reads dead exactly as before. The probe takes the lock
    non-blocking and closes the fd (kernel releases), deliberately WITHOUT `O_CREAT`
    so a read sweep never litters historical run dirs. The fan-out ADOPTION path
    (`_verify_spawn_rec`, the sibling of the same pid-only pattern) is intentionally
    NOT relaxed: adoption runs inside the spawning runner's own namespace where the
    pid+skey-in-cmdline proof is verifiable — relaxing it would gut the PID-reuse
    guard. Tests: `test_cross_container_liveness_91b9a3de` (real peer-process holder,
    real-runner lifecycle end-to-end, mutation-controlled).
  - 00e46adb ancestor gate answers flow downstream (feedback digest 20260929e,
    spool 5ff2806f359c16a1): a go-gate's committed answer reached only the gate's
    DIRECT child (via the auto parent-injection loop), so a fresh verify two hops
    down graded a deliberate owner override as `C1 NOT met / authority UNVERIFIED`
    (fb-fix-030744bc). `build_inputs` now walks the node's transitive `after`
    closure and injects every ANSWERED ancestor gate's committed answer record
    (`gate_answer_valid` efp law, capped like any auto input, labelled
    `<id> (ancestor gate answer)`), so no downstream agent has to be told to read
    it. Fail-closed: a skipped / `on_skip:pass` gate has no `"answer"` key and
    injects NOTHING; ids already covered by an explicit `inputs:` ref or a
    direct-parent block are never duplicated. A solo graph with no released gate
    gains ZERO bytes here (golden-solo EMPTY holds by construction). Tests:
    `test_gate_answer_flows_00e46adb` (chain verify, no-duplication, skipped-gate
    fail-closed, explicit-coverage suppression, mutation-controlled).

## 1.1.0 — 2026-09-28 — bot-team features: optional `profile` / `requires` / `lane_key` / runs-root / provenance

Every feature is OPTIONAL: a no-team run (default profile, no `WF_RUNS_ROOT`) is byte-identical to 1.0.15 — proven by the frozen 1.0.15 suite pass-set plus a golden solo diff (`tests/golden_solo/v1.0.15.json`, EMPTY diff across six fake_hermes graphs) re-run after every lane merge. Stdlib-only backend; desktop imports frozen to `@hermes/plugin-sdk`, `react`, `react/jsx-runtime`. Sprint contract: `wf-team-sprint-1.1/RATIFY.md` (F1–F5; F6 cut, F7 skill-side). Integrator Hermes Agent; lanes A–F built by agent children (git author `Hermes Agent`), one writer per file.

- F1 runs root + run identity (scaffold 0e8b1e8, Hermes Agent): ONE resolver `wfcommon.runs_root()` (`WF_RUNS_ROOT` if set, else `$HERMES_HOME/workflows`) replaces the four inline computations in door, runner, read model and dashboard; `runner_alive` compares the runner's EFFECTIVE root from `/proc` environ (legacy env reduces to the 1.0.15 `HERMES_HOME == r.parent.parent` check verbatim). `run.json` gains `dispatched_by`, `launch_root`, `team`, `lane_key`, `targets[]`, `graph_source` ONLY when derivable; solo default-profile key set == 1.0.15. Children receive `HERMES_WF_RUN_DIR` so `inbox` resolves without guessing the root. `source_digest(graph)` = sha256 over canonical nodes JSON (`graph_fingerprint` untouched).
- F2 profile-routed agent nodes — lane C read model (e43639c, Hermes Agent / lane C) + lane A runner (4a2f15b, Hermes Agent / lane A): `profile` on an agent node (node-level only, `{run.KEY}` rendered BEFORE validation) runs the child AS a named teammate profile — delegation, not isolation. Validation before any write/spawn: non-empty profile DIRECTORY with `config.yaml`, never `default`, and the TARGET-owned `<profile_home>/workflow_team.json` lists the launcher in `accept_from` (launcher identity from the door's own HERMES_HOME, never a graph arg). Runner spawns with `-p <profile>` under an env whitelist + `HERMES_HOME=<root>`, re-checks the target before Popen (deleted target → typed `error_class:"spawn"` `profile gone: <name>`, never a launcher fallback), records `profile_home` on node records and reads child metrics from the target's state.db. `node_facts` render the routed profile.
- F4 `requires` output preconditions — lane C validation + lane A schedule-time check: `requires:{"<ancestor>":["field","dotted.path"]}` on agent OR gate nodes; every key must be in the `after` closure, every path a non-empty string. At wave scheduling a missing OR null value commits the node `failed` with `error_class:"precondition"` (`output.missing` lists the paths) with ZERO spawns; the run fails on the existing failed/blocked path. Retry ladders never see it; a resume re-evaluates (supply the field via `amend`). 1.0.x `skipped` semantics untouched.
- F3 `lane_key` in-flight registry — lane B door (097b717, Hermes Agent / lane B): `run` gains optional `lane_key` (≤128) and `team` (≤64); `status` accepts `lane_key` as an alternative to `run_id` (never spawns). Registry `<runs_root>/lanes/<sha16>.json` written tmp+`os.replace` under a per-key `fcntl.flock`; while an UNFINISHED incumbent holds the key a second `run` is deduped (`{deduped:true, run_id, state, runner_live, needs_resume, last_event_ts, hint}`) — the incumbent is resumed via `wait`, never replaced; terminal `stopped` releases the key. Stored key must EQUAL the supplied key (mismatch → `lane_key hash collision`, never a cross-key dedupe). Entries append-only; keys global per runs root. `list` rows carry `lane_key`/`team` when present.
- F5 library provenance (opt-in) — lane B door: `save` gains optional `source` (≤200). `{owner, source, saved_at, source_digest}` is written into the library entry ONLY when `source` is supplied or the saving door runs under a named profile; a default-profile save without `source` writes the 1.0.15 bytes exactly. Top-level graph key `provenance` (closed key set). `library` rows show the fields only when present; `run from:<name>` stamps `run.json.graph_source`. Attribution only — nothing reads it to allow or deny.
- Desktop — lane D (3a4aa70, Hermes Agent / lane D): node card and panel show `as @<profile>` for routed nodes; solo trees render structurally IDENTICAL to the 1.0.15 baseline (`tests/test_11_ui_imports.mjs` snapshot + frozen import baseline).
- Fixtures — lane E (0a505ba, Hermes Agent / lane E; integrator fixes 4805b0e, 79ae59d): `tests/test_11_integration_*.py`, `tests/fixtures/11-*`, `tests/11-golden-solo.py` capture/compare, `tests/11-*` kill/resume harness (20 cycles), claim-crash wrapper, env-leak probe, throwaway consent profile. Counted matrix on the merged tree: liveness 8/8, cross-profile wait 3/3, validation negatives 4/4, env 2/2, spawn-race 1/1, SOUL 3/3, harvest-DB 2/2, kill/resume cycles 20/20, dedupe 3/3, boundary kills 3/3, concurrent claim 1/1, precondition 5/5, provenance 6/6+1+1.
- Docs — lane F (5a7e631, Hermes Agent / lane F): SKILL.md team paragraph; `references/grammar.md` (`profile`, `requires`, gate `requires`, top-level `provenance`); `references/operations.md` (lanes/dedupe, runs root + identity + TRUST BOUNDARY: a shared `WF_RUNS_ROOT` is common trust, same UID; team fields are provenance, never an ACL).

## 1.0.17 — 2026-09-28

- Landed via #5 (repo owner) — `release 1.0.17 — catalog sync + F5/F6 replay & run_context fixes`: public tree synced with the PR #4-reviewed catalog fixes (visible `composer.submit` gate answers, launcher-only `hermes_bin`, disclosure docs), the 1.0.16 guarded per-route reasoning validation, and the two review fixes below (F5 replay, F6 brace guard). Review round R8 flipped the `f0f154d5`/`v4_fixes` regression tests to any-rule-match and refreshed `graphify-out`.
- R10 replay fix (review of #4, F5): an unstamped (pre-1.0.12) node record whose hash matches BOTH historical fingerprint rules now loads as committed (`done`), not `pending`. Two rules agreeing on the same hash prove the definition is unchanged; the old unique-match rule re-spawned every pre-1.0.12 no-budget node (measured: 25 nodes, 8 runs flipped to `interrupted` over 166 real run dirs). Fail-closed is now zero matches — a real definition change and a budget amendment against an old-rule stamp both still invalidate. Old run dirs stop re-running after upgrade.
- run_context brace guard (review of #4, F6): a `{run.KEY}` value containing `{`/`}` bound into a fan-out goal or `items[].goal` is rejected at the door before any run write. The runner re-renders fan-out goals per item (`fmt_goal`), which would have interpolated the bound value a second time against item fields — contradicting the documented no-interpolation-of-substituted-values contract. Non-fan-out goals are unaffected (rendered once at launch).

## 1.0.16 — 2026-09-28

- fb b3c98b2a0518a8f0: the 1.0.15 rewrite dropped per-route reasoning validation for non-Codex routes, falling back to global levels and accepting anthropic `ultra`; it also crashed coreless hosts on Codex nodes. Restore `route_supported_efforts` with a guarded `codex_supported_efforts` fallback for older cores and guard every host import.
- 1.0.15: a served forbidden model now fails the fan-out parent and its `node.failed` event with `forbidden_model` even when quorum otherwise succeeds; per-item evidence remains typed and downstream is blocked. Reasoning validation uses the installed core's Codex effort function for Codex routes; non-Codex routes fall back to global levels (corrected above). Added credential-free A-door validation and historical fingerprint-run regression tests (dad50be002da89d5, f0f154d5dd80220c).
- Regression pin for f0f154d5dd80220c: budget-bearing, unstamped historical runs report done under the cross-rule reader already shipped in 1.0.12 (7deffc3); an era-A reader reproduces stale/pending. Separate run-shape locks keep both-rule ambiguity, unknown hashes, and genuine budget-bearing amendments fail-closed. Test-only coverage of the earlier fix; no runtime change for this row.
- 1.0.14: model law enforced. Every node commit (solo and per fan-out item) stamps the ACTUAL served model + billing provider from the child metrics onto nodes/<id>.json (`served_model`, `served_billing_provider`); an absent sessions row stays null (unknown, never forbidden). If the served model hits the forbidden set (graph `model_policy.forbidden_models` ∪ seat `workflows_forbidden_models`), the node/item commits `failed` with `error_class='forbidden_model'` and blocks downstream. Seat-floor submit rejection runs BEFORE model resolution so a seat-forbidden name reports field=model instead of being swallowed as unknown-model (fb dad50be002da89d5).
- 1.0.12: fingerprint rule provenance on new run, node, spawn, gate, and runner-exit records. Readers verify each stamped record against the current graph using its own known rule; unknown versions and ambiguous unstamped hashes fail closed. Historical no-budget unstamped commits may replay instead of skip; restart old in-memory readers after deployment (95d7010295d70102).
- 1.0.13: `workflow run from=<name>` accepts optional `run_context`: a non-empty string
  delivered to every first-wave agent (including gate-first/parallel roots), or a
  string map that resolves only explicit `{run.KEY}` in node goals, contexts,
  fan-out goals/item goals, and gate questions. Invalid/missing bindings fail
  before run creation. Resolution follows defaults, precedes persistence and
  fingerprinting; the shelved graph is unchanged. `/wf <name> [note]` now sends
  its note atomically as a string seed rather than a post-launch steer. A seed
  does not replace baked target literals: reusable graphs must contain explicit
  `{run.key}`, `{run.branch}`, `{run.argv}` binding points. Bound values persist
  in prompts and must not contain secrets (fb 4052d57719653b1a).

## 1.0.11 — 2026-09-27 — false when-gate defaults to prune (decorative-gate footgun closed)


- A false `gate.when` with omitted `on_skip` now prunes the gate and exclusively dependent descendants in both human and wait-gate paths; `gate.skipped` reports `prune` to match the saved status. Explicit `pass` still permits the arm, and explicit `prune` is unchanged (fb e4c98f6e550e3a90). This does not change explicit `on_skip:"pass"` in existing graphs, including `library/fb-fix.json` before publish.

- Desktop gate answers now resume the run's owner session through the public
  composer SDK as a visible turn; on older Desktop builds, the resume text is
  inserted for the user to send, or the UI asks for manual resume. No app DOM
  query or private composer event (per maintainer review on
  NousResearch/hermes-agent#122099, teknium1).
- The workflow tool no longer accepts `hermes_bin`; only operator plugin settings
  or `HERMES_WF_HERMES_BIN` may select the child launcher. Tool-arg attempts
  fail closed (per maintainer review on NousResearch/hermes-agent#122099).
- Document the detached runner's lifecycle and the stop-before-disable rule;
  align the documented Hermes floor and catalog metadata.


## 1.0.10 — 2026-09-27 — child work dir is writable under HERMES_WRITE_SAFE_ROOT

- A child's advertised durable work dir (`<run>/work/<node>[.<i>]`) is now
  writable when the runner's env carries a non-empty `HERMES_WRITE_SAFE_ROOT`:
  the runner appends the child's OWN work dir (nothing wider — never the run dir,
  `run/work`, or `$HERMES_HOME/workflows`). Unset/empty is left exactly as
  inherited (unrestricted). Previously children silently lost artifact writes
  (fb 625a3241cfcc9dee). Moots 2c639b96 (fb-fix children could not write
  findings). The TMPDIR half of ea743553 is untouched.

## 1.0.9 — 2026-09-27 — amend keeps committed routes only where they replay

- amend keeps committed nodes' baked routes only where the node replay-skips;
  edited/re-running nodes re-route on the current seat; a node's own tier target
  is a known model (fb 034849a23af94418)

## 1.0.8 — 2026-09-27 — quorum cancels never fire blind

- A fan-out straggler cancelled at an explicit `quorum` now states its output
  state AT THE KILL INSTANT: `cancelled: quorum already met — no output on disk
  at quorum moment` vs `— had output at quorum moment (log N bytes, workdir M
  files)` (snapshot dict `cancel_evidence` rides the record). The kill itself
  is unchanged (A5 opt-in intact); the SIGKILL freezes the capture, so the
  snapshot is faithful (fb a2d7f66443910813).
- Harvest-at-cancel: a cancelled straggler whose frozen capture carries a
  valid fenced answer keeps it (`harvest` + `item.harvested_at_cancel` event).
  Classification and quorum math untouched — error_class stays `cancelled`.

## 1.0.7 — 2026-09-27 — door quorum blurb matches the runner

- The `workflow` tool's graph-param description told authors fanout `quorum`
  "defaults to majority ... stragglers are cancelled" — the runner cancels
  stragglers ONLY when quorum is EXPLICIT; unset quorum waits for every item
  (grammar.md always agreed with the runner; the door did not). Door copy now
  states: quorum OPTIONAL positive int; with it set, once N items commit the
  still-running stragglers get `error_class: cancelled` and are excluded from
  the failure math; without it the fan-out waits for all. Pin test
  `tests/test_door_quorum_copy.py` (red-first) pins the registered string.
  fb 2f9653b1; lane 89d86c9 (red-first pin) + a874255 (copy) + 0c5c05e (graphify).

## 1.0.6 — 2026-09-26 — suite ledger resets; fanout grammar named

- `scripts/suite.py` no longer seeds `exits.json` from disk: each run resets the
  ledger (atomic `[]` seed via tmp+os.replace) so a re-run into a non-empty ci-out
  reports the CURRENT pass only — no more `TOTAL 120` citing the previous run's
  stale red rows. fb 3d2175e9.
- `references/grammar.md` now names the fanout closed set exactly
  `{items | items_from, goal, schema, quorum}` (matching the validator at
  wfcommon.py:68) and documents `fanout.schema` (per-item reply contract, same
  rules as node `schema`) — previously undocumented, authors had to guess.
  fb aa9a65e0.

## 1.0.5 — 2026-09-26 — preflight LIVENESS ping (warn-and-surface)

- A graph pinned to a live-but-quota-dead seat used to launch happily and die hours
  later at the FIRST child spawn (terminal transport failure after the fixed 5s/20s retry
  ladder); `model_preflight` proves resolution, not liveness, and the provider's
  `Retry-After` header never survived to the door. Now: one `wf-preflight-ping`
  auxiliary request (max_tokens=1, ~10s timeout, explicit provider+model, one
  attempt) per DISTINCT resolved route at run AND amend submit annotates the existing
  `routes` entries with `liveness=alive|dead|unknown` + `retry_after_s` (never
  fabricated) + scrubbed note, and dead routes append a repoint hint. STRICTLY
  fail-open: every exception path (timeout, non-core host, transient 5xx, fallback
  answered cross-route) maps to `unknown` and the run LAUNCHES — warn-and-surface,
  no blocking, no new door schema keys. Ledger 152be7f7; lane de228d5+b28254f;
  targeted 34/34, mutation-checked (revert -> 30 FAIL), suite 61/61.

## 1.0.4 — 2026-09-26 — manifest floor matches the fleet

- `requires_hermes` reverted `>=0.21.4` -> `>=0.21`: the CI-pin (09-25) raised the
  floor above the fleet image (0.21.3), so the plugin was silently SKIPPED at boot
  (`Plugin 'hermes-workflows' skipped: requires hermes >=0.21.4, running 0.21.3`)
  and the workflow tool vanished after the next serve restart. The CI pin stays in
  the workflow file; the manifest must only state the floor the stock `-Q`
  contract actually needs. Deploy lesson: after restart, prove
  `Mounted plugin API routes: /api/plugins/hermes-workflows/` in gui.log —
  `/api/health` 200 does NOT prove the plugin loaded.

## 1.0.3 — 2026-09-26 — the feedback fleet's four lane fixes

Merged from the feedback-triage commander's verified lanes (each:
adversarial-check ruling, mutation-checked targeted test, full suite green):

- **Deleted-cwd runner resilience (5c37b19)**: the runner degrades instead of
  dying ENOENT when a written-to dir is gone (deleted cwd, wiped `gates/`);
  spawn garnish and when-skip writes survive, respawn clean.
- **Live-orphan adoption (790c6ad)**: a respawned runner ADOPTS verified live
  fan-out children (pid alive + skey-title-in-argv) instead of re-spawning from
  zero — no duplicate item.started, deadline re-armed from adoption.
- **Fan-out own-goal drift guard (fb12da4)**: when `fo.goal` declares
  `{item.FIELD}` and an item bakes a sibling's value, a loud `item.goal_drift`
  event fires and the template render wins (warn-and-prefer-template ONLY,
  never fail-closed); dangling `{item.X}` placeholders warn at spawn.
- **Validator honesty (validator-duo)**: numeric-bound rejections name the
  offending value AND the cap (`max_turns 240 exceeds cap 200 (must be a
  number in (0, 200])`); the wait tool's 1800s clamp echoes a `timeout_note`
  instead of swallowing it. Pin-test guards the registered graph description.

## 1.0.2 — 2026-09-26 — the run watches itself

Designed by a 4-seat blind counsel (four anonymized seats), critic-voted, and built in 8
write-set lanes. Four overhauls, six additions — every one paid for by a deletion, zero new
verbs, zero new graph keys.

### Launching is showing (no agent control)
- A session-owned strip above the composer shows every run the focused chat launched,
  live, from the dashboard API — no hooks, no TTL, no transcript. Cron launches (no owner)
  show only in the pane. The `transform_llm_output`/`on_session_end` auto-card machine
  (~90 LOC + 2 hooks) is deleted; `provides_hooks` is gone from the manifest.
- The `::workflow{}` directive is now an agent-authored evidence PILL: one line, click to
  expand in place. The tool hands you the exact `card` line; paste it alone in the reply
  that launches or reports a run.
- KEY PAIRING (measured): `owner.session_id` is the runtime id (pairs with
  `host.state.focusedSessionId`); `owner.ui_session_id` is the desktop stored id (pairs with
  `host.state.focusedStoredSessionId`). The SDK exposes both atoms only under `host.state`
  (sdk/index.ts:665-697); reading them at the top level was the #23 render bug, fixed in
  the follow-up below.

### Explorer V2: one node truth, two readers
- `wfcommon.node_facts` is the closed record the panel AND `status` read: `error_class,
  attempts_log, final` (the child's last words), `log_path, prompt_path, efp, steer`.
- NodePanel replaces the Drawer: FACTS column + Log/Output/Prompt/Final tabs, default by
  status; live log tail (4 s, only while open); `unknown` for absent, never zero.
- New read-only route `GET /runs/{id}/nodes/{nid}/log` (contained tail-seek). The
  partial-output drop in `_view` is fixed.

### WORKFLOWS beside SESSIONS | BOTS
- Top-level pane tab (the hermes-bots dock pattern), grouped NEEDS YOU / RUNNING / DONE
  with the one act-on fact per row; `WORKFLOWS · n` title is the sole live count.
- Deleted: the sidebar nav row, the status-bar LiveChip, the hand-rolled runs column.

### Archify: no (verdict + evidence), SMIL for candy
- Edges leaving a running node animate; one fan-stack implementation.

### Additions
- The prompt as sent is durable at `logs/<id>.a<n>.prompt.md` — "what did my child get?"
  is one file read; the `<prompt>` redaction and tempfile dance are gone.
- Failed/partial nodes ship their facts (last words, attempts, paths) in `status`; no
  detail:full + tail dance.
- Every `status`/`wait` ends with `next`: release/wait/amend rows derived from state, or [].
- Children start in a durable `<run>/work/<node>/` cwd — the deleted-cwd crash class dies;
  the write-first law moves from three doc homes into the spawn CONTRACT.
- Fan-out without `quorum` waits for ALL items (stragglers cancel only on an explicit
  `quorum`); the commit threshold is unchanged — partial credit survives.
- budgets.md shrinks to the `shape` presets the door already bakes; a test locks doc==code.


## 1.0.1 — 2026-09-25

Measured against 80 runs / 64 postmortems: 30.6 % of failures died at 0 s on a route or
grammar mistake, 50.8 % were untyped deaths, and authors wrote the same boilerplate in half
of all graphs. Every item below is a fix for something the census counted.

### The door validates from lists
- `(model, provider, reasoning)` validated against the seat catalog at `run`/`amend`;
  `provider` inherited from a provider-qualified alias; `reasoning` validated per route with
  the supported list and nearest level in the error; near-miss model suggestions.
- Full-graph submit validation: unknown keys per node type, `when` syntax, gate vocabulary.

### Deaths become outcomes
- `error_class` on every `node.failed` from a closed set: typed turn-cap deaths
  replace the old `max_turns` classification, `schema` replaces `no_json`; new
  `early_death`, `unresolved_model`, terminal transport failures, `incomplete_work`
  and `graph_invalid` classifications. Exact error-class names: [operations](references/operations.md).
- Harvest-on-death: a child that dies after printing a valid fenced answer is committed as
  `status: partial`; downstream runs on it; the death cause stays as `error_class`.
- Bounded auto-retry: transport, early-death, turn-cap and timeout failures with tool progress
  get exactly one re-drive with a machine resume preamble (`node.retry`); permfails never.
- Stop ≠ failure: `cancelled` is excluded from quorum and failure math; a stopped run reads
  `stopped` and `wait` re-drives the cancelled work.
- Child liveness: no output within 120 s of spawn → `early_death`; `last_tool_at`/`idle_s`
  surfaced for live nodes. Extend-not-kill: a child still writing at the wall gets one 50 %
  extension (`node.extended`).

### The graph carries less
- Graph-level `defaults:{schema, timeout, max_turns, reasoning, provider, model, context}`.
- `shape: recon|build|review|publish` fills budgets from measured p95 presets.
- Schema-derived reply contract written by the runner; last-balanced-object extraction on
  noisy stdout. Authors stop writing contract prose.
- Direct parents' outputs auto-injected under `## Inputs` (8 KB cap per parent).
- `fanout.goal` optional; quorum defaults to a majority; stragglers cancelled at quorum.
- `echo` node type commits a constant with no spawn.
- Gate `default_option` + `hold_timeout`: parks at zero tokens, auto-releases or logs
  `gate.expired` once and keeps holding.

### Operator surface
- The inline card is registered by `run`, `wait` and `status`; `card_rule` prose is gone.
- Steer honesty: `steer.queued/baked/consumed` events; per-node counts in `status`.
- Budget keys (`max_turns`, `timeout`, `run_budget`, `shape`) leave the node fingerprint:
  raising a wall no longer invalidates committed work.

## 0.9.0 — 2026-09-24

### Added
- Boolean is accepted as an output schema type, so a node can contractually return a
  simple yes/no verdict (pre-validation whitelist plus runner-side validation).
- Submit-time model/provider preflight: a graph whose node routes to a dead or unknown
  seat alias is rejected at submit, after the route table and before the first wave
  spawns anything.
- Per-shape budget recipes reference (`references/budgets.md`) with an authoring-skill
  pointer, distilled from measured runs (42 completions vs 9 timeout deaths).
- Every `node.failed` event now carries typed fields — `error_class` and `attempts` —
  so a parent agent reads failure facts instead of inferring them from an exit code
  and prose.
- B1 cooperative steer: a running child can pull late steering itself via the
  `workflow` tool's `action="inbox"`. Steering text is baked per-spawn with a
  high-water mark and a delivery cursor; the read model exposes the evidence.
- Transcript card delivery is kept honest across held-launch replays: the auto card
  survives past blank or interrupted final turns instead of being dropped.
- Fan-out stacks can expand: the stack badge toggles the stack open into per-item
  cards on the canvas.

### Changed
- `status` and `wait` are compact by default: mid-run payloads carry output pointers
  instead of full bodies, `detail: "full"` opts in, and terminal (finished) payloads
  are always full.
- Canvas wrap law: depth columns fold into width-fitted bands, and backwards
  band-to-band edges route through an orthogonal gutter instead of slicing cards.
- Steer is truthful about liveness: steering a finished (terminal) run or node is
  refused honestly rather than quietly accepted.

### Fixed
- Metrics guards: torn metric objects can no longer crash the desktop views
  (item chips, item detail, mini-graph, timeline, run header), and a missing
  `api_calls` count renders as `unknown` rather than `0`.
