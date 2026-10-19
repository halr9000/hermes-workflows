# AGENTS.md — front door for agents

You are an agent. This file is written for you. It covers **installing** the plugin,
**operating** workflows once installed, and **contributing** to this repo. Each
section is self-contained; jump to the one that matches your task. Commands are
exact and copy-pasteable. Where a claim needs proof, the proof command is given.

---

## 1. What this is (30 seconds)

`hermes-workflows` is a [Hermes Agent](https://github.com/NousResearch/hermes-agent)
plugin. It registers one tool, `workflow`, that runs a **DAG of agent nodes** as a
background process owned by the calling session, plus a desktop DAG view.

- **Backend half** (`__init__.py`, `wf.py`, `wfcommon.py`, `dashboard/`): Python,
  stdlib-only, lives under `~/.hermes/plugins/hermes-workflows/` on the machine that
  runs `hermes serve`. Registers the `workflow` tool, the `workflow` skill, and
  read-only dashboard routes.
- **Desktop half** (`desktop/plugin.js`): plain ESM on the Hermes Desktop plugin SDK
  surface only. Lives under `~/.hermes/desktop-plugins/hermes-workflows/` on the
  machine that runs the **app**. When app and backend are the same machine, Hermes
  copies it for you; otherwise you copy it by hand.

Children are spawned as `hermes chat --query-file … -Q --max-turns N` — the stock
quiet one-shot CLI contract. **No patched Hermes is required.** See
[docs/patched-core.md](docs/patched-core.md) for the one optional field.

---

## 2. Install

### 2a. Catalog install (stock Hermes)

```sh
hermes plugins install hermes-workflows
hermes plugins enable hermes-workflows
hermes plugins validate ~/.hermes/plugins/hermes-workflows
```

Then restart the process that SERVES the plugin API — tools and dashboard routes
mount only at serve/dashboard start. `hermes plugins enable` alone hot-loads gateway
*commands* only; on rigs where the dashboard is a separate systemd unit
(`hermes-dashboard` vs `hermes-gateway`), restart whichever unit hosts `hermes serve`
— restarting the wrong one leaves every run reading `unknown` while the plugin looks
enabled. **Proof it loaded:** this line in the gateway's `~/.hermes/logs/gui.log`:

```
Mounted plugin API routes: /api/plugins/hermes-workflows/
```

A `401` on `/api/plugins/hermes-workflows/runs` is *not* proof — the auth layer
answers 401 for any path, mounted or not.

### 2b. Remote desktop app

If the app runs on a different machine than the backend:

```sh
mkdir -p ~/.hermes/desktop-plugins/hermes-workflows
cp desktop/plugin.js ~/.hermes/desktop-plugins/hermes-workflows/plugin.js
shasum -a 256 ~/.hermes/desktop-plugins/hermes-workflows/plugin.js   # compare to the repo's file
```

The app fs-watches that directory and hot-loads. If the WORKFLOWS tab is missing from
the SESSIONS | BOTS | WORKFLOWS strip: Settings → Plugins (toggle on), then ⌘K →
*Reload desktop plugins*.

### 2c. From a release zip

See [INSTALL.md](INSTALL.md) — verify the `.sha256` sidecar and `SHA256SUMS`, unpack,
copy the backend and desktop halves, validate, enable, restart.

### 2d. Optional: typed turn-cap deaths

Stock Hermes records a child that dies on its turn cap as `error_class: unknown`
(partial output and log preserved). Applying the one-field core patch in
[docs/patched-core.md](docs/patched-core.md) makes it `error_class: cap_exhausted` with the
loop's own reason. It is an operator choice; the plugin never patches your core. After
a failed child, `status` reports `turn_report: typed|untyped` so you can tell.

---

## 3. Operate

The bundled `workflow` skill ([SKILL.md](SKILL.md)) is loaded into any session that
has the plugin; it is the authoritative grammar. This section is the operating loop.

### 3a. The loop

```
workflow { "action": "run",    "graph": {…} }             → { run_id, routes, … }
workflow { "action": "wait",   "run_id": "<id>" }         → repeat until status ∈ {done, failed, stopped}
workflow { "action": "status", "run_id": "<id>" }         → read-only; never spawns
workflow { "action": "release","run_id": "<id>", "gate_id": "<gate>", "answer": "…" }
workflow { "action": "stop",   "run_id": "<id>" }         → terminal, not rollback
```

`wait` blocks while a verified runner is live and self-yields ~330 s with a
"call wait again" note to stay under the harness tool deadline. **Stay in the loop.**
Push delivery is best-effort, not guaranteed. `wait` can resume an interrupted run;
`status` is read-only. Completion grants no new authority, and cards/curation are optional.

### 3b. Minimal graph

```json
{ "name": "check",
  "nodes": [
    { "id": "inspect", "type": "agent",
      "goal": "Inspect <target>. Return one fenced JSON object {ok, findings}.",
      "model": "<model-id>", "provider": "<provider-name>", "reasoning": "medium",
      "max_turns": 20, "timeout": 900 }
  ] }
```

Or, the 1.0.1 way — settings once, nodes carry only their work:

```json
{ "name": "check",
  "defaults": { "model": "<model-id>", "provider": "<provider-name>", "reasoning": "medium",
                "schema": { "type": "object", "required": ["ok"] } },
  "nodes": [
    { "id": "inspect", "type": "agent", "shape": "recon", "goal": "Inspect <target>." },
    { "id": "verdict", "type": "agent", "after": ["inspect"], "goal": "Judge the inspection." }
  ] }
```

Rules that bite:

- **Pin `model` + `provider` on every node** unless you have verified the seat
  default is the route you want. An unset model rides the seat default — including
  fan-out children — and a usage-capped or wrong-provider default surfaces as
  `provider_400`/`429` on every child.
- **`reasoning`** rides `hermes chat --reasoning`, validated PER RESOLVED ROUTE at the
  door: the level is checked against the (provider, model) route's supported set and a
  level the route doesn't support is REFUSED at submit with the supported list named
  (and the nearest level suggested) — never silently clamped or downgraded. Some relays enum-gate a
  narrower subset at runtime; on the server's own 400 list the runner clamps
  `--reasoning` to the nearest supported level (never escalates) and re-drives
  once, logged — `provider_400` stands only when nothing clamps; then read the
  server's message and pick from that list.
- **`after` orders AND injects.** Every direct parent's committed output lands under
  `## Inputs` automatically (8 KB per parent). Use `inputs:["a.key"]` only to pick a
  dotted path or a non-parent ancestor, or `fanout.items_from:"a.items"`. A missing
  path fails at spawn.
- **Shared settings go in `defaults`, once.** `defaults:{schema, timeout, max_turns,
  reasoning, provider, model, context}` at graph level; `shape:recon|build|review|publish`
  sizes budgets from measured presets. A `schema` makes the runner write the reply
  contract itself — no contract prose in goals.
- **Leave budgets unset and name a `shape`; see [references/budgets.md](references/budgets.md).**

### 3c. Fan-out, gates, branches

```json
{ "id": "lanes", "type": "agent",
  "fanout": { "items": ["alpha", "beta"], "goal": "Audit {item}. Write <path>/{item}.md first.", "quorum": 1 } }

{ "id": "approve", "type": "gate", "after": ["lanes"],
  "question": "Ship the report?", "options": ["ship", "hold"] }

{ "id": "go",   "type": "gate", "after": ["judge"], "when": "out.judge.verdict == 'ship'", "on_skip": "prune" }
{ "id": "hold", "type": "gate", "after": ["judge"], "when": "out.judge.verdict != 'ship'", "on_skip": "prune" }
```

A human gate holds until `release`; the desktop view shows the question and hands
the answer back to the owning session. `hold_timeout` + `default_option` makes a
decorative gate release itself (`gate.auto_released`); `hold_timeout` alone logs
`gate.expired` once and keeps holding. Fan-out `quorum` (optional) races: once N
succeed, the rest are cancelled; without it the fan-out waits for every item;
`goal` is optional when items carry their own. Machine gates: `wait:{"wait_s":N}` or
`wait:{"until_argv":[…],"every_s":60,"timeout_s":3600}`. Tested examples:
[examples/basics/approve-publish.json](examples/basics/approve-publish.json),
[examples/basics/branch-on-verdict.json](examples/basics/branch-on-verdict.json).
[examples/release/gated-publish.workflow.json](examples/release/gated-publish.workflow.json) is a
human-gated publish TEMPLATE built from the approve-publish seed (draft -> owner
gate -> complementary `when`+`on_skip:"prune"` pair on the answer -> exactly one
arm; no node names a vendor model — role vocabulary lives in prose, routing is the
seat default). Launch seeds `run_context:{"artifact":"<name>"}`. Validate + author
run tested (receipts/gated-publish/).
[examples/release/machine-watch.workflow.json](examples/release/machine-watch.workflow.json) is a
scheduled-watcher TEMPLATE (zero-token machine-gate poll, releases on exit 0;
launch with `lane_key` so cron double-fire dedupes; timeout fails loudly for
the next deduped dispatch). Validate + smoke-run tested.
[examples/ops/incident-response.json](examples/ops/incident-response.json) is a full
lifecycle TEMPLATE (alert-triggered incident: verdict-branch gate pair, machine
recovery probe with human escalation, merge-gated close) meant to be adapted —
its `run_context` seeds and the sweep-adapter contract are the swap points; it
validates but is not smoke-run (real lanes need your stack).
[examples/review/blind-council.workflow.json](examples/review/blind-council.workflow.json) is
a review-council TEMPLATE (blind independent seats -> synthesis with mandatory
verify-list). Its seats pin CAPABILITY CLASSES (`council_a`, `council_b`) bound
through `settings.models` — no vendor model id appears in the file; an unbound
class fails the launch closed. Author-run receipt:
[receipts/blind-council/author-run.json](receipts/blind-council/author-run.json).
[examples/build/triage-route.workflow.json](examples/build/triage-route.workflow.json) is a
queue-triage TEMPLATE (intake -> classifier -> complementary `when` +
`on_skip: prune` carried by the arm heads themselves: human escalation gate with
the answer consumed as data, or a rule-driven batch fan-out). The classifier emits
a `has_urgent` boolean because the when grammar cannot see array length; each arm
head hangs on the classifier and carries its own predicate (prune law: a cascade
skip needs ALL `after` deps skipped, so only what hangs BEHIND a dead head
cascade-prunes — the head's own `when` decides its fate); the fan-out head is an
arm-scoped `batch-prep` forwarding the routine set, not the classifier itself.
Author-run receipt:
[receipts/triage-route/author-run.json](receipts/triage-route/author-run.json).
[examples/build/quorum-probe.workflow.json](examples/build/quorum-probe.workflow.json) folds
the speed/coverage contrast into one file: a `quorum`-raced probe fan-out whose
report must state the cancelled straggler set honestly, beside a no-quorum barrier
that reconciles survivors against an explicit master catalog so a dead lane prints
as a loud coverage gap, never silent loss. The two fan-outs are sibling nodes with no
branch gates, so every author/peer lap walks both folded modes in a single launch;
laps exist per-engine-side, not per-mode.
[examples/build/bulk-transform.workflow.json](examples/build/bulk-transform.workflow.json) is a barrier-fan-out TEMPLATE (declared manifest -> per-file transform lanes -> audit that reconciles all_results against the manifest via an ancestor `inputs:` ref and re-checks the filesystem itself; evidence-without-command is a failed lane). Smoke-run on the author's deployment: [receipts/bulk-transform/author-run.json](receipts/bulk-transform/author-run.json).
[examples/build/census-fanout.workflow.json](examples/build/census-fanout.workflow.json) is a
census TEMPLATE (fan-out auditing N generic machine items with a DETERMINISTIC
TALLY): an echo roster node carries the master item list, a no-quorum barrier
audits every item against `{item, observed, ok}`, and the tally synth READS
`audit.all_results` against the roster — every item answered or NAMED missing,
counts computed from the record, never prose. One item (battery-gauge) is a
designed dead gauge so the tally's loud-row law is exercised on every run; the
report node consumes the tally via `after_partial`. Two author laps — honest
(dead gauge as a loud red row) and severed (a child cut from the record becomes
a NAMED missing row): [receipts/census-fanout/author-run.json](receipts/census-fanout/author-run.json).
[examples/review/escalation-ladder.workflow.json](examples/review/escalation-ladder.workflow.json) is a verify-then-branch TEMPLATE (builder -> fresh independent verifier -> when-pair carried by the two human arm gates themselves: verified lands behind the land gate, failed escalates to a human hold with the whole packet; core has NO loop idiom - retry ownership is spelled out, not implied). Both arms smoke-run: [receipts/escalation-ladder/author-run.json](receipts/escalation-ladder/author-run.json).
[examples/basics/exchange-run.workflow.json](examples/basics/exchange-run.workflow.json) is the
byte-portability TEACHING PAIR — the file itself is the artifact that travels the
save → library → re-run loop (the loop's verbs, told in the description; the proof
laps live in the author receipt, never in node prose). An echo commits the token at
zero tokens, a courier copies it as pure data from its declared input, and the leaf
proves the hand-off only by re-reading both committed records from its `## Inputs` —
never from chat memory. No node names a model; children ride seat defaults.
Author-run receipt: [receipts/exchange-run/author-run.json](receipts/exchange-run/author-run.json).

Library templates: `workflow {"action":"library","name":"<name>"}` or
`/wf show <name>` lists required `{run.KEY}` inputs and an instantiate command.
Supply explicit nonempty string values with `run from:<name>`; the door binds
all shared text fields and refuses any surviving ref before writing or spawning.
Optional `save params:{KEY:{desc,default}}` describes those refs in `meta.params`;
missing/dead declarations on an envelope save warn (non-fatal `s12`; bare saves
derive the contract from refs and never warn s12), omission retains prior params,
and `{}` clears them. Defaults are suggestions, never automatic launch inputs.
Inline unbound machine argv remains refused; ordinary inline text keeps its
existing verbatim leniency.

### 3d. Failures, resume, amend

A graph may reserve one existing agent with `on_exit:"<node id>"` for a once-only
terminal postmortem. It is not ordinary DAG work; it receives terminal run facts
in `## Inputs` and commits `on_exit_receipt.md` without changing the run verdict.
See [grammar](references/grammar.md#terminal-postmortem-optional) for validation,
publication guards, and crash recovery.

- `node.failed` events carry `error_class` from the closed set defined in code
  (`wf.py ERROR_CLASSES`) — `bead_close | budget_exhausted | cancelled | cap_exhausted | config_input | crashed | early_death | effect_receipt |
  fanout_empty | fatal_quota | forbidden_model | incomplete_work |
  inputs | item_record | killed | lane_wreckage | lease_busy | left_live_descendants | malformed_turn | precondition | provider_400 | quorum |
  ratelimit |
  route_substitution_denied | route_unavailable | schema | seat_cap | seat_unsupported | seat_wait | spawn | stale_graph | timeout | transport | transport_exhausted |
  unresolved_model`, plus `unknown` as the harvest-time default when nothing matches —
  plus `attempts`. Read the class, not the prose. `cancelled` (a `stop`, or a fan-out
  straggler at quorum) is never a failure: the run reads `stopped`, and a `wait` re-drives it.
  `budget_exhausted` (est-3acn): a RunBudget cap (graph-root or node `budget:{tokens:N}`; tokens-only —
  USD is excluded per the repo cost-trust law, issue #124) refused the spawn BEFORE submit, logged as
  `dispatch.refused` and never billing (zero attempts); the runner closes the run gracefully as
  `partial` and `budget_ledger.json` names the never-launched nodes. Terminal: outside every retry ladder.
  `seat_wait` (est-g2xx): no global agent seat (`WORKFLOW_MAX_SEATS`) freed within the node wall — the spawn never happened.
  `seat_unsupported` (est-g255): the semaphore was full and the host exposes no process-ancestry
  channel (neither `/proc` nor `ps`), so a nested runner cannot prove which held seat is its own
  lending ancestor — refused at once instead of deadlocking to `seat_wait`; nested runners on such
  a host are unsupported (set `WORKFLOW_MAX_SEATS=0` there). A fan-out item waiting on a seat when
  `quorum` is met ends `cancelled` and never spawns.
  `seat_cap` (est-2ek.1.856): the seat-cap knob (`WORKFLOW_MAX_SEATS` env or config
  `workflows.max_seats`) named a non-integer or a value outside [4, 16]. The door refuses the
  whole launch before any write or spawn; an already-running runner fails the node. Never
  retried, never silently defaulted to `SEATS_DEFAULT`.
  `left_live_descendants` (#61): the runner is process-tree aware — an exit-0 spawn is
  believed only when its own process group is empty (the /proc walk behind liveness). A
  child that BACKGROUNDED the real work (detached suite) and printed progress chatter is
  `failed` with the verdict law in the error, a fenced answer over a live tree commits
  `partial` with the tree killed and proven dead, and a retry never re-spawns while the
  prior attempt's tree lives: the ladder kills it and proves death by /proc re-walk, or
  fails closed. The walk is recursive and the tracked pid-set persists across retries
  (#61b): double-fork/setsid descendants that reparent before exit are still found,
  counted, and re-adopted by every later judgment — never under-counted. An unreadable
  /proc is UNSAFE, never empty: judgment and quarantine probes fail closed typed when
  the process table cannot be read (machines without procfs keep the honest-empty
  degradation). An adopted orphan whose tree outlives it never commits a clean `done`
  — the completeness error rides WITH the answer (`partial` + this class). The class
  is terminal — never re-driven by either retry ladder. #448 closes the reaping-race
  false-red: zombie, vanished-row-with-ESRCH and drained-during-check all count as
  REAPED (a bounded settle precedes every stuck verdict; the commit-edge quiesce's
  dead-or-empty result returns the record untouched, so a banked-complete node
  commits instead of dead-lettering); fail-closed stands only for a tree still
  non-zombie live after the settle.
  `stale_graph` (est-byft8): a spawn that queued on the global agent seat and reached
  the Popen instant AFTER an amend moved its effective fingerprint is refused typed —
  the queued definition never launches. The comparison is the replay-skip efp law
  itself (budgets are not work: a timeout/max_turns-only amend never refuses), so a
  reload would re-drive exactly the nodes the guard refuses. The wave boundary then
  consumes `restart.request`, reloads, and re-drives the node under the CURRENT
  definition; the refusal releases its seat ticket and is outside both retry ladders.
  An unreadable `graph.json` at the check fails OPEN (the guard catches detected
  staleness, never invents a death).
  `config_input` (est-tmuu, operator-verified 2026-10-02): a child that dies rc!=0 within
  ~1 s of spawn AND whose capture carries a provider/alias config-error marker (`Unknown
  provider '<alias>' …`) is a deterministic INPUT error, not transient transport — the node
  fails on the FIRST attempt (the class is outside both retry ladders, the respawn budget is
  never burned) and the error names the fix: the node's provider pin or the seat config.
  `fatal_quota` (#24): a 429 whose own text carries a reset horizon beyond the run's
  reach — fails on the FIRST attempt (the retry ladder cannot beat a multi-day reset)
  and stamps the model into the seat quota cache; the door then refuses a launch on
  that model until the horizon passes (one recovery ping first).
  `lease_busy`: a child that exited rc=130 with the CLI's own fail-closed lease notice
  (`Stopped waiting for another Hermes process on this session. Your message was not
  processed.`) lost the session-lease race — another Hermes process held the session
  for the whole CLI lease wait (~30 min field-measured) and the message never reached
  the model. A pure collision, never a verdict on the work: ONE bounded re-drive as a
  FRESH session under the next attempt key (`#a{n+1}`, never `--continue` of the busy
  one — that lease belongs to someone else), preceded by `node.retrying
  error_class=lease_busy` and the 5 s bounded backoff. The tool-progress gate is
  inverted here on purpose: the CLI's notice PROVES nothing of the attempt ran, so the
  replay-safety evidence the other bounded classes need cannot exist; a harvestable
  fenced answer still outranks the class (#4 harvest law runs first). A rc=130 death
  WITHOUT the notice keeps its existing classification untouched.
  `ratelimit` (#54): a credential-window 429 — the child's stdout carries the CLI's
  own `... credentials are rate-limited for <model> ...` banner. Classified BEFORE
  transport/fatal_quota (the banner wins even when quota phrases co-occur: this death
  never stamps the quota cache — the seat is fine, the credential window is closed).
  The node PARKS at jittered ~5-minute intervals (tunables `ratelimit_interval` /
  `ratelimit_jitter`, run.json meta-only like `retry_backoff`). The wall is a real
  bound: a park fires only when the WORST-CASE jittered wait (interval × (1 + jitter))
  plus a real respawn window (≥ 15% of the wall, floor 0.25 s) still fit inside it —
  otherwise zero parks — the deadline is rechecked after the wait, and every spawn
  after a park is CLAMPED to the remaining wall (a park can never buy a fresh full
  timeout). A banner death discovered AFTER a ladder respawn reaches the same park
  (the transient ladder hands it over, attempts included). The wait also observes the
  fan-out quorum cancel: a satisfied quorum never waits out a parked straggler.
  Each park emits `node.retrying error_class=ratelimit backoff_s=N`;
  give-up commits the verbatim banner as the error text (plus `ratelimit_banner`) so
  a dispatcher can switch model instead of requeue.
  `route_unavailable` (#25): a pinned route the door's ping affirmatively proved dead
  or answered-from-the-fallback-ladder (submit refusal), or a committed served_model
  that contradicts the door's alive-proof (commit hold) — never silent fallback billing.
  `incomplete_work` (digest 29d / 64c6772b): a node with `repo: <lane>` committed done/partial while
  the lane still had uncommitted TRACKED changes — the runner refuses the false hand-off and the
  record carries `lane_dirty` porcelain; commit in the lane, then amend/re-run re-drives the node.
  `lane_wreckage` (est-2ek.1.660): the boot-time twin of the lane gate — a (re-)drive whose graph
  declares `repo:` lanes is checked at STARTUP (`git status --porcelain --untracked-files=no`, the
  #80 boot-sweep shape: admission blocked BEFORE any Popen, never a silent continue). A dirty lane
  refuses the re-drive — the child would execute onto the dead attempt's uncommitted wreckage and
  false-green on it hours later. The typed failure names the dirty files and both banking paths:
  `git stash push -m redrive:<run>:<node>` in the lane, or a patch at `<run>/wip/<node>.patch`.
  Bank the WIP and the re-drive proceeds; untracked files never block, git-unable lanes fail open,
  and an undeclared lane is never scanned (scan-free law).
- A child that dies after printing a valid fenced answer (rc≠0, wall, cap) is committed as
  `status: partial` with the death cause kept as `error_class`; the harvest stays readable, but a
  plain after-edge does NOT release onto it — the descendant fails typed `blocked_by_partial_ancestor`
  unless it opts in with `after_partial: true` (leaf close-out and the node's own fanout partial-credit
  merge are unaffected). A producer may opt its own schema-valid harvest into `done` with
  `harvest_policy: "validate"` (default `strict`; the death stays on the record as `error_class` +
  `clamped`) — see [references/grammar.md](references/grammar.md).
- `transport | early_death | cap_exhausted | timeout` deaths with tool progress get ONE
  automatic re-drive with a machine resume preamble (`node.retry`); permfails never retry.
  A harvested `partial` of those classes takes the same re-drive (est-ggqs, #256); an
  answer-less re-drive keeps the harvest.
  If the prior attempt's session persisted NO messages (#102 — the pretense-resume shape:
  a SIGKILL-wave child that banked nothing), the re-drive spawns as a FRESH session seeded
  instead with a harvest preamble (banked work-dir files + cleaned log tail); the
  `node.retry` event and the `attempts_log` entry stamp `fresh_session: true`.
  `lease_busy` gets the same ONE bounded re-drive but ALWAYS as a FRESH session under the
  next attempt key (never `--continue` the busy session) and WITHOUT the tool-progress
  precondition — the CLI's own lease notice proves the attempt never ran; its machine-wait
  event is `node.retrying error_class=lease_busy`.
- A child silent for 120 s after spawn is killed as `early_death`; a child still writing its
  log when the wall fires gets one 50 % extension (`node.extended`), then dies.
- A wall-killed child's corpse is banked (#131): `nodes/<id>[.<i>].corpus/` holds `work/`
  (its durable work dir), `stdout_tail.txt` and `manifest.json` (`last_written` first); one
  `node.banked` event, and the death record's `banked` points at it. A failed bank logs
  `node.banked` with `error` and never changes the timeout verdict.
- A run with unfinished work and no live runner is `interrupted`. Inspect committed
  outputs, then `wait` to resume — finished nodes replay-skip by fingerprint.
- **Partial-rescue law (#134):** the sole recovery state of a run is the committed
  `nodes/*.json` done-set parsed against the CURRENT `graph.json` — validity is
  the `efp` each record carries recomputed against the live defs, so editing the
  submitted graph changes exactly what a rerun does (`amend`-via-efp is the
  sanctioned recovery path; a done node stays replay-skip until its own def or
  an ancestor's def moves). The plugin therefore carries **no rescue-snapshot
  file by design**: there is no second copy of the graph for a rerun to fall
  back to, and none may be added — a "rescue graph" snapshot would recreate the
  stale-recovery failure mode this law forbids. Contrast DAGMan, whose rescue
  DAG stores only which nodes were done and lets the rerun follow the edited
  `.dag`: same outcome, one fewer artifact to keep true. Enforced by
  `tests/test_partial_rescue_law_134.py`.
- To change the graph mid-flight: `amend` with the **whole** replacement graph.
  `dry_run:true` previews `{added, removed, changed, will_rerun, unchanged}`.
  Amending a `pending` node changes what spawns next; amending a `done` node
  invalidates it.
- For a static preview without provider pings, use `validate`. `run` with
  `dry_run:true` also performs route liveness pings, quota checks and live-route
  enforcement, but creates no run directory, lane entry or runner. It returns
  `{ok, dry_run, models, routes}`; it is not an offline validation substitute.
- To resume a lane that died at its wall with work already banked: amend its `goal`
  to a *resume* prompt that names what is already committed and forbids redoing it.
  Cold re-runs of a timed-out research lane time out again.
- Lane hygiene (#37, digest 20260929f): every build-shape spawn (`shape: "build"` or a
  declared `repo:` lane) is prefixed with a machine preamble — prompt-side only, like the
  resume preamble, so it never touches graph.json, node records or the def hash. It bans
  producing a RED run by checking a base ref out over a dirty tree (that overwrites
  uncommitted work in place), prescribes commit-tests-first + a throwaway detached
  worktree (or a named stash) for the RED state, and orders a WIP commit before the turn
  cap. A lane that still dies unbanked is not lost: its tool calls are journaled in the
  profile's state.db, and `python3 scripts/lane_recover.py --run <id> --node <node>
  [--out <dir>]` (or `--profile <name> --skey <key>`) replays the write_file/patch calls
  into a restore dir — triage list by default, files + an unmatched-patch report with
  `--out`; exit 2 no session, 3 nothing journaled. The db is opened read-only.
- Steering: `steer` queues text; a running child pulls it via `inbox` at its next
  seam. It does not interrupt a child mid-turn.

### 3e. Reporting a finished run

Quote the read model's vanity numbers: `tokens in ▸ out | api_calls | tool_calls`,
per node and run total (they are in every `status`/`wait` payload). Do not lead with
`estimated_cost_usd` — it is a price-table estimate and reads as spend on
subscription routes.

---

## 4. Contribute

### 4a. Map

| Path | Owns |
|---|---|
| `__init__.py` | The tool door: schema, action dispatch (all 17: `run/status/wait/release/release_lock/steer/inbox/amend/stop/kill/diff/list/save/library/submit/validate/doctor_version` — the tool schema is the closed list), model-tier resolution, preflight, compact/full payload shaping |
| `wf.py` | The background runner: scheduling, child spawn (`-Q` contract), retry gate, typed error classification, steer baking, tier stamping |
| `wfcommon.py` | The read model: run state, fingerprints (`efp`), node records, metrics join, liveness. Read-only over a run directory. Also the ONE grammar: `params`/`where`/`by` and the shared run-contract enumeration live here |
| `dashboard/plugin_api.py` | Dashboard routes (API-only; the manifest hides the tab) |
| `desktop/plugin.js` | Desktop half: runs list, DAG canvas, fan-out stacks, timeline, gate hand-off, `::workflow` card |
| `SKILL.md`, `references/` | The authoring skill loaded into sessions. Portable: no host names, install paths, or provider lore |
| `tests/` | Stdlib-only serial scripts; each prints `PASS`/`FAIL` lines, exit 0 = green. `.mjs` under Node. `tests/fake_hermes.py` is the child stand-in (`FAKE_MODE=…`) |
| `scripts/suite.py` | Serial runner with per-test logs + `exits.json` ledger (the merge gate); `--baseline <ledger>` adds `admission.json` splitting reds into introduced vs pre-existing (exact name+exit identities; a base red is blocking, never waived — and a base red whose test was DELETED reports `missing`, which blocks too, so `rm` can't launder a red to green); zero discovery under `tests/` is a failed admission (`green:false` + `zero_discovery:true`, nonzero exit) and an invalid root exits 2 before any out-dir side effects (#112) |
| `scripts/pr_formal_review.py` | Author-aware formal review (est-2ek.1.866): preflight compares `GET /user` vs the PR author BEFORE posting; a self-review (shared credential == author, the GitHub 422 "Can not request changes on your own pull request") NEVER attempts the doomed POST — it takes the verified fallback (findings into the ONE live marker comment — `--comment-id` PATCHes it, law 3 — with the fallback reason recorded before the trailing marker, plus the `changes-requested` label ADDED via `POST issues/{n}/labels`, never the replace-all `PATCH issues/{n}`; both read back); different identities post the real `REQUEST_CHANGES`; one credential, never forged; exit nonzero unless the write is read-back-verified |
| `scripts/pack.py` | Release zip + `SHA256SUMS` + sidecar |
| `scripts/wofify.py` | WOFification authoring seam (est-h0huv): `inventory`/`scaffold`/`validate`/`shelf` for way-of-working-repo→pack conversion; not a door action — the deterministic half of the shelf factory (`references/wofification.md`) |
| `scripts/make_public.py` | Publish-tree exporter with a private-string audit gate (`scripts/.scrub-guards` allow-list) |
| `docs/` | Patched-core guide, manifest decisions, catalog entry + PR body, scrub audit, [native-hooks.md](docs/native-hooks.md) (the default-off p1/p3/p5 switch board) |

### 4b. Run the checks

```sh
python3 tests/test_engine.py                      # one targeted suite while iterating
python3 tests/test_skill_docs.py                  # the portable-skill contract
node --check desktop/plugin.js                    # desktop half parses
node --experimental-strip-types tests/test_edge_routing.mjs
python3 scripts/suite.py . ci-out                 # full serial suite → ci-out/exits.json (merge gate)
hermes plugins validate .                         # manifest + SDK-surface + security scan
python3 wf.py validate [--json] <graph.json>      # lint a GRAPH FILE through the SAME door pipeline
python3 scripts/make_public.py /tmp/public-tree   # private-string audit; must print "0 scrub hits"
```

`wf.py validate` is argv for `action=validate` (#474): the same defaults fill, defect
collection, model/route policy and warnings channel, with no run dir, no liveness ping
and no writes — exit 0 on `ok:true`, 1 on a red verdict (the `errors[]` rows print),
2 on a usage/IO fault. It needs no `--plugin-dir`: `wf.py` binds its own siblings.

A change is done when: its targeted test is green, the full suite is green, validate
prints `Validation passed.`, and the scrub audit prints `0 scrub hits`. CI runs the
same gates ([.github/workflows/ci.yml](.github/workflows/ci.yml)).

### 4b′. Navigate with the knowledge graph (optional)

[`graphify`](https://github.com/Graphify-Labs/graphify) builds a code-navigation
graph locally (`uv tool install graphifyy` → `graphify update .` in a scratch
checkout): `graphify query "<question>"`, `graphify path "act_run" "run_child"
--undirected`, `graphify affected "<symbol>" --depth 2`, `graphify god-nodes`.
It is an exploration aid, not a gate: the output is never committed, CI never
builds it, and a `graphify-out/` in your PR diff is deleted by review. Source
and tests are the spec. (`.graphifyignore` ships so local runs skip generated
noise; `scripts/graph_diagram.py` + `diagram_readme.py` are unrelated — they
draw the example workflow diagrams in `examples/README.md`, which DO ship.)

### 4c. Rules

1. **Lane-scoped edits.** Touch only the files your task names. Never amend or rebase
   shared history — forward-only commits.
2. **Write-first.** Commit the artifact before polishing it. An uncommitted worktree
   at a wall death is lost work.
3. **Targeted tests while iterating; the full suite at merge.** Lanes that run the
   whole suite time out.
4. **No private strings in shipped files.** No hostnames, LAN addresses, machine
   vocabularies, real names outside `plugin.yaml`/`docs/catalog`. The scrub audit is
   the gate; add a guard line to `scripts/.scrub-guards` only with a reason.
5. **SKILL.md stays portable and under 110 lines.** Foreign agents on foreign hosts
   load it. Deep detail goes in `references/`.
6. **Desktop half stays on the SDK surface.** Imports only `@hermes/plugin-sdk`,
   `react`, `react/jsx-runtime`; no `window.hermesDesktop`, no core localStorage
   keys, no DOM edits of core UI. `hermes plugins validate` checks the automated
   subset; a human reviewer checks the rest.
7. **Stdlib-only Python; host imports lazy and guarded.** A local-backend Mac runs
   the plugin under system Python with neither `hermes_cli` nor PyYAML on path.
8. **Honest degradation over hidden failure.** If a field is absent (untyped turn
   report, missing metrics row), say `unknown`; never fabricate a value.
9. **Receipts state per-lap facts as facts and totals as pointers into the
   enumeration.** Never a hand-maintained headline ("four exits", "five legs")
   that must be re-edited every time a lap lands — a count no cold reader can
   re-derive from the cited records is a publication block (born twice in the
   Core-10 mill; both times the author's own re-sweep found more than the block
   named). Enumerate `laps[]` with run_ids; let clauses cite it.
10. **A run's fingerprint lives in `runner_exit.json` (with `reason` +
    `fp_rule_version`); `run.json` carries no fingerprint.** Same-shape trap:
    a shelf row saved from a *run-id* carries the run-dir digest (budgets baked
    in), only save-from-*file* carries the template's own `source_digest`. The
    door writes `source` only when passed explicitly — a null-source row is a
    bare save, not a broken shelf.
11. **Changed an example graph? Its diagram is derived, never drawn.** Edit the
    graph, re-run `python3 scripts/graph_diagram.py --all` + the render/shoot
    loop, and let CI's freshness gate refuse a stale commit. One rule, one
    file: [examples/diagrams/README.md](examples/diagrams/README.md) (DIAGRAM LAW v1).
    The examples/ map keeps a pointer here.

### 4d. Release

```sh
# bump plugin.yaml / SKILL.md / references/grammar.md / CHANGELOG.md to the new version
# re-pin every file:line cite in docs/catalog/disclosure-check.md (AGENTS 4d law:
# the audit is exact-at-commit — a drifted cite sends readers to unrelated code)
python3 scripts/suite.py . ci-out && hermes plugins validate . && python3 scripts/make_public.py /tmp/pub
python3 scripts/pack.py                            # artifacts/hermes-workflows-<v>.zip + .sha256
git tag v<version> && git push --tags
```

The catalog entry pins a full 40-char commit SHA
([docs/catalog/entry.yaml](docs/catalog/entry.yaml)); bump it in a PR to
`NousResearch/hermes-agent` → `plugin-catalog/hermes-workflows.yaml`.

Cross-estate joint eng protocol v1 is the SSOT mirrored publicly at [issue #174](https://github.com/jacobhausler/hermes-workflows/issues/174), sha256 `a7f424c5be3f5d6d1cbeb590f6cd02b6699a60090a3a45646853c67b2360345b` (of the issue body including its trailing newline); private working copy: `jacobhausler/joint-eng-protocol@0bf569a`, file `docs/specs/hermes-workflows-joint-eng-protocol-v1.md`.

---

## 5. Where things live at runtime

| What | Where |
|---|---|
| Run directories | `<runs_root>/<run_id>/` (`$WF_RUNS_ROOT`, else `<resolved home>/workflows` — core's `get_hermes_home()`, which on a profile-scoped host is the profile's home, not the launch-root `$HERMES_HOME`) — `graph.json`, `run.json`, `events.jsonl`, `nodes/<id>.json`, `logs/<id>.a<n>.log`, `steer/`, `gates/` |
| Library | `<runs_root>/library/` |
| Child turn report | `HERMES_QUIET_TURN_REPORT_FILE` (per spawn, read then unlinked) |
| Tier stamp | `<run>/turn_report.tier` (`typed` / `untyped`, write-once) |
| Dashboard API | `/api/plugins/hermes-workflows/runs`, `/runs/{id}`, `POST /runs/{id}/gate` |
| Desktop plugin | `~/.hermes/desktop-plugins/hermes-workflows/plugin.js` on the **app** machine |
