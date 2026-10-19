---
name: workflow
description: "Workflow fan-out audit and census: run agent graphs"
version: 1.4.0
metadata:
  hermes:
    tags: [workflows, fan-out, audit, census, orchestration]
---

# Workflow authoring (1.3.9)

Requires Hermes Agent v2026.9.21 or newer (package >=0.21.4). A Desktop gate answer
reaches the run owner's chat via the composer SDK; on older builds it may only insert
draft text — type the resume line in the owner chat yourself. Before disabling the
plugin, list runs and stop each live run with `workflow{action:"stop",run_id:<id>}`.

Use the `workflow` tool when a task needs independent lanes, a human gate, or a
resumable graph; for one or two independent calls, delegate instead. The authoring
agent owns the graph and its side effects; the dashboard is a reader, not an executor.

The registered tool ID is `workflow` (toolset `workflows`). If the host exposes a
searchable deferred catalog, describe/call its exact returned ID. If no available
entry or discovery capability exists, report that boundary; do not guess aliases,
launch a replacement or mutate configuration.

## Smallest working graph

Build `{ "name": "check", "nodes": [{"id":"inspect","type":"agent","goal":"Inspect the
specified target. Return one fenced JSON object."}] }`. Submit with
`workflow{action:"run", graph:<object>}`, then carry the returned `run_id` into
`workflow{action:"wait", run_id:<id>}` and stay in that loop until a terminal state —
ending your turn right after `run` is how runs stall.

## Authoring rules

- `after` orders nodes AND injects each direct parent's committed output under
  `## Inputs`; `inputs:["a.key"]` picks a dotted path or non-parent ancestor,
  `fanout.items_from:"a.items"` an upstream `items` array. A missing input fails at
  spawn.
- A node `schema` makes the runner write the reply contract into the prompt — keep
  contract prose out of goals.
- Independent tasks: separate nodes or one `fanout` with `items`. `quorum` races: once N succeed, the rest are cancelled — `quorum_drain_s:<s>` (default 0) gives running stragglers a drain window; pair every fan-out with a `consolidate` partner (`{"from":"<fan>","batch":1}`) — left-open items past the cohort clock fire one `fanout.stragglers` wake, never poll.
- Shared settings go in a graph-level `defaults` once. Leave node budgets unset and
  name a `shape`; see [budgets](references/budgets.md). A constant travels as an
  `echo` node, never an agent spawn.
- RunBudget `budget:{tokens:N}` (graph root or node) caps metered tokens only, never `usd`; a capped run closes `partial` and `budget_ledger.json` names the never-launched.
- Routing: an unset `model` rides the seat default, including fan-outs — name
  `model` (with `provider`) per node only when routing matters; never change
  persistent model preferences to make a graph work. A configured tier, alias, or
  literal is a request, not a guarantee — inspect the returned routing table and its
  errors before trusting execution. Pins are fail-closed (`require_route`): a dead
  pin or fallback-ladder answer refuses the launch (`route_unavailable`) instead of
  billing another model; opt into the ladder with `require_route:false`. See
  [grammar](references/grammar.md).
- A `gate` with `question`+`options` holds until you present it and `release` the
  owner's answer; a decorative gate self-releases on `hold_timeout`+`default_option`;
  machine waits use `gate.wait`. Agents and gates share bounded `when`/`on_skip`: a false
  predicate prunes the node and exclusively dependent arms (agents spawn nothing);
  `on_skip:"pass"` lets descendants run. Malformed or non-ancestor-head predicates are refused
  before write/spawn; echo/join reject both. Examples `approve-publish`, `branch-on-verdict`: [grammar](references/grammar.md).
- `profile` runs a node AS a named teammate: DELEGATION, not isolation — same UID,
  the target's SOUL/memory/`.env`/tools; the target's `workflow_team.json` must list
  the launcher in `accept_from`; `default` is never a target.
- `requires` on an agent/gate fails the node (`error_class:"precondition"`, zero
  spawns) when an ancestor's committed output lacks a listed path — the run FAILS,
  never skips quietly; skip-on-missing is `when` + `on_skip:"prune"` on the node itself.
- After-edges block on a `partial` (harvest-on-death) ancestor
  (`blocked_by_partial_ancestor`); consume the harvest with `after_partial:true`; a solo producer may instead set `harvest_policy: "validate"` so its own
  schema-valid, self-declared-complete harvest commits `done` with the death kept on the
  record. Details: [grammar](references/grammar.md).

## Run and handoff

- `running` needs a verified live runner; `interrupted` means unfinished work without
  one — inspect surviving outputs, then `wait` to resume. Fatal runner errors are
  `failed`, not respawn loops; held gates are not running. Every `status`/`wait`
  payload carries `next` — do what `next` says, `wait` until it is empty; `next` is
  derived, never a guess. Before `amend` or stop, read the failed node's `node_facts`
  (error_class from the closed set; `cancelled` is never a failure) and committed
  outputs. `amend` accepts a WHOLE replacement graph or node-scoped `patch`;
  `diff` forces amend's dry-run preview. `run`/`amend` `dry_run:true` preview
  without launching/replacing graphs, but can ping routes and check quota.
  `stop` is terminal. Details: [operations](references/operations.md).
- Lint before launching with `workflow{action:"validate", graph:…}` — the door's
  whole validation, zero writes, no liveness ping.
- Optional recipes: `library` lists; `run` with `from:<name>` replays; `save` explicitly shelves
  reusable graphs and always stamps provenance; a `run_context:{required?,defaults?}` contract
  refuses a launch missing a key, pre-write. One-off `run` needs no library lookup, tags, save or
  `why_not_library`. Explicit study-only `submit` retains its receipt requirement. [grammar](references/grammar.md).
- Optional typed `params` contract (pin/tune) with node `where`/`by` door-time seams: the door materializes, refuses every violation in one answer, and stamps the reproducibility triple. [grammar](references/grammar.md#params--the-typed-contract-and-the-door-time-seam-optional).
- No double-dispatch for pollers: `run` with `lane_key:<key>` — while an UNFINISHED
  incumbent holds the key, a second `run` is deduped, never spawns. `needs_resume`
  means `wait` it — never replace it; `stop` is the explicit abandonment. Keys are
  global per runs root; prefix `<team>/` yourself. Details:
  [operations](references/operations.md).
- Report a finished run by metrics (token in/out, api calls, tool calls), not the
  dollar figure: `estimated_cost_usd` is a price-table estimate (subscription routes
  report `included`) and reads badly as spend.
- Cards are optional desktop presentation; CLI text and returned run IDs suffice.
  Completion is execution state, not task verification or authority for more work.
  Push notifications are best-effort; `wait` is the blocking/resuming path.
- `graph_path` replaces embedding on run/save/amend/validate: a caller-authorized
  absolute JSON file, exactly one graph source. [grammar](references/grammar.md).
  File-shareable graphs: [portable](references/portable.md).

The graph vocabulary lives in [grammar](references/grammar.md); read-model/recovery in
[operations](references/operations.md); build lanes and babysitting in the [operator
playbook](references/operator-playbook.md); skills-repo→pack in [wofification](references/wofification.md).
[Development checks](references/development.md) are for contributors, not user prerequisites.
