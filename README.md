# Hermes Workflows

![Version](https://img.shields.io/badge/version-1.3.9-blue)

**Agent-owned workflow graphs for [Hermes Agent](https://github.com/NousResearch/hermes-agent).**
Your agent authors a JSON graph of agent nodes, fan-outs and gates; a background
runner executes it outside the caller process tree and hands results back through
the same `workflow` tool it launched from. Hermes Desktop draws the live DAG.

![Graph lifecycle: the door admits and validates the graph, the runner spawns one kernel-flock'd process, agent lanes fan out, a gate holds at zero tokens, amend swaps the rest — typed failures land in the ledger beside the report card](docs/pr-diagrams/showcase-dag-lifecycle.png)

Four production diagrams, glossed two lines each:
[docs/showcase.md](docs/showcase.md).

## Why this exists

A single agent turn is one long, fragile thread of work: one context, one
timeout, no resumable checkpoints. A **graph** splits the job into nodes with
explicit dependencies — parallel recon lanes, a synthesis join, an adversarial
review gate — each node a separate child session with its own model, budget, and
typed failure. When something dies or you change your mind mid-flight, only the
affected nodes re-run: completed work is replayed from its stored fingerprint,
not re-paid.

## What you get

| Capability | In one line |
|---|---|
| **Graph runs** | Agent nodes with `goal`, `after` edges, per-node `model`/`provider`/`reasoning`, and `max_turns`/`timeout`/`run_budget` caps; optional per-graph `concurrency`/`item_concurrency`, clamped to owner caps |
| **Fan-out** | One node → N live children from `fanout.items` or `items_from:"<node>.items"`; per-item liveness; `quorum` cancels stragglers once N commit (they are `cancelled`, never a failure) |
| **Human + machine gates** | A `gate` holds on a question until `release`; `gate.wait` holds on a timer or an argv probe at zero token cost; `when` predicates branch on upstream output; `on_skip:"prune"` retires the losing arm; an optional default-off admission gate answers human releases at zero turns (`docs/admission-gates.md`) |
| **Fingerprint resume** | Crash, restart, or `amend` the graph mid-flight — nodes whose effective fingerprint still matches replay-skip; only what actually changed re-runs. `run`/`amend` accept `dry_run:true` for a no-write preview |
| **Cooperative steer** | `steer` queues text for a running node; the child pulls it at its next natural seam via the tool's `inbox` action — prompts are never rewritten mid-flight |
| **Profile delegation** | A node can run *as* another local Hermes profile (its instructions, memory, tools) behind a per-profile consent file — multi-personality graphs on one machine, no isolation claimed |
| **Typed failures** | Every `node.failed` carries `error_class` + `attempts` from a closed set (`timeout`, `cap_exhausted`, `provider_400`, `schema`, `precondition`, `cancelled`, `fatal_quota`, `route_unavailable`, …) — you never infer a cause from prose; failed nodes ship `node_facts` (class, attempts log, final words, log path) |
| **Route integrity** | A node that pins a `model` is fail-closed (`require_route`, on by default): a proven-dead pin refuses to launch rather than silently bill another model; with an owner-configured `confidence_substrate` ladder (#116) the substitution is engine-stamped and disclosed in the node's result contract — never silent |
| **Process-tree honesty** | An exit-0 child is believed only when its whole process tree is dead; a backgrounded worker is typed, never mistaken for done, and a fenced answer over a live tree commits `partial` with proof |
| **Compact status** | Mid-run `status`/`wait` return output *pointers* and per-node metrics (missing evidence reads unknown, never a false zero); `detail:"full"` opts into everything |
| **Wedged-lock recovery** | The runner is admitted by a kernel flock on `runner.lock` held for the process's whole life — a dead holder's lock is released by the kernel itself, the next spawn contends cleanly, and there is no stale lock to clear by hand |
| **Desktop DAG view** | Live graph, fan-out stacks, timeline, and a `::workflow{id="…"}` inline card in any reply; RUNNING / THE REST agent-first panes (every row carries its originating agent), and a session strip under the composer showing this chat's runs (core ≥ v2026.7.30; older shells get the fallback slot) |
| **Library** | `save` a proven graph (description + tags — flat or `facet:value`, e.g. `use_case:code-review`), `library` lists it richly and filters by tags (with a `tag_vocab` echo so agents reuse the live taxonomy), `run from:"<name>"` replays it; optional study-only `submit` takes a `why_not_library` receipt and is quarantined for study, never auto-saved; `inbox kind:"submissions"` lists these submissions. Ordinary one-off `run` executes without lookup or curation |
| **Composite graphs** | A graph-level `include` expands shelved library graphs into a run at launch — namespace-isolated ids, cycle/depth/size guards, model policy unions in and never relaxes |
| **Authoring skill** | Bundled `workflow` skill: grammar, operations, and **measured** per-shape budget presets (`recon`/`build`/`review`/`publish`) |

<table><tr>
<td width="50%"><img src="assets/fanout.png" alt="A fan-out node at 3/3 terminal with its three item rows (host-a, host-b, host-c) expanded, feeding a join node"><br><sub><b>Fan-out.</b> One node, N live children; expanded items each carry their own status and duration, and the join only releases when the quorum is terminal.</sub></td>
<td width="50%"><img src="assets/branch-gate.png" alt="A held human gate in the middle of the run graph: question 'Ship v1.4.0?' with its two answer options pending, the completed upstream node green, the downstream node pending"><br><sub><b>Human gates.</b> A held gate parks the run at zero token cost; the answer (tool, desktop card, or <code>/wf gates</code>) releases the waiting arm.</sub></td>
</tr></table>

## Offline checkout drift probe

`python3 scripts/drift_check.py --repo <checkout>` (default: this checkout).
On demand only — no fetch, daemon, or cron. Final stdout line:
`installed_version`, `upstream_latest` (nearest local release tag by
`git describe --tags`, not the remote's newest), `local_only_commits`,
`trailers_missing`, `verdict`. Exit 0 = `clean`; exit 1 = drift
(`version_mismatch`, `trailers_missing`, then `local_only`, in that precedence —
commits after the tag need `(est-…)` or `#NN` references in their messages);
exit 2 = `error` (absent installed-version changelog heading, missing tag,
unreadable git state). Reads only plugin.yaml, CHANGELOG.md, and git
describe/rev-list/log.

## The `workflow` tool

One tool, action-routed — the complete surface, with the flags that matter:

| Action | What it does |
|---|---|
| `run` | Launch a graph from `graph` (inline), `graph_path` (≤1 MiB local file), or `from` (library name). Optional `name`, `run_context` (seed string, or a binding map replacing `{run.KEY}` refs — malformed input is refused before anything is written; values land in prompts, so no secrets), `team`, `lane_key` (dedupe: a second run on an unfinished incumbent returns it instead of spawning), `dry_run:true` (full validation, zero writes). |
| `status` | Read-only read model: per-node state, metrics, held gate, `node_facts` on failures, derived `next` steps — never spawns a runner. `lane_key` reads the incumbent instead; a lane entry whose claimed run dir no longer exists on disk reports `state: "orphaned"` (a claimed-then-lost run, not a forever-`pending` ghost). |
| `wait` | The resume-and-watch verb: the only read action that respawns an idle runner; blocks to the next boundary (default 600 s, ceiling 1800 s), self-yielding before the host's tool deadline with a "call wait again" note. |
| `release` | Answer a held human gate (`gate_id`, `answer`); respawns the runner when idle. A human release pre-empts a machine `wait` park. |
| `release_lock` | Operator escape hatch for a wedged `runner.lock`: renames the stranded inode aside under the admission heal-barrier and hands the lane a fresh lock path. REFUSES unless zero live holders are proven (kernel ledger + dead lease) — never heals on EACCES or a live holder. |
| `steer` | Queue steering text for a running node (refused on gates and terminal nodes; delivery is cooperative via the child's `inbox` pull — never a mid-prompt injection). `cmd:"kill"` (with `node`) is the lane-kill form, identical to `kill`; it never queues text. |
| `inbox` | Child-side pull of steering lines baked for this spawn — exactly-once per spawn. Parent-side `kind:"submissions"` lists `submit` items newest-first. |
| `amend` | Replace the graph mid-run with the whole new graph (`dry_run:true` previews `will_rerun` without writing). Fingerprint-valid unchanged nodes replay-skip. A `patch` edits a few node fields without resubmitting the whole graph. |
| `diff` | Confirm an amend before applying it: the amend door with `dry_run` FORCED — accepts `patch` or a full graph and returns the preview plus a unified `graph_diff`; touches nothing. |
| `stop` | Request a stop at the next boundary; in-flight children are killed; stop ≠ failure — cancelled work re-drives on resume, and a `lane_key` is freed for the next dispatch. |
| `kill` | Node-targeted lane kill (`run_id` + `node`): SIGKILLs that node's in-flight children and finishes the node `failed` with `error_class=killed`. Siblings and the run keep going (no `stop.request`). The kill marker stays for the run — a killed lane stays killed. |
| `list` | All runs (capped page + full-census counts, per-run provenance rollup when present). |
| `save` | Shelve a graph in the library under a name (overwrite = current best): `description`, `tags` (1–10, flat or `facet:value`; reuse `library`'s `tag_vocab` verbatim), optional `source` attribution writes provenance (owner, digest, timestamp) — attribution, never access control. |
| `submit` | Quarantine a hand-rolled graph the library didn't cover for human-gated study — requires a `why_not_library` receipt (≥80 chars); never joins the library. |
| `library` | List shelved graphs richly (nodes, gates, fan-outs, description, tags, provenance), filterable by ALL-match tags; an empty filtered result says which tag starved. `name:"<name>"` returns required inputs and an instantiate command. |
| `validate` | Dry-run the door's validation pipeline (defaults fill, defect collection, model/route policy) with no ping and no writes — `{ok, errors:[{node,field,msg}], resolved_routes}`. Same pipeline on argv, for heads that have no tool in reach (CI, pre-commit, plain shells): `python3 wf.py validate [--json] <graph.json>` — exit 0 ok, 1 red (the `errors[]` rows print), 2 usage/IO fault. No `--plugin-dir`: `wf.py` binds its own siblings. |
| `doctor_version` | Read-only version truth for THIS install: `{live_version, newest_packaged, source_commit, drift}` — plugin.yaml vs the `install.json` provenance `pack.py` stamps at build time; one read, no network. |

Plus a `/wf` slash command: bare `/wf` lists the library; `/wf <name> [note]`
returns the atomic run instruction for that graph, carrying the note as its
`run_context` seed (the launch itself is the agent's `run` call — the command
never spawns). `/wf show <name>` (or
`workflow {"action":"library","name":"<name>"}`) returns required inputs and
an instantiate command; `/wf gates` boards held gates (needs the
`admission_gates` setting; see [docs/admission-gates.md](docs/admission-gates.md));
`/wf orphans` lists
orphan-held runs across the fleet. Library-sourced launches require nonempty string bindings
for every `{run.KEY}` on the shared text surface, including echo output, gate
options and machine argv; surviving refs refuse before any write. Inline graphs
retain their existing behavior, including refusing unbound machine argv.

A shelf row can declare its launch contract. The typed top-level `params` block
(`pin`/`tune` kinds) plus the `where`/`by` door-time seams bind presence and
patched goals at launch — every violation lands in ONE refusal, and the run
records the reproducibility triple (`meta.binding`). The lighter
`save params:{KEY:{desc,default}}` envelope stores display hints only: `/wf show`
renders them, required keys always derive from graph refs, and defaults are
suggestions — never automatic bindings. See
[references/grammar.md](references/grammar.md).

## Graph grammar in 30 seconds

Four node types — `agent`, `gate`, `echo`, `join` — and a closed key set per type
(anything outside it is refused at submit with a per-defect error list, never
silently ignored). The smallest useful graph:

```json
{ "name": "check",
  "nodes": [{ "id": "inspect", "type": "agent",
              "goal": "Inspect the target. Return one fenced JSON object." }] }
```

```
workflow { "action": "run",  "graph": <the object above> }   → run_id
workflow { "action": "wait", "run_id": "<run_id>" }          → repeat until terminal
```

Fan-out with `quorum`, gates with `when`/`on_skip`/`wait`, `requires` output
preconditions, `after_partial` harvest release, `inputs` selection from upstream
output, string-field `enum`s, answer-schema floors `minItems` (on `type:"array"`)
and `minLength` (on `type:"string"`, counted on stripped chars — a whitespace-only
string is too short), graph-level `defaults` (precedence: explicit node
key > `shape` preset > `defaults`) and `model_policy` — the authoritative
vocabulary is [references/grammar.md](references/grammar.md); the bundled
authoring skill keeps a compressed working copy in [SKILL.md](SKILL.md).
Runnable templates in [examples/](examples/), grouped by role —
`basics/` (one idea each), `build/` (fan-out and ledgers), `review/`
(independent judgment), `release/` (issue→PR, release lifecycle, gates and
watchers), `ops/` (incidents). [examples/README.md](examples/README.md) is the
map: one line per template, what it teaches, and which arm its receipts proved.
Every example ships with its rendered diagram under
[examples/diagrams/](examples/diagrams/).

## What a run leaves behind

Each run is a directory (`~/.hermes/workflows/<run-id>/` by default): the
committed `graph.json`, an append-only `events.jsonl` typed event log, per-node
results under `nodes/`, prompts and logs under `logs/`, gate answers under
`gates/`, and a `summary.md`. Resume, `amend`, and the desktop all read the same
shared state. There is no hidden control plane and no daemon: exactly one runner
process per run, admitted by a kernel lock, spawned outside the caller's process
tree. Run states (`running`, `held`, `interrupted`, `done`, `failed`,
`stopped`), owner wake semantics, silent-runner reaping, and recovery
procedures:
[references/operations.md](references/operations.md).

## Install

```sh
hermes plugins install hermes-workflows      # from the catalog (pinned SHA)
hermes plugins install <git-url>@v1.3.9      # fallback until the catalog entry merges (see INSTALL.md for the zip)
hermes plugins enable hermes-workflows
```

Restart the backend (`hermes serve`) so the tool and dashboard routes mount.
Before disabling or removing the plugin, stop live runs: use `workflow {"action":"list"}`
to find their run IDs, then `workflow {"action":"stop","run_id":"<id>"}` for each.
Detached runners can outlive a session, gateway restart, or plugin disable until a
boundary; disabling alone does not stop them. Desktop gate answers use the SDK to
send a visible resume turn to the run owner's chat; older Desktop builds insert
text for you to send, or ask you to type `workflow wait` in that owner chat.
If Hermes Desktop runs on a **different machine** than the backend, copy
`desktop/plugin.js` to that machine's `~/.hermes/desktop-plugins/hermes-workflows/plugin.js`
— the app hot-loads it. Manual/zip install and removal: [INSTALL.md](INSTALL.md).

## Two builds, one codebase

The plugin needs **no patched Hermes**. Every child spawn rides the stock quiet
one-shot CLI contract, and every install runs the same code. Exactly one field
differs, and it is optional:

|  | stock Hermes (catalog install) | with the optional core patch |
|---|---|---|
| graphs, fan-out, gates, steer, resume, desktop view, cards | ✓ | ✓ |
| a child that dies on its `max_turns` cap | `error_class: unknown` + preserved partial/log | typed `error_class: cap_exhausted` + reason |
| how | nothing to do | one sha-stamped patch — [docs/patched-core.md](docs/patched-core.md) |

The tier is self-reported: after a failed child, `status` shows
`turn_report: typed` or `untyped`, so you always know which build you're on.
When upstream [PR #121041](https://github.com/NousResearch/hermes-agent/pull/121041)
lands, the branch collapses and that doc deletes itself.

## Requirements

- Hermes Agent **≥ v2026.9.21 (package version 0.21.4)** — measured stock `-Q`
  CLI and quiet turn-report contract; see [docs/catalog/pr-body.md](docs/catalog/pr-body.md).
  Older 0.21.3 deployments are below this declared floor and will skip plugin admission.
- Python 3 (stdlib only — the plugin imports nothing outside Hermes)
- Node for the desktop half's tests only; the app loads `plugin.js` uncompiled

## Starter set: run something today (`general/`)

You don't have to author a graph first. Five scrubbed, estate-neutral starter
graphs ship *with the plugin* under `library/general/`, so a fresh install can
run a real DAG out of the box:

```jsonc
{ "action": "run", "from": "general/report-and-verify" }
```

| Address | Reach for it when |
|---|---|
| `general/pr-review-light` | an open PR needs an advisory second read before a human looks |
| `general/issue-triage` | a queue needs a standing rule applied unattended (the `{run.KEY}` seed-contract exemplar) |
| `general/test-first-feature-slice` | one behavior is worth a red-green-proof slice |
| `general/fan-out-audit-census` | a tree needs a per-file audit with an honest tally (the 8 KB per-parent input cap, taught by example) |
| `general/report-and-verify` | a producer will drop a report later and you want zero tokens while waiting (machine `wait` gate exemplar) |

Resolution order for `from: general/<name>`: your **runs-root library first**
(`<runs_root>/library/general/<name>.json`), legacy launch root second, the
**packaged set last**. Every graph routes agents through model *tiers*
(`worker`/`manager`) — none names a vendor model — and each declares its seed
params in `meta.params`.

**Customize:** copy a packaged JSON to your own shelf (or tune it and
`save name=general/<name>`); your copy shadows the packaged one at the same
address, and the door never writes the packaged files.
**Share back:** tuned graphs are welcome upstream — see the submit flow in
issue #50 ("share your tuned graph back"): fork, drop the envelope under
`library/general/`, run the scrub gate, open a PR. Per-graph author notes
live in [`library/general/README.md`](library/general/README.md).

## For agents and contributors

[AGENTS.md](AGENTS.md) is the front door: repo map, operate/contribute
procedures, and the rules that keep the tree publishable. Changes are gated by
the serial suite (`python3 scripts/suite.py . ci-out`) and
`hermes plugins validate .` — both run in [CI](.github/workflows/ci.yml).
`graphify` (a separate tool) can query the code graph locally; it is never
required and never gates.

## License

MIT — see [LICENSE](LICENSE).
