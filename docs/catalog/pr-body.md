# plugin-catalog: add `hermes-workflows` (community, automation)

Submitted by the plugin repository owner (rule 5).

## What it is

`workflow` tool for Hermes Agent: the calling agent owns a JSON graph of agent
nodes, fan-outs and gates; a background runner executes it with fingerprint
replay-skip resume, fan-out quorum, per-node retries, cooperative steer, and
typed failure events (`error_class` + `attempts` on every `node.failed` — the
parent never infers from exit code + prose). Ships a desktop DAG view (SDK
surface only), an inline `::workflow{id}` transcript card, and the authoring
skill with measured budget recipes. Children are spawned through the stock quiet
one-shot CLI (`hermes chat --query-file … -Q --max-turns N`).

Repo: https://github.com/jacobhausler/hermes-workflows — README with
screenshots, `AGENTS.md` (agent front door: install / operate / contribute),
`INSTALL.md`.

## Catalog rules, checked at the pinned SHA

| Rule | Evidence |
|---|---|
| 2 — exact pin | at release, `sha` will be the 40-char commit of tag `v1.3.9`; `image`/`screenshots` are raw URLs pinned to the same SHA |
| 3 — no self-updater | No update checks; the only outbound HTTP is the config-gated, default-OFF p1 webhook (`hooks_outbound.py`, ships OFF: no setting, no socket) and the core-client liveness ping; zero `fetch(` in `desktop/plugin.js`; the plugin never loads code it did not ship |
| 6 — capabilities match | `provides_tools: [workflow]`; 8 hooks declared; every hook-backed feature (admission gates, dispatch witness, P1, P3, P5) is config-gated DEFAULT-OFF — OFF means zero registrations; no middleware; `requires_env: []` — the runner's `HERMES_WF_STEER_*` vars are plugin-internal IPC set by the runner for its own children, never user-provided (`docs/manifest-decisions.md`) |
| 7 — install scanner | `hermes plugins validate .` → `Validation passed.`, security scan `safe`, zero `caution` findings |
| 8 — desktop surface | `desktop/plugin.js` imports only `@hermes/plugin-sdk` / `react`; gate resumes use `host.composer.submit(ownerSessionId, text)` (visible) or SDK `insertText`; no app DOM reach or private composer events |
| 9 — dependencies | `python_dependencies: ["Pillow>=10.0"]` — Pillow powers the workflow-stream launch graphic (stdlib-only otherwise; no `pyproject.toml`) |

## Disclosure (what the plugin actually does at runtime)

Each `workflow run` starts a detached `wf.py run <id>` process, independent of
the originating session and gateway; disabling the plugin does not stop an
already-running process. It continues until a graph boundary or `workflow stop`
(subject to host process shutdown), and interrupted runs need an explicit
resume. Each agent node launches an operator-configured `hermes chat
--query-file … --oneshot -Q …` child (the launcher comes only from plugin
settings / `HERMES_WF_HERMES_BIN`, never from tool arguments), with optional
routing/budget flags and a copy of the runner environment plus workflow IPC
variables. A graph-authored gate `wait.until_argv` executes a fixed argv
command without a shell directly in the runner, outside Hermes tool approval.
Run state is stored under `$HERMES_HOME/workflows/`. The plugin registers no
cron and opens no sockets or credential-store reads of its own; however,
run/amend may issue one provider liveness request per distinct explicitly
routed model through Hermes' core auxiliary client, child agents may make
provider/tool calls with the inherited environment, the Desktop half uses the
Hermes plugin REST API, and gate commands inherit the runner environment.
Stop live runs before disabling. Clause-by-clause `file:line` evidence:
[`docs/catalog/disclosure-check.md`](disclosure-check.md).

## `requires_hermes: ">=0.21.4"` — measured, not guessed

The spawn contract needs every flag the plugin passes (`--query-file -Q
--source --create-if-missing --max-turns --run-budget --provider --reasoning
--toolsets`) in `hermes_cli/_parser.py` and the `HERMES_QUIET_TURN_REPORT_FILE`
turn-report contract in `hermes_cli/quiet_single_query.py`. Walking published
tags newest → older: `v2026.9.24` PASS, `v2026.9.21` PASS, `v2026.9.14` FAIL
(flags present, turn report absent). `v2026.9.21` is `pyproject.toml` version
`0.21.4`.

## Test evidence (re-run on the published pin before submitting)

- Serial suite, desktop syntax, manifest validation and scrub audit must all pass on the exact tagged tree before re-pinning.
- CI in the repo runs the same four gates (ESM parse, suite, private-string
  scrub audit, `hermes plugins validate`) against a clone of `hermes-agent`
  pinned to `v2026.9.21` (`.github/workflows/ci.yml`).
- Core imports are two, both soft with fallbacks (`hermes_constants`,
  `hermes_cli.config`); the state.db metrics join reads stock columns only,
  `mode=ro`.

## Relationship to a patched core

None required. Stock Hermes records a child that dies on its turn cap as
`error_class: unknown` (partial output and log preserved). Open PR #121041 adds
`turn_exit_reason` to the quiet turn report; with it the same death is typed
`cap_exhausted`. The plugin reads the field if present and says `unknown` if not —
nothing auto-patches anyone's core, and `status` reports which tier a failed
child ran under (`turn_report: typed|untyped`). If #121041 merges, the
distinction disappears.
