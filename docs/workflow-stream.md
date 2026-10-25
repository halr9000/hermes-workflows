# workflow-stream — Telegram mirror for workflow runs

A companion daemon that watches the plugin's runs root and mirrors each run
into **one persistent Telegram message per run**: a static topology graphic
sent once at launch, then a text status message sent as its reply and pinned.
The text is the live surface — edited in place as node states change — and is
unpinned at the terminal state (the message stays in chat history).

Inspired by kanban-stream; this implementation shares no imports, code, or
configuration with it.

## Why a dedicated bot token (never the gateway bot)

`WORKFLOW_STREAM_BOT_TOKEN` must be a token from a **separate** @BotFather bot
— never your main Hermes gateway bot's token. The gateway consumes Telegram
updates with a long poll (`getUpdates`); Telegram allows only one consumer per
bot token. A second consumer causes HTTP 409 update conflicts and silently
breaks your primary bot's message delivery. This daemon is send-only (it never
calls `getUpdates`), so any number of send-only consumers can share a token —
but the token must still be its own bot so it can post into your chat without
touching the gateway's update stream.

## Setup

1. Create a bot with @BotFather → copy its token.
2. Put the token in the Hermes env file, `~/.hermes/.env` (the daemon loads
   `WORKFLOW_STREAM_*` keys from it itself — explicit shell env still wins):

```
WORKFLOW_STREAM_BOT_TOKEN=123456:ABC...
```

   `hermes plugins install` prompts for this automatically (masked) and saves
   the answer to the same file; skipping is fine when you don't want Telegram
   mirroring.
3. **Message the bot once** from the destination chat (start with `/start`) —
   Telegram only delivers updates for chats that have talked to the bot.
4. **Discover your chat id** — any of:
   - `/workflow-stream setup` in any Hermes session, or
   - `python3 scripts/workflow_stream.py --discover-chat` from the repo, or
   - DM **@userinfobot** — for a 1:1 chat the chat id simply equals your
     Telegram user id.

   The discovery path does `getMe` plus **one** short `getUpdates` poll and
   prints every chat it saw plus the exact `WORKFLOW_STREAM_CHAT_ID=<id>`
   line to append. This is a setup-only exception to the send-only rule:
   the dedicated bot has no other update consumer, so one poll cannot
   conflict with anything, and the live daemon still never calls
   `getUpdates`. (In a group, the id is negative — use the printed value
   verbatim.) Then append the printed line to `~/.hermes/.env` (or re-run
   the install prompts).
5. Preflight, then run:

```
python3 scripts/workflow_stream.py --check
python3 scripts/workflow_stream.py
```

`--check` validates config presence, the graphic dependencies, and the
credentials against the Bot API (`getMe`/`getChat` — send-side calls that
consume no updates), exiting non-zero with an explanation if either is
rejected. Normal startup runs the same validation, so a dead or mistyped
token fails cleanly at startup instead of surfacing as a bare 404 on the
first `sendPhoto`. From a session, the same preflight is available as the
`workflow` tool's `action=stream_check`, whose result carries `next_steps`
for walking through setup.

## In-session control: `/workflow-stream`

| Command | Effect |
| --- | --- |
| `/workflow-stream` or `status` | Config presence, watch mode, graphic deps, whether the daemon process is alive, and the last recorded stream error (the failure beacon). No network — answers instantly. |
| `on` / `off` | Rewrite `WORKFLOW_STREAM_WATCH=all/off` in `~/.hermes/.env`. The daemon re-reads WATCH **every poll cycle**, so the toggle takes effect live — no restart. `off` idles the daemon; runs already adopted still close out. An explicit process-env `WORKFLOW_STREAM_WATCH` at daemon start pins the mode for that process and ignores file toggles. |
| `setup` | Chat-id discovery when the token exists but the chat doesn't (the one-time `getUpdates` poll above); otherwise reports what's missing. |
| `check` | The full preflight (`getMe`/`getChat` — same as `stream_check`). |

On the first live start the daemon pins a one-card micro-readme into the
destination chat — what the stream does, the tuning knobs, and a docs link.
It is version-keyed (`README_VERSION`): bump it to re-send the card after a
doc/config change; otherwise it is sent exactly once per install.

## Configuration (env, `WORKFLOW_STREAM_` prefix)

| Variable | Default | Meaning |
| --- | --- | --- |
| `BOT_TOKEN` | — (required) | Dedicated BotFather bot token (see above). |
| `CHAT_ID` | — (required) | Destination chat id. |
| `GRAPHIC` | `1` | `1` sends the static launch graphic; `0` disables it. |
| `WATCH` | `all` | `all` mirrors every run; `telegram` mirrors only runs whose owner platform is `telegram`; `off` mirrors nothing (feature off switch). |
| `POLL_S` | `5` | Filesystem poll interval in seconds (clamped 1–600). |

`WATCH=telegram` keys on the run's `run.json` owner platform, so a run
launched from Telegram is mirrored while desktop runs stay local — use it if
you don't want desktop work leaving the machine.

## Message lifecycle

- First observed state → `sendPhoto` (static, type-coded topology graphic,
  rendered once and never re-rendered) → `sendMessage` as its **reply** →
  the **text** message is pinned.
- State changes are edge-triggered: the text is edited via
  `editMessageText` only when the rendered text actually differs, respecting
  Telegram's per-message edit throttle with backoff.
- Terminal state (`done` | `partial` | `failed` | `stopped`) → final edit +
  unpin. The message remains as history.
- Concurrent runs each get their own independent message; pinning is
  best-effort (last-active wins in a DM with several active runs).

## Graphic

Rendered natively with Pillow — 2x supersample + a single LANCZOS downscale,
no SVG intermediate and no OS rasterizer. Typography is fixed (identical font
sizes on every graph — the canvas grows, the type does not) using the vendored
libre monospace font `assets/fonts/LiberationMono-Regular.ttf`
(SIL OFL 1.1, see `assets/fonts/OFL-LICENSE.txt`), so rendering is
byte-deterministic across machines. Shapes are type-coded and state-neutral:
agent = cyan rounded rect, gate = amber diamond, join = purple pill,
echo = gray rectangle, with an `xN` badge on fan-outs and lane labels under
each node.

## Dependency policy (loud, early failure)

Pillow is declared in `plugin.yaml` `python_dependencies`, so it goes through
Hermes' normal consent + package-manager admission at plugin install/update
time — declining the dependency leaves the plugin disabled rather than
half-working. If `GRAPHIC=1` (the default) and Pillow or the vendored font is
missing at daemon startup, it exits immediately with the exact install
instruction. There is no graceful degradation to a silent text-only stream.

## Crash safety

Per-run stream bookkeeping (photo/text message ids, pin state, last rendered
text hash) is persisted as `stream.json` beside the run. On restart the daemon
re-derives run truth from disk via the plugin's own read model
(`wfcommon.run_state`) and resumes the existing message — it reposts only if
Telegram reports the message deleted.

## Failure beacon (no silent degradation)

A token that dies mid-run (revoked, network gone, chat blocked) cannot post
about its own failure to Telegram — so the failure lands locally instead:
every Telegram-path error bumps a failure counter and rewrites
`stream-status.json` beside the runs root (`error`, `failures`, `since`,
`last_at`); a clean poll cycle clears it. `/workflow-stream status` reports
the beacon, so a silently-dead stream is one command away from a diagnosis.

## Discipline

- Send-only Telegram API at runtime: `sendMessage`, `sendPhoto`,
  `editMessageText`, `pinChatMessage`, `unpinChatMessage`. Never
  `getUpdates` — the ONE exception is `--discover-chat` / `/workflow-stream
  setup`, a human setup step that polls updates once against the dedicated
  bot (which has no other update consumer).
- No gate answering: release stays with the agent / `workflow` tool — the
  message informs, it doesn't accept commands.