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
2. Get your chat id (message the bot once, then `getUpdates` once from any
   throwaway client, or use your existing chat id).
3. Export the config and start the daemon:

```
WORKFLOW_STREAM_BOT_TOKEN=123456:ABC...
WORKFLOW_STREAM_CHAT_ID=123456789
python3 scripts/workflow_stream.py
```

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

## Discipline

- Send-only Telegram API: `sendMessage`, `sendPhoto`, `editMessageText`,
  `pinChatMessage`, `unpinChatMessage`. Never `getUpdates`.
- No gate answering: release stays with the agent / `workflow` tool — the
  message informs, it doesn't accept commands.