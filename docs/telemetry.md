# Telemetry (opt-in)

Two default-off observers feed one bounded corpus that `doctor budgets` reads.

- Flag: `plugins.entries.hermes-workflows.settings.telemetry: true`. Default false. Flag off registers no hook and writes nothing, not even the directory. The flag is read at plugin load.
- Hooks: `subagent_stop` (covers `delegate_task`-style children; it never fires for the CLI children the runner spawns) and `on_skill_lifecycle`.
- Corpus: `<runs_root>/telemetry/events-YYYYMMDD.jsonl`; directory 0700, files 0600.
- Rotation: a day file stops growing at 4 MB. Later rows that day are dropped silently; no exception. A new day starts a new file.

## Privacy law: sizes and statuses only

Rows hold these fields and nothing else:

- subagent: `ts, kind, parent_session_id, child_role, child_status, duration_ms, tool_count`
- skill: `ts, kind, action, skill_name, use_count, reused`

`tool_count` is the length of the tool history. The history, the child summary, tool inputs and prompts are never persisted. A missing field becomes null; observers never raise.

## Readout

```
python3 scripts/doctor_budgets.py [--runs-root DIR] [--json]
workflow {"action":"doctor","sub":"budgets"}
```

The readout is read-only. It prints p50/p90 `duration_ms` and `tool_count` per `child_role`, skill reuse rates, finished-node seconds per shape/model, and `presets_delta`: the shape-preset `timeout` the corpus would move, with `n` per cell. A cell with n < 20 prints `insufficient(n)`. Node records carry no turn count, so `max_turns` is reported as not measured. It applies nothing and changes no defaults. Exit 0 on an empty corpus.
