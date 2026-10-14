"""hermes-workflows plugin — the `workflow` tool: agent-owned graph runs.

The engine (wf.py) is a separate process per run; this module is the door:
launch it, read its run-dir via the SHARED read model (wfcommon.run_state), drop
its input files. No standing daemon or control plane, no second opinion on run
state (one runner process per run, detached out of the caller's tree at spawn #8).
"""
import contextlib
import importlib.util, json, os, re, select, shutil, stat, subprocess, sys, time
import fcntl, hashlib, uuid
from datetime import datetime, timezone
from pathlib import Path

# Bind our sibling by path, never another plugin's already-imported wfcommon.
_COMMON_PATH = Path(__file__).resolve().parent / "wfcommon.py"
_spec = importlib.util.spec_from_file_location("_hermes_workflows_wfcommon", _COMMON_PATH)
_common = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_common)
run_state = _common.run_state
validate_graph = _common.validate_graph
validate_graph_errors = _common.validate_graph_errors
admission_ledger_errors = _common.admission_ledger_errors   # #85: the artifact-admission guard
efp = _common.efp
jload = _common.jload
amend_preview = _common.amend_preview
quote_json_parse_error = _common.quote_json_parse_error
kind = _common.kind   # the door consults the ONE node-kind table (wfcommon.NODE_TYPES) via this

# #157: card enforcement — bind our OWN wfcommon (never a sys.path sibling).
_ce_spec = importlib.util.spec_from_file_location("_hermes_workflows_card_enforcement",
                                                  Path(__file__).resolve().parent / "card_enforcement.py")
assert _ce_spec is not None and _ce_spec.loader is not None
_card_enforcement = importlib.util.module_from_spec(_ce_spec)
_ce_spec.loader.exec_module(_card_enforcement)


def _coerce_graph(graph):
    """The door only ever sees `graph` as a parsed object from the tool schema, but a
    model CAN hand the string form; parse it and, on malformed JSON, quote ±40 chars
    around the error offset. Returns (graph_or_None, error_response_or_None)."""
    if isinstance(graph, str):
        try:
            graph = json.loads(graph)
        except json.JSONDecodeError as e:
            return None, {"error": f"graph is not valid JSON: {e}",
                          "near": "…" + quote_json_parse_error(graph, e) + "…"}
    if not isinstance(graph, dict):
        return None, {"error": "graph must be an object {name, nodes:[...]}"}
    return graph, None

GRAPH_MAX_BYTES = 1024 * 1024

def _inline_graph_size_error(graph):
    """#62 F-1: the INLINE branch must cap exactly like the graph_path branch —
    the SAME GRAPH_MAX_BYTES, measured on the serialized bytes, checked BEFORE any
    write. One shared helper wired into _input_graph's inline branch, so submit,
    save, run and amend all refuse an oversized graph (parity law; no second
    validator, no new config). Error wording mirrors the graph_path branch."""
    payload = json.dumps(graph, ensure_ascii=False)
    if len(payload.encode("utf-8")) > GRAPH_MAX_BYTES:
        return {"error": f"graph exceeds {GRAPH_MAX_BYTES} bytes"}
    return None
GRAPH_KEYS = {"name", "nodes", "description", "defaults", "model_policy",
              "provenance",   # 1.1 (RATIFY F5): opt-in library provenance block, door-written
              "grammar",      # #32: dialect tag of a shared file ("wf/1"; absent = wf/1)
              "concurrency", "item_concurrency",  # #100: optional run-level limits
              "requires_plugin",  # est-2ek.1.166: version handshake (coarse >=; stale => arm-time refusal)
              "result",           # est-2ek.1.158: node id whose output is the run verdict
              "on_exit",          # #129: agent reserved for terminal postmortem
              "include"}      # composite graphs: shelved-DAG expansion annotation; STRIPPED on
                              # expand, so a committed graph.json never carries it (only the
                              # library author form does)


def _model_policy_error(graph):
    """Validate effective node routes after defaults and resolution, before graph.json."""
    policy = graph.get("model_policy") or {}
    forbidden = set(policy.get("forbidden_models") or []) | set(_seat_model_cfg().get("workflows_forbidden_models") or [])
    errs = []
    seat = _seat_model_cfg()
    tiers = model_tiers()
    known = set(seat["aliases"]) | ({seat["default"]} - {None}) | set(tiers.values())
    for node in graph["nodes"]:
        if kind(node).spawns is not True:   # non-agent (gate/echo/join): no model route
            continue
        nid, requested = node["id"], node.get("model")
        if policy.get("require_model") and not requested:
            errs.append({"node": nid, "field": "model", "msg": "model policy requires an explicit model"})
        if not forbidden or not requested:
            continue
        _p, alias_target = _alias_provider_pair(node.get("tier") or requested, seat, tiers, known)
        effective = alias_target or requested
        candidates = {requested, effective, node.get("model")}
        if node.get("provider") and "/" not in str(requested):
            # #291 F1: amend checks policy AFTER resolution, where a baked literal reads
            # {provider: X, model: Y}; the ban on the full 'X/Y' must still bind it.
            candidates.add(f"{node['provider']}/{requested}")
        if "/" in effective:
            candidates.add(effective.rsplit("/", 1)[-1])
        if candidates & forbidden:
            errs.append({"node": nid, "field": "model",
                         "msg": f"model policy: node {nid} resolves to forbidden model {effective!r}"})
    if not errs:
        return None
    first = errs[0]
    return {"error": f"graph invalid: node {first['node']}: {first['msg']}", "errors": errs}


def _input_graph(args, *, run_id=False, library=False):
    """Choose one explicitly supplied source; never discover files on the caller's behalf."""
    supplied = [key for key in ("graph", "graph_path") if args.get(key) is not None]
    supplied += [key for key, enabled in (("run_id", run_id), ("from", library))
                 if enabled and args.get(key)]
    if len(supplied) > 1:
        return None, {"error": f"choose one graph source, not {', '.join(supplied)}"}
    if args.get("graph_path") is not None:
        path = args["graph_path"]
        if not isinstance(path, str) or not path or not os.path.isabs(path):
            return None, {"error": "graph_path must be an absolute local file path"}
        try:
            # O_NONBLOCK makes a hostile FIFO/device non-blocking; fstat checks the
            # opened descriptor, not a path that could be replaced between checks.
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(fd, "rb") as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode):
                    return None, {"error": "graph_path must name a regular file"}
                if info.st_size > GRAPH_MAX_BYTES:
                    return None, {"error": f"graph_path exceeds {GRAPH_MAX_BYTES} bytes"}
                raw = source.read(GRAPH_MAX_BYTES + 1)
            if len(raw) > GRAPH_MAX_BYTES:
                return None, {"error": f"graph_path exceeds {GRAPH_MAX_BYTES} bytes"}
            graph = raw.decode("utf-8")
        except (OSError, UnicodeError) as e:
            return None, {"error": f"graph_path cannot be read as UTF-8 JSON: {e}"}
        return _coerce_graph(graph)
    if args.get("graph") is not None:
        graph, bad = _coerce_graph(args["graph"])
        if bad:
            return None, bad
        # #62 F-1: cap the INLINE branch exactly like the graph_path branch above,
        # before any caller reaches a write (act_submit/act_save/act_run/act_amend
        # all source through here).
        bad = _inline_graph_size_error(graph)
        if bad:
            return None, bad
        return graph, None
    return None, None

def _validation_error(graph, run_dir=None):
    """Return graph-level and node-level defects together, before any write/spawn.

    PR#84 review F-2: the structural (graph-level) half is delegated to
    wfcommon.structural_graph_errors — the SAME rules the include door measures
    every expanded shelf with — so submitted graphs and included graphs can
    never diverge in strictness again. This side keeps the door-specific node
    normalization (author-forged route_verified stripping) and composes it with
    the shared node-level validator.

    #85: composes the ARTIFACT-ADMISSION guard too (wfcommon.admission_ledger_errors):
    a fan-out node's declared `fanout.ledger` must map 1:1 to its items' sources or
    admission is REFUSED with a named error — a mis-declared item silently getting
    another item's artifacts (WOFS W1f) becomes impossible-by-construction for
    admitted graphs. `run_dir` (amend; validate given a run dir) additionally measures
    the declared artifact bytes; run creation passes None — the dir does not exist yet."""
    errs = list(_common.structural_graph_errors(graph))
    nodes = graph.get("nodes")
    # The shared validator assumes hashable ids and iterable dependency lists.
    # Normalize only those invalid shapes in a copy, collecting their errors while
    # still letting the shared validator report unrelated defects in the same call.
    safe = [] if isinstance(nodes, list) else nodes
    if isinstance(nodes, list):
        for index, node in enumerate(nodes):
            if not isinstance(node, dict):
                safe.append(node)  # shared validator identifies the first non-object
                continue
            item = dict(node)
            nid = node.get("id")
            if isinstance(nid, (dict, list)):
                errs.append({"node": None, "field": f"nodes[{index}].id",
                             "msg": "node id must be a string"})
                item["id"] = f"__invalid_id_{index}__"
            # #25 (F1): `route_verified` is in the runner's closed set (the door bakes
            # it into graph.json AFTER this check), but an AUTHOR value is never
            # trusted: the door STRIPS it here — on the node it actually examines —
            # so act_save (which validates WITHOUT _resolve_models) can never shelve
            # a forged proof, and a resubmitted committed graph.json re-proves its
            # proof through this submit's ping instead of replaying a stale one.
            # A hard deny would break the documented amend flow (callers legitimately
            # resubmit the current graph.json, proof included, verbatim).
            if node.get("type") == "agent" and "route_verified" in node:
                node.pop("route_verified", None)
                item.pop("route_verified", None)
            # #116: same law for the engine's substitution stamp — the closed set
            # carries the key so the RUNNER loads a committed graph, but an AUTHOR
            # value is never trusted; shelved copies (act_save) drop it too.
            if node.get("type") == "agent" and "substrate_substituted" in node:
                node.pop("substrate_substituted", None)
                item.pop("substrate_substituted", None)
            deps = node.get("after", [])
            if not isinstance(deps, list) or any(not isinstance(dep, str) for dep in deps):
                errs.append({"node": nid if isinstance(nid, str) else None,
                             "field": "after", "msg": "after must be a list of node id strings"})
                item["after"] = []
            safe.append(item)
    errs.extend(validate_graph_errors(safe, admission=True))  # R10: submit door only
    # #85: the artifact-admission guard composes with the validator here, so EVERY
    # door admission path (run/amend/submit/save/validate) measures ledgers before
    # any write/spawn. Artifact BYTES are measured only when a run dir exists to
    # measure them in (amend, and validate given run_id); the 1:1 structural law
    # always runs.
    errs.extend(admission_ledger_errors(graph, run_dir=run_dir))
    if not errs:
        return None
    first = errs[0]
    head = first["msg"] if first["node"] is None else f"node {first['node']}: {first['msg']}"
    return {"error": f"graph invalid: {head}", "error_class": "graph_invalid", "errors": errs}

HERE = Path(__file__).resolve().parent

def _owner_setting_read(key):
    """THE owner-settings read (#41/#42 share it with hermes_bin): plugin-scoped
    ``_CTX.get_config`` -> ``plugins.entries.hermes-workflows.settings.<key>`` (legacy
    ``config`` fallback inside core). None without a ctx or on any core rejection.
    Installed into wfcommon so runs_root()/launcher_profile() read the SAME source at
    CALL time — no restart; a process without a door ctx (runner, dashboard, tests)
    falls back there to the resolved home's config.yaml, read raw."""
    if not _CTX:
        return _common.NO_READER
    try:
        return _CTX.get_config(key, None)
    except Exception:
        return None

_common.set_owner_setting_reader(_owner_setting_read)

# #100: absent settings keep the runner's historic 4/8 defaults. A supplied
# author limit can never exceed its owner cap; an owner cap below the default also
# constrains runs whose graph omitted the key. No optional key is baked otherwise.
_CONCURRENCY_CAPS = {"concurrency": ("max_concurrency", 4),
                     "item_concurrency": ("max_item_concurrency", 8)}

def _concurrency_bake(graph):
    baked = {}
    for key, (setting, default) in _CONCURRENCY_CAPS.items():
        cap = _common.owner_setting(setting)
        if cap is not None and (type(cap) is not int or cap <= 0):
            return None, {"error": f"owner settings invalid: settings.{setting} must be a positive integer"}
        if key in graph or cap is not None:
            limit = min(graph.get(key, default), cap if cap is not None else default)
            # No graph key + no effective change = the old run.json, byte for byte.
            if key in graph or limit != default:
                baked[key] = limit
    return baked, None

def _owner_settings_error():
    """FAIL-CLOSED at the door (#42): a malformed `settings.runs_root` / `settings.profile`
    is an error on EVERY action, never a silent fallback to another root/identity."""
    try:
        _common.settings_runs_root()
        _common.settings_profile()
    except ValueError as e:
        return {"error": f"owner settings invalid: {e} (fix plugins.entries.hermes-workflows.settings)"}
    return None

def _hermes_bin():
    """Operator-controlled launcher; tool arguments never choose a child executable.

    get_config is already plugin-scoped — it resolves
    plugins.entries.<id>.settings.<key> with a legacy ``config`` fallback — so one
    relative read covers every legal place the launcher lives. Asking for a
    reserved root (``plugins``, ``model``, ``security``, ``settings``) raises
    ValueError out of core (plugins_state._plugin_relative_segments), which used to
    kill every workflow launch at the door; reads stay guarded regardless.
    """
    configured = _common.owner_setting("hermes_bin")
    if isinstance(configured, str) and configured.strip():
        return configured.strip()
    env_bin = os.environ.get("HERMES_WF_HERMES_BIN", "").strip()
    if env_bin:
        return env_bin
    w = shutil.which("hermes")
    if w:
        return w
    for c in (Path.home() / ".local/bin/hermes", runs_root().parent / ".local/bin/hermes",
              Path("/usr/local/bin/hermes"), Path("/opt/hermes/bin/hermes")):
        if c.exists():
            return str(c)
    return "hermes"

runner_alive = _common.runner_alive

def _gw_restart_window_match(r, anchor_iso):
    """#8 (P0, remaining half): observed gw-restart reason. The incident shape
    is the gateway itself being SIGTERMed for a restart while the sweep takes
    the runner down — the death window then shows a `Received SIGTERM` line in
    <hermes_home>/logs/gateway.log. Match it from FILES ONLY: any such line
    whose stamp falls within +/-120 s of `anchor_iso` (the death anchor).
    Never fatal: an absent/unreadable log, an unparseable anchor, or any read
    error answers False — the reap keeps today's bare reason. The host log is
    the gateway's own file; the plugin only reads it, never writes."""
    try:
        anchor = datetime.fromisoformat(anchor_iso)
    except (TypeError, ValueError):
        return False
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    try:
        lines = (_common.hermes_home() / "logs" / "gateway.log").read_text(
            errors="replace").splitlines()
    except (OSError, ValueError):
        return False
    for line in lines:
        if "Received SIGTERM" not in line:
            continue
        for tok in line.replace("[", " ").replace("]", " ").split():
            try:
                stamp = datetime.fromisoformat(tok)
            except ValueError:
                continue
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            if abs((stamp - anchor).total_seconds()) <= 120:
                return True
    return False


def _death_anchor(r):
    """The death window anchor: the last event ts on the record (the dead
    runner's last heartbeat), else the wf.pid file's mtime. Files only."""
    try:
        for line in reversed((r / "events.jsonl").read_text().splitlines()):
            try:
                ts = json.loads(line).get("ts")
            except ValueError:
                continue
            if isinstance(ts, str) and ts:
                return ts
    except OSError:
        pass
    try:
        m = (r / "wf.pid").stat().st_mtime
        return datetime.fromtimestamp(m, tz=timezone.utc).isoformat(timespec="seconds")
    except OSError:
        return None


def _reap_silent_death(r):
    """#8 fix-law item 2 (crash-visibility): make a silent runner death loud
    BEFORE a door respawn replaces it. The incident shape: runner + children die
    together out-of-band (gateway restart / cgroup or process-tree sweep) — a
    SIGKILL records nothing, no runner_exit.json exists, node records still claim
    status=running, and every watcher shape (tail|grep, events-offset loops, wait)
    sees silence while act_wait quietly replaces the runner. Law: the door's
    RESPAWN paths (never a read path — run_state/act_status stay pure observers)
    append, in order, one `runner.reaped` (previous pid + observed reason) and one
    `node.interrupted` per node record claiming a running child whose pid fails
    the ONE verification law (_verify_spawn_rec: a verifiably-live child is
    ADOPTED, never interrupted). A graph-bound exit receipt and claim-bound
    interruption receipts expose typed current truth; original spawn bytes stay
    cold and reads remain pure. Best-effort: a reaper that itself throws never
    blocks the resume."""
    try:
        pid = None
        try:
            pid = int((r / "wf.pid").read_text().strip())
        except (OSError, ValueError):
            pass
        if pid is None or _common.runner_alive(r):
            return   # no pid file = fresh run; alive = nothing to reap
        graph = _common.jload(r / "graph.json") or {}
        byid = {n["id"]: n for n in graph.get("nodes", [])}
        # The ONE exit read is the silent-death probe. ONLY
        # `crashed (no exit record)` (pid-dead + no verdict on disk) proves the
        # death was never written down. `stale` does NOT: it only says the
        # recorded verdict's graph fingerprint no longer matches — which is
        # exactly what act_amend itself causes when it replaces graph.json over
        # a CLEAN recorded exit (previous_reason "held at g" etc.). Fingerprint
        # staleness alone is never evidence of an unrecorded death (PR #82
        # review, F2); a VALID recorded exit — fresh or amend-staled — is a
        # verdict, and act_wait's rx gate owns it, no reaping.
        rx = _common.runner_exit_read(r) or {}
        reason = rx.get("reason")
        saved_rx = _common.jload(r / "runner_exit.json") or {}
        if reason == "interrupted: silent runner death":
            reason = saved_rx.get("observed_reason", "crashed (no exit record)")
        # est-6226: a RECORDED external kill (the runner's own SIGTERM handler
        # wrote the external tag before re-raising) is equally a silent death
        # needing a loud reap — the wave shape records the verdict but no
        # node.interrupted and no reaped line, leaving watchers staring at
        # silence while act_wait quietly replaces the runner. The phantom
        # gate ("crashed (no exit record)") and the external tag are the only
        # accepted shapes; every other recorded verdict is a lane verdict and
        # the rx gate in act_wait owns it.
        _external = _common.is_external_kill(reason)
        if reason != "crashed (no exit record)" and not _external:
            return
        # Event-level once-per-record dedup (PR #82 review, F3), content-aware:
        # scan the events after the LAST run.resumed (a replacement that
        # readied re-arms the channel for the NEXT death) and drop candidates
        # already on the record modulo ts. Retrying the same frozen scene
        # appends nothing; a scene that changed since (a claimed child died in
        # the meantime) contributes exactly the NEW lines — never a duplicate
        # runner.reaped or a second interrupt for the same claim. Best-effort
        # check-then-append: concurrent reapers are serialized upstream by the
        # door's own admission path (only one respawn proceeds), so the retry
        # window this pins is sequential.
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        # #8 (P0, remaining half): the observed-reason clause. When the death
        # window matches a gateway restart (a `Received SIGTERM` line in the
        # host gateway log within +/-120 s of the last event ts / wf.pid mtime),
        # the event says so. Never fatal: an absent/unreadable log keeps the
        # bare reason byte-identical (S2-S4 pins in tests/test_reaper_reason_8).
        if _gw_restart_window_match(r, _death_anchor(r)) and \
                "; gw-restart window match" not in reason:
            reason = reason + "; gw-restart window match"
        events = [{"ts": now_iso, "event": "runner.reaped", "prev_pid": pid, "reason": reason}]
        for n in graph.get("nodes", []):
            paths = [(r / "nodes" / f"{n['id']}.json", None)]
            if n.get("fanout"):
                paths.extend((p, p.name[len(n["id"]):-len(".json")].lstrip("."))
                             for p in sorted((r / "nodes").glob(f"{n['id']}.[0-9]*.json")))
            for p, idx in paths:
                if _common.node_rec(r, n, byid)[0] != "pending":
                    continue   # committed truth (done/partial/failed) is never a false claim
                rec = _common.jload(p) or {}
                if rec.get("status") != "running":
                    continue   # nothing ever claimed a live child here
                # The ONE verification law applied to the record actually
                # loaded — NOT active_child(r, n, byid), which always probes the
                # BASE file and would call a live fan-out item's pid a ghost
                # (PR #82 review, F1). A verifiably-live child is adopted.
                if _common._verify_spawn_rec(r, n, byid, rec):
                    continue   # live child: adoption owns it
                ev = {"ts": now_iso, "event": "node.interrupted", "node": n["id"],
                      "pid": rec.get("pid"), "skey": rec.get("skey"),
                      "attempt": rec.get("attempt"), "of_runner": pid}
                if idx is not None:
                    # the runner's own item vocabulary (PR #82 review, F4):
                    # {node: <parent id>, index: N}, joinable to item.* events
                    # and never colliding with a real node named "fan0".
                    ev["index"] = int(idx) if idx.isdigit() else idx
                events.append(ev)
                # Keep original spawn evidence cold; expose current typed truth
                # through a claim-bound interruption receipt (never a new spawn).
                ir = r / "interruptions" / p.name
                ir.parent.mkdir(exist_ok=True)
                receipt = {"status": "interrupted", "error_class": "crashed",
                           "at": now_iso, "of_runner": pid,
                           "claim": {k: rec.get(k) for k in
                                     ("pid", "skey", "attempt", "efp", "fp_rule_version")}}
                tmp = ir.with_name(f"{ir.name}.{os.getpid()}.tmp")
                tmp.write_text(json.dumps(receipt))
                os.replace(tmp, ir)
        if reason.startswith("crashed (no exit record)"):
            receipt = {"reason": "interrupted: silent runner death", "error_class": "crashed",
                       "prev_pid": pid, "observed_reason": reason, "at": now_iso,
                       "graph_fingerprint": _common.graph_fingerprint(graph),
                       "fp_rule_version": _common.FP_RULE_VERSION}
            rp = r / "runner_exit.json"
            if not saved_rx.get("reason"):
                tmp = rp.with_name(f"{rp.name}.{os.getpid()}.tmp")
                tmp.write_text(json.dumps(receipt))
                os.replace(tmp, rp)
        _strip = lambda e: json.dumps({k: v for k, v in e.items() if k != "ts"},
                                      sort_keys=True)
        seen = set()
        try:
            for line in (r / "events.jsonl").read_text().splitlines():
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("event") == "run.resumed":
                    seen = set()   # a readied replacement re-arms the channel
                elif e.get("event") in ("runner.reaped", "node.interrupted"):
                    seen.add(_strip(e))
        except OSError:
            seen = None
        # `seen` now holds every reap line since the last run.resumed (modulo
        # ts). Candidates already on the record are dropped; nothing new =>
        # write nothing. A changed scene appends exactly the NEW lines.
        if seen is not None:
            events = [e for e in events if _strip(e) not in seen]
        if not events:
            return
        with open(r / "events.jsonl", "a") as f:
            for e in events:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
    except Exception:
        pass  # visibility is diagnostics, never a reason to skip the resume


def _respawn_runner(r):
    """ONE bridge: the crash-visibility reaper, then the spawn. Every door path
    that RESPAWNS a possibly-dead runner goes through here; `run` launches a
    fresh dir and stays on the bare spawn."""
    _reap_silent_death(r)
    _spawn_runner(r)


def _resume_after_action(r, grace_s=0.75, consumed=None):
    """Close the live-runner terminal handoff window after a durable action.

    A release/amend/stop can run inside the runner's synchronous owner-wake POST.
    In that shape the runner cannot exit until this call returns, so the runner's
    post-notify boundary consumes the durable answer/marker. For an independent
    actor landing after that boundary but just before process exit, briefly watch
    verified liveness and respawn once the old runner is truly gone. Flock
    admission makes concurrent winners harmless; only one runner can enter.

    `consumed` distinguishes a runner that completed the requested boundary from
    one that died before consuming it; a completed stop/amend/release must never
    be respawned merely because the old process exited quickly.
    """
    def _after_death():
        if consumed is not None and consumed():
            return "consumed"
        st = run_state(r) or {}
        if st.get("status") in ("done", "stopped"):
            return "consumed"
        _respawn_runner(r)
        return "respawned"

    if not runner_alive(r):
        return _after_death()
    deadline = time.monotonic() + grace_s
    while time.monotonic() < deadline:
        time.sleep(0.05)
        if not runner_alive(r):
            return _after_death()
    return "live"


# #8 (P0): the transient daemonize hop. The door Popen's THIS, it forks the real
# runner (own session, ready-pipe write end inherited non-cloexec) and exits at
# once so the runner leaves the caller's process tree BEFORE handle() returns —
# adopted by the nearest enclosing subreaper, else init (cgroup membership is
# unchanged; see _spawn_runner's claim boundary).
_DAEMON_INTERMEDIATE = (
    "import os, sys\n"
    "target, run_id, wd = sys.argv[1], sys.argv[2], int(sys.argv[3])\n"
    "try:\n"
    "    pid = os.fork()\n"
    "except OSError:\n"
    "    os._exit(3)  # fork refused: the door falls back to a direct spawn\n"
    "if pid == 0:\n"
    "    os.setsid()\n"
    "    os.execve(sys.executable, [sys.executable, target, 'run', run_id],\n"
    "              dict(os.environ, HERMES_WF_READY_FD=str(wd)))\n"
    "os._exit(0)  # transient parent: exit NOW so the runner reparents immediately\n")

_READY_WAIT_S = 5.0

# #152 T3 spawn-provenance key: door-spawned runners get this env stamp (value =
# run id) at _spawn_runner; the runner pops it at main() entry — same inherit-
# then-pop pattern as wf.READY_FD_ENV. Direct CLI boots never carry it.
_SPAWNED_BY_ENV = "HERMES_WF_SPAWNED_BY"

def _ready_pid(rd):
    """One newline-terminated pid off the ready pipe, <= _READY_WAIT_S. None on
    EOF or timeout: a WORKFLOW_BUSY loser closes the write end without a line."""
    buf = b""
    deadline = time.monotonic() + _READY_WAIT_S
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            return None
        try:
            r, _, _ = select.select([rd], [], [], left)
            if not r:
                return None
            chunk = os.read(rd, 64)
        except (OSError, ValueError):
            return None
        if not chunk:                      # EOF — no pid was ever written
            return None
        buf += chunk
        if b"\n" in buf:
            tok = buf.split(b"\n", 1)[0].strip()
            return int(tok) if tok.isdigit() else None


def _spawn_runner(r):
    """Spawn the run's runner process — DAEMONIZED out of the caller's tree (#8).

    Law (four reaped-runner incidents, 2026-09-29/30): by the time handle()
    returns the runner must NOT hang in the caller's process tree. Both killers
    walk pids, not sessions: the gateway-restart process-tree SIGTERM sweep, and
    the process_registry completion sweep (_terminate_host_pid: snapshot
    descendants WHILE the parent lives, SIGTERM parent, per-pid SIGKILL
    escalation over the snapshot, re-scan). start_new_session escapes only
    group-directed signals, never a per-pid kill of a snapshotted descendant.
    The double-fork hop reparents the runner out of the caller's tree before the
    door returns, so no CALLER-TREE sweep can ever snapshot it; children inherit
    the escape through the runner. Claim boundary (#8 review, finding 1):
    double-fork/setsid does NOT change cgroup membership — a sweep that kills by
    cgroup membership (e.g. the unit-cgroup ExecStopPost SIGKILL a gateway
    restart runs) still reaches caller, runner and children alike. Surviving an
    enclosing service/cgroup cleanup is external supervision's job (a mitigation
    outside this diff), not what this hop claims.

    Admission stays the runner-side kernel flock (loser exits WORKFLOW_BUSY
    before touching any state). SOLE-OWNER stamp law (#8 review, findings 3+4):
    the ADMITTED runner is the ONLY writer of wf.pid — it self-stamps right
    after it wins the flock (wf.py acquire_lock -> ready_stamp) and announces
    the same pid on the door's ready pipe. The door NEVER writes wf.pid: an
    observed pid may be returned to the caller, but a post-admission door write
    is a second owner racing the runner — the deep review reproduced both
    failures deterministically (a door parked after observing A let B be
    admitted, then its late write resurrected dead A; a forced refused-fork
    fallback stamped the flock-refused dead loser over the live winner). A
    spawn that never readied is left stamped only by whoever WON the flock; the
    explicit wait-resume law covers a dead spawn and the flock makes any retry
    a harmless loser.

    Append mode: runner.log keeps crash diagnostics across respawns. The
    contextvar profile scope does NOT cross processes: a runner spawned for a run
    under the RESOLVED runs root gets that home stamped explicitly (core's own #18594
    guidance for subprocess spawners), so its seat config and child env follow the
    OWNER's profile, not the launch root os.environ carries on a multiplex host.
    A legacy-location run (pre-fix, under the launch root) keeps the inherited env
    verbatim — runner_alive compares it against the run's parent.
    """
    log = open(r / "runner.log", "a")
    env = ({**os.environ, "HERMES_HOME": str(_common.hermes_home())}
           if r.parent == _common.runs_root() else None)
    # #152 T3 spawn-provenance stamp: every process that leaves _spawn_runner is
    # DOOR-spawned (fresh launch, crash-reaper revival, explicit wait-resume,
    # release/amend respawn). The runner pops this at main() entry (never into a
    # child env) and the admission duplicate-consumer gate ONLY retires a boot
    # that proves door provenance — a direct CLI resume (`wf.py run <id>`, the
    # test-locked stopped-run resume shape) carries no stamp and always runs.
    # Bound to the run id so a stray leak into an unrelated shell can only ever
    # affect that one run.
    # The stamp rides BOTH channels: an explicit env dict when the door builds
    # one, and the live os.environ (inherited) for legacy-location spawns. The
    # finally-pop guarantees the door's own process never keeps provenance
    # beyond this spawn — an in-process test door that later Popen's wf.py
    # directly (the B1 direct-resume shape) must NOT hand it the stamp.
    if env is not None:
        env[_SPAWNED_BY_ENV] = r.name
    os.environ[_SPAWNED_BY_ENV] = r.name
    try:
        argv = [sys.executable, str(HERE / "wf.py"), "run", r.name]
        pid = None
        if hasattr(os, "fork"):                # POSIX: daemonize through a transient hop
            rd = wd = None
            try:
                rd, wd = os.pipe()
                proc = subprocess.Popen(
                    [sys.executable, "-c", _DAEMON_INTERMEDIATE,
                     str(HERE / "wf.py"), r.name, str(wd)],
                    stdout=log, stderr=log, stdin=subprocess.DEVNULL,
                    env=env, pass_fds=(wd,), start_new_session=True, cwd=str(HERE))
                os.close(wd); wd = None        # the transient parent must not hold the write end
                pid = _ready_pid(rd)
                try:
                    rc = proc.wait(timeout=5)
                except Exception:
                    rc = 0                     # hung transient: trust the ready line
                if pid is None and rc not in (0, None):
                    pid = _spawn_runner_legacy(argv, env, log)   # fork refused pre-fork: direct spawn
            except Exception:
                pid = _spawn_runner_legacy(argv, env, log)   # daemonize path died pre-exec
                                                           # (no runner alive): direct spawn;
                                                           # the flock makes a race harmless
            finally:
                for fd in (rd, wd):
                    if fd is not None:
                        try:
                            os.close(fd)
                        except OSError:
                            pass
        else:                                  # no-fork platform: today's behavior
            pid = _spawn_runner_legacy(argv, env, log)
    finally:
        os.environ.pop(_SPAWNED_BY_ENV, None)
    # #8 review (findings 3+4): NO door write of wf.pid — the admitted runner is
    # its sole owner (self-stamp at admission, wf.py ready_stamp). `pid` here is
    # only returned to the caller as the observed pid; on the legacy path the
    # Popen'd child IS the runner and stamps ITSELF once it wins the flock, so
    # a flock-refused loser never names itself in wf.pid either.
    try:
        log.close()
    except OSError:
        pass
    return pid

def _spawn_runner_legacy(argv, env, log):
    """Direct spawn for no-fork platforms / refused fork: here the Popen'd child
    IS the runner, and like the daemonized path it STAMPS ITSELF (wf.py
    ready_stamp) only after winning the flock — the door never stamps, so a
    flock-refused loser never names itself in wf.pid. Returns the observed
    Popen pid for the caller's information, not as an ownership proof."""
    proc = subprocess.Popen(argv, stdout=log, stderr=log, stdin=subprocess.DEVNULL,
                            env=env, start_new_session=True, cwd=str(HERE))
    return proc.pid

# ---------- tool schema ----------

WORKFLOW_PARAMS = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["run", "status", "wait", "release", "steer", "inbox", "amend", "diff", "stop", "list", "save", "submit", "library", "doctor_version", "validate"],
            "description": "run=launch a graph; wait=read state, RESPAWNING an idle runner if work is pending (blocks to the next boundary when one is live); status=read-model of a run; release=answer a held human gate; steer=queue steering text for a node; inbox=(child-side, cooperative) pull late steering lines baked for THIS spawn — call once at a natural seam; amend=replace the graph (invalidates changed nodes + all downstream by fingerprint), or pass patch to edit a few node fields without resubmitting the whole graph; diff=confirm surface: runs the amend door with dry_run FORCED (accepts patch or graph), returns the preview + graph_diff and touches nothing; stop=request stop; list=all runs; save=shelve a graph in the library under a name (from run_id or inline graph); library=list shelved graphs richly (name, description, tags, provenance, path-relative id). validate=dry-run the door's validation pipeline (defaults fill + defect collection + model/route policy) with no ping and no writes; returns {ok, errors:[{node,field,msg}], resolved_routes}. run from=<name> replays a shelved graph. submit=quarantine a hand-rolled graph for study (requires why_not_library >=80 chars; never joins the library — the quartermaster's human-gated loop decides); inbox kind=submissions lists them newest-first. doctor_version=read-only version truth for THIS install: {live_version, newest_packaged, source_commit, drift} comparing plugin.yaml against the install.json provenance that pack.py stamps at build time \u2014 one read, no network.",
        },
        "run_id": {"type": "string", "description": "Run id (required for every action except run/list)."},
        "name": {"type": "string", "description": "run: overrides graph.name (default workflow); save: library name overrides graph.name (lowercase, [-_.]); library: return one entry with refs-derived required params and an instantiate command. amend: set graph.name in the replacement graph; omitting it retains the run name."},
        "from": {"type": "string", "description": "run: library graph name to replay (instead of graph or graph_path)."},
        "run_context": {"type": ["string", "object"], "description": "run only: non-empty string seed appended to every first-wave agent (including agents behind gate-only paths), OR non-empty map of identifier keys to non-empty strings replacing only explicit {run.KEY} in node goals/contexts, fan-out goals/item goals and gate questions. Library-sourced launches also render profile, echo string output, gate options and wait.until_argv; every surviving ref refuses before any write. Inline argv retains its existing unbound-placeholder refusal. Missing keys/malformed bindings reject before any run write — as does a seed against a graph with {run.KEY} refs, or a JSON-encoded map passed as a string. Values are persisted in prompts; do not supply secrets. A seed cannot replace baked literals."},
        "description": {"type": "string", "description": "save: one-line purpose shown by library/list."},
        "params": {"type": "object", "additionalProperties": {"type": "object"}, "description": "save: optional meta.params descriptions/default suggestions for run_context keys. Required keys derive from graph refs, never this metadata. Omission retains prior params; {} deliberately clears them. Missing/dead declarations warn, never prevent saving."},
        "tags": {"type": "array", "items": {"type": "string"}, "description": "save (optional): 1-10 discovery tags — legacy flat tokens (lowercase alnum [-_.] <=32) or faceted `facet:value` (facets: use_case, repo, domain, risk, note; values [a-z0-9._-] <=48; e.g. use_case:code-review). For explicit curation, library exposes tag_vocab for optional reuse. Resaving without tags keeps the entry's existing tags; stored in the meta envelope. library (optional): filter to entries carrying ALL listed tags (same 1-10 law as save — an empty array errors on BOTH verbs; omit tags for no filter); an empty result's tag_match_counts says which term starved."},
        "why_not_library": {"type": "string", "description": "submit (REQUIRED, >=80 chars): why no library graph covered this task — name the entries you checked and the shape you needed. Only explicit study submissions require this field; ordinary run does not."},
        "lane": {"type": "string", "description": "submit (optional, <=128 chars): lane label carried beside the submission for the quartermaster's triage; no scheduling effect."},
        "kind": {"type": "string", "enum": ["submissions"], "description": "inbox (optional): 'submissions' = list workflow submit study items newest-first (read-only; promotion is a human decision). Omit inside a child spawn to pull baked steering as before."},
        "team": {"type": "string", "description": "run (optional, <=64 chars): team label stamped into run.json and shown by list; no effect on scheduling."},
        "lane_key": {"type": "string", "description": "run (optional, <=128 chars): in-flight registry key — a second run with the same key while the incumbent is unfinished is deduped (no spawn; returns the incumbent's run_id); status lane_key=<key> reads the incumbent instead of run_id. Keys are global per runs root; prefix with <team>/ yourself."},
        "source": {"type": "string", "description": "save (optional, <=200 chars): where this graph came from (repo path, URL, skill) — records opt-in provenance {owner, source, saved_at, source_digest} in the library entry."},
        "graph_path": {"type": "string", "description": "run/save/amend/validate: absolute path to a caller-supplied local regular UTF-8 JSON graph file (max 1 MiB, no final symlink). Choose exactly one of graph, graph_path, or run's from / save's run_id. Validated before any write or spawn."},
        "graph": {
            "type": "object",
            "description": (
            "For run/amend: {name, nodes:[...], defaults:{schema, timeout, max_turns, reasoning, provider, model, context, require_route, fallback_models}} where `defaults` fills agent node keys the author left unset (explicit node keys always win; defaults.context is the shared preamble prepended once to each agent's own context) and where node = {id, type:'agent'|'gate'|'echo'|'join', after:[node ids], goal, context, "
            "schema (json-schema for child output), model, provider (optional explicit Hermes provider paired with model; passed as --provider; when unset it is INHERITED from a provider-qualified model alias/tier), toolsets, max_turns, timeout (s wall-clock kill, default 900), shape (recon|build|review|publish \u2014 fills max_turns/timeout from the measured p95 census presets when the author left them unset; "
            "explicit keys win), repo (optional path \u2014 absolute, or run-dir-relative \u2014 of the git lane this node owns: a done/partial whose lane still has uncommitted TRACKED changes commits as failed error_class incomplete_work instead of a false hand-off (the dad50be0 shape: fix green but uncommitted, downstream verifies the mutant); commit in the lane, then amend/re-run re-drives), run_budget (s, "
            "child's own budget), reasoning (a hermes reasoning effort: none|minimal|low|medium|high|xhigh|max|ultra \u2014 passed to the child as --reasoning; levels are validated PER ROUTE at the door against the resolved (provider, model) route's supported set, with the supported list and nearest level in the error \u2014 no silent downgrade), "
            "resume_hint (bool, optional; prompt-side only: on a RE-RUN spawn (transient retry, rate-limit park, bounded re-drive) the runner prepends a dead-attempt artifact inventory (work-dir files, this node's prior spawn logs, runner.log; size/mtime/last lines). absent = auto: fires only when the durable work dir already holds a prior attempt's files; true = always; false = never, prompts stay pre-#130; never part of the fingerprint), require_route (bool, "
            "default TRUE on nodes that pin an explicit model: the door's submit ping affirmatively proving the pinned route dead \u2014 or answered from the fallback ladder \u2014 refuses the launch instead of silently billing another model; set false to opt into the ladder explicitly; the ping proving a route alive additionally binds the runner's served-model hold; "
            "when the owner declares an estate `confidence_substrate` (plugin settings / workflows config), a proved-dead pin resolves to the first ping-alive declared rung instead of refusing \u2014 "
            "engine-stamped substitution with a machine-injected result-schema disclosure (#116); no config = the refusal, verbatim; the sibling `route_verified` proof annotation is DOOR-BAKED only \u2014 never author-writable, "
            "an author value is dropped at resolve and re-proved by this submit's ping), inputs:['<ancestor>' | '<ancestor>.<dotted.path>', ...] (inject a committed upstream output into the prompt as a labelled json block under '## Inputs'; unresolvable ref fails the node at spawn; "
            "DIRECT parents from `after` are auto-injected capped at 8KB with a truncation marker \u2014 use inputs only to pick a dotted path or a non-parent ancestor; a parent listed in both appears once), fanout:{items | items_from:'<node_id>.<dotted.path>', goal (OPTIONAL template; an item's own `goal` key overrides it \u2014 when items carry their own goals the shared node goal prefixes each item prompt, "
            "so no placeholder template is ever needed), schema, quorum (OPTIONAL positive int; ONLY when set: once quorum items have committed, the still-running stragglers are cancelled with error_class 'cancelled' and excluded from the failure math; when unset there is NO default \u2014 the fan-out waits for every item)"
            ", quorum_drain_s (OPTIONAL number >= 0; ONLY meaningful WITH an explicit quorum: the quorum cancel is deferred by this many seconds — ONE drain window armed at the quorum moment lets stragglers still running then land their answer (anything that lands inside the window commits like any other item); the stragglers still running at its deadline are cancelled exactly as before. Default 0 = the documented today-behavior: cancellation is immediate at the quorum moment)"
            ", ledger (OPTIONAL item-indexed INPUT LEDGER: one row per item, "
            "positionally aligned; a row is a '<node_id>.<dotted.path>' input-ref string or {source:'<node_id>.<dotted.path>', artifact?:{file:rel/path under the run dir, sha256?:64-hex}} naming the on-disk artifact the item consumes. Admission refuses the graph fail-closed unless rows map 1:1 to items: a missing/extra row, two items sharing one source, a source head outside the node's after ancestry, "
            "or (when run-dir bytes are measurable \u2014 amend/validate) an absent artifact file or sha256 mismatch are each refused with an error naming the item and the missing/extra source \u2014 an item can never silently consume another item's artifacts (#85))}, "
            "on_fail ('skip' | '<fallback-agent-node-id>' \u2014 on this node's failure commit it `skipped` (join-tolerant, run continues; a join with one live dep still runs), and with a fallback id ALSO let that agent node run; validated at run time: fallback must exist, be an agent, not be an ancestor; cancelled deaths are never caught"
            ")} \u2014 agent node. A provider requires a non-empty model; "
            "model aliases are preserved; a literal 'provider/model' id keeps its route identity but is split into provider + model when the seat routes that provider (tiers resolve explicitly, and a matching provider/model prefix is removed for the CLI). Run/amend responses include requested/resolved provider/model routes. Gate node: {id, type:'gate', after, question, options, context, when (bounded expr: out.<node>.<dotted.path> with == != > >= < <=, and/or/not, parens; "
            "malformed when is rejected at run/amend validation and holds the gate at fire \u2014 never a silent skip), wait:{wait_s, until_argv:[fixed argv, no shell], every_s (default 60), timeout_s (default 3600)} (machine-answered gate: parks the run at zero tokens \u2014 wait_s alone = timer; until_argv re-runs until exit 0; timeout \u2192 gate fails; its last stdout/stderr tail is the gate's output, "
            "usable via inputs). A human release pre-empts a park), on_skip:'pass'|'prune' (with when: prune commits the gate `skipped` and every node whose deps are ALL skipped is skipped too \u2014 terminal, not a failure; a join with one live dep runs; default pass = the arm still runs)}. Echo node: {id, type:'echo', after, "
            "output} — commits its `output` verbatim as the node result with zero tokens and no child spawn; downstream nodes consume it via after/inputs like any done node. Composite graphs: a top-level `include:[{as, use, seeds?, exports?}]` names library graphs "
            "to expand into this graph at materialize time (never a node type): `as` is the alias (alnum start, [A-Za-z0-9_], no hyphen or dot, <= 24 chars, unique), `use` the library name, `seeds` a closed map {KEY: non-empty string} that renders {run.KEY} inside the "
            "included subtree ONLY, `exports` an optional {inner_id: public_name} map. Included nodes land namespaced as `alias__<id>`; parent refs must name an exported public name or a literal alias__id (a bare inner id is refused); a present-but-malformed "
            "include (null, object, empty list) is refused; an included graph's model_policy.forbidden_models unions into the parent (noted in include_notes); the committed graph.json is "
            "the EXPANDED, include-STRIPPED truth (amend edits the expanded form; save shelves the author form with the include key — save(run_id) of a composite run refuses, save the author graph inline). Every guard refusal — unknown library entry, include cycle, alias/id collision, unbound seed, oversized merge — returns the same "
            "errors:[{node:'include:<alias>', field, msg}] envelope before any write or spawn; non-fatal resolver warnings (e.g. a shared fixed scratch path) echo as include_notes on run/status and run.json records provenance `includes:[{alias, name, source_digest}]`. "
            "Full grammar and worked examples: references/grammar.md in the `workflow` skill — read it before authoring your first graph. "
            "Join node: {id, type:'join', after:[ids], keys:{label:'<node_id>.<dotted.path>', ...}, wait:'terminal'|'any'} "
            "— commits a deterministic json object {label: resolved parent output} at the wave boundary with zero tokens "
            "and no child spawn (keys committed sorted by label); wait:'terminal' (default) waits for every `after` parent "
            "to settle and fails the join if any failed; wait:'any' fires once one parent is done/partial and drops the "
            "keys of failed/skipped legs (fan-out-quorum flavour); a key whose committed parent lacks the dotted path "
            "fails the node as an author typo. "
            "Any key outside these closed sets is rejected at run/amend with errors:[{node, field, msg}] for EVERY defect."
            ),
        },
        "answer": {"type": "string", "description": "release: the human's answer text (from clarify)."},
        "graph_revision": {"type": "string", "description": "amend/steer/release: optional CAS token — the run's current graph revision (from run/status/amend). A token that differs from the head is refused with {error:'graph_revision_stale', yours, head} and NOTHING is written (no graph.json, no amends row, no restart.request, no inbox line, no gate answer). Absent = last-writer-wins (today's behavior). amend echoes the new head as graph_revision."},
        "gate_id": {"type": "string", "description": "release: gate node id."},
        "node": {"type": "string", "description": "steer: target node id (pending node picks it up at spawn; a LIVE child pulls it at its next seam via its own inbox call — the prompt is never rewritten)."},
        "text": {"type": "string", "description": "steer: the steering message."},
        "dry_run": {"type": "boolean", "description": "amend: true = validate + preview {added, removed, changed, will_rerun, unchanged, graph_diff} ONLY — nothing is written, the runner is not touched. Normal amends echo the same lists. run: true = the same pre-launch gates (validate, bind, profile, policy, model resolve, route ping, quota, route enforcement) return {ok, dry_run, models, routes} and NOTHING is written — no run dir, no lane entry, no spawned runner."},
        "patch": {"type": "object", "description": "amend/diff: per-node field patch instead of resubmitting the whole graph: {\"nodes\": {\"<existing id>\": {\"<field>\": value}}, \"name\": str?} — shallow merge per node, null deletes a field; unknown ids, id changes, new nodes, and other top-level keys are refused by name. Exclusive with graph/graph_path. The patched graph goes through the FULL amend door (validation, CAS, frozen replay, preview); amends.jsonl still records the resolved full graph."},
        "timeout": {"type": "number", "description": "wait: max seconds to block while a runner is live (default 600, capped 1800). Self-yields ~330s segments under the harness tool deadline with status+note — call wait again until terminal."},
        "detail": {"type": "string", "enum": ["full"], "description": "status/wait: 'full' attaches every committed node output and full spawn argv; the default is compact — mid-run waits carry output pointers (keys+bytes) only, terminal payloads always include outputs."},
    },
    "required": ["action"],
}

# Registry shape = {description, parameters}; a bare JSON-schema object registers fine but
# tool_describe reads fn["parameters"] and hands the model {} (papercut 2026-09-22).
WORKFLOW_SCHEMA = {
    "description": (
        "Multi-agent workflow graphs: run a DAG of agent/gate nodes (fan-out, human gates, replay-skip resume, "
        "steering, amend, per-node model tiers) as a background runner owned by this session. "
        "Use it for independent lanes, a human gate, or a resumable graph; for one or two "
        "independent calls use delegate_task instead. "
        "Argument shapes are in parameters; the `workflow` skill has the grammar and examples."
    ),
    "parameters": WORKFLOW_PARAMS,
}

# ---------- model tiers ----------
# User-owned vocabulary: plugins.entries.hermes-workflows.settings.models is a dict
# {tier: model} with ARBITRARY keys ("worker"/"frontier", "1".."5", whatever the
# owner thinks in). node.model accepts a literal model id OR one of those keys.
# Resolution happens HERE at run/amend and the literal is BAKED into the node def:
# it lands in the fingerprint (replay-deterministic), run.json shows what actually
# ran, and remapping a tier later never retroacts onto a live run. A provider field
# is kept explicit and passed to the CLI; no model id is rewritten to an alias.
# Unknown tier keys fail-closed with the valid list.
_CTX = None

def model_tiers():
    tiers = {}
    try:
        tiers = _CTX.get_config("models", {}) if _CTX else {}
    except Exception:
        tiers = {}
    return {str(k): str(v) for k, v in (tiers or {}).items() if v}

_PREFLIGHT_NOT_LIVENESS = ("preflight proves RESOLUTION, not liveness — a literal model id "
                           "is never checked for reachability and is left as-is.")

import difflib

def _route_efforts(provider, model):
    """Prefer the core route API; on older cores use the Codex vocabulary for
    openai-codex, otherwise the global set. Guard every host import."""
    if not (provider or model):
        return _common.reasoning_levels()
    try:
        from agent.reasoning_effort import route_supported_efforts
        sup = tuple(route_supported_efforts(provider, model))
        if sup:
            return sup
    except Exception:
        pass
    if str(provider or '').strip().lower() == 'openai-codex':
        try:
            from agent.reasoning_effort import codex_supported_efforts
            sup = tuple(codex_supported_efforts(model))
            if sup:
                return sup
        except Exception:
            pass
    return _common.reasoning_levels()

def _nearest_effort(level, supported):
    """Nearest supported ladder level (weaker first — never an escalation), or None."""
    try:
        from agent.reasoning_effort import EFFORT_LADDER
    except Exception:
        return None
    if level not in EFFORT_LADDER:
        return None
    idx = EFFORT_LADDER.index(level)
    pool = [l for l in supported if l in EFFORT_LADDER and l != "none"]
    below = [l for l in pool if EFFORT_LADDER.index(l) < idx]
    if below:
        return max(below, key=EFFORT_LADDER.index)
    above = [l for l in pool if EFFORT_LADDER.index(l) > idx]
    return min(above, key=EFFORT_LADDER.index) if above else None

def _alias_provider_pair(requested, seat_raw, tiers, known_names=()):
    """(provider, model) the alias/tier TARGET names — 'provider/model'-prefixed seat
    aliases/tier targets and the core model_aliases (DIRECT_ALIASES) table; provider None
    when the target is bare (the seat default route stays authoritative), model = the
    bare target itself so the reasoning route table still speaks the real model id.
    Names the SEAT already owns (seat aliases/default/tiers) never consult the core
    table — the seat config is authoritative for its own names."""
    name = str(requested or "").strip()
    for target in ((dict((seat_raw or {}).get("aliases") or {}).get(name) or "").strip(),
                   str((tiers or {}).get(name) or "").strip()):
        if "/" in target:
            p, _, m = target.partition("/")
            if p.strip() and m.strip():
                return p.strip(), m.strip()
        elif target:
            return None, target
    if name.lower() in {str(k).strip().lower() for k in known_names}:
        return None, None
    try:
        from hermes_cli.model_switch import _ensure_direct_aliases, DIRECT_ALIASES
        _ensure_direct_aliases()
        da = DIRECT_ALIASES.get(name.lower())
        if da and da.provider and da.model:
            return da.provider, da.model
    except Exception:
        pass
    return None, None

def model_preflight(requests, tiers, seat_raw):
    """Pure (no I/O): the FEEDBACK #43 model preflight, run at run/amend submit time
    after the route table is computed and before the first wave (spawn).

    Every requested name that claims to be an alias must resolve to a non-empty target
    in the SAME seat config the CLI uses (``seat_raw`` = the unfiltered
    ``{"default", "aliases"}``); an empty/blank target resolves to nothing and is
    REJECTED, listing which nodes share the dead model. Tier keys were already resolved
    against a non-empty target by ``model_tiers()`` (empty targets are filtered there,
    which makes such a key surface as an unknown model). Literal ids pass through: this
    proves RESOLUTION, not liveness — a literal model id is never checked for
    reachability and is left as-is. The error text states this verbatim.

    `requests` = [(node_id, requested_model, provider), ...]; the tail of
    ``_resolve_models`` passes the extended shape (..., node, eff_provider,
    eff_model) so the PER-ROUTE reasoning check validates the resolved route
    (inherited provider included) without a second resolution pass. Returns an
    error string or None. Exactly one call site (no duplicate resolution logic).
    """
    aliases = dict((seat_raw or {}).get("aliases") or {})
    default = (seat_raw or {}).get("default")
    dead = {}
    route_errs = []
    for req in requests:
        nid, m, _provider = req[0], req[1], req[2]
        node = req[3] if len(req) > 3 else None
        if not m:
            continue
        if m in aliases and not str(aliases[m]).strip():
            dead.setdefault(m, []).append(nid)
        if node is not None:
            lv = node.get("reasoning")
            pp = req[4] if len(req) > 4 else node.get("provider")
            mm = req[5] if len(req) > 5 else node.get("model")
            if lv and (mm or pp):
                sup = _route_efforts(pp, mm)
                if lv not in sup:
                    near = _nearest_effort(lv, sup)
                    route = f"{pp}/{mm}" if pp and mm else (pp or mm)
                    suggest = (f" — nearest supported level: {near!r}" if near else "")
                    route_errs.append(
                        f"node {nid!r}: reasoning {lv!r} is not supported by route {route!r} "
                        f"(supported: {list(sup)}){suggest} — pick a supported level; the door "
                        f"never silently downgrades reasoning.")
    if dead:
        listed = "; ".join(f"{m!r} (nodes: {', '.join(ids)})" for m, ids in sorted(dead.items()))
        return (f"model preflight: {listed} — dead seat alias: the name resolves to nothing "
                f"(empty alias target in the seat config). Fix the seat's model.aliases or "
                f"repoint these nodes. Note: {_PREFLIGHT_NOT_LIVENESS}")
    if route_errs:
        return "model preflight: " + " | ".join(route_errs)
    return None

_ROUTE_KEYS = ("model", "tier", "provider", "route_verified",
               "substrate_substituted")   # #25: the door's proof; #116: the door's
# substitution stamp rides the same frozen-restore loop — a replay-skipped node
# keeps its engine substitution verbatim.
# (F2): the "def unchanged → keep the committed bake verbatim" comparisons exclude
# the proof annotation — the author can never legally write route_verified (it is
# popped pre-submit / restored from the commit), so requiring equality on it would
# silently disable the un-bake AND the frozen replay for every alive-proved node.
# The frozen RESTORE loop still carries it (it should ride like a route key).
_ROUTE_MATCH_KEYS = ("model", "tier", "provider")
# alive-proof rides frozen restore + re-bake like a route key: a committed node's
# proof restores verbatim (replay-skip holds across amends); a re-resolved node's
# bake is dropped and re-proved by this submit's ping (never trusted from the author).

def _resolve_models(nodes, committed=None, keep=()) -> tuple[str | None, dict | None, dict | None]:
    """Resolve tier keys in place and return (error, model_table, routes).

    Explicit aliases stay unchanged. A literal 'X/Y' with no provider, X a provider the
    seat routes, is split to provider=X, model=Y (est-2ek.1.46); `routes.requested`
    keeps the author's literal, and policy/freeze/quota compare the full 'X/Y'. If a provider is supplied and the
    model is prefixed by that same provider, strip the redundant prefix for the CLI's
    ``-m`` value. `routes` contains requested and effective provider/model pairs only;
    no credentials or config values. At the tail, the FEEDBACK #43 ``model_preflight``
    rejects names that resolve to nothing (fail-closed at submit, never at spawn).

    amend only (fb 034849a23af94418): `committed` = {id: committed graph.json def},
    `keep` = ids that will replay-skip (see _frozen_committed). A kept node restores
    its committed model/tier/provider verbatim and is never re-resolved against the
    current seat. Any other node still carrying its committed bake verbatim is
    UN-baked back to its tier key, so it routes exactly as the author graph would.
    act_run passes neither.
    """
    tiers = model_tiers()
    table, routes = {}, {}
    # tier TARGETS are seat-owned names: resolution bakes them, so re-resolving a baked
    # def must accept them (idempotence; fb 034849a23af94418).
    known = set(_seat_aliases()) | ({_seat_default()} - {None}) | set(tiers.values())
    committed = committed or {}
    requests = []
    for n in nodes:
        if kind(n).spawns is not True:   # non-agent (gate/echo/join): no model route
            continue
        c = committed.get(n.get("id")) or {}
        frozen = bool(c) and n.get("id") in keep
        if not frozen:
            # #25: an alive-proof is ONLY ever the door's. A re-driven node's proof
            # from a previous submit (or an author-supplied one — the validator set
            # carries the key so the RUNNER can load a committed graph) drops here;
            # this submit's ping re-proves it or the node simply runs un-held.
            # Frozen (replay-skip) nodes restore the committed proof via _ROUTE_KEYS.
            n.pop("route_verified", None)
            # #116: the same law for the substitution stamp — an author value is
            # dropped (the runner's closed set carries the key so the committed
            # graph loads); only this submit's dead-pin + declared-substrate bake
            # is trusted. A frozen (replay-skip) node restores its committed stamp
            # verbatim below via _ROUTE_KEYS; a NON-frozen node whose route keys
            # (model/provider/tier) are unchanged from a committed def that carried
            # the stamp restores it verbatim too (the F2 un-bake law: an amend that
            # didn't move the route must not launder away the engine's disclosure
            # stamp), while any route change — or an author stamp with no matching
            # commit — drops it and lets this submit's ping+config re-prove.
            c_st = c.get("substrate_substituted") if c else None
            if n.get("id") not in keep:
                same_route = bool(c_st) and all(n.get(k) == c.get(k) for k in _ROUTE_MATCH_KEYS)
                if same_route:
                    n["substrate_substituted"] = c_st
                else:
                    n.pop("substrate_substituted", None)
        if frozen:
            for k in _ROUTE_KEYS:
                if c.get(k) is None:
                    n.pop(k, None)
                else:
                    n[k] = c[k]
        elif (c and c.get("tier") in tiers
              and all(n.get(k) == c.get(k) for k in _ROUTE_MATCH_KEYS)):
            n.pop("provider", None)
            n.pop("tier", None)
            n["model"] = c["tier"]
        requested_model = n.get("model")
        provider = n.get("provider")
        m = requested_model
        tier = n.get("tier") if frozen else None
        display = "(seat default)"
        if not m or frozen:
            pass
        elif m in tiers:
            tier = m
            n["model"] = m = tiers[m]
            n["tier"] = tier
        elif "/" in m or m in known or provider:
            pass
        else:
            near = difflib.get_close_matches(m, sorted(set(tiers) | known), n=3)
            hint = f" — did you mean {near}?" if near else ""
            return (f"node {n['id']!r}: unknown model {m!r}{hint} — not a tier, alias, or "
                    f"provider/model id; tiers: {sorted(tiers)}; aliases: {sorted(known)}", None, None)
        # provider INHERITED from the model's alias when the node sets model but not
        # provider: a 'provider/model'-prefixed seat alias, a tier target, or a core
        # model_aliases entry carries its own route (the 19 "model X not supported when
        # using Codex" provider_400s were nodes that left provider unset). [N]-tier: the
        # author never writes the provider the alias already names. The model string
        # itself stays verbatim (house contract: aliases are preserved); only the
        # provider is baked so the runner's --provider matches the alias's own route.
        ip = im = lit_provider = None
        if m and not provider and not frozen and (tier or m in known):
            ip, im = _alias_provider_pair(requested_model, _seat_model_cfg(), tiers, known)
            if ip:
                n["provider"] = ip
        if (m and not provider and not frozen and not ip and not tier
                and "/" in m and m not in _seat_aliases()):
            # est-2ek.1.46: a literal 'provider/model' with no node.provider used to reach the
            # child as a bare `-m provider/model` (seat default route + prefixed id -> HTTP 400)
            # while the alias for the same model worked. Bake the provider the same way aliases
            # do, but ONLY for a provider the seat itself routes through: an aggregator's
            # 'vendor/model' namespace is not a provider claim. `requested` below keeps the
            # author's literal (provider None).
            lp, _, lm = m.partition("/")
            if lp.strip() and lm.strip() and lp in _seat_provider_ids(tiers):
                ip = lit_provider = n["provider"] = lp
                n["model"] = m = lm
        eff_provider = provider or ip
        if m and provider:
            prefix, sep, remainder = m.partition("/")
            if sep and prefix == provider and remainder:
                n["model"] = m = remainder
        if m:
            # display: author-explicit provider names the route (base semantics);
            # an inherited provider shows in `routes` only.
            display = f"{provider or lit_provider}/{m}" if (provider or lit_provider) else str(m)
            if tier:
                display += f"  ({tier})"
        table[n["id"]] = display
        # the alias/tier TARGET's model when the name is an alias (the CLI resolves it;
        # the route table — pinged AND reported — speaks the real model id, est-2ek.1.318),
        # else the baked literal. The node def keeps the alias verbatim.
        route_model = im if (tier or m in known) and im else m
        routes[n["id"]] = {
            # requested = what the AUTHOR wrote (an inherited provider is not a request);
            # resolved = the effective route (alias target, else node def after resolution).
            "requested": {"provider": provider or None, "model": requested_model or None},
            "resolved": {"provider": n.get("provider") or None, "model": route_model or None},
        }
        # route for the PER-ROUTE reasoning check (same target id as the route table).
        requests.append((n["id"], requested_model, provider, n,
                         eff_provider, route_model))
    pf_err = model_preflight(requests, tiers, _seat_model_cfg())
    if pf_err:
        return pf_err, None, None
    return None, table, routes

def resolve_models(nodes):
    """Compatibility wrapper: resolve models and return the historical (error, table) pair."""
    err, table, _routes = _resolve_models(nodes)
    return err, table

# ---------- FEEDBACK #152be7f7: preflight LIVENESS ping (warn-and-surface) ----------
# model_preflight proves RESOLUTION, not liveness; a live-but-quota-dead seat used to
# first fail hours later at the first child spawn (transport_exhausted after the retry
# ladder), and the server's Retry-After never survived to the door. This ping fires at
# run/amend submit and ANNOTATES the existing routes entries only — it NEVER blocks the
# launch. Every failure path (timeout, core not importable, transient 5xx, parse
# failure, fallback-ladder surprise) maps to liveness='unknown' and the run launches.
# No new door schema keys: the annotation rides the existing routes dict; the dead
# copy rides the existing hint.
_PING_TASK = "wf-preflight-ping"
PING_TIMEOUT_S = 10.0
_PING_DEAD_STATUSES = (401, 403, 404, 429)
_PING_NOTE_MAX = 160
_PING_KEYISH = re.compile(r"(sk-[A-Za-z0-9_\-]{8,}|Bearer\s+\S+|api[_-]?key\s*[=:]\s*\S+)", re.I)

def _import_call_llm():
    """Call-time lazy core import (rule 7: stdlib at import time; host imports lazy and
    guarded). Raises on non-core hosts — the caller maps that to liveness='unknown'."""
    from agent.auxiliary_client import call_llm
    return call_llm

def _ping_status(exc):
    """Best-effort HTTP status of a ping failure: the SDK attribute first, then the
    openai escape-line format (str(exc) = 'Error code: NNN - {body}'). None otherwise."""
    st = getattr(exc, "status_code", None)
    if isinstance(st, int) and 100 <= st <= 599:
        return st
    m = re.search(r"Error code:\s*(\d{3})", str(exc))
    return int(m.group(1)) if m else None

def _ping_retry_after(exc):
    """Server Retry-After, best-effort via core's parser. None when no header — NEVER
    fabricated."""
    try:
        from agent.retry_utils import parse_retry_after_seconds
    except Exception:
        return None
    try:
        return parse_retry_after_seconds(getattr(getattr(exc, "response", None), "headers", None))
    except Exception:
        return None

def _ping_note(exc, status):
    """Short, credential-scrubbed note for the annotation: type + status + head of str."""
    head = " ".join(str(exc or "").split())[:_PING_NOTE_MAX]
    head = _PING_KEYISH.sub("[redacted]", head)
    return (f"HTTP {status}: {head}" if status else f"{type(exc).__name__}: {head}").strip()

def _same_ping_route(ri, provider, model):
    """True when the route core RECORDED for the ping is the pinned route itself.
    call_llm's recovery ladder may answer from another lane (capacity errors bypass the
    explicit-provider gate); a ping whose recorded route is not the pinned one proves
    nothing about the pinned one — the caller degrades to 'unknown', never 'alive'/'dead'.
    The MODEL half also admits the ONE route-identity contract (wfcommon.route_ids_equal,
    est-2ek.1.699); a provider mismatch is never same-route."""
    def _lbl(v):
        v = str(v or "").strip().lower()
        m = re.search(r"\(([^()]+)\)\s*$", v)  # 'main-agent(openai)' / 'fallback_chain[0](x)'
        return m.group(1) if m else v
    rp, rm = _lbl((ri or {}).get("provider")), _lbl((ri or {}).get("model"))
    lp, lm = str(provider).strip().lower(), str(model).strip().lower()
    return bool(rp) and rp == lp and (rm == lm or rm == lm.rsplit("/", 1)[-1]
                                      or _common.route_ids_equal((ri or {}).get("model"), lm, lp))

_PING_SUBPROCESS = '''import json, sys
try:
    from agent.auxiliary_client import call_llm
    ri = {}
    try:
        call_llm(task="wf-preflight-ping", provider=sys.argv[1], model=sys.argv[2],
                 messages=[{"role": "user", "content": "ping"}], max_tokens=1,
                 timeout=float(sys.argv[3]), route_info=ri)
        verdict = {"route": ri, "ok": True}
    except Exception as e:
        import re
        status = getattr(e, "status_code", None)
        if not isinstance(status, int):
            m = re.search(r"Error code:\\s*(\\d{3})", str(e))
            status = int(m.group(1)) if m else None
        verdict = {"route": ri, "ok": False, "status": status,
                   "note": str(e)[:160], "kind": type(e).__name__}
    print(json.dumps(verdict))
except Exception:
    print(json.dumps({"unavailable": True}))
'''

def _ping_subprocess(provider, model):
    """Core-less cron door: use the operator's child launcher's venv Python.
    Missing binary, unrecognised wrapper, failed import, or timeout = unknown.
    Never run an author-supplied executable; hermes_bin is operator-controlled.
    """
    unknown = {"liveness": "unknown", "note":
               "core auxiliary client not importable (ModuleNotFoundError) — ping skipped"}
    try:
        launcher = Path(shutil.which(_hermes_bin()) or _hermes_bin())
        candidates = [launcher.parent / "python", launcher.parent / "python3"]
        # A shell shim may exec the actual venv launcher; follow only absolute
        # hermes paths in that operator-controlled shim, never arbitrary shell.
        if launcher.is_file() and launcher.stat().st_size < 16384:
            for path in re.findall(r"/[A-Za-z0-9_./+~-]+/hermes\b", launcher.read_text(errors="replace")):
                candidates.extend((Path(path).parent / "python", Path(path).parent / "python3"))
        python = next((p for p in candidates if p.is_file() and os.access(p, os.X_OK)), None)
        if python is None:
            return unknown
        env = os.environ.copy()
        # A venv alone may not have the source checkout on sys.path. The core
        # launcher sets PYTHONPATH to its repo root; mirror that for this -c call.
        repo = python.parent.parent.parent
        env["PYTHONPATH"] = str(repo) + os.pathsep + env.get("PYTHONPATH", "")
        p = subprocess.run([str(python), "-c", _PING_SUBPROCESS, provider, model,
                            str(PING_TIMEOUT_S)], env=env, capture_output=True,
                           text=True, timeout=PING_TIMEOUT_S + 2)
        if p.returncode or not p.stdout.strip():
            return unknown
        verdict = json.loads(p.stdout.strip().splitlines()[-1])
        if not isinstance(verdict, dict) or verdict.get("unavailable"):
            return unknown
        ri = verdict.get("route")
        if not isinstance(ri, dict):
            return unknown
        if verdict.get("ok"):
            if _same_ping_route(ri, provider, model):
                return {"liveness": "alive"}
            return {"liveness": "unknown", "wrong_route": True,
                    "note": "ping answered by a different route (fallback ladder) — not counted as alive"}
        status = verdict.get("status")
        if status in _PING_DEAD_STATUSES and _same_ping_route(ri, provider, model):
            note = _PING_KEYISH.sub("[redacted]", str(verdict.get("note") or "")[:_PING_NOTE_MAX])
            return {"liveness": "dead", "retry_after_s": None,
                    "note": f"HTTP {status}: {note}"}
        if status in _PING_DEAD_STATUSES:
            return {"liveness": "unknown",
                    "note": f"dead-status on an unattributed route {ri.get('provider')!r} — not counted"}
        return {"liveness": "unknown", "note":
                _PING_KEYISH.sub("[redacted]", str(verdict.get("note") or "")[:_PING_NOTE_MAX])}
    except Exception:
        return unknown

def _ping_route_once(provider, model):
    """One auxiliary ping on the pinned (provider, model) route — explicit provider AND
    model, ONE call, no ladder of ours; core's own recovery is only trusted when the
    recorded route is the pinned one. NEVER raises: returns the annotation dict."""
    try:
        call_llm = _import_call_llm()
    except ModuleNotFoundError:
        return _ping_subprocess(provider, model)
    except Exception as e:
        return {"liveness": "unknown",
                "note": f"core auxiliary client not importable ({type(e).__name__}) — ping skipped"}
    ri = {}
    try:
        call_llm(task=_PING_TASK, provider=provider, model=model,
                 messages=[{"role": "user", "content": "ping"}],
                 max_tokens=1, timeout=PING_TIMEOUT_S, route_info=ri)
    except Exception as exc:
        # The annotation itself must never raise (a hostile __str__ is still just an
        # unknown, never a crash at submit — fail-open is the whole contract).
        try:
            st = _ping_status(exc)
        except Exception:
            st = None
        if st in _PING_DEAD_STATUSES and _same_ping_route(ri, provider, model):
            return {"liveness": "dead", "retry_after_s": _ping_retry_after(exc),
                    "note": _safe_ping_note(exc, st)}
        if st in _PING_DEAD_STATUSES:
            return {"liveness": "unknown",
                    "note": f"dead-status on an unattributed route {ri.get('provider')!r} — not counted"}
        return {"liveness": "unknown", "note": _safe_ping_note(exc, st)}
    if not _same_ping_route(ri, provider, model):
        return {"liveness": "unknown", "wrong_route": True,
                "note": "ping answered by a different route (fallback ladder) — not counted as alive"}
    return {"liveness": "alive"}

def _ping_reachable(model):
    """Weak-law recovery probe for a PROVIDER-LESS quota entry (A5): the same-route
    ALIVE law can never fire without an explicit provider (_same_ping_route needs a
    recorded provider to match), so the docstring's recovery promise for a
    seat-default stamp was unreachable — it stayed refused until expiry no matter how
    healthy the model was. Recovery is weaker than attribution: an answered ping whose
    RECORDED MODEL is the stamped one (core's own routing, provider-agnostic label
    match) proves reachability and drops the cache entry. NEVER proves alive/dead
    (that law stays gated on an explicit provider) and never raises: an exception,
    an unimportable core, or a fallback-ladder answer (recorded model != stamped) is
    NOT a recovery — a wrong-route answer proves nothing, same law as #25."""
    try:
        call_llm = _import_call_llm()
    except Exception:
        return False
    ri = {}
    try:
        call_llm(task=_PING_TASK, model=str(model),
                 messages=[{"role": "user", "content": "ping"}],
                 max_tokens=1, timeout=PING_TIMEOUT_S, route_info=ri)
    except Exception:
        return False
    rm = str(ri.get("model") or "").strip().lower()
    m = re.search(r"\(([^()]+)\)\s*$", rm)      # 'main-agent(openai)' label shape
    if m:
        rm = m.group(1)
    lm = str(model).strip().lower()
    return bool(rm) and (rm == lm or rm == lm.rsplit("/", 1)[-1]
                         or _common.route_ids_equal(ri.get("model"), lm, lm.partition("/")[0] if "/" in lm else None))

def _safe_ping_note(exc, status):
    try:
        return _ping_note(exc, status)
    except Exception:
        return f"{type(exc).__name__}: (unprintable error)"

def _route_liveness_ping(routes):
    """FEEDBACK #152be7f7: warn-and-surface liveness, called ONCE at the _resolve_models
    tail of BOTH submit paths (act_run / act_amend), after model_preflight (which stays
    pure — the ping is I/O and therefore lives at the call site, not inside resolution).
    One ping per DISTINCT resolved (provider, model) pair; the outcome annotates every
    route entry sharing that pair IN PLACE (existing routes dict — no new door schema
    keys). A resolved route with no explicit provider+model cannot be pinned without
    letting core auto-route (the fallback surprise condition(1) forbids) — it honestly
    gets liveness='unknown', ping skipped. Returns dead-route hint fragments; the run
    LAUNCHES regardless of every outcome."""
    notes = []
    if not routes:
        return notes
    by_route = {}
    for nid in sorted(routes):
        ent = routes[nid] or {}
        res = ent.get("resolved") or {}
        p, m = res.get("provider"), res.get("model")
        if not p or not m:
            ent["liveness"] = "unknown"
            ent["note"] = "no explicit provider+model to pin (seat default) — ping skipped"
            continue
        by_route.setdefault((str(p), str(m)), []).append(nid)
    for (p, m), ids in by_route.items():
        ann = _ping_route_once(p, m)
        for nid in ids:
            routes[nid].update(ann)
        if ann.get("liveness") == "dead":
            ra = ann.get("retry_after_s")
            when = (f"retry after {int(ra)}s" if isinstance(ra, (int, float))
                    else "retry-after absent")
            notes.append(f"route {p}/{m}: dead ({when}) — repoint node(s) "
                         f"{', '.join(ids)} before dispatch")
    return notes

def _liveness_hint_suffix(notes):
    """Dead-route copy appended to the run/amend hint (agent-visible, warn-and-surface):
    states the verdict AND the resolution-not-liveness caveat verbatim-flavored."""
    if not notes:
        return ""
    return (" — preflight liveness: " + "; ".join(notes)
            + f". Note: {_PREFLIGHT_NOT_LIVENESS} the submit ping is auxiliary and never "
              "blocked this launch; repoint the dead routes before their nodes spawn.")

# ---------- #24/#25: fail-closed route gates at submit (field-report asks) ----------

def _require_route_effective(node, graph):
    """#25: node key > graph defaults > default True on nodes that pin an explicit
    model. A node that pins nothing has nothing to hold (core auto-routes by design)."""
    v = node.get("require_route")
    if v is None:
        v = (graph.get("defaults") or {}).get("require_route")
    if v is None:
        return bool(node.get("model"))
    return bool(v)

def _confidence_substitute(n, ent, reason_note):
    """#116: declared-fallback branch of the #25 gate (R6: same path, not a parallel
    gate). Called ONLY when the submit ping affirmatively proved the node's pinned
    route unusable (dead / fallback-ladder surprise) AND the node did not opt out.
    Consults the estate `confidence_substrate` (wfcommon.confidence_substrate:
    owner-settings key or top-level `workflows:` config; absent = no ladder, the
    #25 refusal rides byte-identically). A rung serves only when ITS OWN ping proves
    it alive (the owner's ladder ruling: first alive rung wins). On success the node
    def is re-routed IN PLACE and engine-stamped (original pin, served substrate,
    reason, config source) with the result-schema disclosure clause machine-injected;
    the routes entry updates to the served route so the alive branch bakes
    `route_verified` against what will actually bill. Returns the stamp dict or None
    (never raises the submit: an unusable config is simply no ladder)."""
    try:
        rungs, source = _common.confidence_substrate()
    except Exception:
        return None
    for rung in rungs:
        p, _, m = rung.partition("/")
        try:
            ann = _ping_route_once(p, m)
        except Exception:
            continue
        if ann.get("liveness") != "alive":
            continue                                   # next rung; the ladder decides
        original = f"{(ent.get('resolved') or {}).get('provider') or ''}/" \
                   f"{(ent.get('resolved') or {}).get('model') or n.get('model') or ''}"
        stamp = {"from": original, "to": rung, "reason": reason_note, "source": source or "declared"}
        n["provider"] = p
        n["model"] = m
        n["substrate_substituted"] = stamp
        _common.apply_substrate_disclosure(n, stamp)
        ent["resolved"] = {"provider": p, "model": m}
        ent["liveness"] = "alive"
        ent["substituted_from"] = original
        ent["note"] = f"#116 confidence_substrate: pinned route unavailable " \
                      f"({reason_note}); sanctioned substrate {rung} ping-alive serves " \
                      f"(source {stamp['source']})"
        return stamp
    return None

def _route_enforcement(graph, routes, skip=(), models=None):
    """#25: a node that pins an explicit route and did NOT opt into the
    fallback ladder refuses to launch when the submit ping AFFIRMATIVELY proves the
    pinned route unusable — `dead`, or the fallback-ladder surprise (recorded route !=
    pinned route: the exact condition under which fb-fix-9c575645 silently billed the
    seat's fallback for 3 nodes). Infra-absence (core not importable, offline seats)
    stays unknown = warn-only: missing evidence never blocks (fail-open law). When the
    ping PROVED the route alive, the node def bakes `route_verified` = the verified
    "provider/model": the runner holds the committed served_model to it
    (route_unavailable). Returns the error string or None — the FIRST refusal fails
    the whole submit (a partial graph on a dead pinned route is a lie anyway).
    #116: the dead/surprise branch first consults the estate confidence_substrate
    (see _confidence_substitute); a substituted node re-enters the same alive/bake
    law against its served route; no config (or no alive rung) = this refusal, verbatim."""
    bad = []
    for n in graph["nodes"]:
        nid = n.get("id")
        if n.get("type") != "agent" or nid in skip or not n.get("model"):
            continue
        if not _require_route_effective(n, graph):
            continue
        ent = (routes or {}).get(nid) or {}
        res = ent.get("resolved") or {}
        p, m = res.get("provider"), res.get("model")
        if not p or not m:
            continue                                  # unpinnable: nothing to hold
        liv = ent.get("liveness")
        if liv == "dead":
            if _confidence_substitute(n, ent, f"HTTP-dead: {ent.get('note') or 'ping failed'}"):
                if models is not None and nid in models:
                    models[nid] = f"{n['provider']}/{n['model']} (confidence_substrate)"
                liv = "alive"                         # re-enter the bake law below
        elif liv == "unknown" and ent.get("wrong_route"):
            if _confidence_substitute(n, ent, "fallback-ladder surprise"):
                if models is not None and nid in models:
                    models[nid] = f"{n['provider']}/{n['model']} (confidence_substrate)"
                liv = "alive"
        if liv == "dead":
            bad.append(f"node {nid!r} pins {p}/{m}: route DEAD at submit "
                       f"({ent.get('note') or 'ping failed'}) — repoint it, or opt into "
                       f"the fallback ladder explicitly with require_route: false ON THAT "
                       f"NODE (a `defaults` flip opts the whole graph in)")
        elif liv == "unknown" and ent.get("wrong_route"):
            bad.append(f"node {nid!r} pins {p}/{m}: the ping answered from the FALLBACK "
                       f"LADDER (different route) — launching would bill a model you did "
                       f"not pin. Repoint it, or accept fallback with require_route: "
                       f"false ON THAT NODE (a `defaults` flip opts the whole graph in)")
        elif liv == "alive":
            # #318: bake what the ping ACTUALLY proved — the resolved route (alias
            # TARGET id when the node pins an alias), not the bare alias name.
            # A re-substituted node (#116) reads its served rung from the same place.
            res_now = ent.get("resolved") or {}
            rp = res_now.get("provider") or n.get("provider") or p
            rm = res_now.get("model") or n.get("model") or m
            n["route_verified"] = f"{rp}/{rm}"
    return ("route_unavailable at submit — " + "; ".join(bad)) if bad else None

def _quota_refusal(graph, routes=None, cache_path=None, skip=()):
    """#24 (c): refuse a launch whose node pins a model the seat KNOWS is
    subscription-exhausted (the runner's fatal_quota cache). Advisory by design:
    one recovery ping first — a route that answers pongs is alive again and the
    cache entry drops. A test-seated cache (WF_QUOTA_CACHE override) pings nothing.
    `skip` = node ids that replay-skip and never spawn (A1: the amend call passes
    the frozen set — a DONE node pinning a quota-marked model must not refuse the
    whole amend when it will not re-run). Returns the error string or None."""
    pth = Path(cache_path) if cache_path else (
        Path(os.environ.get("WF_QUOTA_CACHE")
            or (Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))
                / "cache" / "workflow-quota-cache.json")))
    try:
        cache = json.loads(pth.read_text()) if pth.exists() else {}
    except Exception:
        return None
    if not isinstance(cache, dict) or not cache:
        return None
    import time as _t
    offline = bool(os.environ.get("WF_QUOTA_CACHE"))
    for n in graph["nodes"]:
        if n.get("type") != "agent" or not n.get("model"):
            continue
        nid = n.get("id")
        if nid in skip:
            continue                                        # A1: replay-skip, never spawns
        ent = (routes or {}).get(nid) or {}
        res = ent.get("resolved") or {}
        names = {str(n.get("model")),
                 *( [str(res.get("model"))] if res.get("model") else [] )}
        # #291 F3: entries stamped under the full 'provider/model' id (the pre-bake
        # literal) still bind the baked node {provider, model}.
        req = (ent.get("requested") or {}).get("model")
        if req:
            names.add(str(req))
        if n.get("provider") and "/" not in str(n.get("model")):
            names.add(f"{n['provider']}/{n['model']}")
        for key in names:
            hit = cache.get(key)
            if not isinstance(hit, dict) or hit.get("resets_epoch", 0) <= _t.time():
                continue
            if not offline:
                # A5 (deep review): the same-route law makes _ping_route_once('')
                # unable to EVER attribute a pong (recorded provider can never match
                # ''), so the promised recovery ping could never clear a
                # provider-less entry. Recovery is a WEAKER law than attribution:
                # an explicit-provider route recovers on liveness=alive; a
                # provider-less (seat-default) route recovers on a pong whose
                # RECORDED MODEL is the stamped one (_ping_reachable: reachability,
                # not alive-attribution; an exception, an unimportable core, or a
                # fallback-ladder answer never recovers).
                prov = str(res.get("provider") or "")
                recovered = (_ping_route_once(prov, key).get("liveness") == "alive"
                             if prov else _ping_reachable(key))
                if recovered:
                    cache.pop(key, None)
                    try:
                        pth.write_text(json.dumps(cache))
                    except Exception:
                        pass
                    continue
            eta = datetime.fromtimestamp(hit["resets_epoch"], timezone.utc).isoformat(timespec="minutes")
            return (f"route_unavailable at submit — node {nid!r} pins {key!r}, marked "
                    f"quota-exhausted (resets {eta}Z, per the provider's own horizon). "
                    f"Amend the node to a live model, or the route recovers after the "
                    f"reset and the cache entry expires on its own.")
    return None

def _seat_model_cfg():
    """The seat's `model:` block ({default, aliases}) — hermes_cli when importable, else a
    stdlib-only read of config.yaml (tests, bare CLI: no hermes_cli, maybe no PyYAML)."""
    try:
        from hermes_cli.config import load_config_readonly
        cfg = load_config_readonly()
        if cfg:
            mc = cfg.get("model") or {}
            al = {str(k): (v if isinstance(v, str) else (v or {}).get("model") or "") for k, v in (mc.get("aliases") or {}).items()}
            # Keep EMPTY targets in what we return: the FEEDBACK #43 preflight must see an
            # alias that declares itself but resolves to nothing. Resolution consumers
            # derive their known-name sets via _seat_model_names(), which filters there.
            return {"default": str(mc["default"]) if mc.get("default") else None, "aliases": al,
                    "workflows_forbidden_models": _common.seat_forbidden_models()}
    except Exception:
        pass
    out = {"default": None, "aliases": {},
           "workflows_forbidden_models": _common.seat_forbidden_models()}
    try:
        home = _common.hermes_home()
        section = None; in_aliases = False
        for line in (home / "config.yaml").read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip())
            key, _, val = line.strip().partition(":")
            if indent == 0:
                section, in_aliases = key, False
                continue
            if section != "model":
                continue
            if indent == 2:
                in_aliases = key == "aliases"
                if key == "default" and val.strip():
                    out["default"] = val.strip().strip("'\"")
            elif indent > 2 and in_aliases and val.strip():
                out["aliases"][key.strip()] = val.strip().strip("'\"")
    except Exception:
        pass
    return out

def _seat_provider_ids(tiers=None):
    """Provider ids the SEAT itself routes through: the 'provider/' prefix of every seat
    alias and tier target, plus the configured `providers:` keys when hermes_cli is
    importable (stdlib-only hosts fall back to the alias/tier prefixes). Deliberately not
    the core's whole provider catalogue: an aggregator route's 'vendor/model' namespace
    must not be mistaken for a provider claim (est-2ek.1.46)."""
    out = set()
    for target in [*_seat_aliases().values(), *(tiers or {}).values()]:
        p, sep, rest = str(target).partition("/")
        if sep and p.strip() and rest.strip():
            out.add(p.strip())
    try:
        from hermes_cli.config import load_config_readonly
        out |= {str(k) for k in ((load_config_readonly() or {}).get("providers") or {})}
    except Exception:
        pass
    return out

def _seat_aliases():
    return _seat_model_cfg()["aliases"]

def _seat_default():
    return _seat_model_cfg()["default"]

def _seat_model_names():
    """Names the seat itself resolves for -m: model aliases + the default model."""
    return set(_seat_aliases()) | ({_seat_default()} - {None})

# ---------- run-dir plumbing ----------

def runs_root():
    """ONE resolver (wfcommon.runs_root): `settings.runs_root` (owner, #42) > `WF_RUNS_ROOT`
    > `<hermes_home>/workflows`. Read at call time — no restart."""
    return _common.runs_root()

def run_dir(run_id):
    """Strict: no silent normalization — ids double as directory names.
    Profile-scoped door on a multiplex host: pre-fix runs sit under the launch root —
    find_run keeps those ids resumable (resolved root wins when both exist)."""
    rid = (run_id or "").strip()
    if not rid or rid != "".join(c for c in rid if c.isalnum() or c in "-_.") \
            or rid.startswith(".") or set(rid) == {"."}:
        raise ValueError(f"invalid run_id {run_id!r}")
    return _common.find_run(rid)

_SIBLING_RID_BAD = re.compile(r"^\.\.?$|[\\/]")   # traversal is off-limits on the scan too

def _find_run_sibling_scan(rid):
    """#58 (READ paths only): resolve a run dir across the sibling known roots before
    answering unknown. The consumer's env may resolve a different home than the
    dispatcher's (profile-scoped seat vs estate shared root), stranding a demonstrably
    live run outside resolved-root + legacy launch root (all find_run covers). Known
    roots, from each base home (resolved and launch): the estate root and every
    profile under it when the base sits under a `profiles/` parent; every seat under
    `<base>/profiles/` when the base IS the estate; the base itself. First directory
    holding a graph.json wins. Returns the dir or None. READ-ONLY convenience: write
    verbs (amend/release/steer/stop/save) keep the strict resolved-root law."""
    if not rid or _SIBLING_RID_BAD.search(rid):
        return None
    resolved = _common.runs_root()
    homes = []
    for base in (_common.hermes_home(), _common.launch_runs_root().parent):
        homes.append(base)
        if base.parent.name == "profiles":                  # a profile -> estate + seats
            homes.append(base.parent.parent)
            try:
                homes.extend(p for p in sorted(base.parent.iterdir()) if p.is_dir())
            except OSError:
                pass
        try:                                                # an estate -> its seats
            homes.extend(p for p in sorted((base / "profiles").iterdir()) if p.is_dir())
        except OSError:
            pass
    seen = set()
    for h in homes:
        if str(h) in seen:
            continue
        seen.add(str(h))
        d = h / "workflows" / rid
        if d == resolved / rid:                             # the strict path's own turf
            continue
        try:
            if (d / "graph.json").exists():
                return d
        except OSError:
            continue
    return None

# ---------- graph library (named, re-runnable graphs) ----------

def library_root():
    return runs_root() / "library"

def _library_roots():
    """Resolved library root first; legacy launch-root library second (F1 #14).
    Graphs saved before the profile-home fix live under the launch root on a
    profile-scoped host and stay readable/replayable; new saves go to the
    resolved root only."""
    roots = [library_root()]
    legacy = _common.launch_runs_root() / "library"
    if legacy != roots[0]:
        roots.append(legacy)
    return roots

LIB_OK = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_GENERAL_PREFIX = "general/"
# est-qeul: the tag grammar is ONE source (wfcommon) — save validated writes and
# the canonical read fold in library_entry can never drift apart again. These
# stay as the door's own names (schema text/docs reference them); they alias the
# shared module's values, no copies.
TAG_OK = _common.TAG_OK
TAGS_MAX = _common.TAGS_MAX
TAG_RE = _common.TAG_RE
TAG_FACETS = _common.TAG_FACETS
TAG_LEN = _common.TAG_LEN
DESC_MAX = _common.DESC_MAX

def _norm_tags(tags):
    """#50/#70: delegates to wfcommon.norm_tags — see there for the law
    (1..TAGS_MAX flat or facet:value tokens, lowercase-trimmed, dups collapsed,
    fail-closed). Kept as the door's name so save/query call sites are unchanged."""
    return _common.norm_tags(tags)

def _lib_path(name):
    """WRITE resolver: always the resolved root (current best version lands there).
    #50: `general/<name>` addresses the curated public subset directory (#51)."""
    n = str(name or "").strip().lower()
    if n.startswith(_GENERAL_PREFIX):
        rest = n[len(_GENERAL_PREFIX):]
        if LIB_OK.match(rest):
            return library_root() / _GENERAL_PREFIX / f"{rest}.json"
    if not LIB_OK.match(n):
        raise ValueError(f"invalid library name {name!r} (lowercase alnum, [-_.], <=64)")
    return library_root() / f"{n}.json"

# est-2ek.1.599 (waves 26-31): ra-pr-deep committee LANES re-materialized their run
# graphs onto the SHARED shelf — the runner bakes HERMES_WF_RUN_DIR into every agent
# spawn (wf.py), so a process carrying it is a spawned child: it may only publish
# RUN-LOCAL copies (under its own run dir), never onto the shared library.
def _lane_shelf_guard(p):
    """Refuse a shared-shelf save from a spawned lane child (typed error_class
    lane_shelf_write); run-local saves and parent/owner saves pass untouched.

    r4 (recon #179 marker 5980069065): containment is judged on tgt_r — the
    realpath-resolved target — ONLY. The r3 raw-string branch let a run-dir-SHAPED
    path whose realpath escapes (pre-swapped <run>/library symlink) pass and land
    the graph on the SHARED shelf (saved=atk-sym-escape). Every path is compared
    under realpath (raw tgt kept for display only), plus tgt_r is compared against
    library_root(): any resolve into the shared shelf from a lane child refuses."""
    rd = str(os.environ.get("HERMES_WF_RUN_DIR") or "").strip()
    if not rd:
        return None
    tgt = os.path.abspath(str(p))          # raw — kept for display only
    tgt_r = tgt
    try:
        tgt_r = str(Path(tgt).resolve())
    except OSError:
        pass
    base_r = os.path.abspath(rd)
    try:
        base_r = str(Path(rd).resolve())
    except OSError:
        pass
    sep = os.sep
    def _inside(t, b):
        return t == b or os.path.dirname(t) == b or t.startswith(b + sep)
    # CLAUSE A (r5, recon #179 marker 5980900759): the runner bakes
    # HERMES_WF_RUN_DIR as an absolute dir UNDER the runs root it stamps (wf.py
    # spawn env; WF_RUNS_ROOT is in its keep-set forward and wins in both
    # resolvers, else the launch root — wfcommon.launch_runs_root), and wf.py
    # mkdir's the run dir for real. S3 swapped <run> for a symlink to an ANCESTOR
    # (the runs root itself) with the stamped env UNTOUCHED: base_r then swallowed
    # the shared shelf, the r4 conditional veto (cand_lib inside base_r)
    # self-skipped, and <runs>/library/atk-s3.json LANDED (saved=atk-s3). An
    # honest stamp is: a REAL dir (no symlink), realpath inside the stamped root,
    # and — when it collapses onto the root itself — raw-identical to it (the
    # legal T2 pin stamps WF_RUNS_ROOT AT the run dir). Anything else is an
    # upward escape: the lane's own-shelf exemption dies and CLAUSE B judges
    # tgt_r against realpath(library_root()) unconditionally. Deleting CLAUSE A
    # turns tests/test_lane_shelf_guard_1599.py T5 RED (the escape re-claims its
    # own-shelf exemption and the S3 save lands); deleting CLAUSE B turns T5 RED
    # too (nothing vetoes the shared-shelf landing).
    def _stamped_runs_root():
        override = os.environ.get("WF_RUNS_ROOT", "")
        return Path(override) if override else _common.launch_runs_root()
    raw_root = _stamped_runs_root()
    try:
        root_r = str(Path(str(raw_root)).resolve())
    except OSError:
        root_r = None           # fail closed: an unjudgeable root is an escape
    escape = (root_r is None or not _inside(base_r, root_r)
              or os.path.islink(rd)
              or (base_r == root_r
                  and os.path.abspath(rd) != os.path.abspath(str(raw_root))))
    # r3 raw-string refusals stay (fail-closed): a raw shape that does not even
    # claim the run dir is refused outright, before realpath can rescue it.
    raw_claims_run = tgt == base_r or tgt_r == base_r or os.path.dirname(tgt) == base_r \
            or tgt.startswith(base_r + sep) or tgt_r.startswith(base_r + sep)
    # containment is judged on tgt_r ONLY; the run dir itself under realpath.
    inside_run = _inside(tgt_r, base_r)
    # CLAUSE B (r5, unconditional): tgt_r is judged against realpath(library_root())
    # REGARDLESS of where base_r sits. The only lane-legal shelf is the lane's OWN
    # lexical <run>/library (base_r + '/library') under an HONEST run dir: no
    # upward escape (clause A) and lib_r equal to it, so neither a swapped
    # <run>/library symlink nor an ancestor-swapped <run> (which makes the shared
    # shelf trivially equal the lexical own shelf) can pass. The r4 conditional
    # (`if not _inside(cand_lib, base_r)`) self-skipped for S3-shaped base_r — that
    # is the veto that must judge tgt_r unconditionally.
    try:
        lib_r = str(Path(str(library_root())).resolve())
    except OSError:
        lib_r = None          # fail closed: an unjudgeable shelf is a shared shelf
    onto_shared = lib_r is None or escape or lib_r != base_r + sep + "library"
    if raw_claims_run and inside_run \
            and not (onto_shared and _inside(tgt_r, lib_r if lib_r is not None else "")):
        return None
    rid = str(os.environ.get("HERMES_WF_RUN_ID") or rd).strip()
    return {"error": f"lane child of run '{rid}' may not save '{p.name}' onto the shared "
                     "shelf — lanes write run-local graph copies only "
                     "(est-2ek.1.599; shelf waves 26-31; r4 realpath-only containment)",
            "error_class": "lane_shelf_write"}

def _lib_read(name):
    """READ resolver mirroring find_run: resolved first, legacy only for an EXISTING
    graph absent from the resolved root. Returns the path whether or not it exists
    (callers keep their own None/not-exists handling on the primary shape)."""
    p = _lib_path(name)
    if not p.exists():
        n = str(name or "").strip().lower()
        if LIB_OK.match(n):
            legacy = _common.launch_runs_root() / "library" / f"{n}.json"
            if legacy != p and legacy.exists():
                return legacy
    return p

def _lib_rel_name(p):
    """#50: the replayable name for a library path — `general/<stem>` inside the
    curated public subdir, plain `<stem>` everywhere else (what `_lib_path` and
    `library` both answer to)."""
    return (str(p.relative_to(library_root()))[:-5] if p.parent == library_root() / _GENERAL_PREFIX
            else p.stem)
def _library_reader():
    """The include resolver's library reader: a closure over _lib_read — the SAME
    resolver `from=<name>` uses (resolved root first, legacy launch root second).
    Returns the parsed graph dict, or None when the entry does not exist / does not
    parse (the resolver turns None into its named `unknown library entry` refusal)."""
    def read(name):
        try:
            p = _lib_read(name)
        except ValueError:
            return None          # invalid library name -> resolver's named refusal
        return jload(p) if p.exists() else None
    return read

def _include_error_from_valueerror(e):
    """A resolver ValueError rides the existing door error envelope (the
    _validation_error shape, __init__.py's errors[{node,field,msg}] + error head).
    The alias is extracted from the resolver's `include '<alias>': ...` wording so
    the row names `include:<alias>` as its node; a bare message still lands in one
    named row rather than an untyped crash."""
    msg = str(e)
    node = "include:"
    if msg.startswith("include '") and "':" in msg[9:]:
        node = "include:" + msg[9:msg.index("':")]
    return {"error": f"graph invalid: node {node}: {msg}",
            "errors": [{"node": node, "field": "include", "msg": msg}]}

def _unbound_include_refs(graph, provenance):
    """Fail-closed check for composite runs (live composite-run receipt, 2026-09-30): after include
    seeds and run_context binding, NO {run.KEY} may survive inside an included
    subtree. The check is alias-scoped — the subtree's refs come from shelved
    bytes the parent author never sees — while parent-authored nodes keep the
    plain-graph behavior (seed-less literal spawns are their documented
    leniency). Returns an error envelope naming alias, node, and key.

    PR#84 review F-3: the text surface is the SHARED _include_text_fields
    traversal (goal/context/question/profile, fan-out goal + item goals, AND an
    echo node's string `output`), not a private field list here. The private
    list omitted echo output — the one surface the runner commits VERBATIM — so
    an included echo holding `verdict={run.MISSING}` survived a closed seeds
    map and landed as literal placeholder text in a DONE run, consumable
    downstream as a verdict. One traversal means run/amend/binding/scratch-notes
    can never drift apart again."""
    aliases = [f"{p['alias']}__" for p in provenance]
    for node in graph["nodes"]:
        nid = node.get("id", "")
        if not any(nid.startswith(a) for a in aliases):
            continue
        for text in _common._include_texts(node):
            m = _RUN_REF.search(text)
            if m:
                return {"error": (
                    f"include seed contract unbound: node {nid!r} still references "
                    f"{{run.{m.group(1)}}} after include seeds and run_context binding — "
                    "supply it in the include `seeds` map or the run_context; "
                    "refused before any write or spawn")}
    return None


def _unbound_gate_argv_refs(graph):
    """est-2ek.1.792: a gate's wait.until_argv is exec'd as FIXED argv. run_context
    renders {run.KEY} in goal/context/question/profile and fan-out goals only, so a
    {run.KEY} surviving in an argv element reaches the machine gate as a literal
    token (FileNotFoundError on '{run.python}', forever, no probe ever run). Refuse
    it before any write or spawn, naming node, argv index and key. Applies to
    parent-authored and included gates alike: an included gate's seeds were already
    rendered by expansion, so anything left here is unbound."""
    for node in graph.get("nodes") or []:
        wait = node.get("wait") if isinstance(node, dict) else None
        argv = wait.get("until_argv") if isinstance(wait, dict) else None
        if not isinstance(argv, list):
            continue
        for i, el in enumerate(argv):
            m = _RUN_REF.search(el) if isinstance(el, str) else None
            if m:
                return {"error": (
                    f"run placeholder unbound: node {node.get('id')!r} wait.until_argv[{i}] "
                    f"still references {{run.{m.group(1)}}} — run_context renders goals, "
                    "contexts, questions and profile only, never command argv; write the "
                    "value into the argv element literally (or hand it to the probe through "
                    "a file); refused before any write or spawn")}
    return None


def _expand_includes_at_door(graph):
    """The single door choke point for composite graphs (design: expand BEFORE
    _validation_error, apply_graph_defaults and _bind_run_context, on run/amend/save).
    Returns (graph_out, notes, provenance, bad_response):
      - a graph WITHOUT an `include` key passes through byte-identical with empty
        notes/provenance and no response (no-op: an amended already-expanded run
        amends the expanded form, which carries no include key);
      - a resolver ValueError -> (None, [], None, envelope) — caller returns it
        verbatim before any write/spawn;
      - success -> (expanded include-STRIPPED graph, notes[], provenance[], None).
    Provenance is computed from the AUTHOR form (strip-on-expand means the expanded
    graph honestly has nothing to report), and a provenance-pass failure is the
    same named-envelope refusal as an expansion failure. Expansion and provenance
    are TWO passes over the shelf — a memoized reader makes them one: every library
    name is read ONCE per call, so a shelf rewritten between the two passes can
    never stamp a source_digest that differs from the bytes actually expanded
    (digest and graph must come from ONE view of the library, or the provenance
    lie says so in every later audit)."""
    if not isinstance(graph, dict) or "include" not in graph:
        return graph, [], [], None
    _memo = {}
    base_reader = _library_reader()
    def reader(name):
        # dict-valued memo: the key's presence marks the read; None is a cached
        # absence (the resolver's unknown-entry refusal), not a cache miss.
        if name not in _memo:
            data = base_reader(name)
            # #50: a shelved file is bare graph or the {meta, graph} envelope —
            # unwrap to the graph here, the same normalizer `from=<name>` uses, so
            # the resolver's full standalone validation measures THE GRAPH, not
            # the envelope wrapper (PR#84 review F-2 made the shelf check strict
            # enough to refuse an envelope's unknown `meta`/`graph` keys).
            if isinstance(data, dict) and isinstance(data.get("graph"), dict) \
                    and not isinstance(data.get("nodes"), list):
                data = data["graph"]
            _memo[name] = data
        return _memo[name]
    try:
        expanded, notes = _common.expand_includes(graph, reader)
        provenance = _common.include_provenance(graph, reader)
    except ValueError as e:
        return None, [], None, _include_error_from_valueerror(e)
    return expanded, notes, provenance, None

# est-2ek.1.245 — stale-literal detector for library saves.
# A reusable graph replays its committed text verbatim on every `run from=<name>`.
# When that text hard-codes launch-varying values (ledger keys, branch names,
# per-lane scratch paths) AND the graph carries ZERO {run.KEY} binding points,
# the entry silently replays the launch that froze it — 1.0.13's run_context
# exists but nothing made authors add binding points. Warn-and-surface at the
# save (never block): the author hears "this will go stale" at the exact moment
# they shelve it, and can either add {run.KEY} bindings or accept the freeze.
_STALE_LITERAL_PATTERNS = (
    # (label, pattern) — mirrors dag_lint s11's operative-surface law:
    # provenance/meta cold bytes are exempt; operative goal/context text counts.
    ("ledger key", re.compile(r"\bfb[0-9a-f]{8,}\b")),
    ("estate id", re.compile(r"\best-[a-z0-9]{3,}(?:\.\d+)?\b")),
    ("branch name", re.compile(r"\b(?:fix|feat|chore|est2ek1|wofs|docs)/[A-Za-z0-9._/-]{3,}\b")),
    ("lane/scratch path", re.compile(r"/home/[\w.-]+/\.hermes(?:/[\w.~+-]+)+")),
    ("dated literal", re.compile(r"\b2026-\d{2}-\d{2}\b")),
)
_RUN_REF_RE = re.compile(r"\{run\.[A-Za-z_][A-Za-z0-9_]*\}")

def _operative_texts(obj, path=""):
    """Yield (path, text) for every string in the OPERATIVE subtree (goal,
    context, question, argv-style lists) — never provenance/meta/description."""
    if isinstance(obj, str):
        yield path, obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("provenance", "meta", "description", "source", "saved_at",
                     "source_digest", "owner", "name"):
                continue
            yield from _operative_texts(v, f"{path}.{k}" if path else str(k))
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            yield from _operative_texts(v, f"{path}[{i}]")

def _stale_literal_warnings(graph):
    """[] when the graph binds or is literal-free; else one warning string per
    offending literal, each naming where it lives and the run_context fix."""
    if _RUN_REF_RE.search(json.dumps(graph.get("nodes") or [], ensure_ascii=False)):
        return []   # author uses binding points — replay-safe by design
    hits, seen = [], set()
    for path, text in _operative_texts({"nodes": graph.get("nodes") or []}):
        for label, pat in _STALE_LITERAL_PATTERNS:
            for lit in pat.findall(text):
                key = (label, lit)
                if key in seen:
                    continue
                seen.add(key)
                hits.append(
                    f"stale_literal ({label}): {lit!r} is hard-coded in {path} with no "
                    f"{{run.KEY}} binding — `run from=` will replay this launch's value; "
                    f"pass it via run_context and write it as {{run.KEY}} in the goal, "
                    f"or accept the freeze")
    return hits


def act_save(args):
    """Shelve a graph under a name: from an existing run (`run_id`) or an inline `graph`.
    Overwrites — a library entry is the CURRENT best version of that graph.
    #50: `description` (<=200) and `tags` (1-10 flat or #70 `facet:value` tokens) ride
    in the entry's `meta` envelope; a save carrying NEITHER keeps writing the pre-#50
    BARE bytes. #70: omitting a field carries the previous envelope's value forward."""
    graph, bad = _input_graph(args, run_id=True)
    if bad:
        return bad
    if graph is None and args.get("run_id"):
        graph = jload(run_dir(args["run_id"]) / "graph.json")
        # A composite run's graph.json is the EXPANDED, include-stripped truth —
        # shelving it would freeze one expansion of the shelf and break the
        # author-form contract (a shelved composite must re-expand at each run so
        # shelf fixes propagate). The author form lives in amends history at best,
        # not in a shape save can read back losslessly: refuse and redirect.
        _run_meta = jload(run_dir(args["run_id"]) / "run.json", {}) or {}
        if _run_meta.get("includes"):
            return {"error": f"run {args['run_id']} was a composite (include-expanded); "
                             "its graph.json is the expanded form and shelving it would "
                             "freeze the shelf. Save the AUTHOR graph instead: "
                             "save(graph=...) or save(graph_path=...) with the graph's "
                             "`include` annotation intact"}
    if graph is None:
        return {"error": "save needs graph, graph_path or run_id of an existing run"}
    # Composite graphs: a shelved composite keeps the AUTHOR form (include key on
    # disk) so the shelf tracks shelf updates — resolution stays at run time. The
    # resolver still runs ONCE here, purely to validate guards (unknown use,
    # cycles, collisions, standalone validity) before anything is written. A
    # graph without an include key passes the choke point through byte-identical.
    _expanded, _include_notes, _includes, bad = _expand_includes_at_door(graph)
    if bad:
        return bad
    # Node-level truth is the EXPANDED graph (a composite author form's `alias__id`
    # refs exist only post-expansion; the resolver itself has already guarded the
    # include block's structure). The WRITE below stays the AUTHOR form.
    bad = _validation_error(_expanded)
    if bad:
        return bad
    try:
        p = _lib_path(args.get("name") or graph.get("name"))
    except ValueError as e:
        return {"error": str(e)}
    blocked = _lane_shelf_guard(p)   # est-2ek.1.599: lanes write run-local only
    if blocked:
        return blocked
    source = args.get("source")
    if source is not None and (not isinstance(source, str) or not source.strip() or len(source) > 200):
        return {"error": "source must be a non-empty string of at most 200 characters"}
    desc = args.get("description")
    if desc is not None and (not isinstance(desc, str) or not desc.strip() or len(desc) > DESC_MAX):
        return {"error": f"description must be a non-empty string of at most {DESC_MAX} characters"}
    tags = args.get("tags")
    if tags is not None:
        norm, terr = _norm_tags(tags)
        if terr:
            return {"error": terr}
        tags = norm
    params = args.get("params")
    if "params" in args and (not isinstance(params, dict) or any(
            not isinstance(k, str) or not _RUN_KEY.fullmatch(k) or not isinstance(v, dict)
            for k, v in params.items())):
        return {"error": "params must map identifier keys to metadata objects"}
    if params is None:
        prev_path = _lib_read(p.stem) if p.parent == library_root() else p
        if prev_path.exists():
            prev_meta = _common.library_entry(jload(prev_path)).get("meta") or {}
            if isinstance(prev_meta.get("params"), dict):
                params = prev_meta["params"]
    # #70 RETAIN-ON-OVERWRITE: a resave that says nothing about a meta field carries
    # the previous entry's value (re-shelve-from-run_id is the documented normal flow
    # and must not silently drop discovery coverage). `tags:[]` stays the #50 error
    # (fail-closed: an agent's sloppy empty array never erases coverage; removing a
    # tag is a deliberate edit of the entry file).
    if (desc is None or tags is None):
        prev_path = _lib_read(p.stem) if p.parent == library_root() else p
        if prev_path.exists():
            # #70 peer review: the retain read must not be unguarded — jload
            # returns None on corrupt bytes, which fell through to "no previous
            # envelope" and SILENTLY WIPED a tagged entry's tags on re-shelve.
            # Fail closed: refuse until the file is repaired or deleted.
            prev_raw = jload(prev_path)
            if prev_raw is None:
                return {"error": f"cannot retain from unreadable entry '{p.stem}' "
                                 "(corrupt JSON): repair or delete the file, then "
                                 "resave with explicit tags/description"}
            prev = _common.library_entry(prev_raw)
            # #146 item 1 (est-yzoy): the WIPE class was the whole invalid IMAGE,
            # not just unparseable bytes. A valid-JSON-but-invalid-shape prev
            # (ambiguous envelope+bare, neither-shape, empty nodes[]) returned
            # {invalid: ...} here, meta fell back to {}, the branch concluded
            # "no previous envelope", and the save SUCCEEDED — collapsing the
            # entry to bare bytes and dropping tags/description exactly when the
            # stored image is damaged. Same law as the corrupt read: refuse until
            # repaired or deleted; an EXPLICIT save naming tags+description stays
            # the documented repair route (this branch never runs for it).
            if "invalid" in prev:
                return {"error": f"cannot retain from unreadable entry '{p.stem}' "
                                 f"({prev['invalid']}): repair or delete the file, "
                                 "then resave with explicit tags/description"}
            pm = prev.get("meta") or {}
            # description: library_entry merged meta-over-graph, so this also picks
            # up a BARE entry's top-level description when the entry is re-shelved
            # into an envelope. Only when the incoming graph doesn't state its own —
            # the fresh graph wins over stale carried bytes.
            if (desc is None and isinstance(prev.get("description"), str)
                    and prev["description"].strip() and "description" not in graph):
                desc = prev["description"]
            # #146 item 3: the retain KEYS ON PRESENCE, not truthiness. Truthiness
            # made a deliberate file-edit erase (`meta.tags: []`) indistinguishable
            # from never-tagged (envelope collapsed on next resave), while a truthy
            # garbage value (a non-list) was retained VERBATIM into the new envelope.
            # An absent key retains nothing (pre-envelope entry); a stored [] keeps
            # the erase deliberate and the envelope intact; a non-list stored tags
            # is an invalid shape — fail closed, same law as the corrupt-prev read.
            if tags is None and "tags" in pm:
                stored = pm["tags"]
                if not isinstance(stored, list):
                    return {"error": f"cannot retain from entry '{p.stem}': "
                                     "meta.tags is not a list "
                                     f"({type(stored).__name__}): repair or delete "
                                     "the file, then resave with explicit tags"}
                # stored [] is the deliberate-erase state: retained as [], envelope
                # stays. A non-empty stored list re-normalizes under the save law
                # (a hand-edited element like ["x", 5] fails closed, never rides on).
                if stored:
                    norm, terr = _norm_tags(stored)
                    if terr:
                        return {"error": f"cannot retain from entry '{p.stem}': "
                                         f"{terr}: repair or delete the file, then "
                                         "resave with explicit tags"}
                    tags = norm
                else:
                    tags = []
    p.parent.mkdir(parents=True, exist_ok=True)
    graph = dict(graph, name=p.stem)
    owner = _common.launcher_profile()
    if source is not None or owner != "default":
        graph["provenance"] = {"owner": owner, "source": source,
                               "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                               "source_digest": _common.source_digest(graph)}
    if desc is not None or tags is not None or params is not None:
        # #50 envelope: meta carries discovery fields the wf/1 grammar has no key
        # for; the inner graph stays validator-clean. The normalizer loads either.
        meta = {}
        if desc is not None:
            meta["description"] = desc
        if tags is not None:
            meta["tags"] = tags
        if params is not None:
            meta["params"] = params
        data = {"meta": meta, "graph": graph}
    else:
        # bare form (pre-#50 bytes, and what the solo golden freezes): the
        # description rides top-level like 1.1 always wrote it — there is none here.
        data = graph
    # Atomic replace (the release path's own idiom): a torn write loses an entry outright.
    tmp = p.with_name(f"{p.stem}.json.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    os.replace(tmp, p)
    saved = _lib_rel_name(p)   # #50: a general/<name> save must echo the replayable name
    out = {"saved": saved, "nodes": len(graph["nodes"]),
           "hint": f"re-run any time: workflow run from={saved}  |  /wf {saved}"}
    # est-2ek.1.245: warn-and-surface on a shelved graph that hard-codes
    # launch-varying literals while carrying ZERO {run.KEY} binding points —
    # such entries silently replay the launch that froze them. NON-FATAL by
    # law: the save lands; only a warning key rides the response, and a clean
    # save's response stays byte-identical (no empty key — golden bytes).
    _warn = _stale_literal_warnings(graph)
    # est-kg3y s12: only an ENVELOPE entry declares a contract (meta is where
    # params live), so only it can disagree with its refs. A bare entry's
    # contract is wholly derived from refs — a bound bare save stays warning-free
    # (est-2ek.1.245 T2b: binding points are the cure, never a new warning).
    if data is not graph:
        contract = _common.run_context_contract(_expanded, params)
        if contract["missing"]:
            _warn.append("s12 uncontracted run_context refs: " + ", ".join(contract["missing"]))
        if contract["dead"]:
            _warn.append("s12 dead declared params: " + ", ".join(contract["dead"]))
    if _warn:
        out["save_warnings"] = _warn
    return out

def _library_rows():
    """#50: THE discovery read for the library — rich rows + honest skips. One walk
    shared by the `library` action and the run from=<unknown> fuzzy nudge, so the
    two can never disagree about what the library holds. Shapes: the 1.1 BARE graph
    (R10: still loads and lists verbatim) or the #50 ENVELOPE {meta, graph}; a file
    neither can be (bad JSON, unexpected shape) is QUARANTINED — F-2 (#62) law:
    fail-closed on the entry (named in `skipped`, with a typed refusal reason in
    `quarantined`, never replayable), fail-open on the library as a whole (the walk,
    the rows, and the nudge never crash). `general/` entries carry the prefix in
    their name/id (#51's public set). F1 #14: a graph under the legacy launch root
    stays listed/replayable."""
    rows, skipped, quarantined, seen = [], [], [], set()
    for root in _library_roots():
        if not root.exists():
            continue
        files = ([(_GENERAL_PREFIX + p.stem, p) for p in sorted((root / _GENERAL_PREFIX).glob("*.json"))]
                 if (root / _GENERAL_PREFIX).is_dir() else [])
        files += [(p.stem, p) for p in sorted(root.glob("*.json"))]
        for name, p in files:
            if name in seen:
                continue
            seen.add(name)
            rel = str(p.relative_to(root))
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                data = None
            entry = _common.library_entry(data)
            if "graph" not in entry:
                skipped.append(rel)
                quarantined.append({"id": rel, "name": name,
                                    "reason": entry.get("invalid", "invalid: unreadable")})
                continue
            graph = entry["graph"]
            nodes = graph["nodes"]
            row = {"name": name,
                   "nodes": len(nodes),
                   "gates": sum(1 for n in nodes if n.get("type") == "gate"),
                   "fanouts": sum(1 for n in nodes if n.get("fanout")),
                   "description": entry["description"]}
            # 149c437 emit-only-when-derivable law: a BARE (pre-#50) entry keeps the
            # exact 1.1 row key-set — solo golden stays byte-identical. The #50
            # discovery keys land on ENVELOPE entries (and general/ paths), which is
            # exactly where they were saved from.
            if entry["envelope"] or name.startswith(_GENERAL_PREFIX):
                row["id"] = str(p.relative_to(root))
                row["tags"] = entry["tags"]
            contract = _common.run_context_contract(graph, entry["meta"].get("params"))
            if contract["refs"]:
                row["params"] = contract["params"]
            if contract["dead"]:
                row["dead_params"] = contract["dead"]
            prov = graph.get("provenance")
            if isinstance(prov, dict):
                row.update({k: prov.get(k) for k in ("owner", "source", "source_digest")})
            rows.append(row)
    return rows, skipped, quarantined

def act_library(args):
    rows, skipped, quarantined = _library_rows()
    if args.get("name") is not None:
        name = args["name"]
        try:
            name = _lib_rel_name(_lib_path(name))
        except ValueError as e:
            return {"error": str(e)}
        row = next((r for r in rows if r["name"] == name), None)
        if row is None:
            return _from_unknown_error(name)
        launch = {"action": "run", "from": name}
        # est-kg3y: an UNFILLED required input prints "" (the door refuses an empty
        # value by name). A "<KEY>" placeholder is a non-empty string the door would
        # accept and launch as a literal — the .572 phantom class — so it is never printed.
        unfilled = sorted(k for k, v in (row.get("params") or {}).items() if "default" not in v)
        if row.get("params"):
            launch["run_context"] = {k: v.get("default", "") for k, v in row["params"].items()}
        return {"entry": dict(row, instantiate=launch, unfilled=unfilled,
                              command="workflow " + json.dumps(launch, ensure_ascii=False))}
    # Preserve the bare-library hint; populated recipes remain optional reuse.
    hinted = "workflow run from=<name> replays one; workflow save graph=... shelves a new one"
    if any(("id" in r) for r in rows):
        hinted += "; one-off graphs can run directly without saving or submitting"
    # #70: tag_vocab is the WHOLE-library {tag: count} — it rides every response once
    # the library carries tags (golden-bytes law: nothing to teach, nothing echoed).
    # The anti-drift loop: the agent sees the live taxonomy on the list call it
    # already makes and reuses it; the hint says so at the point of save.
    vocab = {}
    for r in rows:
        for t in r.get("tags") or []:
            vocab[t] = vocab.get(t, 0) + 1
    want = None
    if args.get("tags") is not None:
        # #70 peer review: ONE tags grammar across save and library —
        # `tags:[]` is the #50 error on save, so the same arg on a query errors
        # too rather than silently meaning match-all (an agent that learned the
        # save law reads an empty array as meaningful). Omit tags for no filter.
        want, terr = _norm_tags(args["tags"])
        if terr:
            return {"error": terr}
        # ALL-match (no OR grammar at this scale — two calls to union); untagged
        # entries never match a filtered query: they are simply not tagged yet.
        rows = [r for r in rows if all(t in (r.get("tags") or []) for t in want)]
        hinted += "; filter with tags:[...] ALL-match; reuse tag_vocab values verbatim, never coin unseen tags"
    out = {"library": rows, "hint": hinted}
    if vocab:
        out["tag_vocab"] = vocab
    if want:
        # est-bvg0: the self-diagnosis is promised by the FILTER, not earned by the
        # vocab — an explicit tags query on an EMPTY or fully-tagless shelf must
        # still name each starved term (all at 0 = the shelf itself starves every
        # term), or the agent cannot tell a spelling miss from an empty library
        # (SKILL.md / grammar.md: "an empty result's tag_match_counts says which
        # term starved"). The golden-bytes law is untouched: an UNFILTERED tagless
        # response still grows neither key.
        counts = {t: vocab.get(t, 0) for t in want}
        out["tag_match_counts"] = counts
        # #146 item 2 (est-yzoy): the counts let a reader INFER the cause;
        # an empty result must NAME it — and the cause is promised by the
        # FILTER, not earned by the vocabulary (zap r4): an empty vocab set —
        # empty, tagless/bare, erased-to-[], or quarantined-only shelf — still
        # owes it. spelling_miss lists the queried terms present nowhere in the
        # library (fixable by re-spelling); with no zero term, the kill is
        # CO-OCCURRENCE — every term exists and none ever appear together — and
        # the hint points at the rarest term to drop. Golden-bytes law: the key
        # lands on EMPTY filtered results only; a non-empty response and every
        # unfiltered call keep their exact pre-#146 key-set.
        if not rows:
            misses = [t for t in want if not counts[t]]
            cause = {"spelling_miss": misses, "co_occurrence_starved": not misses}
            out["empty_cause"] = cause
            if misses:
                hinted += ("; empty result — killer term(s) at 0 library-wide: "
                           + ", ".join(misses) + " (spelling miss — re-check against tag_vocab)")
            else:
                rarest = min(want, key=lambda t: counts[t])
                hinted += ("; empty result — every term exists but they never "
                           "co-occur; drop the rarest term (" + rarest
                           + ") or union with two calls")
            out["hint"] = hinted   # out was built pre-diagnosis; refresh it
    if skipped:
        out["skipped"] = skipped
    if quarantined:   # F-2 (#62): every refused entry is named WITH its typed reason;
        out["quarantined"] = quarantined   # a clean library never grows this key (golden bytes)
    return out

def act_validate(args):
    """Dry-run the door's validation pipeline WITHOUT liveness ping or any write:
    the same defaults fill, defect collection, and model/route policy run/amend do,
    in the same order, returning {ok, errors:[{node,field,msg}], resolved_routes}.
    #85: given run_id, the #85 artifact-admission guard additionally measures the
    declared artifact bytes against that run dir (the same law amend applies)."""
    graph, bad = _input_graph(args)
    if bad:
        return bad
    if graph is None:
        return {"error": "validate needs graph or graph_path"}
    _rd = None
    if args.get("run_id"):
        _rd = run_dir(args["run_id"])
        if not (_rd / "graph.json").exists():
            return {"error": "unknown run_id"}
    # est-kg3y s12 (dag_lint parity): a library ENVELOPE is {meta, graph}. Validate the
    # graph body and surface meta.params disagreement as warn rows — never fatal, same
    # wording the save door uses. A bare graph has no declaration to disagree with.
    warnings = []
    if isinstance(graph, dict) and isinstance(graph.get("graph"), dict) and "nodes" not in graph:
        entry = _common.library_entry(graph)
        if "graph" in entry:
            contract = _common.run_context_contract(entry["graph"], (entry.get("meta") or {}).get("params"))
            if contract["missing"]:
                warnings.append("s12 uncontracted run_context refs: " + ", ".join(contract["missing"]))
            if contract["dead"]:
                warnings.append("s12 dead declared params: " + ", ".join(contract["dead"]))
            graph = entry["graph"]
    bad = _validation_error(graph, run_dir=_rd)
    if bad:
        return dict(bad, ok=False)
    try:
        graph = _common.apply_graph_defaults(graph)
    except ValueError as e:
        return {"ok": False, "error": f"graph invalid: defaults/shape: {e}",
                "errors": json.loads(str(e))}
    bad = _profile_error(graph) or _model_policy_error(graph)
    if bad:
        return dict(bad, ok=False)
    err, _models, routes = _resolve_models(graph["nodes"])
    if err:
        return {"ok": False, "error": err,
                "errors": [{"node": None, "field": "model", "msg": err}]}
    out = {"ok": True, "errors": [], "resolved_routes": routes,
           "hint": "validated only — nothing pinged or written; run it: workflow run graph=..."}
    if warnings:
        out["warnings"] = warnings
    return out

# ---------- actions ----------

def _session_env(name):
    """Use the tool worker's task-local session, not another turn's process env."""
    try:
        from gateway.session_context import get_session_env
    except ImportError:  # standalone plugin host without Hermes gateway
        return os.environ.get(name, "")
    return get_session_env(name, "")

def _card(rid):
    return f'::workflow{{id="{rid}"}}'

# #157: ONE card grammar — the enforcement module ships exactly what the tool
# result carries, injected here rather than duplicated there.
_card_enforcement.bind(_common, _card)


def _lifecycle_notice(rid):
    """Optional presentation, never an execution or reply prerequisite."""
    return f'{_card(rid)} — optional desktop card; plain text and run_id also work'

_RUN_REF = re.compile(r"\{run\.([^{}]*)\}")
_RUN_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _bind_run_context(graph, binding, *, library_sourced=False):
    """Resolve a launch binding on a post-defaults copy, before persistence.

    Map substitution is deliberately narrow: no str.format, no interpolation of
    substituted values, and no changes to fan-out's {item}/{index} grammar.
    """
    if isinstance(binding, str):
        if not binding.strip():
            raise ValueError("run_context seed must be a non-empty string")
        # Door-transport guard: a JSON object handed over as a STRING is a caller
        # that meant the map form (tool transports routinely stringify objects).
        # Seeding it would silently skip substitution — refuse, like every other
        # malformed binding, before any run write.
        try:
            _decoded = json.loads(binding)
        except ValueError:
            _decoded = None
        if isinstance(_decoded, dict):
            raise ValueError("run_context is a JSON-encoded map passed as a string: pass the map itself "
                             "(a string is a seed and cannot replace {run.KEY} literals)")
        def _dangling(node):
            texts = [node.get(f) for f in ("goal", "context", "question", "profile")]
            fo = node.get("fanout")
            if isinstance(fo, dict):
                texts.append(fo.get("goal"))
                texts += [i.get("goal") for i in fo.get("items", []) if isinstance(i, dict)]
            for text in texts:
                if isinstance(text, str):
                    m = _RUN_REF.search(text)
                    if m:
                        return f"node {node['id']!r} reference {{run.{m.group(1)}}}"
            return None
        for node in graph["nodes"]:
            bad = _dangling(node)
            if bad:
                raise ValueError(f"run_context seed cannot bind {bad}: a seed only appends to "
                                 "context — pass a map so the reference is substituted")
        byid = {n["id"]: n for n in graph["nodes"]}
        def agent_ancestor(nid, seen):
            for parent_id in byid[nid].get("after", []):
                if parent_id in seen:
                    continue
                parent = byid[parent_id]
                if parent.get("type", "agent") == "agent" or agent_ancestor(parent_id, seen | {parent_id}):
                    return True
            return False
        nodes = []
        roots = 0
        for n in graph["nodes"]:
            n = dict(n)
            if n.get("type", "agent") == "agent" and not agent_ancestor(n["id"], {n["id"]}):
                n["context"] = (n.get("context") + "\n\n" if n.get("context") else "") + binding
                roots += 1
            nodes.append(n)
        if not roots:
            raise ValueError("run_context seed requires at least one first-wave agent")
        return dict(graph, nodes=nodes)
    if not isinstance(binding, dict) or not binding:
        raise ValueError("run_context must be a non-empty string or non-empty map of string bindings")
    for key, value in binding.items():
        if not isinstance(key, str) or not _RUN_KEY.fullmatch(key):
            raise ValueError(f"run_context invalid key {key!r}: expected identifier")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"run_context[{key!r}] must be a non-empty string")
    def render(text, fanout=False):
        used = set()
        def replace(match):
            key = match.group(1)
            if not _RUN_KEY.fullmatch(key):
                raise ValueError(f"run_context malformed reference {match.group(0)!r}")
            if key not in binding:
                raise ValueError(f"run_context missing key {key!r} for {match.group(0)}")
            used.add(key)
            return binding[key]
        out = _RUN_REF.sub(replace, text)
        if fanout:
            # The runner re-renders fan-out goals per item (wf.fmt_goal): a bound
            # value containing braces would be interpolated a SECOND time with
            # item fields, contradicting the no-interpolation-of-substituted-
            # values contract. Reject before any run write, like every other
            # malformed binding.
            for key in used:
                if "{" in binding[key] or "}" in binding[key]:
                    raise ValueError(
                        f"run_context[{key!r}] must not contain braces when bound into a "
                        "fan-out goal — the runner re-interpolates fan-out goals per item")
        return out
    if library_sourced:
        nodes = []
        for original in graph["nodes"]:
            n = original
            fan = isinstance(n.get("fanout"), dict)
            for label, path in _common._include_text_fields(original):
                n = _common._include_set(n, path, render(
                    _common._include_get(original, path),
                    fanout=label.startswith("fanout.") or (label == "goal" and fan)))
            nodes.append(n)
        return dict(graph, nodes=nodes)
    nodes = []
    for original in graph["nodes"]:
        n = dict(original)
        fan = n.get("fanout") if isinstance(n.get("fanout"), dict) else None
        # 1.1 (RATIFY F2): `profile` is rendered here, BEFORE profile validation, so a
        # graph can name its teammate per launch ({"profile": "{run.reviewer}"}).
        for field in ("goal", "context", "question", "profile"):
            if isinstance(n.get(field), str):
                # issue #7: a fan-out node's OWN goal is the runner's fallback
                # template (wf.py: fo.goal or node.goal -> fmt_goal) whenever
                # items resolve without their own goals — and with items_from
                # the door cannot know that at launch. Treat it as fan-out
                # territory: braces-bearing bound values reject like any
                # other fan-out binding instead of re-interpolating per child.
                n[field] = render(n[field], fanout=(field == "goal" and fan is not None))
        if fan is not None:
            fanout = dict(n["fanout"])
            if isinstance(fanout.get("goal"), str):
                fanout["goal"] = render(fanout["goal"], fanout=True)
            if isinstance(fanout.get("items"), list):
                fanout["items"] = [dict(item, goal=render(item["goal"], fanout=True))
                                   if isinstance(item, dict) and isinstance(item.get("goal"), str)
                                   else item for item in fanout["items"]]
            n["fanout"] = fanout
        nodes.append(n)
    return dict(graph, nodes=nodes)


def _profile_error(graph):
    """1.1 (RATIFY F2): node `profile:` validation — AFTER `{run.KEY}` rendering, BEFORE any
    write/spawn. Launcher identity comes from the door's own HERMES_HOME, else the owner's
    `settings.profile` (#41, env-blind gateways) — never a graph arg.
    Same error shape as _validation_error."""
    errs = _common.profile_errors(graph["nodes"], launcher=_common.launcher_profile(),
                                  profiles_dir=_common.profiles_root())
    if not errs:
        return None
    first = errs[0]
    return {"error": f"graph invalid: node {first['node']}: {first['msg']}", "errors": errs}

_TEAM_ARG_CAPS = {"team": 64, "lane_key": 128}

def _team_args_error(args):
    """1.1 (RATIFY F1/F3): `team` (<=64) and `lane_key` (<=128) are optional non-empty strings."""
    for key, cap in _TEAM_ARG_CAPS.items():
        if key in args and args[key] is not None:
            v = args[key]
            if not isinstance(v, str) or not v.strip() or len(v) > cap:
                return {"error": f"{key} must be a non-empty string of at most {cap} characters"}
    return None

def _identity_stamps(args, graph, lib_name=None):
    """1.1 (RATIFY F1): run.json identity keys, emitted ONLY when derivable — a no-team run
    under the default profile without WF_RUNS_ROOT adds NOTHING (1.0.15 key set).
      dispatched_by  launcher profile name (door HERMES_HOME under <root>/profiles/, else the
                     owner's settings.profile — #41); omitted for default
      launch_root    the runs root the run was created under; recorded when it is not the
                     launcher's own default (WF_RUNS_ROOT or settings.runs_root set — #42)
                     or the launcher is a named profile
      team/lane_key  run args, verbatim
      targets[]      distinct node `profile` names (sorted)
      graph_source   {name, owner, source, source_digest} when a library graph carries provenance
    """
    out = {}
    launcher = _common.launcher_profile()
    if launcher != "default":
        out["dispatched_by"] = launcher
    if launcher != "default" or os.environ.get("WF_RUNS_ROOT") \
            or _common.settings_runs_root() is not None:
        out["launch_root"] = str(runs_root())
    for key in ("team", "lane_key"):
        if args.get(key):
            out[key] = args[key]
    targets = sorted({n["profile"] for n in graph.get("nodes", [])
                      if isinstance(n, dict) and isinstance(n.get("profile"), str) and n["profile"]})
    if targets:
        out["targets"] = targets
    prov = graph.get("provenance")
    if lib_name and isinstance(prov, dict):
        out["graph_source"] = {"name": lib_name, "owner": prov.get("owner"), "source": prov.get("source"),
                               "source_digest": prov.get("source_digest")}
    return out

def _lane_paths(key):
    """WRITE side: the resolved root (new entries land with their runs)."""
    stem = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    root = runs_root() / "lanes"
    return root / f"{stem}.json", root / f"{stem}.lock"

def _lane_entry(key):
    # F1 #14: a lane entry saved under the launch root before the profile-home fix
    # stays visible from the scoped door, so an in-flight incumbent is still deduped
    # (one-time upgrade hazard otherwise: same lane_key admits a second runner while
    # the old one lives). Writes always go to the resolved root via _lane_paths.
    stem = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    entry = jload(_lane_paths(key)[0])
    if entry is None:
        legacy = _common.launch_runs_root() / "lanes" / f"{stem}.json"
        if legacy != _lane_paths(key)[0]:
            entry = jload(legacy)
    if entry is not None and entry.get("lane_key") != key:
        return None, {"error": "lane_key hash collision"}
    return entry, None

def _last_event_ts(r):
    try:
        with (r / "events.jsonl").open(encoding="utf-8") as stream:
            last = None
            for line in stream:
                try:
                    last = json.loads(line).get("ts") or last
                except (ValueError, TypeError):
                    continue
            return last
    except OSError:
        return None

def _lane_state(key, entry):
    rid = entry.get("run_id")
    if not rid:
        return {"lane_key": key, "run_id": None, "unfinished": False}
    try:
        r = run_dir(rid)
        st = run_state(r)
    except ValueError:
        st = None
        r = runs_root() / "__invalid_lane_run__"
    state = st["status"] if st else "pending"
    # est-2ek.1.199: a lane entry CLAIMING a run whose dir does not exist on
    # disk is the postmortem's census-blind ghost — 'pending forever' made the
    # ledger loop sleep 6h. Surface it as an explicit orphaned-run status the
    # monitor/STALL line can read at plugin level. The dir check is what
    # distinguishes it from a not-yet-written-but-launching dir: at this point
    # the door has already passed (or refused) its durability proof, so a
    # claimed run without a dir is gone, not pending.
    if st is None and not r.is_dir():
        state = "orphaned"
    live = st.get("runner_live", False) if st else False   # A2 one-read law
    unfinished = state not in ("done", "failed", "stopped")
    return {"lane_key": key, "run_id": rid, "state": state,
            "runner_live": live, "unfinished": unfinished,
            "needs_resume": unfinished and not live, "last_event_ts": _last_event_ts(r)}

def _lane_key_error(key):
    if not isinstance(key, str) or not key.strip() or len(key) > 128:
        return {"error": "lane_key must be a non-empty string of at most 128 characters"}
    return None

WHY_MIN = 80  # #50: the why-not-library receipt floor (charcount)

def _submit_dir():
    return runs_root() / "inbox"

def _slug(name):
    return "".join(c for c in str(name or "").lower() if c.isalnum() or c in "-_")[:24] or "graph"

def act_submit(args):
    """#50 (epic #49): submit a hand-rolled graph for STUDY — the quarantine inbox
    under runs_root()/inbox, never the library. The why-not-library receipt is
    REQUIRED (>=WHY_MIN chars): the nudge that makes 'why didn't the library cover
    it?' part of the cost of hand-rolling. The graph is validated by the SAME
    `_validation_error` act_run uses — no second validator. A second submit with
    the identical graph digest from the same submitted_by is a DEDUPE HINT, not a
    bounce: the existing id is reported. Submit validates the PLAIN form: it does
    not run the include expansion choke point, so a composite whose parent wires
    `alias__id` refs is refused as an unknown id, while a ref-free composite is
    stored in author form as-is (the study inbox stays a plain-graph lane; the
    quartermaster reads it against the shelf). Nothing here auto-joins the library —
    promotion/decision is the quartermaster's human-gated loop; door stores only.
    Consent shape mirrors save: this is a model-reachable write to runs_root; the
    launcher-consent bounce belongs to run, where profile gates are enforced."""
    graph, bad = _input_graph(args)
    if bad:
        return bad
    if graph is None:
        return {"error": "submit needs graph or graph_path"}
    why = args.get("why_not_library")
    if not isinstance(why, str) or not why.strip():
        return {"error": "submit requires why_not_library: what the library lacks "
                         f"(>= {WHY_MIN} chars) — run `workflow library` first"}
    if len(why.strip()) < WHY_MIN:
        return {"error": f"why_not_library too short: {len(why.strip())} chars, "
                         f"needs >= {WHY_MIN} — name the library entries you checked "
                         "and what shape you needed"}
    lane = args.get("lane")
    if lane is not None and (not isinstance(lane, str) or not lane.strip() or len(lane) > 128):
        return {"error": "lane must be a non-empty string of at most 128 characters"}
    bad = _validation_error(graph)
    if bad:
        return bad
    digest = _common.source_digest(graph)
    submitted_by = _common.launcher_profile()
    d = _submit_dir()
    d.mkdir(parents=True, exist_ok=True)
    # Dedupe BEFORE any write: same node-set digest + same submitter = the same
    # study item restated. Report the existing id; never a second copy, never a bounce.
    for existing in sorted(d.glob("*.json"), reverse=True):
        e = jload(existing)
        if not isinstance(e, dict):
            continue
        if e.get("submitted_by") == submitted_by \
                and _common.source_digest(e.get("graph")) == digest:
            return {"deduped": True, "existing": existing.stem,
                    "hint": f"already in the study inbox: workflow inbox (id {existing.stem})"}
    ts = datetime.now(timezone.utc)
    rid = ts.strftime("%Y%m%d-%H%M%S") + "-" + _slug(graph.get("name"))
    n, path = 0, None
    while True:  # collision-resistant exclusive create (same law as _create_run)
        cand = d / f"{rid if n == 0 else rid + '-' + str(n)}.json"
        try:
            fd = os.open(cand, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            path = cand
            break
        except FileExistsError:
            n += 1
    entry = {"submitted_at": ts.isoformat(timespec="seconds"),
             "submitted_by": submitted_by,
             "why_not_library": why,
             "lane": lane,
             "graph": graph}
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)
    return {"ok": True, "submitted": path.stem, "digest": digest,
            "hint": "quarantined for study (quartermaster's loop decides promotion); "
                    "this submit did NOT join the library"}

def act_inbox(args):
    """Two inbox halves, one action name, never in conflict (a child's steer env and a
    maintainer's query never coexist in one invocation):

    CHILD-SIDE (B1, feedback #13/#40, unchanged): with baked steer env, the child pulls
    its late steering lines — see _steer_lines; a stranger without the env simply has
    nothing to pull.

    MAINTAINER-SIDE (#50, read-only): list `workflow submit` study items newest-first
    for the quartermaster / maintainer's human-gated consume loop — explicit with
    `kind:"submissions"`, or as the fallback for any caller that is not a live child
    pull. LIST ONLY: nothing here promotes, deletes, or joins the library — door
    stores, a human decides. (A door inherited by a descendant process that never
    had its steer file written is not a live pull — no file, no child branch.)"""
    kind = args.get("kind")
    f, c = os.environ.get("HERMES_WF_STEER_FILE"), os.environ.get("HERMES_WF_STEER_CURSOR")
    if kind is None and f and c and Path(f).is_file() and Path(c).is_file():
        try:
            hwm = int(os.environ.get("HERMES_WF_STEER_HWM", "0"))
        except ValueError:
            hwm = 0
        texts, n = _steer_lines(f, c, hwm)
        if n:  # #17: the pull is a fact — log it beside the cursor advance
            try:
                # 1.1 (RATIFY F1): the runner bakes HERMES_WF_RUN_DIR (absolute); prefer it over
                # deriving the run dir from the steer file path (same dir in every legacy spawn).
                run_dir_env = os.environ.get("HERMES_WF_RUN_DIR", "")
                _steer_event(Path(run_dir_env) if run_dir_env else Path(f).parent.parent, "steer.consumed",
                             node=os.environ.get("HERMES_WF_STEER_NODE"),
                             spawn=os.environ.get("HERMES_WF_STEER_SPAWN"), pulled=n)
            except OSError:
                pass
        return {"ok": True, "steering": texts, "pulled": n,
                "node": os.environ.get("HERMES_WF_STEER_NODE"),
                "spawn": os.environ.get("HERMES_WF_STEER_SPAWN")}
    if kind not in (None, "submissions"):
        return {"ok": False, "error": f"unknown inbox kind {kind!r} (omit, or 'submissions')"}
    d = _submit_dir()
    rows = []
    if d.is_dir():
        for p in sorted(d.glob("*.json")):
            e = jload(p)
            if not isinstance(e, dict) or "graph" not in e:
                continue  # a half-written/foreign file lists as nothing, never a crash
            graph = e.get("graph") if isinstance(e.get("graph"), dict) else {}
            rows.append({"id": p.stem,
                         "submitted_at": e.get("submitted_at"),
                         "submitted_by": e.get("submitted_by"),
                         "lane": e.get("lane"),
                         "why_not_library": e.get("why_not_library"),
                         "nodes": len(graph.get("nodes") or [])})
    rows.sort(key=lambda r: (str(r.get("submitted_at") or ""), str(r.get("id") or "")),
              reverse=True)  # newest-first; ids break same-second ties
    if kind is None and not rows:
        return {"ok": False,
                "error": "not a workflow child spawn (no baked steer env) and the study "
                         "inbox is empty: workflow submit quarantines hand-rolled graphs here"}
    return {"ok": True, "inbox": rows, "total": len(rows),
            "hint": "list only — promotion to the library is the quartermaster's "
                    "human-gated loop; consume off-box, door never auto-joins"}

def _from_unknown_error(name):
    """Name the missing recipe and optional alternatives without curation duties.
    Closest names come from the same library walk used by explicit discovery."""
    rows, _skipped, _quarantined = _library_rows()
    names = [r["name"] for r in rows]
    closest = difflib.get_close_matches(str(name or ""), names, n=3)
    err = (f"no library graph named {name!r}"
           + (f"; closest: {', '.join(closest)}" if closest else
              ("; the library is empty" if not names else "")))
    out = {"error": err, "error_class": "library_not_found", "closest": closest,
           "hint": "workflow library lists recipes; run with graph or graph_path launches a one-off without saving"}
    return out

def act_run(args):
    graph, bad = _input_graph(args, library=True)
    if bad:
        return bad
    lib_name = None
    if graph is None and args.get("from"):
        try:
            raw = jload(_lib_read(args["from"]))   # F1 #14: legacy-root graphs stay replayable
        except ValueError as e:
            return {"error": str(e)}
        # #50: a library file is either the 1.1 BARE graph (passes through verbatim,
        # R10) or the {meta, graph} ENVELOPE — the normalizer unwraps it. A missing,
        # unreadable, or shapeless file lands in the nudge below (#50: the nudge IS
        # the error text), so a typo never reads as a silently broken entry. F-2
        # (#62): an UNKNOWN shape is quarantined — it lands in the same nudge, and
        # the walk underneath never raises, so a broken sibling file cannot take a
        # good replay or a typo hint down with it.
        entry = _common.library_entry(raw)
        graph = entry.get("graph")
        if graph is None:
            return _from_unknown_error(args["from"])
        lib_name = _lib_rel_name(_lib_path(args["from"]))   # #50: general/ names keep their prefix
    if graph is None:
        return {"error": "run needs graph, graph_path or from=<library name>"}
    # Composite graphs: expand `include` BEFORE every other gate so validation,
    # defaults baking and {run.KEY} binding all see the flat namespaced node list
    # (design: the runner/read model/desktop never learn includes exist).
    graph, _include_notes, _includes, bad = _expand_includes_at_door(graph)
    if bad:
        return bad
    assert graph is not None   # bad None <=> expansion succeeded (same law as `assert models`)
    bad = _validation_error(graph) or _team_args_error(args)
    if bad:
        return bad
    # est-2ek.1.166: the version handshake AT ARM TIME — a graph that declares
    # requires_plugin above this plugin's own version (plugin.yaml, the single
    # source) is REFUSED here, naming both versions, before any write or spawn
    # (spool e6e55416cd78c9bd: a stale seat must read exactly what to upgrade,
    # never a 03:00 mystery death on a grammar the door never had). Absent key
    # = no check: a plain run stays byte-identical (solo golden law).
    if graph.get("requires_plugin") is not None:
        _vh = _common.version_handshake_error(graph.get("requires_plugin"),
                                              _common.plugin_version())
        if _vh:
            return {"error": _vh}
    concurrency_meta, bad = _concurrency_bake(graph)
    if bad:
        return bad
    name = args.get("name", graph.get("name", "workflow"))
    if not isinstance(name, str) or not name.strip():
        return {"error": "run name must be a non-empty string"}
    graph = dict(graph, name=name)
    # Bake `defaults` + shape presets into the node defs BEFORE graph.json: the
    # runner and every fingerprint then see ONE resolved truth (#8/#11).
    try:
        graph = _common.apply_graph_defaults(graph)
    except ValueError as e:
        return {"error": f"graph invalid: defaults/shape: {e}"}
    if lib_name:
        # Post-defaults: `defaults` is already baked into nodes, so the render
        # surface the runner consumes is nodes[] alone (the retained `defaults`
        # block is never rendered and must not read as a survivor).
        refs = _common.run_context_contract({"nodes": graph["nodes"]})["refs"]
        binding = args.get("run_context")
        if refs and not isinstance(binding, dict):
            return {"error": "library run_context must bind non-empty string values for: "
                             + ", ".join(refs) + "; refused before any write or spawn"}
        # est-kg3y: the exact placeholder the instantiate surface used to print is a
        # non-empty string, so the value check alone would launch it literally. Refuse
        # it for every ref'd key, even when pasted from an older printout.
        if refs and isinstance(binding, dict):
            stale = [k for k in refs
                     if isinstance(binding.get(k), str) and binding[k].strip() == f"<{k}>"]
            if stale:
                return {"error": "library run_context value is the instantiate placeholder "
                                 "<KEY> for: " + ", ".join(stale) + "; supply the real value "
                                 "— refused before any write or spawn"}
    if "run_context" in args:
        try:
            graph = _bind_run_context(graph, args["run_context"], library_sourced=bool(lib_name))
        except ValueError as e:
            return {"error": str(e)}
    if lib_name:
        refs = _common.run_context_contract({"nodes": graph["nodes"]})["refs"]
        if refs:
            return {"error": "library run_context has surviving {run.KEY} references: "
                             + ", ".join(refs) + "; refused before any write or spawn"}
    if _includes:
        # Composite runs are fail-closed on unbound refs (live composite-run receipt,
        # 2026-09-30): a shelved sub-graph's {run.KEY} contract is invisible to a
        # parent author, so the plain-graph leniency (literal placeholder spawns)
        # silently seats a placeholder verdict when run_context is absent or a
        # seed value missed a key. Every {run.*} surviving include seeds +
        # run_context binding is a refusal before any write/spawn.
        bad = _unbound_include_refs(graph, _includes)
        if bad:
            return bad
    # est-2ek.1.792: run_context never reaches argv — refuse a surviving {run.KEY}.
    bad = _unbound_gate_argv_refs(graph)
    if bad:
        return bad
    # 1.1 (RATIFY F2): profile validation runs on the RENDERED graph ({run.KEY} resolved).
    bad = _profile_error(graph)
    if bad:
        return bad
    # dad50be0: policy BEFORE resolution — a seat-forbidden model that isn't a valid
    # route must be reported as forbidden (field=model), not swallowed by the
    # unknown-model rejection inside _resolve_models.
    bad = _model_policy_error(graph)
    if bad:
        return bad
    err, models, routes = _resolve_models(graph["nodes"])
    if err:
        return {"error": err}
    assert models is not None and routes is not None
    # FEEDBACK #152be7f7: warn-and-surface liveness ping (annotates `routes` in place;
    # never blocks — every failure path yields liveness='unknown' and the launch).
    _liveness_notes = _route_liveness_ping(routes)
    # #24 (c): a model the seat KNOWS is subscription-exhausted refuses the launch
    # (advisory cache; one recovery ping first, never pings on an offline seat).
    _q = _quota_refusal(graph, routes)
    if _q:
        return {"error": _q}
    # #25: fail-closed pinned routes — an affirmatively dead or
    # fallback-ladder-surprised pinned route refuses the launch unless the node
    # opted into the ladder (require_route: false); alive-proved nodes bake
    # route_verified so the runner holds served_model to it at commit.
    _r = _route_enforcement(graph, routes, models=models)
    if _r:
        return {"error": _r, "routes": routes}
    # run dry_run — the amend dry_run shape on the launch path: every gate above
    # (validate, bind, profile, policy, model resolve, quota, route enforcement) has
    # run; return their verdict WITHOUT touching the lane registry, creating a run
    # dir, or spawning a runner.
    if args.get("dry_run"):
        out = {"ok": True, "dry_run": True, "models": models, "routes": routes,
               "hint": "nothing written — re-run without dry_run to launch"
                       + _liveness_hint_suffix(_liveness_notes)}
        if _include_notes:
            out["include_notes"] = _include_notes
        return out
    key = args.get("lane_key")
    if key is not None:
        path, lock_path = _lane_paths(key)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            entry, collision = _lane_entry(key)
            if collision:
                return collision
            if entry:
                incumbent = _lane_state(key, entry)
                if incumbent["unfinished"]:
                    return {"deduped": True, "run_id": incumbent["run_id"],
                            "state": incumbent["state"], "runner_live": incumbent["runner_live"],
                            "needs_resume": incumbent["needs_resume"],
                            "last_event_ts": incumbent["last_event_ts"],
                            # Keep the incumbent's optional card presentation.
                            "lifecycle_notice": _lifecycle_notice(incumbent["run_id"]),
                            "hint": f"wait run_id={incumbent['run_id']} resumes it"}
            return _create_run(args, graph, lib_name, models, routes, _liveness_notes, path,
                               includes=_includes, include_notes=_include_notes,
                               concurrency_meta=concurrency_meta)
    return _create_run(args, graph, lib_name, models, routes, _liveness_notes,
                       includes=_includes, include_notes=_include_notes,
                       concurrency_meta=concurrency_meta)

def _new_ancestor_dirs(p):
    """est-2ek.1.199: the directory entries a `p.mkdir(parents=True)` would
    create, deepest first — so the durability proof can fsync the ENTRY that
    names each newly created dir, not just the leaf. Best-effort by design: a
    race here only ever means an extra fsync on a dir we did not create.
    """
    out, cur = [], Path(p)
    while True:
        try:
            if cur.exists():
                return out
        except OSError:
            return out
        out.append(cur)
        if cur.parent == cur:
            return out
        cur = cur.parent

# est-2ek.1.199: the three files a fresh run dir MUST carry before its run_id
# may ride back to the caller. The proof treats absence as failure, never skip.
_DURABLE_RUN_FILES = ("graph.json", "run.json", "wake_protocol")

def _prove_run_dir(r, extra_dirs=()):
    """est-2ek.1.199: the door's durability proof for a fresh run dir, run
    BEFORE the runner spawns and BEFORE the run_id rides back to the caller.

    fsync the three mandatory durable files (graph.json, run.json,
    wake_protocol — a missing one is an EXPLICIT failure: a half-written run
    dir must never be handed back as launched), then fsync the run dir, the
    runs root, and every ancestor entry the launch itself created via
    extra_dirs (link durability — a fsynced file in an un-fsynced dir can
    vanish with the dir on crash; `mkdir(parents=True)` can create the root
    AND its ancestors, so each newly created entry needs its own sync). Then
    VERIFY the dir still exists under the resolved durable root — the phantom
    shape is a run_id whose dir is not under the root the estate census walks.
    Any failure raises RuntimeError naming the ATTEMPTED PATH (loud, per the
    postmortem ticket: never a phantom run_id). handle() turns the raise into
    the tool's error payload; act_run callers inside the plugin never see a
    half-proved run advertised as launched.
    """
    attempted = str(r)
    root = r.parent
    try:
        for fname in _DURABLE_RUN_FILES:
            f = r / fname
            if not f.is_file():
                raise RuntimeError(
                    f"run dir proof FAILED: mandatory durable file {fname} is "
                    f"missing at attempted path {f} — run not launched-proof; "
                    "do not resume by id")
            with open(f, "rb") as fh:
                os.fsync(fh.fileno())
        for d in (r, root, *extra_dirs):
            dfd = os.open(d, getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
    except OSError as e:
        raise RuntimeError(
            f"run dir proof FAILED (fsync) at attempted path {attempted}: "
            f"{type(e).__name__}: {e} — run not launched-proof; do not resume by id")
    if not r.is_dir():
        raise RuntimeError(
            f"run dir proof FAILED: run dir vanished after launch — attempted path "
            f"{attempted} is not a directory under the durable runs root {root}; "
            "no run was durably created (check the launch path's root resolution, "
            "esp. a stub/non-registering door carrying settings.runs_root)")


def _create_run(args, graph, lib_name, models, routes, _liveness_notes, lane_path=None,
                *, concurrency_meta=None, includes=None, include_notes=None):
    """Under the lane flock: complete run dir, atomic registry entry, then spawn."""
    name = graph["name"]
    base = time.strftime("%Y%m%d-%H%M%S") + "-" + "".join(
        c for c in name.lower() if c.isalnum() or c in "-_")[:24]
    rid, n = base, 0
    root = runs_root()
    # est-2ek.1.199: snapshot which ancestor entries ALREADY exist before
    # mkdir(parents=True) gets a chance to mint them — the durability proof
    # must fsync the entry naming each newly created dir (blocker 3).
    _new_dirs = _new_ancestor_dirs(root)
    root.mkdir(parents=True, exist_ok=True)
    while True:  # collision-resistant exclusive create
        r = root / (rid if n == 0 else f"{rid}-{n}")
        try:
            (r / "nodes").mkdir(parents=True, exist_ok=False)
            rid = r.name
            break
        except FileExistsError:
            n += 1
    (r / "gates").mkdir(exist_ok=True)
    (r / "graph.json").write_text(json.dumps(graph, ensure_ascii=False, indent=2))
    # est-2ek.1.641: the door's admission proof is durable BEFORE the runner
    # spawns — every node this submit's ping proved alive gets its proved-alive
    # receipt into the lane (the runner refuses any later spawn that would bill
    # a different model, before submit). Best-effort: a receipt write failure
    # never blocks the launch (the runner-side bake re-covers at first spawn).
    try:
        _common.bake_route_receipts(r, graph)   # lives in the privately-bound
        # wfcommon (outbound review ask #1): baking through a spec-load of
        # wf.py mutated the HOST sys.path and leaked a generic `wfcommon`.
    except Exception:
        pass
    meta = {"name": graph.get("name", "workflow"), "hermes_bin": _hermes_bin(),
            "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "fp_rule_version": _common.FP_RULE_VERSION,
            # OWNER = the agent session that spawned the run. The desktop uses it to
            # send a visible SDK composer turn to THIS chat, never the active chat.
            # Absent (tests, CLI) => no owner => UI asks for manual resume.
            "owner": {"session_id": _session_env("HERMES_SESSION_ID") or None,
                      "ui_session_id": _session_env("HERMES_UI_SESSION_ID") or None,
                      "platform": _session_env("HERMES_SESSION_PLATFORM") or None}}
    if concurrency_meta:
        meta.update(concurrency_meta)
    meta.update(_identity_stamps(args, graph, lib_name))   # 1.1: only derivable keys land
    # est-2ek.1.166 → ra-plan#125-1 (issue #125): EVERY run reports the arming
    # door's own truth (plugin.yaml via wfcommon.plugin_version), not only runs
    # that declared requires_plugin — a stale seat is visible from status for
    # plain runs too, without opening a run dir. The stamp stays DERIVED, never
    # invented (R2): an unreadable plugin.yaml ('') keeps the key absent, and
    # act_status' derive-only echo therefore omits it exactly as before. The
    # golden-solo comparison exempts this volatile provenance stamp (same
    # sanctioned-delta precedent as `started`/`fp_rule_version` — est-ciin).
    _pv = _common.plugin_version()
    if _pv:
        meta["plugin_version"] = _pv
    # Composite runs record which shelf bytes they expanded from (author-form
    # provenance) and any non-fatal resolver notes (scratch collisions). Empty =
    # key omitted: a plain run keeps the exact pre-include run.json key set.
    if includes:
        meta["includes"] = includes
    if include_notes:
        meta["include_notes"] = include_notes
    (r / "run.json").write_text(json.dumps(meta))
    # AUTHORITY LAW (owner ruling, PR#97 review): every owner-facing automatic wake
    # is RUNNER-AUTHORED protocol text; graph-authored prose (gate questions etc.)
    # is attributed data — it lives in events.jsonl / status / the desktop card and
    # NEVER rides a role:user owner turn. This stamp is the durable marker that the
    # run was created under that law (the door writes it at creation; the runner
    # reads it for the probe context and never rewrites it). A run dir made outside
    # the door carries no stamp — status shows it, and a replaying runner stays
    # compatible (the text law lives in wf.notify itself, not in this file).
    (r / "wake_protocol").write_text("runner-authored/v1\n")
    if lane_path is not None:
        entry = {"lane_key": args["lane_key"], "run_id": rid,
                 "claimed_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        tmp = lane_path.with_name(f"{lane_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            tmp.write_text(json.dumps(entry, ensure_ascii=False))
            os.replace(tmp, lane_path)
        finally:
            tmp.unlink(missing_ok=True)
    # est-2ek.1.199 (fb-fix STALL postmortem): a door launch returned a run_id
    # whose dir never landed under the durable root; the ledger loop slept 6h
    # on 'in_progress' because nothing ever proved the dir to the launcher.
    # DURABILITY PROOF law: every mandatory durable file is fsynced (absence
    # fails, never skips), then the run dir, the runs root, and any
    # newly-created ancestor entries are fsynced (link durability), and the dir
    # must STILL EXIST under the resolved durable root BEFORE the runner spawns
    # and before the run_id is handed back. A failed proof RAISES with the
    # attempted path — handle() surfaces it as the tool error; a phantom run_id
    # is never returned and no work starts behind a refused proof (a caller
    # retry must not duplicate work that already began).
    _prove_run_dir(r, extra_dirs=_new_dirs)
    _spawn_runner(r)
    # Re-check AFTER the spawn: a dir that vanished while/just after the runner
    # started is still the phantom shape (the door's own writes are already
    # proved and synced at this point; only an external removal lands here).
    if not r.is_dir():
        raise RuntimeError(
            f"run dir proof FAILED: run dir vanished after launch — attempted path "
            f"{r} is not a directory under the durable runs root {r.parent}; "
            "no run was durably created (check the launch path's root resolution, "
            "esp. a stub/non-registering door carrying settings.runs_root)")
    out = {"run_id": rid, "models": models, "routes": routes, "hint":
            'Run accepted. Use wait for blocking observation; it may resume interrupted work. '
            'Push delivery and desktop cards are optional.'
            + _liveness_hint_suffix(_liveness_notes),
            "card": _card(rid), "lifecycle_notice": _lifecycle_notice(rid)}
    if include_notes:
        out["include_notes"] = include_notes   # scratch-collision warnings ride the launch
    return out

def _steer_event(r, ev, **kw):
    """#17: steer is only real if it lands in events.jsonl — the 45 real steers
    across 16 runs that were invisible are the bug being fixed."""
    with open(r / "events.jsonl", "a") as f:
        f.write(json.dumps({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            "event": ev, **kw}, ensure_ascii=False) + "\n")

_steer_state = _common._steer_state  # O2 re-export alias (moved to wfcommon): act_status keeps resolving via the door; bound via _common so a foreign `wfcommon` on sys.path can never hijack it

def _output_pointer(rec):
    """Compact stand-in for a committed node output: enough to DECIDE to pay for
    detail=full (shape + size + where the bytes live), never the bytes."""
    out = rec.get("output")
    try:
        size = len(json.dumps(out, ensure_ascii=False))
    except (TypeError, ValueError):
        size = -1
    keys = sorted(out.keys())[:12] if isinstance(out, dict) else (
        [f"items:{len(out)}"] if isinstance(out, list) else [type(out).__name__])
    return {"output_ptr": "nodes (detail=\"full\")", "output_keys": keys, "output_bytes": size}

def _resolve_read_run(rid):
    """#58: ONE run-dir resolution for the READ verbs (status/wait): strict resolved
    path first (find_run), sibling-root scan as fallback. Returns (dir, run_state,
    resolved_via_or_None). run_state is None when no root holds the run."""
    r = run_dir(rid)
    st = run_state(r)
    if st:
        return r, st, None
    sib = _find_run_sibling_scan((rid or "").strip())
    if sib is not None:
        st = run_state(sib)
        if st:
            return sib, st, str(sib)
    return r, None, None

def act_status(args):
    if args.get("lane_key") is not None:
        key = args["lane_key"]
        bad = _lane_key_error(key)
        if bad:
            return bad
        if args.get("run_id"):
            return {"error": "choose lane_key or run_id for status, not both"}
        entry, collision = _lane_entry(key)
        if collision:
            return collision
        if not entry:
            return {"lane_key": key, "run_id": None, "unfinished": False}
        return _lane_state(key, entry)
    r, st, resolved_via = _resolve_read_run(args.get("run_id"))
    if not st:
        return {"error": f"no run at {r}"}
    full = str(args.get("detail") or "").lower() == "full"
    alive = st.get("runner_live", False)   # A2 (91b9a3de one-read law): THE ONE read
    # that run_state derived from — a fresh probe here can flip across a dying
    # runner's flock and make status vs runner_live disagree.
    out = {"run_id": st["run_id"], "name": st["name"], "status": st["status"],
           "failed_nodes": st["failed_nodes"],   # sys-5lnm17: which nodes died, on every status/wait
           "runner_live": alive, "nodes": {k: {kk: v[kk] for kk in ("type", "status", "fanout",
                                                                     # #128: the artifact-mtime heartbeat rides
                                                                     # status/wait/list identically; honest
                                                                     # absence — no key when no artifact visible
                                                                     # #133: the wall meter rides identically —
                                                                     # {elapsed_s, wall_s, p95_s, meter}, absent
                                                                     # when nothing grounds it
                                                                     "progress", "wall", "clocks", "item_counts") if kk in v}
                                           for k, v in st["nodes"].items()},
           "done": st["done"], "skipped": st["skipped"], "total": st["total"],
           # Optional desktop presentation; plain status remains sufficient.
           "card": _card(st["run_id"]),
           # #19: the graph head rides EVERY status read (derived, never stored —
           # status is where a CAS caller reads the token before a guarded write).
           "graph_revision": _graph_revision(r)}
    if "accounting" in st:
        out["accounting"] = st["accounting"]
    if resolved_via:   # #58: the run lives outside the caller's resolved root — loud, not silent
        out["resolved_via"] = resolved_via
        out["resolved_via_note"] = ("run resolved under a sibling root (dispatch-time home "
                                    "differs from this consumer's); write verbs stay refused here")
    # Composite runs surface their resolver notes (scratch-collision warnings) on
    # every status read, read straight from the run.json the door stamped.
    _inotes = jload(r / "run.json", {}) or {}
    if _inotes.get("include_notes"):
        out["include_notes"] = _inotes["include_notes"]
    # est-2ek.1.166: a run armed with requires_plugin reports the arming door's
    # plugin_version back on every status read — a stale seat is visible WITHOUT
    # opening a run dir (derive-only: the bytes are the stamp this run.json
    # already carries; an armed-less run never gains the key).
    if _inotes.get("plugin_version"):
        out["plugin_version"] = _inotes["plugin_version"]
    # Tier self-report (2026-09-24): a failed child's core -Q turn report carried
    # its typed verdict key ("typed") or not ("untyped"); absent = never noted.
    tier_rec = jload(r / "turn_report.tier")
    # est-2ek.1.95: the same evidence also rides as a STRUCTURED stop_reason so a
    # budget death is never opaque error_class=unknown downstream. Derive-only
    # (A3 law): the bytes come from the tier note and the node records already
    # read in this call — no new state, nothing classified from prose. "class" is
    # passthrough ONLY when a failed/partial node record itself says
    # cap_exhausted; honest absence (no key at all) when no tier note exists.
    stop_reason = None
    if isinstance(tier_rec, dict) and tier_rec.get("tier"):
        out["turn_report"] = str(tier_rec["tier"])
        stop_reason = {"turn_tier": str(tier_rec["tier"]),
                       "tier_via_node": str(tier_rec.get("via") or ""),
                       "source": "turn_report.tier"}
        out["stop_reason"] = stop_reason
    # est-bbfy: a cue-injected lane that STILL died at the cap is a BUDGET
    # stop — the marker file the runner wrote at injection time is the
    # deterministic fact (derive-only, A3 law: one stat, no prose). Honest
    # absence: no marker, no kind (a plain cap death stays class-only).
    if stop_reason is not None and (r / "budget_cue").is_dir():
        _via = stop_reason.get("tier_via_node") or ""
        if _via and ((r / "budget_cue" / _via).is_file()
                     or any((r / "budget_cue").glob(f"{_via}.*"))):
            stop_reason["kind"] = "budget"
    if st.get("runner_exit"):
        out["runner_exit"] = st["runner_exit"]  # <run>/runner_exit.json or the dead-pid crash note
        # est-6226: an EXTERNAL signal death (gateway-restart wave) is
        # respawn-eligible, not a lane failure — the reaper/dispatcher reads
        # the classification here instead of re-deriving it from prose
        # (derive-only, A3 law: the verdict bytes + the ONE classifier).
        _rxr = (st["runner_exit"] or {}).get("reason")
        if _common.is_external_kill(_rxr):
            out["runner_exit"] = {**st["runner_exit"],
                                  "death_class": "external_kill",
                                  "respawn_eligible": True}
    if st["held_gate"]:
        out["gate"] = st["held_gate"]
    for nid, v in st["nodes"].items():
        rec = jload(r / "nodes" / f"{nid}.json")
        # est-2ek.1.95: the stop_reason's class rides ONLY off the via node's own
        # record saying cap_exhausted (the one node-truth read already in hand);
        # unknown stays unknown — never invented from prose.
        if (stop_reason is not None and nid == stop_reason.get("tier_via_node")
                and v["status"] in ("failed", "partial") and rec
                and rec.get("error_class") == "cap_exhausted"):
            stop_reason["class"] = "cap_exhausted"
        if v.get("active_spawn"):
            def _trim(sp):
                return sp if full else {k: x for k, x in sp.items()
                                        if k not in ("argv", "spawn_cmd")}
            out["nodes"][nid]["active_spawn"] = _trim(v["active_spawn"])
            out["nodes"][nid]["active_spawns"] = [_trim(sp) for sp in v.get("active_spawns") or []]
        if v.get("metrics"):
            out["nodes"][nid]["metrics"] = v["metrics"]
        if v.get("blocked_by"):
            out["nodes"][nid]["blocked_by"] = v["blocked_by"]
        if v.get("parked"):
            out["nodes"][nid]["parked"] = v["parked"]
        if v.get("seat_wait"):   # est-2ek.1.833: global seat back-pressure, derive-only
            out["nodes"][nid]["seat_wait"] = v["seat_wait"]
        sm = _steer_state(r, nid)
        if sm:
            out["nodes"][nid]["steer"] = sm
        # mid-run repeat waits were re-shipping every committed output each tick
        # (feedback #76: 91% of a real wait payload). Terminal payload always full;
        # running payload compact unless detail=full.
        show_out = full or st["status"] not in ("running", "pending")
        if rec and v["status"] in ("done", "partial"):   # #4: partial output ships like done (harvest carries the death cause)
            out["nodes"][nid]["output"] = rec.get("output") if show_out else _output_pointer(rec)
            out["nodes"][nid]["ms"] = rec.get("ms")
            if v["status"] == "partial":
                out["nodes"][nid]["error"] = rec.get("error")
                out["nodes"][nid]["harvest"] = rec.get("harvest")
                # est-2ek.1.165: the harvest's DECLARED unfinished tail rides
                # verbatim — structured steps the scheduler/read model ride,
                # never prose (passthrough through the ONE node-truth read;
                # honest absence: a record without the key gains no key here).
                if "remaining" in rec:
                    out["nodes"][nid]["remaining"] = rec.get("remaining")
        elif rec and v["status"] == "failed":
            out["nodes"][nid]["error"] = rec.get("error")
            out["nodes"][nid]["output"] = rec.get("output") if show_out else _output_pointer(rec)
        # A2: a failed/partial node also ships the child's last words and the
        # failure facts — pure passthrough through the ONE node-truth read (O2),
        # so the parent never tails logs to learn why a child died.
        if rec and v["status"] in ("failed", "partial"):
            nf = _common.node_facts(r, nid)
            if nf:
                out["nodes"][nid]["node_facts"] = {
                    k: nf[k] for k in ("error_class", "attempts", "attempts_log",
                                       "final", "log_path", "prompt_path")
                    if k in nf} | ({"stale_because": nf["stale_because"]}
                                   if "stale_because" in nf else {})
        # why-rerun: a pending node WITH a committed record is efp-stale (an amend
        # invalidated it) — say so beside the fingerprint, read model only.
        if rec and v["status"] == "pending":
            sb = _common.explain_stale(r, nid)
            if sb:
                out["nodes"][nid]["stale_because"] = sb
    # Cumulative spend is independent of heartbeat. Only a verified spawn's exact
    # session title can supply current activity; historical unended rows are not live.
    try:
        cm = _common.run_child_metrics(r)
    except Exception:
        cm = {}
    if cm or any(v.get("active_spawn") for v in st["nodes"].values()):
        import time as _t
        # #114 (WF-04): the numeric fold + known/unknown policy is the ONE helper
        # wfcommon.fold_child_metrics (presentation — tokens string, children,
        # scope, cost_usd, heartbeat splice — stays here). api_calls is absent
        # from a fold whose rows include an unknown child, so the run total sums
        # the folds that produced a number and omits api_calls itself whenever
        # ANY node fold omitted it — never a partial sum advertised as exact.
        tot = {"tokens_in": 0, "tokens_out": 0, "api_calls": 0, "tool_calls": 0, "cost": 0.0}
        api_calls_known = True
        for nid in out["nodes"]:
            mine = {k: m for k, m in cm.items() if k.startswith(f"wf:{st['run_id']}:{nid}:")}
            active = st["nodes"][nid].get("active_spawns", [])
            if not mine and not active:
                continue
            # a live-only node (verified spawn, no DB rows yet) folds as zeros,
            # exactly as the pre-helper sum-over-empty-did: "0▸0", not a crash.
            f = _common.fold_child_metrics(mine.values()) or {k: 0 for k in tot}
            current = _common.current_attempt(mine, active)
            line = {"tokens": f"{f['tokens_in']}▸{f['tokens_out']}"}
            if "api_calls" in f:
                line["api_calls"] = f["api_calls"]
            else:
                api_calls_known = False
            line["tool_calls"] = f["tool_calls"]
            line["children"] = len(mine)
            line["scope"] = "cumulative"
            if f["cost"]:
                line["cost_usd"] = round(f["cost"], 4)
            if current["live"]:
                la = current["last_activity"]
                line["live"] = current["live"]
                line["idle_s"] = int(_t.time() - la) if la is not None else None
                line["last"] = current["last_desc"]
                line["last_tool_at"] = la  # #18: the epoch of the last verified activity
            out["nodes"][nid]["metrics"] = {**out["nodes"][nid].get("metrics", {}), **line}
            # #127 (est-zsws): the heartbeat verdict rides EVERY running node's
            # status line — derived from the fold just built plus the #128
            # progress artifact age. Read model only: no writes, no key for a
            # node that is not running.
            if st["nodes"][nid].get("status") == "running":
                out["nodes"][nid]["heartbeat"] = _common.heartbeat(
                    line, out["nodes"][nid].get("progress"))
            for k in tot: tot[k] += f.get(k) or 0
        out["metrics"] = {"tokens": f"{tot['tokens_in']}▸{tot['tokens_out']}",
                          **({"api_calls": tot["api_calls"]} if api_calls_known else {}),
                          "tool_calls": tot["tool_calls"], **({"cost_usd": round(tot["cost"], 4)} if tot["cost"] else {})}
    # #127 (est-zsws): one-line run-level explain composed ONLY from fields the
    # payload already carries (statuses, heartbeat verdicts, blocked_by, parked).
    # Terminal runs have nothing to explain — the key is absent, never an slogan.
    _ex = _common.run_explain(out)
    if _ex:
        out["explain"] = _ex
    # est-2ek.1.158: a graph that declares `result: <node id>` names the node whose
    # output IS the run verdict; terminal status/wait report it (derive-only: the
    # node entry built above). Graphs without the key never gain the field.
    _rid = (jload(r / "graph.json", {}) or {}).get("result")
    if isinstance(_rid, str) and _rid in out["nodes"] and st["status"] in ("done", "failed", "stopped"):
        out["result"] = {"node": _rid, "status": out["nodes"][_rid]["status"],
                         "output": out["nodes"][_rid].get("output")}
    # A3: derived "what next?" — computed ONLY from the run_state fields already in
    # hand (derive-only, never a guess; an unreadable state yields no `next` at all).
    # The agent does what `next` says; an empty `next` means the run is terminal.
    s = st["status"]
    if s == "held" and st.get("held_gate"):
        hg = st["held_gate"]
        out["next"] = [{"action": "release", "gate_id": hg.get("id"),
                       "options": hg.get("options") or []}]
    elif s in ("running", "pending", "interrupted"):
        row = {"action": "wait"}
        # est-2ek.1.833: a live run with no progress because the GLOBAL agent-seat
        # cap is full names that reason, the cap and the oldest wait age.
        sw = {nid: v["seat_wait"] for nid, v in st["nodes"].items() if v.get("seat_wait")}
        if sw and st.get("runner_live"):
            ages = [w["age_s"] for w in sw.values() if isinstance(w.get("age_s"), int)]
            row.update({"reason": "seat_wait", "nodes": sorted(sw),
                        "cap": next((w.get("cap") for w in sw.values() if w.get("cap") is not None), None),
                        "age_s": max(ages) if ages else None})
        out["next"] = [row]
    elif s == "failed":
        rows = [{"action": "amend", "node": nid, "error_class":
                 (out["nodes"][nid].get("node_facts")
                  or _common.node_facts(r, nid) or {}).get("error_class", "unknown")}
                for nid, v in st["nodes"].items() if v["status"] == "failed"]
        # failed with no failed node visible (e.g. a recorded crashed runner): amend the run.
        out["next"] = rows or [{"action": "amend", "node": None, "error_class": "unknown"}]
    elif s in ("done", "stopped"):
        out["next"] = []
    return out

def _wait_foreign(args, r, st, resolved_via):
    """#58: wait on a run resolved OUTSIDE the caller's resolved root. Read-only
    watch: follow to a terminal/attention state, never spawn (the runner belongs to
    the owner's root; _spawn_runner from here would stamp the wrong HERMES_HOME and
    the runner_alive root-compare would reject it anyway). Answers carry the same
    resolved_via warning as the sibling status read."""
    note = ("run lives under a sibling root — this wait is read-only: resume/write "
            "verbs (wait-resume, release, amend, stop) must be issued from the owning home")
    cap = min(float(args.get("timeout", 600)), 1800)
    t0 = time.time()
    while st["status"] in ("running", "pending") and st.get("runner_live"):
        if time.time() - t0 > cap:
            return {**act_status(args), "resolved_via": resolved_via,
                    "note": f"still running after {int(time.time()-t0)}s (wait again) — {note}"}
        time.sleep(2)
        _r, st, _via = _resolve_read_run(args.get("run_id"))
        if not st:
            return {"error": "unknown run_id"}
    out = act_status(args)
    if isinstance(out, dict) and "error" not in out:
        out["resolved_via"] = resolved_via
        out["resolved_via_note"] = note
    return out

def act_wait(args):
    """Explicit resume/watch verb. Read-only status/list never spawn; wait may
    resume pending or interrupted work, then block while a verified runner lives."""
    r, st, resolved_via = _resolve_read_run(args.get("run_id"))
    if not st:
        return {"error": "unknown run_id"}
    if resolved_via:   # #58: read convenience only — never spawn a runner from a foreign root
        return _wait_foreign(args, r, st, resolved_via)
    top_alive = None
    if st["status"] in ("running", "pending", "interrupted"):
        top_alive = bool(st.get("runner_live"))   # A2 one-read law: THE ONE read
        if not top_alive:
            _respawn_runner(r)  # only this explicit wait resumes unfinished work
    cap = min(float(args.get("timeout", 600)), 1800)
    # fb-validator-duo (2026-09-26): the clamp stays (harness deadline guard), but a
    # silent cut is a papercut — echo it in the result when it bites.
    raw_timeout = args.get("timeout", 600)
    clamp_note = (f"timeout clamped to 1800s (requested {raw_timeout})"
                  if float(raw_timeout) > 1800 else None)
    def _echo(out):
        if clamp_note and isinstance(out, dict) and "error" not in out:
            out["timeout_note"] = clamp_note
        return out
    # The harness kills any tool call at its own concurrent-batch deadline (default
    # 420s). Yield at 330s with a clean "wait again" so the parent is never left with
    # a client-side "timed out after 420.0s" error mid-sleep (papercut 2026-09-22).
    seg = min(cap, 330)
    t0 = time.time()
    last_spawn, spawn_tries = 0.0, 0
    def _respawn_throttled():
        """91b9a3de companion (R6): the flock probe has a sub-second HELD window
        after a SIGKILL — the kernel releases only once the dying process is torn
        down — so the top-of-call liveness read can legitimately say alive for a
        runner that is already dying. When the loop then sees the work orphaned,
        re-attempt the spawn — but ONLY when the top read said alive (that dying
        shape; test_live_truth pins one spawn per explicit resume otherwise), at
        most 3 tries, and at most ~1/s. The runner's own flock admission makes a
        double-spawn harmless (loser emits WORKFLOW_BUSY and exits — see
        _spawn_runner's law), so the bound only needs to cap a crash-loop."""
        nonlocal last_spawn, spawn_tries
        if spawn_tries >= 3 or time.time() - last_spawn < 1.0:
            return False
        last_spawn, spawn_tries = time.time(), spawn_tries + 1
        _respawn_runner(r)
        return True
    while True:
        st = run_state(r)
        alive = st.get("runner_live", False)   # THE ONE read (91b9a3de companion) —
        # a re-probe microseconds later can flip across a dying runner's flock and
        # leave status/alive disagreeing (the R6 regression shape).
        # Version-skew guard (burned 2026-09-23): a runner spawned fresh from disk may
        # commit statuses this door's read model predates; its own verdict is the truth.
        rx = (st.get("runner_exit") or {}).get("reason")
        if rx == "done" and not alive:
            return _echo(act_status(args))
        if st["status"] not in ("running", "pending") or not alive:
            if st["status"] == "interrupted" and top_alive \
                    and _respawn_throttled():
                time.sleep(0.25)
                continue
            return _echo(act_status(args))
        if time.time() - t0 > cap:
            return _echo({**act_status(args), "note": f"still running after {cap}s (wait again)"})
        if time.time() - t0 > seg:
            return _echo({**act_status(args), "note": f"still running after {int(time.time()-t0)}s — call wait again (self-yield at {int(seg)}s keeps us under the harness tool deadline)"})
        time.sleep(2)

# ---------- graph revision CAS (issue #19) ----------
# The revision is DERIVED, never stored: "seq:sha16" over the run's own bytes
# (seq = amends.jsonl line count, 0 for a fresh run; sha16 = sha256 of the
# current graph.json bytes, first 16 hex). Nothing to migrate (R10: pre-token
# run dirs already carry everything the token is computed from) and a stored
# token could drift from the graph it claims to name — a derived one cannot.
# Only the door's two graph.json writers exist (act_run at create, act_amend),
# so seq is monotone: every graph mutation appends exactly one amends row
# (act_run mints a run with seq 0). All CAS + write happens under the per-run
# door lock: the runner's admission flock is held for the runner's whole
# lifetime on the runner side, so the door cannot take it (deadlock).

def _graph_revision(r):
    """The run's current graph revision "<seq>:<sha16>", or None when the run
    has no graph.json (unknown run / no graph to pin)."""
    try:
        data = (r / "graph.json").read_bytes()
    except OSError:
        return None
    seq = 0
    try:
        with (r / "amends.jsonl").open("r", encoding="utf-8") as f:
            seq = sum(1 for line in f if line.strip())
    except OSError:
        pass
    return f"{seq}:{hashlib.sha256(data).hexdigest()[:16]}"

def _revision_cas(r, args):
    """Compare-and-set on the graph revision. Tokenless (key absent/None) =
    today's last-writer-wins semantics, unchanged (returns None). A submitted
    token that differs from the head returns the typed refusal — the CALLER
    must have checked this under _graph_lock(r) and must write NOTHING on a
    refusal; the head named in the error is the head the caller must re-read
    and rebase against."""
    tok = args.get("graph_revision")
    if tok is None:
        return None
    head = _graph_revision(r)
    if tok == head:
        return None
    return {"error": "graph_revision_stale", "yours": tok, "head": head}

@contextlib.contextmanager
def _graph_lock(r):
    """#19: the per-run door lock held by every graph-revision CAS and every
    graph.json mutation (act_amend's read-modify-write; the CAS-guarded inbox
    append and gate-answer write). Locking only inside the WRITERS — the shared
    jload primitive is untouched (it is a god node; callers stay unchanged)."""
    with (r / "graph.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)

def _release_core(r, gate_id, answer, ui=False):
    """ONE gate-answer path for tool and UI. Stale answers never block: the answer
    file is overwritten iff absent-or-stale; the gate re-holds unless the efp
    matches. An answer NEVER lands on a run that is stopped or being stopped —
    stop→release is serialized (test-locked), re-run/amend instead."""
    graph = jload(r / "graph.json")
    if not graph:
        return {"ok": False, "error": "unknown run"}
    if (r / "stop.request").exists() or run_state(r)["status"] == "stopped":
        return {"ok": False, "error": "run is stopped/stop pending — answer refused; amend or re-run to continue"}
    byid = {n["id"]: n for n in graph["nodes"]}
    gate = byid.get(gate_id)
    if not gate or gate.get("type") != "gate":
        return {"ok": False, "error": "no such gate node in this run"}
    old = jload(r / "gates" / f"{gate_id}.json")
    if old is not None and _common.gate_answer_valid(r, gate, byid) is not None:
        return {"ok": False, "error": "gate already answered (current graph)"}
    # #152 fail-closed compat check (mixed-version door/runner skew): the respawn
    # this release may trigger runs THIS door's bundled wf.py — so this door's
    # validator IS the respawner's admission validator. If it refuses the
    # committed graph, writing the answer would consume it into a doomed
    # 'crashed: graph invalid' death: the answer becomes unrecoverable
    # ('gate already answered') and the lap must be re-walked. Refuse instead,
    # write NOTHING, leave the hold intact for a door that can honor it.
    # One-pass defect reporting (c01b4a0 style); zero cost when it passes.
    _errs = validate_graph_errors(graph["nodes"])  # the respawner validates exactly this
    if _errs:
        _off = "; ".join(f"node {e['node']}: {e['field']} — {e['msg']}" for e in _errs[:5])
        if len(_errs) > 5:
            _off += f"; (+{len(_errs) - 5} more)"
        return {"ok": False,
                "error": f"this door cannot drive this graph — drive it from its own tree ({_off})"}
    rec = {"answer": answer, "_def": efp(byid, gate),
           "fp_rule_version": _common.FP_RULE_VERSION,
           "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if ui: rec["_ui"] = True
    (r / "gates").mkdir(exist_ok=True)
    p = r / "gates" / f"{gate_id}.json"
    tmp = p.with_name(f"{gate_id}.json.{os.getpid()}.tmp"); tmp.write_text(json.dumps(rec, ensure_ascii=False))
    os.replace(tmp, p)
    return {"ok": True}

def act_release(args):
    r = run_dir(args.get("run_id"))
    if not (r / "graph.json").exists():
        return {"error": "unknown run_id"}
    if (r / "stop.request").exists() or run_state(r)["status"] == "stopped":
        # serialize stop→release: an answer must not land on a run that is being
        # (or already was) stopped — amend or re-run instead.
        return {"error": "run is stopped/stop pending — answer refused; amend or re-run to continue"}
    gate_id = args.get("gate_id")
    # #19 (est-19k9w): optional CAS — a gate answer is a write against the
    # CURRENT graph (its efp is computed from the live defs); answering under a
    # revision the caller never saw is refused with NOTHING written — the hold
    # stays exactly as it was. release never bumps the revision: it does not
    # touch graph.json. The CAS + answer write share one door lock; the respawn
    # happens OUTSIDE it (never hold the door lock across a spawn). The UI path
    # (_release_core directly) is tokenless by construction, today's semantics.
    with _graph_lock(r):
        _stale = _revision_cas(r, args)
        if _stale:
            return _stale
        res = _release_core(r, gate_id, args.get("answer", ""))
    if not res.get("ok"):
        return res
    mode = _resume_after_action(
        r, consumed=lambda: (((run_state(r) or {}).get("nodes", {}).get(gate_id) or {})
                              .get("status") in ("done", "skipped")))
    if mode == "respawned":
        res["auto_resumed"] = True
        hint = "runner respawned; push delivery is optional, use wait for blocking observation"
    else:
        hint = "answer recorded; the live runner commits it at its boundary; push delivery is optional, use wait for blocking observation"
    return {**res, "hint": hint}

def act_steer(args):
    r = run_dir(args.get("run_id"))
    graph = jload(r / "graph.json")
    if not graph:
        return {"error": "unknown run_id"}
    byid = {n["id"]: n for n in graph["nodes"]}
    node = byid.get(args.get("node"))
    if not node:
        return {"error": "no such node"}
    st = run_state(r)
    if not st:
        return {"error": "unknown run_id"}
    run_status = st["status"]
    node_status = (st.get("nodes", {}).get(node["id"]) or {}).get("status")
    if kind(node).spawns is not True:
        return {"ok": False, "error": "gate nodes do not accept steering; use release for a held gate"}
    if run_status in ("done", "failed", "stopped"):
        # Feedback #68: a terminal run has NO runner that will ever spawn that node —
        # reporting ok here made steering text vanish silently. An interrupted run
        # is NOT refused: wait/release/amend resume its runner, which then consumes
        # the inbox (the delivery line below says so). Refuse honestly and name the
        # correct verb.
        return {"ok": False,
                "error": f"run is {run_status} — steering not queued: no live runner will spawn that node, "
                         "so the text would never be delivered; amend is the correct verb (or re-run)"}
    if node_status in ("done", "partial", "failed", "skipped"):   # #4: a harvested partial never re-spawns either
        return {"ok": False,
                "error": f"node is {node_status} — steering not queued: a {node_status} node never spawns again, "
                         "so the text would never be delivered; amend is the correct verb to make it eligible again"}
    # #19 (est-19k9w): optional CAS — steering text must not be queued against a
    # graph revision the caller never saw (the node set / defs may have moved).
    # The append happens under the same lock as the check, and a refusal writes
    # NOTHING: no inbox line, no steer.queued event, gate files untouched.
    with _graph_lock(r):
        _stale = _revision_cas(r, args)
        if _stale:
            return _stale
        with open(r / "inbox.jsonl", "a") as f:
            f.write(json.dumps({"node": node["id"], "text": args.get("text", ""),
                                "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}) + "\n")
    _steer_event(r, "steer.queued", node=node["id"],
                 chars=len(args.get("text", "") or ""))  # #17: the door logs the queue
    if node_status == "running":
        # A verified live child pulls at its NEXT inbox call; lines it never
        # pulled are still baked into the node's next spawn. Both facts, no hedging.
        delivery = (f"queued; the running child will pull it at its next inbox call "
                    f"— no delivery guarantee; anything it does not pull is baked "
                    f"into the next spawn of {node['id']}")
    elif run_status == "held":
        delivery = f"queued; baked into the next spawn of {node['id']} when the held gate releases"
    elif runner_alive(r):
        delivery = (f"queued; baked into the next spawn of {node['id']} — a running "
                    f"child's prompt is never rewritten")
    else:
        delivery = (f"queued; baked into the next spawn of {node['id']} when the runner "
                    f"resumes — call wait to resume it")
    return {"ok": True, "delivery": delivery}

def _steer_lines(path, cursor, hwm):
    """Return (texts, n_pulled) for baked steering lines beyond this spawn's
    cursor, capped at the spawn's HWM; advance the cursor file atomically.
    Honesty law: a line counts as delivered ONLY after this function advances
    the cursor — never claim applied without that evidence."""
    try:
        seen = int(Path(cursor).read_text().strip() or "0")
    except (OSError, ValueError):
        seen = 0
    try:
        lines = [l for l in Path(path).read_text(encoding="utf-8").splitlines() if l.strip()]
    except OSError:
        return [], 0
    recs, out = [], []
    for l in lines:
        try:
            r = json.loads(l)
        except Exception:
            continue
        recs.append(r)
    todo = [r for r in recs[seen:] if int(r.get("i", -1)) < hwm]
    if not todo:
        return [], 0
    out = [r.get("text", "") for r in todo]
    new_seen = seen + len(todo)
    tmp = Path(cursor + f".{os.getpid()}.tmp")
    tmp.write_text(str(new_seen), encoding="utf-8")
    os.replace(tmp, cursor)
    return out, len(todo)

def _frozen_committed(r, old, new_nodes):
    """fb 034849a23af94418: ids whose committed bake an amend keeps verbatim — ONLY nodes
    that will replay-skip. All of: (a) a committed def with that id; (b) the def minus
    budgets and route keys is unchanged; (c) the route is untouched (all of model/tier/
    provider equal, or the author wrote the committed tier key with no/same provider);
    (d) the committed record is valid at the OLD efp; (e) every `after` ancestor is
    frozen too (fixpoint). Edited, re-running and pending nodes route on the current seat."""
    ob = {n["id"]: n for n in old.get("nodes", []) if isinstance(n, dict) and n.get("id")}
    drop = set(_common._BUDGET_KEYS) | set(_ROUTE_KEYS) | set(_common._POLICY_KEYS)
    rest = lambda d: {k: v for k, v in d.items() if k not in drop}
    frozen = set()
    for n in new_nodes:
        c = ob.get(n.get("id"))
        if not c or rest(n) != rest(c):
            continue
        same = all(n.get(k) == c.get(k) for k in _ROUTE_MATCH_KEYS) or (
            bool(c.get("tier")) and n.get("model") == c["tier"]
            and n.get("provider") in (None, c.get("provider"))) or (
            # #291 F2: the author literal 'X/Y' replays as its committed bake {X, Y}
            # (est-2ek.1.46) whether or not the seat still routes X.
            not n.get("provider") and not c.get("tier") and bool(c.get("provider"))
            and n.get("model") == f"{c['provider']}/{c.get('model')}")
        if same and _common.node_rec(r, c, ob)[0] in ("done", "partial", "skipped"):
            frozen.add(n["id"])
    moved = True
    while moved:
        moved = False
        for n in new_nodes:
            if n.get("id") in frozen and any(a not in frozen for a in n.get("after", []) or []):
                frozen.discard(n["id"])
                moved = True
    return ob, frozen

def _graph_unified_diff(old, new, max_lines=400):
    """est-2del: the diff text inside the confirm surface — unified diff of the
    pretty-printed graphs (stable key order). Capped; cap is named."""
    import difflib
    def pretty(g):
        return json.dumps(g, indent=2, sort_keys=True, default=str).splitlines()
    d = list(difflib.unified_diff(pretty(old), pretty(new),
                                  fromfile="graph.json", tofile="proposed", n=3, lineterm=""))
    cap = len(d) > max_lines
    return {"lines": d[:max_lines], "truncated": cap,
            "changed_lines": sum(1 for ln in d if ln[:1] in "+-" and ln[:3] not in ("+++", "---"))}


def act_diff(args):
    """est-2del action=diff: the confirm surface for an amend — same door, same
    gates, dry_run FORCED true. Accepts patch or graph; returns the preview
    (added/removed/changed/will_rerun) plus graph_diff; touches nothing."""
    return act_amend(dict(args or {}, dry_run=True))


def act_amend(args):
    r = run_dir(args.get("run_id"))
    if not (r / "graph.json").exists():
        return {"error": "unknown run_id"}
    _patch = args.get("patch")
    if _patch is not None and (args.get("graph") or args.get("graph_path")):
        return {"error": "patch and graph/graph_path are exclusive — patch edits the "
                         "committed graph in place; send one or the other"}
    new, bad = _input_graph(args)
    if bad:
        return bad
    if _patch is not None:
        try:
            new = _common.apply_node_patch(jload(r / "graph.json") or {}, _patch)
        except ValueError as e:
            return {"error": f"patch: {e}"}
    if new is None:
        return {"error": "amend needs full replacement graph, graph_path, or patch"}
    # #19 (est-19k9w): opt-in compare-and-set on the graph revision, checked
    # EARLY so a stale caller is refused before the expensive gates and before
    # anything is written (a dry_run reports staleness too). Tokenless = today's
    # last-writer-wins, unchanged. The write site re-checks under the same lock.
    with _graph_lock(r):
        _stale = _revision_cas(r, args)
    if _stale:
        return _stale
    # An amend of an include-expanded run passes the EXPANDED graph (no `include`
    # key — strip-on-expand), so this is the identity no-op; a replacement graph
    # that DOES carry includes is a fresh author form and expands before validation
    # exactly like run. On-disk graph.json stays the expanded truth either way.
    new, _include_notes, _includes, bad = _expand_includes_at_door(new)
    if bad:
        return bad
    assert new is not None
    if _includes:
        # amend never re-binds run_context, so an author-form include graph under
        # amend is ALWAYS unbound — same fail-closed law as run, scoped to the
        # included subtree.
        bad = _unbound_include_refs(new, _includes)
        if bad:
            return bad
    bad = _unbound_gate_argv_refs(new)    # est-2ek.1.792: same door law as run
    if bad:
        return bad
    bad = _validation_error(new, run_dir=r) or _profile_error(new)
    if bad:
        return bad
    # est-2ek.1.166 (door run/amend, as the suite docstring claims): the SAME
    # arm-time handshake act_run enforces, mirrored here — an amend is a graph
    # submit, and without this a run armed clean could smuggle a future
    # requires_plugin in via amend, trading a typed refusal for the 03:00 boot
    # death the handshake exists to prevent. Refusal lands BEFORE graph.json /
    # amends.jsonl are touched, naming both versions. Absent key = no check.
    if new.get("requires_plugin") is not None:
        _vh = _common.version_handshake_error(new.get("requires_plugin"),
                                              _common.plugin_version())
        if _vh:
            return {"error": _vh}
    old = jload(r / "graph.json") or {}
    new = dict(new, name=new.get("name", old.get("name", "workflow")))
    # Same resolved-truth rule as run: bake defaults/shape before the write, so
    # the preview, the fingerprints, and the runner all see the resolved defs.
    try:
        new = _common.apply_graph_defaults(new)
    except ValueError as e:
        return {"error": f"graph invalid: defaults/shape: {e}"}
    # fb 034849a23af94418: freeze (against the OLD graph, BEFORE resolution) only the
    # nodes that will replay-skip; everything else re-resolves on the current seat.
    _old_byid, _frozen = _frozen_committed(r, old, new["nodes"])
    err, _models, _routes = _resolve_models(new["nodes"], committed=_old_byid, keep=_frozen)
    if err:
        return {"error": err}
    assert _models is not None and _routes is not None
    bad = _model_policy_error(new)
    if bad:
        return bad
    # FEEDBACK #152be7f7: the same warn-and-surface ping at the amend submit (annotates
    # `_routes` in place; never blocks — the amend applies regardless of ping outcome).
    _liveness_notes = _route_liveness_ping(_routes)
    # #24/#25: the same fail-closed gates at the amend submit. Frozen (replay-skip)
    # nodes keep their committed defs and are never re-gated — they already ran.
    _q = _quota_refusal(new, _routes, skip=_frozen)
    if _q:
        return {"error": _q}
    _r = _route_enforcement(new, _routes, skip=_frozen, models=_models)
    if _r:
        return {"error": _r, "routes": _routes}
    # Q3 preview — the same replay-skip law the runner applies (wfcommon.efp +
    # node_rec), computed on the RESOLVED defs (what lands in graph.json), read-only.
    # dry_run returns here WITHOUT touching graph.json, amends.jsonl, or the runner.
    preview = amend_preview(r, new["nodes"])
    if args.get("dry_run"):
        return {"ok": True, "dry_run": True, "models": _models, "routes": _routes, **preview,
                "graph_diff": _graph_unified_diff(old, new),
                "hint": "nothing written — re-run without dry_run to apply"
                        + _liveness_hint_suffix(_liveness_notes)}
    # #19 (est-19k9w): the write site holds the per-run door lock across the
    # whole read-modify-write (the 09-27 alternating-writers shape: two amends
    # each read the old graph, the later replace silently loses the earlier).
    # The CAS token is RE-CHECKED here — between the early refuse and this lock
    # another writer may have moved the head (check+write under one lock is the
    # atomic unit; the early check is only the cheap pre-empt). A refusal lands
    # BEFORE amends.jsonl / graph.json / restart.request / gate files are touched.
    with _graph_lock(r):
        _stale = _revision_cas(r, args)
        if _stale:
            return _stale
        _base_rev = _graph_revision(r)
        (r / "amends.jsonl").open("a").write(
            json.dumps({"at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "old": jload(r / "graph.json"), "new": new,
                        # #19: the row names the head it was written against and
                        # whether the caller pinned it (cas:true = refused-if-stale).
                        "base_revision": _base_rev,
                        "cas": args.get("graph_revision") is not None}) + "\n")
        tmp = r / f"graph.json.{os.getpid()}.tmp"
        tmp.write_text(json.dumps(new, ensure_ascii=False, indent=2))
        os.replace(tmp, r / "graph.json")
        _new_rev = _graph_revision(r)
    # est-2ek.1.641: an applied amend re-proves routes through this submit's
    # ping — bake the proved-alive receipts for the NEW defs (frozen replay-skip
    # nodes keep their committed receipt: the file is merged, never rewritten).
    try:
        _common.bake_route_receipts(r, new)     # private wfcommon, never wf.py
    except Exception:
        pass
    meta = jload(r / "run.json", {}) or {}
    # An author-form amend of a composite run restamps run.json's include truth the
    # same way act_run writes it: the notes/provenance describing THIS graph, not
    # the previous one. SET-OR-REMOVE: fresh author-form provenance replaces the
    # notes even when the new graph produces NONE (PR#84 review F-5: replacing a
    # warning-bearing include with a clean one succeeded while run.json/status
    # kept warning about the old graph's fixed scratch path — a stale note list
    # describes a graph the run no longer carries). An expanded-form amend yields
    # empty lists here, so the keys are left exactly as they stand (the
    # intentional no-op: a plain run never grows them).
    if _includes:
        meta["includes"] = _includes
        if _include_notes:
            meta["include_notes"] = _include_notes
        else:
            meta.pop("include_notes", None)
    if meta.get("name") != new["name"] or _includes or _include_notes:
        meta["name"] = new["name"]
        mtmp = r / f"run.json.{os.getpid()}.tmp"
        mtmp.write_text(json.dumps(meta, ensure_ascii=False))
        os.replace(mtmp, r / "run.json")
    with open(r / "events.jsonl", "a") as f:
        f.write(json.dumps({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            "event": "graph.amended"}) + "\n")
    (r / "restart.request").write_text("1")
    mode = _resume_after_action(r, consumed=lambda: not (r / "restart.request").exists())
    if mode == "respawned":
        applies = "runner respawned; efp replay-skip re-runs changed nodes and everything downstream"
    else:
        applies = "live runner hot-reloads at its next wave boundary; push delivery is optional"
    return {"ok": True, "models": _models, "routes": _routes, "applies": applies,
            "graph_revision": _new_rev,   # #19: the new head — the token a next CAS writes against
            "hint": "continuation is automatic; wait blocks to a boundary and may resume an interrupted runner" + _liveness_hint_suffix(_liveness_notes), **preview}

def act_stop(args):
    r = run_dir(args.get("run_id"))
    if not (r / "graph.json").exists():
        return {"error": "unknown run_id"}
    st = run_state(r)
    if st and st["status"] in ("done", "failed", "stopped"):
        return {"ok": True, "already": st["status"]}
    (r / "stop.request").write_text(datetime.now(timezone.utc).isoformat(timespec="seconds"))
    mode = _resume_after_action(r, consumed=lambda: not (r / "stop.request").exists())
    if mode == "respawned":
        return {"ok": True, "note": "runner spawned to consume the stop marker"}
    return {"ok": True, "note": "stop recorded; the live runner consumes it at its next boundary"}

def act_list(_args):
    roots = [runs_root()]
    legacy = _common.launch_runs_root()
    if legacy != roots[0]:
        roots.append(legacy)   # pre-fix runs under the launch root stay listed
    runs, seen = [], set()
    dispatched_by_set = 0
    # est-2ek.1.762 census hygiene: enumerate through wfcommon.iter_run_dirs —
    # unique by RUN-DIR NAME across every scanned root (symlinked/profile
    # roots multiply hits), never raw per-root iterdir.
    scanned = _common.iter_run_dirs(roots, reverse=True)
    for r in scanned:
        st = run_state(r, telemetry=False)
        if st and st["run_id"] not in seen:
            seen.add(st["run_id"])
            row = {"run_id": st["run_id"], "name": st["name"], "status": st["status"],
                   "gate": (st["held_gate"] or {}).get("id"),
                   "nodes_done": st["done"], "nodes_skipped": st["skipped"], "nodes_total": st["total"],
                   "runner_live": st.get("runner_live", False)}  # A2 one-read law
            meta = jload(r / "run.json", {}) or {}
            # #57 census fold (QM digest, #52 pinned vocab — no renames): one field read
            # on the run.json this loop ALREADY loads (zero extra scans). Absent key or
            # null = pre-identity run: counts toward total only.
            if meta.get("dispatched_by"):
                dispatched_by_set += 1
            for key in ("lane_key", "team"):
                if key in meta:
                    row[key] = meta[key]
            runs.append(row)
    out = {"runs": runs[:50], **_common.run_summary(runs)}
    # est-2ek.1.280: an EMPTY scan is the silent case the multi-profile papercut
    # burned 10h on (profile-scoped tool writes profiles/<p>/workflows while the
    # API globs the canonical root — both answer []). Emit the scanned roots,
    # resolved-first, ONLY when nothing was found; a non-empty payload keeps the
    # golden-solo key set {runs, total, counts(, provenance)} byte-identical.
    if not runs:
        out["roots"] = [str(x) for x in roots]
    # Emitted ONLY when derivable (the F1 identity law): a solo root where no run carries
    # dispatched_by keeps the v1.0.15 key set {runs, total, counts} exactly (golden-solo
    # EMPTY). A root with stamped runs gets provenance:{dispatched_by_set,total}.
    if dispatched_by_set:
        out["provenance"] = {"dispatched_by_set": dispatched_by_set, "total": len(runs)}
    return out

def act_doctor_version(args):
    """est-2ek.1.159 version truth: one read, no network — compare the seat's live
    plugin.yaml version against the install.json provenance pack.py stamped at build
    time. drift:true means "fixed upstream, not deployed here" (the 1.0.1-seat vs
    1.1.0-source shape); a missing install.json says null provenance, never an
    invented version."""
    d = Path(args["plugin_dir"]).expanduser() if args.get("plugin_dir") else HERE
    yaml_p, inst_p = d / "plugin.yaml", d / "install.json"
    if not yaml_p.is_file():
        return {"error": f"no plugin.yaml under {d}"}
    live = None
    m = re.search(r"(?m)^version:\s*(\S+)\s*$", yaml_p.read_text(encoding="utf-8", errors="replace"))
    if m:
        live = m.group(1)
    packaged = commit = None
    if inst_p.is_file():
        try:
            rec = json.loads(inst_p.read_text(encoding="utf-8", errors="replace"))
        except (ValueError, OSError):
            rec = {}
        if isinstance(rec, dict):
            packaged = rec.get("version")
            commit = rec.get("source_commit")
    return {"live_version": live, "newest_packaged": packaged,
            "source_commit": commit,
            "drift": bool(packaged is not None and live is not None and packaged != live)}


ACTIONS = {"run": act_run, "status": act_status, "wait": act_wait, "release": act_release,
           "steer": act_steer, "inbox": act_inbox, "amend": act_amend, "diff": act_diff, "stop": act_stop, "list": act_list,
           "save": act_save, "submit": act_submit, "library": act_library,
           "doctor_version": act_doctor_version, "validate": act_validate}

def handle(args, **kwargs):
    try:
        if "hermes_bin" in args:
            return json.dumps({"error": "hermes_bin is not a workflow tool argument; configure plugins.entries.hermes-workflows.settings.hermes_bin or HERMES_WF_HERMES_BIN"})
        fn = ACTIONS.get(args.get("action"))
        if not fn:
            return json.dumps({"error": f"unknown action {args.get('action')!r}", "actions": sorted(ACTIONS)})
        bad = _owner_settings_error()   # #42: malformed owner settings fail-closed, never fall back
        if bad:
            return json.dumps(bad)
        return json.dumps(fn(args), ensure_ascii=False, default=str)
    except Exception as e:
        import traceback
        return json.dumps({"error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()[-800:]})

def _wf_command(raw_args):
    """`/wf` — the library front door. `/wf <name> [note]` supplies the note
    atomically as a launch seed, never as post-launch steering."""
    arg = (raw_args or "").strip()
    if arg.startswith("show "):
        detail = act_library({"name": arg[5:].strip()})
        if "error" in detail:
            return detail["error"]
        entry = detail["entry"]
        lines = [f"Workflow **{entry['name']}** — required inputs: "
                 + (", ".join(entry.get("params", {})) or "(none)")]
        for k, spec in (entry.get("params") or {}).items():
            line = f"  - {k}"
            if spec.get("desc"):
                line += f": {spec['desc']}"
            if "default" in spec:
                line += f" (suggested default: {spec['default']})"
            else:
                line += " — fill this (empty until supplied)"
            lines.append(line)
        if entry.get("unfilled"):
            lines.append("Fill every empty run_context value, then run "
                         "(the door refuses an empty value by name):")
        lines.append(entry["command"])
        return "\n".join(lines)
    out = act_library({})
    lib = out["library"]
    quarantined = out.get("quarantined") or []
    if not arg or arg in ("list", "ls"):
        if not lib and not quarantined:
            return ("Workflow library is empty. Shelve one: `workflow save run_id=<run> name=<name>` "
                    "or ask the agent to save a graph it just ran.")
        rows = "\n".join(f"- **{x['name']}** — {x['nodes']} nodes, {x['gates']} gate(s), {x['fanouts']} fan-out(s)"
                          + (" [" + " ".join(x["tags"]) + "]" if x.get("tags") else "")
                          + (f": {x['description']}" if x.get('description') else "") for x in lib)
        # F-2 (#62): a quarantined entry lists with WHY it was refused — visible,
        # never fatal; a clean library never grows these lines (golden bytes).
        rows += ("\n" + "\n".join(f"- **{q['name']}** — refused: {q['reason']}"
                                  for q in quarantined)) if quarantined else ""
        return f"Workflow library:\n{rows}\n\nRun one: `/wf <name> [note for the run]`"
    name, _, note = arg.partition(" ")
    try:
        p = _lib_read(name)   # F1 #14: /wf reaches a pre-fix graph where it was saved
    except ValueError as e:
        return f"{e}"
    if not p.exists():
        names = ", ".join(x["name"] for x in lib) or "(empty)"
        return f"No library graph named `{name}`. Available: {names}"
    # #50: a library file is bare graph or {meta, graph} envelope; the normalizer
    # unwraps either, and the replayable name carries general/ when it applies.
    # F-2 (#62): an unknown shape is QUARANTINED — the operator sees the typed
    # refusal instead of a crash or a deceptive 0-node replay instruction.
    entry = _common.library_entry(jload(p))
    if "graph" not in entry:
        return (f"Library graph `{name}` is refused: {entry.get('invalid', 'invalid: unreadable')}. "
                "Fix or remove the file; the rest of the library stays usable.")
    g = entry["graph"]
    replay_name = _lib_rel_name(p) if p.parent != library_root() else p.stem
    return (f"Replay the shelved workflow **{replay_name}** ({len(g.get('nodes') or [])} nodes)"
            + (f" — operator note: {note}" if note else "") + ".\n"
            f"Agent: call `workflow{{action:\"run\", from:\"{replay_name}\""
            + (f", run_context:{json.dumps(note)}" if note else "")
            + "}`"
            + "; then `wait` and answer human gates only with the human's decision. Cards are optional.")

def register(ctx):
    # Core v0.21.5 runs import + register() on a deadline worker
    # (plugins.load_timeout_seconds, default 10s); on overrun it calls
    # ctx._abandon_load() and every later register_* is silently ignored.
    # Keep this body cheap and non-blocking: measured ~0.1s import, register()
    # < 1ms — one cached config read (model_tiers) and in-memory registrations,
    # no subprocess/network/run-dir scan (card_enforcement's module exec is
    # defs only; its I/O runs at hook time). Never pass override=True to
    # register_tool: it is operator-gated (PluginToolOverrideError). Pinned by
    # tests/test_hook_abandon_safety.py.
    global _CTX
    _CTX = ctx
    ctx.register_skill("workflow", HERE / "SKILL.md", description="Run and orchestrate agent workflows.")
    tiers = model_tiers()
    if tiers:
        WORKFLOW_PARAMS["properties"]["graph"]["description"] += (
            " node.model: a literal model id OR one of the owner's tiers " + json.dumps(tiers) +
            " — pick per node by how hard the step is. "
            "Unset = seat default. run echoes the resolved {node: model} table.")
    ctx.register_tool(name="workflow", toolset="workflows", schema=WORKFLOW_SCHEMA, handler=handle)
    # #157: last-resort card shipper (core fires this once per turn before the
    # assistant row persists; fail-open, marker-ledgered — see card_enforcement.py).
    ctx.register_hook("transform_llm_output", _card_enforcement._card_hook)
    ctx.register_command("wf", _wf_command, description="Workflow library: `/wf` lists, `/wf <name> [note]` replays a shelved graph",
                         args_hint="[name] [note]", argument_mode="text")
