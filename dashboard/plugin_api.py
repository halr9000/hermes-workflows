"""Dashboard backend for hermes-workflows — thin projection of the SHARED read model.

State truth lives in wfcommon.run_state (same function the door uses); gate release
shares the door's single answer path. Endpoints land under /api/plugins/hermes-workflows.
The UI does NOT resume runs — the owning agent does (philosophy: agent owns the run).
"""
import hashlib, importlib.util, json, os, sys
from pathlib import Path

_PLUGIN_ROOT = Path(__file__).resolve().parent.parent
_COMMON_MODULE = "_hermes_workflows_wfcommon_" + hashlib.sha256(
    str(_PLUGIN_ROOT).encode("utf-8")).hexdigest()[:16]

def _workflow_common():
    """Load this plugin's sibling module without binding global ``wfcommon``."""
    module = sys.modules.get(_COMMON_MODULE)
    if module is not None:
        return module
    spec = importlib.util.spec_from_file_location(_COMMON_MODULE, _PLUGIN_ROOT / "wfcommon.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load workflow module from {_PLUGIN_ROOT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_COMMON_MODULE] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(_COMMON_MODULE, None)
        raise
    return module

try:
    from fastapi import APIRouter, HTTPException
    router = APIRouter()
except ImportError:
    router = None

def _root():
    """ONE resolver (wfcommon.runs_root): `settings.runs_root` (owner, #42) > `WF_RUNS_ROOT`
    > `<hermes_home>/workflows`. Read at call time — no restart."""
    return _workflow_common().runs_root()

def _safe_run(run_id):
    rid = (run_id or "").strip()
    if not rid or rid != "".join(c for c in rid if c.isalnum() or c in "-_.") \
            or rid.startswith("."):
        return None
    r = _workflow_common().find_run(rid)   # legacy-location fallback for pre-fix ids
    return r if r.is_dir() else None

def _events(r, limit=400):
    out = []
    try:
        lines = (r / "events.jsonl").read_text().splitlines()[-limit:]
    except FileNotFoundError:
        return out
    for l in lines:
        try: out.append(json.loads(l))
        except Exception: pass
    return out

LOG_TAIL = 16384  # O2: chosen constant, not a measured one and not a config.

def _node_log_tail(r, nid, index, attempt, kind, tail):
    """O2's single read-only log route core. The path comes ONLY from the record's
    log_path/prompt_path (the client never sends a path) and must resolve inside
    <run>/logs. Tails the last `tail` bytes by seek, never reads the whole file.
    Returns dict|None; None = 404-worthy (bad id, no record, escape, missing file)."""
    common = _workflow_common()
    if kind not in ("log", "prompt") or not common.ID_OK.match(nid or ""):
        return None
    try:
        tail = int(tail); attempt = None if attempt is None else int(attempt)
        index = None if index is None else int(index)
    except (TypeError, ValueError):
        return None
    if not 0 < tail <= LOG_TAIL or (attempt is not None and attempt < 0):
        return None
    name = nid + (f".{index}" if index is not None else "")
    rec = common.jload(r / "nodes" / f"{name}.json")
    if not isinstance(rec, dict):
        return None
    p = rec.get("prompt_path" if kind == "prompt" else "log_path")
    if not isinstance(p, str) or not p:
        return None
    lp = Path(p)
    if attempt is not None:  # sibling spawn log: same base, different .a<n>
        ext = ".prompt.md" if kind == "prompt" else ".log"
        lp = lp.with_name(f"{nid}" + (f".{index}" if index is not None else "") + f".a{attempt}{ext}")
    logs = (r / "logs").resolve()
    try:
        rp = lp.resolve()
        rp.relative_to(logs)  # containment; ValueError on escape
    except (OSError, ValueError):
        return None
    try:
        size = rp.stat().st_size
        with open(rp, "rb") as f:
            if size > tail:
                f.seek(-tail, os.SEEK_END)
            data = f.read()
    except OSError:
        return None
    return {"id": r.name, "node": name, "kind": kind, "size": size,
            "bytes": len(data), "content": data.decode("utf-8", errors="replace")}

def _view(r, full=False, lock_state=None):
    st = _workflow_common().run_state(r, lock_state=lock_state)
    if not st:
        return None
    view = {"id": st["run_id"], "name": st["name"], "status": st["status"],
            "started": st.get("started"), "owner": st.get("owner"),
            "nodes_done": st["done"], "nodes_skipped": st["skipped"], "nodes_total": st["total"],
            "updated": (r / "events.jsonl").stat().st_mtime if (r / "events.jsonl").exists() else 0}
    if st.get("runner_lock") and st["runner_lock"] not in ("busy", "free"):
        # #44: a wedge (or an EACCES-ambiguous lock) is visible on the card —
        # honest absence otherwise (golden key sets byte-unchanged for busy/free).
        view["runner_lock"] = st["runner_lock"]
        if st.get("runner_lock_detail"):
            view["runner_lock_detail"] = st["runner_lock_detail"]
    # Agent-first pane (2026-10-03): stamp WHO launched this run. The read model
    # resolves owner.session_id -> profile name via the profiles' own session
    # tables (wfcommon.profiles_by_session, read-only). Absent stays absent —
    # the UI falls back to the raw session id, never invents a bot.
    try:
        p = _workflow_common().profiles_by_session().get((st.get("owner") or {}).get("session_id"))
        if p:
            view["owner_profile"] = p
    except Exception:
        pass
    if st["held_gate"]:
        view["held_gate"] = st["held_gate"]
    if st.get("runner_exit"):
        view["runner_exit"] = st["runner_exit"]  # shared read model (wfcommon.run_state)
    if full:
        import time
        common = _workflow_common()
        # est-la8m: fold each node's children through its OWN child-DB home
        # (wfcommon.run_child_metrics, the fb-904f seam the door uses) — a bare
        # caller-home fold reads only the launcher's state.db and false-zeros
        # every profile-routed child in the dashboard's full view (issue #81).
        cm = common.run_child_metrics(r)
        nodes = {}
        for nid, v in st["nodes"].items():
            nodes[nid] = dict(v)
            # O2: ONE node truth — every node WITH a record gets the closed fact set
            # verbatim (was: done|failed only, which dropped `partial` output even
            # though the read model treats partial as output, wfcommon.run_state).
            facts = common.node_facts(r, nid)
            if facts is not None:
                facts = {k: x for k, x in facts.items() if k != "status"}
                nodes[nid].update(facts)
            # metrics: node-level = the node's own child, or the fold of its items' children.
            # Keys are prefix-matched (any efp suffix) so an amended re-run's fresh rows count.
            prefix = f"wf:{st['run_id']}:{nid}:"
            mine = {k: m for k, m in cm.items() if k.startswith(prefix)}
            active = v.get("active_spawns", [])
            if mine or active:
                groups = {}
                def item_index(key):
                    parts = key[len(prefix):].split(":")  # "<i>:<efp8>.<nonce>" | "<efp8>.<nonce>"
                    return int(parts[0]) if len(parts) == 2 and parts[0].isdigit() else None
                for k, m in mine.items():
                    groups.setdefault(item_index(k), {})[k] = m
                for spawn in active:
                    key = spawn["skey"].split("#a", 1)[0]
                    if key.startswith(prefix):
                        groups.setdefault(item_index(key), {})
                heartbeat = common.current_attempt(mine, active)
                nodes[nid]["metrics"] = _fold_metrics(mine.values()) or {}
                nodes[nid]["metrics"].update(heartbeat)
                nodes[nid]["metrics"]["scope"] = "cumulative"
                items = {}
                for i, ms in groups.items():
                    if i is not None:
                        items[i] = _fold_metrics(ms.values()) or {}
                        items[i].update(common.current_attempt(ms, [s for s in active
                            if s["skey"].split("#a", 1)[0].startswith(f"{prefix}{i}:")]))
                if items:
                    nodes[nid]["item_metrics"] = items
        active = [s for v in st["nodes"].values() for s in v.get("active_spawns", [])]
        rollup = _fold_metrics(cm.values()) if cm else ({} if active else None)
        if rollup is not None:
            rollup.update(common.current_attempt(cm, active))
            rollup["scope"] = "cumulative"
            # #114: no local unknown-rule here — fold_child_metrics already
            # omitted api_calls for any unknown row (was: a per-copy pop, the
            # half of the divergence this fix removes).
        view.update({"graph": st["graph"], "nodes": nodes, "events": _events(r),
                     "gate": dict(st["held_gate"] or {}), "metrics": rollup})
    return view

def _fold_metrics(ms):
    # #114 (WF-04): thin adapter — the ONE fold + known/unknown policy is
    # wfcommon.fold_child_metrics (AGENTS.md: the metrics join belongs to
    # wfcommon). Every level (node line, fanout item, rollup) omits api_calls
    # on ANY unknown child — the door and this surface cannot diverge.
    return _workflow_common().fold_child_metrics(ms)

def _list_runs():
    # F1 #14: merge the legacy launch root the way the door's act_list does — a
    # pre-fix run opens by URL via _safe_run's find_run fallback and must also
    # appear in the list. Resolved root wins on id collision.
    # est-2ek.1.762 census hygiene: enumerate through wfcommon.iter_run_dirs
    # (unique by REALPATH across every scanned root — a same-named torn stub
    # in one root no longer hides the valid run in another; est-7ps8), and
    # report how many rows EXECUTED (logs present).
    # executed counts ONLY rows that land in `runs` (a valid, de-duplicated
    # row) — dirs _view rejects never become rows, so they must not inflate
    # the counter (zap non-blocking note 2). This listing ALWAYS measures:
    # the actual count — including a measured zero — is supplied to
    # run_summary, which renders an omitted count as the absent key
    # (wfcommon.run_summary's supplied-versus-unmeasured contract).
    common = _workflow_common()
    roots = [_root()]
    legacy = common.launch_runs_root()
    if legacy != roots[0]:
        roots.append(legacy)
    runs, seen = [], set()
    executed = 0
    # #44 (PR #47 round-3 sibling gap): 50 rows used to spawn 50 probe children.
    # ONE batch probe child + ONE ledger read for the whole census now; _view
    # consumes the pre-observed per-dir lock state (zero extra probes).
    scanned = list(common.iter_run_dirs(roots, reverse=True))
    _lock_states = common.runner_lock_states(scanned)
    for r in scanned:
        v = _view(r, lock_state=_lock_states.get(r))
        if v and v["id"] not in seen:
            seen.add(v["id"])
            # est-7ps8 (note 2): count EXECUTED only for rows that land in
            # `runs` — a torn dir that never reaches the list must not
            # inflate the counter (executed > total was the probe).
            if common.run_executed(r):
                executed += 1
            runs.append(v)
    summary = common.run_summary(runs, executed=executed)
    return {**({"roots": [str(x) for x in roots]} if not runs else {}),
            "runs": runs[:100], **summary}


if router is not None:
    @router.get("/runs")
    async def list_runs():
        return _list_runs()

    @router.get("/runs/{run_id}")
    async def get_run(run_id: str):
        r = _safe_run(run_id)
        return (_view(r, full=True) if r else None) or {"error": "unknown run"}

    @router.get("/runs/{run_id}/nodes/{nid}/log")
    async def get_node_log(run_id: str, nid: str, index: int | None = None,
                           attempt: int | None = None, kind: str = "log",
                           tail: int = LOG_TAIL):
        """O2's ONE tail (the desktop's `rec?.raw` plumbing dies with this route).
        Read-only; path from the record only; contained to <run>/logs."""
        r = _safe_run(run_id)
        if not r:
            raise HTTPException(status_code=404, detail="unknown run")
        out = _node_log_tail(r, nid, index, attempt, kind, tail)
        if out is None:
            raise HTTPException(status_code=404, detail="no such log")
        return out

    @router.post("/runs/{run_id}/gate")
    async def release_gate(run_id: str, body: dict):
        """UI door onto the SAME answer path the tool uses (incl. stale-answer
        overwrite). Does NOT resume the run — the owning agent does."""
        r = _safe_run(run_id)
        if not r:
            return {"ok": False, "error": "unknown run"}
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_hermes_workflows_door", Path(__file__).resolve().parent.parent / "__init__.py")
        door = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(door)
        return door._release_core(r, body.get("gate_id"), body.get("answer", ""), ui=True)
