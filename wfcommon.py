"""hermes-workflows shared semantics — ONE validator, ONE fingerprint rule, ONE read model.

Imported by wf.py (runner), __init__.py (tool door), dashboard/plugin_api.py (UI API).
Everything that decides "is this result still trustworthy" or "what state is this run
in" lives here, so the three readers can never disagree.
"""
import hashlib, json, os, re, shlex, subprocess, sys, time
from datetime import datetime, timezone
from collections import namedtuple
from pathlib import Path

def jload(p, default=None):
    try:
        return json.loads(Path(p).read_text())
    except Exception:
        return default

# est-3acn: `budget` (the RunBudget token cap) is budget policy, not committed
# work — exactly like its wall-seconds sibling run_budget: raising a cap must
# NEVER un-freeze committed nodes (that would re-drive finished work to buy
# headroom the author already paid for). Nodes without the key hash
# byte-identically (no legacy drift, same law as every budget key).
_BUDGET_KEYS = ("max_turns", "timeout", "run_budget", "shape", "budget")
# #100: scheduling limits are policy, not a change to committed node work.
_CONCURRENCY_KEYS = ("concurrency", "item_concurrency")
# #24/#25 (A3): route POLICY (require_route) and the door's proof ANNOTATION
# (route_verified) are not work either — like budgets they must not participate in
# def_hash: the door bakes route_verified into graph.json after validation, and the
# runner computes efp on the baked node; an author amend that flips
# defaults.require_route would otherwise un-freeze every committed node and re-run
# it. Nodes without these keys hash byte-identically to before (no legacy drift).
# #116: the engine's substitution ANNOTATION is policy too (like the proof it
# accompanies): the door bakes it into graph.json after validation, so a stamped
# node must not un-freeze on replay. The node's real model/provider DO participate —
# a substitution changes the work's route, and the hash sees it (unlike route_verified,
# which annotates a route that never moved).
# #130: `resume_hint` only shapes the RE-RUN prompt (prompt-side policy), never the
# work — flipping it on an amend must not un-freeze committed nodes.
_POLICY_KEYS = ("require_route", "route_verified", "substrate_substituted", "resume_hint")
FP_RULE_LEGACY = 1  # before b79fa21: budgets participated in def_hash
FP_RULE_VERSION = 2  # b79fa21: exclude budgets
FP_RULES = (FP_RULE_LEGACY, FP_RULE_VERSION)

def def_hash(node, rule=FP_RULE_VERSION):
    # Budgets are not work: raising a wall or naming a shape must not invalidate a
    # committed node (and apply_graph_defaults baking budgets into an old run's
    # frozen graph at amend must not re-run everything it already finished).
    if rule not in FP_RULES:
        raise ValueError(f"unknown fingerprint rule: {rule!r}")
    if rule == FP_RULE_VERSION:
        node = {k: v for k, v in node.items()
                if k not in _BUDGET_KEYS and k not in _CONCURRENCY_KEYS and k not in _POLICY_KEYS}
    elif "resume_hint" in node:
        # #130 did not exist in legacy records; exclude only this new prompt key.
        # Historical budget, concurrency and route-policy semantics stay intact.
        node = {k: v for k, v in node.items() if k != "resume_hint"}
    return hashlib.sha256(json.dumps(node, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]

_EFP_SEP = "\u241f"

def efp(byid, node, _seen=None, *, rule=FP_RULE_VERSION, memo=None):
    """Effective fingerprint: own def + every ancestor's effective fingerprint.
    An amended node (or any ancestor) changes the efp of everything downstream, so
    downstream results are stale and nodes downstream re-run or re-hold — transitively.

    DAG-memoized: pass a shared `memo` dict (one per live `byid` table) and each
    node's effective fingerprint computes once per (id, rule) instead of once per
    path reaching it. Diamond-shaped graphs are exponential without it — an N-wide
    join layer costs 2^N def_hash re-hashes of the SAME nodes — which is what made
    a listing over real runs hash a quarter-million node definitions. `memo=None`
    (the default) is the classic path, byte-identical to before; a fresh empty
    dict is a live memo. Callers that mix several
    graphs into one `byid` (or mutate node defs while the table lives) pass no memo.
    Memo values are pure functions of the graph, so a table holding one graph may
    share one memo across every read of it.
    """
    _seen = _seen or set()
    if memo is None:
        return _efp(byid, node, _seen, rule, None)[0]
    hit = memo.get((node["id"], rule))
    if hit is not None:
        return hit
    val, tainted = _efp(byid, node, _seen, rule, memo)
    if not tainted:
        memo[(node["id"], rule)] = val
    return val

def _efp(byid, node, _seen, rule, memo):
    """efp's walk: (value, tainted). `tainted` marks a value whose computation
    depended on a cycle cut (a node already in `_seen` — the "?" law firing on a
    malformed cyclic byid), so the result is walk-path-dependent and NEVER
    enters the memo; an un-tainted value is a pure function of the node's
    acyclic closure and a memo hit is therefore byte-identical to recomputing
    it. Values, cycle law included, are identical to the un-memoized walk."""
    nid = node["id"]
    if nid in _seen:
        return "?", True  # unreachable for validated (acyclic) graphs
    _seen = _seen | {nid}
    if memo is not None:
        hit = memo.get((nid, rule))
        if hit is not None:
            return hit, False
    parts = [def_hash(node, rule)]
    tainted = False
    for a in sorted(node.get("after", [])):
        if a in byid:
            v, t = _efp(byid, byid[a], _seen, rule, memo)
            parts.append(v)
            tainted = tainted or t
    return hashlib.sha256(_EFP_SEP.join(parts).encode()).hexdigest()[:16], tainted


def graph_fingerprint(graph, *, rule=FP_RULE_VERSION):
    """Stable signature of the node definitions that a runner verdict describes."""
    nodes = (graph or {}).get("nodes")
    if not isinstance(nodes, list) or any(not isinstance(n, dict) or not n.get("id") for n in nodes):
        return None
    try:
        byid = {n["id"]: n for n in nodes}
        fp_memo = {}   # same single-graph read as run_state: one shared memo
        facts = {str(n["id"]): efp(byid, n, rule=rule, memo=fp_memo) for n in nodes}
        raw = json.dumps(facts, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
    except Exception:
        return None

def source_digest(graph):
    """1.1 (RATIFY F5): sha256 hex over the canonical `nodes` JSON of a graph — the
    provenance identity of a shelved graph. NOT the effective fingerprint:
    graph_fingerprint() keeps its name and meaning (per-node efp facts, 16 hex);
    this is the whole node list, budgets included, full 64-hex digest. None when the
    graph has no node list."""
    nodes = (graph or {}).get("nodes") if isinstance(graph, dict) else None
    if not isinstance(nodes, list):
        return None
    try:
        raw = json.dumps(nodes, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(raw.encode()).hexdigest()

# ---------- runs root + launcher identity (1.1, RATIFY F1/F2) ----------
# ONE resolver for "where do runs live": the runner, the door and the dashboard used to
# compute `$HERMES_HOME/workflows` inline in four places. `WF_RUNS_ROOT` (absolute dir)
# overrides so a team can share one root that survives any profile's deletion; unset or
# empty = the 1.0.15 default, byte-identical.

# ---------- est-2ek.1.699: the ONE route-identity contract ----------
# The door proves a route at submit under the author's spelling; core bills under its
# own normalized one. Every identity comparison between those worlds (the door's
# same-route ping law, the runner's #25 commit hold, the #641 receipt hold) goes
# through route_ids_equal, so a proven-and-run item cannot die route_unavailable over
# punctuation. Provider-aware and a mirror of stock core's normalize_model_name
# (agent/anthropic_message_convert.py): only claude prefixed Anthropic ids fold, each
# '.' to '-'; '_' is kept, Bedrock ids keep their namespace dots; every other
# provider compares as spelled, and an aggregator's vendor namespace is part of the id.
_BEDROCK_PREFIXES = ("global.", "us.", "eu.", "apac.", "ap.", "au.", "jp.", "ca.",
                     "sa.", "me.", "af.", "anthropic.")

def canonical_model_id(model, provider=None):
    """Identity form of `model` on `provider`'s route; '' for absent input (absence
    proves nothing). Lowercase, strip, peel a leading '<provider>/' only when it
    names the route's OWN provider; Anthropic only, claude prefixed, non-Bedrock: each
    '.' becomes '-' (as stock core bills it). A literal '(suffix)' is part of the id:
    stock core preserves it and records the concrete model verbatim, so this contract
    never peels it for any provider. Never rewrites what gets billed or
    relaxes a gate."""
    m = str(model or "").strip().lower()
    if not m:
        return ""
    prov = str(provider or "").strip().lower()
    if prov and m.startswith(prov + "/"):
        m = m[len(prov) + 1:]
    if prov == "anthropic" and m.startswith("claude-") and not m.startswith(_BEDROCK_PREFIXES):
        m = m.replace(".", "-")
    return m

def route_ids_equal(a, b, provider=None):
    """True when `a` and `b` name the same model on `provider`'s route under the
    contract above. Empty on either side is never equal."""
    ca, cb = canonical_model_id(a, provider), canonical_model_id(b, provider)
    return bool(ca) and bool(cb) and ca == cb

def route_provider(node, verified=None):
    """The provider whose spelling rules apply: the RECEIPT's own (leading segment of a
    provider-qualified `verified`), else the node's `provider`, else ''. When both are
    present and differ the route is cross-provider: '' (nothing folds, so a
    substitution is refused before any normalization)."""
    prov = str((node or {}).get("provider") or "").strip().lower()
    v = str(verified or "")
    vp = v.partition("/")[0].strip().lower() if "/" in v else ""
    if vp and prov and vp != prov:
        return ""
    return vp or prov

def hermes_home():
    """Core's resolution when importable, else the raw env (1.0.15 semantics, unchanged).

    ``hermes_constants.get_hermes_home`` is context-local override -> HERMES_HOME env ->
    platform default. A profile-scoped/embedding host (gateway ``-p`` under a supervisor,
    multiplexed dashboard ``--open-profile``) keeps ``os.environ["HERMES_HOME"]`` at the
    LAUNCH root and serves the profile through that override — reading the env alone
    hands the door, the runner, and every child the BASE profile: seat config invisible,
    profile-defined providers die 'Unknown provider', and run dirs land under the wrong
    home. Core's own #18594 warning instructs subprocess spawners to pass HERMES_HOME
    explicitly; the door honours that by stamping the resolved home into the runner env.
    """
    try:
        from hermes_constants import get_hermes_home
        return get_hermes_home()
    except Exception:
        return Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))


# ---------- originating-bot attribution (agent-first pane, 2026-10-03) ----------
# run.json owner.session_id names the LAUNCHING session but not WHO it was. Core
# stores every session in the profile that owns it: the estate root's own state.db
# (root/default profile) and profiles/<name>/state.db (sessions: id, profile_name).
# A read-only sweep of those tables resolves session -> bot name for the desktop
# pane and the drawer; it NEVER writes and never fails a listing (a locked/absent
# db just contributes no rows). TTL cache so a pane poll doesn't reopen every
# profile's db; the cache is keyed by the NORMALIZED estate root (est-wk7l): a
# named-profile home resolves through hermes_root() to the estate root, so sibling
# profiles and the root's own default db participate, and one root's cached map can
# never be served for a different root (a stale map is a wrong label, not a cache win).
_PROFILE_BY_SESSION_TTL = 60.0
_profile_by_session_cache = {}   # normalized root str -> {"at": ts, "map": {...}}


def profiles_by_session(home=None):
    """{session_id: profile_name} across the estate root's default db and every
    profile's state.db (read-only). Absent/locked db or missing table contributes
    nothing; a session absent from every table resolves to nothing — callers fall
    back honestly, never invent."""
    import time
    root = hermes_root(home)          # named profile home -> estate root (normalizer)
    key = str(root)
    now = time.time()
    c = _profile_by_session_cache.get(key)
    if c is not None and now - c["at"] < _PROFILE_BY_SESSION_TTL:
        return c["map"]
    out = {}
    try:
        db_files = [root / "state.db", *sorted(profiles_root(home).glob("*/state.db"))]
    except Exception:
        db_files = []
    for db in db_files:
        try:
            import sqlite3
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=1.5)
            try:
                for sid, prof in con.execute(
                        "SELECT id, profile_name FROM sessions WHERE profile_name IS NOT NULL"):
                    out[str(sid)] = str(prof)
            finally:
                con.close()
        except Exception:
            continue  # locked/foreign-schema/absent db: no rows, never an error
    # every sweep commits its OWN answer for its OWN root (empty included): a
    # different root never inherits the previous root's labels.
    _profile_by_session_cache[key] = {"at": now, "map": out}
    return out


# ---------- owner settings (#41/#42: tool-bridge first-class) ----------
# `plugins.entries.hermes-workflows.settings.{runs_root,profile}` are OWNER vocabulary,
# read at CALL time (no restart) through the same plugin-scoped helper `_hermes_bin`
# uses (PluginContext.get_config). The door installs that helper via
# `set_owner_setting_reader`; a process without a plugin ctx (runner, dashboard API,
# standalone tests) falls back to a side-effect-free raw read of the resolved
# `<hermes_home>/config.yaml` — the SAME file core's loader resolves for the
# profile, never core's load_config (which materialises a home skeleton on read).
# Unset keys = byte-identical to the resolver before owner settings shipped. The model is never told to
# set these; a graph/run argument can never substitute for them.
PLUGIN_ID = "hermes-workflows"
NO_READER = object()   # a reader answers this when it has no plugin ctx to ask
_OWNER_SETTING_READER = None
_RAW_SETTINGS_CACHE = {}  # path -> ((mtime_ns, size), (settings, legacy))

def set_owner_setting_reader(fn):
    """Install the door's plugin-scoped reader: fn(key) -> value | None | NO_READER."""
    global _OWNER_SETTING_READER
    _OWNER_SETTING_READER = fn

def _yaml_load(text):
    """Core's own YAML policy when importable (hermes_yaml: ruamel, YAML 1.1 booleans),
    else ruamel/pyyaml directly — the config file must parse the way core parses it."""
    try:
        from hermes_yaml import safe_load
        return safe_load(text)
    except ImportError:
        pass
    try:
        from ruamel.yaml import YAML
        y = YAML(typ="safe", pure=True)
        y.version = (1, 1)
        return y.load(text)
    except ImportError:
        import yaml
        return yaml.safe_load(text)

# ---- `${VAR}` / `${env:VAR}` expansion, ported from core's loader ----
# hermes_cli/config.py `_expand_env_vars` / `_env_expand_match` / `_env_ref_var_name`
# / `_env_ref_lookup` (rules copied verbatim, no core import): a raw reader that skips
# this makes the door (core loader, expanded) and the runner/dashboard (raw read)
# disagree on the SAME key — the door creates a run under the expanded path, the runner
# refuses the literal `${HOME}/...` as non-absolute and the run is invisible (F2f).
_ENV_REF_RE = re.compile(r"\${([^}]+)}")

def _is_non_env_secret_ref(ref):
    """True for a SecretRef body with a non-`env` source (`bitwarden:FOO`, `vault:...`)."""
    return ":" in ref and re.match(r"^[a-z][a-z0-9_-]*:", ref) is not None

def _env_ref_var_name(ref):
    """Env-var name a `${VAR}` / `${env:VAR}` ref reads, or None for a non-env source / empty `env:`."""
    ref = ref.strip()
    if ref.startswith("env:"):
        return ref[len("env:"):].strip() or None
    if _is_non_env_secret_ref(ref):
        return None
    return ref

def _env_ref_lookup(name):
    """Core's policy verbatim: the profile secret scope when one is active, else plain os.environ."""
    try:
        from agent.secret_scope import current_secret_scope, get_secret as _get_secret
    except Exception:
        return os.environ.get(name)
    if current_secret_scope() is None:
        return os.environ.get(name)
    return _get_secret(name)

def _expand_config_value(raw):
    """Expand one `${VAR}` (legacy bare name) or `${env:VAR}` (Cursor-style SecretRef) string.
    Non-env sources and UNRESOLVED refs stay verbatim (exactly like core); the validators
    below then refuse a surviving `${` on EVERY reader, so no path ever resolves a run
    against a half-expanded value."""
    def _m(m):
        inner = m.group(1).strip()
        name = _env_ref_var_name(inner)
        if name is None:
            return m.group(0)          # non-env source or empty `env:` — keep verbatim
        val = _env_ref_lookup(name)
        return val if val is not None else m.group(0)   # unset var — keep verbatim (core)
    return _ENV_REF_RE.sub(_m, raw)

def _expand_config_values(obj):
    """Recursive `${VAR}`/`${env:VAR}` expansion over a settings mapping (keys/non-strings untouched)."""
    if isinstance(obj, str):
        return _expand_config_value(obj)
    if isinstance(obj, dict):
        return {k: _expand_config_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_config_values(item) for item in obj]
    return obj

def _no_unresolved_ref(label, raw):
    """A surviving `${...}` after expansion means the referenced var is unset (or a
    non-env source): core keeps such templates verbatim, so EVERY reader must refuse
    them with the same error — never let the door alone see a value."""
    if isinstance(raw, str) and _ENV_REF_RE.search(raw):
        raise ValueError(f"{label} has an unresolved ${{...}} reference: {raw!r} "
                         "— set the referenced environment variable or use a literal absolute path")

def _raw_owner_settings():
    """`plugins.entries.hermes-workflows` raw read from the resolved home's config.yaml
    (no core import, no skeleton side effects): returns `(settings, legacy_config)` —
    two dicts, either empty when absent/unparseable. Values come back EXPANDED with
    core's `${VAR}` / `${env:VAR}` semantics (`_expand_config_value` mirrors
    hermes_cli/config.py), so a raw read answers exactly what core's loader hands the
    door's plugin ctx. Unresolved refs stay verbatim HERE and are refused downstream
    (`_no_unresolved_ref`), identically on every reader."""
    p = hermes_home() / "config.yaml"
    try:
        st = p.stat()
        sig = (st.st_mtime_ns, st.st_size)
    except OSError:
        return {}, {}
    hit = _RAW_SETTINGS_CACHE.get(str(p))
    if hit and hit[0] == sig:
        return hit[1]
    settings, legacy = {}, {}
    try:
        cfg = _yaml_load(p.read_text()) or {}
        entry = ((cfg.get("plugins") or {}).get("entries") or {}).get(PLUGIN_ID) or {}
        found = entry.get("settings")
        settings = found if isinstance(found, dict) else {}
        conf = entry.get("config")
        legacy = conf if isinstance(conf, dict) else {}
        settings = _expand_config_values(settings)
        legacy = _expand_config_values(legacy)
    except Exception:
        settings, legacy = {}, {}
    _RAW_SETTINGS_CACHE[str(p)] = (sig, (settings, legacy))
    return settings, legacy

_UNSET = object()   # THE missing-key sentinel — one instance, compared by identity

def _nested(mapping, key):
    cur = mapping
    for seg in key.split("."):
        if not isinstance(cur, dict) or seg not in cur:
            return _UNSET
        cur = cur[seg]
    return cur

def owner_setting(key):
    """One owner-settings read: the door's plugin ctx when it has one (its answer is
    final, unset included), else the raw config file of the resolved home. The raw
    fallback is PER KEY, mirroring core's `PluginContext.get_config`
    (hermes_cli/plugins.py): `settings.<key>` wins when present, else the legacy
    `config.<key>` subtree, else unset — never whole-mapping substitution, or the
    door (core reader) and the runner/dashboard (raw reader) disagree on WHICH value
    a key holds and the door creates runs the runner cannot find."""
    if _OWNER_SETTING_READER is not None:
        try:
            v = _OWNER_SETTING_READER(key)
        except Exception:
            v = NO_READER
        if v is not NO_READER:
            return v
    settings, legacy = _raw_owner_settings()
    v = _nested(settings, key)
    if v is _UNSET:
        v = _nested(legacy, key)
    return None if v is _UNSET else v

def settings_runs_root(home=None):
    """`settings.runs_root` as a validated absolute Path, or None when unset/empty.
    FAIL-CLOSED: a malformed value raises ValueError (the door surfaces it as the
    action's error) — it is never silently ignored, because a silent fallback would
    re-create exactly the invisible door-vs-tab divergence #42 describes.
      * expanduser applied; the result MUST be absolute (a relative root would be
        cwd-dependent: a different root per process = invisible runs).
      * MUST NOT resolve inside another profile's home under the caller's estate
        (`<hermes_root>/profiles/<name>/`, the directory core's `-p` names live in)
        unless that home IS the caller's own resolved home — a typo pinning the shared
        estate to one profile's private dir would make every other profile's runs
        vanish into it; the caller's own profile home is the ordinary per-profile
        default and stays legal. Only the estate's profiles root counts: a path that
        merely contains a `profiles` segment elsewhere on disk is not a Hermes profile."""
    raw = owner_setting("runs_root")
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("settings.runs_root must be a string (absolute directory path)")
    raw = raw.strip()
    if not raw:
        return None
    _no_unresolved_ref("settings.runs_root", raw)   # an unset ${VAR} stays verbatim in core
    p = Path(raw).expanduser()
    if not p.is_absolute():
        raise ValueError(f"settings.runs_root must be an absolute path, got {raw!r}")
    own = Path(home) if home is not None else hermes_home()
    def _res(x):
        try:
            return x.resolve()
        except OSError:
            return x
    own_r, p_r = _res(own), _res(p)
    profiles = _res(profiles_root(own))
    if p_r == profiles:
        raise ValueError(
            f"settings.runs_root {raw!r} IS the estate's profiles root — refused: it would "
            "mix every seat's namespace into one runs dir and collide with profile homes "
            "(pick a dedicated directory, e.g. <estate>/workflows)")
    if len(p_r.parts) > len(profiles.parts) and p_r.parts[:len(profiles.parts)] == profiles.parts:
        prof_home = Path(*p_r.parts[:len(profiles.parts) + 1])
        if prof_home != own_r:
            raise ValueError(
                f"settings.runs_root {raw!r} resolves inside profile home {prof_home} — "
                "refused: a runs root inside another profile's private dir pins the estate to "
                "that profile (only the launcher's own profile home is allowed)")
    return p_r

def settings_profile():
    """`settings.profile` validated with the same rules `profile:` node keys use
    (_PROFILE_NAME_BAD); None when unset/empty; ValueError on an unsafe name.
    FAIL-CLOSED on a GHOST (#46 R1): the named profile home `<hermes_root>/profiles/
    <name>/` must EXIST — a stamped `dispatched_by` nobody can verify poisons the
    consent model (targets list the ghost, team readmodel resolves nothing) and is
    exactly the silent divergence #42 was about. Applies to the RESOLVED value
    whatever subtree it came from (settings or legacy `config`), identically on every
    reader, so a typo says 'does not exist' at the door instead of inventing an
    identity. The 'default' fallback (nothing configured) is untouched."""
    raw = owner_setting("profile")
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("settings.profile must be a string (a profile directory name)")
    raw = raw.strip()
    if not raw:
        return None
    if _PROFILE_NAME_BAD.search(raw):
        raise ValueError(f"settings.profile {raw!r} is not a safe profile directory name "
                         "(no separators, no '..', no leading dot)")
    _no_unresolved_ref("settings.profile", raw)   # unset ${VAR} stays verbatim in core
    ph = profile_home(raw)
    if not ph.is_dir():
        raise ValueError(f"settings.profile {raw!r} does not exist: no profile home at {ph} "
                         "(create the profile or fix plugins.entries.hermes-workflows.settings.profile)")
    return raw


def launch_runs_root():
    """Runs root of the RAW process environment (never the context-resolved home).

    ONLY for legacy-location lookup: runs created before the profile-home fix live
    under the launch root on a profile-scoped host and stay resolvable by run_id.
    New runs never land here. `WF_RUNS_ROOT` still wins (shared team root).
    """
    override = os.environ.get("WF_RUNS_ROOT", "")
    if override:
        return Path(override)
    return Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes")) / "workflows"


def find_run(rid):
    """Locate a run dir by id: resolved runs_root() first; legacy launch root only
    for an EXISTING run (pre-fix ids stay resumable, new ids never land there).
    Selection mirrors the listing: among the same-id dirs across the roots, the
    first that YIELDS A VIEW wins (resolved first, so an intact resolved dir is
    untouched — the legacy probe is that same rule in the legacy-only shape).
    A dir yields a view when run_state() reads a state from it. Unknown ids
    still answer the resolved path (the create/resume shape callers rely on)."""
    root = runs_root()
    legacy = launch_runs_root()
    for cand in iter_run_dirs([root, legacy] if legacy != root else [root]):
        if cand.name == rid and run_state(cand):
            return cand
    if legacy != root and (legacy / rid).is_dir() and not (root / rid).exists():
        return legacy / rid
    return root / rid


def iter_run_dirs(roots, reverse=False):
    """Every run dir under one or more runs roots, unique by REALPATH.

    Accepts a single root (str/Path) or a sequence of roots. Skips non-dirs and
    dot-dirs (<runs_root>/.seats is the global seat-ticket dir, est-g2xx — never
    a run). Census hygiene (est-2ek.1.762): symlinked/profile-scoped roots
    multiply hits — the same run appears under the resolved root, the legacy
    launch root, and every profile home that mirrors it — so one run counts
    once; first-seen wins, callers pass the resolved root first.
    est-7ps8 (zap probe, PR#259 6030355791): the dedupe key is the RESOLVED
    path, not the bare NAME — two physically-distinct dirs that merely share a
    name (a torn partial under the resolved root + the valid run under the
    legacy root) are BOTH enumerated; name-dedupe let the torn first-root stub
    hide the valid twin from every read model. Same-physical-dir mirrors still
    collapse because they resolve to one path. The ONE enumeration every
    list/read-model/scan routes through, never raw `root.iterdir()`. Sorted by
    name (newest-first when reverse=True) within each root, roots taken in
    order."""
    if isinstance(roots, (str, Path)):
        roots = [roots]
    seen, out = set(), []
    for root in roots:
        try:
            entries = sorted(Path(root).iterdir(), reverse=reverse)
        except OSError:
            continue
        for r in entries:
            try:
                if r.name.startswith(".") or not r.is_dir():
                    continue
                key = r.resolve()
            except OSError:
                continue
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
    return out


def quota_cache_path(cache_path=None):
    """ONE resolver for the #24(b) seat quota cache path: explicit argument >
    `WF_QUOTA_CACHE` env > `<hermes_home>/cache/workflow-quota-cache.json`.
    The door's refusal gate and the runner's stamp MUST agree on this path —
    a silent drift turns the fail-closed quota gate into a no-op on an empty
    cache (est-7m6wv horde finding: the law was hand-duplicated)."""
    if cache_path:
        return Path(cache_path)
    override = os.environ.get("WF_QUOTA_CACHE")
    if override:
        return Path(override)
    return (Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))
            / "cache" / "workflow-quota-cache.json")


def runs_root():
    """ONE resolver. Precedence (#42): `settings.runs_root` (owner, validated,
    fail-closed) > `WF_RUNS_ROOT` env (non-empty) > `<hermes_home>/workflows`
    (core-resolved home when importable, else `$HERMES_HOME`, else `$HOME/.hermes`)."""
    configured = settings_runs_root()
    if configured is not None:
        return configured
    override = os.environ.get("WF_RUNS_ROOT", "")
    if override:
        return Path(override)
    return hermes_home() / "workflows"

def effective_runs_root(environ):
    """The runs root a process RUNS UNDER, from its environment mapping (str->str, e.g. a
    parsed /proc/<pid>/environ). `settings.runs_root` (this process's owner setting)
    first — an owner-pinned root applies to every process of the estate, so a sibling
    runner's env is judged against the same single root runs_root() answers; else
    `WF_RUNS_ROOT` if set, else `HERMES_HOME/workflows`, else None (nothing to compare —
    legacy `if homes:` guard)."""
    try:
        configured = settings_runs_root()
    except ValueError:
        configured = None   # liveness is a read path: a bad setting is the door's error, not ours
    if configured is not None:
        return configured
    override = environ.get("WF_RUNS_ROOT", "")
    if override:
        return Path(override)
    home = environ.get("HERMES_HOME", "")
    if home:
        return Path(home) / "workflows"
    return None

def hermes_root(home=None):
    """The non-secret Hermes ROOT: `HERMES_HOME.parent.parent` when HERMES_HOME is a
    named profile home (`<root>/profiles/<name>`), else HERMES_HOME itself."""
    home = Path(home) if home is not None else hermes_home()
    return home.parent.parent if home.parent.name == "profiles" else home

def launcher_profile(home=None):
    """Launcher identity — NEVER from a graph/run arg. Ranked (#41):
      1. the process's own resolved HERMES_HOME when it is `<root>/profiles/<name>` -> name
      2. else `settings.profile` (owner-declared fallback for env-blind gateways: the
         desktop bridge / a gateway that launches the door without a profile home)
      3. else "default" (byte-identical to the stamp before owner settings shipped)
    Env wins whenever present: an owner setting can only fill the identity the process
    could not carry, never override one it does. An explicit `home=` is an env-shaped
    caller (tests, runner_alive) and follows the same ranking."""
    home = Path(home) if home is not None else hermes_home()
    if home.parent.name == "profiles":
        return home.name
    configured = settings_profile()
    return configured if configured else "default"

def profiles_root(home=None):
    return hermes_root(home) / "profiles"

def profile_home(name, home=None):
    return profiles_root(home) / str(name)

_PROFILE_NAME_BAD = re.compile(r"(^[/~.]|[\\/]|\.\.|[\x00-\x1f])")

def profile_errors(nodes, launcher=None, profiles_dir=None):
    """1.1 (RATIFY F2/B1) door-level validation of agent `profile:` keys, run AFTER the
    door rendered `{run.KEY}` bindings and BEFORE any run write. Returns [{node,
    field:'profile', msg}]. Rules: non-empty string (a safe directory name); name !=
    "default" (default is never a target); `<profiles_dir>/<name>/config.yaml` exists;
    `<profiles_dir>/<name>/workflow_team.json` exists and its `accept_from` list contains
    `launcher` ("default" is a legal launcher name). DELEGATION, NOT ISOLATION: consent is
    TARGET-owned — the target profile lists who may launch it; the launcher identity is
    resolved from the door's own HERMES_HOME (never a graph arg) via launcher_profile()."""
    launcher = launcher if launcher is not None else launcher_profile()
    profiles_dir = Path(profiles_dir) if profiles_dir is not None else profiles_root()
    errs = []
    def E(nid, msg):
        errs.append({"node": nid, "field": "profile", "msg": msg})
    for n in nodes or []:
        if not isinstance(n, dict) or "profile" not in n or kind(n).spawns is not True:
            continue  # non-agent profile keys die on the closed-key grammar
        nid = n.get("id")
        prof = n["profile"]
        if not isinstance(prof, str) or not prof.strip():
            E(nid, "profile must be a non-empty string (the teammate's profile DIRECTORY name)")
            continue
        if _PROFILE_NAME_BAD.search(prof):
            E(nid, f"profile {prof!r} is not a safe profile directory name "
                   "(no separators, no '..', no leading dot)")
            continue
        if prof == "default":
            E(nid, "profile 'default' is never a target — 'default' is only a legal launcher name")
            continue
        ph = profiles_dir / prof
        if not (ph / "config.yaml").is_file():
            E(nid, f"profile {prof!r} does not exist: no config.yaml at {ph / 'config.yaml'}")
            continue
        consent = jload(ph / "workflow_team.json")
        if consent is None:
            E(nid, f"profile {prof!r} has no consent file: write {ph / 'workflow_team.json'} "
                   f'with {{"accept_from": ["{launcher}"]}} from the TARGET side '
                   "(profile: is delegation, not isolation — the target must opt in)")
            continue
        accept_from = consent.get("accept_from") if isinstance(consent, dict) else None
        if not isinstance(accept_from, list):
            E(nid, f"profile {prof!r}: workflow_team.json needs an 'accept_from' list of "
                   f"launcher profile names; launcher {launcher!r} is not accepted")
        elif launcher not in accept_from:
            E(nid, f"profile {prof!r} does not accept launcher {launcher!r}: its "
                   f"workflow_team.json accept_from is {json.dumps(accept_from)} "
                   "(the TARGET profile must list its launcher — delegation, not isolation)")
    return errs

def requires_errors(nodes, parents=None):
    """1.1 (RATIFY F4) structural validation of `requires` on agent/gate nodes, called by
    validate_graph_errors for every typed agent/gate node that carries the key. Returns
    [{node, field, msg}]. Rules: value is an object `{"<ancestor>": ["field",
    "dotted.path", ...]}`; every key must be in the node's `after` closure (transitive;
    `parents` = {id: [direct parent ids]} — built from the nodes' own `after` lists when
    omitted); every entry of the path list a non-empty string (and the list non-empty).
    A graph that WANTS skip-on-missing uses the `when` gate + on_skip:prune, not requires."""
    errs = []
    def E(nid, field, msg):
        errs.append({"node": nid, "field": field, "msg": msg})
    nodes = [n for n in (nodes or []) if isinstance(n, dict)]
    idset = {n.get("id") for n in nodes if n.get("id")}
    if parents is None:
        parents = {n["id"]: [a for a in n.get("after", []) if a in idset]
                   for n in nodes if n.get("id")}
    else:
        # validate_graph_errors passes ALL nodes' ancestry, including echo nodes:
        # an echo between a producer and consumer is still part of the closure.
        idset.update(parents)
    for n in nodes:
        if "requires" not in n:
            continue
        nid = n.get("id")
        req = n["requires"]
        if not isinstance(req, dict) or not req:
            E(nid, "requires", "requires must be a non-empty object "
                               '{"<ancestor>": ["field", "dotted.path", ...]}')
            continue
        closure, stack = set(), list(parents.get(nid) or n.get("after") or [])
        while stack:
            a = stack.pop()
            if a in closure or a not in idset:
                continue
            closure.add(a)
            stack.extend(parents.get(a, []))
        for anc, paths in req.items():
            if not isinstance(anc, str) or anc not in closure:
                E(nid, "requires", f"requires key {anc!r} is not an ancestor in this node's "
                                   "`after` closure (requires is an input edge: only committed "
                                   "upstream outputs may be required)")
                continue
            if not isinstance(paths, list) or not paths:
                E(nid, f"requires.{anc}", f"requires[{anc!r}] must be a non-empty list of "
                                           "field path strings (\"field\" or \"dotted.path\")")
                continue
            for i, p in enumerate(paths):
                if not isinstance(p, str) or not p.strip():
                    E(nid, f"requires.{anc}", f"requires[{anc!r}][{i}] {p!r} must be a "
                                               "non-empty field path string")
    return errs

ID_OK = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

# Closed grammar (Q2): an unknown key is a REJECTED key, never a silently ignored one.
# `tier` is in the agent set because resolve_models stamps it at run/amend (test_tiers
# locks it) and the runner re-validates saved graphs — the door must never reject a
# graph it baked itself. `reasoning` is validated per node (Q5).
AGENT_KEYS = {"id", "type", "after", "goal", "context", "schema", "model", "provider", "toolsets",
              "max_turns", "timeout", "run_budget", "inputs", "fanout", "reasoning",
              # est-3acn: per-node RunBudget — budget:{"tokens": N}, a token cap
              # checked pre-dispatch at the child_metrics join seam. Distinct
              # currency from run_budget (wall-seconds); USD excluded per the
              # repo cost-trust law (issue #124 scope decision 2026-10-14).
              "budget",
              # est-fanout-cell (sys-96dcod): this agent is a fan-out's CONSOLIDATE
              # partner — the engine streams its batch children as the fan's items
              # finish (batch:1 = as-they-finish) and it commits a machine tally at
              # the wave boundary instead of a spawn. See CONSOLIDATE_KEYS.
              "consolidate",
              "tier", "shape", "repo", "bead_close",
              # est-bwv0h: door-time parameter seam — presence gate + conditional patch
              "where", "by",
              # #24/#25: fail-closed pinned routes. Default TRUE for nodes
              # that pin an explicit model — a submit ping that AFFIRMATIVELY proves
              # the pinned route dead or answering from the fallback ladder refuses the
              # launch; `false` = explicit opt-in to the ladder. (The door's
              # `route_verified` proof-annotation is door-baked AFTER validation; it
              # lives in the validator's set so the runner accepts the committed
              # graph, but _resolve_models drops any author/pre-submit value — only
              # this submit's ping (or a frozen committed restore) can prove a route.)
              "require_route", "route_verified",
              # #116: the engine's substitution stamp (from/to/reason/source) is
              # door-baked AFTER validation like route_verified — it lives here so the
              # runner accepts the committed graph; _resolve_models strips any
              # author-submitted value (only this submit's ping+config bake is trusted).
              "substrate_substituted",
              # 1.1 (RATIFY F2/F4): OPTIONAL team keys. `profile` = run this node AS a named
              # teammate profile (consent-gated, node-level only); `requires` = output
              # preconditions on ancestors ({"<ancestor>": ["field", "dotted.path", ...]}).
              # e68544a37be37657: `after_partial` (bool) opts this node INTO consuming a
              # harvest-on-death `partial` ancestor — a plain after-edge blocks on one
              # (typed fail at the wave boundary, never a silent release, never a spawn).
              "profile", "requires", "after_partial",
              # #130: `resume_hint` (bool; absent = auto when the durable work dir holds a
              # prior attempt's artifacts) — prompt-side dead-attempt inventory on re-runs.
              "resume_hint",
              # est-ij0: `order_only` (list, subset of `after`) marks ORDERING-only
              # predecessors (a convoy chain): no data flows over them, and a dead one
              # (failed, or never-runnable behind a failed data edge) is SPLICED out —
              # the node waits on the dead member's own order_only predecessors instead.
              "order_only",
              # est-2ek.1.603: publisher capability — an EXPLICIT declaration, never
              # inferred from prose. `publishes` (bool, agent/echo) says this node has
              # publication side effects; the runner REFUSES to start it until a
              # verified suite-proof token exists among its after-ancestors.
              # `suite_proof` (bool, agent/gate) declares a recognized suite-PROOF
              # producer: on commit done the runner mints the durable token
              # nodes/<id>.suite-proof.json (node id + committed efp).
              "publishes", "suite_proof",
              # Agents reuse the gate predicate grammar and skip policy.
              "when", "on_skip",
              # est-pygyy (issue #56): a publication node may DECLARE effect
              # receipts — postconditions the runner PROVES at the commit edge
              # before a done can stand. Absent = today's behavior byte-identical.
              "effects",
              # jam-h23: on-death catch. 'skip' commits the failed node `skipped`
              # (join-tolerant); '<fallback-node-id>' additionally lets that agent run.
              "on_fail",
              # est-2ek.1.164: transport fallback rungs — tried IN ORDER, only
              # when the same-model Q4 ladder exhausted with
              # error_class=transport_exhausted. List of non-empty strings
              # (<=8 rungs, billing guard); every other error class never
              # falls back. Validated by shape below; honored by
              # wf._fallback_ladder.
              "fallback_models",
              # est-ggqs: per-node clamp policy for a harvest-on-death `partial`
              # (HARVEST_POLICIES). 'strict' (= absent) keeps the after-edge block;
              # 'validate' commits a schema-valid, self-declared-complete harvest
              # `done` with the death kept visible (wf._harvest_clamp). Solo only.
              "harvest_policy",
              # hw-23c: author-tunable retry caps per ladder half ({transient,
              # bounded}, 0..8, default = today's law 2/1). Rides graph
              # `defaults` too; the per-run budget still caps every ladder.
              "retry",
              # #459 G3 (est-vp148): bounded rework cycle — GRAMMAR ACCEPTANCE ONLY.
              # `rework:{to, max_iters}` names the node a verdict can send work back
              # to, with a hard iteration bound. The engine has no loop-back edge yet
              # (wf/1 is strictly acyclic), so the door REFUSES any launch whose graph
              # carries this key (error_type engine_loop_not_wired, zero writes). The
              # slice that wires the loop replaces the refusal with the real thing;
              # until then a pack can express the shape and get a typed refusal, not a
              # silent degradation to exit-and-relaunch.
              "rework",
              # WF-GRAFT-1 (est-1f7cn): wake-noise tier for this node — `full` |
              # `quiet` | `silent`, overriding the graph-level `wake_tier` default.
              # It governs ONLY the notices whose subject is this node (its fan-out's
              # stragglers observation, this gate's own hold); run-terminal wakes
              # never consult it. Enforced in wf._wake_suppression; the law lives in
              # references/grammar.md.
              "wake_tier"}
RETRY_BOUNDS = {"transient": 8, "bounded": 8}   # hw-23c: author ceiling per half
RETRY_DEFAULTS = {"transient": 2, "bounded": 1}  # today's hardcoded law


def _retry_block_errors(v, field):
    """hw-23c: the ONE retry-block shape law, shared by the node key and the
    graph defaults key. {transient?: int 0..8, bounded?: int 0..8}, either half
    optional; 0 disables that ladder. Returns an error-list fragment."""
    errs = []
    def E(f, m):
        errs.append({"node": None, "field": f, "msg": m})
    if not isinstance(v, dict) or not v or set(v) - set(RETRY_BOUNDS):
        E(field, f"retry {v!r} invalid: an object with optional integer keys "
                 + json.dumps(sorted(RETRY_BOUNDS)) + ", each within its own bound "
                 + json.dumps(RETRY_BOUNDS) + " "
                 "(0 disables that ladder; transient = api_calls==0 re-spawns, "
                 "bounded = resume re-drive after a progressed death)")
        return errs
    for k in sorted(v):
        x = v[k]
        if type(x) is not int or x < 0 or x > RETRY_BOUNDS[k]:
            E(field, f"retry.{k} {x!r} invalid: must be an integer 0..{RETRY_BOUNDS[k]}")
    return errs


HARVEST_POLICIES = ("strict", "validate")   # est-ggqs: agent `harvest_policy` values
# WF-GRAFT-1 (est-1f7cn): the closed wake-noise tier ladder, legal at graph top level
# (the default) and on agent/gate nodes (the override). The ladder is ordered by how
# much the owner is NOT woken, and the floor is fixed by law, not by value:
#   full   — every notice whose subject is the node wakes (today's behavior).
#   quiet  — the node's mid-flight OBSERVATIONS (fanout.stragglers) are recorded in
#            the probe instead of waking; a hold and every run-terminal notice still
#            wake.
#   silent — every node-SUBJECT notice (including this gate's own hold, which the
#            author then owns polling) is recorded, not woken.
# No tier governs run.done / run.failed / run.partial — see wf._TIERED_WAKE_EVENTS.
WAKE_TIERS = ("full", "quiet", "silent")
GATE_KEYS = {"id", "type", "after", "question", "options", "context", "when", "wait", "on_skip",
             "default_option", "hold_timeout",
             # est-bwv0h: door-time parameter seam — presence gate + conditional patch
             "where", "by",
             # 1.1 (RATIFY F4): gates take output preconditions too; gates obey the same
             # after_partial law as agents (e68544a37be37657).
             "requires", "after_partial", "order_only",
             # est-2ek.1.603: a gate may DECLARE a suite-proof producer (the token is
             # minted on its committed answer); it can never declare itself a publisher.
             "suite_proof",
             # WF-GRAFT-1: a gate's own tier governs whether ITS hold wakes the owner
             # (`silent` = the author owns polling it; `quiet` deliberately does NOT
             # silence a hold — a held gate asks for an action, not an observation).
             "wake_tier"}
ECHO_KEYS = {"id", "type", "after", "output",
             # est-bwv0h: door-time parameter seam (see GATE_KEYS).
             "where", "by",
             # est-2ek.1.603: an echo commits at the wave boundary WITHOUT a spawn, so
             # the publisher gate covers the echo commit path too.
             "publishes",
             # est-pygyy (issue #56): the receipt gate covers the echo commit path
             # exactly like the publisher gate — an echo that publishes must prove it.
             "effects",
             # est-13z1: an echo commits its `output` verbatim at the wave boundary —
             # a close claim riding that object is read back from the store exactly
             # like an agent's, so the echo path carries the declaration too.
             "bead_close"}
# est-pygyy (issue #56): the closed shape of one effect-receipt row (validated at
# the door, enforced by wf._effect_gate at the runner's commit edge).
EFFECT_KEYS = {"file", "sha256", "min_bytes"}
# est-13z1: closed shape of a bead-close declaration — the estate store path,
# the bead id, and the actor the lane is entitled to close as (runner law:
# --actor must match the assignee read from the store).
BEAD_CLOSE_KEYS = {"store", "id", "actor"}
EXTERNAL_EFFECT_KEYS = {"target", "expected-readback"}

def external_effect_receipts(node, rec):
    """Validate committed publication evidence, never a license to redo its author.

    External adapters own the remote readback. The runner checks that each
    declaration has exactly one matching receipt, a literal true verdict and
    non-empty actual evidence equal to the declared expectation. Metrics are
    derived, not accepted as an alternate source of truth.
    """
    declared = [r for r in node.get("effects", []) if "target" in r]
    if not declared:
        return []
    out = (rec or {}).get("output")
    sources = [out]
    if node.get("fanout") and isinstance(out, dict):
        items = out.get("all_results")
        items = items if isinstance(items, list) else []
        sources = [item.get("output") for item in items
                   if isinstance(item, dict) and item.get("status") in ("done", "partial")]
    supplied = [r for source in sources if isinstance(source, dict)
                and isinstance(source.get("effect_receipts"), list)
                for r in source["effect_receipts"]]
    rows = []
    for row in declared:
        target, expected = row["target"], row["expected-readback"]
        matches = [r for r in supplied if isinstance(r, dict) and r.get("target") == target]
        receipt = matches[0] if len(matches) == 1 else {}
        actual = receipt.get("actual-readback")
        proven = (receipt.get("verdict") is True
                  and receipt.get("expected-readback") == expected
                  and isinstance(actual, str) and bool(actual.strip()) and actual == expected)
        rows.append({"target": target, "expected-readback": expected,
                     "actual-readback": actual, "verdict": proven})
    return rows
_HEX64_OK = re.compile(r"^[0-9a-f]{64}$")
# #485: a bare item token ({FIELD}, {item}, {index}) inside a bead_close id/actor
# template. Legal ONLY on a fan-out node, where the runner renders it per item
# with fmt_goal; on a solo/echo commit nothing resolves it, so the door refuses
# at admission instead of probing literal brace text at the commit edge.
# THE one definition (est-libks): wf.py re-bound the same pattern beside its
# commit-edge gate; the two literals drifting apart would let the door admit a
# template the gate then reads differently. wf.py now binds THIS object off the
# private instance, so door and commit edge agree by construction.
_RE_BC_ITEM_TOKEN = re.compile(r"\{[A-Za-z0-9_]+\}")
JOIN_KEYS = {"id", "type", "after", "keys", "wait"}
# ONE table for node kinds: closed key-set + the schedule hook. `spawns` is True
# (agent: spawn a child when ready), False (gate: hold at the wave boundary), None
# (echo: commit `output` verbatim at the boundary, zero tokens). Scheduling and
# validation consult NODE_TYPES, never a hardcoded type-literal tuple. The join
# kind (jam-h25, est-6ksu) is deliberately NOT a table row — it is the fourth
# boundary-commit kind with its own dedicated block (JOIN_KEYS closed set at the
# validator, join loop in wf.py); kind() gives it the non-spawning fallback, so
# it matches none of the True/False/None schedule branches.
NodeKind = namedtuple("NodeKind", "keys spawns")
NODE_TYPES = {"agent": NodeKind(AGENT_KEYS, True),
              "gate": NodeKind(GATE_KEYS, False),
              "echo": NodeKind(ECHO_KEYS, None)}

def kind(node):
    """A node's NodeKind, with the implicit-agent default for an untyped node and a
    non-spawning fallback for an unknown type (never raises — the read model must
    not crash on a legacy graph; the validator owns rejection)."""
    return NODE_TYPES.get(node.get("type", "agent")) or NodeKind(frozenset(), "none")
# 1.1 (RATIFY F5) opt-in provenance RETIRED by est-syim (#126, owner ruling 2026-10-15):
# the door's `save` stamps the block on EVERY save (DEFAULT-ON); `version` (plugin release)
# joins the key set and is always present. Pre-1.4 entries keep loading verbatim — no
# migration, no rewrites, golden-solo bytes unchanged. Top-level graph key.
PROVENANCE_KEYS = {"owner", "version", "source", "saved_at", "source_digest"}

# ---------- library entry normalizer (#50, R10 migration law) ----------
# A library file is either the 1.1 BARE form (the graph object itself — the bytes
# every pre-#50 `save` wrote, which must keep loading and listing verbatim) or the
# #50 ENVELOPE {"meta": {description?, tags?}, "graph": {...}}. Anything else is an
# UNKNOWN shape: the door QUARANTINES the entry — listed with a typed refusal reason,
# never a crash, never replayable (F-2 #62: one corrupt file must not take discovery,
# the typo nudge, or /wf down with it). Returns {meta, graph, description, tags,
# envelope} for a usable entry, or {"invalid": "invalid: <why>"} for a refused one —
# always a dict, callers key on the "invalid" marker / the presence of "graph".
# ---- Library tag grammar (#50/#70; est-qeul: SINGLE SOURCE for save AND read) ----
# The grammar moved here from the door so the SHARED read model can fold stored
# bytes through the exact grammar the save API enforced at write time; the door's
# _norm_tags delegates. Behavioral pins live in tests/test_library.py (L8x).
TAG_OK = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,31}$")
TAGS_MAX = 10
# #70 faceted tags: one colon, both sides non-empty charset-safe; the facet set is
# CLOSED (the only hard check in the feature); `note:` is the sanctioned escape hatch.
TAG_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*:[a-z0-9][a-z0-9._-]*$")
TAG_FACETS = ("domain", "note", "repo", "risk", "use_case")
TAG_LEN = 48
DESC_MAX = 200

def norm_tags(tags):
    """#50/#70: `tags` is a list of 1..TAGS_MAX tokens, each a legacy FLAT token
    (library-name grammar) or a FACETED `facet:value` tag — hard facet namespace
    {use_case,repo,domain,risk,note}. Returns (normalized, error): lowercase-
    trimmed, dups collapsed, exactly one ':' with [a-z0-9._-] both sides.
    Fail-closed: anything else is an error, never a silent drop."""
    if not isinstance(tags, list) or not 1 <= len(tags) <= TAGS_MAX:
        return None, f"tags must be a list of 1-{TAGS_MAX} tokens (flat or facet:value)"
    out = []
    for t in tags:
        if not isinstance(t, str):
            return None, f"invalid tag {t!r} (want a string)"
        t = t.strip().lower()
        if ":" in t:
            if len(t) > TAG_LEN or not TAG_RE.match(t):
                return None, (f"invalid tag {t[:60]!r} (facet:value — one ':', non-empty "
                              "both sides, [a-z0-9._-], <=48 chars)")
            if t.split(":", 1)[0] not in TAG_FACETS:
                return None, f"unknown facet in {t!r}; allowed facets: {list(TAG_FACETS)}"
        elif not TAG_OK.match(t):
            return None, f"invalid tag {t!r} (flat: lowercase alnum [-_.] <=32; or facet:value)"
        if t not in out:
            out.append(t)
    return out, None

def _normalize_tags_read(tags):
    """est-qeul: canonical READ fold for meta.tags. Each stored token goes through
    the save grammar (norm_tags, ONE ':' and all); a token invalid there is dropped
    from the read, never an error — quarantine (F-2 #62) is for whole-entry shape,
    and a single junk element must not poison the entry's discovery coverage any
    more than the old isinstance filter silently did. Empty list (or the deliberate
    erase `meta.tags: []`) survives untouched — the retain branch keys on presence
    and must still SEE the [] state (see act_save's #146 item 3)."""
    if not isinstance(tags, list):
        return []
    out, seen = [], set()
    for t in tags:
        if not isinstance(t, str):
            continue
        one, err = norm_tags([t])
        if err or not one:
            continue
        if one[0] not in seen:
            seen.add(one[0])
            out.append(one[0])
    return out

def library_entry(data):
    if not isinstance(data, dict):
        return {"invalid": "invalid: not a JSON object"}
    if isinstance(data.get("graph"), dict):
        if isinstance(data.get("nodes"), list):
            return {"invalid": "invalid: ambiguous file — carries both envelope"
                               " (graph:) and bare (nodes:) markers"}
        meta = data.get("meta")
        meta = meta if isinstance(meta, dict) else {}
        graph = data["graph"]
        envelope = True
    elif isinstance(data.get("nodes"), list):
        meta, graph, envelope = {}, data, False
    else:
        return {"invalid": "invalid: neither a bare graph (no nodes[] list)"
                           " nor a {meta, graph} envelope"}
    if not graph.get("nodes") or not isinstance(graph.get("nodes"), list):
        return {"invalid": "invalid: graph has no non-empty nodes[] list"}
    for i, n in enumerate(graph["nodes"]):
        if not isinstance(n, dict):
            return {"invalid": f"invalid: nodes[{i}] is not an object"}
    description = meta.get("description")
    if not isinstance(description, str) or not description.strip():
        description = graph.get("description")
    # est-qeul: CANONICAL READ NORMALIZER. meta.tags may carry raw bytes the save
    # API itself would never accept (pre-normalizer writes, file editors, other
    # publishers): duplicates, surrounding whitespace/newlines, uppercase. Those
    # raw shapes silently break every reader — tag_vocab counts a duplicate twice
    # for one entry, and a newline-suffixed row cannot be selected even by passing
    # its echoed token verbatim (query goes through the save grammar, stored side
    # never did). Fold each stored token through the SAME grammar save enforces;
    # a token that is invalid there is DROPPED from the read (never an error —
    # quarantine is for whole-entry shape, F-2 #62 — and the vocabulary/filter/
    # replay readers all read the normalized list). Deterministic order: the
    # normalizer dedups preserving first-seen order. The tag grammar lives HERE
    # in the shared module (est-qeul moved it out of the door; the door's
    # _norm_tags now delegates to norm_tags), so the read fold calls it
    # directly — no hook, and no dependency from the shared module back onto
    # the door's import graph.
    tags = _normalize_tags_read(meta.get("tags"))
    return {"meta": meta, "graph": graph, "description": description,
            "tags": tags, "envelope": envelope}
# #32 (publish-as-file): top-level `grammar` names the dialect a shared file was written
# in. Absent = "wf/1" (every pre-#32 file is a wf/1 file); unknown = fail-closed with the
# reader's supported list, so a newer dialect is refused honestly instead of misrun.
# An ANNOTATION like `provenance`: top-level only, never part of any node, so def_hash /
# efp / graph_fingerprint / source_digest are untouched (golden-solo bytes unchanged).
GRAMMAR_DEFAULT = "wf/1"
GRAMMAR_SUPPORTED = (GRAMMAR_DEFAULT,)

def grammar_errors(graph):
    """Return [{node:None, field:'grammar', msg}] for a top-level `grammar` value this
    reader cannot run; [] when absent (back-compat wf/1) or supported."""
    if not isinstance(graph, dict) or "grammar" not in graph:
        return []
    value = graph["grammar"]
    if not isinstance(value, str) or value not in GRAMMAR_SUPPORTED:
        return [{"node": None, "field": "grammar",
                 "msg": f"unsupported grammar {value!r}; this reader supports: "
                        + json.dumps(list(GRAMMAR_SUPPORTED))
                        + f" (absent = {GRAMMAR_DEFAULT!r})"}]
    return []
FANOUT_KEYS = {"items", "items_from", "goal", "schema", "quorum", "quorum_drain_s", "ledger"}
# ---------- est-fanout-cell (sys-96dcod): the [fan + consolidate] cell ----------
# Owner directive (2026-10-10): a standard unit pairing WITH the fan so operators
# get wave-consolidation — finished members flow into consolidation while the
# straggler tail is still out, instead of the whole aggregate waiting on it (the
# straggler wake says "smoke"; the cell is how you stop needing to). A consolidate
# node declares which fan-out it serves and how big a batch triggers a child:
CONSOLIDATE_KEYS = {"from", "batch"}


def consolidate_tally(n_items, finals, inflight=0, pending_batch=None):
    """The machine tally an agent-free consolidate node commits at the wave
    boundary (census law: every dispatched item is ANSWERED or NAMED missing,
    counted from records — the partner seat never gets to narrate coverage).

      finals      committed batch-final rows [{batch, items:[i...], status,
                  error_class?}, ...] read from consolidate/<fan>.jsonl
      inflight    batch children still running when the tally is taken
      pending_batch  indices collected for a batch that was never dispatched
                  (flushed at wave end; it rides the tally as named, honest,
                  never silently dropped)

    An item is consolidated iff a final with status done/partial covers it.
    Everything else is NAMED (never counted away). verdict:
      'consolidated'        all items covered, nothing open
      'consolidated_partial' some item not covered (named in `missing`)
    """
    pending_batch = pending_batch or []
    covered, batch_rows = set(), []
    for f in sorted(finals, key=lambda r: r.get("batch", 0)):
        its = [i for i in f.get("items", []) if isinstance(i, int)]
        ok = f.get("status") in ("done", "partial")
        batch_rows.append({"batch": f.get("batch"), "items": its,
                           "status": f.get("status"),
                           "error_class": f.get("error_class")})
        if ok:
            covered.update(its)
    missing = [i for i in range(n_items) if i not in covered]
    return {"dispatched": n_items,
            "consolidated": n_items - len(missing),
            "missing": missing,
            "open_inflight": inflight,
            "pending_batch": list(pending_batch),
            "batches": batch_rows,
            "verdict": ("consolidated" if not missing and not inflight
                        and not pending_batch else "consolidated_partial")}

# ---------- est-fanout-straggler (sys-nxrhxo): the census-earned cohort floor ----------
# Two independent offices measured their run shelves before picking the constant:
# 36 cohorts (tail-past-cohort-median: median 153s, p90 5,693s; 16/36 >5min) and
# 34 cohorts / 808 children at the >=10-item cut (p90 max/median 4.0, worst drag
# 3,183s). GRACE_S=180 sits above the healthy-median tail of BOTH censuses. It is
# an ENGINE CONSTANT, not an author knob (peer ruling: a threshold the timed party
# can tune is culture, not a sign); a `fanout.straggler_grace_s` key is promoted
# ONLY if a future census shows misfires — added at the census, not pre-hung.
STRAGGLER_GRACE_S = 180


def straggler_verdict(now_ts, finish_ts, launch_ts, durations_s, open_idx, *,
                      grace_s=STRAGGLER_GRACE_S):
    """The pure cohort math behind the fanout.stragglers wake — no clock, no
    filesystem, so the door and tests replay ledgers directly.

      finish_ts   wall-clock finish stamps of COMPLETED items (>=1 or no verdict)
      launch_ts   {index: spawn wall-clock} for still-open items (unlaunched/queued
                  items are scheduling, not squirreling — they get no verdict)
      durations_s completed items' own runtimes (seconds) — a legitimately slow
                  cohort raises the floor with itself: floor = max(grace, p50)
      open_idx    indices still open

    t0 is the COHORT MEDIAN finish, never wave start: fast lanes declare sooner and
    the constant self-calibrates per workflow shape. An item launched after t0 is
    measured from its launch. Requires >=1 completed and >=2 open — a majority
    already committed needs no smoke (quorum law); one lone long item is a timer
    problem, not a cohort-straggler problem. Returns None (silent) or a dict with
    the named counts the events.jsonl line carries."""
    import statistics
    if len(finish_ts) < 1 or len(open_idx) < 2:
        return None
    launched = [launch_ts[i] for i in open_idx if launch_ts.get(i) is not None]
    if not launched:
        return None                       # every open item is queued behind the cap:
        #                                    scheduling, not squirreling — no verdict
    t0 = statistics.median(finish_ts)
    floor_s = max(grace_s, statistics.median(durations_s) if durations_s else 0)
    # An item launched AFTER the cohort median is young: age it from its launch,
    # never from t0 (a late wave-2 item under the cap must not read as a straggler).
    oldest = max(now_ts - max(t0, lt) for lt in launched)
    if oldest <= floor_s:
        return None
    return {"cohort_median_ts": t0, "floor_s": round(floor_s, 1),
            "oldest_open_age_s": round(oldest, 1),
            "open": sorted(open_idx), "done": len(finish_ts),
            "items": len(finish_ts) + len(open_idx)}

DEFAULTS_KEYS = {"schema", "timeout", "max_turns", "reasoning", "provider", "model", "context",
                 "require_route",   # #25: bool — fail-closed pinned routes (see AGENT_KEYS)
                 # est-2ek.1.164: the transport fallback rungs fill from graph
                 # defaults like any defaults key (per-node key wins).
                 "fallback_models",
                 "retry"}   # hw-23c: the retry block rides defaults too (per-node key wins)
# Shape presets (sprint101 #11): max_turns/timeout per rough node shape = the p95 of
# SUCCESSFUL agent nodes per shape, measured 2026-09-25 over the run dirs behind
# census.json (80 runs, 219 committed-success agent nodes; shape classified from
# node id/goal keywords). The table IS the measured p95 per shape
# (max_turns / timeout_s); samples: recon 97, build 62, review 57,
# publish 3 (thin sample — keep an eye on it).
# Explicit per-node keys always win; the preset only fills what the author left unset.
SHAPE_PRESETS = {
    "recon":   {"max_turns": 100, "timeout": 2400},
    "build":   {"max_turns": 65,  "timeout": 1500},
    "review":  {"max_turns": 65,  "timeout": 2400},
    "publish": {"max_turns": 75,  "timeout": 1500},
}
DEFAULT_SHAPE = "build"

def _defaults_errors(d):
    """Per-key rules for a graph-level `defaults:` block — the SAME checks a node key
    gets (numbers in range, reasoning from the enum, schema subset, provider needs
    model, strings for model/context). Returns [{node:None, field, msg}]."""
    errs = []
    def E(field, msg):
        errs.append({"node": None, "field": field, "msg": msg})
    if not isinstance(d, dict):
        return [{"node": None, "field": "defaults", "msg": "defaults must be an object"}]
    for k in sorted(set(d) - DEFAULTS_KEYS):
        E(f"defaults.{k}", "unknown key; allowed: " + json.dumps(sorted(DEFAULTS_KEYS)))
    for k, hi in (("timeout", 86400), ("max_turns", 200)):
        v = d.get(k)
        if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool)
                              or v <= 0 or v > hi):
            # fb-validator-duo (2026-09-26): name the offending value AND the cap — the
            # old message cited only the cap, so 'max_turns 240 vs 200' read as a riddle.
            E(f"defaults.{k}", f"{k} {v!r} exceeds cap {hi} (must be a number in (0, {hi}])")
    if d.get("reasoning") is not None and d["reasoning"] not in reasoning_levels():
        E("defaults.reasoning", f"reasoning {d['reasoning']!r} invalid; allowed: "
                                f"{list(reasoning_levels())}")
    if d.get("schema") is not None and not isinstance(d["schema"], dict):
        E("defaults.schema", "schema must be an object")
    if d.get("model") is not None and not isinstance(d.get("model"), str):
        E("defaults.model", "model must be a string")
    if d.get("require_route") is not None and not isinstance(d.get("require_route"), bool):
        E("defaults.require_route", f"require_route {d['require_route']!r} must be a boolean")
    if "provider" in d:
        p = d.get("provider")
        if not isinstance(p, str) or not p or p.strip() != p:
            E("defaults.provider", "provider must be a non-empty provider id without "
                                   "surrounding whitespace")
        m = d.get("model")
        if not isinstance(m, str) or not m.strip():
            E("defaults.provider", "provider requires a non-empty model")
    if d.get("context") is not None and not isinstance(d["context"], str):
        E("defaults.context", "context must be a string")
    if "retry" in d:
        # hw-23c: same closed shape as the node key, shared through one helper.
        errs.extend(_retry_block_errors(d["retry"], "defaults.retry"))
    if "fallback_models" in d:
        # est-2ek.1.164: same closed shape as the node key — list of non-empty
        # strings, <=8 rungs — so a bad defaults value is named at the door,
        # never discovered at spawn.
        fm = d["fallback_models"]
        if not isinstance(fm, list) or not fm or not all(
                isinstance(x, str) and x.strip() for x in fm):
            E("defaults.fallback_models", "fallback_models must be a non-empty list "
                                          "of non-empty model-id strings")
        elif len(fm) > 8:
            E("defaults.fallback_models", f"fallback_models has {len(fm)} rungs; the "
                                          "billing cap is 8 (every rung is one extra "
                                          "spawn)")
    return errs

_PRE_RUN_REF = re.compile(r"\{run\.[^{}]*\}")

def _preamble_baked(pre, ctx):
    """est-2ek.1.838: is `pre` (defaults.context) already the head of `ctx`? The door
    bakes defaults BEFORE binding run_context, so a persisted node context carries the
    BOUND preamble while graph.json's defaults.context keeps the {run.KEY} template.
    Each {run.KEY} slot therefore matches any non-empty text; every literal byte of
    the preamble must still match exactly (an edited preamble is not 'baked')."""
    if ctx.startswith(pre):
        return True
    parts = _PRE_RUN_REF.split(pre)
    if len(parts) == 1:
        return False
    rx = "(?s:.+?)".join(re.escape(p) for p in parts)
    # the preamble is the WHOLE context or is followed by the "\n\n" own-context seam
    return re.match(rf"(?s){rx}(?:\n\n|\Z)", ctx) is not None

def apply_graph_defaults(graph):
    """Bake run-level `defaults` + per-node `shape` presets into the agent node defs,
    called by the door BEFORE graph.json is written: the runner and every fingerprint
    then see ONE resolved truth (no second resolution path to drift). Precedence:
    explicit node key > node shape preset > graph defaults. `defaults.context` is the
    shared preamble, prepended once to each agent node's own context. Returns the new
    graph (defaults key stays on it; it is idempotent to re-apply). Raises ValueError
    with the error list on invalid shape/defaults (door validates first; this is the
    runner's load-time net)."""
    graph = graph if isinstance(graph, dict) else {"nodes": graph}
    defaults = graph.get("defaults") or {}
    errs = _defaults_errors(defaults)
    if errs:
        raise ValueError(json.dumps(errs))
    nodes = []
    for n in graph.get("nodes", []):
        n = dict(n)
        if kind(n).spawns is True:   # implicit-agent default; unknown types skip (validator owns them)
            if n.get("shape") is not None and n["shape"] not in SHAPE_PRESETS:
                raise ValueError(json.dumps(
                    [{"node": n.get("id"), "field": "shape",
                      "msg": f"shape {n['shape']!r} invalid; allowed: "
                             f"{sorted(SHAPE_PRESETS)}"}]))
            preset = SHAPE_PRESETS.get(n.get("shape", DEFAULT_SHAPE), {})
            for k in ("max_turns", "timeout"):
                if n.get(k) is None:
                    # precedence: an author-named shape preset is per-node
                    # intent and wins; otherwise graph defaults fill, with the
                    # DEFAULT_SHAPE preset as the floor ([D] smart default).
                    v = preset.get(k) if n.get("shape") is not None else \
                        defaults.get(k, preset.get(k))
                    if v is not None:
                        n[k] = v
            for k in ("schema", "reasoning", "provider", "model"):
                if n.get(k) is None and defaults.get(k) is not None:
                    n[k] = defaults[k]
            # est-2ek.1.164: the fallback rungs fill like any defaults key
            # (per-node key wins; a COPY so mutation never touches defaults).
            if n.get("fallback_models") is None and defaults.get("fallback_models") is not None:
                n["fallback_models"] = list(defaults["fallback_models"])
            # hw-23c: the retry block fills like any defaults key (per-node key
            # wins; a COPY so mutation never touches defaults).
            if n.get("retry") is None and defaults.get("retry") is not None:
                n["retry"] = dict(defaults["retry"])
            # #25: `require_route` fills from defaults like any defaults key, but is
            # NEVER baked when both author and defaults left it unset — the runner
            # treats absent as the effective default (True on pinned nodes). Baking
            # it would move solo user-visible bytes on every graph.json.
            if n.get("require_route") is None and "require_route" in defaults:
                n["require_route"] = defaults["require_route"]
            pre = defaults.get("context") or ""
            if pre and not _preamble_baked(pre, str(n.get("context") or "")):
                n["context"] = pre + ("\n\n" + n["context"] if n.get("context") else "")
        nodes.append(n)
    return dict(graph, nodes=nodes)

REASONING_FALLBACK = ("none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra")
REASONING_IMPORT_PATH = [None]  # "hermes_constants" | "fallback-literal" — which tuple validated

def reasoning_levels():
    """Canonical reasoning set: ('none',) + hermes_constants.VALID_REASONING_EFFORTS.
    Import INSIDE the function (the module must import on hosts without core on
    sys.path); on import failure fall back to that exact tuple literal and log one
    line. REASONING_IMPORT_PATH records which path served (the test reports it)."""
    try:
        import hermes_constants
        allowed = ("none",) + tuple(hermes_constants.VALID_REASONING_EFFORTS)
        REASONING_IMPORT_PATH[0] = "hermes_constants"
    except Exception:
        allowed = REASONING_FALLBACK
        REASONING_IMPORT_PATH[0] = "fallback-literal"
        print("wfcommon: hermes_constants import failed — using the reasoning-level "
              "fallback literal (keep it in sync with core).", file=sys.stderr)
    return allowed

def budget_shape_errors(b, where):
    """est-3acn RunBudget shape — `budget:{"tokens": N}` and nothing else, at the
    graph root and on agent nodes alike. USD is deliberately NOT a key: the cap
    is tokens-only per the repo cost-trust law (issue #124 scope decision
    2026-10-14) — an author who writes budget.usd is REFUSED, never ignored
    (a silently dropped cap key is a cap the author believes is on)."""
    errs = []
    if not isinstance(b, dict):
        return [f"budget at {where} must be an object like {{\"tokens\": 200000}}"]
    for k in sorted(set(b) - {"tokens"}):
        errs.append(f"budget.{k} at {where} unknown; RunBudget is a tokens-only cap "
                    f"(USD is excluded per the repo cost-trust law)")
    if "tokens" not in b:
        if not errs:
            # an unknown key (the usd shape) already names the tokens-only law —
            # stacking a second complaint over it is noise on the SAME defect.
            errs.append(f"budget at {where} must carry a positive integer tokens cap")
    elif type(b["tokens"]) is not int or b["tokens"] <= 0:
        errs.append(f"budget.tokens at {where} must be a positive integer "
                    f"(got {b['tokens']!r})")
    return errs


def validate_graph_errors(nodes, *, admission=False):
    """Return a LIST of {node, field, msg} — EVERY defect, not the first. Strict ids:
    node ids double as filenames.
    #32: accepts a whole graph object too — `{grammar?, nodes:[...]}` — in which case
    the top-level `grammar` tag is checked first (absent = wf/1; unknown = refused,
    listing the supported values) and then its `nodes`. A bare node list is unchanged.
    admission=True: NEW-run admission rules (submit door only — _validation_error /
    validate_graph_full). Persisted graphs re-validated at gate release or runner
    restart pass the default False and are never stranded by them (PR #280 R10)."""
    errs = []
    if isinstance(nodes, dict):
        errs.extend(grammar_errors(nodes))
        nodes = nodes.get("nodes")
    def E(nid, field, msg):
        errs.append({"node": nid, "field": field, "msg": msg})

    def schema_check(nid, field, schema):
        # The runner's validator consumes exactly this subset; description is
        # prompt-only annotation; enum is ENFORCED at harvest (#107 — the old
        # law "never accept a constraint that cannot be enforced" stays TRUE:
        # enum joined the subset only once wf.validate() checks membership).
        # #96: minItems/minLength joined the SAME way — wf.validate() now
        # enforces both (array length; non-blank char count via v.strip(),
        # fail-closed: a whitespace-only probe is not a probe), so the door
        # admits exactly what the harvester can enforce, nothing more.
        # #472: maxItems joins as minItems' mirror (wf.validate enforces the
        # cap), and enum widens onto type:'boolean'/'integer' — membership in
        # those closed domains is trivial at harvest. A refusal now also NAMES
        # the fallback: an unenforceable cap belongs in the description prose,
        # advisory by admission, never silently dropped.
        for k in sorted(set(schema) - {"type", "required", "properties", "items",
                                       "description", "enum", "minItems", "minLength",
                                       "maxItems"}):
            E(nid, f"{field}.{k}", "unsupported schema keyword; supported: type, required, properties, items, description, enum, minItems, minLength, maxItems — a constraint the harvester cannot enforce here (integer bounds, patterns) belongs in the description prose: an advisory ceiling beats a silent pass")
        if "type" in schema and schema["type"] not in ("object", "array", "string", "number", "integer", "boolean"):
            E(nid, f"{field}.type", "unsupported schema type")
        if "required" in schema and (not isinstance(schema["required"], list)
                                      or not all(isinstance(x, str) for x in schema["required"])):
            E(nid, f"{field}.required", "required must be a list of strings")
        if "enum" in schema:
            # #107: closed vocabulary. #472 widened placement from string-only
            # to the three closed domains whose membership wf.validate()
            # enforces trivially: type:'string' (non-empty string members),
            # type:'boolean' (boolean members — the bug-fix playbook's
            # `reproduced: enum[true,false]` gate-key is the repro), and
            # type:'integer' (integer members; bool is never a member — the
            # same bool law as #113). Still REJECTED on a type-less, number-,
            # array- or object-typed schema: the closed-subset law refuses it
            # at the door instead of shipping a dead keyword.
            en = schema["enum"]
            _et = schema.get("type")
            if not isinstance(en, list) or not en:
                E(nid, f"{field}.enum", "enum must be a non-empty list of non-empty strings")
            elif _et == "string" and not all(isinstance(x, str) and x for x in en):
                E(nid, f"{field}.enum", "enum members must be a list of non-empty strings")
            elif _et == "boolean" and not all(isinstance(x, bool) for x in en):
                E(nid, f"{field}.enum", "enum members must be a list of booleans")
            elif _et == "integer" and not all(isinstance(x, int) and not isinstance(x, bool) for x in en):
                E(nid, f"{field}.enum", "enum members must be a list of integers (bool is never a member)")
            elif _et not in ("string", "boolean", "integer"):
                E(nid, f"{field}.enum", "enum is only legal on type:'string', 'boolean' or 'integer' (the harvester enforces membership there only)")
        # #96: minItems/minLength — same enforceability law as enum. The door
        # admits a floor ONLY where wf.validate() can actually enforce it:
        # minItems on type:'array', minLength on type:'string', each a
        # non-negative int (bool is never a count — same bool law as #113).
        # #472: maxItems joins as minItems' mirror — type:'array', the same
        # non-negative int shape, enforced as a cap at harvest.
        for _kw, _t in (("minItems", "array"), ("minLength", "string"), ("maxItems", "array")):
            if _kw in schema:
                mv = schema[_kw]
                if isinstance(mv, bool) or not isinstance(mv, int) or mv < 0:
                    E(nid, f"{field}.{_kw}", f"{_kw} must be a non-negative integer")
                elif schema.get("type") != _t:
                    E(nid, f"{field}.{_kw}", f"{_kw} is only legal on type:'{_t}' (the harvester enforces it only there)")
        # #472: an unsatisfiable interval is a dead constraint — with both
        # bounds otherwise well-formed, minItems > maxItems admits no value at
        # all, so the door refuses the pair instead of shipping a schema that
        # can only ever fail the harvest.
        _mi, _ma = schema.get("minItems"), schema.get("maxItems")
        if (isinstance(_mi, int) and not isinstance(_mi, bool) and _mi >= 0
                and isinstance(_ma, int) and not isinstance(_ma, bool) and _ma >= 0
                and _mi > _ma):
            E(nid, f"{field}.maxItems", f"unsatisfiable range: minItems {_mi} > maxItems {_ma}")
        props = schema.get("properties")
        if props is not None:
            if not isinstance(props, dict):
                E(nid, f"{field}.properties", "properties must be an object")
            else:
                for k, sub in props.items():
                    if not isinstance(sub, dict):
                        E(nid, f"{field}.properties.{k}", "property schema must be an object")
                    else:
                        schema_check(nid, f"{field}.properties.{k}", sub)
        if "items" in schema:
            if not isinstance(schema["items"], dict):
                E(nid, f"{field}.items", "items schema must be an object")
            else:
                schema_check(nid, f"{field}.items", schema["items"])

    if not isinstance(nodes, list) or not nodes:
        return errs + [{"node": None, "field": "nodes", "msg": "nodes must be a non-empty list"}]
    bad_el = [i for i, n in enumerate(nodes) if not isinstance(n, dict)]
    if bad_el:
        return [{"node": None, "field": f"nodes[{bad_el[0]}]",
                 "msg": f"nodes[{bad_el[0]}] is not an object"}]
    ids = [n.get("id") for n in nodes]
    if not all(ids):
        E(None, "nodes", "missing node ids")
    if len(set(ids)) != len(ids):
        dupes = sorted({i for i in ids if ids.count(i) > 1}, key=str)
        E(None, "nodes", f"duplicate node ids: {dupes}")
    if not all(ids):
        return errs  # per-node checks below need real ids
    for i in ids:
        if not isinstance(i, str) or not ID_OK.match(i):
            E(i, "id", f"invalid node id {i!r} (alnum start, [A-Za-z0-9_.-], <=64)")
        if not isinstance(i, str):
            return errs  # non-string ids cannot key the ancestry maps safely
    idset = set(ids)
    parents = {n["id"]: [a for a in n.get("after", []) if a in idset] for n in nodes}
    byid_of = {n["id"]: n for n in nodes if isinstance(n.get("id"), str)}   # jam-h23: on_fail target lookup
    for n in nodes:
        nid = n["id"]
        t = n.get("type")
        if t == "join":                                  # jam-h25: dedicated kind
            _type_keys = JOIN_KEYS
        elif t in NODE_TYPES:
            _type_keys = NODE_TYPES[t].keys  # the ONE table (est-voip); no second dict
        else:
            _type_keys = None
            E(nid, "type", "type must be agent|gate|echo")
            # NO continue: type-INDEPENDENT defects (after refs, numeric bounds,
            # ids) still surface below in one pass; only the per-type key grammar
            # (undefined without a type) is skipped via _type_keys below.
        if _type_keys is not None:
            for k in sorted(set(n) - _type_keys):
                # dedicated errors below own these keys (clearer messages, no double-report)
                if t == "agent" and k == "wait":
                    continue
                if t == "gate" and k == "inputs":
                    continue
                if k in ("after_partial", "order_only"):
                    continue  # the dedicated blocks below name the key (echo-meaningless / type)
                E(nid, k, "unknown key; allowed: " + json.dumps(sorted(_type_keys)))
        for a in n.get("after", []):
            if a not in idset:
                E(nid, "after", f"references unknown 'after': {a}")
        if "fallback_models" in n:
            # est-2ek.1.164: closed shape — a list of non-empty strings, <=8
            # (billing guard: every rung is ONE extra spawn sharing the run's
            # retry budget; an unbounded list is an unbounded bill). A bare
            # string or a non-string element is refused HERE, never discovered
            # at spawn.
            fm = n["fallback_models"]
            if not isinstance(fm, list) or not fm or not all(
                    isinstance(x, str) and x.strip() for x in fm):
                E(nid, "fallback_models", "fallback_models must be a non-empty list "
                                          "of non-empty model-id strings")
            elif len(fm) > 8:
                E(nid, "fallback_models", f"fallback_models has {len(fm)} rungs; the "
                                          "billing cap is 8 (every rung is one extra "
                                          "spawn)")
        if "toolsets" in n:
            # spool key c258f0730346b426 (from est-2ek.1.142): the field passed
            # the closed-set validator with NO shape check, so a dict/int/nested
            # element survived to spawn and detonated inside _filter_child_toolsets
            # (the reported PathLike/TypeError death). Refuse every non-List[str]
            # / comma-string shape at the door, naming node+key+value; the
            # honored shapes stay exactly the runner's (list of strings,
            # comma-string). #427 amends the tail: a declared-EMPTY selection
            # is now REFUSED too — -t cannot express zero toolsets (the core
            # collapses an empty flag value to "use config defaults") and an
            # omitted -t BROADENS to the seat's defaults, so honoring [] failed
            # the author's minimisation silently OPEN (boundary probe: 4
            # declared tools -> 41 defaults).
            ts = n["toolsets"]
            shape_ok = (isinstance(ts, str)
                        or (isinstance(ts, list)
                            and all(isinstance(x, str) and x.strip() for x in ts)))
            if not shape_ok:
                E(nid, "toolsets", f"toolsets {ts!r} invalid: must be a list of "
                                    "non-empty toolset-name strings (or a comma-"
                                    "separated string)")
            elif (isinstance(ts, list) and not ts) or (
                    isinstance(ts, str) and not any(
                        s.strip() for s in ts.split(","))):
                # issue #427: declared but EMPTY ([] / empty/whitespace/comma-
                # only string) means "no tools" — inexpressible at -t, and the
                # only available spelling (omission) silently broadens.
                E(nid, "toolsets", f"toolsets {ts!r} declares an EMPTY selection "
                                    "the runner cannot honor: an omitted -t "
                                    "BROADENS to the seat's default toolsets. "
                                    "Name at least one toolset, or use "
                                    'toolsets "none" to opt out explicitly '
                                    "(flag omitted, seat defaults apply).")
        if "after_partial" in n:
            # e68544a37be37657: agent/gate-only key; bool only — an unvalidated
            # truthy is never enough to open a harvest edge. Echo rejects it
            # explicitly (its closed set also flags it unknown) so the error
            # NAMES the key, per the issue's validation contract.
            if t == "echo":
                E(nid, "after_partial", "after_partial is meaningless on echo "
                                        "nodes (agent/gate only)")
            elif not isinstance(n["after_partial"], bool):
                E(nid, "after_partial", "after_partial must be a boolean "
                                        "(true = this node consumes a partial ancestor's harvest)")
        if "wake_tier" in n:
            # WF-GRAFT-1 (est-1f7cn): closed value set (agent/gate only — an echo
            # carries no node-subject notice, so the closed key set already refuses it
            # by name above). A tier is policy over WAKES only; it never touches
            # scheduling, and it can never govern a run-terminal wake.
            wt = n["wake_tier"]
            if not isinstance(wt, str) or wt not in WAKE_TIERS:
                E(nid, "wake_tier", f"wake_tier {wt!r} invalid: must be one of "
                                    + json.dumps(list(WAKE_TIERS))
                                    + " (full = every notice about this node wakes; "
                                      "quiet = its fan-out straggler observation "
                                      "records without waking; silent = its "
                                      "node-subject notices record, including this "
                                      "gate's own hold)")
        if "harvest_policy" in n:
            # est-ggqs: closed value set; a fan-out aggregate already merges harvest
            # credit (partial items count done), so the key means nothing there —
            # refused by name instead of silently ignored.
            hp = n["harvest_policy"]
            if not isinstance(hp, str) or hp not in HARVEST_POLICIES:
                E(nid, "harvest_policy", f"harvest_policy {hp!r} invalid: must be one of "
                                         + json.dumps(list(HARVEST_POLICIES))
                                         + " (strict = a harvest-on-death partial blocks plain "
                                           "after-edges; validate = a schema-valid, "
                                           "self-declared-complete harvest commits done)")
            elif n.get("fanout"):
                E(nid, "harvest_policy", "harvest_policy is meaningless on a fan-out node "
                                         "(the aggregate already merges harvested items as "
                                         "done credit); set it on solo agent nodes")
        if "retry" in n:
            # hw-23c: node-level retry budget, defaults cascade already applied;
            # the same closed shape the defaults key gets (shared helper).
            for e in _retry_block_errors(n["retry"], "retry"):
                e["node"] = nid
                errs.append(e)
        if "rework" in n:
            # #459 G3 spike: shape-only acceptance; the door refuses the LAUNCH
            # (act_run/act_amend) with error_type engine_loop_not_wired until the
            # loop slice lands. A malformed value is refused here, by name —
            # a graph cannot even reach the launch gate carrying junk.
            rw = n["rework"]
            if t != "agent":
                E(nid, "rework", "rework is agent-only (gate/echo/join cannot loop work back)")
            elif not isinstance(rw, dict) or set(rw) - {"to", "max_iters"} or "to" not in rw:
                E(nid, "rework", "rework must be {to, max_iters?} — `to` names the node "
                                 "the verdict sends work back to")
            else:
                tgt = rw.get("to")
                if not isinstance(tgt, str) or tgt not in idset:
                    E(nid, "rework.to", f"rework.to {tgt!r} must name an existing node id")
                elif tgt == nid:
                    E(nid, "rework.to", "rework.to must name a DIFFERENT node (self-loops "
                                        "are the retry ladder's job, not a cycle's)")
                if "max_iters" in rw:
                    mi = rw["max_iters"]
                    if not isinstance(mi, int) or isinstance(mi, bool) or not (1 <= mi <= 8):
                        E(nid, "rework.max_iters", "rework.max_iters must be an integer in [1, 8] "
                                                   "(a bounded loop, not a spin)")
                if n.get("fanout"):
                    E(nid, "rework", "rework is not defined on a fan-out node (the loop "
                                     "targets the fan; set it on the solo agent)")
        if "order_only" in n:
            # est-ij0: ordering-only edges are a declared SUBSET of after — the cycle,
            # topo and downstream machinery keep reading `after` unchanged.
            oo = n["order_only"]
            if t == "echo":
                E(nid, "order_only", "order_only is meaningless on echo nodes (agent/gate only)")
            elif not isinstance(oo, list) or not all(isinstance(x, str) for x in oo):
                E(nid, "order_only", "order_only must be a list of node ids (a subset of after)")
            else:
                stray = [x for x in oo if x not in (n.get("after") or [])]
                if stray:
                    E(nid, "order_only", f"order_only ids must also appear in after; not in after: {stray}")
        if "publishes" in n:
            # est-2ek.1.603: publisher capability is an EXPLICIT declaration — the
            # validator never infers side effects from prose. agent/echo only (a gate
            # can never publish); bool only — an unvalidated truthy never opens the gate.
            if t == "gate":
                E(nid, "publishes", "publishes is meaningless on gate nodes "
                                   "(agent/echo only — a gate has no side effects to declare)")
            elif not isinstance(n["publishes"], bool):
                E(nid, "publishes", "publishes must be a boolean "
                                    "(true = this node has publication side effects; the "
                                    "runner refuses it until a verified suite proof token exists)")
        if "suite_proof" in n:
            # est-2ek.1.603: recognized suite-PROOF producer (agent/gate only; echo
            # rejects it). On commit done the runner mints nodes/<id>.suite-proof.json.
            if t == "echo":
                E(nid, "suite_proof", "suite_proof is meaningless on echo nodes "
                                      "(agent/gate only — an echo proves nothing)")
            elif not isinstance(n["suite_proof"], bool):
                E(nid, "suite_proof", "suite_proof must be a boolean "
                                      "(true = committing done mints a suite proof token)")
        if "effects" in n:
            # est-pygyy (issue #56): declared effect receipts — the postconditions
            # the runner PROVES at the commit edge before a done may stand. The
            # engine never infers effects from prose; a declared receipt that cannot
            # be proven fails the node typed (error_class effect_receipt), so the
            # door validates the SHAPE fail-closed here: non-empty list <=8 rows,
            # each local {file, sha256?, min_bytes?} or external
            # {target, expected-readback}. External adapters record remote
            # evidence in output.effect_receipts; a false/absent receipt blocks
            # finalize without invalidating the committed artifact author.
            # Local paths are run-dir relative; escaping paths are refused here.
            ef = n["effects"]
            if not isinstance(ef, list) or not ef:
                E(nid, "effects", "effects must be a non-empty list of effect-receipt "
                                  "objects {file, sha256?, min_bytes?}")
            elif len(ef) > 8:
                E(nid, "effects", f"effects has {len(ef)} rows; the cap is 8")
            else:
                for j, row in enumerate(ef):
                    fld = f"effects[{j}]"
                    if not isinstance(row, dict):
                        E(nid, fld, "each effect receipt must be an object "
                                    "{file, sha256?, min_bytes?}")
                        continue
                    allowed = EXTERNAL_EFFECT_KEYS if "target" in row else EFFECT_KEYS
                    stray = sorted(set(row) - allowed)
                    if stray:
                        E(nid, fld, f"{fld} unknown key(s) {stray}; allowed: "
                                    + ", ".join(sorted(allowed)))
                    if "target" in row:
                        for key in sorted(EXTERNAL_EFFECT_KEYS):
                            value = row.get(key)
                            if not isinstance(value, str) or not value.strip():
                                E(nid, fld, f"{fld} {key} must be a non-empty string")
                        continue
                    f_ = row.get("file")
                    if not isinstance(f_, str) or not f_.strip():
                        E(nid, fld, f"{fld} file must be a non-empty string")
                    elif f_.startswith("/") or "\\" in f_ or ".." in f_.split("/"):
                        E(nid, fld, f"{fld} file {f_!r} invalid: must be a run-dir "
                                    "relative path (no absolute path, no '..')")
                    if "sha256" in row and not (isinstance(row["sha256"], str)
                                                and _HEX64_OK.match(row["sha256"])):
                        E(nid, fld, f"{fld} sha256 must be a 64-hex digest")
                    mb = row.get("min_bytes")
                    if mb is not None and (not isinstance(mb, int) or isinstance(mb, bool)
                                           or mb <= 0):
                        E(nid, fld, f"{fld} min_bytes must be a positive integer")
        if "bead_close" in n:
            # est-13z1: declared bead-close — the runner READS BACK the estate store
            # (`bd -C <store> show <id> --json`) at the commit edge whenever this
            # node's committed output claims the bead closed; an unproven claim
            # fails typed (error_class bead_close). The door validates the SHAPE
            # fail-closed: an object {store, id, actor?} (closed keys), store/id
            # non-empty strings, id with no whitespace (it rides the fixed argv).
            bc = n["bead_close"]
            if not isinstance(bc, dict):
                E(nid, "bead_close", "bead_close must be an object "
                                     "{store, id, actor?}")
            else:
                stray = sorted(set(bc) - BEAD_CLOSE_KEYS)
                if stray:
                    E(nid, "bead_close", f"bead_close unknown key(s) {stray}; allowed: "
                                         "store, id, actor")
                st_ = bc.get("store")
                if not isinstance(st_, str) or not st_.strip():
                    E(nid, "bead_close", "bead_close.store must be a non-empty string")
                elif "\x00" in st_ or "\n" in st_:
                    E(nid, "bead_close", "bead_close.store must be a plain path "
                                         "(no NUL/newline)")
                bi_ = bc.get("id")
                if not isinstance(bi_, str) or not bi_.strip():
                    E(nid, "bead_close", "bead_close.id must be a non-empty string")
                elif any(c.isspace() for c in bi_):
                    E(nid, "bead_close", "bead_close.id must contain no whitespace "
                                         "(it rides the store read-back argv)")
                if "actor" in bc and (not isinstance(bc["actor"], str)
                                       or not bc["actor"].strip()):
                    E(nid, "bead_close", "bead_close.actor, when present, must be a "
                                         "non-empty string")
                if not n.get("fanout") and any(
                        _RE_BC_ITEM_TOKEN.search(str(bc.get(k) or ""))
                        for k in ("id", "actor") if isinstance(bc.get(k), str)):
                    E(nid, "bead_close", "bead_close id/actor item-token interpolation "
                                         "({FIELD}) requires a fan-out node — nothing "
                                         "resolves the token on a solo/echo commit "
                                         "(drop the braces or fan the node out)")
        for k, hi in (("timeout", 86400), ("max_turns", 200), ("run_budget", 86400)):
            v = n.get(k)
            if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v <= 0 or v > hi):
                # fb-validator-duo (2026-09-26): value AND cap in every numeric-bound
                # rejection; the parenthetical is kept for substring-matchers.
                E(nid, k, f"{k} {v!r} exceeds cap {hi} (must be a number in (0, {hi}])")
        if n.get("budget") is not None:
            # est-3acn: per-node RunBudget — tokens-only, positive integer.
            for m_ in budget_shape_errors(n["budget"], f"node {nid}"):
                E(nid, "budget", m_)
        if n.get("reasoning") is not None:
            lv = n["reasoning"]
            if lv not in reasoning_levels():
                E(nid, "reasoning", f"reasoning {lv!r} invalid; allowed: "
                                    f"{list(reasoning_levels())}")
        if t == "gate" and n.get("options") is not None:
            opts = n["options"]
            if not isinstance(opts, list) or not opts \
                    or not all(isinstance(o, str) and o.strip() for o in opts):
                E(nid, "options", "gate options must be a non-empty list of non-empty strings "
                                  "(or omit the key for a free-form answer)")
        if t == "gate":
            # sprint101 #14: gate defaults are validated at the door, never discovered at the wall.
            dopt = n.get("default_option")
            if dopt is not None:
                opts = n.get("options")
                if not isinstance(dopt, str) or not dopt.strip():
                    E(nid, "default_option", "default_option must be a non-empty string")
                elif not isinstance(opts, list) or dopt not in opts:
                    E(nid, "default_option", f"default_option {dopt!r} must be one of the gate's options")
                if n.get("wait") is not None:
                    E(nid, "default_option", "default_option applies to human gates, not machine wait-gates")
            hto = n.get("hold_timeout")
            if hto is not None and (not isinstance(hto, (int, float)) or isinstance(hto, bool) or hto <= 0):
                E(nid, "hold_timeout", "hold_timeout must be a positive number of seconds")
            # #59 (fb-fix ledger 97e90c2205f17fb0): string-typed gate keys are
            # TYPE-checked at submit, never discovered at the wall — a dict/list
            # question reached the held-card and desktop as a React child crash, and
            # a non-str context reached the event line and downstream concat. The
            # optional keys stay OPTIONAL (absent or '' legal) but a key that is
            # PRESENT must be a str — explicit null is a present non-str, rejected
            # like the rest (ra-59 review finding 2: string-when-present contract).
            if "question" in n and not isinstance(n["question"], str):
                E(nid, "question", f"question {n['question']!r} must be a string")
            if "context" in n and not isinstance(n["context"], str):
                E(nid, "context", f"context {n['context']!r} must be a string")
        if t == "agent":
            # #59 (fb-fix ledger 97e90c2205f17fb0): the string-typed agent keys
            # are TYPE-checked at submit — a list/dict `context` or non-str `goal`
            # passed the truthy-only check and died at FIRST spawn in run_child's
            # prompt concat ('TypeError: can only concatenate str', error_class
            # 'crashed', burned gate release). Mirror the fanout item-goal law
            # (:592): named-node errors, no coercion at resolve (closed-grammar law,
            # :274). PRESENT non-str (incl. [], {}, 0, False, None) is rejected
            # independently of truthiness — the ra-59 review found the first cut
            # still let falsy goals through to a created run dir. ABSENT goal keeps
            # the exact legacy 'agent node has no goal' message below; a present
            # str that is empty/whitespace keeps that legacy message too (absent-
            # equivalent), preserving test_validate_0923's pin.
            if "goal" in n:
                g = n["goal"]
                if not isinstance(g, str):
                    E(nid, "goal", f"goal {g!r} must be a non-empty string")
                elif n.get("fanout") is None and not g.strip():
                    # empty/whitespace-only str on a PLAIN agent is an absent goal:
                    # '' keeps the exact legacy message (test_validate_0923 pin);
                    # whitespace-only is the same non-empty law. A fan-out node's
                    # own goal may legally stay empty when fo.goal or every item
                    # carries the real text (spawn: `fo.get("goal") or n.goal`).
                    E(nid, "goal", "agent node has no goal" if not g
                                   else f"goal {g!r} must be a non-empty string")
            ctx = n.get("context")
            if "context" in n and not isinstance(ctx, str):
                # explicit null is a present non-str: the string-when-present
                # contract admits only absent or str (review finding 2)
                E(nid, "context", f"context {ctx!r} must be a string")
            shp = n.get("shape")
            if shp is not None and shp not in SHAPE_PRESETS:
                E(nid, "shape", f"shape {shp!r} invalid; allowed: {sorted(SHAPE_PRESETS)}")
            rp = n.get("repo")
            # deep review #29 F3: a mistyped `repo` must never silently disable the
            # lane-clean gate it opts into (123/["x"]/{...}/"" all read as no-declaration
            # downstream and the node commits `done` over a dirty lane, fail-open).
            if rp is not None and (not isinstance(rp, str) or not rp.strip()
                                   or rp.strip() != rp):
                E(nid, "repo", "repo must be a non-empty path string without surrounding whitespace")
            if n.get("model") is not None and not isinstance(n.get("model"), str):
                E(nid, "model", "model must be a string")
            if "provider" in n:
                provider = n.get("provider")
                if not isinstance(provider, str) or not provider or provider.strip() != provider:
                    E(nid, "provider", "provider must be a non-empty provider id without surrounding whitespace")
                model = n.get("model")
                if not isinstance(model, str) or not model.strip():
                    E(nid, "provider", "provider requires a non-empty model")
            fo = n.get("fanout")
            if fo is not None:
                if not isinstance(fo, dict):
                    E(nid, "fanout", "fanout must be an object")
                else:
                    for k in sorted(set(fo) - FANOUT_KEYS):
                        E(nid, f"fanout.{k}", "unknown key; allowed: "
                                              + json.dumps(sorted(FANOUT_KEYS)))
                    if fo.get("items") is None and not fo.get("items_from"):
                        E(nid, "fanout", "fanout needs items or items_from")
                    elif fo.get("items") is None and not isinstance(fo.get("items_from"), str):
                        E(nid, "fanout.items_from", "fanout.items_from must be a "
                                                    "'<node_id>.<dotted.path>' string")
                    if fo.get("items") is not None and not isinstance(fo["items"], list):
                        E(nid, "fanout.items", "fanout.items must be a list")
                    # est-wr0p (vacuous-replay class of est-077y): a baked literal
                    # items:[] passed the door and replayed with ZERO children —
                    # a vacuous pass, not a fan-out. The runtime fails it typed
                    # fanout_empty; the door must refuse the shape at submit so
                    # every consumer is safe, not just the QM admission gate.
                    # items_from stays the dynamic path — its count is unknown
                    # at admit and is never tripped here.
                    # R10: SUBMIT door only — a persisted graph whose empty node was
                    # pruned/skipped must still release and restart.
                    if admission and isinstance(fo.get("items"), list) and not fo["items"]:
                        E(nid, "fanout.items",
                          "fanout.items must be a non-empty literal or use items_from")
                    items = fo.get("items") if isinstance(fo.get("items"), list) else []
                    for i, it in enumerate(items):
                        if isinstance(it, dict) and "goal" in it and not (isinstance(it["goal"], str) and it["goal"].strip()):
                            E(nid, f"fanout.items[{i}].goal", f"fanout.items[{i}].goal must be a non-empty string (it overrides fanout.goal)")
                    # #59: the goal TEMPLATE is rendered by fmt_goal (re.sub over the
                    # text) and prefixed onto the node goal per item — a truthy
                    # non-str here is the same first-spawn TypeError class as the
                    # item-goal law directly above; mirror that style (R8 sibling).
                    # A PRESENT key must be a str regardless of truthiness; '' /
                    # absent remain the documented "fall back to the node goal"
                    # shape (run_child: `fo.get("goal") or node.get("goal")`).
                    if "goal" in fo and not isinstance(fo["goal"], str):
                        E(nid, "fanout.goal", f"fanout.goal {fo['goal']!r} must be a "
                                              "non-empty string (the item goal template)")
                    if items and not (fo.get("goal") or n.get("goal")) \
                            and not all(isinstance(it, dict) and isinstance(it.get("goal"), str) for it in items):
                        E(nid, "fanout.goal", "fanout needs a goal template or a goal on every item")
                    # (moved above: the template type check sits with the other
                    # fanout checks; this block only reports the missing-goal law)
                    q = fo.get("quorum")
                    if q is not None and (not isinstance(q, int) or isinstance(q, bool) or q < 1):
                        E(nid, "fanout.quorum", "fanout.quorum must be a positive int")
                    # est-06xk (#123): drain-with-deadline is an OPT-IN modifier of
                    # the quorum race — a number of seconds >= 0 (0 = today's
                    # immediate SIGKILL at the quorum moment). bool is not a number
                    # here (True would silently mean 1s); without an explicit quorum
                    # there is no race and nothing to drain — refuse, never ignore.
                    ds = fo.get("quorum_drain_s")
                    if ds is not None:
                        if isinstance(ds, bool) or not isinstance(ds, (int, float)) or not ds >= 0:
                            E(nid, "fanout.quorum_drain_s",
                              "fanout.quorum_drain_s must be a number >= 0 (drain-window "
                              "seconds; 0 = cancel stragglers immediately at the quorum moment)")
                        elif q is None:
                            E(nid, "fanout.quorum_drain_s",
                              "fanout.quorum_drain_s is only meaningful with an explicit "
                              "fanout.quorum (without quorum the fan-out waits for every item)")
                    # #85: the input LEDGER is a list of per-item declarations; row
                    # CONTENT (source/artifact shape) is measured by the admission
                    # guard below the door — here only the container shape is a
                    # validator defect, so an undeclared-input graph stays
                    # VALIDATOR-CLEAN and the refusal can only come from the guard.
                    led = fo.get("ledger")
                    if led is not None and not isinstance(led, list):
                        E(nid, "fanout.ledger", "fanout.ledger must be a list of input-ledger "
                                                "rows (one per fan-out item: a '<node_id>.<dotted.path>' "
                                                "string or {\"source\": ..., \"artifact\": {\"file\": ..., "
                                                "\"sha256\": <hex>?}})")
                    if fo.get("items") is None and isinstance(fo.get("items_from"), str):
                        head = fo["items_from"].split(".")[0]
                        if head == nid or head not in idset or head not in n.get("after", []):
                            E(nid, "fanout.items_from",
                              f"fanout.items_from ref {fo['items_from']!r} head {head!r} must be an "
                              f"ancestor — a node in this node's `after` list "
                              f"(its committed output feeds the fan-out)")
                    fsc = fo.get("schema")
                    if fsc is not None:
                        if not isinstance(fsc, dict):
                            E(nid, "fanout.schema", "fanout.schema must be an object")
                        else:
                            schema_check(nid, "fanout.schema", fsc)
            # est-fanout-cell (sys-96dcod): the [fan + consolidate] cell's shape.
            # A consolidate node is an ordinary agent that ALSO declares which
            # fan-out it consolidates — the pairing is the machine-checked edge,
            # never prose. Cross-node laws (from names a real fan-out, the edge
            # exists, exactly one partner per fan) sit after the node loop.
            con = n.get("consolidate")
            if con is not None:
                if not isinstance(con, dict):
                    E(nid, "consolidate", "consolidate must be an object "
                                          "(keys: " + json.dumps(sorted(CONSOLIDATE_KEYS)) + ")")
                else:
                    for k in sorted(set(con) - CONSOLIDATE_KEYS):
                        E(nid, f"consolidate.{k}", "unknown key; allowed: "
                                                  + json.dumps(sorted(CONSOLIDATE_KEYS)))
                    _f = con.get("from")
                    if not isinstance(_f, str) or not _f.strip():
                        E(nid, "consolidate.from", "consolidate.from must name the "
                                                   "fan-out node this node consolidates")
                    elif _f == nid:
                        E(nid, "consolidate.from", "consolidate.from cannot name itself")
                    _b = con.get("batch")
                    if _b is not None and (isinstance(_b, bool) or not isinstance(_b, int) or _b < 1):
                        E(nid, "consolidate.batch", "consolidate.batch must be a positive "
                                                    "int (1 = consolidate as items finish)")
                    if fo is not None:
                        E(nid, "consolidate", "one node is either the fan or its "
                                              "consolidate partner, never both")
                    if n.get("fanout") is None and "goal" not in n:
                        # batch children are spawned with this node's goal as their
                        # template — the cell's seat still needs a mission.
                        E(nid, "goal", "consolidate node has no goal (it templates its batch children)")
            elif fo is None and "goal" not in n:
                # (the consolidate branch above owns the goal law for partner
                # nodes; this legacy branch keeps its exact pre-cell semantics:
                # ABSENT goal on a non-fan-out, non-consolidate agent, message
                # pinned by test_validate_0923 / test_string_type_validation_59)
                E(nid, "goal", "agent node has no goal")
            if n.get("require_route") is not None and not isinstance(n.get("require_route"), bool):
                # #25: boolean only — an unknown truthy value must not silently
                # disable (or enable) the fail-closed route gate.
                E(nid, "require_route", f"require_route {n['require_route']!r} must be a boolean")
            if n.get("resume_hint") is not None and not isinstance(n.get("resume_hint"), bool):
                E(nid, "resume_hint", f"resume_hint {n['resume_hint']!r} must be a boolean")   # #130
            if n.get("schema") is not None:
                if not isinstance(n["schema"], dict):
                    E(nid, "schema", "schema must be an object")
                else:
                    schema_check(nid, "schema", n["schema"])
        if t == "echo":
            # #59: echo commits `output` VERBATIM (the documented contract). The
            # shapes downstream consumes are JSON values (fixtures and examples
            # author dicts/lists — a str-only law would kill the feature), so the
            # submit law is JSON-typed: anything json.dumps cannot encode (a set,
            # a custom object) crashes the door's own graph.json write or the
            # runner's node commit as an unclassified crash — refuse it here,
            # named-node, like every other defect.
            o = n.get("output")
            try:
                json.dumps(o)
            except (TypeError, ValueError):
                E(nid, "output", f"echo output {o!r} must be a JSON-serialisable "
                                 f"value ({type(o).__name__} cannot be committed verbatim)")
        osk = n.get("on_skip")
        if osk is not None and n.get("type") in ("gate", "agent"):   # est-l2ey: gate|agent (echo/join: closed-key check already rejected it)
            if osk not in ("pass", "prune"):
                E(nid, "on_skip", f"on_skip {osk!r} invalid; allowed: ['pass', 'prune']")
            elif n.get("when") is None:
                E(nid, "on_skip", "on_skip needs a `when` (nothing else can skip the node)")
        ofk = n.get("on_fail")   # jam-h23: agent-only (closed key set rejected others);
        if ofk is not None:      # non-'skip' value must name an existing, non-ancestor AGENT
            anc, stack = set(), list(n.get("after", []))
            while stack:
                a = stack.pop()
                if a in anc or a not in idset:
                    continue
                anc.add(a); stack.extend(parents[a])
            if ofk != "skip":
                tgt = byid_of.get(ofk) if isinstance(ofk, str) else None
                if not isinstance(ofk, str) or tgt is None:
                    E(nid, "on_fail", f"on_fail {ofk!r} must be 'skip' or an existing node id")
                elif tgt.get("type") != "agent":
                    E(nid, "on_fail", f"on_fail target {ofk!r} must be an agent node")
                elif ofk in anc or ofk == nid:
                    E(nid, "on_fail", f"on_fail target {ofk!r} must not be an ancestor of {nid}")
        w = n.get("wait")
        if w is not None:
            if t == "join":                              # jam-h25: join wait
                if w not in ("terminal", "any"):
                    E(nid, "wait", f"join wait {w!r} invalid; allowed: ['terminal', 'any']")
            elif t == "gate":
                for e in wait_spec_ok(w):
                    field = "wait" if e["field"] is None else f"wait.{e['field']}"
                    E(nid, field, f"gate node wait: {e['msg']}")
            else:
                E(nid, "wait", "only gate nodes take wait")   # agent: dedicated error pinned by test_validate_0923
        anc = set()
        if (n.get("when") is not None and t in ("gate", "agent")) or n.get("inputs") is not None:   # est-l2ey: agents take `when` too
            stack = list(n.get("after", []))
            while stack:
                a = stack.pop()
                if a in anc or a not in idset:
                    continue
                anc.add(a)
                stack.extend(parents[a])
        ks = n.get("keys")
        if ks is None and t == "join":
            E(nid, "keys", "join node needs keys: {label: '<node_id>.<dotted.path>', ...}")
        if ks is not None:   # join-only (closed key set rejected it elsewhere)
            if t != "join":
                E(nid, "keys", "only join nodes take keys")
            elif not isinstance(ks, dict) or not ks:
                E(nid, "keys", "join keys must be a non-empty object {label: '<node_id>.<dotted.path>'}")
            else:
                anc = set(); stack = list(n.get("after", []))
                while stack:
                    a = stack.pop()
                    if a in anc or a not in idset:
                        continue
                    anc.add(a); stack.extend(parents[a])
                for label, ref in ks.items():
                    if not isinstance(label, str) or not label.strip():
                        E(nid, "keys", f"join key label {label!r} must be a non-empty string")
                    if not isinstance(ref, str) or not ref.strip():
                        E(nid, f"keys.{label}", "join key ref must be a non-empty '<node_id>.<dotted.path>' string")
                        continue
                    head = ref.split(".")[0]
                    if head not in anc:
                        E(nid, f"keys.{label}", f"join key ref {ref!r} head {head!r} is not an "
                                                f"existing node in its `after` ancestry")

        if n.get("when") is not None and t in ("gate", "agent"):   # est-l2ey: gate|agent
            if not isinstance(n["when"], str):   # a non-str when would crash the tokenizer — name it instead
                E(nid, "when", "when must be a string (bounded expression)")
            else:
                err = when_expr_ok(n["when"])
                if err:
                    E(nid, "when", err)   # parse-only (syntax mode is total): NO head check on a broken expr
                else:
                    # `when` reads out.<ancestor>.<path> (references/grammar.md). Parse-only
                    # when_expr_ok cannot see heads (sentinel operands), so a non-ancestor
                    # head — sibling, typo, ghost — would validate clean and resolve to
                    # None at fire time: False ⇒ silent skip (default on_skip:prune = dead
                    # branch), True ⇒ holds, depending on unrelated commit order. Validate
                    # the ref heads against the SAME ancestry closure `inputs` uses below.
                    for tok in _tok_when(n["when"]):
                        if tok.startswith("out."):
                            head = tok.split(".")[1]
                            if head not in anc:
                                E(nid, "when", f"when ref {tok!r} head {head!r} is not an existing "
                                               f"node in its `after` ancestry (when must descend from it)")
        ins = n.get("inputs")
        if ins is not None:
            if t == "gate":
                E(nid, "inputs", "gates cannot have inputs")
            elif not isinstance(ins, list) or not all(isinstance(x, str) and x.strip() for x in ins):
                E(nid, "inputs", "inputs must be a list of non-empty ref strings")
            else:
                for ref in ins:
                    head = ref.split(".")[0]
                    if head not in anc:
                        E(nid, "inputs", f"inputs ref {ref!r} head {head!r} is not an existing "
                                         f"node in its `after` ancestry (inputs must descend from it)")
    # 1.1 (RATIFY F4): output preconditions — structural check, ancestry via `parents`.
    # Only typed agent/gate nodes reach here (echo's closed key set already rejected it).
    errs.extend(requires_errors([n for n in nodes if NODE_TYPES.get(n.get("type"))
                                 and NODE_TYPES[n["type"]].spawns is not None], parents))
    indeg = {i: 0 for i in idset}
    kids = {i: [] for i in idset}
    for n in nodes:
        for a in n.get("after", []):
            if a not in idset:
                continue  # unknown after already reported; can't key the topo maps
            indeg[n["id"]] += 1
            kids[a].append(n["id"])
    queue, seen = [i for i, d in indeg.items() if d == 0], 0
    while queue:
        x = queue.pop(); seen += 1
        for k in kids[x]:
            indeg[k] -= 1
            if indeg[k] == 0:
                queue.append(k)
    if seen != len(idset):
        stuck = sorted(i for i in idset if indeg[i] > 0)
        E(None, "after", f"cycle in graph (nodes still waiting on each other: {stuck})")
    # est-fanout-cell (sys-96dcod): the cell's CROSS-node laws — the pairing is a
    # machine-checked edge, never prose. `from` must name a real fan-out node that
    # is a DIRECT `after` parent of the partner (the same data-edge law as
    # fanout.items_from's head: a consolidate consumes its fan's committed wave);
    # and exactly ONE partner per fan (two consumers of one wave = two ledgers).
    _fan_seen = {}
    for n in nodes:
        if not isinstance(n, dict):
            continue
        con = n.get("consolidate")
        if not isinstance(con, dict):
            continue
        nid = n.get("id")
        _f = con.get("from")
        if isinstance(_f, str) and _f and _f != nid:
            tgt = byid_of.get(_f)
            if tgt is None:
                E(nid, "consolidate.from", f"consolidate.from {_f!r} names no node")
            elif not isinstance(tgt.get("fanout"), dict):
                E(nid, "consolidate.from", f"consolidate.from {_f!r} is not a fan-out node")
            elif _f not in (n.get("after") or []):
                E(nid, "consolidate.from", f"consolidate.from {_f!r} must be in this "
                                           f"node's `after` list (the cell is an edge)")
            else:
                if _f in _fan_seen:
                    E(nid, "consolidate.from", f"fan-out {_f!r} already has consolidate "
                                               f"partner {_fan_seen[_f]!r}; one partner per wave")
                _fan_seen[_f] = nid
    return errs

def validate_graph(nodes):
    """Old signature — the FIRST error as a string ("node X: ...") or None. Callers
    that want every defect use validate_graph_errors; the runner (wf.py) keeps this."""
    errs = validate_graph_errors(nodes)
    if not errs:
        return None
    e = errs[0]
    return (e["msg"] if e["node"] is None else f"node {e['node']}: {e['msg']}")

# ---------- #85: artifact-admission guard — input ledgers map 1:1 to sources ----------
# The class (WOFS W1f evidence-loss, cluster spool aaca9fe5a15f5f2b): a fan-out item
# consumed an artifact another item's source actually covered — a disposition ledger
# had silently lost 85/95 rationales and nothing at admission could tell the runner.
# `fanout.ledger` is the ITEM-INDEXED input ledger: one row per item, positionally
# aligned; a row is a plain '<node_id>.<dotted.path>' input-ref string or
# {"source": "<node_id>.<dotted.path>", "artifact": {"file": rel/path, "sha256"?}}
# naming the on-disk artifact the item will consume. This guard runs at the door
# BEFORE any write/spawn and refuses, fail-closed, unless rows map 1:1 to items:
#   * a row without an item ("row N has no item"), or an item (by position) whose
#     row is absent ("item N is missing ledger source ...");
#   * two items sharing one source (sources are consumed at most once);
#   * a source head outside the node's transitive `after` ancestry (same data-edge
#     law as `inputs` refs — only committed upstream outputs may feed the fan-out);
#   * a declared artifact whose run-dir file is absent or whose sha256 disagrees
#     (measured ONLY when run_dir is given — run creation has no run dir yet;
#     amend/validate-with-dir measure the bytes).
# Graphs WITHOUT a ledger declaration are byte-unchanged (no ledger, no guard;
# golden-solo EMPTY-diff law). Row CONTAINER shape (non-list ledger) is a
# validator defect; everything else about a row is measured here, so the RED
# graph (an item with no ledger row) stays VALIDATOR-CLEAN and can only be
# refused by this guard — that split is pinned by tests/test_admission_ledger_85.py.

_SHA_OK = re.compile(r"\A[0-9a-fA-F]{64}\Z")

def _ledger_item_label(item, i):
    if isinstance(item, dict):
        for k in ("key", "id", "name", "file", "source"):
            v = item.get(k)
            if isinstance(v, str) and v.strip():
                return repr(v)
        return f"{json.dumps(item, ensure_ascii=False)[:80]}"
    return f"(index {i})"

def admission_ledger_errors(graph, run_dir=None):
    """[{node, field:'fanout.ledger', msg}] — see the #85 block comment. Structural
    laws always; artifact file/sha laws only when run_dir is provided (the bytes
    must exist at MEASUREMENT time; run creation has none yet)."""
    errs = []
    def E(nid, msg):
        errs.append({"node": nid, "field": "fanout.ledger", "msg": msg})
    if not isinstance(graph, dict):
        return errs
    nodes = [n for n in (graph.get("nodes") or []) if isinstance(n, dict)]
    idset = {n.get("id") for n in nodes if isinstance(n.get("id"), str)}
    # The guard runs BEFORE the node validator, so malformed `after` values (int,
    # dict, str...) must never crash ancestry building — the validator reports the
    # defect; we treat a non-list `after` as no known parents (same law as the
    # ledger container shape above).
    def _after_list(n):
        a = n.get("after")
        return a if isinstance(a, list) else []
    parents = {n["id"]: [a for a in _after_list(n) if isinstance(a, str) and a in idset]
               for n in nodes if isinstance(n.get("id"), str)}
    for n in nodes:
        nid = n.get("id")
        fo = n.get("fanout")
        if not isinstance(fo, dict):
            continue
        led = fo.get("ledger")
        if led is None:
            continue                       # no ledger, no guard (scope law)
        if not isinstance(led, list):
            continue                       # container shape is the validator's defect
        items = fo.get("items")
        static = isinstance(items, list)   # items_from: count unknown at admit
        if static:
            for i in range(len(led) - 1, len(items) - 1, -1) if len(led) > len(items) else ():
                E(nid, f"ledger row {i} has no item — rows must map 1:1 to "
                       f"fanout.items ({len(items)} items, {len(led)} rows)")
            for i, item in enumerate(items):
                if isinstance(item, dict) and i >= len(led):
                    E(nid, f"item {i} {_ledger_item_label(item, i)} is missing ledger source "
                           f"(fanout.ledger row {i} is absent — every item's declared input "
                           f"must have exactly one ledger row)")
        seen = {}                          # source ref -> first row index
        for i, row in enumerate(led):
            if isinstance(row, str):
                src = row
                art = None
            elif isinstance(row, dict):
                src = row.get("source")
                art = row.get("artifact")
            else:
                E(nid, f"ledger row {i} {json.dumps(row, ensure_ascii=False)[:80]} must be a "
                       f"'<node_id>.<dotted.path>' string or an object "
                       f'{{"source": ..., "artifact": {{"file": ..., "sha256"?}}}}')
                continue
            if not isinstance(src, str) or not src.strip() or src.strip() != src \
                    or "." not in src or not src.split(".", 1)[0] or not src.split(".", 1)[1]:
                E(nid, f"ledger row {i} has an empty or malformed source "
                       f"{src!r}: a source is '<node_id>.<dotted.path>'")
                continue
            head = src.split(".", 1)[0]
            closure, stack = set(), list(parents.get(nid) or _after_list(n))
            while stack:
                a = stack.pop()
                if a in closure or a not in idset:
                    continue
                closure.add(a)
                stack.extend(parents.get(a, []))
            if head not in closure:
                E(nid, f"ledger row {i} source {src!r} head {head!r} is not an ancestor of "
                       f"node {nid!r} (not in its `after` ancestry — only committed "
                       f"upstream outputs may feed the fan-out)")
                continue
            if src in seen:
                E(nid, f"item {i} is missing ledger source: {src!r} is already declared by "
                       f"ledger row {seen[src]} — sources must map 1:1, an item may not "
                       f"consume another item's source")
                continue
            seen[src] = i
            if art is None:
                continue
            if not isinstance(art, dict) or not art:
                E(nid, f"ledger row {i} artifact must be an object "
                       f'{{"file": rel/path, "sha256"?}}, got {json.dumps(art, ensure_ascii=False)[:80]}')
                continue
            bad_keys = sorted(set(art) - {"file", "sha256"})
            if bad_keys:
                E(nid, f"ledger row {i} artifact has unknown key(s) {bad_keys}; "
                       f"allowed: [file, sha256]")
            f_, sha_ = art.get("file"), art.get("sha256")
            if not isinstance(f_, str) or not f_.strip() or os.path.isabs(f_):
                E(nid, f"ledger row {i} artifact.file {f_!r} must be a non-empty relative "
                       f"path under the run dir")
                continue
            if sha_ is not None and (not isinstance(sha_, str) or not _SHA_OK.match(sha_)):
                E(nid, f"ledger row {i} artifact.sha256 {sha_!r} must be a 64-char hex digest")
                sha_ = None
            if run_dir is None:
                continue                   # bytes unmeasurable without a run dir
            base = Path(run_dir).resolve()
            target = (base / f_).resolve()
            try:
                inside = target != base and str(target).startswith(str(base) + os.sep)
            except (OSError, ValueError):
                inside = False
            if not inside:
                E(nid, f"ledger row {i} artifact.file {f_!r} escapes the run dir")
                continue
            if not target.is_file():
                E(nid, f"item {i} is missing source artifact '{f_}': the declared input "
                       f"file does not exist in the run dir")
                continue
            if sha_ is not None:
                h = hashlib.sha256()
                try:
                    with target.open("rb") as fh:
                        for chunk in iter(lambda: fh.read(1 << 20), b""):
                            h.update(chunk)
                except OSError as exc:
                    E(nid, f"ledger row {i} artifact '{f_}' cannot be read: {exc}")
                    continue
                if h.hexdigest().lower() != sha_.lower():
                    E(nid, f"ledger row {i} artifact '{f_}' sha256 mismatch: on disk "
                           f"{h.hexdigest()}, declared {sha_} — the declared bytes and the "
                           f"run-dir bytes are not the same artifact")
    return errs

# ---------- full structural graph validation (shared: door + include door) ----------
# PR#84 review F-2: the door's `_validation_error` and the include resolver's
# shelf check were TWO validators with different strictness — a shelf carrying an
# unknown graph key, invalid defaults, a malformed model_policy or provenance was
# refused when submitted directly and silently accepted when included, because the
# lossy top-level projection in the expansion pass drops those keys before the
# fused graph ever reaches the door validator. ONE validator now serves both
# doors: the structural (graph-level) half lives here, next to the node half it
# composes with, and the stdlib-only core stays independent of the door (the door
# delegates here; this module never imports the door).

STRUCTURAL_GRAPH_KEYS = {"name", "nodes", "description", "defaults", "model_policy",
                         "provenance", "grammar", "include",
                         # est-bwv0h: `params` — the typed launch contract (declaration
                         # in the row; the graph IS its manifest). Supersedes the #126
                         # run_context contract for graphs that carry it; co-declaration
                         # is refused (one contract per graph).
                         "params",
                         "concurrency", "item_concurrency",  # #100: optional run-level limits
                         # est-3acn: RunBudget — an optional run-wide token cap
                         # ({"tokens": N}); enforced pre-dispatch, graceful
                         # budget_exhausted partial. USD is deliberately NOT a
                         # key: token-only per the repo cost-trust law
                         # (issue #124 scope decision 2026-10-14).
                         "budget",
                         # est-2ek.1.856 deep review D2: the advertised per-run
                         # seat-cap opt-out ("graph max_seats: 0") is a REAL key —
                         # accepted here, validated by value below, baked into
                         # run.json by the door; the runner's _max_seats honours it.
                         "max_seats",
                         # est-2ek.1.166: the version handshake — an author may
                         # declare the minimum plugin version the graph needs;
                         # a stale runner refuses LOUDLY at arm time, naming both.
                         "requires_plugin",
                         # est-2ek.1.158: the node whose output IS the run verdict.
                         "result", "on_exit",
                         # WF-GRAFT-1 (est-1f7cn): the graph-wide wake-noise DEFAULT
                         # ("full" | "quiet" | "silent"); a node's own `wake_tier`
                         # overrides it. Validated by value below; honored by
                         # wf._wake_suppression at the notify seam.
                         "wake_tier",
                         # #126 (est-syim): machine-readable {run.KEY} binding contract —
                         # {required?: [ids], defaults?: {id: non-empty str}}; shape
                         # validated here (run_context_contract_errors), enforced at run
                         # launch BEFORE any run write (typed missing-key refusal).
                         "run_context"}

# ---------- #509: door UX on unknown-key refusals (grammar-layer registry) ----------
# A bare `unknown graph key; allowed: [...]` made a key that EXISTS in a known
# grammar layer (a newer plugin, a different layer) read the same as pure junk:
# the wave-9 shelf audit paid three round-trips diagnosing `params` on a 1.3.9
# door whose files on disk were 1.4.0. Three asks, implemented together:
#   (a) every graph_invalid refusal banner names the door's own version;
#   (b) a refused key WITH A ROW HERE is named as a grammar-layer key with its
#       floor, pointing at the requires_plugin handshake; when the plugin file
#       on disk and the running process diverge (the reload boundary that born
#       the audit), the note says so explicitly;
#   (c) a near-miss key gets the difflib-mearest accepted suggestion.
# RELEASE LAW (AGENTS 4d): a new top-level graph key adds a row HERE with the
# release that first accepts it; the test gate refuses a STRUCTURAL_GRAPH_KEYS
# member with no row. Values are the first TAG at which acceptance is PROVEN
# (tag-probe of this set / its predecessors) — never earlier, so the floor a
# stale door prints is upgrade-sufficient, not a promise the named release
# might still refuse. Re-derive: probe STRUCTURAL_GRAPH_KEYS membership at
# every `git tag` (tests/test_door_ux_509.py cites the recipe).
GRAPH_KEY_MIN_VERSION = {
    "name": "0.9.0", "nodes": "0.9.0", "description": "0.9.0",
    "defaults": "1.0.1",
    "grammar": "1.1.1",
    "concurrency": "1.1.4", "item_concurrency": "1.1.4",
    "include": "1.3.0", "model_policy": "1.3.0", "provenance": "1.3.0",
    "requires_plugin": "1.3.1",
    "result": "1.3.2",
    "on_exit": "1.3.7",
    "budget": "1.3.9", "max_seats": "1.3.9",
    "params": "1.4.0", "run_context": "1.4.0",
    # WF-GRAFT-1: wake-noise tiers. Floor is the next feature release (never earlier
    # than proven — a 1.3.x door genuinely refuses this key, so the hint a stale door
    # prints stays upgrade-sufficient).
    "wake_tier": "1.4.0",
}

# The truth of THIS module — what this process loaded. plugin_version() above
# reads plugin.yaml FROM DISK: file truth. On the reload boundary the two split
# (newer files, older process) and the door must not silently pass file-truth
# off as its own. Re-pinned at release in the SAME commit as plugin.yaml
# (AGENTS 4d; a test gate compares them at HEAD).
CODE_VERSION = "1.3.9"

def code_version():
    return CODE_VERSION

def _padded_eq_len(a, b):
    if a is None or b is None:
        return a, b
    n = max(len(a), len(b))
    return a + (0,) * (n - len(a)), b + (0,) * (n - len(b))

def door_reload_note():
    """#509(b): the reload-boundary signature. plugin.yaml (file truth) vs
    CODE_VERSION (process truth) — equal or unreadable = no note. Divergent =
    the exact shape that turns 'the docs promise this key' into a bare
    allowed-list: name it so a cold reader reloads instead of archaeologizing."""
    file_v = plugin_version()
    if not file_v or not CODE_VERSION or file_v == CODE_VERSION:
        return ""
    fa, fb = _padded_eq_len(version_tuple(file_v), version_tuple(CODE_VERSION))
    rel = ("newer" if (fa and fb and fa > fb) else
           "older" if (fa and fb and fa < fb) else "divergent")
    return (f"RELOAD BOUNDARY: the plugin file on disk is {file_v} but this "
            f"process runs {CODE_VERSION} (file is {rel}) — restart/reload the "
            "process that serves the plugin; keys from the file's version are "
            "not live in this door")

def unknown_graph_key_msg(key, allowed):
    """The full refusal text for ONE unknown top-level graph key (#509). The
    legacy prefix stays byte-identical (`unknown graph key; allowed: [...]`) —
    pinned by existing tests; hints append after it, each honest about what a
    door can know: rows in GRAPH_KEY_MIN_VERSION name the layer floor; a key
    whose floor exceeds this process says REQUIRES >=X (this door: Y) + the
    handshake pointer; the reload note fires when file/process split; a near
    miss gets difflib's nearest accepted."""
    msg = f"unknown graph key; allowed: {sorted(allowed)}"
    minv = GRAPH_KEY_MIN_VERSION.get(key)
    if minv:
        have, need = _padded_eq_len(version_tuple(CODE_VERSION), version_tuple(minv))
        if have is not None and need is not None and have < need:
            msg += (f"; `{key}` is a grammar-layer key that requires plugin >={minv} "
                    f"(this door: {CODE_VERSION}) — update the plugin or declare "
                    f"requires_plugin {minv} to fail fast: see the requires_plugin "
                    "handshake")
        else:
            msg += (f"; `{key}` is a known grammar-layer key (first accepted "
                    f"{minv}) but this door ({CODE_VERSION}) does not take it "
                    "at graph top level")
        note = door_reload_note()
        if note:
            msg += f"; {note}"
    try:
        close = _difflib.get_close_matches(key, list(allowed), n=2, cutoff=0.6)
    except Exception:
        close = []
    if close:
        msg += f"; nearest accepted key(s): {close} — possible typo"
    return msg

def workflow_nodes(graph):
    """The ordinary DAG excludes the agent reserved for terminal cleanup (#129)."""
    return [n for n in graph.get("nodes", []) if n.get("id") != graph.get("on_exit")]


def on_exit_errors(graph):
    """An exit handler is one agent, never a dependency of the ordinary DAG."""
    if "on_exit" not in graph:
        return []
    ref = graph["on_exit"]
    nodes = graph.get("nodes") or []
    nodes = nodes if isinstance(nodes, list) else []
    target = next((n for n in nodes if isinstance(n, dict) and n.get("id") == ref), None)
    errors = []
    def E(msg):
        errors.append({"node": None, "field": "on_exit", "msg": msg})
    if not isinstance(ref, str) or not ref.strip():
        E("on_exit must be a non-empty agent node id")
    elif target is None or target.get("type") != "agent":
        E(f"on_exit {ref!r} must name an existing agent node")
    elif "fanout" in target:
        E("on_exit must name a single agent, not a fan-out")
    if isinstance(ref, str):
        if graph.get("result") == ref:
            E("on_exit cannot also be the ordinary run result")
        for n in nodes:
            if not isinstance(n, dict) or n.get("id") == ref:
                continue
            # Block every data/order surface that could wait on a terminal-only node.
            after, requires, inputs = n.get("after"), n.get("requires"), n.get("inputs")
            fo = n.get("fanout")
            heads = list(after) if isinstance(after, list) else []
            heads += list(requires) if isinstance(requires, dict) else []
            heads += [str(r).split(".")[0] for r in inputs] if isinstance(inputs, list) else []
            heads += [str(fo.get("items_from", "")).split(".")[0]] if isinstance(fo, dict) else []
            heads += re.findall(r"out\.([A-Za-z0-9_-]+)\.", str(n.get("when") or ""))
            if ref in heads or n.get("on_fail") == ref:
                E(f"ordinary node {n.get('id')!r} cannot depend on on_exit {ref!r}")
    return errors


def result_errors(graph):
    """Graph-level defects for the optional `result` key (est-2ek.1.158): it must
    name an existing non-gate node (a gate commits an answer, not run output).
    Absent key = no check. The field is `result` so the refusal names the key."""
    if not isinstance(graph, dict) or "result" not in graph:
        return []
    rid = graph["result"]
    nodes = graph.get("nodes")
    byid = {n["id"]: n for n in nodes if isinstance(n, dict) and isinstance(n.get("id"), str)} \
        if isinstance(nodes, list) else {}
    if not isinstance(rid, str) or rid not in byid:
        return [{"node": None, "field": "result",
                 "msg": f"result {rid!r} must name an existing node id"}]
    if byid[rid].get("type") == "gate":
        return [{"node": None, "field": "result",
                 "msg": f"result {rid!r} is a gate; name the agent, echo or join node whose output is the verdict"}]
    return []

# ---------- est-2ek.1.166: the version handshake (shared: door + runner boot) ----------
# spool key e6e55416cd78c9bd: a 1.0.x graph died at 03:00 on a schema the seat's
# 0.8.0 door never had — a grammar mismatch across plugin versions surfaced as a
# mystery runner death. The handshake makes it a NAMED refusal BEFORE anything
# spawns: the plugin reports its OWN version (plugin.yaml, single source) on
# run.json/status, and an optional graph key `requires_plugin` (coarse dot-
# integer >= comparison) refuses at the door AND at runner boot, naming both
# versions. Absent key = no check, byte-identical old behavior.

def plugin_version():
    """This plugin's own version — the version: line of plugin.yaml beside this
    module (stdlib scan, no YAML dep). '' when unreadable: an unknown runner
    version NEVER blocks an armed run (absence is not a death), but the
    requires_plugin check against a stated requirement then REFUSES (a stale
    seat cannot prove it is new enough)."""
    try:
        text = (Path(__file__).resolve().parent / "plugin.yaml").read_text()
    except OSError:
        return ""
    m = re.search(r"(?m)^version:\s*(\S+)\s*$", text)
    return m.group(1) if m else ""

def drift_report(repo_dir):
    """Offline #125 probe against the nearest reachable LOCAL release tag.

    Does not claim remote freshness. Reads only; error outranks version_mismatch,
    then trailers_missing, then local_only. Counts describe tag..HEAD commits.
    """
    report: dict = dict(installed_version="unknown", upstream_latest="unknown",
                  local_only_commits=None, trailers_missing=None, verdict="error")
    try:
        repo = Path(repo_dir).resolve()
        manifest = (repo / "plugin.yaml").read_text()
        m = re.search(r"(?m)^version:[ \t]*['\"]?(\d+(?:\.\d+)+)['\"]?[ \t]*$", manifest)
        if not m:
            raise ValueError("installed_version_missing_or_invalid")
        report["installed_version"] = m.group(1)
        changelog = (repo / "CHANGELOG.md").read_text()
        if not re.search(r"(?m)^##[ \t]+\[?" + re.escape(m.group(1))
                         + r"(?:\]|[ \t]|$)", changelog):
            raise ValueError("changelog_heading_missing")
        # Ignore caller-selected git state and forbid partial-clone lazy fetching.
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env.update(GIT_NO_LAZY_FETCH="1", GIT_TERMINAL_PROMPT="0")
        def git(verb, *args):
            # The only subprocess boundary: no network verbs, shell, or user argv.
            if verb not in ("describe", "rev-list", "log"):
                raise ValueError("git_verb_not_allowed")
            return subprocess.run(["git", verb, *args], cwd=repo, env=env,
                                  capture_output=True, text=True, check=True,
                                  timeout=15).stdout.strip()
        tag = git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*",
                  "--match", "[0-9]*", "HEAD")
        latest = tag.removeprefix("v")
        if not re.fullmatch(r"\d+(?:\.\d+)+", latest):
            raise ValueError("release_tag_invalid")
        report["upstream_latest"] = latest
        span = tag + "..HEAD"
        count = int(git("rev-list", "--count", span, "--"))
        messages = git("log", "--format=%B%x00", span, "--").split("\0")
        messages = [msg for msg in messages if msg.strip()]
        if len(messages) != count:
            raise ValueError("commit_log_count_mismatch")
        missing = sum(not re.search(r"\(est-[^)\s]+\)|#\d+\b", msg) for msg in messages)
        report.update(local_only_commits=count, trailers_missing=missing)
        report["verdict"] = ("version_mismatch" if m.group(1) != latest else
                             "trailers_missing" if missing else "local_only" if count else "clean")
    except (OSError, ValueError, UnicodeError, subprocess.SubprocessError) as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    return report


def version_tuple(v):
    """Dot-integer tuple for coarse >= comparison; None when not parseable
    ('99.0.0' -> (99, 0, 0); 'latest' -> None)."""
    if not isinstance(v, str) or not v.strip():
        return None
    parts = v.strip().split(".")
    out = []
    for p in parts:
        if not p.isdigit():
            return None
        out.append(int(p))
    return tuple(out) or None

def requires_plugin_errors(graph):
    """Graph-level defects for the optional `requires_plugin` key: a coarse
    dot-integer version string (absent = no check). A malformed value is a
    NAMED validation error, never a crash. The STALENESS refusal itself is
    _version_handshake_error (it needs the runner's own version)."""
    if not isinstance(graph, dict) or "requires_plugin" not in graph:
        return []
    v = graph["requires_plugin"]
    if version_tuple(v) is None:
        return [{"node": None, "field": "requires_plugin",
                 "msg": f"requires_plugin {v!r} invalid: must be a dot-integer version "
                        f"string like '1.2.0' (coarse >= comparison)"}]
    return []

def version_handshake_error(required, runner_version):
    """The typed refusal text when `required` exceeds `runner_version`, else
    None. Both versions are NAMED in the message — the whole point of the
    handshake is that a stale seat reads exactly what to upgrade. An
    un-parseable/un-stated runner_version with a stated requirement fails
    closed (cannot prove new enough = refuse); no requirement = always None.
    Mixed-length spellings of the SAME release ('1.2.0' vs '1.2') compare
    equal: tuples are padded to equal length first, so the refusal never
    fires on trailing zeros alone (#261 follow-up: raw tuple compare made
    (1,2,0) < (1,2) and refused the plugin's own version spelled longer)."""
    req = version_tuple(required) if isinstance(required, str) else None
    if req is None:
        return None                             # absent/invalid handled at validation
    have = version_tuple(runner_version)
    if have is not None:
        n = max(len(have), len(req))
        have = have + (0,) * (n - len(have))
        req = req + (0,) * (n - len(req))
        if have >= req:
            return None
    return (f"plugin version handshake failed: this graph declares "
            f"requires_plugin {required!r} but the installed hermes-workflows "
            f"runner-version is {(runner_version or 'unknown')!r} — update the plugin "
            f"(hermes plugins update hermes-workflows) before arming this run")

def model_names_valid(names):
    return isinstance(names, list) and all(isinstance(n, str) and n.strip() for n in names)

def model_policy_errors(policy):
    """Closed-set + type rules for one model_policy object, as messages of the
    form 'model_policy.<field>: <detail>'. ONE list of rules: the structural
    graph validator AND the include fuse (which ORs child require_model floors
    into the parent) both consume it — a policy that would fail direct submission
    can never slip through the merge's coercion path (PR#84 review F-2: a parent
    require_model:'false' became boolean True via the OR and then passed)."""
    msgs = []
    if not isinstance(policy, dict):
        return ["model_policy: model_policy must be an object"]
    for key in sorted(set(policy) - {"require_model", "forbidden_models"}):
        msgs.append(f"model_policy.{key}: unknown policy key")
    if "require_model" in policy and not isinstance(policy["require_model"], bool):
        msgs.append("model_policy.require_model: require_model must be boolean")
    if "forbidden_models" in policy and not model_names_valid(policy["forbidden_models"]):
        msgs.append("model_policy.forbidden_models: forbidden_models must be a "
                    "list of non-empty strings")
    return msgs

_RUN_CONTRACT_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_RUN_CONTRACT_REF = re.compile(r"\{run\.([^{}]*)\}")

def _run_contract_refer_keys(graph):
    """The {run.KEY} names referenced by a graph's OPERATIVE text — the exact
    surface the launcher renders (est-7m6wv P-04/05/06/07): the shared
    _include_text_fields walker (goal/context/question/profile + fan-out goal
    and item goals + echo output + gate options[] + wait.until_argv[]) — PLUS
    defaults.context (baked into every node context at launch), PLUS the string
    leaves of every `by` patch value (a patch becomes operative text the moment
    its enum arm is selected — "operative" means under ANY binding, or the
    total-contract law is a hole an enum value can drive a truck through).
    Shared on purpose: the s12 envelope check, the #126 unrefused-required
    rule, the params dead/undeclared laws and the instantiate surface all read
    THIS one enumeration — one renderer, one enumeration, one law."""
    out = set()
    nodes = graph.get("nodes")
    if not isinstance(nodes, list):
        return out
    dfs = graph.get("defaults")
    if isinstance(dfs, dict) and isinstance(dfs.get("context"), str):
        for m in _RUN_CONTRACT_REF.finditer(dfs["context"]):
            out.add(m.group(1))
    for n in nodes:
        if not isinstance(n, dict):
            continue
        texts = _include_texts(n)
        fo = n.get("fanout")
        if isinstance(fo, dict):
            texts.append(fo.get("goal"))
            texts += [i.get("goal") for i in fo.get("items", []) if isinstance(i, dict)]
        for t in texts:
            if isinstance(t, str):
                for m in _RUN_CONTRACT_REF.finditer(t):
                    out.add(m.group(1))
    # est-bwv0h: where/by slots are refs too — a tune knob that drives presence or
    # patching is consumed AT THE DOOR, so a gate-driven key is never dead.
    # est-7m6wv P-04: and every string leaf inside a by-patch is operative text
    # under the arm that selects it — scanned here, refused at save if undeclared.
    for n in nodes:
        if not isinstance(n, dict):
            continue
        w = n.get("where")
        if isinstance(w, dict) and isinstance(w.get("param"), str):
            out.add(w["param"])
        b = n.get("by")
        if isinstance(b, dict):
            out.update(k for k in b if isinstance(k, str))
            for slot_map in b.values():
                if not isinstance(slot_map, dict):
                    continue
                for patch_ in slot_map.values():
                    stack = [patch_]
                    while stack:
                        cur = stack.pop()
                        if isinstance(cur, str):
                            for m in _RUN_CONTRACT_REF.finditer(cur):
                                out.add(m.group(1))
                        elif isinstance(cur, dict):
                            stack.extend(cur.values())
                        elif isinstance(cur, list):
                            stack.extend(cur)
    return out

PARAM_KEYS = {"kind", "desc", "domain", "default"}
_WHERE_KEYS = {"param", "in"}

def params_errors(graph):
    """est-bwv0h: defects for the optional top-level `params` CONTRACT block — the typed
    launch contract that subsumes the #126 run_context contract. Shape:
    {slot: {kind: "pin"|"tune", desc?, domain? (tune: distinct non-empty strings),
    default? (tune only, in domain)}}. `pin` carries run content (never defaulted,
    never domain-gated); `tune` is a knob (domain required; optional stamped default).
    With a params block the contract is TOTAL both ways: every operative {run.KEY} ref
    must be declared, and every declared slot must be referenced (operative text or a
    where/by gate) — bind what you declare, declare what you bind. Node `where`
    (presence gate on a tune slot, door-time) and node `by` (value-conditional patch,
    door-time) are validated here too: their slots must be declared tune, values must
    live inside the domain (a dead gate/patch is a typo, refused), and `by` patch keys
    must be legal for the node's type. Absent key = no check: a plain graph stays
    byte-identical."""
    if not isinstance(graph, dict) or "params" not in graph:
        return []
    errs = []
    def E(field, msg):
        errs.append({"node": None, "field": field, "msg": msg})
    p = graph["params"]
    if "run_context" in graph:
        E("params", "graph declares both `params` and a `run_context` contract block — one "
                    "contract per graph: `params` is the typed superset of #126")
    if not isinstance(p, dict) or not p:
        E("params", "params must be a non-empty object of slot -> declaration")
        return errs
    nodes = [n for n in (graph.get("nodes") or []) if isinstance(n, dict)]
    referenced = set(_run_contract_refer_keys(graph))
    # (est-7m6wv horde) gate-driven slots need no local re-collection:
    # _run_contract_refer_keys folds every where-param and by-slot into
    # `referenced` on purpose — the shared enumeration IS the law.
    for slot in sorted(p, key=str):
        d = p[slot]
        f = f"params.{slot}"
        if not isinstance(slot, str) or not _RUN_CONTRACT_KEY.match(slot):
            E(f, f"invalid slot name {slot!r}: expected identifier")
            continue
        if not isinstance(d, dict):
            E(f, "declaration must be an object (kind/desc/domain/default)")
            continue
        for k in sorted(set(d) - PARAM_KEYS):
            E(f"{f}.{k}", f"unknown key; allowed: {sorted(PARAM_KEYS)}")
        kind_ = d.get("kind")
        if kind_ not in ("pin", "tune"):
            E(f + ".kind", 'kind must be "pin" or "tune"')
            continue
        if "desc" in d and (not isinstance(d["desc"], str) or not d["desc"].strip()):
            E(f + ".desc", "desc must be a non-empty string")
        if kind_ == "pin" and ("default" in d or "domain" in d):
            E(f, "pin slots carry run content: no default, no domain — a knob belongs in a tune slot")
        elif kind_ == "tune":
            dom = d.get("domain")
            if (not isinstance(dom, list) or not dom
                    or not all(isinstance(x, str) and x.strip() for x in dom)
                    or len(set(dom)) != len(dom)):
                E(f + ".domain", "tune needs a domain: non-empty list of distinct non-empty strings")
                continue
            if "default" in d and d["default"] not in dom:
                E(f + ".default", f"default {d['default']!r} is outside the domain {dom}")
    declared = {k for k in p if isinstance(k, str) and _RUN_CONTRACT_KEY.match(k)}
    dead = sorted(declared - referenced)
    if dead:
        E("params", f"declared slot(s) {dead} never referenced as {{run.KEY}} in operative text "
                    "nor used by a where/by gate — bind what you declare")
    undeclared = sorted(referenced - declared)
    if undeclared:
        E("params", f"operative {{run.KEY}} ref(s) {undeclared} are not declared — with a params "
                    "block the contract is TOTAL: declare every binding the nodes consume")
    for n in nodes:
        nid = n.get("id")
        t = n.get("type", "agent")
        tkeys = NODE_TYPES[t].keys if t in NODE_TYPES else frozenset()
        w = n.get("where")
        if w is not None:
            if not isinstance(w, dict):
                E(f"nodes.{nid}.where", "where must be an object {param, in}")
                w = None
            else:
                for k in sorted(set(w) - _WHERE_KEYS):
                    E(f"nodes.{nid}.where.{k}", f"unknown key; allowed: {sorted(_WHERE_KEYS)}")
                ps = w.get("param")
                dp = p.get(ps) if isinstance(ps, str) and ps in declared else None
                if dp is None:
                    E(f"nodes.{nid}.where.param", f"names an undeclared slot {ps!r} — params is the contract")
                elif dp.get("kind") != "tune":
                    E(f"nodes.{nid}.where.param", f"slot {ps!r} is a pin — where/by gate on tune slots only")
                else:
                    inv = w.get("in")
                    if (not isinstance(inv, list) or not inv
                            or not all(isinstance(x, str) and x.strip() for x in inv)
                            or len(set(inv)) != len(inv)):
                        E(f"nodes.{nid}.where.in", "in must be a non-empty list of distinct non-empty strings")
                    else:
                        out = [x for x in inv if x not in (dp.get("domain") or [])]
                        if out:
                            E(f"nodes.{nid}.where.in", f"value(s) {out} outside the domain — a presence "
                                                       "gate can never fire nor pass as written")
        b = n.get("by")
        if b is not None:
            if not isinstance(b, dict) or not b:
                E(f"nodes.{nid}.by", "by must be a non-empty object slot -> case -> patch")
                continue
            for slot_key in sorted(b, key=str):
                ff = f"nodes.{nid}.by.{slot_key}"
                dp = p.get(slot_key) if isinstance(slot_key, str) and slot_key in declared else None
                if dp is None:
                    E(ff, f"names an undeclared slot {slot_key!r} — params is the contract")
                    continue
                if dp.get("kind") != "tune":
                    E(ff, f"slot {slot_key!r} is a pin — where/by gate on tune slots only")
                    continue
                cases = b[slot_key]
                if not isinstance(cases, dict) or not cases:
                    E(ff, "cases must be a non-empty object case -> patch")
                    continue
                for case in sorted(cases, key=str):
                    if case not in (dp.get("domain") or []):
                        E(f"{ff}.{case}", "case outside the domain — a patch that can never apply is a typo")
                        continue
                    patch_ = cases[case]
                    if not isinstance(patch_, dict) or not patch_:
                        E(f"{ff}.{case}", "patch must be a non-empty object of node fields")
                        continue
                    for pk in sorted(set(patch_) - set(tkeys)):
                        E(f"{ff}.{case}.{pk}", f"patch key not allowed on {t} nodes")
    return errs

def run_context_contract_errors(graph):
    """#126 (est-syim): defects for the optional top-level `run_context` CONTRACT block —
    a machine-readable declaration of the {run.KEY} bindings a graph needs at launch.
    Shape: {required?: [identifier...], defaults?: {identifier: non-empty string}} — the
    SAME identifier grammar the launch binding uses. Every declared required key must be
    referenced as {run.KEY} in operative text (an unreferenced requirement is a typo,
    refused at save/submit like every other contract defect). Absent key = no check:
    a plain graph stays byte-identical."""
    if not isinstance(graph, dict) or "run_context" not in graph:
        return []
    c = graph["run_context"]
    errs = []
    def E(field, msg):
        errs.append({"node": None, "field": field, "msg": msg})
    if not isinstance(c, dict) or not c:
        E("run_context", "run_context contract must be a non-empty object "
                         "(required and/or defaults)")
        return errs
    for key in sorted(set(c) - {"required", "defaults"}):
        E(f"run_context.{key}", 'unknown key; allowed: ["defaults", "required"]')
    if "required" in c:
        req = c["required"]
        if (not isinstance(req, list) or not req
                or not all(isinstance(k, str) and _RUN_CONTRACT_KEY.match(k) for k in req)):
            E("run_context.required", "required must be a non-empty list of identifier strings")
        elif len(set(req)) != len(req):
            E("run_context.required", "required keys must be unique")
        else:
            unref = [k for k in req if k not in _run_contract_refer_keys(graph)]
            if unref:
                E("run_context.required",
                  f"declared required key(s) {unref} never referenced as {{run.KEY}} in "
                  "operative text (goal/context/question/profile/fan-out) — bind what you declare")
    if "defaults" in c:
        d = c["defaults"]
        if not isinstance(d, dict) or not d:
            E("run_context.defaults", "defaults must be a non-empty object of identifier -> non-empty string")
        else:
            for k in sorted(d):
                if not isinstance(k, str) or not _RUN_CONTRACT_KEY.match(k):
                    E("run_context.defaults", f"invalid key {k!r}: expected identifier")
                elif not isinstance(d[k], str) or not d[k].strip():
                    E("run_context.defaults", f"defaults[{k!r}] must be a non-empty string")
    return errs

def structural_graph_errors(graph, extra_keys=()):
    """Graph-level (non-node) defects as [{node:None, field, msg}] — the exact
    checks the door historically ran, moved verbatim so submitted graphs AND each
    expanded shelf are measured by the SAME rules. `extra_keys` extends the
    closed top-level key set (the door passes its own additions); `include` is
    always allowed here — the author form carries it by design, and the fused
    output is include-STRIPPED, so the STRUCTURAL_GRAPH_KEYS law holds on
    every submitted surface. Callers compose with validate_graph_errors (the
    node-level half)."""
    if not isinstance(graph, dict):
        return [{"node": None, "field": "graph", "msg": "graph must be an object"}]
    errs = []
    def E(field, msg):
        errs.append({"node": None, "field": field, "msg": msg})
    for key in sorted(set(graph) - STRUCTURAL_GRAPH_KEYS - set(extra_keys)):
        # #509: layer hint + reload note + nearest-key on the refusal text;
        # the legacy prefix before the first `;` stays byte-identical.
        E(key, unknown_graph_key_msg(key, STRUCTURAL_GRAPH_KEYS | set(extra_keys)))
    # #32: a file may state its dialect; absent = wf/1, unknown = refused.
    errs.extend(grammar_errors(graph))
    # #100: run-level concurrency limits, positive integers only.
    for key in ("concurrency", "item_concurrency"):
        if key in graph and (type(graph[key]) is not int or graph[key] <= 0):
            E(key, f"{key} must be a positive integer")
    # est-3acn: run-wide RunBudget — graph-root budget:{"tokens": N}, validated
    # beside the #100 limits; the door bakes it into run.json meta at submit.
    if graph.get("budget") is not None:
        for m_ in budget_shape_errors(graph["budget"], "graph root"):
            E("budget", m_)
    # est-2ek.1.856 deep review D2: the advertised per-run seat-cap opt-out is
    # measured by the SAME law as the operator knobs — one integer inside
    # [SEAT_CAP_FLOOR, SEAT_CAP_CEILING] or the explicit 0 off-sentinel; bool is
    # the typo shape. A bad value refuses at submit naming the key, never
    # 'unknown graph key' and never a silent default (SeatCapError is a
    # ValueError, so the refuse message carries the knob name and range).
    if "max_seats" in graph:
        try:
            _seat_cap_valid(graph["max_seats"], "graph max_seats")
        except SeatCapError as e_:
            E("max_seats", str(e_))
    # est-2ek.1.166: shape-only here (dot-integer string); the staleness
    # refusal itself belongs at the arm/launch seams so it names the runner's
    # own version — same message the door and the boot guard both use.
    for e_ in requires_plugin_errors(graph):
        E(e_["field"], e_["msg"])
    for e_ in result_errors(graph) + on_exit_errors(graph):
        E(e_["field"], e_["msg"])
    # WF-GRAFT-1 (est-1f7cn): the graph-wide wake default is a closed value, refused
    # by name at the door — never a silent degradation to a louder or quieter default
    # (the runner's own read fails open to `full`, so an unvalidated value would wake
    # differently than the author wrote).
    if "wake_tier" in graph:
        _wt = graph["wake_tier"]
        if not isinstance(_wt, str) or _wt not in WAKE_TIERS:
            E("wake_tier", f"wake_tier {_wt!r} invalid: must be one of "
                           + json.dumps(list(WAKE_TIERS))
                           + " (full = wake on every notice about a node; quiet = that "
                             "node's straggler observation records without waking; "
                             "silent = its node-subject notices record, including a "
                             "gate's own hold). Run-terminal wakes are never tiered.")
    for e_ in run_context_contract_errors(graph):
        E(e_["field"], e_["msg"])
    for e_ in params_errors(graph):
        E(e_["field"], e_["msg"])
    # est-bwv0h: the seam without a contract is a silent no-op — the door only
    # materializes where/by for graphs that carry the typed block. Refuse the shape
    # by name, never shelve a gate that can never fire.
    if not (isinstance(graph.get("params"), dict) and graph["params"]):
        for n in (graph.get("nodes") or []):
            if isinstance(n, dict) and ("where" in n or "by" in n):
                E(f"nodes.{n.get('id')}",
                  "where/by require a top-level params contract block — the door "
                  "materializes the seam only for graphs that declare it")
    if "defaults" in graph:
        errs.extend(_defaults_errors(graph["defaults"]))
    if "model_policy" in graph:
        for msg in model_policy_errors(graph["model_policy"]):
            field, _, detail = msg.partition(": ")
            E(field, detail)
    if not model_names_valid(seat_forbidden_models()):
        E("model.workflows_forbidden_models", "seat forbidden model floor must be a list of non-empty strings")
    for key in ("name", "description"):
        if key in graph and (not isinstance(graph[key], str) or not graph[key].strip()):
            E(key, f"{key} must be a non-empty string")
    if "provenance" in graph:
        prov = graph["provenance"]
        if not isinstance(prov, dict):
            E("provenance", "provenance must be an object")
        else:
            for key in sorted(set(prov) - PROVENANCE_KEYS):
                E(f"provenance.{key}", "unknown key; allowed: " + json.dumps(sorted(PROVENANCE_KEYS)))
    return errs

def validate_graph_full(graph):
    """The WHOLE structural+node validation of a graph object (unnormalized) —
    what a submitted graph is measured with at the door. Returns the defect list;
    empty means sound. Node-shape normalization (unhashable ids, non-list after,
    author-forged route_verified) is the door's caller-side job — see
    _validation_error there; this function never mutates its input."""
    if not isinstance(graph, dict):
        return [{"node": None, "field": "graph", "msg": "graph must be an object"}]
    errs = structural_graph_errors(graph)
    nodes = graph.get("nodes")
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
            deps = node.get("after", [])
            if not isinstance(deps, list) or any(not isinstance(dep, str) for dep in deps):
                errs.append({"node": nid if isinstance(nid, str) else None,
                             "field": "after", "msg": "after must be a list of node id strings"})
                item["after"] = []
            safe.append(item)
    errs.extend(validate_graph_errors(safe, admission=True))
    errs.extend(_run_ref_scan(graph, safe))
    return errs


def _run_ref_scan(graph, safe_nodes):
    """Malformed {run.X} references are validate-time defects, not launch surprises.

    A captured key that is not a bare identifier ('{run.*}', '{run.a or b}')
    can NEVER bind — _bind_run_context raises on it for EVERY run_context, so
    the graph refuses to launch at any binding, after validate has said ok.
    The include resolver already refuses this exact class inside composite
    subtrees (_include_render, same identifier law); plain-graph authors
    deserved the same static refusal. One defect per distinct (node, literal)
    occurrence, worded so the fix is executable without reading the binder.
    Surfaces scanned: exactly what the binder renders (goal/context/question/
    profile/repo, fan-out goal + item goals, echo string output, gate options,
    wait.until_argv — via the shared _include_texts traversal) PLUS
    defaults.context, which the pre-run seed pass renders (wfcommon
    _PRE_RUN_REF) after prepending. Non-run braces ({item.x}) are the
    fan-out placeholder surface — untouched here by design."""
    def scan(text, node_id, field):
        out = []
        seen = set()
        for m in _INCLUDE_RUN_REF.finditer(text):
            key = m.group(1)
            if _INCLUDE_RUN_KEY.fullmatch(key) or m.group(0) in seen:
                continue
            seen.add(m.group(0))
            out.append({"node": node_id, "field": "run_ref", "msg": (
                f"malformed run_context reference {m.group(0)!r} in {field}: the key "
                "must be a single identifier ([A-Za-z_][A-Za-z0-9_]*), so this literal "
                "can never bind and the launch would refuse at every run_context — "
                "reword the prose, write the value literally, or bind a real key")})
        return out

    errs = []
    for node in safe_nodes:
        if not isinstance(node, dict):
            continue
        nid = node.get("id") if isinstance(node.get("id"), str) else None
        for text in _include_texts(node):
            if isinstance(text, str):
                errs.extend(scan(text, nid, "node render surface"))
    defaults = graph.get("defaults") if isinstance(graph, dict) else None
    if isinstance(defaults, dict) and isinstance(defaults.get("context"), str):
        errs.extend(scan(defaults["context"], "defaults", "defaults.context"))
    return errs

# ---------- include-by-expansion (design 2026-09-30) ----------
# Composition at MATERIALIZE time, never invocation: a graph carrying a top-level
# `include: [{as, use, seeds, exports}]` annotation expands to a plain wf/1 graph
# BEFORE validation/persistence. The runner, read model, gates and desktop never
# learn includes exist; the closed-set node validator is untouched (include is not
# a node kind — it is a graph-level annotation like `grammar`, :564-568). The door
# owns the call sites (run/amend/save, before _validation_error) and supplies
# library_reader(name) -> dict|None (the same resolver `from=<name>` uses); this
# module must never import the door.
#
# Invariants this pass is built to keep:
#   * deterministic namespacing: id -> `<alias>__<inner-id>` (double underscore,
#     never a dot — ids double as nodes/<id>.json basenames and `out.<id>` syntax),
#     length-checked against ID_OK's 64-char cap: on overflow REFUSE, never truncate.
#   * five id-ref surfaces rewritten in lockstep inside the subtree: after, inputs
#     heads, requires keys, fanout.items_from head AND its after entry (the
#     items_from head must remain a direct after parent, :914-920), and `when`
#     out.<id>. paths. `when` heads are NOT existence-checked by the validator
#     (:1298-1309 parses structure only), so this pass validates them against the
#     namespace-local id set itself — a missed rewrite is a silent gate-fire failure.
#   * seed contract: include `seeds` render {run.KEY} INSIDE the included subtree
#     ONLY; an unbound reference there is the #69 fail-closed refusal (the message
#     names the alias). Parent {run.X} text is never touched by child seeds.
#   * strip-on-expand: the returned graph has NO `include` key (golden-solo byte
#     pins; a committed graph.json carrying `include` is by definition unexpanded).
#   * efp stability: parent nodes not wired to an include stay byte-identical —
#     def_hash/efp/replay-skip are untouched by construction; only rewired nodes
#     move, which is precisely the intended re-run granularity.
# Constants here MIRROR the door's (GRAPH_MAX_BYTES __init__.py:51); the door owns
# its copies and may re-check the merged artifact with its own caps after expansion.
INCLUDE_DEPTH_MAX = 4        # nested composites: A includes B includes C includes D
INCLUDE_NODES_MAX = 256      # merged node-count cap (tunable)
INCLUDE_BYTES_MAX = 1024 * 1024   # merged bytes cap, mirrors door GRAPH_MAX_BYTES
INCLUDE_KEYS = {"as", "use", "seeds", "exports"}
# No hyphen, no dots: an alias becomes the head prefix of generated ids, and ids
# appear inside `when` out.<id> paths — WHEN_TOKEN (:1738) lexes heads as
# [A-Za-z0-9_.] only, so a hyphenated alias + a child when-gate would make the
# when rewrite/head-check silently no-op (a silent gate-fire failure). The alias
# grammar must stay a SUBSET of the when-token head grammar; refuse at declaration.
INCLUDE_ALIAS_OK = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_]{0,23}$")   # <= 24
INCLUDE_USE_OK = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")          # mirrors door LIB_OK
_INCLUDE_RUN_REF = re.compile(r"\{run\.([^{}]*)\}")
_INCLUDE_RUN_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
# absolute/~-rooted path literals with >= 2 segments (a bare "/x" is prose noise) —
# the shared fixed-scratch-path probe. Lookbehind keeps URL/email tails (http://host/p,
# x/y inside words) out.
_INCLUDE_ABS_PATH = re.compile(
    r"(?<![\w~./:-])(?:/[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)+|~(?:/[A-Za-z0-9._-]+)+)")
_INCLUDE_NS_SEP = "__"


def _include_error(alias, msg):
    """ONE error shape for every refusal: a single human-readable string that names
    the include (the door's door turns the ValueError into its errors[include:<alias>]
    envelope; never raise a list/dict here)."""
    return ValueError(f"include '{alias}': {msg}" if alias else f"include: {msg}")


def _include_text_fields(node):
    """The string fields the {run.KEY} seed-render surface touches: the exact set
    _bind_run_context (map mode) renders — goal/context/question/profile/repo plus the
    fan-out goal template and item goals — PLUS an echo node's string `output`
    PLUS a gate's `options[]` and `wait.until_argv[]`.
    The echo addition closes PR#84 review F-3: echo output is a text surface the
    runner commits VERBATIM (wf.py echo pass), so a seed placeholder surviving it
    became a literal `{run.MISSING}` verdict in a done run. The options/argv
    addition closes the F-3 carry-over (PR#84 round-2 P1): `options` surface
    VERBATIM on the human release card and `until_argv` is exec'd as fixed argv
    (wf.py wait pass) — a surviving `{run.OPTION}` was a literal button label /
    a literal unbound argv element. Only PRESENT string fields are yielded (a
    dict/other output is data, not text). One definition feeds seed-render,
    scratch-path detection, and the door's survivor check — run/amend/binding/
    notes cannot drift apart again. Duplicated here (not imported from the door)
    so the resolver stays hermes-free."""
    for f in ("goal", "context", "question", "profile", "repo"):
        if isinstance(node.get(f), str):
            yield f, (f,)
    if node.get("type") == "echo" and isinstance(node.get("output"), str):
        yield "output", ("output",)
    opts = node.get("options")
    if isinstance(opts, list):
        for i, o in enumerate(opts):
            if isinstance(o, str):
                yield f"options[{i}]", ("options", i)
    wait = node.get("wait")
    if isinstance(wait, dict) and isinstance(wait.get("until_argv"), list):
        for i, a in enumerate(wait["until_argv"]):
            if isinstance(a, str):
                yield f"wait.until_argv[{i}]", ("wait", "until_argv", i)
    fo = node.get("fanout")
    if isinstance(fo, dict):
        if isinstance(fo.get("goal"), str):
            yield "fanout.goal", ("fanout", "goal")
        for i, it in enumerate(fo.get("items") or []):
            if isinstance(it, dict) and isinstance(it.get("goal"), str):
                yield f"fanout.items[{i}].goal", ("fanout", "items", i, "goal")


def run_context_contract(graph, params=None):
    """Refs are the binding contract; optional envelope metadata only describes it.

    Read malformed params as absent, never quarantine an otherwise usable shelf.
    Defaults are descriptive, not implicit launch bindings. All live refs remain
    required even if a declaration attempts to mark one optional.
    """
    declared = params if isinstance(params, dict) else {}
    refs = set()
    nodes = list(graph.get("nodes") or [])
    defaults = graph.get("defaults")
    if isinstance(defaults, dict) and isinstance(defaults.get("context"), str):
        nodes.append({"context": defaults["context"]})
    for node in nodes:
        if isinstance(node, dict):
            for text in _include_texts(node):
                refs.update(_INCLUDE_RUN_REF.findall(text))
    # est-bwv0h: where/by slots are refs too (consumed at the door) — the instantiate
    # surface and the s12 law must see a gate-driven knob exactly like a text ref.
    for node in nodes:
        if not isinstance(node, dict):
            continue
        w = node.get("where")
        if isinstance(w, dict) and isinstance(w.get("param"), str):
            refs.add(w["param"])
        b = node.get("by")
        if isinstance(b, dict):
            refs.update(k for k in b if isinstance(k, str))
    derived = {}
    for key in sorted(refs):
        spec = declared.get(key)
        spec = spec if isinstance(spec, dict) else {}
        derived[key] = {"required": True}
        if isinstance(spec.get("desc"), str):
            derived[key]["desc"] = spec["desc"]
        if isinstance(spec.get("default"), str) and spec["default"].strip():
            derived[key]["default"] = spec["default"]
    keys = {k for k in declared if isinstance(k, str)}
    return {"refs": sorted(refs), "params": derived,
            "missing": sorted(refs - keys), "dead": sorted(keys - refs)}


def _include_get(node, path):
    """Read one authored string by path segments (str = dict key, int = list
    index). Companion of _include_set; both walk the same grammar
    _include_text_fields yields."""
    cur = node
    for p in path:
        cur = cur[p]
    return cur


def _include_set(node, path, val):
    """Write one rendered string back by path segments, COPY-ON-WRITE at every
    level: each dict/list on the path is shallow-copied before the child write,
    so the shelf's own node objects are never mutated (shelf immutability) and
    NO nested shape needs to be special-cased. This replaces the old hand-rolled
    setter that assumed every nested field was `fanout` — with options[] and
    wait.until_argv[] in the traversal it would have rebuilt the wrong chain
    (PR#84 round-2 fix caution)."""
    head, rest = path[0], path[1:]
    if not isinstance(node, dict):
        # node is a LIST: head is an integer index
        items = list(node)
        items[head] = val if not rest else _include_set(items[head], rest, val)
        return items
    if not rest:
        return {**node, head: val}
    child = node[head]
    if isinstance(child, dict) or isinstance(child, list):
        return {**node, head: _include_set(child, rest, val)}
    return {**node, head: val}


def _include_texts(node):
    out = []
    for _, path in _include_text_fields(node):
        out.append(_include_get(node, path))
    return out


def _include_render(text, seeds, *, fanout=False, alias=None, where=None):
    """One-pass {run.KEY} substitution mirroring _bind_run_context's contract: no
    str.format, substituted values are never re-interpolated. Unbound key = the #69
    fail-closed refusal, worded to name the alias and the include surface."""
    def replace(m):
        key = m.group(1)
        if not _INCLUDE_RUN_KEY.fullmatch(key):
            raise _include_error(alias, f"malformed reference {m.group(0)!r} in {where}")
        if key not in seeds:
            raise _include_error(alias, f"unbound {{run.{key}}} in {where}: "
                                        f"add {key!r} to the include's seeds")
        if fanout and ("{" in seeds[key] or "}" in seeds[key]):
            raise _include_error(alias, f"seed {key!r} must not contain braces when "
                                        f"bound into a fan-out goal ({where})")
        return seeds[key]
    return _INCLUDE_RUN_REF.sub(replace, text)


def _include_seed_scan(seeds, alias):
    """Seeds are a CLOSED map: values are non-empty strings, keys are identifiers,
    and no value may carry braces (existing fan-out guard, applied to every include
    seed — the resolver cannot know which field a value lands in at bind time)."""
    if not isinstance(seeds, dict) or not seeds:
        raise _include_error(alias, "seeds must be a non-empty object {KEY: value}")
    for k, v in seeds.items():
        if not isinstance(k, str) or not _INCLUDE_RUN_KEY.fullmatch(k):
            raise _include_error(alias, f"seeds invalid key {k!r}: expected identifier")
        if not isinstance(v, str) or not v.strip():
            raise _include_error(alias, f"seeds[{k!r}] must be a non-empty string")
        if "{" in v or "}" in v:
            raise _include_error(alias, f"seeds[{k!r}] must not contain braces "
                                        "(no-interpolation-of-substituted-values contract)")


def _include_scratch_paths(node):
    """Absolute/~-rooted path literals appearing in a node's authored strings."""
    found = set()
    for t in _include_texts(node):
        found.update(_INCLUDE_ABS_PATH.findall(t))
    return found


def _ledger_ref_rewrite(row, map_head):
    """#85: a fanout.ledger row is an id-ref surface like `inputs` — rewrite the
    source HEAD through `map_head` (string rows and {source, artifact?} rows; any
    other shape passes through untouched — row shape is the guard's named defect).
    Used by BOTH include passes so shelf refs and parent refs namespace in
    lockstep with after/inputs/items_from (the five-surface law, grammar.md)."""
    def _r(ref):
        head, sep, rest = ref.partition(".")
        return map_head(head) + (sep + rest if sep else "")
    if isinstance(row, str):
        return _r(row)
    if isinstance(row, dict) and isinstance(row.get("source"), str):
        return dict(row, source=_r(row["source"]))
    return row

def _rewrite_child_head(h, ns):
    """Map one ref head inside the included subtree: internal -> namespaced,
    anything else kept (child-standalone validation rejects true danglers first)."""
    return ns.get(h, h)


def _include_when_rewrite(expr, map_head):
    """Token-wise rewrite of `out.<head>.<path>` refs in a when expression — via
    the WHEN_TOKEN tokenizer so quoted string LITERALS that merely contain
    'out.x.y' text are never rewritten or validated. Returns the ORIGINAL string
    byte-identical when no head maps — a parent node whose when does not touch an
    include keeps its def bytes (efp stability). Malformed expr passes through:
    the validator owns that error."""
    try:
        toks = _tok_when(expr)
    except ValueError:
        return expr
    changed = False
    pieces = []
    for t in toks:
        if t.startswith("out."):
            segs = t.split(".")
            nh = map_head(segs[1]) if len(segs) > 1 else segs[-1]
            if nh != segs[1]:
                segs[1] = nh
                changed = True
            pieces.append(".".join(segs))
        else:
            pieces.append(t)
    # Rebuild joins tokens with single spaces: every WHEN_TOKEN match is ONE token
    # (string literals keep their internal spacing inside the token), so the result
    # re-parses identically. Only ever done when a head actually mapped — untouched
    # whens stay byte-identical (efp stability).
    return " ".join(pieces) if changed else expr


def _include_when_heads(expr):
    """Heads of genuine out.<id> references (tokenizer-driven; string literals
    excluded) — the set this pass must existence-check since the validator does
    not."""
    try:
        toks = _tok_when(expr)
    except ValueError:
        return []
    return [t.split(".")[1] for t in toks if t.startswith("out.") and len(t.split(".")) > 1]


def _include_namespace_child(child, alias, library_name):
    """Step 3+4 of the design pass: namespace every id of the (already recursively
    expanded) child graph and rewrite the FIVE ref surfaces inside it in lockstep.
    Returns (nodes, nsset). Raises ValueError (named) on dot/overflow, dangling
    items_from lockstep, or a dangling `when` head."""
    cids = [n["id"] for n in child["nodes"]]
    ns = {}
    for cid in cids:
        gen = f"{alias}{_INCLUDE_NS_SEP}{cid}"
        # no dots in generated ids (ids double as nodes/<id>.json basenames and the
        # dot IS out.<id> syntax); 64-char cap via ID_OK — REFUSE, never truncate.
        if "." in gen or not ID_OK.match(gen):
            raise _include_error(alias, f"namespaced id {gen!r} for library node "
                                        f"{cid!r} of '{library_name}' is invalid "
                                        f"(no dots, alnum start, <= 64 chars) — "
                                        f"shorten the alias or the inner id")
        ns[cid] = gen
    nsset = set(ns.values())
    out = []
    for n in child["nodes"]:
        n = dict(n)
        n["id"] = ns[n["id"]]                      # the id itself gets namespaced
        if isinstance(n.get("after"), list):
            n["after"] = [_rewrite_child_head(a, ns) for a in n["after"]]
        if isinstance(n.get("inputs"), list):
            n["inputs"] = [f"{_rewrite_child_head(ref.split('.', 1)[0], ns)}"
                           f"{('.' + ref.split('.', 1)[1]) if '.' in ref else ''}"
                           for ref in n["inputs"]]
        if isinstance(n.get("requires"), dict):
            n["requires"] = {ns.get(k, k): v for k, v in n["requires"].items()}
        fo = n.get("fanout")
        if isinstance(fo, dict) and isinstance(fo.get("ledger"), list):
            # #85: ledger rows carry '<node_id>.<path>' source refs — the sixth
            # id-ref surface, namespaced in lockstep with after/inputs/requires.
            n["fanout"] = dict(fo, ledger=[_ledger_ref_rewrite(r, lambda h: ns.get(h, h))
                                           for r in fo["ledger"]])
            fo = n["fanout"]
        if isinstance(fo, dict) and isinstance(fo.get("items_from"), str) \
                and fo.get("items") is None:
            head, _, rest = fo["items_from"].partition(".")
            n["fanout"] = dict(fo, items_from=f"{ns.get(head, head)}.{rest}" if rest
                               else ns.get(head, head))
            # items_from+after LOCKSTEP (:914-920 law): the rewritten head must be a
            # direct rewritten parent. Same map over both surfaces makes this
            # structural; assert anyway — a pre-rewrite authoring slip must not
            # survive into the merged graph.
            new_head = n["fanout"]["items_from"].split(".")[0]
            if new_head not in (n.get("after") or []):
                raise _include_error(alias, f"node {n['id']!r}: fanout."
                                            f"items_from head {new_head!r} is not in "
                                            f"its after list (the source must remain "
                                            f"a direct parent after namespacing)")
        if isinstance(n.get("when"), str):
            n["when"] = _include_when_rewrite(n["when"], lambda h: ns.get(h, h))
            # validate-time never existence-checks when heads -> do it HERE against
            # the namespace-local id set (the whole point of the mechanical rewrite).
            for h in _include_when_heads(n["when"]):
                if h not in nsset:
                    raise _include_error(alias, f"node {n['id']!r}: `when` references "
                                                f"out.{h} whose head is not a "
                                                f"node in the included graph — dangling "
                                                f"gate condition")
        out.append(n)
    return out, nsset


def _rewrite_parent_head(h, parent_ids, full_ns, aliases, exports_map, inner_bare,
                         alias_of_inner, nid, site):
    """Parent-ref-site mapping (design step 7). Parent may touch an included graph
    ONLY through namespaced names: a literal alias__id (kept), an exported public
    name (rewritten to the namespaced id). A bare inner id or a bare alias is a
    REFUSE — never a silent pass-through the door validator can't catch (when)."""
    if h in parent_ids or h in full_ns:
        return h
    if h in exports_map:
        return exports_map[h]
    head_alias = h.split(_INCLUDE_NS_SEP, 1)[0]
    if _INCLUDE_NS_SEP in h and head_alias in aliases:
        raise ValueError(f"include '{head_alias}': node {nid!r} {site} references "
                         f"{h!r} — no such node in that included graph "
                         f"(use an exported name or '{head_alias}__<id>')")
    if h in aliases:
        raise ValueError(f"include '{h}': node {nid!r} {site} names the include "
                         f"alias directly — an alias is not a node id (use "
                         f"'{h}__<id>' or an exported name)")
    if h in inner_bare:
        raise _include_error(alias_of_inner[h], f"node {nid!r} {site} references the "
                             f"bare inner id {h!r} of this include — parent graphs "
                             f"may only reach an included graph through "
                             f"'{alias_of_inner[h]}__{h}' or an exported name")
    return h  # not include-related: parent's own ref (dangling or not — door law)


def _expand_include_pass(graph, library_reader, notes, chain, depth):
    """One recursive pass: inner-first expansion of every include, namespacing,
    subtree rewrite, seed rendering, parent-ref rewrite, guards, and the strip.
    `chain` = tuple of library names on the expansion stack (cycle visit-set)."""
    def own(alias, msg):
        raise _include_error(alias, msg)

    nodes = graph.get("nodes", [])
    if not isinstance(nodes, list) or any(not isinstance(n, dict) for n in nodes):
        own(None, "graph nodes must be a list of node objects")
    # ABSENT key = nothing to expand (byte-identical passthrough, the golden-solo
    # pin). PRESENT-but-not-a-list — including explicit null — is a REFUSE: the
    # passthrough branch must never retain an `include` key, or the fused output
    # would violate the include-stripped storage contract (a committed graph.json
    # carrying `include` is by definition unexpanded).
    if "include" not in graph:
        return dict(graph, nodes=[dict(n) for n in nodes])
    includes = graph["include"]
    if not isinstance(includes, list) or not includes:
        own(None, "include must be a non-empty list of {as, use, seeds?, exports?} directives")


    # Issue #192 (PR#84 follow-up, D15-sibling): a malformed PARENT id (truthy but
    # unhashable — id:["bad"]) reached the parent_ids set build below and TypeErrors
    # before any validator could name it, leaking {error, trace} instead of the
    # errors[] envelope the include contract promises. Checked here — after the
    # include-shape refusals, before the first hashable-id contact. Include-free
    # graphs are untouched (they returned above at the passthrough; the door's own
    # full validator owns their malformed ids), and shelved CHILD graphs keep the
    # F-2 validate_graph_full refusal (named by alias): this guard only covers the
    # parent's own node list on the include path.
    for n in nodes:
        if n.get("id") is not None and not isinstance(n.get("id"), str):
            own(None, f"node id {n.get('id')!r} must be a string "
                      f"(malformed parent node id before expansion)")
    # --- guards first (structure, alias collisions, depth) ---
    aliases = []
    alias_of_inner = {}
    for inc in includes:
        if not isinstance(inc, dict):
            own(None, f"include entry {inc!r} is not an object")
        bad = sorted(set(inc) - INCLUDE_KEYS)
        if bad:
            own(inc.get("as"), f"unknown key(s) {bad}; allowed: {sorted(INCLUDE_KEYS)}")
        alias = inc.get("as")
        if not isinstance(alias, str) or not INCLUDE_ALIAS_OK.match(alias):
            own(alias, f"invalid alias {alias!r} (alnum start, [A-Za-z0-9_], "
                       f"no dots or hyphens, <= 24 chars)")
        use = inc.get("use")
        if not isinstance(use, str) or not INCLUDE_USE_OK.match(use):
            own(alias, f"invalid library name {use!r} (lowercase alnum start, "
                       f"[a-z0-9_.-], <= 64 chars)")
        if "exports" in inc and not isinstance(inc["exports"], dict):
            own(alias, "exports must be an object {inner_id: public_name}")
        if alias in aliases:
            own(alias, "duplicate alias — two includes share the alias "
                       f"{alias!r} (every alias must be unique)")
        aliases.append(alias)
    if depth + 1 > INCLUDE_DEPTH_MAX:
        own(aliases[0], f"nested includes exceed the maximum depth of "
                        f"{INCLUDE_DEPTH_MAX} (chain: {' -> '.join(chain)})")

    parent_ids = {n.get("id") for n in nodes if n.get("id")}
    expanded = [dict(n) for n in nodes]          # parent nodes, rewritten after grafting
    merged = []                                  # grafted included nodes, decl order
    exports_map = {}                             # public_name -> namespaced id
    export_claims = []                           # [(public, alias, ns_id)] — F-4 pass 1
    full_ns = set()                              # every generated alias__id
    inner_bare = set()                           # every child inner id (bare-ref refuse)
    child_scratch_seen = set()                   # paths in previously expanded graphs
    child_policies = []                          # [(alias, forbidden_models)] to union at fuse
    child_require_aliases = []                   # [(alias, bool)] — require_model floor
    parent_scratch = set()
    for n in nodes:
        parent_scratch.update(_include_scratch_paths(n))

    for inc in includes:
        alias, use = inc["as"], inc["use"]
        if use in chain:
            own(alias, f"include cycle: {' -> '.join(chain)} -> {use}")
        entry = library_reader(use)
        if entry is None:
            own(alias, f"unknown library entry {use!r} — shelve it first "
                       f"(save graph=...) or fix the include's use")
        if not isinstance(entry, dict):
            own(alias, f"library entry {use!r} is not a graph object")
        if entry.get("defaults"):
            notes.append(f"{alias}: included graph's `defaults` are NOT applied — "
                         f"the merged run bakes under the parent's defaults only")
        # inner-first: recursively expand the child BEFORE namespacing it
        child = _expand_include_pass(entry, library_reader, notes,
                                     chain + (use,), depth + 1)
        # PR#84 review F-2: the shelf is measured with validate_graph_full — the
        # SAME complete structural+node validator a submitted graph gets at the
        # door — not the node-only validate_graph_errors. Before this, the lossy
        # top-level projection (fused = parent keys minus include) dropped a
        # child's unknown graph key / bad defaults / bad provenance before the
        # door ever saw them: refused when submitted directly, silently accepted
        # when included. The full pass ALSO runs before any hashable-id contact
        # (validate_graph_errors' duplicate-id `set(ids)` used to TypeError on
        # id:["bad"] and leak a trace) — a malformed child now gets the named
        # include-envelope refusal, same contract as direct submission.
        cerr = validate_graph_full(child)
        if cerr:
            e = cerr[0]
            where = e["node"] or "graph"
            own(alias, f"library entry {use!r} fails to validate standalone: "
                       f"{where}: {e['msg']}")
        # child model_policy.forbidden_models must SURVIVE composition (unioned
        # into the parent at fuse; see below) — a shelved graph that forbids a
        # model may not stop forbidding it because someone included it. The
        # child is already recursively expanded, so a nested composite's merged
        # policy rides up transitively at this level. Anything malformed here is
        # a named refusal, never a silent drop. The rules are ONE list with the
        # door's (model_policy_errors): an int/str require_model:0 that direct
        # submission refuses can no longer slip in as truthy through the bool().
        cp = child.get("model_policy")
        if cp is not None:
            cperr = model_policy_errors(cp)
            if cperr:
                own(alias, f"library entry {use!r} has an invalid model_policy: "
                           + "; ".join(cperr))
            if cp.get("require_model") is not None:
                child_require_aliases.append((alias, cp["require_model"]))
            _fb = cp.get("forbidden_models")
            if _fb:
                child_policies.append((alias, list(_fb)))
        child_nodes, nsset = _include_namespace_child(child, alias, use)

        # --- seed contract: render {run.KEY} inside the subtree ONLY ---
        seeds = inc.get("seeds")
        if seeds is not None:
            _include_seed_scan(seeds, alias)
            rendered = []
            for n in child_nodes:
                n = dict(n)
                for label, path in _include_text_fields(n):
                    text = _include_get(n, path)
                    is_fo = label.startswith("fanout") or (label == "goal"
                                                           and isinstance(n.get("fanout"), dict))
                    val = _include_render(text, seeds, fanout=is_fo, alias=alias,
                                          where=f"node {n['id']} {label}")
                    # write back through the generic copy-on-write setter: the
                    # old hand-rolled fanout-chain rebuild ASSUMED every nested
                    # path was fanout, so options[i] and wait.until_argv[i]
                    # could never land (PR#84 round-2 fix caution).
                    n = _include_set(n, path, val)
                rendered.append(n)
            child_nodes = rendered

        # --- shared fixed-scratch-path detection (note, never rewrite) ---
        child_paths = set()
        for n in child_nodes:
            child_paths.update(_include_scratch_paths(n))
        for p in sorted((child_paths & parent_scratch) | (child_paths & child_scratch_seen)):
            notes.append(f"{alias}: fixed path {p} shared; run single-instance or "
                         f"materialize")
        child_scratch_seen |= child_paths

        # --- exports: inner_id -> public_name, verified against the child ids ---
        # PR#84 review F-4: the namespace-shadowing half is a TWO-PASS check —
        # claims are collected here and measured after the loop against the
        # COMPLETE fused namespace. The old in-loop check saw only namespaced ids
        # generated SO FAR: include A exporting `s` as `b__s` passed, then include
        # B minted a real `b__s` node, and the parent-ref mapper preferred the
        # real node — A's exported verdict silently resolved to B's, and only the
        # reverse declaration order refused. Declaration order must never decide
        # which shelf a public name points at.
        for inner, public in (inc.get("exports") or {}).items():
            ns_id = f"{alias}{_INCLUDE_NS_SEP}{inner}"
            if not isinstance(inner, str) or ns_id not in nsset:
                own(alias, f"exports names {inner!r} — no such node in library "
                           f"entry {use!r}")
            if not isinstance(public, str) or not ID_OK.match(public):
                own(alias, f"exports public name {public!r} is not a valid id "
                           f"(alnum start, [A-Za-z0-9_.-], <= 64)")
            if public in exports_map:
                own(alias, f"exports public name {public!r} collides with include "
                           f"'{exports_map[public].split(_INCLUDE_NS_SEP, 1)[0]}'")
            exports_map[public] = ns_id
            export_claims.append((public, alias, ns_id))

        if nsset & (parent_ids | full_ns):
            clash = sorted(nsset & (parent_ids | full_ns))[0]
            own(alias, f"namespaced id {clash!r} collides with an existing node id "
                       f"in the merged graph — rename the parent node or the alias")
        inner_bare |= {n["id"] for n in child["nodes"]}   # child-form raw ids
        for n in child["nodes"]:
            alias_of_inner.setdefault(n["id"], alias)
        full_ns |= nsset
        merged.extend(child_nodes)

        # --- per-include merged-size guards ---
        if len(expanded) + len(merged) > INCLUDE_NODES_MAX:
            own(alias, f"merged graph would exceed the node-count cap "
                       f"({INCLUDE_NODES_MAX}) — the include pushes the composite "
                       f"past the materialize budget")
        candidate = json.dumps(dict(graph, nodes=expanded + merged), sort_keys=True,
                               ensure_ascii=False, separators=(",", ":"))
        if len(candidate.encode()) > INCLUDE_BYTES_MAX:
            own(alias, f"merged graph would exceed the size cap "
                       f"({INCLUDE_BYTES_MAX} bytes) — do not include graphs this "
                       f"large into an already-large parent")

    # --- F-4 pass 2: export claims vs the COMPLETE fused namespace. By here
    # full_ns holds every alias__id of EVERY include (pass 1 only saw the ones
    # generated so far), parent_ids and aliases were closed before the loop — so
    # every refusal below is order-independent. `public != ns_id` keeps the
    # benign self-shape (a public name that IS the claim's own namespaced id:
    # the ref mapper resolves it to the same node) out of the refusal, matching
    # the old in-loop position's tolerance; everything else shadowing any real
    # id, alias, or namespace is refused no matter which include came first.
    for public, alias, ns_id in export_claims:
        if public != ns_id and (public in parent_ids or public in aliases
                                or public in full_ns):
            own(alias, f"exports public name {public!r} shadows an existing "
                       f"node id or include alias")

    # --- parent ref sites rewritten against the include boundary (step 7) ---
    def map_site(nid, site, h):
        return _rewrite_parent_head(h, parent_ids, full_ns, set(aliases), exports_map,
                                    inner_bare, alias_of_inner, nid, site)
    rewritten_parent = []
    for n in expanded:
        n = dict(n)
        nid = n.get("id")
        if isinstance(n.get("after"), list):
            n["after"] = [map_site(nid, "after", a) for a in n["after"]]
        if isinstance(n.get("inputs"), list):
            n["inputs"] = [f"{map_site(nid, 'inputs', ref.split('.', 1)[0])}"
                           f"{('.' + ref.split('.', 1)[1]) if '.' in ref else ''}"
                           for ref in n["inputs"]]
        if isinstance(n.get("requires"), dict):
            n["requires"] = {map_site(nid, "requires", k): v
                             for k, v in n["requires"].items()}
        fo = n.get("fanout")
        new_item_head = None
        if isinstance(fo, dict) and isinstance(fo.get("ledger"), list):
            # #85: ledger source heads are parent refs at the include boundary —
            # mapped like inputs/requires (the sixth id-ref surface, in lockstep).
            fo = dict(fo, ledger=[_ledger_ref_rewrite(r, lambda h: map_site(nid, "fanout.ledger", h))
                                  for r in fo["ledger"]])
            n["fanout"] = fo
        if isinstance(fo, dict) and isinstance(fo.get("items_from"), str) \
                and fo.get("items") is None:
            head, _, rest = fo["items_from"].partition(".")
            new_head = map_site(nid, "fanout.items_from", head)
            new_item_head = f"{new_head}.{rest}" if rest else new_head
            n["fanout"] = dict(fo, items_from=new_item_head)
        if isinstance(n.get("when"), str):
            n["when"] = _include_when_rewrite(n["when"], lambda h: map_site(nid, "when", h))
        if new_item_head is not None and \
                new_item_head.split(".")[0] not in (n.get("after") or []):
            raise _include_error(None, f"node {nid!r}: fanout.items_from head "
                                 f"{new_item_head!r} must be a direct parent in the "
                                 f"node's after list (items_from+after lockstep)")
        rewritten_parent.append(n)

    fused = {k: v for k, v in graph.items() if k != "include"}   # strip-on-expand
    fused["nodes"] = rewritten_parent + merged
    # C1 story: an included graph's model_policy.forbidden_models is a shelved
    # safety floor — including a graph must never relax what it forbids. UNION
    # (set semantics, deterministic sorted order) into the parent's policy;
    # require_model ORs upward below; other child top-level keys (defaults, ...)
    # are NOT carried and are noted where meaningful.
    # PR#84 review F-2: the PARENT's model_policy is type-checked BEFORE the
    # forbidden union / require_model OR run, with the same model_policy_errors
    # list the door uses. Without this the merge itself repaired a malformed
    # parent: require_model:'false' is truthy, `want = pol.get(...) or any(...)`
    # kept it, and the write-back stored boolean True — a policy that direct
    # submission refuses emerged from composition as a coerced floor. Malformed
    # parent policy is now a named refusal, never a silent repair. (Checked on
    # any composition that touches the policy — union OR require propagation.)
    if (child_policies or child_require_aliases) and "model_policy" in fused:
        _perr = model_policy_errors(fused["model_policy"])
        if _perr:
            own(None, "parent model_policy is invalid before a child policy can "
                      "merge into it: " + "; ".join(_perr))
    if child_policies:
        if "model_policy" in fused and not isinstance(fused["model_policy"], dict):
            own(None, "parent model_policy must be an object before a child policy "
                      "can merge into it (the door's validator rejects the shape "
                      "too — refused here so the merge never crashes)")
        pol = dict(fused.get("model_policy") or {})
        merged_fb = set(pol.get("forbidden_models") or [])
        for alias, fb in child_policies:
            added = set(fb) - merged_fb
            merged_fb |= set(fb)
            notes.append(f"{alias}: model_policy.forbidden_models merged into "
                         f"parent" + ("" if added else " (already covered)"))
        pol["forbidden_models"] = sorted(merged_fb)
        fused["model_policy"] = pol
    if child_require_aliases:
        # require_model is a floor too: an included graph whose shelf demands an
        # explicit model per agent node must not stop demanding it because someone
        # included it. OR it upward (never down — a parent asserting True keeps it;
        # a child asserting False cannot relax a parent's True). Loud by note.
        pol = dict(fused.get("model_policy") or {})
        want = pol.get("require_model", False) or any(v for _, v in child_require_aliases)
        if want != bool(pol.get("require_model", False)):
            names = ", ".join(a for a, v in child_require_aliases if v)
            notes.append(f"{names}: model_policy.require_model propagated to the "
                         "composite (every agent node must name an explicit model)")
        if want:
            pol["require_model"] = True
            fused["model_policy"] = pol
    # The per-include guards above measured the PRE-rewrite candidate; the parent
    # ref rewrite and the policy merge just happened. Re-check the FINAL fused
    # artifact (door's graph.json write form) against both caps — an oversized
    # composite must refuse here, not ship bytes the door will clip or choke on.
    if len(fused["nodes"]) > INCLUDE_NODES_MAX:
        own(None, f"expanded graph exceeds the node-count cap "
                  f"({INCLUDE_NODES_MAX}: {len(fused['nodes'])} nodes) after the "
                  f"parent-reference rewrite")
    if len(json.dumps(fused).encode()) > INCLUDE_BYTES_MAX:
        own(None, f"expanded graph exceeds the size cap ({INCLUDE_BYTES_MAX} "
                  f"bytes) after the parent-reference rewrite — the merged form, "
                  f"not just the candidate, must fit the materialize budget")
    return fused


def expand_includes(graph, library_reader):
    """expand_includes(graph, library_reader) -> (expanded_graph, notes[]).

    Expand a graph's top-level `include` annotation into a plain, include-STRIPPED
    wf/1 graph: every included library graph is recursively expanded inner-first,
    namespaced (`alias__<inner-id>`), rewritten across the five id-ref surfaces,
    seed-rendered on {run.KEY} INSIDE its subtree only, then spliced; the parent's
    refs to the include must use exported names or literal alias__id (bare inner
    ids are refused). Every guard is a ValueError with a named single-string
    message (missing/cyclic/oversized include, alias/id collision, depth cap,
    unbound seed, dot/overflow id). notes[] carries non-fatal warnings — shared
    fixed-scratch-path collisions between included graphs and the parent (or other
    includes) are reported there, NEVER rewritten. library_reader(name) -> dict is
    injected by the door; this module never touches the library root."""
    if not isinstance(graph, dict):
        raise ValueError("expand_includes: graph must be an object {name, nodes, include?}")
    if not callable(library_reader):
        raise ValueError("expand_includes: library_reader must be callable "
                         "(name -> graph dict | None)")
    notes = []
    expanded = _expand_include_pass(graph, library_reader, notes, chain=("<graph>",),
                                    depth=0)
    return expanded, notes


def include_provenance(graph, library_reader):
    """[{alias, name, source_digest}] for every include a graph uses — the stamp the
    door writes into run.json so each expanded run records which shelf's node bytes
    it ran with. source_digest hashes the canonical `nodes` list only (its standing
    contract): it pins node content, NOT include directives, seed values, or
    model_policy — an edit outside `nodes` keeps the digest (PR#84 review F-6: the
    prose says exactly this, never promising whole-author-file integrity). Walks the author form (include keys intact), recursively, in declaration
    order, inner-aliased under their parents (`outer__inner`); digests are over the
    raw library entries as read (shelf bytes, not the expansion). [] when the graph
    carries no includes (a stripped/expanded graph is honestly provenance-less here:
    the stamp belongs to the author form)."""
    out = []
    def walk(g, prefix, chain):
        for inc in (g.get("include") or []):
            if not isinstance(inc, dict) or "as" not in inc or "use" not in inc:
                raise ValueError(f"include provenance: malformed directive {inc!r}")
            alias = f"{prefix}{_INCLUDE_NS_SEP}{inc['as']}" if prefix else inc["as"]
            use = inc["use"]
            if use in chain:
                raise ValueError(f"include '{alias}': include cycle: "
                                 f"{' -> '.join(chain)} -> {use}")
            entry = library_reader(use)
            if entry is None:
                raise ValueError(f"include '{alias}': unknown library entry {use!r}")
            if not isinstance(entry, dict):
                raise ValueError(f"include '{alias}': library entry {use!r} is not "
                                 f"a graph object")
            out.append({"alias": alias, "name": use,
                        "source_digest": source_digest(entry)})
            walk(entry, alias, chain + (use,))
    if isinstance(graph, dict):
        walk(graph, "", ("<graph>",))
    return out


def quote_json_parse_error(text, exc):
    """±40 chars of the source around the offset of a JSONDecodeError — what the
    door shows when a graph ever arrives as a malformed JSON string."""
    t = str(text)
    off = getattr(exc, "pos", None)
    if not isinstance(off, int) or off < 0 or off > len(t):
        return t[:80]
    return t[max(0, off - 40):off + 40]

# ---------- runner identity and exit record (Lane A writes, this side ONLY reads) ----------
#
# #44 (fix/lock-heal-44b) — the lock truth of a run dir is a TRI-STATE read, never
# a bool. The in-process bool probe is what wedged the fleet: a door SIGKILLed
# mid-probe can strand the open file description it created, and the flock rides
# that fd beyond the door's death (observed 2026-09-29: Errno 11 forever, children
# gone, wait-resume permanently impossible). Three laws, both PR #47 review blocks
# honored:
#   B1  EACCES on an EXISTING lock is 'unknown', NEVER free: a permission failure
#       proves nothing about holders; PR #47 mapped it to missing->dead and let a
#       wait respawn over a LIVE holder.
#   B2  probe -> heal is closed against a concurrently-advancing holder: the heal's
#       FINAL re-verification runs under the admission heal-barrier and reads the
#       kernel ledger (/proc/locks) — a holder that wakes between check and commit
#       shows a row and the heal refuses. The check and the commit observe the same
#       kernel truth with nothing mutable shared between observers (no per-path
#       side state at all — every observation returns its own verdict+detail).
#       The wedge is healed by REPLACE (rename the stranded inode aside — never
#       unlink, evidence kept — and hand out a NEW path/inode), NOT by stealing:
#       the same inode an invisible orphan still holds stays exactly where it is,
#       so a live-or-ambiguous holder is never displaced.
# The probe NEVER takes the flock in-process: a short-lived child opens + flocks +
# exits; the child's death IS the release, so the door's own fd table never holds
# the lock across anything (issue ask 1's kill-the-probe-mid-flight contract).

PROC_LOCKS_PATH_ENV = "WF_PROC_LOCKS_PATH"   # test seam; default /proc/locks verbatim
RUNNER_LEASE = "runner.lease"                # admission identity record (wf.py writes)
LEASE_STALE_S = 120.0                        # a live runner heartbeats 'at' every loop
LOCK_PROBE_CHILD = (
    # opens WITHOUT O_CREAT (read-path no-litter law: a missing file is honest
    # absence), flocks EX|NB, answers one word on stdout. Its exit releases.
    "import errno,fcntl,os,sys\n"
    "try:\n"
    "    fd=os.open(sys.argv[1],os.O_RDWR)\n"
    "except FileNotFoundError:\n"
    "    print('missing'); raise SystemExit(0)\n"
    "except PermissionError:\n"
    "    print('eacces'); raise SystemExit(0)\n"
    "except OSError as e:\n"
    "    print('errno'+str(e.errno)); raise SystemExit(0)\n"
    "try:\n"
    "    try:\n"
    "        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
    "        print('free')\n"
    "    except OSError as e:\n"
    "        print('busy' if e.errno==errno.EAGAIN else 'errno'+str(e.errno))\n"
    "finally:\n"
    "    os.close(fd)\n")

def _lock_probe_child(lock_path, timeout=5.0):
    """One word from a dying process: missing|eacces|free|busy|errno<n>|unknown.
    'unknown' = the probe itself could not be trusted (fork failure, timeout,
    garbage) — never evidence of absence, never evidence of presence."""
    try:
        p = subprocess.run([sys.executable, "-c", LOCK_PROBE_CHILD, str(lock_path)],
                           capture_output=True, text=True, timeout=timeout)
    except Exception:
        return "unknown"
    out = (p.stdout or "").strip().splitlines()
    word = out[-1].strip() if out else ""
    return word if word in ("missing", "eacces", "free", "busy") or word.startswith("errno") \
        else "unknown"

_LOCK_PROBE_BATCH_CHILD = (
    # #44 (PR #47 C2 law): ONE child probes the WHOLE batch — a census of 50 run
    # dirs costs one spawn, not fifty. A poisoned path (fork-time error on one
    # entry) answers 'unknown' for that entry and preserves the other answers.
    "import errno,fcntl,json,os,sys\n"
    "out={}\n"
    "for k,p in json.loads(sys.argv[1]).items():\n"
    "    try:\n"
    "        fd=os.open(p,os.O_RDWR)\n"
    "    except FileNotFoundError:\n"
    "        out[k]='missing'; continue\n"
    "    except PermissionError:\n"
    "        out[k]='eacces'; continue\n"
    "    except OSError as e:\n"
    "        out[k]='errno'+str(e.errno); continue\n"
    "    try:\n"
    "        try:\n"
    "            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
    "            out[k]='free'\n"
    "        except OSError as e:\n"
    "            out[k]='busy' if e.errno==errno.EAGAIN else 'errno'+str(e.errno)\n"
    "    finally:\n"
    "        os.close(fd)\n"
    "print(json.dumps(out))\n")

def _lock_probe_children(lock_paths, timeout=8.0):
    """Batch child-probe: {path: word}. Any batch failure answers 'unknown' for
    EVERY entry (fail-closed busy, never free)."""
    paths = [str(p) for p in lock_paths]
    if not paths:
        return {}
    try:
        p = subprocess.run([sys.executable, "-c", _LOCK_PROBE_BATCH_CHILD,
                            json.dumps({str(i): q for i, q in enumerate(paths)})],
                           capture_output=True, text=True, timeout=timeout)
        raw = json.loads((p.stdout or "").strip().splitlines()[-1])
    except Exception:
        return {q: "unknown" for q in paths}
    return {q: (raw.get(str(i)) or "unknown") for i, q in enumerate(paths)}

def _proc_locks_index():
    """The kernel ledger in ONE read: {dev:ino-hex: [pid, ...]} for advisory
    FLOCK rows, or None when the ledger itself is unreadable (honest unknown —
    callers fail-closed to busy, never free)."""
    out = {}
    try:
        with open(os.environ.get(PROC_LOCKS_PATH_ENV) or "/proc/locks",
                  errors="replace") as f:
            for line in f:
                fields = line.split()
                #  343: FLOCK  ADVISORY  WRITE  PID DEV:INO 0 EOF
                if len(fields) >= 6 and fields[1] == "FLOCK":
                    try:
                        out.setdefault(fields[5], []).append(int(fields[3]))
                    except ValueError:
                        out.setdefault(fields[5], []).append(-1)
    except OSError:
        return None
    return out

def _ledger_pids_for(ledger, path):
    """PIDs holding an advisory flock on this inode's (dev, ino): [] via a REAL
    ledger read means VERIFIED zero holders; None means the ledger or the
    inode identity was unreadable (fail-closed). The kernel's OWN answer to
    'who holds this flock' — it sees holders our /proc view cannot (sibling
    pid namespaces)."""
    if ledger is None:
        return None
    try:
        want = os.stat(path)
        # kernel format: major:minor are %02x, the inode is DECIMAL
        # (`00:43:28345670`) — verified against /proc/locks; hex inode never
        # matches and silently launders every held lock to zero-row.
        key = "%02x:%02x:%d" % (os.major(want.st_dev), os.minor(want.st_dev),
                                want.st_ino)
    except OSError:
        return None
    return ledger.get(key, [])

def _lease_state(r):
    """Read the admission lease: (verdict, detail). A lease is written at
    admission and HEARTBEATED (field 'at') by the live holder (wf.py
    acquire_lock/_lease_beat) — two independent proofs, both fail-closed LIVE:
      live    either (a) pid+boottime verify against /proc right now (identity —
              boottime mismatch is the anti-pid-reuse proof, so a stale 'at' on
              a VERIFIED identity is a paused holder, not a corpse), or
              (b) the heartbeat is fresh (only a living process writes 'at'; a
              sibling-namespace holder we cannot see still beats — the 91b9a3de
              fleet shape), or the pid exists but is EPERM (someone else's
              world — unverified is never proof of death).
      dead    ONLY when the kernel disproves BOTH proofs at once: the pid is
              absent (ESRCH) AND the heartbeat is stale (older than
              LEASE_STALE_S, so it cannot be a beat in flight), or the pid is
              present with a DIFFERENT boottime (pid reuse — kernel-proven).
      absent  no lease / unreadable — a legacy dir: honest absence, fail-closed
              to the 91b9a3de law verbatim (held => live)."""
    try:
        rec = jload(Path(r) / RUNNER_LEASE)
    except Exception:
        rec = None
    if not isinstance(rec, dict):
        return "absent", {}
    pid, bt, at = rec.get("pid"), rec.get("boottime"), rec.get("at")
    try:
        pid = int(pid)
        bt = int(bt)
        at = float(at)
    except (TypeError, ValueError):
        return "absent", rec
    fresh = (time.time() - at) <= LEASE_STALE_S
    perm = False
    try:
        os.kill(pid, 0)
        pid_exists = True
    except OSError as e:
        pid_exists = e.errno == 1               # EPERM: exists, not ours to signal
        perm = e.errno == 1
    if pid_exists and not perm:
        try:
            rest = Path(f"/proc/{pid}/stat").read_text(errors="replace").rsplit(")", 1)[1].split()
            observed_bt = int(rest[19])
        except (OSError, IndexError, ValueError):
            return "live", rec                  # kill-0 passed, row unreadable:
                                                # cannot disprove identity
        if observed_bt == bt:
            return "live", rec                  # identity VERIFIED (outranks a stale beat)
        return "dead", rec                      # pid reuse: the kernel says so
    if pid_exists:                              # EPERM — verified to exist
        return "live", rec
    # pid NOT visible here: the cross-namespace shape (91b9a3de) OR genuinely
    # dead. The heartbeat is the tie-breaker only a live process can pull.
    if fresh:
        return "live", rec                      # beating right now => alive next door
    return "dead", rec                          # invisible AND silent: proven dead

def _lease_live(r):
    return _lease_state(r)[0] == "live"

def _classify_lock_word(word, lock, r, ledger):
    """Fold one probe word + kernel ledger + lease into (state, detail).
      free    absent file, or uncontended probe (kernel proved nobody holds it)
      eacces  the path EXISTS but is unreadable — permission proves NOTHING about
              holders (B1): never free, never dead, heal must refuse
      busy    probe says held AND the kernel ledger has a row, or the ledger is
              unreadable, or a live/fresh lease vouches (cross-ns fleet shape)
      wedged  probe Errno 11, ZERO ledger rows for the inode, AND the lease is
              proven dead: no live holder can be found anywhere we can look, yet
              the inode will not be granted. The #44 field shape.
    Shared by the single probe and the batch census so BOTH answer from the same
    evidence with NO mutable per-path side state (the observer-race class)."""
    if word in ("missing", "free"):
        return "free", {"probe": word}
    if word == "eacces":
        return "eacces", {"probe": word}
    if word != "busy":
        # A probe we could not trust (unknown/errno<n>) proves nothing either way
        # — fail-closed busy, NEVER free (the false-dead class PR #47 shipped).
        return "busy", {"probe": word}
    pids = _ledger_pids_for(ledger, lock)
    if pids is None:
        return "busy", {"probe": word, "ledger": "unreadable"}
    lv, lease = _lease_state(r)
    if pids:
        return "busy", {"probe": word, "ledger_pids": pids}
    if lv == "absent":
        # legacy dir pre-dating the lease: the 91b9a3de law applies verbatim —
        # held => live. Zero rows across a mount boundary is not proof of absence
        # (the field shows exactly that). Only the explicit operator verb
        # (release_lock) carries authority over this shape; wait never heals it.
        return "busy", {"probe": word, "ledger": "zero", "lease": "absent-legacy"}
    if lv in ("live", "fresh"):
        return "busy", {"probe": word, "ledger": "zero", "lease": lv}
    return "wedged", {"probe": word, "ledger": "zero", "lease": lv,
                      "holder_pid": lease.get("pid")}

def runner_lock_state(r):
    """The ONE lock truth of a run dir: (state, detail) — see _classify_lock_word.
    Deliberately NO O_CREAT anywhere in the probe (read-path no-litter law)."""
    r = Path(r)
    lock = r / "runner.lock"
    word = _lock_probe_child(lock)
    return _classify_lock_word(word, lock, r, _proc_locks_index())

def runner_lock_states(runs):
    """Batch census of run dirs: {run: (state, detail)} with ONE probe child and
    ONE ledger read for the WHOLE batch (the dashboard/act_list read path was
    spawning one probe per row; #47 round-3 sibling gap, closed here)."""
    runs = [Path(x) for x in runs]
    if not runs:
        return {}
    words = _lock_probe_children([r / "runner.lock" for r in runs])
    ledger = _proc_locks_index()
    return {r: _classify_lock_word(words[str(r / "runner.lock")],
                                    r / "runner.lock", r, ledger) for r in runs}

def runner_lock_held(r):
    """91b9a3de: cross-container liveness truth. The runner takes an exclusive
    blocking flock on <run>/runner.lock for its whole life (wf.py acquire_lock)
    — and flock is enforced by the KERNEL across every pid namespace that shares
    the mount, so it holds even when the runner lives in a sibling container (our
    fleet: shared ~/.hermes volume, separate pid namespaces). os.kill(pid, 0)
    CANNOT see those pids: the same pid reads as dead here while it is very much
    alive next door, and a wait-based caller respawns a second runner over live
    children (the fb-squad false-'interrupted' class). #44: the probe is a
    short-lived CHILD (the child's death is the release — a SIGKILLed door never
    strands an fd it never opened) and the answer is tri-state; this bool view
    reads HELD for anything that is not proven free-or-wedged. Missing file is
    exactly as honest an absence of a holder as an uncontended one. EACCES and
    probe failure count HELD (fail-closed, B1/B2 law)."""
    state, _ = runner_lock_state(r)
    return state not in ("free", "wedged")

def heal_wedged_runner_lock(r, between_check_and_commit=None):
    """The #44 escape hatch (issue ask 2), safe against BOTH PR #47 blocks.

    Refuses unless the wedge is PROVEN (probe busy + zero kernel ledger rows +
    lease proven dead); on EACCES/unknown/ledger-unreadable/holder-alive it
    REFUSES, never heals (B1). The commit happens under the SAME admission
    heal-barrier wf.py acquire_lock takes before it ever opens the lock path
    (runner.lock.heal, LOCK_EX|LOCK_NB): a cooperating admission cannot be
    mid-flock while we commit. Immediately before the rename the state is
    RE-VERIFIED — fresh probe + fresh ledger scan + fresh lease read — so a
    holder that wakes between check and commit shows its kernel row or wins the
    fresh probe and the heal REFUSES (B2; the test injects exactly that
    interleaving). The stranded inode is RENAMED aside (runner.lock.stranded-*,
    never unlinked: it belongs to whoever holds it and stays forensically
    available); a NEW path takes over. A live holder of the OLD inode is never
    displaced — nothing steals a lock whose holder is alive-or-ambiguous: the
    replacement hands the lane a fresh inode while the orphan keeps the corpse.
    Returns {ok, reason} and appends a run.lock_healed event (derive-of-record:
    the event only rides after the rename actually happened)."""
    r = Path(r)
    lock = r / "runner.lock"
    state, detail = runner_lock_state(r)
    if state == "free":
        return {"ok": True, "reason": "free", "detail": detail}   # idempotent no-op
    if state != "wedged":
        # 'busy' proves a holder (kernel row) or refuses to disprove one (ledger
        # unreadable / legacy lease) — either way the refusal names the holder
        # ground; eacces names the permission ground (B1).
        _why = "holder-alive" if state == "busy" else f"not-wedged:{state}"
        return {"ok": False, "reason": _why, "detail": detail}
    barrier = r / "runner.lock.heal"
    try:
        import fcntl
        bfd = os.open(str(barrier), os.O_CREAT | os.O_RDWR, 0o644)
    except OSError as e:
        return {"ok": False, "reason": f"barrier-open-errno{e.errno}", "detail": detail}
    try:
        try:
            fcntl.flock(bfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # An admission is IN FLIGHT on this lane. It either wins the lock
            # (a holder — heal refuses, it won) or dies; re-probing after a
            # bounded wait lets the ledger, not this function, decide.
            try:
                fcntl.flock(bfd, fcntl.LOCK_EX)      # wait: admission is bounded
            except OSError:
                return {"ok": False, "reason": "barrier-unlockable", "detail": detail}
        if between_check_and_commit is not None:
            between_check_and_commit()               # test seam: the waking holder
        # --- B2: the SAME kernel truth again, atomically before the commit. ---
        state2, detail2 = runner_lock_state(r)
        if state2 != "wedged":
            return {"ok": False, "reason": f"holder-advanced:{state2}", "detail": detail2}
        strand = None
        if lock.exists():
            strand = r / f"runner.lock.stranded-{int(time.time())}-{os.getpid()}"
            try:
                os.rename(str(lock), str(strand))
            except OSError as e:
                return {"ok": False, "reason": f"rename-errno{e.errno}", "detail": detail2}
        try:
            fd = os.open(str(lock), os.O_CREAT | os.O_RDWR, 0o644)
            os.close(fd)                             # new path/inode: lockable fresh
        except OSError as e:
            if strand is not None:                   # put the corpse back; no-op heal
                try: os.rename(str(strand), str(lock))
                except OSError: pass
            return {"ok": False, "reason": f"recreate-errno{e.errno}", "detail": detail2}
        try:
            with open(r / "events.jsonl", "a") as f:
                f.write(json.dumps({"ts": datetime.now(timezone.utc).isoformat(
                    timespec="seconds"), "event": "run.lock_healed",
                    "stranded": str(strand) if strand else None,
                    "detail": {k: v for k, v in detail2.items() if k != "probe"}}) + "\n")
        except OSError:
            pass                                     # event legibility, never fatal
        return {"ok": True, "reason": "healed", "stranded": str(strand) if strand else None,
                "detail": detail2}
    finally:
        try:
            fcntl.flock(bfd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(bfd)

def _runner_pid_alive(r, pid_path=None):
    """A pid is not ownership: verify a live, non-zombie `wf.py run <id>`.
    /proc gives argv and (when readable) the runner's environment; ps supplies macOS/BSD.
    Unknown identity is NOT evidence of a running workflow.
    1.1 (RATIFY F1/B3): the environment check compares the runner's EFFECTIVE runs root
    with this run's parent: `WF_RUNS_ROOT` (when the runner has one) must equal `r.parent`;
    otherwise the 1.0.15 comparison `HERMES_HOME == r.parent.parent` applies verbatim."""
    r = Path(r)
    try:
        pid = int(Path(pid_path or r / "wf.pid").read_text().strip())
        if pid <= 0:
            return False
        os.kill(pid, 0)
        try:
            os.waitpid(pid, os.WNOHANG)  # our own zombie, if still unreaped
        except (ChildProcessError, OSError, AttributeError):
            pass
        try:
            state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().decode(errors="replace").split("\0")
            argv = [a for a in argv if a]
            try:
                environ = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
                env = {}
                for entry in environ:   # first occurrence wins (as the legacy homes[0] did)
                    k, sep, v = entry.partition(b"=")
                    if sep and k in (b"HERMES_HOME", b"WF_RUNS_ROOT") and k not in env:
                        env[k.decode()] = v.decode(errors="replace")
                # #42: an owner-pinned settings.runs_root is the ONE root every process of
                # the estate runs under — the runner's env is judged against it. Otherwise
                # the 1.1 / 1.0.15 comparisons apply VERBATIM.
                try:
                    pinned = settings_runs_root()
                except ValueError:
                    pinned = None   # liveness is a read path: a bad setting is the door's error
                if pinned is not None:
                    if pinned.resolve() != r.parent.resolve():
                        return False
                elif env.get("WF_RUNS_ROOT"):
                    # the runner runs under an explicit runs root: it must BE this run's parent
                    if Path(env["WF_RUNS_ROOT"]).resolve() != r.parent.resolve():
                        return False
                elif env.get("HERMES_HOME"):
                    # legacy env: the 1.0.15 comparison, unchanged (effective root is
                    # HERMES_HOME/workflows, i.e. HERMES_HOME == r.parent.parent)
                    if Path(env["HERMES_HOME"]).resolve() != r.parent.parent.resolve():
                        return False
            except OSError:
                pass
        except OSError:  # macOS/BSD: ps gives state AND full command
            row = subprocess.run(["ps", "-ww", "-p", str(pid), "-o", "stat=", "-o", "command="],
                                 capture_output=True, text=True, check=True, timeout=2).stdout.strip()
            state, cmd = row.split(None, 1)
            argv = shlex.split(cmd)
        return not state.startswith("Z") and len(argv) >= 3 and Path(argv[-3]).name == "wf.py" \
            and argv[-2:] == ["run", r.name]
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return False

def runner_alive(r, pid_path=None, lock_state=None):
    """91b9a3de: ONE liveness law, cross-container safe. The runner holds an
    exclusive flock on <run>/runner.lock for its whole life, and flock is
    kernel-enforced across every pid namespace sharing the mount — so the probe
    reads a sibling-container runner (our fleet: shared ~/.hermes volume, separate
    pid namespaces) as LIVE where os.kill(pid, 0) alone sees a ghost and a
    next=wait would spawn a second runner over live children. Law: HELD lock =>
    live, unconditionally (the holder proved itself by taking the lock, whatever
    its pid namespace). Otherwise fall back to the ORIGINAL pid-identity law
    verbatim (argv `wf.py run <id>` + effective-root match), so a lock file no
    one holds, or a foreign/crashed pid, behaves exactly as before. The flock
    half fails CLOSED: a held probe can only ever ADD liveness, never remove it.

    #44: the probe is the child-probe TRI-STATE (runner_lock_state), and 'wedged'
    — Errno 11 with the kernel ledger proving ZERO holders and the admission
    lease proven dead — is the ONE held-with-no-holder state that does NOT add
    liveness: nothing can progress on it, and wait's escape hatch (or the
    explicit release_lock verb) is the recovery. EACCES/unknown stay fail-closed
    HELD (B1): never free, never dead. lock_state optionally carries an already
    observed (state, detail) from a batch census (no second probe — A2 one-read
    law, and the dashboard/act_list one-child-per-batch law)."""
    if lock_state is None:
        lock_state = runner_lock_state(r)
    if lock_state[0] not in ("free", "wedged"):
        return True
    return _runner_pid_alive(r, pid_path)

def fingerprint_valid(stored, actual, rule):
    """A record's own rule is authoritative; an unstamped record matches ANY rule.

    A run-level stamp cannot attest to commits after a mixed-version resume, so
    only a record's OWN stamp is trusted outright. For unstamped (pre-1.0.12)
    records, a match under ANY rule proves the definition is unchanged in that
    rule's view (rule 1 = fully unchanged; rule 2 = unchanged modulo budgets,
    which def_hash treats as not-work). Fail-closed is ZERO matches: an
    ambiguity where two rules AGREE on the same stored hash re-spawns 25
    committed nodes and buys no safety (a real definition change matches none).
    """
    if rule is not None:
        return type(rule) is int and rule in FP_RULES and stored == actual(rule) and stored is not None
    matches = [v for v in FP_RULES if stored is not None and stored == actual(v)]
    return len(matches) >= 1

def record_efp_valid(rec, byid, node, field="efp", memo=None):
    return isinstance(rec, dict) and ("fp_rule_version" not in rec or
                                      (type(rec["fp_rule_version"]) is int and rec["fp_rule_version"] in FP_RULES)) and fingerprint_valid(
        rec.get(field), lambda rule: efp(byid, node, rule=rule, memo=memo), rec.get("fp_rule_version"))

def runner_exit_read(r, pid_path=None):
    """Graph-bound runner verdict, or a crash when a previous pid lacks identity.
    No pid file means a fresh, not-yet-spawned run, not a crash."""
    rec = jload(Path(r) / "runner_exit.json")
    if isinstance(rec, dict) and rec.get("reason"):
        current_graph = jload(Path(r) / "graph.json")
        if ("fp_rule_version" in rec and rec["fp_rule_version"] not in FP_RULES) or not fingerprint_valid(rec.get("graph_fingerprint"),
                                 lambda rule: graph_fingerprint(current_graph, rule=rule),
                                 rec.get("fp_rule_version")):
            return {"reason": "stale", "previous_reason": rec.get("reason"), "at": rec.get("at")}
        return {k: v for k, v in rec.items()
                if k in ("reason", "at", "detail", "graph_fingerprint")}
    path = Path(pid_path) if pid_path is not None else Path(r) / "wf.pid"
    if not path.exists():
        return None
    return None if runner_alive(r, path) else {"reason": "crashed (no exit record)"}

# est-6226: the ONE external-kill classifier. A runner NEVER SIGTERMs itself
# (stop rides the cooperative stop.request boundary -> "stopped"), so a
# "terminated: SIGTERM (external: ...)" verdict — the tag wf.py's signal
# handler writes — is an out-of-band signal death (witnessed shape: the
# gateway-restart wave propagating beyond the gateway, 2026-10-06 10:06Z).
# Reaper/dispatcher/read model all classify through THIS function — nobody
# re-derives external-ness from prose. Strict by design: the bare pre-fix
# string and every lane-failure shape ("crashed: ...", "crashed (no exit
# record)", "done", "stopped") are NOT external kills.
def is_external_kill(reason):
    return (isinstance(reason, str)
            and reason.startswith("terminated: SIGTERM")
            and "(external:" in reason)

# ---------- node state (the ONE validity rule: stored efp == current efp) ----------

def _legacy_chain_unchanged(r, n, byid, memo=None):
    """A pre-efp record is valid ONLY if the stored def_hash matches AND every
    ancestor was itself committed in the legacy era (its record also lacks efp)
    with a matching def_hash — chain-checked recursively. 'Done right now' is NOT
    proof of 'unchanged since this commit': an amended ancestor that re-ran must
    NOT resurrect stale downstream records or gate answers (MF2)."""
    # The chain walk is itself exponential on diamonds; `memo` is a per-WALK
    # visited map (False/True per ancestor), NOT the efp memo — pass no memo
    # when `byid` may mix graphs.
    if memo is not None and n["id"] in memo:
        return memo[n["id"]]
    rec = jload(r / "nodes" / f"{n['id']}.json")
    if not rec or "efp" in rec or not fingerprint_valid(
            rec.get("def_hash"), lambda rule: def_hash(n, rule), rec.get("fp_rule_version")):
        result = False
    else:
        result = all(a not in byid or _legacy_chain_unchanged(r, byid[a], byid, memo)
               for a in n.get("after", []))
    if memo is not None:
        memo[n["id"]] = result
    return result

def _answer_harvest_valid(n, rec):
    """Honest status (jam-aus): a committed record whose answer validates against the
    node's schema IS an answer, whatever exit the child died with. This is the #4
    harvest-on-death law applied at READ time, so a record an older/odd runner path
    committed as `failed` despite carrying a complete harvest (the runner wrote the
    `harvest` stamp only when the fenced block parsed to a dict and validated) reads
    done without re-driving finished work. Qualification is strict: harvest evidence
    stamped by the runner, a dict output that re-validates, and a child-declared
    terminal status (e.g. 'BLOCKED') never reads as done. The validator lives
    HERE (moved out of wf.py, outbound review NousResearch/hermes-agent#133387
    ask #2): the old lazy generic import of the runner's validate raised an
    uncaught ImportError — or imported a FOREIGN top-level runner module —
    whenever a door/dashboard process reached this read path. The binding is
    private; no generic top-level token is ever imported here."""
    hv = rec.get("harvest")
    if not isinstance(hv, dict):
        return False
    declared = hv.get("declared_status")
    if isinstance(declared, str) and declared.strip().lower() not in ("done", "ok", "success", "pass"):
        return False
    out = rec.get("output")
    if not isinstance(out, dict):
        return False
    schema = n.get("schema") or (n.get("fanout") or {}).get("schema")
    return not validate(out, schema)

def node_rec(r, n, byid, memo=None, legacy_memo=None):
    """Return (status, rec): done|partial|failed|pending. Stale (efp mismatch after an
    amend) == pending — its stored result must never be presented as current.
    Legacy (pre-efp) records carried def_hash only; a bare stamp is a downgrade
    attack on the validity law, so the whole ancestor chain must be proven
    unchanged legacy commits. `partial` (#4 harvest-on-death) IS a commit:
    partial output is committed output, downstream may consume it.
    Honest status: an efp-valid `failed` record whose harvest answer validates
    reads `done` — the child's nonzero exit is history, the answer is the fact."""
    rec = jload(r / "nodes" / f"{n['id']}.json")
    st = (rec or {}).get("status")
    if st == "running":
        interruption = jload(r / "interruptions" / f"{n['id']}.json") or {}
        claim = {k: rec.get(k) for k in ("pid", "skey", "attempt", "efp", "fp_rule_version")}
        if interruption.get("claim") == claim and record_efp_valid(rec, byid, n, memo=memo) \
                and not _verify_spawn_rec(r, n, byid, rec):
            rec = {**rec, "status": "interrupted", "error_class": "crashed",
                   "interruption": {k: v for k, v in interruption.items() if k != "claim"}}
    if st == "failed" and (rec or {}).get("error_class") == "cancelled":
        return "pending", rec   # stop != failure (#7): a resume re-drives cancelled work
    if st in ("done", "partial", "failed", "skipped"):
        if record_efp_valid(rec, byid, n, memo=memo):
            if st == "failed" and _answer_harvest_valid(n, rec):
                return "done", rec
            return st, rec
        if "efp" not in rec and _legacy_chain_unchanged(r, n, byid, legacy_memo):
            return st, rec
        return "pending", rec
    return "pending", rec

# ---------- publisher capability gate (est-2ek.1.603) ----------
# A node that has publication side effects DECLARES it (`publishes: true`); the
# runner refuses it pre-spawn until a verified suite-proof token exists among its
# after-ancestors. A token is minted ONLY when a `suite_proof: true` node commits
# done (same efp save_node stamps). Verification is fail-closed: the token must
# name a declared producer, its recorded node record must be a CURRENT efp-valid
# `done` commit, and the token's own efp must match that record — a stale or
# forged token never launders a publish. Returns (ok, missing_names):
# ok=False => the caller must refuse; missing_names = the proof-producing
# ancestors whose token is absent/invalid (what the refusal names).

def suite_proof_token_path(r, n):
    return Path(r) / "nodes" / f"{n['id']}.suite-proof.json"

def _token_valid(r, byid, producer):
    tok = jload(suite_proof_token_path(r, producer))
    if not isinstance(tok, dict):
        return False
    if tok.get("node") != producer["id"] or tok.get("efp") is None:
        return False
    rec = jload(Path(r) / "nodes" / f"{producer['id']}.json")
    if not isinstance(rec, dict) or rec.get("status") != "done":
        return False
    # the token must match the CURRENT efp-valid commit — and the committed
    # record's efp must match the current definition (an amend invalidates it).
    if tok.get("efp") != rec.get("efp"):
        return False
    return record_efp_valid(rec, byid, producer)

def publisher_gate_check(r, n, byid):
    if n.get("type") == "gate" or n.get("publishes") is not True:
        return True, []        # not a declared publisher: untouched
    producers, seen = [], set()
    stack = list(n.get("after", []))
    while stack:
        a = stack.pop()
        if a in seen or a not in byid:
            continue
        seen.add(a)
        stack.extend(byid[a].get("after", []))
        if byid[a].get("suite_proof") is True and byid[a].get("type") != "echo":
            producers.append(a)
    if not producers:
        return False, []       # declared publisher with no declared producer upstream
    missing = [a for a in producers if not _token_valid(r, byid, byid[a])]
    return (not missing), missing

# ---------- why-rerun: read-model verdict for a stale committed node (hackathon jam-h27) ----------

def _amend_snapshots(r):
    """[(at, old_nodes, new_nodes)] from amends.jsonl; malformed lines skipped.
    Only entries with node lists on both sides survive — the same snapshot shape
    act_amend writes ({at, old, new})."""
    snaps = []
    try:
        lines = (Path(r) / "amends.jsonl").read_text(encoding="utf-8").splitlines()
    except OSError:
        return snaps
    for line in lines:
        if not line.strip():
            continue
        try:
            m = json.loads(line)
        except Exception:
            continue
        old, new = (m.get("old") or {}).get("nodes"), (m.get("new") or {}).get("nodes")
        if isinstance(old, list) and isinstance(new, list):
            snaps.append((m.get("at"), old, new))
    return snaps

def explain_stale(r, nid, index=None):
    """One-line 'why is this committed node re-running' verdict for a stale node
    (stored efp != recomputed efp — node_rec's 'pending with a committed record'
    state), or None when the record is current/absent (never fabricate).

    Reconstructs the graph timeline from amends.jsonl (first `old` graph, then
    each `new`, then the current graph.json), finds the EARLIEST state the stale
    record still validates under — the era the commit really happened in (the
    whole old/new graph snapshots make the transitive efp replay exact) — then
    walks the node's own definition plus its ancestor chain (closest first)
    across each later transition and reports the FIRST transition that changed
    one: 'stale because <node>: <fields> changed at <ts>'. A transition with no
    amend behind it (current graph != last recorded `new`) is definition drift.
    Read model only: nodes/, graph.json, amends.jsonl are the only files read."""
    name = str(nid) + (f".{index}" if index is not None else "")
    rec = jload(Path(r) / "nodes" / f"{name}.json")
    if not isinstance(rec, dict) or rec.get("status") not in ("done", "partial", "failed", "skipped"):
        return None
    cur_nodes = (jload(Path(r) / "graph.json") or {}).get("nodes")
    if not isinstance(cur_nodes, list):
        return None
    byid_of = lambda ns: {n["id"]: n for n in ns if isinstance(n, dict) and n.get("id")}
    cur_byid = byid_of(cur_nodes)
    node = cur_byid.get(str(nid))  # fan-out item records ride their parent's def
    if node is None or record_efp_valid(rec, cur_byid, node):
        return None  # current — nothing to explain
    rule = rec.get("fp_rule_version")
    snaps = _amend_snapshots(r)
    # Timeline: (ts the state took effect, node list). Dedupe states whose
    # canonical nodes JSON is identical; the current graph closes the timeline —
    # when no amend's `new` equals it, that last transition is drift (ts None).
    states = []
    def push(ts, ns):
        if states and states[-1][1] == ns:
            return
        states.append((ts, ns))
    if snaps:
        push(None, snaps[0][1])
        for at, _old, new in snaps:
            push(at, new)
    if not states or states[-1][1] != cur_nodes:
        push(None, cur_nodes)
    def commit_era():
        for i, (_ts, ns) in enumerate(states):
            bid, x = byid_of(ns), next((n for n in ns if isinstance(n, dict) and n.get("id") == str(nid)), None)
            if x is not None and record_efp_valid(rec, bid, x):
                return i
        return None
    k = commit_era()
    if k is None:
        return "stale: definition drift (no amend on record)"  # commit matches no recorded state
    def diff_fields(a, b):  # def_hash is the truth; budgets are not work (rule 2+)
        rules = (rule,) if type(rule) is int and rule in FP_RULES else FP_RULES
        if any(def_hash(a, rl) == def_hash(b, rl) for rl in rules):
            return []
        keys = {k2 for k2 in a} | {k2 for k2 in b}
        if not (type(rule) is int and rule == FP_RULE_LEGACY):
            keys -= set(_BUDGET_KEYS)
        return sorted(k2 for k2 in keys if a.get(k2) != b.get(k2))
    for i in range(k + 1, len(states)):
        prev_byid, cur_i_byid = byid_of(states[i - 1][1]), byid_of(states[i][1])
        # c's ancestor chain at the pre-transition state, closest first (self included:
        # 'c re-ran because c changed' is as honest as 'because b changed')
        chain, seen, queue = [], set(), [str(nid)]
        while queue:
            x = queue.pop(0)
            if x in seen or x not in prev_byid:
                continue
            seen.add(x)
            chain.append(x)
            queue.extend(str(a2) for a2 in prev_byid[x].get("after", []) or [])
        for x in chain:
            a, b = prev_byid.get(x), cur_i_byid.get(x)
            if not (isinstance(a, dict) and isinstance(b, dict)):
                continue  # added/removed: not a field change to name
            fields = diff_fields(a, b)
            if fields:
                ts = states[i][0]
                if ts is None:
                    return "stale: definition drift (no amend on record)"
                return f"stale because {x}: {', '.join(fields)} changed at {ts}"
    return "stale: definition drift (no amend on record)"

def gate_answer_valid(r, gate, byid, memo=None, legacy_memo=None):
    ans = jload(r / "gates" / f"{gate['id']}.json")
    if ans is None:
        return None
    if record_efp_valid(ans, byid, gate, "_def", memo=memo):
        return ans
    if "fp_rule_version" in ans:
        return None
    gate_rec = jload(r / "nodes" / f"{gate['id']}.json", {}) or {}
    if "efp" not in gate_rec and "efp" not in ans:
        # Pre-efp answers require a uniquely attributable historical definition
        # and a chain of equally verified pre-efp ancestor commits.
        if not fingerprint_valid(ans.get("_def"), lambda rule: def_hash(gate, rule), None):
            return None
        return ans if all(a not in byid or _legacy_chain_unchanged(r, byid[a], byid, legacy_memo)
                          for a in gate.get("after", [])) else None
    return None

WHEN_TOKEN = re.compile(r"\s*(?:(==|!=|>=|<=|>|<|\(|\)|,|[A-Za-z_][A-Za-z0-9_.]*|'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|-?\d+(?:\.\d+)?|\bTrue\b|\bFalse\b|\bNone\b|and\b|or\b|not\b))")

def _tok_when(s):
    toks, i = [], 0
    while i < len(s):
        m = WHEN_TOKEN.match(s, i)
        if not m or m.end() == i:
            if s[i].isspace(): i += 1; continue
            raise ValueError(f"bad char {s[i]!r} at {i}")
        toks.append(m.group(1)); i = m.end()
    return toks


def _when_or(toks, pos, outputs):
    val, pos = _when_and(toks, pos, outputs)
    while pos < len(toks) and toks[pos] == "or":
        rhs, pos = _when_and(toks, pos + 1, outputs)
        val = val or rhs
    return val, pos

def _when_and(toks, pos, outputs):
    val, pos = _when_not(toks, pos, outputs)
    while pos < len(toks) and toks[pos] == "and":
        rhs, pos = _when_not(toks, pos + 1, outputs)
        val = val and rhs
    return val, pos

def _when_not(toks, pos, outputs):
    if pos < len(toks) and toks[pos] == "not":
        val, pos = _when_not(toks, pos + 1, outputs)
        return (not val), pos
    return _when_cmp(toks, pos, outputs)

class _SV:
    """Syntax-mode value: total-order sentinel so a PARSE-ONLY pass never raises
    on value semantics — structural errors surface, value errors cannot mask them."""
    def __eq__(self, o): return True
    def __ne__(self, o): return False
    def __lt__(self, o): return True
    def __le__(self, o): return True
    def __gt__(self, o): return True
    def __ge__(self, o): return True
    def __bool__(self): return True
_SYNTAX = _SV()

def _when_atom(toks, pos, outputs):
    if pos >= len(toks):
        raise ValueError("unexpected end of expression")
    t = toks[pos]
    if t == "(":
        val, pos = _when_or(toks, pos + 1, outputs)
        if pos >= len(toks) or toks[pos] != ")":
            raise ValueError("missing )")
        return val, pos + 1
    if outputs is _SYNTAX:  # syntax mode: EVERY atom is a sentinel — literals can
        if t.startswith("'") or t.startswith('"') or re.fullmatch(r"-?\d+(?:\.\d+)?", t) \
                or t in ("True", "False", "true", "false", "None"):
            return _SV(), pos + 1     # never carry real values that TypeError mid-parse
    if t.startswith("'") or t.startswith('"'):
        return t[1:-1].encode().decode("unicode_escape"), pos + 1
    if re.fullmatch(r"-?\d+(?:\.\d+)?", t):
        return (float(t) if "." in t else int(t)), pos + 1
    if t in ("True", "False", "true", "false", "None"):
        return {"True": True, "False": False, "true": True, "false": False, "None": None}[t], pos + 1
    if t == "out" or t.startswith("out."):
        if outputs is _SYNTAX:
            return _SV(), pos + 1
        path = t.split(".")
        cur = outputs
        for p in path[1:]:
            if isinstance(cur, list):
                try: cur = cur[int(p)]
                except (ValueError, IndexError): return None, pos + 1
            elif isinstance(cur, dict): cur = cur.get(p)
            else: return None, pos + 1
        return cur, pos + 1
    raise ValueError(f"unexpected token {t!r}")

def _when_cmp(toks, pos, outputs):
    lhs, pos = _when_atom(toks, pos, outputs)
    if pos < len(toks) and toks[pos] in ("==", "!=", ">", ">=", "<", "<="):
        op = toks[pos]; pos += 1
        rhs, pos = _when_atom(toks, pos, outputs)
        # TypeError (comparing mismatched types) PROPAGATES — when_true turns it
        # into a HOLD. A gate must never be skipped by a failing assumption.
        # Lazy dispatch: evaluating all operators eagerly would raise on the
        # unchosen ones (None > True) and poison valid == comparisons.
        if op == "==":   return lhs == rhs, pos
        elif op == "!=": return lhs != rhs, pos
        elif op == ">":  return lhs > rhs, pos
        elif op == ">=": return lhs >= rhs, pos
        elif op == "<":  return lhs < rhs, pos
        else:            return lhs <= rhs, pos
    return lhs, pos

def when_expr_ok(expr):
    """Parse-only check for validate_graph — VALUE-INDEPENDENT (sentinel operands),
    so a value TypeError can never short-circuit the structural pass and mask a
    trailing token. Malformed syntax is REJECTED at submit and at runner startup;
    value errors against live data surface at fire time as a HOLD (fail-safe)."""
    GRAMMAR = ("grammar: out.<node>.<dotted.path> compared (== != > >= < <=) with "
               "literals, joined by and/or/not, parentheses allowed")
    toks = None
    try:
        toks = _tok_when(expr)
    except ValueError as e:
        return f"when: {e} ({GRAMMAR})"
    if not toks:
        return f"empty when expression ({GRAMMAR})"
    try:
        _, pos = _when_or(toks, 0, _SYNTAX)
    except ValueError as e:
        return f"when: {e} ({GRAMMAR})"
    except Exception as e:  # syntax mode must be total: ANY raise = malformed
        return f"when: {type(e).__name__}: {e} ({GRAMMAR})"
    if pos != len(toks):
        return f"when: unexpected token {toks[pos]!r} ({GRAMMAR})"
    return None

def when_true(gate, outputs):
    """Conditional-gate predicate over a BOUNDED grammar (out paths, literals,
    comparisons, and/or/not, parens) — never Python eval. A broken 'when' FAILS
    SAFE to hold (a typo must never silently skip a human gate): both value
    errors AND unconsumed trailing tokens raise into the hold path."""
    w = gate.get("when")
    if not w:
        return True
    val, pos = _when_or(_tok_when(w), 0, outputs)
    if pos != len(_tok_when(w)):
        raise ValueError("when: trailing tokens")
    return bool(val)

# ---------- the ONE read model ----------

WAIT_KEYS = {"wait_s", "until_argv", "every_s", "timeout_s"}

def wait_spec_ok(w):
    """gate.wait = {wait_s?, until_argv?, every_s?, timeout_s?}: a machine-answered gate.
    wait_s alone = timer. until_argv = a fixed argv list (never a shell string) re-run
    every every_s until exit 0. At least one of wait_s/until_argv. timeout_s bounds the
    whole park (default 3600, max 86400). Returns a LIST of {field, msg} dicts (empty
    = ok); field None names the wait block itself, else a sub-key."""
    out = []
    def E(field, msg):
        out.append({"field": field, "msg": msg})
    if not isinstance(w, dict):
        return [{"field": None, "msg": "must be an object"}]
    for k in sorted(set(w) - WAIT_KEYS):
        E(k, f"unknown key {k!r}; allowed: {sorted(WAIT_KEYS)}")
    if w.get("wait_s") is None and not w.get("until_argv"):
        E(None, "needs wait_s and/or until_argv")
    for k, hi in (("wait_s", 86400), ("every_s", 3600), ("timeout_s", 86400)):
        v = w.get(k)
        if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v <= 0 or v > hi):
            E(k, f"{k} must be a number in (0, {hi}]")
    argv = w.get("until_argv")
    if argv is not None and (not isinstance(argv, list) or not argv
                             or not all(isinstance(a, str) and a for a in argv)):
        E("until_argv", "until_argv must be a non-empty list of strings (fixed argv, no shell)")
    return out

def blocked_by(n, states, nodes_meta=None):
    """P1 (jury form): the NEAREST unfinished ancestors of a pending node, each with its
    state, computed at query time and never persisted. nodes_meta may carry per-node
    liveness ({id: {"idle_s": .., "parked": {...}}}) to enrich the state word."""
    out = []
    for a in n.get("after", []):
        st = states.get(a)
        if st == "partial" and n.get("after_partial"):   # the opt-in consumes the harvest
            continue                                     # e68544a37be37657
        if st in ("done", "skipped"):
            continue
        m = (nodes_meta or {}).get(a) or {}
        if st == "failed":
            word = "failed"
        elif st == "partial":
            # #4 + e68544a37be37657: a harvest does NOT satisfy a plain after-edge —
            # it is an unfinished ancestor until the descendant opts in.
            word = "partial (harvested; needs after_partial)"
        elif m.get("held"):
            word = "held at gate"
        elif m.get("parked"):
            pk = m["parked"]
            word = f"parked ({pk.get('kind')}, attempt {pk.get('attempt', 0)})"
        elif m.get("running"):
            idle = m.get("idle_s")
            word = f"running idle {int(idle)}s" if idle is not None else "running"
        else:
            word = "pending"
        out.append(f"{a}: {word}")
    return out

def prune_states(nodes, states):
    """ONE derivation shared by runner and read model (no second store). A gate skipped
    with on_skip:'prune' commits `skipped`; a node whose `after` deps are ALL skipped is
    itself `skipped` (terminal, non-failure). A join with >=1 live dep runs normally —
    skipped deps count as satisfied. Mutates `states`; returns ids newly derived skipped
    (pending before) so the runner can commit them as efp-stamped facts."""
    derived, changed = set(), True
    by_map = {n["id"]: n for n in nodes}
    while changed:
        changed = False
        for n in nodes:
            deps = n.get("after", [])
            if states.get(n["id"]) == "pending" and deps and all(states.get(a) == "skipped" for a in deps):
                # jam-h23: ONE exception — a fallback survives the pruner. If a
                # skipped dep died FAILED and its on_fail names THIS node, that
                # death is the node's reason to run, not a reason to prune it.
                if any((by_map.get(a) or {}).get("on_fail") == n["id"] for a in deps):
                    continue
                states[n["id"]] = "skipped"; derived.add(n["id"]); changed = True
    return derived

def dep_satisfied(states, a, allow_partial=False):
    """After-edge release law. #4 (harvest-on-death) keeps a `partial` ancestor's
    OUTPUT committed and consumable, but e68544a37be37657 ends the equivalence
    with `done` for RELEASE: a plain after-edge is NOT satisfied by a `partial`
    (the child died mid-work — releasing verify/suite onto an incomplete
    candidate is the false-green class). A descendant that genuinely wants the
    harvest opts in per-node with `after_partial: true` (agent/gate key)."""
    st = states.get(a)
    if st == "partial":
        return bool(allow_partial)
    return st in ("done", "skipped")

def dead_set(nodes, states):
    """est-ij0: ids that can never commit — `failed`, or `pending` behind a dead
    DATA edge (an after-edge not listed in the node's order_only). A dead ORDER
    edge never kills: that is exactly what the splice removes."""
    byid = {n["id"]: n for n in nodes}
    memo = {}
    def dead(nid):
        if nid in memo:
            return memo[nid]
        memo[nid] = False                      # DAG is validated; guard anyway
        st = states.get(nid)
        if st == "failed":
            memo[nid] = True
        elif st == "pending" and nid in byid:
            oo = set(byid[nid].get("order_only") or ())
            memo[nid] = any(dead(a) for a in byid[nid].get("after", []) if a not in oo)
        return memo[nid]
    return {n["id"] for n in nodes if dead(n["id"])}

def release_law(nodes, states):
    """ONE after-edge release law for runner and read model -> (deps_ok, deps_res,
    spliced). Data edges keep dep_satisfied. An order_only edge to X is satisfied
    when X is satisfied, OR X is dead and X's own order_only predecessors are
    (recursively) satisfied — the convoy splice: no node inherits a dead member as
    its predecessor, and chain order among the living is preserved.
    spliced(n) -> the dead order predecessors this node is released past."""
    byid = {n["id"]: n for n in nodes}
    dead = dead_set(nodes, states)
    def order_ok(a, seen=()):
        # an ordering edge waits for its predecessor to FINISH, consuming nothing:
        # a harvested `partial` is finished (it never runs again), so it releases.
        if states.get(a) in ("done", "skipped", "partial"):
            return True
        if a not in dead or a in seen:
            return False
        return all(order_ok(p, seen + (a,)) for p in ((byid.get(a) or {}).get("order_only") or ()))
    def edge_ok(n, a):
        if a in (n.get("order_only") or ()):
            return order_ok(a)
        return dep_satisfied(states, a, allow_partial=bool(n.get("after_partial")))
    def deps_ok(n):
        return all(edge_ok(n, a) for a in n.get("after", []))
    def deps_res(n):
        oo = n.get("order_only") or ()
        return all(states.get(a) in ("done", "partial", "failed", "skipped")
                   or (a in oo and order_ok(a)) for a in n.get("after", []))
    def spliced(n):
        return [a for a in (n.get("order_only") or ()) if a in dead and order_ok(a)]
    return deps_ok, deps_res, spliced

def _verify_spawn_rec(r, n, byid, rec):
    """ONE verification law for a spawn record (790c6ad): status=running + efp
    match + pid alive (non-zombie) + the recorded skey title present in the
    process's argv. Returns the verified identity
    {pid, skey, started, attempt, log_path} or None. The skey-in-cmdline check
    is the PID-reuse guard and is NEVER relaxed: unknown identity is not proof
    of an active child. Shared by _active_spawns (display path) and
    active_child (runner adoption) so runner and read model can never disagree.
    """
    if not isinstance(rec, dict) or rec.get("status") != "running" \
            or not record_efp_valid(rec, byid, n):
        return None
    pid, skey = rec.get("pid"), rec.get("skey")
    if not isinstance(pid, int) or pid <= 0 or not isinstance(skey, str) or not skey:
        return None
    title = skey if "#a" in skey or not isinstance(rec.get("attempt"), int) else f"{skey}#a{rec['attempt']}"
    try:
        os.kill(pid, 0)
        try:
            stat_text = Path(f"/proc/{pid}/stat").read_text()
            state = stat_text.rsplit(")", 1)[1].split()[0]
            cmd = Path(f"/proc/{pid}/cmdline").read_bytes().decode(errors="replace").replace("\0", " ")
        except OSError:  # macOS/BSD: verify state and identity without procfs.
            row = subprocess.run(["ps", "-ww", "-p", str(pid), "-o", "stat=", "-o", "command="],
                                 capture_output=True, text=True, check=True, timeout=2).stdout
            state, cmd = row.strip().split(None, 1)
        if state.startswith("Z") or title not in cmd.split():
            return None
    except (OSError, IndexError, ValueError, subprocess.SubprocessError):
        return None  # unknown identity is not proof of an active child
    return {**{k: rec[k] for k in ("pid", "started", "attempt", "log_path") if k in rec},
            "skey": title}

def _foreign_ns_spawn(r, n, byid, rec, ledger=None):
    """est-2ek.1.818: a child the reader cannot see in its own /proc (the runner's
    container has another pid namespace) is not thereby dead. DISPLAY path only —
    adoption and the reaper keep the strict _verify_spawn_rec law. The runner lock
    (flock, cross-namespace safe) is already proven held by the caller; the
    evidence added here is the runner's own spawn-ledger: a `child` row for this
    exact (pid, skey) written AFTER the last `runner` row (spawned by the runner
    now holding the lock, not a crashed predecessor) with no `child_end` after it,
    on a status=running record whose efp is current. A pid that IS visible but
    fails identity (zombie, skey absent from argv: PID reuse) is never relaxed.
    Returns the identity marked verified=False, or None."""
    if not isinstance(rec, dict) or rec.get("status") != "running" \
            or not record_efp_valid(rec, byid, n):
        return None
    pid, skey = rec.get("pid"), rec.get("skey")
    if not isinstance(pid, int) or pid <= 0 or not isinstance(skey, str) or not skey:
        return None
    if Path(f"/proc/{pid}").exists():
        return None   # visible here: _verify_spawn_rec already judged its identity
    if not runner_lock_held(r):
        return None   # no cross-namespace proof of a live runner: nothing to inherit from
    try:
        os.kill(pid, 0)
        return None   # signalable but absent from /proc (hidepid etc.): unknown, not proof
    except ProcessLookupError:
        pass
    except OSError:
        return None
    if ledger is None:
        try:
            ledger = [json.loads(x) for x in (Path(r) / "spawn-ledger.jsonl").read_text().splitlines() if x.strip()]
        except (OSError, ValueError):
            return None
    spawned = False
    for row in ledger:
        if not isinstance(row, dict):
            continue
        if row.get("role") == "runner":
            spawned = False   # a newer runner generation: earlier children are its predecessor's
        elif row.get("pid") == pid and row.get("skey") == skey:
            if row.get("role") == "child":
                spawned = True
            elif row.get("role") == "child_end":
                spawned = False
    if not spawned:
        return None
    title = skey if "#a" in skey or not isinstance(rec.get("attempt"), int) else f"{skey}#a{rec['attempt']}"
    return {**{k: rec[k] for k in ("pid", "started", "attempt", "log_path") if k in rec},
            "skey": title, "verified": False,
            "unverified_because": "pid not visible in this pid namespace; runner lock held and "
                                  "spawn-ledger shows this child spawned by the current runner, not ended"}

def _active_spawns(r, n, byid):
    """All verified uncommitted child identities, never historical DB liveness.
    A live runner's own ledger-proven child in a foreign pid namespace is listed
    too, marked verified=False (est-2ek.1.818)."""
    records = [r / "nodes" / f"{n['id']}.json"]
    if n.get("fanout"):
        records.extend(sorted((r / "nodes").glob(f"{n['id']}.[0-9]*.json")))
    active = []
    for path in records:
        rec = jload(path)
        v = _verify_spawn_rec(r, n, byid, rec) or _foreign_ns_spawn(r, n, byid, rec)
        if v and isinstance(rec, dict):
            # Keep the verified attempt title for liveness/session joins, and
            # the exact stored key for the ledger's distinct representation.
            item = re.fullmatch(re.escape(n["id"]) + r"\.([0-9]+)\.json", path.name)
            active.append({**v, "ledger_skey": rec["skey"],
                           "index": int(item[1]) if item else None})
    return active

def _progress_for(r, n):
    """#128 artifact-mtime progress channel: the write-first file IS the heartbeat.
    The child work dir <run>/work/<node>[.<i>]/ (child_work_dir, wf.py — the dir
    WORK_DIR_NOTE already mandates) is scanned at QUERY time for the most recently
    modified regular file across the node's item dirs (fan-out: latest activity
    wins). Pure read, no writes, NEVER raises: absent/unreadable artifact or a
    stat/decode failure means an ABSENT `progress` field, never a fabricated one
    (R2/when_error law), and absence draws no stall inference. last_line is the
    last NEWLINE-TERMINATED line — a trailing partial line is dropped so the
    reader never sees a torn line mid-append."""
    try:
        import stat as _stat
        import time
        base = re.sub(r"[^A-Za-z0-9_.-]", "_", str(n.get("id")))
        work = Path(r) / "work"
        if not base or not work.is_dir():
            return None
        dirs = []
        if (work / base).is_dir():
            dirs.append(work / base)
        try:  # fan-out siblings <base>.<i> (runner names them via child_work_dir)
            dirs.extend(p for p in sorted(work.iterdir())
                        if p.is_dir() and p.name.startswith(base + "."))
        except OSError:
            pass
        if not dirs:
            return None
        best = None  # (mtime, size, absolute path)
        seen = 0
        for d in dirs:
            for dirpath, _sub, files in os.walk(d):
                for fn in files:
                    seen += 1
                    if seen > 2000:  # a pathological tree never stalls status
                        break
                    p = Path(dirpath) / fn
                    try:
                        st_ = p.stat()
                    except OSError:
                        continue
                    if not _stat.S_ISREG(st_.st_mode):
                        continue
                    if best is None or st_.st_mtime > best[0]:
                        best = (st_.st_mtime, st_.st_size, p)
                    if seen > 2000:
                        break
                if seen > 2000:
                    break
        if best is None:
            return None
        mtime, size, p = best
        try:  # tail-read only: a growing artifact is never fully re-read
            with open(p, "rb") as fh:
                if size > 65536:
                    fh.seek(-65536, os.SEEK_END)
                    fh.readline()  # drop the window's own head: it may itself be torn
                    data = fh.read()
                else:
                    data = fh.read()
        except OSError:
            return None
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return None  # a binary/unreadable artifact yields no key, no error
        prog = {"artifact": str(p.relative_to(Path(r))),
                "size": size,
                "mtime_age_s": max(0.0, time.time() - mtime)}
        if text.endswith("\n"):
            lines = text[:-1].split("\n")
            if text[:-1]:
                prog["last_line"] = lines[-1]
        elif "\n" in text:  # trailing PARTIAL line: drop it, last whole line wins
            prog["last_line"] = text.rsplit("\n", 1)[0].rsplit("\n", 1)[-1]
        # a file with NO newline-terminated line yet: honest absence of last_line
        return prog
    except Exception:
        return None  # fail-safe read model: this NEVER raises into status/wait/list

def active_child(r, n, byid, index=None):
    """Per-item entry point to the verification law (790c6ad): the runner's
    fan-out branch calls this BEFORE Popen on a resumed runner — a verified live
    orphan (status=running record + efp match + pid alive + skey in argv) is
    ADOPTED, never re-spawned; a dead or unverifiable record returns None and
    the item spawns fresh. The display path (_active_spawns → run_state) shares
    the same law through _verify_spawn_rec. Returns {pid, skey, started,
    attempt, log_path} or None."""
    fname = f"{n['id']}" + (f".{index}" if index is not None else "")
    return _verify_spawn_rec(r, n, byid, jload(r / "nodes" / f"{fname}.json"))


# est-2ek.1.833: the runner logs `seat.wait` when the GLOBAL agent-seat semaphore
# is full (wf.py _seat_acquire on_wait) and writes a spawn-ledger child row the
# moment it gets a seat and spawns. A wait with no later spawn/terminal event for
# the same (node, index) is OPEN — status reads it instead of an unexplained
# all-pending live run. Derive-only (A3): two files the runner already writes.
_SEAT_WAIT_CLOSERS = frozenset(("node.done", "node.failed", "node.finished", "node.cancelled",
                                "node.skipped", "node.spliced", "item.finished", "item.adopted",
                                "item.harvested_at_cancel"))

def open_seat_waits(r, events_text=None):
    """{node_id: {(index): {since, cap, spawn}}} for seat waits not yet closed by a
    later child spawn (spawn-ledger), node/item terminal event, or runner (re)start
    / stop. Unreadable files are honest absence (empty), never a raise."""
    from datetime import datetime
    def ts(s):
        try:
            return datetime.fromisoformat(str(s)).timestamp()
        except (TypeError, ValueError):
            return None
    rows = []
    for fname, src in (("events.jsonl", "ev"), ("spawn-ledger.jsonl", "led")):
        try:
            text = events_text if src == "ev" and events_text is not None else (Path(r) / fname).read_text()
        except (OSError, UnicodeError):
            continue
        for line in text.splitlines():
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if isinstance(e, dict) and ts(e.get("ts")) is not None:
                rows.append((ts(e["ts"]), 0 if src == "ev" else 1, src, e))
    rows.sort(key=lambda x: (x[0], x[1]))   # same second: the wait precedes its spawn
    waits = {}
    for _t, _o, src, e in rows:
        if src == "led":
            if e.get("role") == "child" and e.get("node"):
                waits.pop((e["node"], e.get("index")), None)
            continue
        ev = e.get("event")
        if not isinstance(ev, str):
            continue
        if ev in ("run.started", "run.resumed", "run.stopped"):
            waits.clear()
        elif ev == "seat.wait" and e.get("node"):
            waits[(e["node"], e.get("index"))] = {"since": e.get("ts"), "cap": e.get("cap"),
                                                  "spawn": e.get("spawn")}
        elif ev in _SEAT_WAIT_CLOSERS and e.get("node"):
            if ev.startswith("item.") and e.get("index") is not None:
                waits.pop((e["node"], e.get("index")), None)
            else:
                for k in [k for k in waits if k[0] == e["node"]]:
                    waits.pop(k)
    out = {}
    for (nid, idx), w in waits.items():
        out.setdefault(nid, {})[idx] = w
    return out

def seat_wait_view(per_index, now_s=None):
    """One node's open waits -> {since, age_s, cap, spawn[, waiting]} (oldest wait)."""
    import time as _time
    from datetime import datetime
    oldest = min(per_index.items(), key=lambda kv: str(kv[1].get("since") or ""))[1]
    try:
        age = int((now_s if now_s is not None else _time.time())
                  - datetime.fromisoformat(str(oldest.get("since"))).timestamp())
    except (TypeError, ValueError):
        age = None
    view = {"since": oldest.get("since"), "age_s": age, "cap": oldest.get("cap"),
            "spawn": oldest.get("spawn")}
    idxs = sorted(i for i in per_index if isinstance(i, int))
    if idxs:
        view["waiting"] = idxs
    return view

WALL_NEAR_RATIO = 0.8   # #133: >80 % of either bound reads near-wall

def _iso_epoch(s):
    """epoch seconds off a stored ISO ts, None when unparseable (never raises)."""
    from datetime import datetime
    try:
        return datetime.fromisoformat(str(s)).timestamp()
    except (TypeError, ValueError):
        return None

def _node_extends(r, events_text=None):
    """{node: [(ts_epoch, extra_s)]} from the committed node.extended events —
    the #133 wall-meter input: the runner's ONE 50 % extend-not-kill bump (#11)
    is already in events.jsonl, the meter only sums what is there. Malformed
    lines, foreign nodes and rows without a numeric extra_s are ignored; an
    unreadable ledger yields NO bumps (honest absence, never a fabricated wall)."""
    import math
    out = {}
    try:
        text = events_text if events_text is not None else (Path(r) / "events.jsonl").read_text()
    except (OSError, UnicodeError):
        return out
    for line in text.splitlines():
        if '"node.extended"' not in line:      # cheap pre-filter before the parse
            continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        if not isinstance(e, dict) or e.get("event") != "node.extended":
            continue
        node, ex, ts = e.get("node"), e.get("extra_s"), _iso_epoch(e.get("ts"))
        if not isinstance(node, str) or not node or ts is None:
            continue
        if not isinstance(ex, (int, float)) or isinstance(ex, bool):
            continue
        try:
            ex = float(ex)
        except (ValueError, OverflowError):
            continue
        if math.isfinite(ex) and ex >= 0:
            out.setdefault(node, []).append((ts, ex))
    return out

def _wall_meter(n, active, extends):
    """#133 wall meter for ONE running node — pure arithmetic over facts the
    runner already committed: the verified active spawn's `started` (fan-out:
    the senior item sets the node's meter), the BAKED def `timeout`, and the
    node.extended bumps committed at or after that spawn. p95_s is the measured
    p95 the SHAPE_PRESETS table already holds for the node's shape; an explicit
    timeout beyond it is the author's own wall, so the meter names both bounds.
    One word: over-p95 (elapsed at/over either bound) / near-wall (>80 % of
    either) / on-track. Honest absence (#128/R2 law): no parseable started, or
    no bound at all => NO key — never a fabricated number."""
    import time as _time
    starts = [t for t in (_iso_epoch(a.get("started")) for a in active) if t is not None]
    if not starts:
        return None
    started = min(starts)
    timeout = n.get("timeout")
    wall_s = (int(timeout) if isinstance(timeout, (int, float))
              and not isinstance(timeout, bool) and timeout > 0 else None)
    if wall_s is not None:
        # only bumps committed by THIS spawn's life count (a pre-spawn
        # extension belongs to a dead attempt, not to the wall we sit inside)
        wall_s += int(sum(ex for (ts, ex) in extends.get(n.get("id"), ())
                          if ts is None or ts >= started - 1))
    preset = SHAPE_PRESETS.get(n.get("shape", DEFAULT_SHAPE), {}) or {}
    p95 = preset.get("timeout")
    p95 = (int(p95) if isinstance(p95, (int, float))
           and not isinstance(p95, bool) and p95 > 0 else None)
    bounds = [b for b in (wall_s, p95) if b is not None]
    if not bounds:
        return None
    elapsed = int(_time.time() - started)
    over = any(elapsed >= b for b in bounds)
    near = any(elapsed > WALL_NEAR_RATIO * b for b in bounds)
    return {"elapsed_s": elapsed, "wall_s": wall_s, "p95_s": p95,
            "meter": "over-p95" if over else "near-wall" if near else "on-track"}

# Telemetry is a bounded prefix, not an authorization or a complete audit.
CLOCK_LEDGER_MAX_BYTES = 4 * 1024 * 1024


def _clock_ledger(r, node_ids):
    """One bounded snapshot/index per state read; partial coverage stays explicit."""
    indexed = {nid: [] for nid in node_ids}
    coverage = {"status": "unavailable", "bytes_read": 0,
                "max_bytes": CLOCK_LEDGER_MAX_BYTES, "scope": "current nodes; prefix"}
    try:
        with (r / "spawn-ledger.jsonl").open("rb") as stream:
            data = stream.read(CLOCK_LEDGER_MAX_BYTES)
            partial = stream.tell() < os.fstat(stream.fileno()).st_size
        coverage.update(status="partial" if partial else "read", bytes_read=len(data))
        if partial:
            data = data[:data.rfind(b"\n") + 1]
        for line in data.splitlines():
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    coverage["status"] = "partial"
                    continue
                nid = row.get("node")
                if isinstance(nid, str) and nid in indexed:
                    indexed[nid].append(row)
            except (ValueError, UnicodeError):
                coverage["status"] = "partial"
    except (OSError, UnicodeError):
        pass
    return indexed, coverage


def execution_clocks(n, view, waits, now_s, ledger=(), ledger_available=True):
    """Read-only clocks; queue callback time is later than admission-clock start.

    Consequently its remaining value is an upper bound, never a dispatch ETA.
    The runner has per-attempt walls, not a task-total authorization ceiling.
    """
    wall = n.get("timeout")
    queue = []
    for index, row in waits.items():
        start = _iso_epoch(row.get("since"))
        elapsed = max(0, now_s - start) if start is not None else None
        remaining = (max(0, wall - elapsed) if elapsed is not None
                     and isinstance(wall, (int, float)) and not isinstance(wall, bool) else None)
        queue.append({"index": index, "elapsed_s": elapsed,
                      "configured_timeout_s": wall,
                      "remaining_upper_bound_s": remaining})
    active = []
    live_keys = {}
    for sp in view.get("active_spawns", []):
        start = _iso_epoch(sp.get("started"))
        elapsed = max(0, now_s - start) if start is not None else None
        live_keys[(sp.get("pid"), sp.get("ledger_skey", sp.get("skey")))] = start
        active.append({"index": sp.get("index"), "elapsed_s": elapsed,
                       "configured_remaining_s": max(0, wall - elapsed)
                       if elapsed is not None and isinstance(wall, (int, float)) else None,
                       "effective_remaining_s": None})
    # Each child/child_end pair is one attempt, not a node-start-to-now timer.
    # Unclosed non-live attempts are unknown, never extended to the present.
    pending, observed, attempts, seen = {}, 0, 0, set()
    unknown = 0
    for row in ledger:
        if row.get("node") != n["id"]:
            continue
        if type(row.get("pid")) is not int or not isinstance(row.get("skey"), str):
            continue
        key = (row["pid"], row["skey"])
        stamp = _iso_epoch(row.get("ts"))
        if stamp is None:
            continue
        if row.get("role") == "child":
            identity = (key, stamp)
            if identity in seen:
                continue
            seen.add(identity)
            if key in pending:
                unknown += 1
            pending[key] = stamp
            attempts += 1
        elif row.get("role") == "child_end":
            if key in pending:
                observed += max(0, stamp - pending.pop(key))
            else:
                unknown += 1
    # The writer stamps the spawn record before appending its ledger start.
    # An older open interval with a reused PID/bare key is not this attempt.
    matched = {key for key, start in pending.items()
               if live_keys.get(key) is not None and start >= live_keys[key]}
    unknown += len(live_keys.keys() - matched)
    for key, start in pending.items():
        if key in matched:
            observed += max(0, now_s - start)
        else:
            unknown += 1
    return {"phase": "active" if active else "queued" if queue else view["status"],
            "queue": queue, "active": active,
            "work": {"observed_s": observed, "attempts_observed": attempts, "coverage": "best_effort",
                     "unknown_intervals": unknown if ledger_available or unknown else None,
                     "completeness": "unknown", "lower_bound": True,
                     "basis": "spawn-ledger intervals; cumulative across attempts"},
            "task_total_ceiling": "not_enforced"}


BUDGET_TOKEN_COUNTERS = ("tokens_in", "tokens_out", "cache_read", "reasoning")

def budget_tokens(m):
    """est-3acn: the RunBudget counter — every provider-metered token column
    state.db exposes (in/out/cache_read/reasoning), summed. One definition for
    the pre-dispatch check and the read view; honest absence reads zero."""
    try:
        return sum(int(m.get(k) or 0) for k in BUDGET_TOKEN_COUNTERS)
    except (TypeError, ValueError):
        return 0

def run_state(r, *, telemetry=True, lock_state=None):
    """Derived truth of a run dir: status, per-node status, held gate meta.
    status: pending|running|interrupted|held|done|partial|failed|stopped.
    'interrupted' has unfinished work but NO verified runner; only wait/release/amend
    explicitly resume it. A later amend demotes a stopped run to interrupted.
    'partial' (est-3acn) is a RunBudget-capped run that finalized gracefully:
    every committed output is preserved, the ledger names what never launched."""
    stored_graph = jload(r / "graph.json")
    if not stored_graph or not stored_graph.get("nodes"):
        return None
    # Exit-agent records are diagnostics, never part of the run's verdict math.
    graph = {**stored_graph, "nodes": workflow_nodes(stored_graph)}
    byid = {n["id"]: n for n in stored_graph["nodes"]}
    # One shared efp memo for this graph's read: every node hash below covers a
    # single stored graph whose defs nothing here mutates, so ancestor hashes
    # computed for one node's law-check are exactly the values every other
    # node's check needs (diamond graphs make the un-memoized walk exponential).
    fp_memo, legacy_memo = {}, {}
    # #44: ONE lock observation for the whole read — runner_alive consumes the
    # same (state, detail) the payload publishes as runner_lock (A2 one-read law:
    # a second probe microseconds later can flip across a dying runner's flock).
    # A census caller (act_list, dashboard _list_runs) passes the batched probe
    # result in so the whole listing costs ONE probe child, not one per row
    # (PR #47 round-3 sibling gap: 50 rows spawned 50 children).
    _lock_state = lock_state if lock_state is not None else runner_lock_state(r)
    live = runner_alive(r, lock_state=_lock_state)
    try:
        events_text = (r / "events.jsonl").read_text()
    except (OSError, UnicodeError):
        events_text = ""
    outputs, states, nodes, recs = {}, {}, {}, {}
    for n in graph["nodes"]:
        st, rec = node_rec(r, n, byid, fp_memo, legacy_memo)
        states[n["id"]] = st; recs[n["id"]] = rec
        if st in ("done", "partial"):   # #4: partial output IS output for when/refs
            outputs[n["id"]] = (rec or {}).get("output")
    prune_states(graph["nodes"], states)   # derived view: pruned-but-uncommitted read as skipped
    _ext_map = [None]   # #133: the node.extended ledger, lazily parsed once per call
    for n in graph["nodes"]:
        st, rec = states[n["id"]], recs[n["id"]]
        active = _active_spawns(r, n, byid) if live and st == "pending" and kind(n).spawns is True else []
        nodes[n["id"]] = {"type": n["type"], "status": "running" if active else st, "after": n.get("after", []),
                          "fanout": bool(n.get("fanout")),
                          "stale_of_amend": bool(rec) and st == "pending" and rec.get("status") in ("done", "partial", "failed", "skipped") or None}
        if st in ("done", "partial"):
            receipts = external_effect_receipts(n, rec)
            if receipts:
                nodes[n["id"]]["metrics"] = {"effect_receipts": receipts}
        if active:
            nodes[n["id"]]["active_spawn"] = active[0]
            nodes[n["id"]]["active_spawns"] = active
            # #128: the write-first artifact under the node's child work dir is the
            # heartbeat — derive-only, honest absence (no key when no file is visible).
            prog = _progress_for(r, n)
            if prog:
                nodes[n["id"]]["progress"] = prog
            # #133 wall meter: elapsed-vs-wall-and-p95, right now — arithmetic
            # over the spawn's `started`, the baked def timeout, and the one
            # node.extended bump. Honest absence: no key when nothing grounds it.
            if _ext_map[0] is None:
                _ext_map[0] = _node_extends(r, events_text)   # shared poll snapshot
            wm = _wall_meter(n, active, _ext_map[0])
            if wm:
                nodes[n["id"]]["wall"] = wm
    # est-2ek.1.833: a live runner blocked on the global seat cap says so per node.
    waits = open_seat_waits(r, events_text) if live else {}
    if live:
        for nid, per in waits.items():
            if nid in nodes and states.get(nid) == "pending" and per:
                nodes[nid]["seat_wait"] = seat_wait_view(per)
    # e68544a37be37657 + est-ij0: the runner's own release law (partial blocks a
    # plain edge; a dead order_only predecessor is spliced) — one function, two callers.
    deps_ok, _deps_res, _spliced = release_law(graph["nodes"], states)
    last = None
    for line in events_text.splitlines()[-20:]:
        try:
            event = json.loads(line)
        except Exception:
            continue
        if isinstance(event, dict):
            last = event
    status = "running" if live else "interrupted"
    held = None
    if (last or {}).get("event") == "run.stopped":
        status = "stopped"
    elif (last or {}).get("event") == "run.partial":
        # est-3acn: a RunBudget-capped run that finalized gracefully (the event
        # is the fact; the runner_exit verdict below outranks it when present).
        status = "partial"
    elif any(s == "failed" for s in states.values()):
        status = "failed"
    elif all(s in ("done", "partial", "skipped") for s in states.values()):   # #4: partial closes the run
        status = "done"
    else:
        gate = next((n for n in graph["nodes"] if kind(n).spawns is False
                     and states[n["id"]] == "pending" and deps_ok(n)), None)
        if gate and gate.get("wait") and gate_answer_valid(r, gate, byid, fp_memo, legacy_memo) is None:
            # machine-answered gate: the runner is polling it — 'running', never 'held'.
            # Its blockage is self-explaining via nodes[id].parked (P4 + jury tweak).
            pk = jload(r / "gates" / f"{gate['id']}.parked.json", {}) or {}
            if live and record_efp_valid(pk, byid, gate, "_def", memo=fp_memo):
                nodes[gate["id"]]["parked"] = {k: v for k, v in pk.items() if k != "_def"}
            elif live:
                nodes[gate["id"]]["parked"] = {"kind": "timer" if not gate["wait"].get("until_argv") else "check", "attempt": 0}
            gate = None
        if gate and gate_answer_valid(r, gate, byid, fp_memo, legacy_memo) is None:
            try:
                cond = when_true(gate, outputs)
                held = {"id": gate["id"], "question": gate.get("question"),
                        "options": gate.get("options"), "context": gate.get("context")}
            except Exception as e:
                # Fail-safe law applies to the READ MODEL too: a broken 'when'
                # shows as HELD (with the error surfaced), never a silent skip —
                # and NEVER raises into status/wait/list/release/stop/dashboard.
                cond = True
                held = {"id": gate["id"], "question": gate.get("question"),
                        "options": gate.get("options"), "context": gate.get("context"),
                        "when_error": str(e)}
            if cond:
                status = "held"
            else:
                held = None
        elif not (r / "events.jsonl").exists() and not (r / "wf.pid").exists():
            status = "pending"
    # P1: nearest unfinished ancestors with their state, derived here, never stored.
    meta = {}
    for n in graph["nodes"]:
        nid = n["id"]
        if held and held.get("id") == nid:
            meta[nid] = {"held": True}
        elif nodes[nid].get("parked"):
            meta[nid] = {"parked": nodes[nid]["parked"]}
        elif states[nid] == "pending" and kind(n).spawns is True and deps_ok(n) and nodes[nid].get("active_spawn"):
            meta[nid] = {"running": True}
    for n in graph["nodes"]:
        if states[n["id"]] == "pending" and not deps_ok(n):
            nodes[n["id"]]["blocked_by"] = blocked_by(n, states, meta)
    unreceipted = any(not row["verdict"] for n in graph["nodes"]
                      if states[n["id"]] in ("done", "partial")
                      for row in external_effect_receipts(n, recs[n["id"]]))
    if unreceipted and status == "done":
        status = "failed"  # committed artifacts remain done; publication is owed
    exit_state = runner_exit_read(r)
    # Honest status: the runner's own graph-bound verdict in runner_exit.json
    # OUTRANKS the pid-liveness guess. A fresh runner deletes the record at boot
    # (wf.py), so a present, fingerprint-valid record was written by a runner
    # process that already exited — a recycled/stale pid reading "live" must not
    # keep a finished run looking running (or an exited one look resumable).
    verdict = str((exit_state or {}).get("reason") or "")
    if verdict == "done" and not unreceipted and not any(s == "failed" for s in states.values()):
        status = "done"
    elif verdict == "stopped":
        status = "stopped"
    elif verdict == "budget_exhausted":
        # est-3acn: the runner's own capped-out verdict — a graceful partial
        # regardless of which node died the cap (failed and partial node records
        # both stand as committed; the run is terminal, not blocked).
        status = "partial"
    elif verdict.startswith("blocked by failed"):
        status = "failed"
    if verdict.startswith("crashed:") and (status == "interrupted" or stored_graph.get("on_exit")):
        status = "failed"  # an exit handler must never mask the recorded crash verdict
    # sys-5lnm17 (upstream of sys-ntdq1v): WHICH nodes died, derived here from the
    # SAME efp-valid states the status verdict used — never stored, never from
    # prose. A crash verdict with no failed node reads empty (honest absence: the
    # runner died, not a node).
    failed_nodes = [{"id": n["id"], "error": (recs[n["id"]] or {}).get("error"),
                     "error_class": (recs[n["id"]] or {}).get("error_class")}
                    for n in graph["nodes"] if states[n["id"]] == "failed"]
    accounting = {}
    if telemetry:
        import time as _time
        clock_now = _time.time()
        ledger, ledger_coverage = _clock_ledger(r, nodes)
        ledger_available = ledger_coverage["status"] == "read"
        for n in graph["nodes"]:
            nid = n["id"]
            clock = execution_clocks(
                n, nodes[nid], waits.get(nid, {}) if states[nid] == "pending" else {}, clock_now, ledger[nid], ledger_available)
            rec = recs[nid] or {}
            if status == "stopped" and clock["phase"] in ("pending", "queued"):
                clock["phase"] = "stopped"
            if states[nid] in ("failed", "partial") and rec.get("error_class") in ("seat_wait", "timeout", "cap_exhausted"):
                clock["phase"] = "expired"
                clock["exhaustion_reason"] = {"seat_wait": "queue_timeout", "timeout": "work_timeout",
                                              "cap_exhausted": "turn_cap"}[rec["error_class"]]
                if rec.get("error_class") == "seat_wait":
                    clock["queue_elapsed_s"] = rec["ms"] / 1000 if isinstance(rec.get("ms"), (int, float)) else None
            if held and held.get("id") == nid:
                hm = jload(r / "gates" / f"{nid}.held.json", {}) or {}
                since = hm.get("since") if record_efp_valid(hm, byid, n, "_def") else None
                elapsed = max(0, clock_now - since) if isinstance(since, (int, float)) else None
                timeout = n.get("hold_timeout")
                remaining = max(0, timeout - elapsed) if isinstance(timeout, (int, float)) and elapsed is not None else None
                clock["phase"] = "held"
                clock["hold"] = {"elapsed_s": elapsed, "remaining_s": remaining,
                                 "ceiling": "configured" if timeout is not None else "unbounded",
                                 "expired": remaining == 0 if remaining is not None else None,
                                 "expiry_action": "default_option" if n.get("default_option") else "keep_holding"}
            nodes[nid]["clocks"] = clock
            fanout = n.get("fanout")
            if fanout:
                items = fanout.get("items")
                counts = {"basis": "literal fanout items" if isinstance(items, list) else "dynamic items unknown",
                          "total": len(items) if isinstance(items, list) else None,
                          **{key: 0 if isinstance(items, list) else None
                             for key in ("done", "partial", "failed", "cancelled", "unknown")}}
                for index in range(len(items)) if isinstance(items, list) else ():
                    item = jload(r / "nodes" / f"{nid}.{index}.json", {}) or {}
                    state = item.get("status") if record_efp_valid(item, byid, n) else None
                    if state in ("failed", "partial") and item.get("error_class") in ("seat_wait", "timeout", "cap_exhausted"):
                        reason = {"seat_wait": "queue_timeout", "timeout": "work_timeout", "cap_exhausted": "turn_cap"}[item["error_class"]]
                        group = clock.setdefault("item_expirations", {}).setdefault(reason, {"count": 0})
                        group["count"] += 1
                        if reason == "queue_timeout":
                            ms = item.get("ms")
                            if isinstance(ms, (int, float)) and not isinstance(ms, bool) and ms >= 0:
                                group["observed_queue_s"] = group.get("observed_queue_s", 0) + ms / 1000
                            else:
                                group["unknown_durations"] = group.get("unknown_durations", 0) + 1
                    if state == "failed" and item.get("error_class") == "cancelled":
                        state = "cancelled"
                    counts[state if state in ("done", "partial", "failed", "cancelled") else "unknown"] += 1
                nodes[nid]["item_counts"] = counts
        unknown_counts = [v["clocks"]["work"]["unknown_intervals"] for v in nodes.values()]
        accounting = {"task_total_ceiling": "not_enforced", "coverage": "best_effort",
                      "completeness": "unknown", "lower_bound": True, "ledger": ledger_coverage,
                      "basis": "observed child-seconds; sum across nodes, items and attempts, not elapsed wall time",
                      "observed_work_s": sum(v["clocks"]["work"]["observed_s"] for v in nodes.values()),
                      "unknown_intervals": sum(unknown_counts) if all(x is not None for x in unknown_counts) else None}
    out = {"run_id": r.name, "name": graph.get("name"), "status": status, "accounting": accounting,
           "failed_nodes": failed_nodes,
           "held_gate": held, "nodes": nodes, "graph": stored_graph,
           # 91b9a3de companion: publish the ONE liveness read this state was
           # derived from — a second runner_alive() call a few microseconds
           # later can flip across a dying runner's kernel-released flock, and
           # status vs a re-probed alive disagreeing sent act_wait spinning a
           # respawn it then abandoned (the R6 regression shape). Consumers
           # that need liveness use THIS field.
           "runner_live": live,
           # #44: the ONE lock observation this state was derived from, published
           # so the door never re-probes (wedged = Errno 11 + kernel ledger proves
           # zero holders + lease dead — the recoverable wedge; see runner_alive).
           "runner_lock": _lock_state[0],
           **({"runner_lock_detail": {k: v for k, v in _lock_state[1].items()
                                      if k != "probe"}}
              if _lock_state[0] in ("wedged", "eacces") else {}),
           "runner_exit": exit_state,
           # hw-on5 truth law: status is NOT a completion fact — 'done' legitimately
           # reads done when no work is left (the sys-1603 leaf-fail law), so it must
           # never be consumed as 'finished green'. `completed` is the one field that
           # separates the two senses, derived through the SAME fold as status and
           # failed_nodes (derive-not-stored, sys-5lnm17 law): true only for a
           # terminal done-verdict with zero failed nodes. The key rides only runs
           # that carry a recorded terminal verdict — a run with no verdict stamped
           # yet keeps its exact pre-key key set (the est-3acn D7 verbatim-key-set
           # law), so the field is honest absence before terminal, never a guess.
           **({"completed": (status == "done" and not failed_nodes)}
              if isinstance(exit_state, dict) and exit_state.get("reason") else {}),
           "started": (jload(r / "run.json", {}) or {}).get("started"),
           "owner": (jload(r / "run.json", {}) or {}).get("owner"),
           "done": sum(1 for s in states.values() if s in ("done", "skipped", "partial")),   # #4: a harvest counts done
           "skipped": sum(1 for s in states.values() if s == "skipped"), "total": len(graph["nodes"])}
    # est-3acn: RunBudget read view — spent-vs-cap accounting derived here from
    # the SAME child_metrics join the runner enforces against (derive-not-stored,
    # same law as failed_nodes). Honest absence: no caps on this run => no keys
    # at all, the payload is byte-identical to a pre-budget build's.
    _run_meta = jload(r / "run.json", {}) or {}
    _cap_run = (_run_meta.get("budget") or {}).get("tokens") \
        if isinstance(_run_meta.get("budget"), dict) else None
    _node_caps = {n["id"]: (n.get("budget") or {}).get("tokens")
                  for n in stored_graph["nodes"]
                  if isinstance(n.get("budget"), dict) and (n.get("budget") or {}).get("tokens")}
    if _cap_run is not None or _node_caps:
        rows_by_home = {}
        for n in stored_graph["nodes"]:
            _h = str((recs.get(n["id"]) or {}).get("profile_home") or hermes_home())
            if _h not in rows_by_home:
                try:
                    rows_by_home[_h] = child_metrics(r.name, _h)
                except Exception:
                    rows_by_home[_h] = {}
        per_node = {}
        seen_skeys = set()
        for n in stored_graph["nodes"]:
            nid = n["id"]
            _h = str((recs.get(nid) or {}).get("profile_home") or hermes_home())
            pref = f"wf:{r.name}:{nid}"
            spent = sum(budget_tokens(m) for k, m in rows_by_home.get(_h, {}).items()
                        if (k == pref or k.startswith(pref + ":")) and k not in seen_skeys)
            seen_skeys |= {k for k in rows_by_home.get(_h, {})
                           if k == pref or k.startswith(pref + ":")}
            row = {"spent_tokens": spent}
            if _node_caps.get(nid):
                row["cap_tokens"] = _node_caps[nid]
            if spent or _node_caps.get(nid):
                per_node[nid] = row
        led = jload(r / "budget_ledger.json", {}) or {}
        view = {"cap_tokens": _cap_run, "per_node": per_node,
                "spent_tokens": sum(budget_tokens(m) for home_rows in rows_by_home.values()
                                    for m in home_rows.values())}
        if led.get("cause") == "budget_exhausted":
            view["exhausted_nodes"] = led.get("exhausted_nodes")
            view["never_launched"] = led.get("never_launched")
            view["at"] = led.get("at")
            if status == "partial":
                out["error"] = "budget_exhausted"
        out["budget"] = view
    return out

# ---------- O2: the ONE node-truth read (1.1) ----------
# Closed key set read verbatim from nodes/<id>[.<i>].json. Two readers (dashboard
# _view, the door's A2 facts) share it so they can never disagree. Absent keys read
# `unknown`, never 0 (AGENTS.md rule 8). `steer` rides along from _steer_state,
# which moved here from the door — the door re-imports it (move, not copy).

FACT_KEYS = ("status", "error_class", "error", "attempts", "attempts_log", "final",
             "harvest", "output", "ms", "started", "log_path", "prompt_path", "efp", "skey")
# No-team FACT_KEYS and node_facts shape stay byte-identical. Profile facts are
# appended only when routed children actually carry them in a spawn/result record.

def precondition_facts(rec):
    """1.1 (RATIFY F4) fact rendering for a precondition failure: the string
    'failed (precondition: fix.pr_url)' listing the missing refs, or None when the record
    is not a precondition failure. The refs come from the runner-committed
    `output.missing` list; a record missing that list falls back to the text after
    'precondition unmet: ' in the error. Read model only — the runner owns the commit."""
    if not isinstance(rec, dict) or rec.get("error_class") != "precondition":
        return None
    out = rec.get("output")
    missing = out.get("missing") if isinstance(out, dict) else None
    if not (isinstance(missing, list) and missing):
        tail = str(rec.get("error") or "").split("precondition unmet:", 1)
        missing = [s.strip() for s in tail[1].split(",")] if len(tail) > 1 else []
        missing = [m for m in missing if m]
    if not missing:
        return None
    return "failed (precondition: " + ", ".join(str(m) for m in missing) + ")"

def node_child_home(r, nid, index=None):
    """The state.db HOME a node's children ran under (1.1 RATIFY F2/B7): the record's
    `profile_home` when the node was profile-routed (deriving `<profiles_root>/<profile>`
    when only `profile` survived), else None = the caller's hermes_home() — byte-identical
    for no-team runs. Absent/unreadable record → None (never guess a teammate DB)."""
    name = str(nid) + (f".{index}" if index is not None else "")
    rec = jload(Path(r) / "nodes" / f"{name}.json")
    if not isinstance(rec, dict):
        return None
    ph = rec.get("profile_home")
    if isinstance(ph, str) and ph.strip():
        return Path(ph)
    prof = rec.get("profile")
    if isinstance(prof, str) and prof.strip() and prof != "default":
        return profile_home(prof)
    return None

def run_child_metrics(r):
    """fb 904f5101496be8c1: the run-wide fold the STATUS/WAIT views use — per node
    record through its OWN child DB home (node_child_home), NOT a single query against
    the caller's state.db. A profile-routed node's sessions rows live in the TARGET
    profile's DB; folding only the launcher home returned {} for those nodes and the
    view rendered api_calls/tool_calls 0 + idle_s null for demonstrably-live children
    (false-stall). Solo runs are byte-identical: records with no profile keys all take
    the None home = child_metrics(default). Rows fold per skey; distinct homes can
    never collide because a title embeds the run id and the item index."""
    r = Path(r)
    nodes = r / "nodes"
    out = {}
    seen_skeys = set()
    try:
        names = sorted(p.name for p in nodes.glob("*.json"))
    except OSError:
        names = []
    iterated = set()                      # each distinct home folded exactly once
    for name in names:
        stem = name[:-5]
        nid, _, index = stem.partition(".")
        home = node_child_home(r, nid, index or None)
        if home is None:
            continue                      # solo path: folded once, below, from default home
        mkey = str(home)
        if mkey in iterated:
            continue
        iterated.add(mkey)
        for k, v in child_metrics(Path(r).name, home=home).items():
            prev = out.get(k)
            if prev is None:
                out[k] = v
            else:                         # the SAME skey in a genuinely different home:
                seen_skeys.add(k)         # merge additively, never drop counters.
                for kk in ("tokens_in", "tokens_out", "cache_read", "reasoning",
                           "api_calls", "tool_calls", "attempts"):
                    prev[kk] += v.get(kk) or 0
                prev["cost"] += v.get("cost") or 0.0
                for kk in ("model", "billing_provider", "last_activity", "last_desc"):
                    prev[kk] = prev.get(kk) or v.get(kk)
                prev["api_calls_known"] = prev.get("api_calls_known", True) and v.get("api_calls_known", True)
                prev["sessions"].update(v.get("sessions") or {})
    # Any node record without a profile home (and the whole-run case where NONE exist)
    # folds from the caller's home — but at most once.
    unowned = [p.name[:-5] for p in (nodes.glob("*.json") if nodes.is_dir() else [])
               if (lambda s: node_child_home(r, s.partition(".")[0], s.partition(".")[2] or None))(p.name[:-5]) is None]
    if unowned or not seen_skeys:
        for k, v in child_metrics(Path(r).name).items():
            out.setdefault(k, v)
    return out

def _aggregate_item_fact(r, nid, index):
    """Read-only projection of an item from a CURRENT done fan-out aggregate.
    Never a recovery record: node_rec still reads only the aggregate on disk."""
    graph = jload(Path(r) / "graph.json")
    if not isinstance(graph, dict) or not isinstance(graph.get("nodes"), list):
        return None
    byid = {n["id"]: n for n in graph["nodes"] if isinstance(n, dict) and n.get("id")}
    parent = str(nid)
    # Accept either the explicit item index or its canonical record stem, but
    # never split a real graph node whose own id happens to contain a dot.
    if index is None and parent not in byid:
        parent, _, index = parent.rpartition(".")
    if isinstance(index, str) and index.isdecimal():
        index = int(index)
    n = byid.get(parent)
    if type(index) is not int or index < 0 or not n or not n.get("fanout"):
        return None
    st, aggregate = node_rec(Path(r), n, byid)
    if st != "done" or not isinstance(aggregate, dict):
        return None  # stale, amended, partial and uncommitted aggregates cannot attest
    output = aggregate.get("output")
    rows = output.get("all_results") if isinstance(output, dict) else None
    row = rows[index] if isinstance(rows, list) and index < len(rows) else None
    if not isinstance(row, dict) or row.get("status") not in ("done", "partial", "failed", "skipped"):
        row = {"status": "superseded"}  # aggregate closed it, exact item verdict unknown
    return parent + ".json", row


def node_facts(r, nid, index=None):
    """Record facts for one node (fan-out item via `index`), plus its steer truth.
    A current done aggregate supersedes leftover running item spawn facts.
    None when the node has no record at all (never fabricate a record)."""
    name = str(nid) + (f".{index}" if index is not None else "")
    rec = jload(Path(r) / "nodes" / f"{name}.json")
    if not isinstance(rec, dict):
        return None
    superseded = _aggregate_item_fact(r, nid, index) if rec.get("status") == "running" else None
    if superseded:
        source, row = superseded
        # Keep spawn provenance, but outcome fields come ONLY from the committed
        # aggregate row. No writes, item rescue, or fabricated success verdict.
        rec = {k: v for k, v in rec.items() if k not in
               ("status", "output", "error", "error_class", "harvest", "final", "ms")}
        rec.update(row)
    facts = {k: (rec[k] if k in rec and rec[k] is not None else "unknown")
             for k in FACT_KEYS}
    if superseded:
        facts["superseded_by_aggregate"] = source
        if rec["status"] == "superseded":
            pid = rec.get("pid")
            facts["pid_state"] = "unknown"
            if type(pid) is int and pid > 0:
                try:
                    os.kill(pid, 0)
                    facts["pid_state"] = "present"  # presence is not verified spawn identity
                except ProcessLookupError:
                    facts["pid_state"] = "absent"
                except OSError:
                    pass
    facts["steer"] = _steer_state(Path(r), str(nid))
    if rec.get("profile"):
        facts["profile"] = rec["profile"]
        facts["profile_home"] = rec.get("profile_home") or "unknown"
    pf = precondition_facts(rec)
    if pf:
        facts["fact"] = pf
    sb = explain_stale(Path(r), nid, index)   # why-rerun: absent unless actually stale
    if sb:
        facts["stale_because"] = sb
    return facts

def _steer_state(r, nid):
    """B1 + #17 evidence read model for one node: queued = lines addressed to the
    node in the run inbox; baked = how many its LATEST spawn copied into its bake
    file; consumed = how far the spawn's cursor advanced (inbox pulls; also
    echoed as `delivered` for existing readers). All derived from files only —
    no model self-report is trusted."""
    try:
        raw = [l for l in (r / "inbox.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    except OSError:
        return None
    addressed = 0
    for l in raw:
        try:
            m = json.loads(l)
        except Exception:
            continue
        if m.get("node") == nid and m.get("text") is not None and m.get("cmd") != "kill":
            addressed += 1
    rec = jload(r / "nodes" / f"{nid}.json") or {}
    baked = delivered = 0
    if (r / "steer").is_dir():
        files = sorted((r / "steer").glob(f"{nid}.a*.jsonl"),
                       key=lambda p: int(p.name[len(nid) + 2:-len(".jsonl")] or 0))
        spawn_rec = files[-1] if files else None
    else:
        spawn_rec = None
    if spawn_rec is not None:
        try:
            baked = len([l for l in spawn_rec.read_text(encoding="utf-8").splitlines() if l.strip()])
        except OSError:
            baked = 0
        cur = spawn_rec.with_name(spawn_rec.name.replace(".jsonl", ".cursor"))
        try:
            delivered = min(int(cur.read_text().strip() or "0"), baked)
        except (OSError, ValueError):
            delivered = 0
    if not addressed and not baked:
        return None
    return {"queued": addressed, "baked": baked, "consumed": delivered}

def run_executed(r):
    """Was this run dir actually EXECUTED? A run that ever spawned work has
    logs under <run>/logs; a zero-log dir is a fixture, not an executed run
    (the 774-dir production leak was entirely zero-log fixtures — est-2ek.1.762
    census). Read-only, never raises."""
    try:
        logs = r / "logs"
        return logs.is_dir() and any(logs.iterdir())
    except OSError:
        return False


def run_summary(runs, executed=None):
    """Counts cover the complete census, even when a caller returns a recent page.
    `executed` (est-2ek.1.762): when a listing tool supplies the logs-present
    count, it rides inside `counts` as `executed` — total-minus-executed is the
    fixture/stub population. Absent stays absent (v1.0.15 key set untouched)."""
    counts = {"running": 0}
    for run in runs:
        status = run["status"]
        counts[status] = counts.get(status, 0) + 1
    if executed is not None:
        counts["executed"] = executed
    return {"total": len(runs), "counts": counts}

# ---------- amend preview (the replay-skip law the runner applies, computed ahead) ----------

def _downstream(byid):
    kids = {i: [] for i in byid}
    for n in byid.values():
        for a in n.get("after", []):
            if a in kids:
                kids[a].append(n["id"])
    return kids

def blocked_legibility(nodes, states, blocked):
    """Read-only context for run.blocked: finished work that cannot converge and
    the failed roots upstream of each blocked-pending node. Reuse the same
    dependency adjacency as amend_preview; preserve graph order in every list."""
    byid = {n["id"]: n for n in nodes}
    kids = _downstream(byid)
    blocked_set = set(blocked)
    failed = [n["id"] for n in nodes if states[n["id"]] == "failed"]
    blockers = {nid: [] for nid in blocked}
    unconverged = []
    for n in nodes:
        nid = n["id"]
        if states[nid] not in ("failed", "done", "partial"):
            continue
        seen, stack = set(), list(kids[nid])
        while stack:
            child = stack.pop()
            if child in seen:
                continue
            seen.add(child)
            stack.extend(kids[child])
        hits = blocked_set & seen
        if states[nid] in ("done", "partial") and hits:
            unconverged.append(nid)
        elif nid in failed:
            for target in blocked:
                if target in hits:
                    blockers[target].append(nid)
    return unconverged, blockers

def residue(nodes, states, blocked, unconverged, outputs):
    """est-ij0: classify run.blocked residue by CAUSE, not crowd.
    data_dead  = blocked ids reachable from a failed node over DATA edges only
                 (true descendants of the death: they need the dead work redone);
    order_dead = the rest — blocked only through an order_only (convoy) edge, so
                 their own work never depended on the death.
    verdicts   = per unconverged id, the committed output's `verdict` (a world-state
                 claim) beside the committed status — never node.status alone: a
                 `done` merge seat whose verdict is `not-approved` merged nothing.
    Graph order everywhere; read-only."""
    byid = {n["id"]: n for n in nodes}
    dkids = {i: [] for i in byid}
    for n in nodes:
        oo = set(n.get("order_only") or ())
        for a in n.get("after", []):
            if a in dkids and a not in oo:
                dkids[a].append(n["id"])
    seen, stack = set(), [n["id"] for n in nodes if states.get(n["id"]) == "failed"]
    while stack:
        x = stack.pop()
        for k in dkids.get(x, []):
            if k not in seen:
                seen.add(k)
                stack.append(k)
    bset = set(blocked)
    data_dead = [n["id"] for n in nodes if n["id"] in bset and n["id"] in seen]
    order_dead = [n["id"] for n in nodes if n["id"] in bset and n["id"] not in seen]
    verdicts = {}
    for nid in unconverged:
        out = outputs.get(nid)
        v = out.get("verdict") if isinstance(out, dict) else None
        verdicts[nid] = {"status": states.get(nid), "verdict": v if isinstance(v, str) else None}
    return {"data_dead": data_dead, "order_dead": order_dead, "verdicts": verdicts}

def apply_node_patch(old_graph, patch_obj):
    """est-2del: per-node patch for amend — change a couple of fields without
    resubmitting the whole (multi-KB) graph. Pure + fail-closed:

      - patch_obj is {"nodes": {id: {field: value}}, "name": str}? — `nodes` is
        the ONLY structural key; anything else is refused, so a typo'd patch
        can never silently drop authoring (adding NEW nodes stays full-graph
        work: patch edits, it does not author).
      - a node's patch is a SHALLOW field merge; a field value of null deletes
        the field; patching `id` or patching an unknown node id is refused by
        name (no silent no-op, no re-keying).
      - "name", when present, replaces the graph name.
    Returns a NEW graph (deep-copied nodes, original order); never mutates.
    Raises ValueError with a named reason — the door turns it into a typed
    refusal before anything is written."""
    import copy as _copy
    if not isinstance(patch_obj, dict):
        raise ValueError("patch must be an object")
    unknown = sorted(set(patch_obj) - {"nodes", "name"})
    if unknown:
        raise ValueError(f"patch keys {unknown} not allowed (only 'nodes' and 'name')")
    nodes = patch_obj.get("nodes")
    if nodes is None and "name" not in patch_obj:
        raise ValueError("patch is empty: needs nodes and/or name")
    if nodes is not None:
        if not isinstance(nodes, dict) or not nodes:
            raise ValueError("patch.nodes must be a non-empty object of node id -> field updates")
    if not isinstance(old_graph, dict) or not isinstance(old_graph.get("nodes"), list):
        raise ValueError("committed graph has no nodes to patch")
    byid = {}
    for n in old_graph["nodes"]:
        if isinstance(n, dict) and isinstance(n.get("id"), str):
            byid[n["id"]] = n
    new_nodes = []
    seen = set()
    for n in old_graph["nodes"]:
        nid = n.get("id") if isinstance(n, dict) else None
        if isinstance(nid, str) and nodes and nid in nodes:
            upd = nodes[nid]
            seen.add(nid)
            if not isinstance(upd, dict) or not upd:
                raise ValueError(f"patch.nodes.{nid} must be a non-empty object of field updates")
            if "id" in upd:
                raise ValueError(f"patch.nodes.{nid} cannot change the node id")
            m = _copy.deepcopy(n)
            for k, v in upd.items():
                if v is None:
                    m.pop(k, None)
                else:
                    m[k] = _copy.deepcopy(v)
            new_nodes.append(m)
        else:
            new_nodes.append(_copy.deepcopy(n))
    if nodes:
        missing = sorted(set(nodes) - seen)
        if missing:
            raise ValueError(f"patch names unknown node ids {missing} (patch edits existing nodes only; adding nodes needs the full graph)")
    out = _copy.deepcopy(old_graph)
    out["nodes"] = new_nodes
    if "name" in patch_obj:
        if not isinstance(patch_obj["name"], str) or not patch_obj["name"]:
            raise ValueError("patch.name must be a non-empty string")
        out["name"] = patch_obj["name"]
    return out


def amend_preview(r, new_nodes):
    """{added, removed, changed, will_rerun, unchanged, will_reset_running} for a
    proposed graph against the run dir's committed graph and node records. Added
    nodes are work; changed committed records plus their downstream nodes also rerun.
    Unchanged done records replay-skip. Other never-committed nodes stay out unless
    they descend from added/changed work. Read-only.

    `will_reset_running` (#366): every node whose on-disk record is a LIVE SPAWN
    record (status="running") at amend time. `node_rec` reads that record as pending
    (running is never a commit), so the restart/reload this amend triggers RESETS the
    node whatever its def says — and outside the fan-out branch the live child is
    never adopted, so its in-flight work dies and re-runs from zero. Such a node used
    to appear NOWHERE in this response (def-identical, so not `changed`; never
    committed, so not `unchanged`; not downstream, so not in `will_rerun`), which let
    the door answer `changed:[effect_receipts] will_rerun:[effect_receipts,
    verify_lanes]` for an amend that also destroyed two running nodes' children. The
    key names the disruption whether or not the def moved; the def-drift class stays
    in `changed` exactly as #18 shipped it."""
    old_nodes = (jload(r / "graph.json", {}) or {}).get("nodes", [])
    old_ids = {n["id"] for n in old_nodes
               if isinstance(n, dict) and isinstance(n.get("id"), str)}
    byid = {n["id"]: n for n in new_nodes}
    new_ids = set(byid)
    added = [n["id"] for n in new_nodes if n["id"] not in old_ids]
    removed = [n["id"] for n in old_nodes
               if isinstance(n, dict) and isinstance(n.get("id"), str)
               and n["id"] not in new_ids]
    kids = _downstream(byid)
    status, committed_mismatch, def_drift, live_reset = {}, set(), set(), set()
    for n in new_nodes:
        st, rec = node_rec(r, n, byid)
        status[n["id"]] = st
        if st == "pending" and (rec or {}).get("status") in ("done", "partial", "failed", "skipped"):
            committed_mismatch.add(n["id"])  # committed but efp-stale
        elif st == "pending" and (rec is None or rec.get("status") == "running"):
            # never committed (pending, or RUNNING with only the spawn-time record):
            # the reset class is named below regardless of def drift; the def compare
            # is only about #18 (a model/provider-only edit must not preview as no-work).
            if isinstance(rec, dict) and rec.get("status") == "running":
                live_reset.add(n["id"])   # #366: a live child this amend resets, def or no def
            old = next((o for o in old_nodes if isinstance(o, dict) and o.get("id") == n["id"]), None)
            if isinstance(old, dict) and def_hash(old) != def_hash(n):
                def_drift.add(n["id"])
    changed = sorted(committed_mismatch | def_drift)
    will = set()
    stack = changed + added
    while stack:
        x = stack.pop()
        if x in will:
            continue
        will.add(x)
        stack.extend(kids.get(x, []))
    will_rerun = [n["id"] for n in new_nodes if n["id"] in will]
    unchanged = [n["id"] for n in new_nodes
                 if status[n["id"]] in ("done", "skipped", "partial") and n["id"] not in will]
    will_reset_running = [n["id"] for n in new_nodes if n["id"] in live_reset]
    return {"added": added, "removed": removed, "changed": changed,
            "will_rerun": will_rerun, "unchanged": unchanged,
            "will_reset_running": will_reset_running}

# ---------- seat-level model floor (stdlib-only when core isn't importable) ----------

SEATS_DEFAULT = 4          # the semaphore's shipped default (wf.py binds the same law)
SEAT_CAP_FLOOR = 4         # the smallest cap a raise/override may name (est-2ek.1.856)
SEAT_CAP_CEILING = 16      # the largest cap config/env may name (est-2ek.1.856)


class SeatCapError(ValueError):
    """An operator-named seat cap (WORKFLOW_MAX_SEATS env or config workflows.max_seats)
    that is not an integer inside [SEAT_CAP_FLOOR, SEAT_CAP_CEILING]. A typo'd cap is an
    operator INPUT error, never a silent SEATS_DEFAULT: the door refuses the launch with
    it and the runner fails closed with it, exactly the seat_forbidden_models law."""


def _seat_cap_valid(v, label):
    """Validate one operator-named cap. bool is NOT an int here (True -> 1 is a typo
    shape, not a cap). Out-of-band or unparseable raises SeatCapError naming the knob.
    0 is the documented semaphore-off sentinel (the est-g255 `WORKFLOW_MAX_SEATS=0`
    guidance on hosts with no ancestry channel) — valid ONLY as an explicit 0; every
    other value must sit inside [SEAT_CAP_FLOOR, SEAT_CAP_CEILING]."""
    if isinstance(v, bool) or not isinstance(v, (int, str)):
        raise SeatCapError(f"{label}={v!r} is not an integer")
    try:
        n = int(v)
    except ValueError:
        raise SeatCapError(f"{label}={v!r} is not an integer")
    if n == 0:
        return 0                                    # explicit off, est-g255 guidance
    if not (SEAT_CAP_FLOOR <= n <= SEAT_CAP_CEILING):
        raise SeatCapError(f"{label}={v!r} is outside [{SEAT_CAP_FLOOR}, {SEAT_CAP_CEILING}] "
                           "(or 0 to disable the semaphore)")
    return n


def seat_max_seats(meta_cap=None, dotenv_reader=None):
    """The global agent-seat cap, resolved at every use (config is re-read, never baked).

    Precedence (est-2ek.1.856): explicit per-run run.json meta max_seats (the door's
    existing knob, 0 disables) > WORKFLOW_MAX_SEATS env > config workflows.max_seats
    > SEATS_DEFAULT. An operator-named cap (env or config) MUST be an integer inside
    [SEAT_CAP_FLOOR, SEAT_CAP_CEILING] or this raises SeatCapError — an invalid value
    is a clear door error (refuse before any write or spawn), never a silent default.
    0/false via env/config is NOT the opt-out (0 would silently disable the
    semaphore); the opt-out stays the explicit per-run meta max_seats.
    Config is read core-first (load_config_readonly — the seat's profile-aware
    config, including the `workflows` section core defaults away), then a lite
    top-level scan of <home>/config.yaml for bare hosts without hermes_cli
    (same shape as seat_forbidden_models)."""
    if isinstance(meta_cap, int) and not isinstance(meta_cap, bool):
        return meta_cap
    env = (os.environ.get("WORKFLOW_MAX_SEATS") or "").strip()
    if env:
        return _seat_cap_valid(env, "WORKFLOW_MAX_SEATS")
    # est-gxjh6 (union with est-2ek.1.856): a long-lived runner inherits its
    # launcher's environment, so the .env files are re-read FRESH at every cap
    # decision via this hook (profile .env then root .env). The reader itself
    # stays lenient — garbage/negative/non-numeric lines are skipped, an
    # unreadable file is absent — but a value it does return is an
    # operator-named cap and rides the same validation as the exported env.
    if dotenv_reader is not None:
        d = dotenv_reader()
        if d is not None:
            return _seat_cap_valid(d, "WORKFLOW_MAX_SEATS (.env)")
    raw = None
    try:
        from hermes_cli.config import load_config_readonly
        cfg = load_config_readonly()
        if cfg:
            w = cfg.get("workflows")
            if isinstance(w, dict):
                raw = w.get("max_seats")
    except Exception:
        raw = None
    if raw is None:
        home = None
        try:
            from hermes_constants import get_hermes_home
            home = get_hermes_home()
        except Exception:
            home = Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))
        try:
            lines = (home / "config.yaml").read_text().splitlines()
        except OSError:
            lines = []
        section = None
        for line in lines:
            line = line.split(" #", 1)[0]
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip())
            text = line.strip()
            if indent == 0:
                section = text.partition(":")[0]
            elif section == "workflows" and indent == 2:
                key, _, scalar = text.partition(":")
                if key == "max_seats" and scalar.strip():
                    raw = scalar.strip().strip("'\"")
                    break
    if raw is None:
        return SEATS_DEFAULT
    return _seat_cap_valid(raw, "config workflows.max_seats")


def seat_forbidden_models():
    """Read model.workflows_forbidden_models on the child seat, including bare CLI hosts.

    A malformed configured floor is not an empty ban: reject it at the door and
    fail closed at the runner. Missing config is the only opt-out.
    """
    try:
        from hermes_cli.config import load_config_readonly
        cfg = load_config_readonly()
        if cfg:
            return (cfg.get("model") or {}).get("workflows_forbidden_models", [])
    except Exception:
        pass
    home = hermes_home()
    try:
        lines = (home / "config.yaml").read_text().splitlines()
    except OSError:
        return []
    section = key = None
    values = []
    for raw in lines:
        line = raw.split(" #", 1)[0]
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        text = line.strip()
        if indent == 0:
            section, key = text.partition(":")[0], None
        elif section == "model" and indent == 2:
            key, _, scalar = text.partition(":")
            if key == "workflows_forbidden_models" and scalar.strip():
                # Simple flow list is supported; malformed scalar remains malformed.
                try:
                    import json
                    return json.loads(scalar.strip())
                except (ValueError, TypeError):
                    return scalar.strip()
        elif section == "model" and key == "workflows_forbidden_models" and indent > 2:
            if text.startswith("- "):
                values.append(text[2:].strip().strip("'\""))
            else:
                return text  # malformed list, not an empty ban
    return values

# ---------- #116: confidence_substrate — the estate's sanctioned fallback ladder ----------
# When a node's PINNED confidence route is proved dead at submit the #25 gate
# fail-closes (correct). confidence_substrate is the owner's declared, sanctioned
# substitute: consulted ONLY on that proved-dead branch, so absent config keeps
# today's fail-closed behavior byte-identical. Resolution is ENGINE-stamped (node
# record + machine-injected result-schema disclosure) — a child can neither learn
# the substitution away nor author the honest label itself.
# Sources, first hit wins: env WF_CONFIDENCE_SUBSTRATE (explicit override; comma
# list) > `plugins.entries.hermes-workflows.settings.confidence_substrate`
# (owner-settings vocabulary, door ctx or raw config read) > top-level
# `workflows: confidence_substrate:` in the seat's config.yaml. Shapes accepted:
# "provider/model", comma-separated list of those, list of "provider/model"
# strings, list of {provider, model} mappings. A malformed value is NO ladder
# (fail closed as today) — never a substitution invented from garbage.

SUBSTRATE_ENV = "WF_CONFIDENCE_SUBSTRATE"
SUBSTRATE_DISCLOSURE_KEY = "substrate_disclosure"

def _substrate_rungs(raw):
    """Normalize any accepted raw shape to an ORDERED list of 'provider/model'
    rungs; unusable entries are dropped, an unusable whole value yields [] ."""
    if raw is None:
        return []
    items = []
    if isinstance(raw, str):
        items = [s.strip() for s in raw.split(",")] if "," in raw else [raw]
    elif isinstance(raw, dict):
        items = [raw]
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    rungs = []
    for it in items:
        if isinstance(it, str):
            s = it.strip().strip("'\"")
            p, sep, m = s.partition("/")
            if sep and p.strip() and m.strip():
                rungs.append(f"{p.strip()}/{m.strip()}")
        elif isinstance(it, dict):
            p = str(it.get("provider") or "").strip()
            m = str(it.get("model") or "").strip()
            if p and m:
                rungs.append(f"{p}/{m}")
    return rungs

def _substrate_from_config_top(home):
    """Top-level `workflows: confidence_substrate:` — full YAML when a loader is
    importable, else the YAML-lite scan (simple list/scalar shapes only), the same
    spirit as seat_forbidden_models' bare-CLI fallback."""
    try:
        text = (Path(home) / "config.yaml").read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        cfg = _yaml_load(text) or {}
        if isinstance(cfg, dict):
            sec = cfg.get("workflows")
            return (sec.get("confidence_substrate") if isinstance(sec, dict) else None)
    except Exception:
        pass
    section = None
    key = None
    values = []
    scalar = None
    for raw in text.splitlines():
        line = raw.split(" #", 1)[0]
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        t = line.strip()
        if indent == 0:
            if section == "workflows" and key == "confidence_substrate":
                break
            section, key = t.partition(":")[0], None
        elif section == "workflows" and indent == 2:
            k, _, v = t.partition(":")
            if k == "confidence_substrate":
                key = k
                if v.strip():
                    scalar = v.strip().strip("'\"")
        elif section == "workflows" and key == "confidence_substrate" and indent > 2:
            if t.startswith("- "):
                values.append(t[2:].strip().strip("'\""))
            else:
                return None      # lite parser can't answer this shape honestly
    if scalar is not None:
        return scalar
    return values or None

def confidence_substrate(home=None):
    """(ordered rungs, source) for the estate's sanctioned fallback substrate.
    [] = no declared substrate (the #116 branch never fires; #25 stays law)."""
    env = (os.environ.get(SUBSTRATE_ENV) or "").strip()
    if env:
        rungs = _substrate_rungs(env)
        if rungs:
            return rungs, f"env:{SUBSTRATE_ENV}"
        return [], None
    try:
        v = owner_setting("confidence_substrate")
    except Exception:
        v = None
    rungs = _substrate_rungs(v)
    if rungs:
        return rungs, "settings:confidence_substrate"
    if v is not None and (isinstance(v, (str, dict, list))):
        # a present-but-unusable settings value is a no-ladder, like a bad config
        return [], None
    raw = _substrate_from_config_top(home if home is not None else hermes_home())
    return _substrate_rungs(raw), ("config:workflows.confidence_substrate" if raw is not None else None)

def substrate_disclosure_text(stamp):
    """The engine's honest-label sentence — verbatim material for the schema
    description and the committed record. Built ONLY from the stamp dict."""
    return (f"SUBSTRATE DISCLOSURE (engine-stamped, not author-written): this node was "
            f"pinned {stamp.get('from')!r}; the estate confidence_substrate policy served "
            f"{stamp.get('to')!r} ({stamp.get('reason') or 'route proved unavailable at submit'}).")

def apply_substrate_disclosure(node, stamp):
    """Engine-inject the disclosure clause into the node's RESULT SCHEMA (mirrors how
    the runner forces the answer/schema blocks — the child cannot author it away):
    property + REQUIRED key `substrate_disclosure`, description naming the original
    pin. Mutates node['schema'] in place (creates one when the node had none)."""
    schema = node.get("schema")
    if not isinstance(schema, dict):
        schema = {"type": "object", "properties": {}}
        node["schema"] = schema
    props = schema.setdefault("properties", {})
    props[SUBSTRATE_DISCLOSURE_KEY] = {"type": "string",
                                       "description": substrate_disclosure_text(stamp)}
    req = schema.setdefault("required", [])
    if SUBSTRATE_DISCLOSURE_KEY not in req:
        req.append(SUBSTRATE_DISCLOSURE_KEY)

def strip_engine_disclosure(schema, stamp):
    """The validation-view of a substituted node's schema: the disclosure key is
    ENGINE-owned (the runner stamps it into the record at commit), so its ABSENCE in
    a child answer is never a schema failure — the child cannot author it and must
    not be killed for omitting it. Returns the schema unchanged for everything else."""
    if not stamp or not isinstance(schema, dict):
        return schema
    s = dict(schema)
    if SUBSTRATE_DISCLOSURE_KEY in (s.get("required") or []):
        s["required"] = [k for k in s["required"] if k != SUBSTRATE_DISCLOSURE_KEY]
        if not s["required"]:
            s.pop("required", None)
    props = s.get("properties")
    if isinstance(props, dict) and SUBSTRATE_DISCLOSURE_KEY in props:
        s["properties"] = {k: v for k, v in props.items() if k != SUBSTRATE_DISCLOSURE_KEY}
        if not s["properties"]:
            s.pop("properties", None)
    return s

# ---------- live child metrics (state.db join) ----------
# Children run with `--continue wf:<run>:<node>[:<i>]:<efp8>#a<attempt>`, so each child's
# sessions row carries that key as its title. One read-only query per view returns every
# row for the run; rows fold per node/item across attempts. Deterministic liveness: a
# running child's api_call_count / tool_call_count / last_activity_at move while it cooks.
_METRIC_COLS = ("input_tokens", "output_tokens", "cache_read_tokens", "reasoning_tokens",
                "api_call_count", "tool_call_count", "estimated_cost_usd")

def child_metrics(run_id, home=None):
    """{skey: {tokens_in, tokens_out, cache_read, reasoning, api_calls, tool_calls, cost,
    model, last_activity, last_desc, attempts, ended}} for every child of the run, or {}."""
    import sqlite3
    home = Path(home) if home else hermes_home()
    db = home / "state.db"
    if not db.exists():
        return {}
    out = {}
    try:
        c = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=0.5)
        try:
            have_bp = any(r[1] == "billing_provider" for r in c.execute("pragma table_info(sessions)"))
            bp_sel = "billing_provider, " if have_bp else ""
            rows = c.execute(
                f"select title, model, {bp_sel}input_tokens, output_tokens, cache_read_tokens, reasoning_tokens, "
                "api_call_count, tool_call_count, estimated_cost_usd, last_activity_at, "
                "last_activity_description, ended_at, started_at from sessions where title like ? "
                "order by title, started_at, rowid",
                (f"wf:{run_id}:%",)).fetchall()
        finally:
            c.close()
    except Exception:
        return {}
    for (title, model, *rest) in rows:
        billing_provider = None
        if len(rest) == 11:   # legacy schema (no billing_provider column): honest unknown
            (ti, to, cr, rs, api, tools, cost, last, desc, ended, started) = rest
        else:
            (billing_provider, ti, to, cr, rs, api, tools, cost, last, desc, ended, started) = rest
        key = title.split("#a", 1)[0]
        m = out.setdefault(key, {"tokens_in": 0, "tokens_out": 0, "cache_read": 0, "reasoning": 0,
                                 "api_calls": 0, "api_calls_known": True,
                                 "tool_calls": 0, "cost": 0.0, "attempts": 0,
                                 "model": None, "billing_provider": None, "last_activity": None, "last_desc": None,
                                 "ended": None, "started": None, "sessions": {}})
        m["sessions"][title] = {"last_activity": last or started,
                                "last_desc": desc or None}
        if api is None:
            m["api_calls_known"] = False
        m["tokens_in"] += ti or 0; m["tokens_out"] += to or 0; m["cache_read"] += cr or 0
        m["reasoning"] += rs or 0; m["api_calls"] += api or 0; m["tool_calls"] += tools or 0
        m["cost"] += cost or 0.0; m["attempts"] += 1
        m["model"] = model or m["model"]
        m["billing_provider"] = billing_provider or m["billing_provider"]
        la = last or started
        if la and (m["last_activity"] is None or la > m["last_activity"]):
            m["last_activity"], m["last_desc"] = la, desc or None
        if m["started"] is None or (started and started < m["started"]):
            m["started"] = started
        m["ended"] = (ended if m["attempts"] == 1 else
                      max(m["ended"], ended) if m["ended"] is not None and ended is not None else None)
    return out

STALL_IDLE_S = 600   # #127 (jam-h1): the query-time stall threshold, tunable per call

def heartbeat(metrics, progress=None, now=None, stall_s=STALL_IDLE_S):
    """#127 (est-zsws): the ONE per-running-node heartbeat verdict, derived at query
    time from evidence the read model ALREADY has — no new writes, no child
    cooperation, no persisted surface (the caller splices the returned dict into
    the status payload next to `metrics`/`progress`).
      alive   = a verified spawn is live (the existing `live` count decides).
      last_evidence_age_s = the FRESHEST known evidence age among the child's
                session-row idle_s and the #128 artifact progress (mtime_age_s) —
                a child that writes artifacts but never ticks its DB row is NOT
                stalled; None when neither is known (honest absence, R2).
      stalled = alive AND a KNOWN age past the threshold. A stall is NEVER
                inferred from absence: unknown age => stalled False, always.
    api_calls_delta_over_window stays out of v1 per the issue-plan correction:
    child rows carry only the cumulative api_call_count, no per-call series, and
    a delta needs persisted windows this issue forbids."""
    ages = []
    idle = (metrics or {}).get("idle_s")
    if isinstance(idle, (int, float)) and not isinstance(idle, bool):
        ages.append(max(0, int(idle)))
    mtime_age = (progress or {}).get("mtime_age_s")
    if isinstance(mtime_age, (int, float)) and not isinstance(mtime_age, bool):
        ages.append(max(0, int(mtime_age)))
    alive = bool((metrics or {}).get("live"))
    age = min(ages) if ages else None
    return {"alive": alive,
            "last_evidence_age_s": age,
            "stalled": bool(alive and age is not None and age >= stall_s)}

def run_explain(out):
    """#127 (est-zsws): ONE line telling a scheduler why the run is where it is,
    composed ONLY from fields the status payload already carries (node statuses,
    heartbeat verdicts, blocked_by words, parked markers). Terminal runs have
    nothing to explain — None, never an empty slogan."""
    status = out.get("status")
    if status in ("done", "failed", "stopped"):
        return None
    nodes = out.get("nodes") or {}
    running, stalled, pending, parked, blocked_names = 0, [], 0, 0, []
    for nid, v in nodes.items():
        st = v.get("status")
        if st == "running":
            running += 1
            hb = v.get("heartbeat") or {}
            if hb.get("stalled"):
                age = hb.get("last_evidence_age_s")
                stalled.append(f"{nid} {age}s" if age is not None else nid)
        elif st == "pending":
            if v.get("parked"):
                parked += 1
                continue
            pending += 1
            for b in v.get("blocked_by") or []:
                name = str(b).split(":", 1)[0].strip()
                if name and name not in blocked_names:
                    blocked_names.append(name)
    parts = []
    if running:
        parts.append(f"{running} running" + (f" ({len(stalled)} stalled: {', '.join(stalled)})" if stalled else ""))
    if pending:
        p = f"{pending} pending"
        if blocked_names:
            p += f" blocked by {', '.join(blocked_names)}"
        parts.append(p)
    if parked:
        parts.append(f"{parked} parked")
    if not parts:
        return f"{status}: no running work"
    return f"{status}: " + ", ".join(parts)

def current_attempt(cm, spawns):
    """Activity only for live spawn session titles (verified, or ledger-marked foreign-ns); DB rows alone prove no liveness."""
    live = {}
    for spawn in spawns:
        title = (spawn or {}).get("skey")
        if title:
            live[title] = cm.get(title.split("#a", 1)[0], {}).get("sessions", {}).get(title, {})
    recent = [(row["last_activity"], title, row.get("last_desc"))
              for title, row in live.items() if row.get("last_activity") is not None]
    last, _, desc = max(recent) if recent else (None, None, None)
    return {"live": len(live), "last_activity": last, "last_desc": desc}

# R8 (graphify @ 2d97807): the complete fold call set is __init__.py act_status
# (node lines + run total; act_status itself reached only from act_wait and
# _wait_foreign — no fold there, they delegate here) and dashboard/plugin_api.py
# _view's three _fold_metrics sites (node line, fanout item, rollup) — no third
# fold; scripts/ grep for api_calls|child_metrics|_fold is empty.
def fold_child_metrics(rows):
    """#114 (WF-04): THE ONE numeric fold + known/unknown policy for child metric
    rows (the dicts child_metrics/run_child_metrics return). Counters sum exactly
    as the projections always have; api_calls is OMITTED from the result whenever
    ANY contributing row says api_calls_known: false — an unknown count is absent
    (the existing omission convention, R2: absent reads unknown, never invented),
    never a zero and never a partial sum advertised as exact. Pure over
    already-read rows: no IO, no new field, persisted shapes unchanged (R10).
    Both projection adapters (door act_status, dashboard _view) route EVERY
    aggregation level — node line, fanout item, run total/rollup — through here
    so the surfaces can never diverge on which levels hide an unknown.
    Returns None for no rows (the callers' existing "no metrics" sentinel)."""
    rows = list(rows)
    if not rows:
        return None
    f = {"tokens_in": 0, "tokens_out": 0, "cache_read": 0, "reasoning": 0, "api_calls": 0,
         "tool_calls": 0, "cost": 0.0, "attempts": 0, "children": len(rows), "live": 0,
         "last_activity": None, "last_desc": None, "models": []}
    known = True
    for m in rows:
        for k in ("tokens_in", "tokens_out", "cache_read", "reasoning", "api_calls", "tool_calls", "cost", "attempts"):
            f[k] += m.get(k) or 0
        if not m.get("api_calls_known", True):
            known = False
        if m.get("model") and m["model"] not in f["models"]:
            f["models"].append(m["model"])
    if not known:
        f.pop("api_calls")
    return f


# ---------- est-2ek.1.641 route receipts (moved out of wf.py, outbound review
# NousResearch/hermes-agent#133387 ask #1, option A): the DOOR writes the
# admission bake through this privately-bound module — the door already
# spec-loads wfcommon by path, and it must never spec-load the runner wf.py,
# whose old module-level `sys.path.insert(0, plugin-dir)` then poisoned the
# HOST process's import path. wf.py binds these names back (import aliases) so
# every existing runner-side call site keeps resolving byte-identically. ------

ROUTE_RECEPTS_NAME = "route_receipts.json"

def _route_receipts_path(run):
    return Path(run) / ROUTE_RECEPTS_NAME

def _route_receipt_load(run):
    return jload(_route_receipts_path(run), {}) or {}

def bake_route_receipts(run, graph):
    """Door-side admission write (est-2ek.1.641): every agent node the door's
    ping PROVED alive at THIS submit (route_verified baked by
    _route_enforcement) records its proved-alive receipt into the run dir,
    merged over what earlier submits proved. Durable in the LANE (survives
    runner respawn); the runner refuses any later spawn that would bill a
    different model. No proof baked = no receipt written = legacy shape."""
    run = Path(run)
    wrote = False
    p = _route_receipts_path(run)
    try:
        rec = _route_receipt_load(run)
        for n in (graph or {}).get("nodes", []) or []:
            if n.get("type") != "agent" or not n.get("id"):
                continue
            verified = n.get("route_verified")
            route = f"{n.get('provider') or ''}/{n.get('model') or ''}".strip("/")
            if not verified or not route or route == "/":
                continue
            if rec.get(n["id"]) != str(verified):
                rec[n["id"]] = str(verified)
                wrote = True
        if wrote:
            run.mkdir(parents=True, exist_ok=True)
            tmp = p.with_name(f"{p.name}.{os.getpid()}.tmp")
            tmp.write_text(json.dumps(rec, ensure_ascii=False, indent=2))
            os.replace(tmp, p)
    except OSError:
        pass                                   # receipt write best-effort; the HOLD is strict
    return wrote


# ---------- the tiny forgiving schema validator (moved out of wf.py, outbound
# review ask #2): the harvest read path needs it WITHOUT the old generic
# generic lazy import of the runner's validate, which — reached from the door
# or the dashboard —
# raised an uncaught ImportError or imported a FOREIGN top-level `wf`. It now
# lives here, privately bound; wf.py re-exports it for the runner's spawn-time
# checks so `wf.validate` keeps resolving for every existing caller. ---------

import difflib as _difflib

def _value_type_ok(val, prop):
    """est-2ek.1.62: does `val` satisfy the declared type in schema fragment
    `prop` ({} when nothing is declared — an undeclared type leaves nothing
    left to contradict)? Same predicates as chk() below; bool is never a
    number (#113), and 'integer' means an integral value. The suggestion
    gate uses this to REFUSE a rename that would pass `required` and then die
    on the type check."""
    if not prop:
        return True
    t = prop.get("type")
    if t == "object":  return isinstance(val, dict)
    if t == "array":   return isinstance(val, list)
    if t == "string":  return isinstance(val, str)
    if t == "boolean": return isinstance(val, bool)
    if t in ("number", "integer"):
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            return False
        return not (t == "integer" and isinstance(val, float) and not val.is_integer())
    return True                       # untyped fragment: nothing to contradict

def _rename_hint(r, v, s):
    """est-2ek.1.62: when a required key is MISSING but the object carries an
    extra key the schema doesn't name that is CLOSE to it and type-consistent
    with it, the child most likely RE-USED A SIBLING KEY NAME (mean_ranking
    for mean_rank, proposal for id) — a complete, correct answer that died
    error_class=schema because the bare "missing required" gave the retry
    nothing to correct. Name the rename explicitly so the typed-correction
    retry converges instead of re-emitting the same shape. The enriched
    string flows verbatim into the existing retry prompt (validate's callers
    pass errors through unchanged). Closeness is difflib.get_close_matches
    (n=1, cutoff 0.6) PLUS a short-name rule: the worked example from the report
    id/proposal has difflib ratio 0.0 (no shared characters), so similarity
    is meaningless at that length — when the required name is <=2 chars and
    EXACTLY ONE unnamed sibling carries a type-consistent value, that single
    candidate is named. A required name >=3 chars that is a substring of an
    extra key also counts. Non-suggestion paths return "" so every other
    error stays byte-identical (the #107 law)."""
    named = set((s.get("properties") or {}).keys())
    extra = [k for k in v if k not in named]
    if not extra:
        return ""
    prop = (s.get("properties") or {}).get(r) or {}
    cand = _difflib.get_close_matches(r, extra, n=1, cutoff=0.6)
    hit = cand[0] if cand else None
    if hit is None and len(r) <= 2:
        # 'id'-class names are too short for any similarity signal to fire
        # (ratio 0.0 against 'proposal'); guess ONLY when exactly one extra
        # key is type-consistent, so the suggestion is never ambiguous.
        typed = [k for k in extra if _value_type_ok(v[k], prop)]
        if len(typed) == 1:
            hit = typed[0]
    if hit is None and len(r) >= 3:
        contained = [k for k in extra if r in k]
        if contained:
            hit = min(contained, key=lambda k: (len(k), extra.index(k)))
    if hit is None:
        return ""
    if not _value_type_ok(v[hit], prop):
        return ""                     # never recommend a rename that still fails
    return f" (you wrote '{hit}'? the schema needs '{r}')"

def validate(out, schema):
    """Tiny forgiving validator: type / required / properties / items / enum
    / minItems / minLength / maxItems.
    #107: `enum` membership is ENFORCED here — a str value against a
    string-membered enum (exactly what the door's schema_check admits: a
    non-empty list of non-empty strings on type:'string'), so a misspelled
    verdict takes the same typed-correction-retry path as a `type` violation
    and the error string names the allowed set for the retry prompt.
    #96: `minItems` (array length) and `minLength` (string floor) are
    ENFORCED here on exactly the placements the door admits — type:'array' /
    type:'string' with a non-negative int — and only AFTER the value's type
    checked (a wrong-typed value gets the type error, never a pile-on).
    minLength counts NON-BLANK chars (`len(v.strip())`): a whitespace-only
    probe is not a probe — a deliberate, fail-closed deviation from JSON
    Schema, where minLength counts raw chars. Malformed floors (non-int) are
    ignored here, never crash: the door is the enforcement point; a
    hand-built schema must not take the runner down. An all-blank
    verify_list-style array now fails closed like the rest of the law.
    #472: `maxItems` is the cap mirror of minItems (same admitted placement,
    error "{path}: expected at most N item(s)"), and enum membership widens
    onto the boolean and integer closed domains the door now admits — with
    #113's bool law preserved: a bool never satisfies an integer enum (True
    must not hit the 1 rung), while an integral float may hit its integer
    rung exactly as the type check allows it. A heterogeneous or malformed
    enum is ignored here, never crash."""
    errs = []
    if not schema:
        return errs
    def chk(v, s, path):
        t = s.get("type")
        if t == "object" and not isinstance(v, dict): errs.append(f"{path}: expected object")
        elif t == "array" and not isinstance(v, list): errs.append(f"{path}: expected array")
        elif t == "string" and not isinstance(v, str): errs.append(f"{path}: expected string")
        elif t == "boolean" and not isinstance(v, bool): errs.append(f"{path}: expected boolean")
        elif t in ("number", "integer") and (
                isinstance(v, bool)
                or not isinstance(v, (int, float))
                or (t == "integer" and isinstance(v, float) and not v.is_integer())):
            # #113: the two admitted numeric types are DISTINCT contracts.
            # bool is never a number (Python bool subclasses int — the old
            # isinstance(v, (int, float)) let True/False through both).
            # integer accepts integral-valued floats (1.0 is an integer, the
            # modern JSON Schema contract) but rejects fractional ones.
            errs.append(f"{path}: expected number")
        en = s.get("enum")
        if isinstance(en, list) and en:
            # #107 (string members) + #472 (boolean/integer members):
            # membership enforced on exactly the closed domains the door
            # admits. Bool is never an integer member (True must not hit the
            # 1 rung); an integral float may hit its integer rung (#113's own
            # contract); a heterogeneous or malformed enum is IGNORED — the
            # forgiving-validator law, the door is the enforcement point.
            _dom = ((isinstance(v, str) and all(isinstance(x, str) for x in en))
                    or (isinstance(v, bool) and all(isinstance(x, bool) for x in en))
                    or (((isinstance(v, int) and not isinstance(v, bool))
                         or (isinstance(v, float) and v.is_integer()))
                        and all(isinstance(x, int) and not isinstance(x, bool) for x in en)))
            if _dom and v not in en:
                errs.append(f"{path}: not an allowed value (allowed: "
                            + ", ".join(repr(x) for x in en) + ")")
        # #96: floors, enforced only where the door admits them (right value
        # type, non-negative int floor). bool refused: True is not a count.
        mi = s.get("minItems")
        if (t == "array" and isinstance(v, list) and isinstance(mi, int)
                and not isinstance(mi, bool) and mi >= 0 and len(v) < mi):
            errs.append(f"{path}: expected at least {mi} item(s)")
        ml = s.get("minLength")
        if (t == "string" and isinstance(v, str) and isinstance(ml, int)
                and not isinstance(ml, bool) and ml >= 0 and len(v.strip()) < ml):
            errs.append(f"{path}: expected at least {ml} non-blank char(s)")
        # #472: the cap mirror of minItems — same placement, same forgiving
        # shape: enforced only on a real list under an admitted integer cap.
        mx = s.get("maxItems")
        if (t == "array" and isinstance(v, list) and isinstance(mx, int)
                and not isinstance(mx, bool) and mx >= 0 and len(v) > mx):
            errs.append(f"{path}: expected at most {mx} item(s)")
        if isinstance(v, dict):
            for r in s.get("required", []):
                if r not in v:
                    errs.append(f"{path}: missing required '{r}'" + _rename_hint(r, v, s))
            for k, sub in (s.get("properties") or {}).items():
                if k in v: chk(v[k], sub, f"{path}.{k}")
        if isinstance(v, list) and s.get("items"):
            for i, it in enumerate(v): chk(it, s["items"], f"{path}[{i}]")
    chk(out, schema, "$")
    return errs
