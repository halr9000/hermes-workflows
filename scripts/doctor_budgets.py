#!/usr/bin/env python3
"""Read-only budget calibration: telemetry corpus + per-node records -> plain-text table.

Usage: python3 scripts/doctor_budgets.py [--runs-root DIR] [--json]
Applies nothing, changes no defaults. Exit 0 even when the corpus is empty.
Any cell with n < MIN_N prints `insufficient(n)` (never a confident number from thin data).
"""
import argparse
import importlib.util
import json
import math
import sys
from pathlib import Path

MIN_N = 20
ROOT = Path(__file__).resolve().parents[1]


def _load_common():
    spec = importlib.util.spec_from_file_location("_wf_doctor_common", ROOT / "wfcommon.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def pct(sorted_vals, p):
    """Nearest-rank percentile (p in 0..100) of an ascending list; None when empty."""
    if not sorted_vals:
        return None
    k = max(1, math.ceil(p / 100.0 * len(sorted_vals)))
    return sorted_vals[k - 1]


def _cell(vals):
    s = sorted(vals)
    n = len(s)
    if n < MIN_N:
        return {"n": n, "p50": f"insufficient({n})", "p90": f"insufficient({n})"}
    return {"n": n, "p50": pct(s, 50), "p90": pct(s, 90)}


def _jl(p):
    try:
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if isinstance(r, dict):
                yield r
    except OSError:
        return


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def collect(runs_root, presets=None, default_shape="build"):
    root = Path(runs_root)
    sub_dur, sub_tools, skills = {}, {}, {}
    for f in sorted((root / "telemetry").glob("events-*.jsonl")):
        for r in _jl(f):
            if r.get("kind") == "subagent":
                role = r.get("child_role") or "(none)"
                if _num(r.get("duration_ms")) is not None:
                    sub_dur.setdefault(role, []).append(r["duration_ms"])
                if _num(r.get("tool_count")) is not None:
                    sub_tools.setdefault(role, []).append(r["tool_count"])
            elif r.get("kind") == "skill" and r.get("skill_name"):
                s = skills.setdefault(r["skill_name"], {"events": 0, "reused": 0, "known": 0})
                s["events"] += 1
                if isinstance(r.get("reused"), bool):
                    s["known"] += 1
                    s["reused"] += int(r["reused"])
    node_dur = {}   # (shape, model) -> [seconds]
    for rec in sorted(root.glob("*/nodes/*.json")):
        try:
            d = json.loads(rec.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(d, dict) or d.get("status") != "done" or _num(d.get("ms")) is None:
            continue
        shape, model = default_shape, "seat-default"
        try:
            g = json.loads((rec.parent.parent / "graph.json").read_text(encoding="utf-8"))
            for n in g.get("nodes") or []:
                if n.get("id") == rec.stem:
                    shape = n.get("shape") or (g.get("defaults") or {}).get("shape") or default_shape
                    model = n.get("model") or (g.get("defaults") or {}).get("model") or model
        except (OSError, ValueError, AttributeError):
            pass
        node_dur.setdefault((shape, model), []).append(d["ms"] / 1000.0)
    out = {"subagents": {r: {"duration_ms": _cell(v), "tool_count": _cell(sub_tools.get(r, []))}
                         for r, v in sorted(sub_dur.items())},
           "skills": {k: {**v, "reuse_rate": (round(v["reused"] / v["known"], 3) if v["known"] >= MIN_N
                                              else f"insufficient({v['known']})")}
                      for k, v in sorted(skills.items())},
           "nodes": {f"{s}/{m}": _cell(v) for (s, m), v in sorted(node_dur.items())}}
    delta = {}
    by_shape = {}
    for (s, _m), v in node_dur.items():
        by_shape.setdefault(s, []).extend(v)
    for s, preset in sorted((presets or {}).items()):
        vals = sorted(by_shape.get(s, []))
        n = len(vals)
        cur = preset.get("timeout")
        if n < MIN_N:
            delta[s] = {"field": "timeout", "current": cur, "n": n, "observed_p95_s": f"insufficient({n})",
                        "delta_s": None}
        else:
            p95 = pct(vals, 95)
            delta[s] = {"field": "timeout", "current": cur, "n": n, "observed_p95_s": round(p95, 1),
                        "delta_s": round(p95 - cur, 1) if cur is not None else None}
        delta[s]["max_turns"] = {"current": preset.get("max_turns"), "observed": "insufficient(0)",
                                 "note": "node records carry no turn count; not measured"}
    out["presets_delta"] = delta
    return out


def render(rep):
    L = [f"BUDGET CALIBRATION (read-only; cells with n<{MIN_N} print insufficient(n))", ""]
    L.append("subagents by child_role (subagent_stop corpus):")
    if not rep["subagents"]:
        L.append("  (empty) need: telemetry: true in plugin settings and delegate_task children finishing")
    for r, c in rep["subagents"].items():
        d, t = c["duration_ms"], c["tool_count"]
        L.append(f"  {r:<20} n={d['n']:<5} duration_ms p50={d['p50']} p90={d['p90']}  tool_count p50={t['p50']} p90={t['p90']}")
    L += ["", "skills (on_skill_lifecycle corpus):"]
    if not rep["skills"]:
        L.append("  (empty)")
    for k, s in rep["skills"].items():
        L.append(f"  {k:<28} events={s['events']} known={s['known']} reuse_rate={s['reuse_rate']}")
    L += ["", "finished nodes by shape/model (seconds):"]
    if not rep["nodes"]:
        L.append("  (empty) need: finished runs under the runs root")
    for k, c in rep["nodes"].items():
        L.append(f"  {k:<40} n={c['n']:<5} p50={c['p50']} p90={c['p90']}")
    L += ["", "presets_delta (what the corpus would move; NOTHING is applied):"]
    for s, d in rep["presets_delta"].items():
        L.append(f"  {s:<8} timeout current={d['current']} observed_p95_s={d['observed_p95_s']} "
                 f"delta_s={d['delta_s']} n={d['n']}; max_turns {d['max_turns']['observed']}")
    return "\n".join(L) + "\n"


def report(runs_root=None):
    common = _load_common()
    root = Path(runs_root) if runs_root else common.runs_root()
    rep = collect(root, common.SHAPE_PRESETS, common.DEFAULT_SHAPE)
    rep["runs_root"] = str(root)
    return rep


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs-root")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    try:
        rep = report(a.runs_root)
    except Exception as e:   # read-only readout: degrade, never crash
        print(f"doctor budgets: cannot read corpus ({type(e).__name__}: {e})")
        return 0
    sys.stdout.write(json.dumps(rep, indent=1) + "\n" if a.json else render(rep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
