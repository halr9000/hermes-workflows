#!/usr/bin/env python3
"""workflow-stream renderer tests: byte determinism, type-coded state-neutral
shapes, fixed typography across differently-sized graphs."""
import importlib.util
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "workflow_stream", ROOT / "scripts" / "workflow_stream.py")
ws = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ws)

GRAPH = {"name": "shape-test", "nodes": [
    {"id": "scan", "type": "agent", "goal": "scan"},
    {"id": "ok", "type": "gate", "after": ["scan"], "question": "Ship?",
     "options": ["ship", "hold"], "wait": {"wait_s": 0.01}},
    {"id": "act", "type": "agent", "after": ["ok"],
     "fanout": {"items": ["a", "b", "c"], "goal": "go {item}"}},
    {"id": "tally", "type": "join", "after": ["act"]},
    {"id": "note", "type": "echo", "after": ["tally"]},
]}
SMALL = {"name": "small", "nodes": [{"id": "only", "type": "agent"}]}


def test_same_graph_renders_identical_bytes():
    from PIL import Image
    first = ws.render_graph_png(GRAPH)
    second = ws.render_graph_png(GRAPH)
    assert first == second, "rendering must be byte-deterministic for a given graph"
    Image.open(io.BytesIO(first)).verify()


def test_type_coded_shapes_have_their_named_colors():
    from PIL import Image
    png = ws.render_graph_png(GRAPH)
    img = Image.open(io.BytesIO(png))
    for box in ws.layout_graph(GRAPH):
        style = ws.NODE_STYLE[box["type"]]
        # Sample inside the shape above the centered label (name text starts
        # ~15px below the top edge; the top strip is pure fill in every shape).
        px = img.getpixel((box["x"] + ws.NODE_W // 2, box["y"] + 8))
        assert px == style["fill"], (box["id"], px, style["fill"])


def test_fanout_badge_count_in_layout():
    boxes = {b["id"]: b for b in ws.layout_graph(GRAPH)}
    assert boxes["act"]["fanout_n"] == 3
    assert boxes["scan"]["fanout_n"] == 0
    bare = ws.layout_graph({"nodes": [{"id": "f", "type": "agent", "fanout": {"goal": "x"}}]})
    assert bare[0]["fanout_n"] == 1  # bare fan-out badge renders as a bare x


def test_fixed_typography_across_differently_sized_graphs():
    big = {"name": "big", "nodes": (
        [{"id": f"n{i}", "type": "agent", "goal": "g", "after": [f"n{i-1}"] if i else []}
         for i in range(12)])}
    small_boxes = ws.layout_graph(SMALL)
    big_boxes = ws.layout_graph(big)
    # The canvas grows; the type does not — every node label keeps the same px.
    assert {b["label_px"] for b in small_boxes} == {b["label_px"] for b in big_boxes} \
        == {ws.FONT_SIZES["name"]}
    assert ws.FONT_SIZES == {"name": 13, "type": 9, "badge": 11, "title": 15,
                             "kicker": 11}
    # Node box geometry is fixed even though the canvas is not.
    for boxes in (small_boxes, big_boxes):
        for b in boxes:
            assert ws.NODE_W == 150 and ws.NODE_H == 46


def test_unknown_node_type_falls_back_to_echo_gray():
    png_graph = {"nodes": [{"id": "odd", "type": "weird"}]}
    boxes = ws.layout_graph(png_graph)
    style = ws.NODE_STYLE.get(boxes[0]["type"], ws._FALLBACK_STYLE)
    assert style is ws.NODE_STYLE["echo"]


if __name__ == "__main__":
    tests = sorted((name, fn) for name, fn in globals().items()
                   if name.startswith("test_") and callable(fn))
    failures = 0
    for name, fn in tests:
        try:
            fn()
            print("PASS", name)
        except Exception as exc:
            failures += 1
            print("FAIL", name, type(exc).__name__, str(exc))
    print(f"TOTAL {len(tests)} FAIL {failures}")
    sys.exit(bool(failures))