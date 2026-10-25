#!/usr/bin/env python3
"""workflow-stream — a standalone Telegram mirror for Hermes workflow runs.

One persistent Telegram message per run: a static, type-coded topology
graphic sent once at launch, then a TEXT status message sent as its reply
and pinned. The text is the live surface — edited in place on
edge-triggered state changes, unpinned at the terminal state (the message
stays in chat history).

Send-only Telegram discipline: the daemon never calls getUpdates. That is
also why the stream bot must be a dedicated BotFather bot: sharing the main
gateway bot's token would make this process and the gateway's long poll
fight over update delivery (HTTP 409 conflicts) and break the primary bot.

Read model: wfcommon.run_state() over the run dir — one source of truth,
polled from the filesystem, diffed against the last-rendered state.

Crash safety: per-run stream bookkeeping (message ids, pin state, last
rendered text hash) is persisted as stream.json beside the run and
recovered from disk on restart.

Inspired by kanban-stream; shares no imports, code, or configuration with it.

Dependency policy: stdlib only at import time. When GRAPHIC=1 (the default),
Pillow and the vendored libre mono font (assets/fonts/LiberationMono-
Regular.ttf) are required — the daemon exits at startup with install
instructions rather than silently degrading to a text-only stream.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import wfcommon  # noqa: E402

ENV_PREFIX = "WORKFLOW_STREAM_"
FONT_PATH_REL = Path("assets/fonts/LiberationMono-Regular.ttf")
TERMINAL_STATUSES = ("done", "partial", "failed", "stopped")
STREAM_BOOK_FILE = "stream.json"  # per-run bookkeeping, persisted beside the run
STREAM_STATUS_FILE = "stream-status.json"  # runs-root failure beacon (last telegram error)
EDIT_MIN_GAP_S = 2.0              # per-message edit throttle floor (backoff raises it)
DEFAULT_POLL_S = 5.0


class MissingGraphicDependency(RuntimeError):
    """GRAPHIC=1 but Pillow or the vendored font is unavailable (hard fail)."""

    def __init__(self, instruction):
        super().__init__(instruction)
        self.instruction = instruction


class TelegramError(RuntimeError):
    def __init__(self, status, description, retry_after=None):
        super().__init__(f"{status}: {description}")
        self.status = status
        self.description = description or ""
        self.retry_after = retry_after


def env(name: str, default=None):
    return os.environ.get(ENV_PREFIX + name, default)


def load_hermes_env_file():
    """Load WORKFLOW_STREAM_* keys from the Hermes default env file
    (`<hermes_home>/.env`). Setup is then a single file edit — no shell
    sourcing dance. Keys already present in the environment win (explicit
    `WORKFLOW_STREAM_...=... python ...` still overrides). Deliberately
    prefix-scoped: this loader never touches unrelated configuration."""
    path = wfcommon.hermes_home() / ".env"
    try:
        text = path.read_text()
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        key, sep, value = line.partition("=")
        if not sep or not key.strip().startswith(ENV_PREFIX):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)


def _iter_env_file_lines(path: Path):
    """Yield (raw_line, key, value) for WORKFLOW_STREAM_* assignments in the
    env file — the same parse rules as load_hermes_env_file, without touching
    os.environ (used for live re-reads and in-place edits)."""
    try:
        text = path.read_text()
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        key, sep, value = line.partition("=")
        if not sep or not key.strip().startswith(ENV_PREFIX):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        yield raw, key.strip(), value


def read_env_file_watch(default: str = "all") -> str:
    """Re-read WORKFLOW_STREAM_WATCH straight from the env file, bypassing the
    os.environ setdefault cache, so a `/workflow-stream on|off` toggle takes
    effect on the daemon's next poll cycle — no restart."""
    path = wfcommon.hermes_home() / ".env"
    value = None
    for _raw, key, val in _iter_env_file_lines(path):
        if key == ENV_PREFIX + "WATCH":
            value = val  # last assignment wins, like dotenv semantics
    if value is None:
        return default
    return value.strip().lower() or default


def set_env_file_value(key: str, value: str) -> Path:
    """Set KEY=value in the Hermes env file (create if absent), replacing an
    existing assignment in place and preserving every other line. Scoped to
    WORKFLOW_STREAM_* keys only — this helper never writes unrelated config.
    Returns the path written."""
    if not key.startswith(ENV_PREFIX):
        raise ValueError(f"refusing to write non-{ENV_PREFIX} key {key!r}")
    path = wfcommon.hermes_home() / ".env"
    try:
        lines = path.read_text().splitlines()
    except OSError:
        lines = []
    prefix = "export " if any(
        raw.lstrip().startswith("export ") and raw.split("=", 1)[0].strip().endswith(key)
        for raw, k, _v in _iter_env_file_lines(path) if k == key) else ""
    entry = f"{prefix}{key}={value}"
    replaced = False
    for index, raw in enumerate(lines):
        stripped = raw.strip()
        if stripped.startswith("export "):
            stripped = stripped[len("export "):].strip()
        candidate = stripped.partition("=")[0].strip()
        if candidate == key:
            lines[index] = entry
            replaced = True
            break
    if not replaced:
        lines.append(entry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return path


# ---------------------------------------------------------------------------
# Launch graphic — Pillow-native raster (no SVG, no system rasterizer).
# ---------------------------------------------------------------------------

# Fixed typography: identical font sizes on every graph regardless of size —
# the canvas grows, the type does not. Values are final-scale pixels; drawing
# happens at 2x supersample, then a single LANCZOS downscale.
SCALE = 2
FONT_SIZES = {"name": 13, "type": 9, "badge": 11, "title": 15, "kicker": 11}
NODE_W, NODE_H = 150, 46
GAP_X, GAP_Y = 70, 26
PAD, TITLE_H = 36, 56
BG = (23, 26, 33)        # canvas (Telegram-dark friendly)
EDGE = (99, 110, 133)    # connectors
TITLE = (230, 234, 240)  # title text
SUBTLE = (128, 138, 155) # kicker / secondary text
# Type-coded, state-neutral: color and shape encode node TYPE only (the
# graphic is static — everything starts pending, so status colors would
# be meaningless there). Dark fill + bright border per mock v3.
NODE_STYLE = {
    "agent": {"border": (56, 199, 214), "label": (56, 199, 214),
              "fill": (24, 44, 50), "shape": "rounded"},   # cyan rounded rect
    "gate":  {"border": (240, 176, 70), "label": (240, 176, 70),
              "fill": (52, 44, 24), "shape": "diamond"},   # amber diamond
    "join":  {"border": (168, 122, 235), "label": (168, 122, 235),
              "fill": (40, 32, 54), "shape": "pill"},      # purple capsule
    "echo":  {"border": (128, 138, 155), "label": (180, 188, 200),
              "fill": (38, 42, 50), "shape": "rounded"},   # gray rounded rect
}
_FALLBACK_STYLE = NODE_STYLE["echo"]


def font_path(root=None) -> Path:
    base = Path(root) if root else ROOT
    path = base / FONT_PATH_REL
    if not path.is_file():
        raise MissingGraphicDependency(
            "workflow-stream: vendored font missing: "
            f"{path}\n  fix: restore assets/fonts/LiberationMono-Regular.ttf and "
            "assets/fonts/OFL-LICENSE.txt from the plugin source (no OS font "
            "package is involved — typography must stay byte-deterministic)")
    return path


def require_graphic_deps(root=None) -> None:
    """Hard dependency policy: GRAPHIC=1 with Pillow or the font missing is a
    startup failure with install instructions — never a silent text-only stream."""
    try:
        import PIL  # noqa: F401  (lazy: a GRAPHIC=0 install never needs it)
    except ImportError:
        raise MissingGraphicDependency(
            'workflow-stream: WORKFLOW_STREAM_GRAPHIC=1 but Pillow is not installed.\n'
            '  fix: python -m pip install "Pillow>=10.0"\n'
            '  (or set WORKFLOW_STREAM_GRAPHIC=0 to run without the launch graphic)')
    font_path(root)


def fanout_n(node) -> int:
    """Per-child count for the xN badge: items length when known, else 1 for a
    bare fan-out (badge renders as a bare x when the count is unknown)."""
    f = node.get("fanout")
    if isinstance(f, dict):
        items = f.get("items")
        if isinstance(items, list):
            return len(items)
        return 1
    return 0


def layout_graph(graph):
    """Pure, deterministic column/row layout: x by longest dependency depth,
    y by declaration order within a column. No rendering here."""
    nodes = [n for n in ((graph or {}).get("nodes") or [])
             if isinstance(n, dict) and n.get("id")]
    depth = {}
    pending = list(nodes)
    while pending:
        rest, progressed = [], False
        for n in pending:
            afters = [a for a in (n.get("after") or []) if a in depth]
            if all(a in depth for a in afters):
                depth[n["id"]] = 1 + max((depth[a] for a in afters), default=-1)
                progressed = True
            else:
                rest.append(n)
        if not progressed:  # cycle guard: pin leftovers after what exists
            for n in rest:
                depth.setdefault(n["id"], len(depth))
            rest = []
        pending = rest
    per_col = {}
    boxes = []
    for n in nodes:
        col = depth[n["id"]]
        row = per_col.get(col, 0)
        per_col[col] = row + 1
        boxes.append({"id": str(n["id"]), "type": str(n.get("type") or "echo"),
                      "fanout_n": fanout_n(n), "col": col, "row": row,
                      "label_px": FONT_SIZES["name"],
                      "x": PAD + col * (NODE_W + GAP_X),
                      "y": TITLE_H + PAD + row * (NODE_H + GAP_Y)})
    return boxes


def _canvas(boxes):
    if not boxes:
        return PAD * 2 + NODE_W, TITLE_H + PAD * 2 + NODE_H
    cols = max(b["col"] for b in boxes) + 1
    rows = max(sum(1 for b in boxes if b["col"] == c) for c in range(cols))
    return (PAD * 2 + cols * (NODE_W + GAP_X) - GAP_X,
            TITLE_H + PAD + rows * (NODE_H + GAP_Y) - GAP_Y + PAD)


_font_cache = {}


def _font(px: int):
    from PIL import ImageFont
    key = px * SCALE
    font = _font_cache.get(key)
    if font is None:
        font = ImageFont.truetype(str(font_path()), key)
        _font_cache[key] = font
    return font


def render_graph_png(graph, root=None) -> bytes:
    """Render the launch graphic per mock v3: dark fills with bright type-coded
    borders, name+type labels inside the shape, curved edges with arrowheads,
    WORKFLOW title, xN fan badges. 2x supersample, single LANCZOS downscale,
    byte-deterministic for a given graph (no wall-clock bytes anywhere)."""
    require_graphic_deps(root)
    import math
    from PIL import Image, ImageDraw
    boxes = layout_graph(graph)
    width, height = _canvas(boxes)
    img = Image.new("RGB", (width * SCALE, height * SCALE), BG)
    draw = ImageDraw.Draw(img)
    pos = {b["id"]: (b["x"], b["y"]) for b in boxes}

    def measure(text, px):
        b = draw.textbbox((0, 0), text, font=_font(px))
        return b[2] - b[0], b[3] - b[1]

    # ---- title: WORKFLOW kicker + graph name ----
    name = str((graph or {}).get("name") or "workflow")
    draw.text((PAD * SCALE, 20 * SCALE), "WORKFLOW",
              font=_font(FONT_SIZES["kicker"]), fill=SUBTLE)
    kw, _ = measure("WORKFLOW", FONT_SIZES["kicker"])
    draw.text(((PAD + kw + 10) * SCALE, 18 * SCALE), name,
              font=_font(FONT_SIZES["title"]), fill=TITLE)

    # ---- edges: cubic bezier right-mid -> left-mid, filled arrowhead ----
    def bezier(c):
        p0, p1, p2, p3 = c[:2], c[2:4], c[4:6], c[6:8]
        pts = []
        for i in range(25):
            t = i / 24
            x = ((1 - t) ** 3 * p0[0] + 3 * (1 - t) ** 2 * t * p1[0]
                 + 3 * (1 - t) * t ** 2 * p2[0] + t ** 3 * p3[0])
            y = ((1 - t) ** 3 * p0[1] + 3 * (1 - t) ** 2 * t * p1[1]
                 + 3 * (1 - t) * t ** 2 * p2[1] + t ** 3 * p3[1])
            pts.append((x * SCALE, y * SCALE))
        return pts

    for n in ((graph or {}).get("nodes") or []):
        at = pos.get(n.get("id"))
        if not at:
            continue
        for dep in (n.get("after") or []):
            src = pos.get(dep)
            if not src:
                continue
            p0 = (src[0] + NODE_W, src[1] + NODE_H // 2)
            p3 = (at[0], at[1] + NODE_H // 2)
            mid = (p0[0] + p3[0]) / 2
            curve = bezier((p0[0], p0[1], mid, p0[1], mid, p3[1], p3[0], p3[1]))
            draw.line(curve, fill=EDGE, width=2 * SCALE, joint="curve")
            ax, ay = curve[-1]
            bx, by = curve[-3]
            ang = math.atan2(ay - by, ax - bx)
            length, spread = 13 * SCALE, 0.45
            draw.polygon([(ax, ay),
                          (ax - length * math.cos(ang - spread),
                           ay - length * math.sin(ang - spread)),
                          (ax - length * math.cos(ang + spread),
                           ay - length * math.sin(ang + spread))], fill=EDGE)

    # ---- nodes: dark fill + bright border, labels inside ----
    for b in boxes:
        style = NODE_STYLE.get(b["type"], _FALLBACK_STYLE)
        border, label_color, fill = style["border"], style["label"], style["fill"]
        x, y = b["x"] * SCALE, b["y"] * SCALE
        w, h = NODE_W * SCALE, NODE_H * SCALE
        cx, cy = x + w // 2, y + h // 2
        if style["shape"] == "diamond":
            draw.polygon([(cx, y), (x + w, cy), (cx, y + h), (x, cy)],
                         fill=fill, outline=border, width=2 * SCALE)
        elif style["shape"] == "pill":
            draw.rounded_rectangle([x, y, x + w, y + h], radius=h // 2,
                                   fill=fill, outline=border, width=2 * SCALE)
        elif style["shape"] == "rounded":
            draw.rounded_rectangle([x, y, x + w, y + h], radius=8 * SCALE,
                                   fill=fill, outline=border, width=2 * SCALE)
        else:
            draw.rectangle([x, y, x + w, y + h], fill=fill, outline=border,
                           width=2 * SCALE)
        # name label inside the shape (truncate to fit), type tag under it
        text = b["id"]
        while measure(text, FONT_SIZES["name"])[0] > (NODE_W - 22) * SCALE \
                and len(text) > 3:
            text = text[:-1]
        tw, th = measure(text, FONT_SIZES["name"])
        draw.text((cx - tw / 2, cy - th / 2 - 2 * SCALE), text,
                  font=_font(FONT_SIZES["name"]), fill=label_color)
        tag = b["type"].upper()
        tw2, th2 = measure(tag, FONT_SIZES["type"])
        draw.text((cx - tw2 / 2, cy + th / 2 + SCALE), tag,
                  font=_font(FONT_SIZES["type"]),
                  fill=tuple(int(v * 0.75) for v in label_color))
        # fan badge: filled pill at the node's top-right
        if b["fanout_n"]:
            badge = f"×{b['fanout_n']}" if b["fanout_n"] > 1 else "×"
            bw, bh = measure(badge, FONT_SIZES["badge"])
            bx, by = b["x"] + NODE_W - 14, b["y"] - 12
            draw.rounded_rectangle(
                [bx * SCALE, by * SCALE, (bx + bw + 12) * SCALE, (by + bh + 8) * SCALE],
                radius=10 * SCALE, fill=fill, outline=border, width=SCALE)
            draw.text(((bx + 6) * SCALE, (by + 2) * SCALE), badge,
                      font=_font(FONT_SIZES["badge"]), fill=label_color)

    out = img.resize((width, height), Image.LANCZOS)
    buf = io.BytesIO()
    out.save(buf, format="PNG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Text status message (the live surface).
# ---------------------------------------------------------------------------

STATUS_EMOJI = {"pending": "hourglass", "running": "play", "done": "check", "partial": "half",
                "failed": "cross", "skipped": "next", "held": "pause", "interrupted": "warn",
                "stopped": "stop", "cancelled": "ban", "superseded": "return"}
_EMOJI = {"hourglass": "⏳", "play": "▶️", "check": "✅", "half": "◑️", "cross": "❌",
          "next": "⏭️", "pause": "⏸️", "warn": "⚠️", "stop": "⏹️", "ban": "🚫", "return": "↪️"}


def _parse_ts(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _fmt_dur(seconds):
    if seconds is None:
        return "?"
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _elapsed(state, now):
    started = _parse_ts(state.get("started"))
    if started is None:
        return None
    if started.tzinfo is None:
        started = started.astimezone()
    return now - started.timestamp()


def render_text(state, now=None) -> str:
    """Header + one numbered line per node in graph declaration order, held
    gate question/options, per-child fan-out progress, terminal summary."""
    now = time.time() if now is None else now
    status = str(state.get("status") or "pending")
    name = state.get("name") or state.get("run_id") or "?"
    done, total = state.get("done", 0), state.get("total", 0)
    held = state.get("held_gate") or {}
    held_id = held.get("id")
    lines = [f"🔀 {name} — {status} · {done}/{total} · ⏱ {_fmt_dur(_elapsed(state, now))}"]
    for i, (nid, node) in enumerate((state.get("nodes") or {}).items(), 1):
        node_status = node.get("status")
        emoji = _EMOJI.get(STATUS_EMOJI.get(node_status), "·")
        bits = [str(node.get("type") or "echo")]
        if nid == held_id:
            emoji = _EMOJI["pause"]
            bits.append("held — waiting on you")
        counts = node.get("item_counts")
        if isinstance(counts, dict) and counts:
            n = counts.get("total")
            if not isinstance(n, (int, float)):
                n = sum(v for k, v in counts.items()
                        if isinstance(v, (int, float)) and k != "total")
            settled = sum(counts.get(k, 0) or 0
                          for k in ("done", "partial", "failed", "cancelled"))
            bits.append(f"x{n} · {settled}/{n} settled")
        if node.get("seat_wait"):
            bits.append("waiting for a seat")
        line = f"[{i}] {emoji} {nid} — " + " · ".join(bits)
        refs = node.get("after") or []
        if refs:
            line += f" (→ {', '.join(str(r) for r in refs)})"
        lines.append(line)
    if held_id:
        lines.append(f"   ❓ {held.get('question') or '(no question)'}")
        options = held.get("options")
        if options:
            lines.append("   ⚙️ " + " / ".join(str(o) for o in options))
    for failed in state.get("failed_nodes") or []:
        reason = failed.get("error_class") or failed.get("error") or "failed"
        lines.append(f"   ❌ {failed.get('id')}: {reason}")
    if status in TERMINAL_STATUSES:
        extra = f" · {state.get('skipped')} skipped" if state.get("skipped") else ""
        lines.append(f"🏁 {status} — {done}/{total} settled{extra} · ⏱ {_fmt_dur(_elapsed(state, now))}")
    return "\n".join(lines)


def text_sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Bookkeeping (crash-safe, persisted beside the run).
# ---------------------------------------------------------------------------

def _book_path(run: Path) -> Path:
    return Path(run) / STREAM_BOOK_FILE


def load_book(run: Path) -> dict:
    try:
        data = json.loads(_book_path(run).read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_book(run: Path, book: dict) -> None:
    path = _book_path(run)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(book, indent=1, sort_keys=True))
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Transport seam — send-only by contract; never getUpdates.
# ---------------------------------------------------------------------------

class Transport:
    """Seam between the daemon and a chat backend (offline tests fake this)."""

    def send_photo(self, png: bytes, caption: str = "") -> int:
        raise NotImplementedError

    def send_text(self, text: str, reply_to: int | None = None) -> int:
        raise NotImplementedError

    def edit_text(self, message_id: int, text: str) -> None:
        raise NotImplementedError

    def pin(self, message_id: int) -> None:
        raise NotImplementedError

    def unpin(self, message_id: int) -> None:
        raise NotImplementedError


class TelegramTransport(Transport):
    """Telegram Bot API, send-only: sendMessage, sendPhoto, editMessageText,
    pinChatMessage, unpinChatMessage. NEVER getUpdates — this process must not
    compete with the gateway's long poll for update delivery."""

    def __init__(self, token: str, chat_id, api_base="https://api.telegram.org"):
        self.token = token
        self.chat_id = str(chat_id)
        self.api_base = api_base.rstrip("/")

    def _urlopen(self, req) -> dict:
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            description, retry_after = body[:300], None
            try:
                parsed = json.loads(body)
                description = str(parsed.get("description") or description)
                retry_after = (parsed.get("parameters") or {}).get("retry_after")
            except ValueError:
                pass
            raise TelegramError(exc.code, description, retry_after) from None
        if not isinstance(data, dict) or not data.get("ok"):
            raise TelegramError(500, str((data or {}).get("description") or "bad telegram response"))
        return data.get("result") or {}

    def _post(self, method: str, payload: dict) -> dict:
        url = f"{self.api_base}/bot{self.token}/{method}"
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                     headers={"Content-Type": "application/json"}, method="POST")
        return self._urlopen(req)

    def send_photo(self, png: bytes, caption: str = "") -> int:
        boundary = "wfstream" + uuid.uuid4().hex
        chunks = []
        fields = [("chat_id", self.chat_id)]
        if caption:
            fields.append(("caption", caption))
        for key, value in fields:
            chunks.append((f'--{boundary}\r\nContent-Disposition: form-data; '
                           f'name="{key}"\r\n\r\n{value}\r\n').encode("utf-8"))
        chunks.append((f'--{boundary}\r\nContent-Disposition: form-data; '
                       'name="photo"; filename="launch.png"\r\n'
                       'Content-Type: image/png\r\n\r\n').encode("utf-8"))
        chunks.append(png)
        chunks.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
        url = f"{self.api_base}/bot{self.token}/sendPhoto"
        req = urllib.request.Request(url, data=b"".join(chunks), method="POST",
                                     headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        return (self._urlopen(req) or {}).get("message_id")

    def send_text(self, text: str, reply_to: int | None = None) -> int:
        payload = {"chat_id": self.chat_id, "text": text}
        if reply_to is not None:
            payload["reply_to_message_id"] = int(reply_to)
        return (self._post("sendMessage", payload) or {}).get("message_id")

    def edit_text(self, message_id: int, text: str) -> None:
        self._post("editMessageText", {"chat_id": self.chat_id, "message_id": int(message_id),
                                       "text": text})

    def pin(self, message_id: int) -> None:
        self._post("pinChatMessage", {"chat_id": self.chat_id, "message_id": int(message_id)})

    def unpin(self, message_id: int) -> None:
        self._post("unpinChatMessage", {"chat_id": self.chat_id, "message_id": int(message_id)})


# ---------------------------------------------------------------------------
# Watcher — poll the filesystem, diff against last-rendered state.
# ---------------------------------------------------------------------------

def _is_deleted_message_error(err: TelegramError) -> bool:
    text = err.description.lower()
    return ("message to edit not found" in text
            or ("message" in text and "not found" in text and "chat not found" not in text))


class Watcher:
    """Polls run dirs via wfcommon.run_state and mirrors each into one message.

    WATCH filter applies at adoption time only: runs already adopted keep
    closing out even if the filter changes mid-flight (crash-recovery friendly)."""

    def __init__(self, transport: Transport, roots, *, graphic=True, watch="all", now=None):
        self.transport = transport
        self.roots = [Path(r) for r in roots]
        self.graphic = graphic
        self.watch = str(watch).lower()
        self.now = now or time.time
        self.books = {}
        self.fail_streak = 0
        self.fail_since = None
        self.status_path = self.roots[0] / STREAM_STATUS_FILE if self.roots else None

    def _wants(self, state) -> bool:
        if self.watch == "off":
            return False
        if self.watch == "telegram":
            return bool(((state.get("owner") or {}).get("platform")) == "telegram")
        return True

    def _note_failure(self, exc) -> None:
        """Record a Telegram-path failure: stderr line per occurrence plus a
        persistent runs-root beacon (`stream-status.json`) that survives the
        daemon and is surfaced by `stream_status_report()` / the
        `/workflow-stream status` command — a mid-run dead token is never
        silently swallowed."""
        now = datetime.fromtimestamp(self.now()).astimezone().isoformat()
        if self.fail_streak == 0:
            self.fail_since = now
            print("workflow-stream: telegram path degraded — beacon at "
                  f"{self.status_path}", file=sys.stderr)
        self.fail_streak += 1
        record = {"error": str(exc)[:300], "failures": self.fail_streak,
                  "since": self.fail_since, "last_at": now}
        if self.status_path is None:
            return
        try:
            self.status_path.parent.mkdir(parents=True, exist_ok=True)
            self.status_path.write_text(json.dumps(record, indent=1))
        except OSError:
            pass

    def _note_success(self) -> None:
        if self.fail_streak and self.status_path is not None:
            self.fail_streak = 0
            self.fail_since = None
            try:
                self.status_path.unlink(missing_ok=True)
            except OSError:
                pass

    def poll(self) -> dict:
        stats = {"seen": 0, "adopted": 0, "edited": 0, "closed": 0, "throttled": 0, "errors": 0}
        for run in wfcommon.iter_run_dirs(self.roots):
            try:
                state = wfcommon.run_state(run)
            except Exception as exc:  # one unreadable run must not stall the loop
                stats["errors"] += 1
                print(f"workflow-stream: run_state failed for {run.name}: {exc}", file=sys.stderr)
                continue
            if not state:
                continue
            stats["seen"] += 1
            try:
                self._handle(run, state, stats)
            except Exception as exc:
                stats["errors"] += 1
                self._note_failure(exc)
                print(f"workflow-stream: {run.name}: {exc}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
        if stats["errors"] == 0:
            self._note_success()
        return stats

    def _handle(self, run: Path, state: dict, stats: dict) -> None:
        name = state["run_id"]
        book = self.books.get(name) or load_book(run) or {}
        if book.get("closed"):
            return
        text = render_text(state, now=self.now())
        sha = text_sha(text)
        terminal = state.get("status") in TERMINAL_STATUSES
        if not book.get("text_message_id"):
            if not self._wants(state):
                return  # do not adopt a run the WATCH filter excludes
            book["adopted_at"] = datetime.fromtimestamp(
                self.now()).astimezone().isoformat()
            photo_id = None
            if self.graphic:
                photo_id = self.transport.send_photo(render_graph_png(state.get("graph")))
            text_id = self.transport.send_text(text, reply_to=photo_id)
            self.transport.pin(text_id)
            book.update(photo_message_id=photo_id, text_message_id=text_id,
                        pinned=True, last_text_sha=sha)
            save_book(run, book)
            self.books[name] = book
            stats["adopted"] += 1
        if terminal:
            if sha != book.get("last_text_sha"):
                self._edit(run, book, text, sha, stats)
            if book.get("pinned"):
                self.transport.unpin(book["text_message_id"])
                book["pinned"] = False
            book["closed"] = True
            save_book(run, book)
            stats["closed"] += 1
        elif sha != book.get("last_text_sha"):
            self._edit(run, book, text, sha, stats)

    def _edit(self, run: Path, book: dict, text: str, sha: str, stats: dict) -> None:
        now = self.now()
        if now < float(book.get("edit_ok_at") or 0):
            stats["throttled"] += 1  # edge stays pending: sha not updated, retried next poll
            return
        try:
            self.transport.edit_text(book["text_message_id"], text)
        except TelegramError as exc:
            if exc.retry_after:
                book["edit_ok_at"] = now + max(float(exc.retry_after), EDIT_MIN_GAP_S)
                save_book(run, book)
                stats["throttled"] += 1
                return
            if _is_deleted_message_error(exc):
                # Telegram reports the message gone: repost once (fresh photo
                # + text + pin). Any other failure is logged, not fatal.
                self._repost(run, book, text, sha, stats)
                return
            stats["errors"] += 1
            self._note_failure(exc)
            print(f"workflow-stream: {run.name}: edit failed: {exc}", file=sys.stderr)
            return
        book["last_text_sha"] = sha
        book["edit_ok_at"] = now + EDIT_MIN_GAP_S
        save_book(run, book)
        stats["edited"] += 1

    def _repost(self, run: Path, book: dict, text: str, sha: str, stats: dict) -> None:
        photo_id = None
        if self.graphic:
            photo_id = self.transport.send_photo(render_graph_png(
                wfcommon.run_state(run).get("graph")))
        text_id = self.transport.send_text(text, reply_to=photo_id)
        self.transport.pin(text_id)
        book.update(photo_message_id=photo_id, text_message_id=text_id,
                    pinned=True, last_text_sha=sha)
        save_book(run, book)
        stats["edited"] += 1


# ---------------------------------------------------------------------------
# Daemon entry point.
# ---------------------------------------------------------------------------

def _config():
    problems = []
    token = (env("BOT_TOKEN") or "").strip()
    chat = (env("CHAT_ID") or "").strip()
    if not token:
        problems.append("WORKFLOW_STREAM_BOT_TOKEN is required — a DEDICATED BotFather "
                        "bot token, never the gateway bot's token (see docs/workflow-stream.md "
                        "for why: getUpdates/long-poll conflicts would break the primary bot)")
    if not chat:
        problems.append("WORKFLOW_STREAM_CHAT_ID is required (destination chat id) — "
                        "message the bot once from that chat, then run --discover-chat "
                        "to print it (or DM @userinfobot: a 1:1 chat id equals your user id)")
    watch = (env("WATCH") or "all").strip().lower()
    if watch not in ("all", "telegram", "off"):
        problems.append(f"WORKFLOW_STREAM_WATCH must be all|telegram|off, got {watch!r}")
    graphic_raw = (env("GRAPHIC") or "1").strip()
    if graphic_raw not in ("0", "1"):
        problems.append(f"WORKFLOW_STREAM_GRAPHIC must be 0 or 1, got {graphic_raw!r}")
    try:
        poll_s = float((env("POLL_S") or DEFAULT_POLL_S))
    except ValueError:
        problems.append("WORKFLOW_STREAM_POLL_S must be a number of seconds")
        poll_s = DEFAULT_POLL_S
    poll_s = min(max(poll_s, 1.0), 600.0)
    return token, chat, watch, graphic_raw == "1", poll_s, problems


def run_check(token: str, chat: str, watch: str, graphic: bool, poll_s: float):
    """Pre-flight validation, shared by --check and normal startup. Verifies
    the Bot API credentials with getMe/getChat — both are send-side calls
    that consume NO updates, so send-only discipline holds. Catches a dead
    or mistyped token at startup (clean exit 2) instead of a bare 404 from
    the first sendPhoto mid-run. Returns (ok_lines, problems)."""
    ok_lines, problems = [], []
    if graphic:
        try:
            require_graphic_deps()
        except MissingGraphicDependency as exc:
            problems.append(exc.instruction)
    if watch == "off":
        return ok_lines, problems  # feature off: the daemon just idles, no credentials needed
    if not token:
        return ok_lines, problems  # presence problems already listed by _config()
    transport = TelegramTransport(token, chat)
    try:
        me = transport._post("getMe", {})
        ok_lines.append(f"bot verified: @{me.get('username', '?')} (id {me.get('id', '?')})")
    except TelegramError as exc:
        problems.append(f"BOT_TOKEN rejected by Telegram ({exc}) — is it the token of a "
                        "DEDICATED BotFather bot, complete and unrevoked? (a stale token "
                        "surfaces as HTTP 404 Not Found here)")
        return ok_lines, problems
    if chat:
        try:
            info = transport._post("getChat", {"chat_id": chat})
            ok_lines.append(f"chat verified: {info.get('title') or info.get('username') or chat} "
                            f"(type {info.get('type', '?')})")
        except TelegramError as exc:
            problems.append(f"CHAT_ID rejected by Telegram ({exc}) — message the bot once, then retry")
    return ok_lines, problems


def discover_chats(token: str, api_base="https://api.telegram.org"):
    """Setup-only chat-id discovery: getMe + ONE short getUpdates poll, then
    print every chat seen with the exact env line to append. The LIVE daemon
    path never calls getUpdates — send-only discipline is a runtime policy;
    a human setup step may consume updates once against the dedicated bot
    (it has no other update consumer, so there is no conflict to cause).
    Returns (lines, problems)."""
    transport = TelegramTransport(token, "", api_base=api_base)
    try:
        me = transport._post("getMe", {})
    except TelegramError as exc:
        return [], [f"BOT_TOKEN rejected by Telegram ({exc}) — is it the token of a "
                    "DEDICATED BotFather bot, complete and unrevoked?"]
    lines = [f"bot verified: @{me.get('username', '?')} (id {me.get('id', '?')})"]
    try:
        result = transport._post("getUpdates", {"timeout": 0, "limit": 100})
    except TelegramError as exc:
        return lines, [f"getUpdates failed ({exc}) — is another client long-polling this bot?"]
    chats: dict = {}
    for update in result if isinstance(result, list) else []:
        for key in ("message", "edited_message", "channel_post",
                    "my_chat_member", "chat_member", "callback_query"):
            payload = update.get(key) if isinstance(update, dict) else None
            if key == "callback_query":
                message = (payload or {}).get("message") or {}
                payload = {"chat": message.get("chat")}
            chat = (payload or {}).get("chat") if isinstance(payload, dict) else None
            if isinstance(chat, dict) and "id" in chat:
                chats[chat["id"]] = chat  # dict keeps insertion order; last wins below
    if not chats:
        problems = ["no messages seen yet — send ANY message to this bot from the "
                    "destination chat (start with /start), then run --discover-chat again"]
        return lines, problems
    for chat in chats.values():
        who = (chat.get("title") or ("@" + chat["username"] if chat.get("username")
                                     else chat.get("first_name")) or "")
        lines.append(f"chat {chat['id']} — type {chat.get('type', '?')}"
                     + (f" ({who})" if who else ""))
    suggested = list(chats.values())[-1]
    lines.append("append to the Hermes env file (~/.hermes/.env):")
    lines.append(f"  WORKFLOW_STREAM_CHAT_ID={suggested['id']}")
    lines.append("(1:1 chat? its id equals your Telegram user id — DM @userinfobot to cross-check)")
    return lines, []


def _daemon_pids():
    """PIDs of live workflow_stream.py daemon processes (Linux /proc scan;
    empty elsewhere). Best-effort — used only for the status report."""
    pids = []
    try:
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                cmdline = (entry / "cmdline").read_bytes().split(b"\x00")
            except OSError:
                continue
            if any(part.endswith(b"workflow_stream.py") for part in cmdline):
                pids.append(int(entry.name))
    except OSError:
        return []
    return sorted(pids)


def status_beacon() -> dict:
    """Read the runs-root failure beacon (last telegram-path error), if any."""
    path = wfcommon.runs_root() / STREAM_STATUS_FILE
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def stream_status_report() -> dict:
    """Local truth for the /workflow-stream slash command — config presence,
    watch mode, dependency state, daemon liveness, and the failure beacon.
    Deliberately NO network (slash handlers must answer fast); the live
    credential check stays with `--check` / `stream_check` / `/workflow-stream check`."""
    load_hermes_env_file()
    deps_ok, deps_msg = True, ""
    try:
        require_graphic_deps()
    except MissingGraphicDependency as exc:
        deps_ok, deps_msg = False, exc.instruction
    return {
        "token_set": bool((env("BOT_TOKEN") or "").strip()),
        "chat_set": bool((env("CHAT_ID") or "").strip()),
        # WATCH is file truth: the loader's os.environ setdefault would cache a
        # pre-toggle value here and misreport the live daemon's next mode.
        "watch": read_env_file_watch("all"),
        "graphic_deps_ok": deps_ok,
        "graphic_deps_msg": deps_msg,
        "daemon_pids": _daemon_pids(),
        "beacon": status_beacon(),
        "daemon_script": str(Path(__file__).resolve()),
    }


# ---------------------------------------------------------------------------
# Pinned micro-readme — the stream's own one-card system alert, sent once
# ---------------------------------------------------------------------------
README_VERSION = 3  # bump to re-send the card after doc/config changes
README_DOCS_URL = "https://hermes-agent.nousresearch.com/docs/plugins/hermes-workflows"
README_TEXT = (
    "🛰 workflow-stream is live\n"
    "Every workflow run posts ONE message pair here: a static topology graphic "
    "+ a pinned text status, edited as the run progresses, unpinned at the end "
    "(history kept).\n"
    "Send-only bot — replies here are never read; steer runs in your Hermes chat.\n"
    "Control it in any Hermes session: /workflow-stream status | on | off (live toggle).\n"
    "Tune: WORKFLOW_STREAM_WATCH=all|telegram|off · GRAPHIC=0|1 · POLL_S=<sec>\n"
    f"Docs: {README_DOCS_URL}"
)
STREAM_README_MARKER = "stream-readme.json"  # beside the runs root, version-keyed


def send_readme_once(transport, marker: Path) -> None:
    """Pin a one-card micro-readme into the stream chat on the first live
    start (what the stream does, how to tune it, docs pointer). Marker file
    beside the runs root is version-keyed: bumping README_VERSION re-sends
    after a doc/config change. Best-effort — a failed pin never blocks the
    daemon from mirroring runs."""
    try:
        done = json.loads(marker.read_text()) if marker.exists() else {}
    except (OSError, ValueError):
        done = {}
    if done.get("version", 0) >= README_VERSION:
        return
    try:
        msg_id = transport.send_text(README_TEXT)
        transport.pin(msg_id)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps({"version": README_VERSION, "message_id": msg_id}))
    except (TelegramError, OSError) as exc:
        print(f"workflow-stream: readme pin failed (continuing): {exc}", file=sys.stderr)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow-stream",
        description="Mirror Hermes workflow runs into one persistent Telegram "
                    "message per run (static launch graphic + live text status).")
    parser.add_argument("--once", action="store_true",
                        help="run a single poll cycle and exit (smoke tests)")
    parser.add_argument("--check", action="store_true",
                        help="validate config, deps and Telegram credentials, then exit "
                             "(getMe/getChat only — no updates consumed)")
    parser.add_argument("--discover-chat", action="store_true",
                        help="setup-only chat-id discovery: message the bot from the "
                             "destination chat first, then run this — it does ONE short "
                             "getUpdates poll (never in live mode) and prints the exact "
                             "WORKFLOW_STREAM_CHAT_ID line to append")
    args = parser.parse_args(argv)
    load_hermes_env_file()
    if args.discover_chat:
        token = (env("BOT_TOKEN") or "").strip()
        if not token:
            print("workflow-stream: WORKFLOW_STREAM_BOT_TOKEN is not set — create a "
                  "DEDICATED bot with @BotFather, append its token to ~/.hermes/.env, "
                  "then re-run --discover-chat", file=sys.stderr)
            return 2
        lines, problems = discover_chats(token)
        for line in lines:
            print(f"workflow-stream: {line}")
        for problem in problems:
            print(f"workflow-stream: {problem}", file=sys.stderr)
        return 2 if problems else 0
    # WATCH live-toggle: an explicit process-env value pins the mode for this
    # process; otherwise the env file is re-read every poll cycle so
    # /workflow-stream on|off takes effect without a restart.
    shell_watch = os.environ.get(ENV_PREFIX + "WATCH")
    token, chat, watch, graphic, poll_s, problems = _config()
    ok_lines, cred_problems = run_check(token, chat, watch, graphic, poll_s)
    problems = problems + cred_problems
    if problems:
        for problem in problems:
            print(f"workflow-stream: {problem}", file=sys.stderr)
        return 2
    if args.check:
        print(f"workflow-stream: watch={watch} graphic={int(graphic)} poll_s={poll_s:g}")
        for line in ok_lines:
            print(f"workflow-stream: {line}")
        print("workflow-stream: OK")
        return 0
    watcher_transport = TelegramTransport(token, chat)
    if watch != "off":
        send_readme_once(watcher_transport, wfcommon.runs_root() / STREAM_README_MARKER)
    watcher = Watcher(watcher_transport, roots=[wfcommon.runs_root()],
                      graphic=graphic, watch=watch)
    try:
        while True:
            if shell_watch is None:
                watcher.watch = read_env_file_watch(watcher.watch)
            watcher.poll()
            if args.once:
                return 0
            time.sleep(poll_s)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())