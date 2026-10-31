"""Pick device-reported fields out of the cached MQTT frames without transport side effects.

Status fields (``status``, ``valve``, ``rotate``, ``waterpress``, ``hydropenia``) arrive in upChan status
frames; the freshest frame carrying a field wins. Section payloads (``NetStat``, ``OpInfo``, ``AlarmReport`` /
``Alarm``) arrive in AWS shadow documents, the cloud report topic or keyed upChan frames, at varying depths.
Values are passed through unconverted: ``valve`` and ``waterpress`` are raw device units with no known scale.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# upChan commands whose bodies carry live device status (see coordinator._async_handle_mqtt_message).
STATUS_SOURCES = (
    "workInfo",
    "workInfoReport",
    "realtimeStatus",
    "realtimeStatusReport",
    "realTimeProgress",
    "realTimeProgressReport",
)
# Section key -> payload names it may appear under.
SECTIONS = {
    "net_stat": ("NetStat",),
    "op_info": ("OpInfo",),
    "alarm": ("AlarmReport", "Alarm"),
}
_SKIP_KEYS = {"previous", "metadata"}      # stale shadow state / per-field timestamps, not values
_MAX_DEPTH = 6


def _is_time(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def latest_field(mqtt_slot: Mapping[str, Any], key: str) -> tuple[Any, float | None]:
    """Return ``(value, arrival time)`` of ``key`` from the freshest status frame carrying it."""
    best: tuple[Any, float | None] = (None, None)
    for command in STATUS_SOURCES:
        frame = mqtt_slot.get("up_" + command)
        if not isinstance(frame, Mapping):
            continue
        body, ts = frame.get("data"), frame.get("_ts")
        if not isinstance(body, Mapping) or key not in body or not _is_time(ts):
            continue
        if best[1] is None or ts > best[1]:
            best = (body[key], ts)
    return best


def heading_degrees(rotate: Any) -> float | None:
    """Device ``rotate`` (centi-degrees, 0..35999) as signed degrees, clockwise positive."""
    if not _is_time(rotate):
        return None
    degrees = rotate / 100
    if degrees > 180:
        degrees -= 360
    return round(degrees, 2)


def as_flag(value: Any) -> bool | None:
    """Interpret a 0/1-style device flag; None when absent or not recognisably boolean."""
    if isinstance(value, bool):
        return value
    if _is_time(value) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in ("0", "1", "true", "false"):
        return value.strip().lower() in ("1", "true")
    return None


def extract_sections(message: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Find NetStat / OpInfo / alarm payloads anywhere in one message (shallowest match wins)."""
    names = {name: key for key, aliases in SECTIONS.items() for name in aliases}
    found: dict[str, dict[str, Any]] = {}
    command = message.get("type")
    if isinstance(command, str) and isinstance(message.get("data"), Mapping):
        key = names.get(command.removeprefix("up_"))
        if key:
            found[key] = dict(message["data"])
    level = [message]
    for _ in range(_MAX_DEPTH):
        following = []
        for node in level:
            for name, value in node.items():
                if not isinstance(name, str) or name in _SKIP_KEYS or not isinstance(value, Mapping):
                    continue
                key = names.get(name)
                if key and key not in found:
                    found[key] = dict(value)
                else:
                    following.append(value)
        level = following
    return found


def raw_snapshot(mqtt_slot: Mapping[str, Any]) -> tuple[float | None, dict[str, Any]]:
    """Latest frame per topic bucket (without duplicated raw copies) and the newest arrival time."""
    frames: dict[str, Any] = {}
    newest: float | None = None
    for bucket, frame in mqtt_slot.items():
        if bucket in ("other", "sections") or not isinstance(frame, Mapping):
            continue
        frames[bucket] = {k: v for k, v in frame.items() if k not in ("_raw", "_sn")}
        ts = frame.get("_ts")
        if _is_time(ts) and (newest is None or ts > newest):
            newest = ts
    return newest, frames
