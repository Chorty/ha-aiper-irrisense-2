"""Normalize the app's MQTT response envelopes without transport side effects."""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any


def iter_upchan_commands(
    message: Mapping[str, Any],
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield command bodies from wrapped or command-keyed upChan frames.

    Android CmdFactory.decrypt excludes ``res`` and ``chksum`` from keyed
    command selection. Transport metadata must not become a command either.
    Only object bodies are consumed by the coordinator; malformed scalar or
    list values cannot acknowledge a command or overwrite valid state.
    """
    command = message.get("type")
    if isinstance(command, str) and command:
        body = message.get("data")
        if isinstance(body, dict):
            yield command.removeprefix("up_"), body
        return

    for command, body in message.items():
        if (
            not isinstance(command, str)
            or not command
            or command.startswith("_")
            or command in {"res", "chksum", "type", "data"}
            or not isinstance(body, dict)
        ):
            continue
        yield command.removeprefix("up_"), body
