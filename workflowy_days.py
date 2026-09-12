# v2
"""Extract a compact, deterministic index of materialized Workflowy Calendar day nodes."""

from __future__ import annotations

import calendar
import json
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

_TIME_TAG = re.compile(r"^\s*<time\b(?P<attrs>[^>]*)>.*?</time>\s*$", re.IGNORECASE | re.DOTALL)
_TIME_ATTR = re.compile(
    r"""\b(startYear|startMonth|startDay)\s*=\s*["'](\d{1,4})["']""",
    re.IGNORECASE,
)
_UUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _parse_exact_day_name(name: Any) -> date | None:
    if not isinstance(name, str):
        return None
    match = _TIME_TAG.fullmatch(name)
    if not match:
        return None
    attrs = {key.lower(): int(value) for key, value in _TIME_ATTR.findall(match.group("attrs"))}
    if set(attrs) != {"startyear", "startmonth", "startday"}:
        return None
    try:
        return date(attrs["startyear"], attrs["startmonth"], attrs["startday"])
    except ValueError:
        return None


def _clean_name(node: Mapping[str, Any] | None) -> str:
    return str((node or {}).get("name") or "").strip()


def _is_calendar_day(
    day_node: Mapping[str, Any],
    day: date,
    nodes_by_id: Mapping[str, Mapping[str, Any]],
) -> bool:
    month_node = nodes_by_id.get(str(day_node.get("parent_id") or ""))
    if month_node is None or _clean_name(month_node) != calendar.month_abbr[day.month]:
        return False

    year_node = nodes_by_id.get(str(month_node.get("parent_id") or ""))
    if year_node is None or _clean_name(year_node) != str(day.year):
        return False

    calendar_root = nodes_by_id.get(str(year_node.get("parent_id") or ""))
    return calendar_root is not None and _clean_name(calendar_root) == "📆 Calendar"


def extract_workflowy_days(nodes: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Return sorted {date,node_id} records for real Calendar day nodes only.

    Workflowy can contain more than one materialized Calendar node for the same date. Those
    nodes are preserved instead of choosing one heuristically; consumers can decide how to
    present an ambiguous day.
    """
    materialized = [node for node in nodes if isinstance(node, Mapping)]
    nodes_by_id = {
        str(node["id"]): node
        for node in materialized
        if node.get("id") is not None
    }

    by_date: dict[str, set[str]] = {}
    for node in materialized:
        parsed = _parse_exact_day_name(node.get("name"))
        if parsed is None or not _is_calendar_day(node, parsed, nodes_by_id):
            continue

        node_id = str(node.get("id") or "")
        if not _UUID.fullmatch(node_id):
            raise ValueError(f"Calendar day {parsed.isoformat()} has invalid Workflowy node id: {node_id!r}")
        by_date.setdefault(parsed.isoformat(), set()).add(node_id)

    return [
        {"date": iso, "node_id": node_id}
        for iso in sorted(by_date)
        for node_id in sorted(by_date[iso])
    ]


def make_feed(
    days: Sequence[Mapping[str, str]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "schema_version": 1,
        "generated_at": generated,
        "workflowy_days": [
            {"date": str(item["date"]), "node_id": str(item["node_id"])}
            for item in days
        ],
    }


def write_feed_atomic(
    days: Sequence[Mapping[str, str]],
    output_path: Path,
    generated_at: str | None = None,
) -> None:
    """Atomically replace the compact feed without ever exposing a partial file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = make_feed(days, generated_at=generated_at)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=str(output_path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, output_path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
