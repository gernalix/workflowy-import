"""Workflowy node and Calendar-day SQLite operations on a caller-owned connection.

No API, credential, notification, path discovery or connection ownership.
The caller retains its transaction and decides when to commit or roll back.
"""
from __future__ import annotations
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

def utc_z_from_epoch(epoch_seconds: Optional[int]) -> Optional[str]:
    if epoch_seconds is None:
        return None
    dt = datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def iter_nodes(payload: Any) -> Iterable[Dict[str, Any]]:
    # list-all is usually: {"items": [ ... ]}
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and "id" in item:
                yield item
        return

    if isinstance(payload, dict):
        for key in ("items", "nodes", "data", "list", "results"):
            val = payload.get(key)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and "id" in item:
                        yield item
                return

        # last resort
        for val in payload.values():
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and "id" in item:
                        yield item
                return

    raise ValueError("Unrecognized JSON structure: expected list of node dicts or dict containing such a list.")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS workflowy_nodes (
        id TEXT PRIMARY KEY,
        name TEXT,
        note TEXT,
        parent_id TEXT,
        layout_mode TEXT,

        created_at_epoch INTEGER,
        edited_at_epoch INTEGER,
        completed_at_epoch INTEGER,

        created_at TEXT,   -- UTC/Z
        edited_at TEXT,    -- UTC/Z
        completed_at TEXT, -- UTC/Z

        data_json TEXT,
        imported_at TEXT,  -- UTC/Z
        source_json TEXT
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workflowy_nodes_parent_id ON workflowy_nodes(parent_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workflowy_nodes_edited_at_epoch ON workflowy_nodes(edited_at_epoch);")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS workflowy_import_runs (
        run_id TEXT PRIMARY KEY,
        started_at TEXT,
        finished_at TEXT,
        status TEXT,
        items_total INTEGER,
        items_imported INTEGER,
        items_updated INTEGER,
        json_path TEXT,
        db_path TEXT,
        error TEXT
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS workflowy_days (
        date TEXT NOT NULL,
        node_id TEXT NOT NULL,
        imported_at TEXT NOT NULL,
        PRIMARY KEY(date, node_id)
    );
    """)


def replace_workflowy_days(
    conn: sqlite3.Connection,
    days: list[dict[str, str]],
    imported_at: str,
) -> None:
    """Replace the complete materialized-day index in the same DB transaction."""
    conn.execute("DELETE FROM workflowy_days")
    conn.executemany(
        "INSERT INTO workflowy_days(date,node_id,imported_at) VALUES (?,?,?)",
        [(item["date"], item["node_id"], imported_at) for item in days],
    )


def upsert_nodes(conn: sqlite3.Connection, payload: Dict[str, Any], source_json: str) -> tuple[int, int, int]:
    imported = 0
    updated = 0
    total = 0

    now_z = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    cur = conn.cursor()
    for node in iter_nodes(payload):
        total += 1
        node_id = node.get("id")
        name = node.get("name")
        note = node.get("note")
        parent_id = node.get("parent_id")

        data = node.get("data") or {}
        layout_mode = data.get("layoutMode") if isinstance(data, dict) else None

        created_epoch = node.get("createdAt")
        edited_epoch = node.get("modifiedAt")
        completed_epoch = node.get("completedAt")

        created_at = utc_z_from_epoch(created_epoch)
        edited_at = utc_z_from_epoch(edited_epoch)
        completed_at = utc_z_from_epoch(completed_epoch)

        data_json = json.dumps(data, ensure_ascii=False) if isinstance(data, (dict, list)) else None

        # ignore 'priority' entirely by design

        existing = cur.execute("SELECT edited_at_epoch FROM workflowy_nodes WHERE id = ?", (node_id,)).fetchone()
        if existing is None:
            cur.execute(
                """
                INSERT INTO workflowy_nodes(
                    id,name,note,parent_id,layout_mode,
                    created_at_epoch,edited_at_epoch,completed_at_epoch,
                    created_at,edited_at,completed_at,
                    data_json,imported_at,source_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    node_id, name, note, parent_id, layout_mode,
                    created_epoch, edited_epoch, completed_epoch,
                    created_at, edited_at, completed_at,
                    data_json, now_z, source_json
                ),
            )
            imported += 1
        else:
            old_edited = existing[0]
            if old_edited != edited_epoch:
                cur.execute(
                    """
                    UPDATE workflowy_nodes SET
                        name=?, note=?, parent_id=?, layout_mode=?,
                        created_at_epoch=?, edited_at_epoch=?, completed_at_epoch=?,
                        created_at=?, edited_at=?, completed_at=?,
                        data_json=?, imported_at=?, source_json=?
                    WHERE id=?
                    """,
                    (
                        name, note, parent_id, layout_mode,
                        created_epoch, edited_epoch, completed_epoch,
                        created_at, edited_at, completed_at,
                        data_json, now_z, source_json, node_id
                    ),
                )
                updated += 1

    return total, imported, updated
