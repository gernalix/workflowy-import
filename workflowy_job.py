# v3
"""workflowy_job.py

Oracle job: fetch Workflowy list-all JSON via API key, write JSON backup,
import into SQLite workflowy.db for Datasette, and publish a compact Calendar-day feed.

Requirements:
- python3-requests (or requests installed)
- telegram_notify.py available in same folder (copied by patch)

Env (via /etc/workflowy_import/workflowy_import.env):
- WORKFLOWY_API_KEY: Workflowy API key (https://workflowy.com/api-key)
- WORKFLOWY_API_URL: optional, default https://beta.workflowy.com/api/beta/list-all/
- WORKFLOWY_JSON_DIR: optional, default /home/ubuntu/imports/workflowy_backups
- WORKFLOWY_JSON_LATEST: optional, default /home/ubuntu/imports/workflowy_list_all.json
- WORKFLOWY_DB_PATH: optional, default /home/ubuntu/db/workflowy.db
- WORKFLOWY_DAYS_OUTPUT: optional, default /home/ubuntu/imports/workflowy_days.json
- WORKFLOWY_NOTIFY_PREFIX: optional, default workflowy-import
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

try:
    import requests
except Exception as e:  # pragma: no cover
    print(f"[ERROR] Missing dependency 'requests': {e}")
    sys.exit(10)

import telegram_notify
from workflowy_days import extract_workflowy_days, write_feed_atomic


DEFAULT_API_URL = "https://beta.workflowy.com/api/beta/list-all/"
DEFAULT_JSON_DIR = "/home/ubuntu/imports/workflowy_backups"
DEFAULT_JSON_LATEST = "/home/ubuntu/imports/workflowy_list_all.json"
DEFAULT_DB_PATH = "/home/ubuntu/db/workflowy.db"
DEFAULT_DAYS_OUTPUT = "/home/ubuntu/imports/workflowy_days.json"
DEFAULT_PREFIX = "workflowy-import"


def notify_best_effort(message: str, *, prefix: str) -> None:
    try:
        telegram_notify.notify(message, prefix=prefix)
    except Exception as error:
        print(f"[WARN] Telegram notification failed: {type(error).__name__}", file=sys.stderr)


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


def fetch_workflowy_json(api_key: str, api_url: str, timeout_s: int = 300) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    r = requests.get(api_url, headers=headers, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"Workflowy API HTTP {r.status_code}: {r.text[:500]}")
    return r.json()


def write_json_files(payload: Dict[str, Any], json_dir: Path, latest_path: Path) -> Path:
    json_dir.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    out = json_dir / f"workflowy-list-all-{ts}.json"

    tmp = out.with_suffix(out.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    tmp.replace(out)

    # also write/overwrite latest (atomic)
    tmp2 = latest_path.with_suffix(latest_path.suffix + ".tmp")
    with tmp2.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    tmp2.replace(latest_path)

    return out


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


def main() -> int:
    api_key = os.getenv("WORKFLOWY_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("WORKFLOWY_API_KEY missing (set it in /etc/workflowy_import/workflowy_import.env)")

    api_url = os.getenv("WORKFLOWY_API_URL", DEFAULT_API_URL).strip() or DEFAULT_API_URL
    json_dir = Path(os.getenv("WORKFLOWY_JSON_DIR", DEFAULT_JSON_DIR).strip() or DEFAULT_JSON_DIR)
    latest_path = Path(os.getenv("WORKFLOWY_JSON_LATEST", DEFAULT_JSON_LATEST).strip() or DEFAULT_JSON_LATEST)
    db_path = Path(os.getenv("WORKFLOWY_DB_PATH", DEFAULT_DB_PATH).strip() or DEFAULT_DB_PATH)
    days_output = Path(os.getenv("WORKFLOWY_DAYS_OUTPUT", DEFAULT_DAYS_OUTPUT).strip() or DEFAULT_DAYS_OUTPUT)
    prefix = os.getenv("WORKFLOWY_NOTIFY_PREFIX", DEFAULT_PREFIX).strip() or DEFAULT_PREFIX

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    ensure_schema(conn)
    conn.commit()

    conn.execute(
        "INSERT OR REPLACE INTO workflowy_import_runs(run_id, started_at, status, json_path, db_path) VALUES (?,?,?,?,?)",
        (run_id, started_at, "RUNNING", None, str(db_path)),
    )
    conn.commit()

    try:
        payload = fetch_workflowy_json(api_key=api_key, api_url=api_url)
        json_file = write_json_files(payload, json_dir=json_dir, latest_path=latest_path)

        days = extract_workflowy_days(iter_nodes(payload))
        imported_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        total, imported, updated = upsert_nodes(conn, payload, source_json=str(json_file))
        replace_workflowy_days(conn, days, imported_at=imported_at)
        conn.commit()

        write_feed_atomic(days, days_output, generated_at=imported_at)

        finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.execute(
            """
            UPDATE workflowy_import_runs
            SET finished_at=?, status=?, items_total=?, items_imported=?, items_updated=?, json_path=?
            WHERE run_id=?
            """,
            (finished_at, "OK", total, imported, updated, str(json_file), run_id),
        )
        conn.commit()

        notify_best_effort(
            f"✅ Import OK\nDB: {db_path}\nJSON: {json_file}\n"
            f"Days: {len(days)} ({days_output})\n"
            f"Items: {total} (new {imported}, upd {updated})",
            prefix=prefix,
        )
        return 0

    except Exception as e:
        finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        err = "".join(traceback.format_exception(type(e), e, e.__traceback__))[-3500:]
        try:
            conn.execute(
                """
                UPDATE workflowy_import_runs
                SET finished_at=?, status=?, error=?
                WHERE run_id=?
                """,
                (finished_at, "ERROR", err, run_id),
            )
            conn.commit()
        except Exception:
            pass

        notify_best_effort(
            f"❌ Import FAILED\nDB: {db_path}\nError: {str(e)}\n\n{err}",
            prefix=prefix,
        )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
