"""Offline-safe failure handling for the job-owned import transaction.

Exception messages, causes and tracebacks can contain credentials or node data.
Public diagnostics deliberately use a fixed message instead.
"""

FAILURE_MESSAGE = "Workflowy import failed; sensitive diagnostics omitted."


class ImportFailure(RuntimeError):
    def __init__(self) -> None:
        super().__init__(FAILURE_MESSAGE)


def record_import_failure(conn, *, run_id: str, finished_at: str) -> bool:
    """Discard pending import writes, then record failure of a committed run.

The job owns this transaction; storage helpers continue to leave transaction
control to their caller. Already committed data/files cannot be rolled back.
"""
    try:
        conn.rollback()
        conn.execute(
            """UPDATE workflowy_import_runs
               SET finished_at=?, status=?, error=? WHERE run_id=?""",
            (finished_at, "ERROR", FAILURE_MESSAGE, run_id),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
