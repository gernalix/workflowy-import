"""Failure contracts without importing or executing the runtime job adapter."""
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import traceback
import unittest

from workflowy_failure import FAILURE_MESSAGE, ImportFailure, record_import_failure
import workflowy_storage as storage


class WorkflowyFailureTests(unittest.TestCase):
    def connection(self):
        conn = sqlite3.connect(':memory:')
        storage.ensure_schema(conn)
        conn.execute("INSERT INTO workflowy_import_runs(run_id,started_at,status) VALUES('run','start','RUNNING')")
        storage.upsert_nodes(conn, {'items': [{'id': 'baseline', 'modifiedAt': 1}]}, 'baseline.json')
        conn.execute("INSERT INTO workflowy_days VALUES('2026-01-01','baseline','baseline')")
        conn.commit()
        return conn

    def test_failed_node_batch_preserves_committed_snapshot_and_records_error(self):
        with closing(self.connection()) as conn:
            before = conn.execute('SELECT * FROM workflowy_nodes').fetchall()
            with self.assertRaises(ValueError):
                storage.upsert_nodes(conn, {'items': [
                    {'id': 'baseline', 'name': 'pending edit', 'modifiedAt': 2},
                    {'id': 'new', 'modifiedAt': 1},
                    {'id': 'bad', 'modifiedAt': 'invalid'},
                ]}, 'candidate.json')
            self.assertTrue(conn.in_transaction)
            self.assertTrue(record_import_failure(conn, run_id='run', finished_at='finished'))
            self.assertFalse(conn.in_transaction)
            self.assertEqual(conn.execute('SELECT * FROM workflowy_nodes').fetchall(), before)
            self.assertEqual(conn.execute('SELECT * FROM workflowy_days').fetchall(),
                             [('2026-01-01', 'baseline', 'baseline')])
            self.assertEqual(conn.execute('SELECT status,error,finished_at FROM workflowy_import_runs').fetchone(),
                             ('ERROR', FAILURE_MESSAGE, 'finished'))

    def test_failed_calendar_replacement_rolls_back_delete_and_insert(self):
        with closing(self.connection()) as conn:
            with self.assertRaises(KeyError):
                storage.replace_workflowy_days(conn, [
                    {'date': '2026-01-02', 'node_id': 'candidate'},
                    {'date': '2026-01-03'},
                ], 'candidate')
            self.assertTrue(record_import_failure(conn, run_id='run', finished_at='finished'))
            self.assertEqual(conn.execute('SELECT * FROM workflowy_days').fetchall(),
                             [('2026-01-01', 'baseline', 'baseline')])

    def test_rejected_failure_receipt_cannot_commit_pending_node_changes(self):
        with closing(self.connection()) as conn:
            conn.execute("CREATE TRIGGER reject_error BEFORE UPDATE ON workflowy_import_runs BEGIN SELECT RAISE(ABORT,'SYNTHETIC_PRIVATE_TRIGGER'); END")
            conn.commit()
            storage.upsert_nodes(conn, {'items': [{'id': 'pending', 'modifiedAt': 1}]}, 'candidate')
            self.assertFalse(record_import_failure(conn, run_id='run', finished_at='finished'))
            self.assertFalse(conn.in_transaction)
            self.assertEqual(conn.execute('SELECT id FROM workflowy_nodes').fetchall(), [('baseline',)])
            self.assertEqual(conn.execute('SELECT status,error FROM workflowy_import_runs').fetchone(), ('RUNNING', None))

    def test_public_failure_suppresses_exception_messages_and_chained_context(self):
        class UnprintableError(Exception):
            def __str__(self):
                raise AssertionError('original error must not be formatted')
        for error in (ValueError('SYNTHETIC_PRIVATE_TOKEN\ncontinued'), UnprintableError()):
            try:
                try:
                    raise error
                except Exception:
                    raise ImportFailure() from None
            except ImportFailure as safe:
                rendered = ''.join(traceback.format_exception(type(safe), safe, safe.__traceback__))
                self.assertEqual(str(safe), FAILURE_MESSAGE)
                self.assertIn(FAILURE_MESSAGE, rendered)
                self.assertNotIn('SYNTHETIC_PRIVATE_TOKEN', rendered)
                self.assertNotIn('UnprintableError', rendered)

    def test_already_committed_data_is_preserved_on_later_failure(self):
        with closing(self.connection()) as conn:
            storage.upsert_nodes(conn, {'items': [{'id': 'committed', 'modifiedAt': 1}]}, 'committed')
            conn.commit()
            self.assertTrue(record_import_failure(conn, run_id='run', finished_at='finished'))
            self.assertEqual(conn.execute("SELECT count(*) FROM workflowy_nodes WHERE id='committed'").fetchone()[0], 1)

    def test_independent_import_without_runtime_dependencies(self):
        script = """import sys
sys.path.insert(0, sys.argv[1])
import workflowy_failure
assert not {'requests', 'telegram_notify', 'workflowy_job'} & sys.modules.keys()
"""
        subprocess.run([sys.executable, '-I', '-S', '-B', '-c', script,
                        str(Path(__file__).parent)], check=True, capture_output=True)


if __name__ == '__main__':
    unittest.main()
