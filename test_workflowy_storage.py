"""Offline persistence contracts; importing storage needs no Telegram runtime."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest

import workflowy_storage as storage


class WorkflowyStorageTests(unittest.TestCase):
    def test_independent_import_without_site_packages_or_runtime_adapters(self):
        script = """import sys
sys.path.insert(0, sys.argv[1])
import workflowy_storage
assert 'requests' not in sys.modules
assert 'telegram_notify' not in sys.modules
assert 'workflowy_job' not in sys.modules
"""
        subprocess.run([sys.executable, '-I', '-S', '-B', '-c', script,
                        str(Path(__file__).parent)], check=True, capture_output=True)

    def test_schema_and_upsert_preserve_existing_nodes_and_unchanged_revisions(self):
        with closing(sqlite3.connect(':memory:')) as conn:
            storage.ensure_schema(conn)
            first = {'id': 'fixture-a', 'name': 'original', 'modifiedAt': 100,
                     'createdAt': 0, 'data': {'layoutMode': 'bullets'}, 'priority': 99}
            self.assertEqual(storage.upsert_nodes(conn, {'items': [first]}, 'fixture.json'), (1, 1, 0))
            conn.commit()
            original = conn.execute('SELECT * FROM workflowy_nodes').fetchall()
            storage.ensure_schema(conn)
            self.assertEqual(conn.execute('SELECT * FROM workflowy_nodes').fetchall(), original)
            self.assertEqual(storage.upsert_nodes(conn, {'items': [dict(first, name='ignored same revision')]}, 'other.json'), (1, 0, 0))
            self.assertEqual(conn.execute('SELECT * FROM workflowy_nodes').fetchall(), original)
            self.assertEqual(storage.upsert_nodes(conn, {'items': [dict(first, name='updated', modifiedAt=101)]}, 'new.json'), (1, 0, 1))
            self.assertEqual(conn.execute('SELECT name,created_at,source_json FROM workflowy_nodes').fetchone(),
                             ('updated', '1970-01-01T00:00:00Z', 'new.json'))
            self.assertEqual(json.loads(conn.execute('SELECT data_json FROM workflowy_nodes').fetchone()[0]),
                             {'layoutMode': 'bullets'})
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_caller_can_roll_back_nodes_and_calendar_replacement_together(self):
        with closing(sqlite3.connect(':memory:')) as conn:
            storage.ensure_schema(conn)
            conn.execute("INSERT INTO workflowy_days VALUES('2026-01-01','old','baseline')")
            conn.commit()
            conn.execute('BEGIN')
            storage.upsert_nodes(conn, {'items': [{'id': 'new', 'modifiedAt': 1}]}, 'fixture')
            storage.replace_workflowy_days(conn, [{'date': '2026-01-02', 'node_id': 'new'}], 'candidate')
            self.assertTrue(conn.in_transaction)
            conn.rollback()
            self.assertEqual(conn.execute('SELECT * FROM workflowy_days').fetchall(), [('2026-01-01','old','baseline')])
            self.assertEqual(conn.execute('SELECT count(*) FROM workflowy_nodes').fetchone()[0], 0)

    def test_existing_payload_shapes_and_invalid_input_contract(self):
        node = {'id': 'fixture'}
        for payload in ([node], {'items': [node]}, {'nodes': [node]}, {'custom': [node]}):
            self.assertEqual(list(storage.iter_nodes(payload)), [node])
        with self.assertRaises(ValueError):
            list(storage.iter_nodes({'unexpected': 'scalar'}))


if __name__ == '__main__':
    unittest.main()
