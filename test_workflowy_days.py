# v2
import json
import tempfile
import unittest
from pathlib import Path

from workflowy_days import extract_workflowy_days, write_feed_atomic


def node(node_id, name, parent_id=None):
    return {"id": node_id, "name": name, "parent_id": parent_id}


ROOT = "00000000-0000-0000-0000-000000000001"
YEAR = "00000000-0000-0000-0000-000000000002"
SEP = "00000000-0000-0000-0000-000000000003"


class WorkflowyDaysTest(unittest.TestCase):
    def base(self):
        return [
            node(ROOT, "📆 Calendar"),
            node(YEAR, "2026", ROOT),
            node(SEP, "Sep", YEAR),
        ]

    def test_extracts_only_exact_calendar_days_and_sorts(self):
        nodes = self.base() + [
            node(
                "11111111-1111-1111-1111-111111111111",
                '<time startDay="7" startMonth="9" startYear="2026">Mon, Sep 7, 2026</time>',
                SEP,
            ),
            node(
                "22222222-2222-2222-2222-222222222222",
                '<time startYear="2026" startMonth="9" startDay="1">Tue, Sep 1, 2026</time>',
                SEP,
            ),
            node(
                "33333333-3333-3333-3333-333333333333",
                'resolved <time startYear="2026" startMonth="9" startDay="2">date</time>',
                SEP,
            ),
        ]
        self.assertEqual(
            extract_workflowy_days(nodes),
            [
                {"date": "2026-09-01", "node_id": "22222222-2222-2222-2222-222222222222"},
                {"date": "2026-09-07", "node_id": "11111111-1111-1111-1111-111111111111"},
            ],
        )

    def test_rejects_lookalike_outside_calendar_hierarchy(self):
        other_parent = "44444444-4444-4444-4444-444444444444"
        nodes = self.base() + [
            node(other_parent, "Sep"),
            node(
                "55555555-5555-5555-5555-555555555555",
                '<time startYear="2026" startMonth="9" startDay="5">Sat, Sep 5, 2026</time>',
                other_parent,
            ),
        ]
        self.assertEqual([], extract_workflowy_days(nodes))

    def test_invalid_date_is_ignored(self):
        nodes = self.base() + [
            node(
                "66666666-6666-6666-6666-666666666666",
                '<time startYear="2026" startMonth="9" startDay="31">invalid</time>',
                SEP,
            ),
        ]
        self.assertEqual([], extract_workflowy_days(nodes))

    def test_duplicate_calendar_date_preserves_both_nodes(self):
        nodes = self.base() + [
            node(
                "77777777-7777-7777-7777-777777777777",
                '<time startYear="2026" startMonth="9" startDay="7">a</time>',
                SEP,
            ),
            node(
                "88888888-8888-8888-8888-888888888888",
                '<time startYear="2026" startMonth="9" startDay="7">b</time>',
                SEP,
            ),
        ]
        self.assertEqual(
            [
                {"date": "2026-09-07", "node_id": "77777777-7777-7777-7777-777777777777"},
                {"date": "2026-09-07", "node_id": "88888888-8888-8888-8888-888888888888"},
            ],
            extract_workflowy_days(nodes),
        )

    def test_atomic_feed_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workflowy_days.json"
            write_feed_atomic(
                [{"date": "2026-09-07", "node_id": "11111111-1111-1111-1111-111111111111"}],
                path,
                generated_at="2026-09-12T00:00:00Z",
            )
            value = json.loads(path.read_text())
            self.assertEqual(1, value["schema_version"])
            self.assertEqual("2026-09-12T00:00:00Z", value["generated_at"])
            self.assertEqual("2026-09-07", value["workflowy_days"][0]["date"])


if __name__ == "__main__":
    unittest.main()
