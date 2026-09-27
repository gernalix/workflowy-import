# workflowy-import repository guidance

This repository's scheduled job operates on the Oracle runtime at `/opt/workflowy_import`. Work only in an isolated `repo-task` worktree and submit repository changes through a protected PR. Never modify or deploy to the remote service as part of source validation.

`workflowy_job.py` requires `WORKFLOWY_API_KEY`, contacts the authenticated Workflowy API, writes timestamped/latest JSON backups, updates the configured SQLite database, writes the Calendar-day feed, and may notify Telegram. Do not run it in tests or without explicit runtime authorization. Keep credentials and the gitignored `telegram_notify.py` runtime helper out of Git.

For offline checks, run `python3 -m unittest -v test_workflowy_days.py` and the Python compilation command from `.github/workflows/ci.yml`. Capsule FAST/FULL checks use the C2 validator declared in `project-capsule.yaml`; FULL runs only the offline unit test hook. Preserve remote database and backup data, and gate any schema migration or restore behind a verified backup and explicit runtime task.
