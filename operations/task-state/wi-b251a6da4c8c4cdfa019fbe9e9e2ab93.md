# C2 work item wi:b251a6da4c8c4cdfa019fbe9e9e2ab93

Objective: adopt Project Capsule v1 for MegaVault project 94 / repository R0094 (`gernalix/workflowy-import`) without changing the Oracle runtime.

Constraints: use the exact GitHub source remote in a dedicated local clone and `repo-task` worktree; preserve `/opt/workflowy_import`, its service/timer, credentials, backups, SQLite database, and feed. No authenticated API call or deployment.

Checklist:
- [x] Verify C2 start receipt #3024 is applied and item is running.
- [x] Resolve project identity, source remote, and runtime facts from MegaVault.
- [x] Clone exact source remote locally and allocate isolated task worktree.
- [x] Add minimal Project Capsule manifest, AGENTS guidance, and task checkpoint.
- [x] Run FAST/FULL and offline tests/compile; do not run runtime job.
- [ ] Queue PR and read back integration/receipt before terminal PASS.

Verified facts: MegaVault project 94 is `workflowy-import`; R0094 canonical runtime path is `/opt/workflowy_import`, remote is `https://github.com/gernalix/workflowy-import`, and service SVC0073 records `workflowy-import.service+workflowy-import.timer` on Oracle H0002. Its purpose is list-all backup/import into `/home/ubuntu/sync_root/db/workflowy.db` for Datasette. Source code defaults are environment-overridable. The isolated clone is `/home/daniele/.local/share/codex-github-autosync/repos/gernalix/workflowy-import`; task worktree is `/home/daniele/.local/share/codex-github-autosync/worktrees/gernalix_workflowy-import/wi-b251a6da4c8c4cdfa019fbe9e9e2ab93`, branch `task/wi-b251a6da4c8c4cdfa019fbe9e9e2ab93`, based on main at `caec39835029dad0ff60922023a81effaa5817e7`.

Verification: Capsule FAST PASS against MegaVault identity, paths, freshness, and both changed-file mappings; Capsule FULL PASS with offline unit tests (`exit=0`); CI-equivalent `py_compile` passed. No authenticated API call or runtime write was run.

Acceptance: root manifest matches Capsule v1 and MegaVault identity; FAST/FULL and targeted offline checks pass; integration head and C2 terminal receipt are read back before PASS.

Next action: queue the isolated branch through `repo-task finish` and verify its PR/integration state.
