# SPEC/TASK Traceability Matrix (Iteration 12)

Legend: Done / Partial / Missing

## Verification Artifacts (this iteration)
- Build/test baseline from prior iteration: `artifacts/iter10/zig-build-test.log`, `artifacts/iter10/engine-zig-test.log`
- Current change-set verification: local `zig test src/ziggy/engine.zig` (51/51 passed)
- Full project verification: local `zig build test` (pass, including CLI E2E)
- ENOSPC integration (host run): `artifacts/iter10/enospc.log`, `artifacts/enospc-iter10/result.json` (ok=true, skipped on non-Linux host)

## Requirement Checklist (TASK.md)

| Task | Status | Implementation paths | Test evidence | Artifact |
|---|---|---|---|---|
| 1.1 WAL serializer/framer + CRC | Done | `src/ziggy/wal.zig` | `task 1.1 positive...`, `task 1.1 negative...`, durability tests | `artifacts/iter9/zig-build-test.log` |
| 1.2 Atomic MANIFEST/CURRENT | Done | `src/ziggy/manifest.zig` | `task 1.2 positive...`, `task 1.2 negative...` | `artifacts/iter9/zig-build-test.log` |
| 2.1 Concurrent memtable | Done | `src/ziggy/memtable.zig` | `task 2.1 positive...`, `task 2.1 negative...` | `artifacts/iter9/zig-build-test.log` |
| 3.1 Slotted page | Done | `src/ziggy/slotted_page.zig` | `task 3.1 positive...`, `task 3.1 negative...` | `artifacts/iter9/zig-build-test.log` |
| 3.2 Prefix compression | Done | `src/ziggy/prefix_block.zig` | `task 3.2 positive...`, `task 3.2 negative...` | `artifacts/iter9/zig-build-test.log` |
| 4.1 MVCC snapshot read | Done | `src/ziggy/mvcc.zig` | `task 4.1 positive...`, `task 4.1 negative...` | `artifacts/iter9/zig-build-test.log` |
| 4.2 Lock manager + deadlock detect | Done | `src/ziggy/lock_manager.zig` | `task 4.2 positive...`, `task 4.2 negative...` | `artifacts/iter9/zig-build-test.log` |
| 4.3 K-way merge + bloom bypass | Done | `src/ziggy/access.zig` | `task 4.3 positive...`, `task 4.3 negative...` | `artifacts/iter9/zig-build-test.log` |
| 5.1 Lock-free clock cache | Done | `src/ziggy/clock_cache.zig` | `task 5.1 positive...`, stress/fault tests, leak-detect negative | `artifacts/iter9/zig-build-test.log` |
| 6.1 Leveled compaction + rollback safety | Done | `src/ziggy/compaction.zig` | `task 6.1 positive...`, `task 6.1 negative...`, durability/safety tests | `artifacts/iter9/zig-build-test.log` |
| 6.2 PITR + archive recovery | Done | `src/ziggy/recovery.zig` | `task 6.2 positive...`, `task 6.2 negative...` | `artifacts/iter9/zig-build-test.log` |
| 7.1 Engine lifecycle API | Done | `src/ziggy/engine.zig` | `task 7.1 positive...`, `task 7.1 negative...` | `artifacts/iter9/zig-build-test.log` |
| 7.2 Crash-safe runtime recovery | Done | `src/ziggy/engine.zig` | `task 7.2 positive...`, `task 7.2 negative...` | `artifacts/iter9/zig-build-test.log` |
| 7.3 CLI (put/get/delete/scan/stats/json) | Done | `src/main.zig`, `scripts/cli_e2e.sh` | main.zig task 7.3 tests + e2e script | `artifacts/iter9/cli-e2e.log`, `artifacts/iter9/zig-build-test.log` |
| 7.4 API/CLI consistency + shutdown determinism | Done | `src/ziggy/engine.zig` | `task 7.4 positive...`, `task 7.4 negative...` | `artifacts/iter9/zig-build-test.log` |
| 8.1 Canonical LSM write path | Done | `src/ziggy/engine.zig` | `task 8.1 positive...`, `task 8.1 negative...` | `artifacts/iter9/zig-build-test.log` |
| 8.2 Canonical LSM read path | Done | `src/ziggy/engine.zig` | `task 8.2 positive...`, `task 8.2 negative...` | `artifacts/iter9/zig-build-test.log` |
| 9.1 Bounded scheduler queues/workers | Done | `src/ziggy/engine.zig` | `task 9.1 positive...`, `task 9.1 negative...` | `artifacts/iter9/zig-build-test.log` |
| 9.2 Stall state machine | Done | `src/ziggy/engine.zig` | `task 9.2 negative...`, regression test | `artifacts/iter9/zig-build-test.log` |
| 9.3 Compaction I/O limiter | Done | `src/ziggy/engine.zig` | `task 9.3 positive...`, `task 9.3 negative...` | `artifacts/iter9/zig-build-test.log` |
| 10.1 Crash-consistency fault matrix | Done | `scripts/fault_matrix.sh` | script pass/fail matrix with invariants | `artifacts/fault-matrix-iter9/result.json` |
| 10.2 Metrics + property API | Done | `src/ziggy/engine.zig`, `src/main.zig` | task 10.2 positive/negative tests (engine+CLI) | `artifacts/iter9/zig-build-test.log` |
| 10.3 Soak + compatibility suite | Done | `scripts/soak_compat.sh`, `scripts/enospc_integration.sh`, CI workflow | 120s soak pass + compat gates + ENOSPC integration/skipped-host marker | `artifacts/soak-iter9/result.json`, `artifacts/enospc-iter10/result.json` |
| 11.1 Atomic WriteBatch commit path | Done | `src/ziggy/engine.zig` (`writeBatch`, batch WAL encode/replay, put/delete unified path) | `task p1 positive: writeBatch applies multiple ops and survives reopen`, `task p1 negative: crash after batch WAL append before mem apply replays exactly once`, `task p1 negative: corrupted batch WAL fails open deterministically` | local `zig test src/ziggy/engine.zig` (pass) |
| 11.2 Public snapshot contract | Done | `src/ziggy/engine.zig` (`beginSnapshot`, `getSnapshot`, `releaseSnapshot`) | `task p1 positive: snapshot read remains stable while newer batch commits` | local `zig test src/ziggy/engine.zig` (pass) |
| 11.3 Error classification surface | Done | `src/ziggy/engine.zig` (`ErrorClass`, `classifyError`) | `task p1 regression: classifyError maps key engine errors` | local `zig test src/ziggy/engine.zig` (pass) |
| 12.1 OS/architecture capability model | Done | `src/ziggy/platform.zig`, `src/ziggy/engine.zig` (platform properties, startup gating) | `task 10.2 positive: platform capability properties are exposed`, `platform capabilities are internally consistent` | local `zig test src/ziggy/engine.zig` (pass) |
| 12.2 Comptime-specialized decode path | Done | `src/ziggy/comptime_bytes.zig`, `src/ziggy/wal.zig`, `src/ziggy/engine.zig` | `comptime little-endian reads decode expected values`, WAL/engine recovery suites remain green | local `zig test src/ziggy/engine.zig` (pass) |
| 12.3 Cross-target build matrix | Done | `build.zig`, `.github/workflows/nightly-soak.yml` | `zig build platform-matrix` compiles linux/macos x x86_64/aarch64 targets | local `zig build test`, workflow gate added |
| 12.4 Portable checkpoint protocol | Done | `src/ziggy/engine.zig` (`createPortableCheckpoint`, incomplete marker fail-closed open path) | `task 12.1 positive: checkpoint creates reopenable snapshot`, `task 12.1 safety: incomplete portable checkpoint fails closed on open` | local `zig test src/ziggy/engine.zig` (pass) |
| 12.5 GitHub Actions release automation | Done | `.github/workflows/ci.yml`, `.github/workflows/release.yml` | workflow matrices cover CI and tagged multi-target release packaging | repository workflow definitions |

## Additional checkpoint safety coverage
- `task 12.1 positive: checkpoint creates reopenable snapshot`
- `task 12.1 negative: checkpoint rejects empty path`
- `task 12.1 safety: checkpoint rejects parent traversal path`
- `task 12.1 safety: checkpoint fails on existing directory without overwrite`
- `task 12.1 regression: concurrent checkpoint creators are deterministic and leave no partial output`

Evidence: `src/ziggy/engine.zig` tests, `artifacts/iter10/engine-zig-test.log`.

## Security review (explicit)
Reviewed modified runtime paths:
- `src/ziggy/engine.zig` (`checkpoint`, `renameNoReplace`, recovery/open path)

Findings and actions:
1. **Path handling / traversal**
   - `checkpoint` rejects empty path and `..` traversal (`containsParentTraversal`, `openDirNoSymlinkChain`).
   - Tests: `task 12.1 negative...`, `task 12.1 safety...parent traversal...`.
2. **Fail-closed corruption handling**
   - WAL corruption open path returns typed `EngineError.CorruptedWalRecord` deterministically.
   - Tests: `task 7.2 negative...`, fault matrix `wal_corruption_fail_open`.
3. **Atomic destination collision behavior**
   - Linux: `renameat2(RENAME_NOREPLACE)` enforces kernel-atomic no-overwrite semantics and returns `CheckpointAlreadyExists` on collisions.
   - Non-Linux: `checkpoint` now fails closed with `EngineError.UnsupportedPlatform` (no best-effort pre-check/rename path).
   - Tests: `task 12.1 safety...existing directory without overwrite`, `task 12.1 regression...concurrent checkpoint creators...`.
4. **Resource cleanup**
   - `checkpoint` temp dir protected by `errdefer parent.deleteTree(tmp_name)`; copied file handles are scoped/deferred.
   - Verified by race regression test (no leftover `.ziggy-checkpoint-*`) + repeated runs in `zig build test`.

Checkpoint destination guarantee wording:
- **Linux:** destination collision handling is atomic and no-overwrite safe at kernel boundary.
- **Non-Linux:** no-overwrite checkpoint creation is **unsupported by design** and returns `UnsupportedPlatform` without creating destination output.

Residual security caveat:
- ENOSPC destructive integration still requires a Linux run artifact with `skipped=false`; host run remains a non-Linux skip marker.
