# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Public `writeBatch` API for atomic multi-operation commit at the WAL/replay boundary.
- Public snapshot API: `beginSnapshot`, `getSnapshot`, and `releaseSnapshot`.
- Error classification surface for host DBMS retry/fail-fast policy integration.
- Platform capability model with tiered OS/architecture support reporting.
- Runtime platform properties for OS, architecture, pointer width, checkpoint behavior, and direct-I/O capability.
- Comptime-specialized little-endian decode utility reused by WAL and engine recovery paths.
- Cross-target build matrix via `zig build platform-matrix`.
- Portable non-Linux checkpoint protocol using fail-closed incomplete markers.
- GitHub Actions CI workflow for push/PR validation.
- GitHub Actions release workflow for tag-driven multi-target binary packaging and publishing.

### Changed
- Unified single-operation `put/delete` through the batch commit path.
- Reworked WAL replay to understand both legacy single-op records and batch records.
- Expanded README with supported platform tiers, release artifact details, and release instructions.

### Fixed
- Corrected replay decoding bugs in batch WAL parsing and sequence replay handling.
- Fixed latency recording in the batch write path.
- Hardened checkpoint handling to reject incomplete checkpoint directories during open.

## [0.1.0] - 2026-03-06

### Added
- Integrated embedded engine API with `open`, `put`, `get`, `delete`, `scan`, `stats`, `doctor`, and checkpoint support.
- WAL framing with checksums and replay.
- Atomic MANIFEST/CURRENT metadata model.
- MVCC snapshot visibility model.
- Background flush and compaction scheduling.
- Fault-matrix, ENOSPC, and soak verification scripts.
- Tier-1 target build and release automation scaffolding.

### Notes
- `0.1.0` is the first tagged baseline for the integrated engine runtime and CI/release automation.
- On-disk formats and APIs may continue to evolve before a stable `1.0.0` contract.
