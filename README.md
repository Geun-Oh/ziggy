# ziggy

`ziggy` is an **LSM-tree-based embedded key-value storage engine** implemented in Zig.
The repository now includes a usable integrated `Engine` API and operator-facing CLI on top of the core storage building blocks.

## What this project provides

- WAL (Write-Ahead Log) framing, checksums, and replay
- Atomic MANIFEST + `CURRENT` pointer based metadata versioning
- Lock-free SkipList MemTable
- Slotted-page block layout + prefix compression
- MVCC snapshot read model
- Sharded lock manager + deadlock detection
- Bloom filter + k-way merge iterator
- Lock-free clock-style cache handle/reference model
- Leveled compaction + tombstone cleanup + PITR archive replay

> This repository is an actively evolving engine-core project. Public APIs and formats may change.

## Is the binary production-usable right now?

`zig build run -- <command>` now exposes a functional CLI runtime:

- `open`
- `put`
- `get`
- `delete`
- `scan`
- `stats`
- `doctor`

The current runtime is intentionally small and single-process-writer oriented, but it supports durable WAL-backed mutations and restart recovery.

## Package compatibility

- Minimum Zig version: `0.15.2` (`build.zig.zon`)
- Package name: `ziggy`
- Module root: `src/root.zig`

## Supported platforms

ziggy now exposes a platform capability model at runtime and uses tiered support language for operational clarity.

### Tier 1

Fully supported and covered by CI/release automation:

- Linux `x86_64`
- Linux `aarch64`
- macOS `x86_64`
- macOS `aarch64`

### Tier 2

Expected to work, but with reduced automated verification:

- FreeBSD `x86_64`
- FreeBSD `aarch64`

### Experimental

- Other POSIX-like targets that compile but are not yet covered by release automation or operational validation

### Unsupported

The engine fails fast on unsupported host/platform combinations during `Engine.open()`.

### Platform capability properties

The engine property API exposes runtime capability flags such as:

- `engine.platform.os`
- `engine.platform.arch`
- `engine.platform.tier`
- `engine.platform.pointer_bits`
- `engine.platform.supported`
- `engine.platform.checkpoint_atomic_noreplace`
- `engine.platform.checkpoint_portable_reservation`
- `engine.platform.direct_io_path`

## Quickstart

### Build

```bash
zig build
```

### Run CLI binary

```bash
zig build run -- open --path ./data
zig build run -- put --path ./data --key hello --value world
zig build run -- get --path ./data --key hello
```

### Run tests

```bash
zig build test
```

The test suite includes positive/negative scenarios for durability, concurrency, and failure-injection paths.

## Import from another Zig project

Wire the dependency in your `build.zig` and import it as `ziggy`.

```zig
const ziggy_dep = b.dependency("ziggy", .{
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("ziggy", ziggy_dep.module("ziggy"));
```

Use in code:

```zig
const ziggy = @import("ziggy");
```

Local path dependency example (`build.zig.zon`):

```zig
.dependencies = .{
    .ziggy = .{ .path = "../ziggy" },
},
```

## How to store data today (module-level usage)

Until the integrated top-level engine API lands, you can compose modules directly.

### 1) Durable write to WAL

```zig
const std = @import("std");
const ziggy = @import("ziggy");

pub fn main() !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var durable = try ziggy.wal.DurableWal.init(tmp.dir, "wal.log", .{
        .mode = .fdatasync,
    });
    defer durable.deinit();

    try durable.appendRecord(std.heap.page_allocator, "hello-ziggy");
}
```

### 2) Read records back from WAL

```zig
const bytes = try tmp.dir.readFileAlloc(std.heap.page_allocator, "wal.log", 1024 * 1024);
defer std.heap.page_allocator.free(bytes);

const records = try ziggy.wal.readAllRecords(std.heap.page_allocator, bytes);
defer {
    for (records) |r| std.heap.page_allocator.free(r);
    std.heap.page_allocator.free(records);
}

// records[0] == "hello-ziggy"
```

### 3) Persist and reload LSM metadata (MANIFEST + CURRENT)

```zig
var store = ziggy.manifest.ManifestStore.init(std.heap.page_allocator, tmp.dir);
var version = ziggy.manifest.VersionSet.init(std.heap.page_allocator);
defer version.deinit();

try store.bootstrap(&version);

const file_meta = ziggy.manifest.FileMeta{
    .level = 0,
    .file_number = 1,
    .size = 4096,
    .min_key = @constCast("a"),
    .max_key = @constCast("z"),
};

try store.applyEditAtomic(&version, .{
    .new_files = &.{file_meta},
    .deleted_files = &.{},
    .next_sequence = 2,
    .next_file_number = 2,
});

var recovered = try store.loadVersionSet();
defer recovered.deinit();
```

### 4) PITR-style replay from archived WAL segments

Use `ziggy.recovery` to encode mutations, archive WAL segments, and restore state up to a target timestamp. See:

- `src/ziggy/recovery.zig` tests:
  - `task 6.2 positive: PITR replays WAL archive and stops at target microsecond`
  - `task 6.2 negative: archived WAL corruption yields error.ArchiveCorruption`

## Public module map

`src/root.zig` exports:

- `ziggy.wal`
- `ziggy.manifest`
- `ziggy.memtable`
- `ziggy.slotted_page`
- `ziggy.prefix_block`
- `ziggy.mvcc`
- `ziggy.lock_manager`
- `ziggy.access`
- `ziggy.clock_cache`
- `ziggy.compaction`
- `ziggy.recovery`
- `ziggy.engine`

## Repository structure

- `src/root.zig`: package entrypoint and exports
- `src/main.zig`: executable sample entrypoint
- `src/ziggy/*.zig`: storage-engine core modules
- `SPEC.md`: architecture and design spec
- `TASK.md`: implementation tasks and acceptance criteria
- `TRACEABILITY.md`: spec-to-test traceability matrix

## Development commands

```bash
zig build test
zig build run
zig build platform-matrix
```

## Release artifacts

Version tags trigger a GitHub Actions release workflow that builds and publishes per-target archives for the Tier 1 matrix.

Generated artifacts:

- `ziggy-x86_64-linux.tar.gz`
- `ziggy-aarch64-linux.tar.gz`
- `ziggy-x86_64-macos.tar.gz`
- `ziggy-aarch64-macos.tar.gz`
- matching `.sha256` checksum files for each archive

Each archive contains:

- `ziggy` binary
- `README.md`
- `LICENSE`

## How to cut a release

1. Ensure `main` is green in CI.
2. Update `CHANGELOG.md` with the release highlights.
3. Create and push a semantic version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

3. GitHub Actions will automatically:
   - run the release build matrix
   - package binaries for each supported Tier 1 OS/architecture target
   - generate SHA-256 checksum files
   - publish/update the corresponding GitHub Release

Release note categories are configured in `.github/release.yml`, the human-readable project history is tracked in `CHANGELOG.md`, and the full release operator checklist lives in [`RELEASING.md`](./RELEASING.md).

## CI automation

The repository uses GitHub Actions for three layers of automation:

1. **CI (`ci.yml`)**
   - runs on pushes to `main`, pull requests, and release tags
   - executes build/test validation on representative native runners
   - runs the cross-target build gate via `zig build platform-matrix`

2. **Nightly soak (`nightly-soak.yml`)**
   - runs scheduled long-duration compatibility, fault-matrix, ENOSPC, and soak verification

3. **Release (`release.yml`)**
   - runs on `v*` tag pushes
   - builds per-target release archives
   - publishes artifacts to GitHub Releases automatically

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md).

## License

MIT License. See [`LICENSE`](./LICENSE).
