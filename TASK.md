Here is the highly detailed, production-grade Task Document and Todo List based on the ziggy specification. It is structured hierarchically, outlining the specific implementation details along with mandatory Positive and Negative testing criteria for each component.

# Project ziggy: Production-Grade Implementation Task List

## Phase 1: Foundation & Durability Layer

The foundational layer ensures absolute data integrity and crash consistency before any complex features are built.

### Task 1.1: Implement Write-Ahead Log (WAL) Serializer & Framer

* **Detail:** Implement the WAL using fixed-size 32 KB blocks to prevent torn writes. Every logical record must be fragmented if it exceeds block boundaries. Each fragment must be prefixed with a 7-byte header consisting of a 4-byte CRC32C checksum, a 2-byte length, and a 1-byte type marker (`FIRST`, `MIDDLE`, `LAST`, `FULL`). Operations must utilize `O_DIRECT` or `fdatasync` based on engine configuration.


* **Positive Test (Success):** Write a 100 KB payload to the WAL. Verify the framer correctly splits the payload across four 32 KB blocks, writes the appropriate `FIRST`, `MIDDLE`, and `LAST` type markers, and calculates accurate CRC32C checksums for each fragment.


* **Negative Test (Failure):** Intentionally corrupt a single random byte of a WAL payload on disk. Initialize the database recovery sequence. Verify the reader detects the CRC32C mismatch, rejects the block, and yields an `error.CorruptedBlock` without triggering a Zig panic.



### Task 1.2: Atomic Manifest & VersionSet State Machine

* **Detail:** Create the `MANIFEST` file to act as a transactional log for structural LSM-Tree metadata. Implement `VersionEdit` structs to record `NewFiles`, `DeletedFiles`, and `NextSequence`. Implement the `CURRENT` file pointer update logic using temporary files and the atomic POSIX `rename` kernel operation.


* **Positive Test (Success):** Programmatically apply a `VersionEdit` simulating the addition of a new Level 1 SSTable. Verify the metadata is flushed to the `MANIFEST`, the `CURRENT` pointer is atomically updated, and a simulated database reboot perfectly reconstructs the `VersionSet` layout.


* **Negative Test (Failure):** Hard-kill the engine process immediately after the `MANIFEST` is written but exactly before the `CURRENT` file is renamed. Upon engine restart, verify it seamlessly falls back to the previous valid `MANIFEST` and recovers the data from the WAL without corruption.



---

## Phase 2: In-Memory Structures

This phase focuses on extremely high-throughput, concurrent ingestion memory structures leveraging Zig's low-level hardware control.

### Task 2.1: Lock-Free Concurrent SkipList (MemTable)

* **Detail:** Implement the in-memory MemTable as a probabilistic SkipList. Remove all global mutexes. Implement insertions using `Compare-And-Swap (CAS)` operations with `Release` memory ordering. Implement asynchronous reader traversal using `@atomicLoad` with `Acquire` memory ordering. Utilize Zig's `comptime` to inline key comparator logic.


* **Positive Test (Success):** Spawn 100 independent writer threads, each rapidly inserting 1,000 unique, randomly generated keys. Simultaneously spawn 10 reader threads. Verify the readers successfully extract all inserted keys in perfect lexicographical order without encountering race conditions.


* **Negative Test (Failure):** Attempt to insert a key that wildly exceeds the engine's configured maximum key size. Verify the MemTable immediately returns `error.KeyTooLarge` without mutating any internal atomic pointers or allocating memory.



---

## Phase 3: Immutable Storage & Disk Formatting

This phase dictates the physical file layout required to minimize Read Amplification and Space Amplification.

### Task 3.1: SSTable Slotted Page Block Builder

* **Detail:** Structure the 4KB - 16KB physical SSTable blocks using a Slotted Page layout. The layout must feature a downward-growing tuple data region starting after the header, and an upward-growing slot array (containing exact physical offsets) located at the block's footer. This enables $O(1)$ block lookups via binary search over the slot array.


* **Positive Test (Success):** Write 50 variable-length keys to a block builder. Verify the slot array at the footer accurately captures the physical byte offsets. Execute a binary search for the 25th key and verify it resolves instantly by dereferencing the slot pointer.


* **Negative Test (Failure):** Attempt to write a payload that exceeds the contiguous unallocated free space gap in the center of the block. Verify the builder successfully rejects the payload, yields `error.BlockFull`, and safely finalizes the current block.



### Task 3.2: Prefix Compression & Delta Encoding Algorithm

* **Detail:** Implement Prefix Compression (Front Compression) to eliminate redundant key bytes within SSTable blocks. Store the `Shared Prefix Length`, `Unshared Suffix Length`, and the raw suffix bytes. Inject complete, uncompressed "Restart Points" every 16 keys to bound the computational cost of linear decoding.


* **Positive Test (Success):** Provide an array of 100 sequentially numbered keys sharing a long identical prefix (e.g., `user_profile_data_001` to `user_profile_data_100`). Verify the resulting block size is significantly smaller than the raw uncompressed bytes, and that random access successfully reconstructs any given key.


* **Negative Test (Failure):** Provide the decoder with an intentionally malformed block where the `Unshared Suffix Length` is manipulated to exceed the physical block boundary. Verify the decoder safely halts the scan and returns `error.DecompressionFailure` instead of experiencing a buffer overflow.



---

## Phase 4: Concurrency Control & Access Management

This phase bridges the gap between concurrent user requests and the underlying storage files.

### Task 4.1: Multi-Version Concurrency Control (MVCC) Engine

* **Detail:** Implement Snapshot Isolation. Embed an 8-byte global `SequenceNumber` into all write payloads. During read operations, actively ignore any key version whose sequence number is strictly greater than the reading transaction's acquired snapshot sequence number.


* **Positive Test (Success):** Open a transaction (Snapshot A) at Sequence 100. Concurrently, write a new value to `Key X` which receives Sequence 105. Query `Key X` using Snapshot A and verify it returns the older, valid version from before Sequence 100.


* **Negative Test (Failure):** Query a key under a snapshot where the only valid version historically available is a Tombstone (Delete) marker. Verify the MVCC engine masks the underlying historical data and returns `error.KeyNotFound`.



### Task 4.2: Lock Manager & Wait-For Graph Deadlock Detection

* **Detail:** Implement a sharded row-level lock manager for pessimistic transactions to minimize CPU cache contention. Implement a directed Wait-For Graph (WFG) that executes a Breadth-First Search (BFS) to identify circular locking dependencies dynamically.


* **Positive Test (Success):** Initiate Transaction A targeting `Key 1` and Transaction B targeting `Key 2`. Verify both transactions acquire locks in different hash stripes and execute concurrently without blocking.


* **Negative Test (Failure):** Induce a classic deadlock: Transaction A locks `Key 1` and waits for `Key 2`; Transaction B locks `Key 2` and waits for `Key 1`. Verify the WFG detects the cycle, automatically terminates the newest transaction, and returns `error.DeadlockDetected`.



### Task 4.3: K-Way Merging Iterator & Bloom Filter Integration

* **Detail:** Build an Access Manager utilizing a Min-Heap data structure to traverse multiple overlapping LSM-Tree levels simultaneously (K-Way Merge). Integrate 10-bit-per-key Bloom Filters at the block level to mathematically prevent useless disk seeks.


* **Positive Test (Success):** Query a key mathematically proven to be absent. Verify the Bloom filter accurately yields a negative result and the access layer completely bypasses all disk I/O operations for that file.


* **Negative Test (Failure):** Continuously call `Next()` on the K-Way Merging Iterator until all internal file cursors are fully depleted. Verify the next call gracefully returns `error.IteratorExhausted` rather than attempting an unsafe pop operation on an empty Min-Heap.



---

## Phase 5: Buffer Pool Management

Optimizing memory usage while supporting thousands of concurrent reads.

### Task 5.1: Lock-Free Clock Cache (HyperClockCache)

* **Detail:** Replace traditional mutex-bound LRU caches with a Lock-Free Clock Cache. Pack the `Acquire Count`, `Release Count`, `State`, and `Hit` tracking bits into a single, atomic 64-bit integer per cache slot to allow concurrent priority updates without locking.


* **Positive Test (Success):** Simulate 100 threads simultaneously fetching the exact same heavily-contended block. Verify the slot's atomic `Acquire Count` increments flawlessly under load without a single thread blocking on a mutex.


* **Negative Test (Failure):** Simulate an application layer function that fetches a block but "forgets" to call the `Release` defer function on the handle. Execute the Zig `std.testing.leak_check`. Verify the test suite fails violently, enforcing the strict explicit memory management contract.



---

## Phase 6: Compaction & Disaster Recovery

Ensuring long-term operational stability and recovery from catastrophic administrative errors.

### Task 6.1: Leveled Compaction & Tombstone Garbage Collection

* **Detail:** Implement the background compaction thread. Monitor level sizes ($10^L$ multiplier). When a threshold is breached, trigger a merge-sort combining a file from Level $L$ with all overlapping files in Level $L+1$. Permanently drop Tombstone markers only if the key does not exist in deeper levels.


* **Positive Test (Success):** Rapidly flood Level 0 until its size threshold is exceeded. Verify the compaction thread awakens, successfully merges L0 into L1, completely deletes the old overlapping L0 files from disk, and updates the `MANIFEST`.


* **Negative Test (Failure):** Inject a mock `error.DiskFull` environment variable midway through a background compaction write cycle. Verify the compactor aborts, deletes any partially written temporary SSTables, leaves the original database state perfectly intact, and logs the OS error.



### Task 6.2: Point-In-Time Recovery (PITR) & WAL Archiving

* **Detail:** Build the continuous archiving protocol. Expose a configuration to trigger a background script whenever a 32 KB WAL block or file is closed, copying it to remote storage. Implement a Restore Manager capable of parsing archived WALs and halting replay at a strict `recovery_target_time`.


* **Positive Test (Success):** Restore an old base backup and point the Restore Manager to an archive of WAL segments. Provide a target timestamp. Verify the database perfectly replays all transactions and safely halts execution exactly at the requested microsecond.


* **Negative Test (Failure):** Attempt a roll-forward recovery using an archived WAL segment that suffered bit-rot during network transit (invalid CRC32C). Verify the Restore Manager immediately aborts the replay sequence and throws `error.ArchiveCorruption`, protecting the recovered database from absorbing corrupted state.


---

## Phase 7: Integrated Engine API & CLI Productization

This phase assembles existing storage primitives into a usable key-value engine runtime and command-line interface.

### Task 7.1: Implement Stable `Engine` Lifecycle API

* **Detail:** Introduce a top-level `engine` module exposing `open`, `close`, `put`, `get`, `delete`, and `scan`. The implementation must orchestrate WAL, MemTable, MANIFEST, compaction worker, and recovery paths behind a cohesive API boundary.


* **Positive Test (Success):** Open a fresh database directory, write 1,000 key-value pairs, read them back successfully, then close and reopen the engine. Verify all data remains visible after restart.


* **Negative Test (Failure):** Attempt to open the same database directory from a second writer process while the first process is active. Verify the second open fails with an explicit lock-related error and does not corrupt on-disk state.


### Task 7.2: Crash-Safe End-to-End Runtime Recovery

* **Detail:** Wire startup recovery so `Engine.open` replays WAL segments, restores metadata from MANIFEST/CURRENT, reconstructs sequence counters, and resumes background maintenance safely.


* **Positive Test (Success):** Perform `put` operations, hard-kill the process before graceful shutdown, then reopen using `Engine.open`. Verify all acknowledged writes are recoverable and no duplicate application occurs.


* **Negative Test (Failure):** Inject corruption into the latest WAL segment before startup. Verify `Engine.open` fails fast with a deterministic recovery error and never transitions to a partially initialized runtime.


### Task 7.3: Implement Operator-Facing CLI (`ziggy`)

* **Detail:** Extend the executable into a functional CLI supporting at minimum: `put`, `get`, `delete`, `scan`, and `stats` with `--path` scoping. Add optional `--json` output for automation.


* **Positive Test (Success):** Execute CLI commands in sequence (`put` -> `get` -> `delete` -> `get` miss) on the same database directory. Verify outputs and exit codes match expected behavior in both text and JSON modes.


* **Negative Test (Failure):** Invoke commands with missing required arguments, invalid paths, and malformed flags. Verify the CLI exits non-zero and prints explicit usage/error messages without panics.


### Task 7.4: Public API/CLI Consistency and Snapshot Semantics

* **Detail:** Ensure API and CLI semantics match: successful writes are durable, `get` resolves latest visible value, and `scan` reads from a stable snapshot during each invocation.


* **Positive Test (Success):** Start a long-running scan while concurrent writers update overlapping keys. Verify the scan returns a consistent snapshot view (no mid-stream mutation visibility drift).


* **Negative Test (Failure):** Simulate `close()` during active background compaction. Verify shutdown completes deterministically, releases resources, and leaves no leaked file descriptors or orphaned locks.


---

## Phase 8: LSM Runtime Unification (RocksDB Gap Closure)

### Task 8.1: Replace Map-Centric Engine Path with Canonical LSM Write Path

* **Detail:** Route `put/delete` through `WAL -> active MemTable -> immutable MemTable -> flush trigger`, removing hash-map authority as durable truth. Keep sequence assignment and WAL ordering as the single durability boundary.


* **Positive Test (Success):** Write enough keys to force at least one MemTable rotation and flush to L0, restart engine, and verify all keys are queryable solely through LSM structures.


* **Negative Test (Failure):** Kill process after WAL append but before MemTable apply during stress loop. On reopen, verify replay recovers acknowledged writes exactly once and yields no duplicated sequence application.


### Task 8.2: Canonical LSM Read Path Across MemTables + Levels

* **Detail:** Implement `get/scan` lookup order: active MemTable, immutable MemTables, overlapping L0 files, then one candidate per level for L1+ with sequence-aware visibility and tombstone masking.


* **Positive Test (Success):** Seed multiple versions of same key across mutable/immutable/L0/L1 and verify read returns newest visible non-tombstone under snapshot.


* **Negative Test (Failure):** Inject conflicting versions where newest visible entry is tombstone in L0 and older value exists in L2. Verify `get` returns `KeyNotFound`.


---

## Phase 9: Background Scheduling, Backpressure, and Stall Control

### Task 9.1: Flush/Compaction Scheduler with Bounded Queues

* **Detail:** Add dedicated background workers, queue depth limits, and deterministic work ordering to prevent unbounded pending flush/compaction debt.


* **Positive Test (Success):** Under sustained write load, verify scheduler drains immutable memtables and compaction queues while foreground writes continue.


* **Negative Test (Failure):** Saturate scheduler queue beyond configured limit and verify engine enters explicit backpressure state rather than unbounded memory growth.


### Task 9.2: Write Stall State Machine (`normal` / `slowdown` / `stop`)

* **Detail:** Implement level-pressure thresholds and state transitions controlling write throughput and admission. Expose current stall reason for diagnostics.


* **Positive Test (Success):** Increase L0 file count to slowdown threshold and verify write latency increases via configured pacing but operations still complete.


* **Negative Test (Failure):** Exceed hard stop threshold and verify writes fail fast with explicit stall error until background debt decreases.


### Task 9.3: Compaction I/O Rate Limiter

* **Detail:** Add token-bucket style limiter for background compaction output/input bandwidth to protect foreground p99 latency.


* **Positive Test (Success):** With limiter enabled, observe bounded compaction throughput and stable foreground `get` latency under mixed workload.


* **Negative Test (Failure):** Configure invalid rate (zero/negative) and verify engine rejects config with explicit validation error.


---

## Phase 10: Durability Hardening, Observability, and Soak Validation

### Task 10.1: Crash-Consistency Fault Injection Matrix

* **Detail:** Implement deterministic injection points for WAL sync boundaries, SST temp file rename boundaries, MANIFEST/CURRENT update boundaries, disk-full, and partial-write corruption.


* **Positive Test (Success):** Execute full matrix campaign and verify each crash point either recovers to last acknowledged state or fails closed with deterministic error.


* **Negative Test (Failure):** Corrupt CURRENT pointer target and verify startup rejects broken metadata path without partial initialization.


### Task 10.2: Metrics and Property Introspection API

* **Detail:** Export counters/gauges/histograms and `engine property` API/CLI (memtable bytes, L0 files, pending compaction bytes, cache usage, stall reason).


* **Positive Test (Success):** Run workload and verify metrics monotonicity/consistency and property values reflect real-time runtime state.


* **Negative Test (Failure):** Query unknown property key and verify stable, typed error response (no panic, no undefined output).


### Task 10.3: Long-Run Soak + Compatibility Suite

* **Detail:** Add multi-hour soak tests, randomized crash-restart loops, and on-disk format compatibility tests across minor versions.


* **Positive Test (Success):** Complete soak duration with bounded memory, no descriptor leaks, and stable latency envelopes within configured SLO.


* **Negative Test (Failure):** Attempt startup with unsupported major on-disk format version and verify immediate fail-fast with migration-required error.

---

## Phase 11: DBMS-Internal Integration Readiness (Phase 1)

### Task 11.1: Atomic WriteBatch Commit Path

* **Detail:** Add a public `writeBatch` API that commits multiple `put/delete` operations atomically at WAL/replay boundary. Single-operation `put/delete` must be implemented through the same batch path to avoid divergence.


* **Positive Test (Success):** Execute a mixed batch (`put`, `put`, `delete`), reopen the engine, and verify all operations are reflected in one durable commit boundary.


* **Negative Test (Failure):** Simulate crash after batch WAL append but before memtable apply. Verify reopen replays the batch exactly once with no duplicate sequence advancement.


### Task 11.2: Public Snapshot Contract for Host DBMS

* **Detail:** Expose `beginSnapshot/getSnapshot/releaseSnapshot` as stable API so host DBMS layers can pin read visibility explicitly and avoid accidental latest-state reads.


* **Positive Test (Success):** Acquire snapshot, commit newer writes, and verify snapshot reads remain stable while latest reads observe new values.


* **Negative Test (Failure):** Query a key created after snapshot capture via snapshot API and verify `KeyNotFound` for that snapshot view.


### Task 11.3: Error Classification Surface for Retry/Fail-Fast Policy

* **Detail:** Provide typed error classification (`busy`, `corruption`, `retryable`, `fatal`, `not_found`) so host DBMS orchestration can apply deterministic retry and escalation policies.


* **Positive Test (Success):** Verify core engine errors map to expected classes (e.g., `WriteStall` -> `busy`, `CorruptedWalRecord` -> `corruption`).


* **Negative Test (Failure):** Ensure unknown/unclassified errors default to `fatal` to avoid silent retry loops.

---

## Phase 12: Platform Coverage and Comptime Optimization

### Task 12.1: OS/Architecture Capability Model and Runtime Exposure

* **Detail:** Add a compile-time capability model that classifies OS/architecture support tier and exposes host capabilities (`os`, `arch`, pointer bits, checkpoint atomic rename availability, direct-I/O path availability) through engine properties.


* **Positive Test (Success):** Query platform properties and verify non-empty OS/arch/tier metadata and valid boolean/integer capability fields.


* **Negative Test (Failure):** Attempt engine startup on unsupported platform tiers and verify deterministic `UnsupportedPlatform` fail-fast behavior.


### Task 12.2: Comptime-Specialized Decode Path for WAL/Engine

* **Detail:** Introduce a reusable comptime little-endian integer reader and migrate hot decode paths (`wal` frame header parsing and engine WAL record parsing) to the generic utility to reduce duplicate parsing logic and improve compile-time specialization.


* **Positive Test (Success):** Validate integer decoding for `u16/u32/u64` with exact expected values and confirm all WAL/engine recovery tests continue to pass.


* **Negative Test (Failure):** Verify out-of-bounds decode attempts return a typed bounds error and are translated to corruption errors in recovery paths.


### Task 12.3: Cross-Target Build Matrix for Tiered Platform Support

* **Detail:** Add a build-system target matrix that compiles the CLI/library for core Tier 1 operating system and architecture combinations (`linux/macos` x `x86_64/aarch64`) and wire the matrix into CI as a non-optional gate.


* **Positive Test (Success):** Run the platform matrix build step and verify all configured targets compile successfully.


* **Negative Test (Failure):** Introduce an unsupported target policy violation or target-specific compile breakage and verify the matrix step fails the build deterministically.


### Task 12.4: Portable Checkpoint Creation on Non-Linux Targets

* **Detail:** Replace the previous non-Linux `UnsupportedPlatform` checkpoint behavior with a portable reservation-based checkpoint protocol: atomically reserve the destination directory, materialize a fail-closed `CHECKPOINT_INCOMPLETE` marker during copy, finalize with a completion marker, and reject opens against incomplete checkpoint directories.


* **Positive Test (Success):** Create a checkpoint on any supported platform, reopen it successfully, and verify durable data visibility.


* **Negative Test (Failure):** Simulate an incomplete portable checkpoint directory and verify `Engine.open` fails closed with a typed incomplete-checkpoint error.


### Task 12.5: GitHub Actions Release Automation by OS/Architecture

* **Detail:** Add GitHub Actions workflows that (a) validate push/PR builds on representative runners and (b) produce release artifacts for each Tier 1 OS/architecture target on tag pushes, publishing per-target archives automatically to GitHub Releases.


* **Positive Test (Success):** Trigger the release workflow on a version tag and verify artifacts are built and uploaded for `linux/macos` x `x86_64/aarch64`.


* **Negative Test (Failure):** Break one target-specific build and verify the corresponding matrix job fails, preventing an incomplete silent release.
