//! Integrated LSM-tree engine that ties together WAL, memtables, manifests, scans, and background jobs.
const std = @import("std");
const builtin = @import("builtin");
const wal = @import("wal.zig");
const manifest = @import("manifest.zig");
const platform = @import("platform.zig");
const cbytes = @import("comptime_bytes.zig");

pub const EngineError = error{
    InvalidConfig,
    LockHeld,
    EngineClosed,
    KeyNotFound,
    CorruptedWalRecord,
    UnsafeCheckpointPath,
    CheckpointAlreadyExists,
    UnsupportedPlatform,
    UnknownProperty,
    WriteStall,
    UnsupportedFormat,
    IncompleteCheckpoint,
};

// Runtime configuration for a database instance.
pub const Config = struct {
    path: []const u8,
    durability_mode: wal.DurabilityMode = .fdatasync,
    max_key_size: usize = 1024,
    memtable_max_bytes: usize = 64 * 1024,
    scheduler_queue_limit: usize = 64,
    slowdown_l0_files: usize = 8,
    stop_l0_files: usize = 12,
    slowdown_immutable_count: usize = 4,
    stop_immutable_count: usize = 8,
    compaction_trigger_l0_files: usize = 4,
    compaction_rate_bytes_per_sec: u64 = 8 * 1024 * 1024,
};

// Typed property payload returned by the property() inspection API.
pub const PropertyValue = union(enum) {
    u64: u64,
    text: []const u8,
};

// Materialized row returned by scan APIs.
pub const ScanItem = struct {
    key: []u8,
    value: []u8,
};

// Half-open range bounds used by general scans: [start, end).
pub const ScanBounds = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

// Small summary surfaced by the stats command.
pub const EngineStats = struct {
    key_count: usize,
    next_sequence: u64,
    wal_size_bytes: u64,
};

pub const BatchOp = struct {
    op: Op,
    key: []const u8,
    value: []const u8 = "",
};

pub const Snapshot = struct {
    seq: u64,
};

pub const ErrorClass = enum {
    retryable,
    busy,
    corruption,
    fatal,
    not_found,
};

pub fn classifyError(err: anyerror) ErrorClass {
    return switch (err) {
        EngineError.WriteStall, EngineError.LockHeld => .busy,
        EngineError.CorruptedWalRecord, EngineError.UnsupportedFormat => .corruption,
        EngineError.KeyNotFound => .not_found,
        EngineError.EngineClosed => .retryable,
        else => .fatal,
    };
}

const StallState = enum {
    normal,
    slowdown,
    stop,
};

const StallReason = enum {
    none,
    l0_pressure,
    immutable_pressure,
    scheduler_queue_full,
};

const PreWrite = enum {
    proceed,
    slowdown,
    stop,
};

const Op = enum(u8) {
    put = 1,
    delete = 2,
};

const ValueEntry = struct {
    seq: u64,
    tombstone: bool,
    value: ?[]u8,
};

const ImmutableMem = struct {
    id: u64,
    map: std.StringHashMap(ValueEntry),
    bytes: u64,
};

const latency_bucket_bounds_us = [_]u64{ 10, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 50_000, 100_000 };
const sst_format_major: u64 = 1;
const sst_format_minor: u64 = 0;
const sst_format_minor_max_compatible: u64 = 1;
const checkpoint_incomplete_marker = "CHECKPOINT_INCOMPLETE";
const checkpoint_complete_marker = "CHECKPOINT_COMPLETE";

// Fixed bucket histogram used for exported latency percentiles.
const LatencyHistogram = struct {
    buckets: [latency_bucket_bounds_us.len + 1]u64 = [_]u64{0} ** (latency_bucket_bounds_us.len + 1),
    count: u64 = 0,

    fn observeNs(self: *LatencyHistogram, latency_ns: u64) void {
        const latency_us = latency_ns / std.time.ns_per_us;
        var idx: usize = 0;
        while (idx < latency_bucket_bounds_us.len) : (idx += 1) {
            if (latency_us <= latency_bucket_bounds_us[idx]) {
                self.buckets[idx] += 1;
                self.count += 1;
                return;
            }
        }

        self.buckets[self.buckets.len - 1] += 1;
        self.count += 1;
    }

    fn percentileUs(self: *const LatencyHistogram, numerator: u64, denominator: u64) u64 {
        if (self.count == 0 or denominator == 0) return 0;

        var target = (self.count * numerator + denominator - 1) / denominator;
        if (target == 0) target = 1;

        var cumulative: u64 = 0;
        for (self.buckets, 0..) |bucket, idx| {
            cumulative += bucket;
            if (cumulative >= target) {
                if (idx < latency_bucket_bounds_us.len) return latency_bucket_bounds_us[idx];
                return latency_bucket_bounds_us[latency_bucket_bounds_us.len - 1] * 2;
            }
        }

        return latency_bucket_bounds_us[latency_bucket_bounds_us.len - 1] * 2;
    }
};

// Monotonic counters and latency summaries for observability properties.
const RuntimeMetrics = struct {
    put_count: u64 = 0,
    delete_count: u64 = 0,
    get_count: u64 = 0,
    get_hit_count: u64 = 0,
    get_miss_count: u64 = 0,
    scan_count: u64 = 0,
    wal_bytes_appended: u64 = 0,
    flush_count: u64 = 0,
    compaction_count: u64 = 0,
    compaction_rate_limited_count: u64 = 0,
    compaction_rate_sleep_ns_total: u64 = 0,
    stall_count: u64 = 0,
    write_latency_ns_total: u64 = 0,
    write_latency_ns_max: u64 = 0,
    stall_duration_ns_total: u64 = 0,
    put_latency: LatencyHistogram = .{},
    delete_latency: LatencyHistogram = .{},
    get_latency: LatencyHistogram = .{},
    scan_latency: LatencyHistogram = .{},
};

// Simple token bucket that rate-limits background compaction I/O.
const TokenBucket = struct {
    rate_bytes_per_sec: u64,
    burst: f64,
    tokens: f64,
    last_refill_ns: u64,

    fn init(rate_bytes_per_sec: u64) TokenBucket {
        const now: u64 = @intCast(std.time.nanoTimestamp());
        return .{
            .rate_bytes_per_sec = rate_bytes_per_sec,
            .burst = @as(f64, @floatFromInt(rate_bytes_per_sec)),
            .tokens = @as(f64, @floatFromInt(rate_bytes_per_sec)),
            .last_refill_ns = now,
        };
    }

    fn consume(self: *TokenBucket, bytes: u64) u64 {
        if (bytes == 0) return 0;

        const now: u64 = @intCast(std.time.nanoTimestamp());
        const elapsed_ns = now - self.last_refill_ns;
        self.last_refill_ns = now;

        const refill = (@as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s))) * @as(f64, @floatFromInt(self.rate_bytes_per_sec));
        self.tokens = @min(self.burst, self.tokens + refill);

        const need = @as(f64, @floatFromInt(bytes));
        if (self.tokens >= need) {
            self.tokens -= need;
            return 0;
        }

        const missing = need - self.tokens;
        self.tokens = 0;
        const seconds = missing / @as(f64, @floatFromInt(self.rate_bytes_per_sec));
        return @intFromFloat(seconds * @as(f64, @floatFromInt(std.time.ns_per_s)));
    }
};

// Full database runtime: open files, mutable state, scheduler queues, workers, and metrics.
pub const Engine = struct {
    allocator: std.mem.Allocator,
    dir: std.fs.Dir,
    lock_file: std.fs.File,
    wal_file: std.fs.File,
    manifest_store: manifest.ManifestStore,
    version: manifest.VersionSet,

    active_mem: std.StringHashMap(ValueEntry),
    active_mem_bytes: u64,
    immutables: std.ArrayList(ImmutableMem),
    next_mem_id: u64,

    flush_queue: std.ArrayList(u64),
    compaction_queue: std.ArrayList(u8),

    queue_limit: usize,
    next_sequence: u64,
    wal_logical_pos: u64,
    closed: bool,

    max_key_size: usize,
    durability_mode: wal.DurabilityMode,
    memtable_max_bytes: usize,
    slowdown_l0_files: usize,
    stop_l0_files: usize,
    slowdown_immutable_count: usize,
    stop_immutable_count: usize,
    compaction_trigger_l0_files: usize,

    stall_state: StallState,
    stall_reason: StallReason,

    workers_started: bool,

    flush_thread: ?std.Thread,
    flush_stop: std.atomic.Value(bool),
    flush_pending: bool,
    flush_worker_mutex: std.Thread.Mutex,
    flush_worker_cond: std.Thread.Condition,

    compaction_thread: ?std.Thread,
    compaction_stop: std.atomic.Value(bool),
    compaction_pending: bool,
    compaction_worker_mutex: std.Thread.Mutex,
    compaction_worker_cond: std.Thread.Condition,

    io_limiter: TokenBucket,
    metrics: RuntimeMetrics,
    scan_test_delay_ns: u64,

    mutex: std.Thread.Mutex,

    // Opens the database directory, acquires the single-writer lock, reloads metadata, and replays the WAL.
    pub fn open(allocator: std.mem.Allocator, config: Config) !Engine {
        if (config.path.len == 0) return EngineError.InvalidConfig;
        if (config.memtable_max_bytes == 0) return EngineError.InvalidConfig;
        if (config.scheduler_queue_limit == 0) return EngineError.InvalidConfig;
        _ = try validateCompactionRate(@as(i64, @intCast(config.compaction_rate_bytes_per_sec)));
        if (config.slowdown_l0_files > config.stop_l0_files) return EngineError.InvalidConfig;
        if (config.slowdown_immutable_count > config.stop_immutable_count) return EngineError.InvalidConfig;

        const caps = platform.capabilities();
        if (!caps.supported) return EngineError.UnsupportedPlatform;

        var dir = try std.fs.cwd().makeOpenPath(config.path, .{ .iterate = true });
        errdefer dir.close();

        const lock_file = try acquireLock(dir);
        errdefer lock_file.close();

        dir.access(checkpoint_incomplete_marker, .{}) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return EngineError.IncompleteCheckpoint,
        };
        if (dir.access(checkpoint_incomplete_marker, .{})) |_| {
            return EngineError.IncompleteCheckpoint;
        } else |_| {}

        var store = manifest.ManifestStore.init(allocator, dir);
        var version = store.loadVersionSet() catch |err| switch (err) {
            manifest.ManifestError.MissingCurrent => blk: {
                var fresh = manifest.VersionSet.init(allocator);
                try store.bootstrap(&fresh);
                break :blk fresh;
            },
            else => return err,
        };
        errdefer version.deinit();

        try validateSstSet(allocator, dir, &version);

        var active_mem = std.StringHashMap(ValueEntry).init(allocator);
        errdefer deinitMap(allocator, &active_mem);

        var max_seq_seen: u64 = 0;
        var active_bytes: u64 = 0;
        try replayWal(allocator, dir, &active_mem, &active_bytes, &max_seq_seen);

        var wal_file = try dir.createFile("wal.log", .{ .truncate = false, .read = true });
        errdefer wal_file.close();
        try wal_file.seekFromEnd(0);
        const wal_pos = try wal_file.getPos();

        if (max_seq_seen >= version.next_sequence) version.next_sequence = max_seq_seen + 1;

        const engine = Engine{
            .allocator = allocator,
            .dir = dir,
            .lock_file = lock_file,
            .wal_file = wal_file,
            .manifest_store = store,
            .version = version,
            .active_mem = active_mem,
            .active_mem_bytes = active_bytes,
            .immutables = .empty,
            .next_mem_id = 1,
            .flush_queue = .empty,
            .compaction_queue = .empty,
            .queue_limit = config.scheduler_queue_limit,
            .next_sequence = version.next_sequence,
            .wal_logical_pos = wal_pos,
            .closed = false,
            .max_key_size = config.max_key_size,
            .durability_mode = config.durability_mode,
            .memtable_max_bytes = config.memtable_max_bytes,
            .slowdown_l0_files = config.slowdown_l0_files,
            .stop_l0_files = config.stop_l0_files,
            .slowdown_immutable_count = config.slowdown_immutable_count,
            .stop_immutable_count = config.stop_immutable_count,
            .compaction_trigger_l0_files = config.compaction_trigger_l0_files,
            .stall_state = .normal,
            .stall_reason = .none,
            .workers_started = false,
            .flush_thread = null,
            .flush_stop = std.atomic.Value(bool).init(false),
            .flush_pending = false,
            .flush_worker_mutex = .{},
            .flush_worker_cond = .{},
            .compaction_thread = null,
            .compaction_stop = std.atomic.Value(bool).init(false),
            .compaction_pending = false,
            .compaction_worker_mutex = .{},
            .compaction_worker_cond = .{},
            .io_limiter = TokenBucket.init(config.compaction_rate_bytes_per_sec),
            .metrics = .{},
            .scan_test_delay_ns = 0,
            .mutex = .{},
        };

        return engine;
    }

    // Drains background work, persists the final manifest counters, and releases filesystem resources.
    pub fn close(self: *Engine) !void {
        self.mutex.lock();
        if (self.closed) {
            self.mutex.unlock();
            return;
        }
        self.closed = true;
        const started = self.workers_started;
        self.mutex.unlock();

        if (started) self.stopWorkers();

        while (true) {
            const next_mem_id = blk: {
                self.mutex.lock();
                defer self.mutex.unlock();
                if (self.immutables.items.len == 0) break :blk null;
                break :blk self.immutables.items[0].id;
            };
            const mem_id = next_mem_id orelse break;
            _ = self.flushImmutable(mem_id) catch {};
        }

        while (true) {
            const level_opt = self.popCompactionJob();
            const level = level_opt orelse break;
            _ = self.compactLevel(level) catch {};
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.durability_mode == .fdatasync) try std.posix.fdatasync(self.wal_file.handle);

        try self.manifest_store.applyEditAtomic(&self.version, .{
            .new_files = &.{},
            .deleted_files = &.{},
            .next_sequence = self.next_sequence,
            .next_file_number = self.version.next_file_number + 1,
        });

        self.wal_file.close();
        self.lock_file.close();

        deinitMap(self.allocator, &self.active_mem);
        for (self.immutables.items) |*im| deinitMap(self.allocator, &im.map);
        self.immutables.deinit(self.allocator);
        self.flush_queue.deinit(self.allocator);
        self.compaction_queue.deinit(self.allocator);
        self.version.deinit();
        self.dir.close();
    }

    // Appends a value mutation to the WAL and applies it to the active memtable.
    pub fn put(self: *Engine, key: []const u8, value: []const u8) !void {
        const one = BatchOp{ .op = .put, .key = key, .value = value };
        return self.writeBatch(&.{one});
    }

    // Appends a tombstone mutation and updates in-memory visibility state.
    pub fn delete(self: *Engine, key: []const u8) !void {
        const one = BatchOp{ .op = .delete, .key = key, .value = "" };
        return self.writeBatch(&.{one});
    }

    pub fn writeBatch(self: *Engine, ops: []const BatchOp) !void {
        if (ops.len == 0) return;
        try self.ensureWorkersStarted();

        for (ops) |op| {
            if (op.key.len > self.max_key_size) return error.KeyTooLarge;
            if (op.op == .put and op.value.len > std.math.maxInt(u32)) return EngineError.InvalidConfig;
        }

        const op_start: u64 = @intCast(std.time.nanoTimestamp());
        var slowed_once = false;

        while (true) {
            self.mutex.lock();
            var lock_held = true;
            errdefer if (lock_held) self.mutex.unlock();

            try self.ensureOpenLocked();

            const pre = self.preWriteActionLocked();
            if (pre == .stop) {
                self.metrics.stall_count += 1;
                self.mutex.unlock();
                lock_held = false;
                return EngineError.WriteStall;
            }

            if (pre == .slowdown and !slowed_once) {
                slowed_once = true;
                self.mutex.unlock();
                lock_held = false;

                const s = std.time.nanoTimestamp();
                std.Thread.sleep(1 * std.time.ns_per_ms);
                const elapsed: u64 = @intCast(std.time.nanoTimestamp() - s);

                self.mutex.lock();
                self.metrics.stall_duration_ns_total += elapsed;
                self.mutex.unlock();
                continue;
            }

            const seq_base = self.next_sequence;
            const encoded = if (ops.len == 1)
                try encodeRecord(self.allocator, ops[0].op, seq_base, ops[0].key, ops[0].value)
            else
                try encodeBatchRecord(self.allocator, seq_base, ops);
            defer self.allocator.free(encoded);

            const start = std.time.nanoTimestamp();
            try appendFramed(self, encoded);
            self.metrics.wal_bytes_appended += @as(u64, @intCast(encoded.len));

            var seq = seq_base;
            var put_count: u64 = 0;
            var del_count: u64 = 0;
            for (ops) |op| {
                try applyMutation(self, seq, op.op, op.key, op.value);
                seq += 1;
                switch (op.op) {
                    .put => put_count += 1,
                    .delete => del_count += 1,
                }
            }
            self.next_sequence = seq;

            self.metrics.put_count += put_count;
            self.metrics.delete_count += del_count;

            const elapsed_ns: u64 = @intCast(std.time.nanoTimestamp() - start);
            self.metrics.write_latency_ns_total += elapsed_ns;
            if (elapsed_ns > self.metrics.write_latency_ns_max) self.metrics.write_latency_ns_max = elapsed_ns;

            if (ops.len == 1) {
                const op_elapsed: u64 = @intCast(std.time.nanoTimestamp() - op_start);
                switch (ops[0].op) {
                    .put => self.metrics.put_latency.observeNs(op_elapsed),
                    .delete => self.metrics.delete_latency.observeNs(op_elapsed),
                }
            }

            try self.maybeRotateActiveLocked();
            self.refreshStallStateLocked();

            self.mutex.unlock();
            lock_held = false;
            return;
        }
    }

    // Reads the newest visible value for a key across mutable, immutable, and SST sources.
    pub fn get(self: *Engine, key: []const u8) ![]u8 {
        return self.getInternal(key, null, true);
    }

    pub fn beginSnapshot(self: *Engine) !Snapshot {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.ensureOpenLocked();
        const seq = if (self.next_sequence == 0) 0 else self.next_sequence - 1;
        return .{ .seq = seq };
    }

    pub fn getSnapshot(self: *Engine, s: Snapshot, key: []const u8) ![]u8 {
        return self.getInternal(key, s.seq, false);
    }

    pub fn releaseSnapshot(self: *Engine, s: Snapshot) void {
        _ = self;
        _ = s; // reserved for the future Snapshot
    }

    // Internal snapshot read used by tests and scan visibility checks.
    fn getAtSnapshot(self: *Engine, key: []const u8, snapshot_seq: u64) ![]u8 {
        return self.getInternal(key, snapshot_seq, false);
    }

    // Shared point-read path that merges memory and disk sources under one visibility rule.
    fn getInternal(self: *Engine, key: []const u8, snapshot_seq: ?u64, count_metrics: bool) ![]u8 {
        const op_start: u64 = @intCast(std.time.nanoTimestamp());

        self.mutex.lock();
        var lock_held = true;
        errdefer if (lock_held) self.mutex.unlock();

        try self.ensureOpenLocked();
        if (count_metrics) self.metrics.get_count += 1;

        const effective_snapshot_seq = snapshot_seq orelse if (self.next_sequence == 0) 0 else self.next_sequence - 1;

        var sources = std.ArrayList(ScanSource).empty;
        defer {
            for (sources.items) |*src| src.deinit(self.allocator);
            sources.deinit(self.allocator);
        }

        if (try collectMemPointSource(self.allocator, &self.active_mem, key)) |src| {
            try sources.append(self.allocator, src);
        }

        var idx = self.immutables.items.len;
        while (idx > 0) {
            idx -= 1;
            if (try collectMemPointSource(self.allocator, &self.immutables.items[idx].map, key)) |src| {
                try sources.append(self.allocator, src);
            }
        }

        var snapshot = try self.version.clone(self.allocator);
        self.mutex.unlock();
        lock_held = false;
        defer snapshot.deinit();

        const exact_end = try makeExactKeyEnd(self.allocator, key);
        defer self.allocator.free(exact_end);
        try collectDiskScanSources(self.allocator, self.dir, &snapshot, .{ .start = key, .end = exact_end }, &sources);

        const winner = selectVisibleWinnerForKey(sources.items, key, effective_snapshot_seq);
        const out = if (winner) |entry|
            if (entry.tombstone)
                null
            else
                try self.allocator.dupe(u8, entry.value orelse "")
        else
            null;

        self.mutex.lock();
        defer self.mutex.unlock();
        if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));

        if (out) |value| {
            if (count_metrics) self.metrics.get_hit_count += 1;
            return value;
        }

        if (count_metrics) self.metrics.get_miss_count += 1;
        return EngineError.KeyNotFound;
    }

    // Releases a value buffer returned by get().
    pub fn freeValue(self: *Engine, value: []u8) void {
        self.allocator.free(value);
    }

    // Scans all keys sharing a prefix by translating it into half-open bounds when possible.
    pub fn scanPrefix(self: *Engine, prefix: []const u8) ![]ScanItem {
        const prefix_end = try makePrefixEnd(self.allocator, prefix);
        defer if (prefix_end) |end| self.allocator.free(end);

        if (prefix_end == null and prefix.len > 0) {
            const rows = try self.scan(.{ .start = prefix });
            errdefer self.freeScan(rows);

            var out = std.ArrayList(ScanItem).empty;
            errdefer {
                for (out.items) |it| {
                    self.allocator.free(it.key);
                    self.allocator.free(it.value);
                }
                out.deinit(self.allocator);
            }

            for (rows) |row| {
                if (!std.mem.startsWith(u8, row.key, prefix)) continue;
                try out.append(self.allocator, .{
                    .key = try self.allocator.dupe(u8, row.key),
                    .value = try self.allocator.dupe(u8, row.value),
                });
            }
            self.freeScan(rows);
            return out.toOwnedSlice(self.allocator);
        }

        return self.scan(.{
            .start = prefix,
            .end = prefix_end,
        });
    }

    // Produces a snapshot-consistent ordered range scan across memory and SST sources.
    pub fn scan(self: *Engine, bounds: ScanBounds) ![]ScanItem {
        if (bounds.start) |start| {
            if (bounds.end) |end| {
                if (std.mem.order(u8, start, end) == .gt) return EngineError.InvalidConfig;
            }
        }

        const op_start: u64 = @intCast(std.time.nanoTimestamp());
        self.mutex.lock();
        try self.ensureOpenLocked();
        self.metrics.scan_count += 1;

        var sources = std.ArrayList(ScanSource).empty;
        errdefer {
            for (sources.items) |*src| src.deinit(self.allocator);
            sources.deinit(self.allocator);
        }

        if (try collectMemScanSource(self.allocator, &self.active_mem, bounds)) |src| {
            try sources.append(self.allocator, src);
        }
        for (self.immutables.items) |*im| {
            if (try collectMemScanSource(self.allocator, &im.map, bounds)) |src| {
                try sources.append(self.allocator, src);
            }
        }

        var snapshot = try self.version.clone(self.allocator);
        const snapshot_seq = if (self.next_sequence == 0) 0 else self.next_sequence - 1;
        self.mutex.unlock();
        defer snapshot.deinit();

        if (builtin.is_test and self.scan_test_delay_ns > 0) {
            std.Thread.sleep(self.scan_test_delay_ns);
        }

        try collectDiskScanSources(self.allocator, self.dir, &snapshot, bounds, &sources);

        var out = std.ArrayList(ScanItem).empty;
        errdefer {
            for (out.items) |it| {
                self.allocator.free(it.key);
                self.allocator.free(it.value);
            }
            out.deinit(self.allocator);
        }

        while (pickNextScanKey(sources.items)) |next_key| {
            if (selectVisibleWinnerForKey(sources.items, next_key, snapshot_seq)) |winner| {
                if (!winner.tombstone) {
                    try out.append(self.allocator, .{
                        .key = try self.allocator.dupe(u8, winner.key),
                        .value = try self.allocator.dupe(u8, winner.value orelse ""),
                    });
                }
            }

            for (sources.items) |*src| {
                const entry = src.current() orelse continue;
                if (!std.mem.eql(u8, entry.key, next_key)) continue;
                try src.advance();
            }
        }

        for (sources.items) |*src| src.deinit(self.allocator);
        sources.deinit(self.allocator);

        const result = try out.toOwnedSlice(self.allocator);
        self.mutex.lock();
        self.metrics.scan_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
        self.mutex.unlock();
        return result;
    }

    // Releases a result set returned by scan or scanPrefix.
    pub fn freeScan(self: *Engine, items: []ScanItem) void {
        for (items) |it| {
            self.allocator.free(it.key);
            self.allocator.free(it.value);
        }
        self.allocator.free(items);
    }

    // Computes a small live-state summary without reading every SST from disk.
    pub fn stats(self: *Engine) !EngineStats {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.ensureOpenLocked();

        const stat = try self.dir.statFile("wal.log");

        var live = std.StringHashMap(ValueEntry).init(self.allocator);
        defer deinitMap(self.allocator, &live);

        try mergeMapByPrefix(self.allocator, &live, &self.active_mem, "");
        for (self.immutables.items) |*im| try mergeMapByPrefix(self.allocator, &live, &im.map, "");

        var count: usize = 0;
        var it = live.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.tombstone) count += 1;
        }

        return .{ .key_count = count, .next_sequence = self.next_sequence, .wal_size_bytes = stat.size };
    }

    // Returns one typed diagnostic property or metric by name.
    pub fn property(self: *Engine, name: []const u8) !PropertyValue {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.ensureOpenLocked();

        const caps = platform.capabilities();
        if (std.mem.eql(u8, name, "engine.platform.os")) return .{ .text = @tagName(caps.os_tag) };
        if (std.mem.eql(u8, name, "engine.platform.arch")) return .{ .text = @tagName(caps.arch) };
        if (std.mem.eql(u8, name, "engine.platform.tier")) return .{ .text = @tagName(caps.tier) };
        if (std.mem.eql(u8, name, "engine.platform.pointer_bits")) return .{ .u64 = caps.pointer_bits };
        if (std.mem.eql(u8, name, "engine.platform.supported")) return .{ .u64 = @intFromBool(caps.supported) };
        if (std.mem.eql(u8, name, "engine.platform.checkpoint_atomic_noreplace")) return .{ .u64 = @intFromBool(caps.has_linux_renameat2_noreplace) };
        if (std.mem.eql(u8, name, "engine.platform.checkpoint_portable_reservation")) return .{ .u64 = @intFromBool(!caps.has_linux_renameat2_noreplace and caps.has_posix_nofollow_open) };
        if (std.mem.eql(u8, name, "engine.platform.direct_io_path")) return .{ .u64 = @intFromBool(caps.has_direct_io_path) };

        if (std.mem.eql(u8, name, "engine.memtable.bytes")) {
            var total = self.active_mem_bytes;
            for (self.immutables.items) |im| total += im.bytes;
            return .{ .u64 = total };
        }
        if (std.mem.eql(u8, name, "engine.l0.files")) return .{ .u64 = self.version.levels[0].items.len };
        if (std.mem.eql(u8, name, "engine.pending_compaction_bytes")) return .{ .u64 = pendingCompactionBytesLocked(self) };
        if (std.mem.eql(u8, name, "engine.compaction.rate_limit.bytes_per_sec")) return .{ .u64 = self.io_limiter.rate_bytes_per_sec };
        if (std.mem.eql(u8, name, "engine.cache.usage_bytes")) return .{ .u64 = 0 };
        if (std.mem.eql(u8, name, "engine.stall.state")) return .{ .text = @tagName(self.stall_state) };
        if (std.mem.eql(u8, name, "engine.stall.reason")) return .{ .text = @tagName(self.stall_reason) };
        if (std.mem.eql(u8, name, "engine.scheduler.flush_queue_depth")) return .{ .u64 = self.flush_queue.items.len };
        if (std.mem.eql(u8, name, "engine.scheduler.compaction_queue_depth")) return .{ .u64 = self.compaction_queue.items.len };

        if (std.mem.eql(u8, name, "engine.metrics.put.count")) return .{ .u64 = self.metrics.put_count };
        if (std.mem.eql(u8, name, "engine.metrics.delete.count")) return .{ .u64 = self.metrics.delete_count };
        if (std.mem.eql(u8, name, "engine.metrics.get.count")) return .{ .u64 = self.metrics.get_count };
        if (std.mem.eql(u8, name, "engine.metrics.get.hit.count")) return .{ .u64 = self.metrics.get_hit_count };
        if (std.mem.eql(u8, name, "engine.metrics.get.miss.count")) return .{ .u64 = self.metrics.get_miss_count };
        if (std.mem.eql(u8, name, "engine.metrics.scan.count")) return .{ .u64 = self.metrics.scan_count };
        if (std.mem.eql(u8, name, "engine.metrics.wal.bytes")) return .{ .u64 = self.metrics.wal_bytes_appended };
        if (std.mem.eql(u8, name, "engine.metrics.flush.count")) return .{ .u64 = self.metrics.flush_count };
        if (std.mem.eql(u8, name, "engine.metrics.compaction.count")) return .{ .u64 = self.metrics.compaction_count };
        if (std.mem.eql(u8, name, "engine.metrics.compaction.rate_limited.count")) return .{ .u64 = self.metrics.compaction_rate_limited_count };
        if (std.mem.eql(u8, name, "engine.metrics.compaction.rate_limit.sleep_ms")) return .{ .u64 = self.metrics.compaction_rate_sleep_ns_total / std.time.ns_per_ms };
        if (std.mem.eql(u8, name, "engine.metrics.stall.count")) return .{ .u64 = self.metrics.stall_count };
        if (std.mem.eql(u8, name, "engine.metrics.stall.duration_ms")) return .{ .u64 = self.metrics.stall_duration_ns_total / std.time.ns_per_ms };
        if (std.mem.eql(u8, name, "engine.metrics.write_latency.max_us")) return .{ .u64 = self.metrics.write_latency_ns_max / std.time.ns_per_us };
        if (std.mem.eql(u8, name, "engine.metrics.write_latency.avg_us")) {
            const writes = self.metrics.put_count + self.metrics.delete_count;
            if (writes == 0) return .{ .u64 = 0 };
            return .{ .u64 = (self.metrics.write_latency_ns_total / writes) / std.time.ns_per_us };
        }

        if (std.mem.eql(u8, name, "engine.metrics.latency.put.p50_us")) return .{ .u64 = self.metrics.put_latency.percentileUs(50, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.put.p95_us")) return .{ .u64 = self.metrics.put_latency.percentileUs(95, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.put.p99_us")) return .{ .u64 = self.metrics.put_latency.percentileUs(99, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.delete.p50_us")) return .{ .u64 = self.metrics.delete_latency.percentileUs(50, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.delete.p95_us")) return .{ .u64 = self.metrics.delete_latency.percentileUs(95, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.delete.p99_us")) return .{ .u64 = self.metrics.delete_latency.percentileUs(99, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.get.p50_us")) return .{ .u64 = self.metrics.get_latency.percentileUs(50, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.get.p95_us")) return .{ .u64 = self.metrics.get_latency.percentileUs(95, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.get.p99_us")) return .{ .u64 = self.metrics.get_latency.percentileUs(99, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.scan.p50_us")) return .{ .u64 = self.metrics.scan_latency.percentileUs(50, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.scan.p95_us")) return .{ .u64 = self.metrics.scan_latency.percentileUs(95, 100) };
        if (std.mem.eql(u8, name, "engine.metrics.latency.scan.p99_us")) return .{ .u64 = self.metrics.scan_latency.percentileUs(99, 100) };

        return EngineError.UnknownProperty;
    }

    // Copies the current on-disk state into a fresh checkpoint directory after path safety checks.
    pub fn checkpoint(self: *Engine, path: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.ensureOpenLocked();
        if (path.len == 0) return EngineError.InvalidConfig;
        if (containsParentTraversal(path)) return EngineError.UnsafeCheckpointPath;

        const dst_name = std.fs.path.basename(path);
        if (dst_name.len == 0 or std.mem.eql(u8, dst_name, ".") or std.mem.eql(u8, dst_name, "..")) return EngineError.InvalidConfig;

        const parent_path = std.fs.path.dirname(path) orelse ".";
        var parent = try openDirNoSymlinkChain(self.allocator, parent_path);
        defer parent.close();

        const caps = platform.capabilities();
        if (!caps.has_linux_renameat2_noreplace) {
            try createPortableCheckpoint(self.allocator, self.dir, parent, dst_name);
            return;
        }

        const tmp_name = try std.fmt.allocPrint(self.allocator, ".ziggy-checkpoint-{d}", .{std.time.microTimestamp()});
        defer self.allocator.free(tmp_name);

        try parent.makeDir(tmp_name);
        errdefer parent.deleteTree(tmp_name) catch {};

        var tmp_dst = try parent.openDir(tmp_name, .{});
        defer tmp_dst.close();

        try copyFile(self.allocator, self.dir, tmp_dst, "wal.log");
        try copyFile(self.allocator, self.dir, tmp_dst, "CURRENT");

        var it = self.dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .file) continue;
            const is_manifest = std.mem.startsWith(u8, entry.name, "MANIFEST-");
            const is_sst = std.mem.startsWith(u8, entry.name, "sst-") and std.mem.endsWith(u8, entry.name, ".sst");
            if (!is_manifest and !is_sst) continue;
            try copyFile(self.allocator, self.dir, tmp_dst, entry.name);
        }

        try renameNoReplace(self.allocator, parent, tmp_name, dst_name);
    }

    // Validates manifest readability and WAL frame integrity for operator diagnostics.
    pub fn doctor(self: *Engine) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.ensureOpenLocked();

        var loaded = try self.manifest_store.loadVersionSet();
        loaded.deinit();

        const wal_bytes = self.dir.readFileAlloc(self.allocator, "wal.log", 128 * 1024 * 1024) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer self.allocator.free(wal_bytes);

        const records = wal.readAllRecords(self.allocator, wal_bytes) catch return EngineError.CorruptedWalRecord;
        defer {
            for (records) |record| self.allocator.free(record);
            self.allocator.free(records);
        }

        for (records) |record| _ = decodeRecord(record) catch return EngineError.CorruptedWalRecord;
    }

    // Fails fast when an API is used after close().
    fn ensureOpenLocked(self: *Engine) !void {
        if (self.closed) return EngineError.EngineClosed;
    }

    // Lazily starts the flush and compaction threads on the first write.
    fn ensureWorkersStarted(self: *Engine) !void {
        self.mutex.lock();
        if (self.workers_started) {
            self.mutex.unlock();
            return;
        }
        self.workers_started = true;
        self.mutex.unlock();

        self.flush_stop.store(false, .release);
        errdefer {
            self.flush_stop.store(true, .release);
            if (self.flush_thread) |t| t.join();
            self.flush_thread = null;
            self.mutex.lock();
            self.workers_started = false;
            self.mutex.unlock();
        }
        self.flush_thread = try std.Thread.spawn(.{}, flushWorkerMain, .{self});

        self.compaction_stop.store(false, .release);
        self.compaction_thread = try std.Thread.spawn(.{}, compactionWorkerMain, .{self});
    }

    // Signals both background workers, wakes them, and joins their threads.
    fn stopWorkers(self: *Engine) void {
        self.flush_stop.store(true, .release);
        self.compaction_stop.store(true, .release);

        self.flush_worker_mutex.lock();
        self.flush_pending = true;
        self.flush_worker_cond.broadcast();
        self.flush_worker_mutex.unlock();

        self.compaction_worker_mutex.lock();
        self.compaction_pending = true;
        self.compaction_worker_cond.broadcast();
        self.compaction_worker_mutex.unlock();

        if (self.flush_thread) |t| {
            t.join();
            self.flush_thread = null;
        }
        if (self.compaction_thread) |t| {
            t.join();
            self.compaction_thread = null;
        }

        self.mutex.lock();
        self.workers_started = false;
        self.mutex.unlock();
    }

    // Marks flush work as pending and wakes the flush worker.
    fn signalFlushWorker(self: *Engine) void {
        self.flush_worker_mutex.lock();
        self.flush_pending = true;
        self.flush_worker_cond.signal();
        self.flush_worker_mutex.unlock();
    }

    // Marks compaction work as pending and wakes the compaction worker.
    fn signalCompactionWorker(self: *Engine) void {
        self.compaction_worker_mutex.lock();
        self.compaction_pending = true;
        self.compaction_worker_cond.signal();
        self.compaction_worker_mutex.unlock();
    }

    // Queues one immutable memtable for flush and enforces scheduler backpressure.
    fn enqueueFlushLocked(self: *Engine, mem_id: u64) !void {
        if (self.flush_queue.items.len >= self.queue_limit) {
            self.stall_state = .stop;
            self.stall_reason = .scheduler_queue_full;
            return EngineError.WriteStall;
        }
        try self.flush_queue.append(self.allocator, mem_id);
        self.signalFlushWorker();
    }

    // Adds at most one pending compaction job per level.
    fn enqueueCompactionLocked(self: *Engine, level: u8) !void {
        if (self.compaction_queue.items.len >= self.queue_limit) return;
        for (self.compaction_queue.items) |existing| if (existing == level) return;
        try self.compaction_queue.append(self.allocator, level);
        self.signalCompactionWorker();
    }

    // Swaps the active memtable into the immutable queue once it crosses the byte budget.
    fn maybeRotateActiveLocked(self: *Engine) !void {
        if (self.active_mem_bytes < self.memtable_max_bytes) return;

        const old_map = self.active_mem;
        self.active_mem = std.StringHashMap(ValueEntry).init(self.allocator);

        const imm = ImmutableMem{ .id = self.next_mem_id, .map = old_map, .bytes = self.active_mem_bytes };
        self.next_mem_id += 1;
        self.active_mem_bytes = 0;
        try self.immutables.append(self.allocator, imm);

        try self.enqueueFlushLocked(imm.id);
    }

    // Computes whether the next write should proceed, sleep briefly, or fail with backpressure.
    fn preWriteActionLocked(self: *Engine) PreWrite {
        self.refreshStallStateLocked();
        return switch (self.stall_state) {
            .normal => .proceed,
            .slowdown => .slowdown,
            .stop => .stop,
        };
    }

    // Re-evaluates write stall state from queue depth, immutable count, and L0 pressure.
    fn refreshStallStateLocked(self: *Engine) void {
        if (self.flush_queue.items.len >= self.queue_limit or self.compaction_queue.items.len >= self.queue_limit) {
            self.stall_state = .stop;
            self.stall_reason = .scheduler_queue_full;
            return;
        }

        const l0_files = self.version.levels[0].items.len;
        if (l0_files >= self.stop_l0_files) {
            self.stall_state = .stop;
            self.stall_reason = .l0_pressure;
            return;
        }
        if (self.immutables.items.len >= self.stop_immutable_count) {
            self.stall_state = .stop;
            self.stall_reason = .immutable_pressure;
            return;
        }

        if (l0_files >= self.slowdown_l0_files) {
            self.stall_state = .slowdown;
            self.stall_reason = .l0_pressure;
            return;
        }
        if (self.immutables.items.len >= self.slowdown_immutable_count) {
            self.stall_state = .slowdown;
            self.stall_reason = .immutable_pressure;
            return;
        }

        self.stall_state = .normal;
        self.stall_reason = .none;
    }

    // Pops the oldest queued immutable memtable flush job.
    fn popFlushJob(self: *Engine) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.flush_queue.items.len == 0) return null;
        return self.flush_queue.orderedRemove(0);
    }

    // Pops the oldest queued compaction level.
    fn popCompactionJob(self: *Engine) ?u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.compaction_queue.items.len == 0) return null;
        return self.compaction_queue.orderedRemove(0);
    }

    // Background loop that drains immutable memtable flush requests.
    fn flushWorkerMain(self: *Engine) void {
        while (true) {
            if (self.popFlushJob()) |id| {
                _ = self.flushImmutable(id) catch {};
                continue;
            }

            if (self.flush_stop.load(.acquire)) break;

            self.flush_worker_mutex.lock();
            while (!self.flush_stop.load(.acquire) and !self.flush_pending) {
                _ = self.flush_worker_cond.timedWait(&self.flush_worker_mutex, 50 * std.time.ns_per_ms) catch {};
            }
            self.flush_pending = false;
            self.flush_worker_mutex.unlock();
        }
    }

    // Background loop that drains queued compaction requests.
    fn compactionWorkerMain(self: *Engine) void {
        while (true) {
            if (self.popCompactionJob()) |level| {
                _ = self.compactLevel(level) catch {};
                continue;
            }

            if (self.compaction_stop.load(.acquire)) break;

            self.compaction_worker_mutex.lock();
            while (!self.compaction_stop.load(.acquire) and !self.compaction_pending) {
                _ = self.compaction_worker_cond.timedWait(&self.compaction_worker_mutex, 50 * std.time.ns_per_ms) catch {};
            }
            self.compaction_pending = false;
            self.compaction_worker_mutex.unlock();
        }
    }

    // Converts one immutable memtable into an L0 SST and records it in the manifest.
    fn flushImmutable(self: *Engine, mem_id: u64) !void {
        var imm_opt: ?ImmutableMem = null;

        self.mutex.lock();
        var i: usize = 0;
        while (i < self.immutables.items.len) : (i += 1) {
            if (self.immutables.items[i].id == mem_id) {
                imm_opt = self.immutables.orderedRemove(i);
                break;
            }
        }
        self.mutex.unlock();

        var imm = imm_opt orelse return;
        defer deinitMap(self.allocator, &imm.map);

        if (imm.map.count() == 0) return;

        const new_file_number = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            const n = self.version.next_file_number;
            self.version.next_file_number += 1;
            break :blk n;
        };

        const meta = try writeSstFromMap(self, 0, new_file_number, &imm.map);
        defer {
            self.allocator.free(meta.min_key);
            self.allocator.free(meta.max_key);
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        try self.manifest_store.applyEditAtomic(&self.version, .{
            .new_files = &.{meta},
            .deleted_files = &.{},
            .next_sequence = self.next_sequence,
            .next_file_number = self.version.next_file_number,
        });

        self.metrics.flush_count += 1;
        if (self.version.levels[0].items.len >= self.compaction_trigger_l0_files) {
            self.enqueueCompactionLocked(0) catch {};
        }
        self.refreshStallStateLocked();
    }

    // Compacts all current L0 files into one L1 file, honoring the I/O rate limiter.
    fn compactLevel(self: *Engine, level: u8) !void {
        if (level != 0) return;

        var files = std.ArrayList(manifest.FileMeta).empty;
        defer {
            for (files.items) |f| {
                self.allocator.free(f.min_key);
                self.allocator.free(f.max_key);
            }
            files.deinit(self.allocator);
        }

        self.mutex.lock();
        if (self.version.levels[0].items.len < 2) {
            self.mutex.unlock();
            return;
        }
        for (self.version.levels[0].items) |f| try files.append(self.allocator, try f.clone(self.allocator));
        self.mutex.unlock();

        var merged = std.StringHashMap(ValueEntry).init(self.allocator);
        defer deinitMap(self.allocator, &merged);

        for (files.items) |f| {
            const sleep_ns = self.io_limiter.consume(f.size);
            if (sleep_ns > 0) {
                std.Thread.sleep(sleep_ns);
                self.mutex.lock();
                self.metrics.compaction_rate_limited_count += 1;
                self.metrics.compaction_rate_sleep_ns_total += sleep_ns;
                self.mutex.unlock();
            }

            const entries = try readSstEntries(self.allocator, self.dir, f.file_number);
            defer {
                for (entries) |entry| {
                    self.allocator.free(entry.key);
                    if (entry.value) |v| self.allocator.free(v);
                }
                self.allocator.free(entries);
            }

            for (entries) |entry| try mergeEntry(self.allocator, &merged, entry.key, entry);
        }

        const new_file_number = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            const n = self.version.next_file_number;
            self.version.next_file_number += 1;
            break :blk n;
        };

        const new_meta = try writeSstFromMap(self, 1, new_file_number, &merged);
        defer {
            self.allocator.free(new_meta.min_key);
            self.allocator.free(new_meta.max_key);
        }

        var deletes = std.ArrayList(manifest.DeletedFile).empty;
        defer deletes.deinit(self.allocator);
        for (files.items) |f| try deletes.append(self.allocator, .{ .level = 0, .file_number = f.file_number });

        self.mutex.lock();
        defer self.mutex.unlock();

        try self.manifest_store.applyEditAtomic(&self.version, .{
            .new_files = &.{new_meta},
            .deleted_files = deletes.items,
            .next_sequence = self.next_sequence,
            .next_file_number = self.version.next_file_number,
        });

        for (files.items) |f| deleteSstFile(self.dir, f.file_number);
        self.metrics.compaction_count += 1;
        self.refreshStallStateLocked();
    }
};

// One decoded record read from an SST file.
const SstEntry = struct {
    key: []u8,
    seq: u64,
    tombstone: bool,
    value: ?[]u8,
};

// Unified scan record shape shared by memory and SST scan sources.
const ScanEntry = struct {
    key: []u8,
    seq: u64,
    tombstone: bool,
    value: ?[]u8,
};

// Eager in-memory scan source backed by a sorted slice.
const MemScanSource = struct {
    entries: []ScanEntry,
    index: usize = 0,

    // Returns the current in-memory scan row, if any.
    fn current(self: *const MemScanSource) ?*const ScanEntry {
        if (self.index >= self.entries.len) return null;
        return &self.entries[self.index];
    }

    // Moves to the next in-memory scan row.
    fn advance(self: *MemScanSource) void {
        if (self.index < self.entries.len) self.index += 1;
    }

    // Frees all key/value buffers owned by the materialized memory scan source.
    fn deinit(self: *MemScanSource, allocator: std.mem.Allocator) void {
        for (self.entries) |entry| {
            allocator.free(entry.key);
            if (entry.value) |v| allocator.free(v);
        }
        allocator.free(self.entries);
    }
};

const max_sst_scan_line_bytes: usize = 4 * 1024 * 1024;

// Streaming SST scan source that advances line-by-line within requested bounds.
const SstScanSource = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    reader: std.fs.File.Reader,
    read_buf: [4096]u8,
    bounds: ScanBounds,
    done: bool = false,
    current_entry: ?ScanEntry = null,

    // Opens one SST file, validates its header, and positions at the first in-range entry.
    fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, file_number: u64, bounds: ScanBounds) !SstScanSource {
        var name_buf: [64]u8 = undefined;
        const file_name = try std.fmt.bufPrint(&name_buf, "sst-{d}.sst", .{file_number});
        const file = try dir.openFile(file_name, .{});

        var out = SstScanSource{
            .allocator = allocator,
            .file = file,
            .reader = undefined,
            .read_buf = undefined,
            .bounds = bounds,
        };
        out.reader = out.file.reader(&out.read_buf);

        const header_line = (try out.reader.interface.takeDelimiter('\n')) orelse return EngineError.UnsupportedFormat;
        if (header_line.len > max_sst_scan_line_bytes) return error.StreamTooLong;
        try validateSstHeaderLine(header_line);

        try out.advance();
        return out;
    }

    // Returns the currently buffered SST entry.
    fn current(self: *const SstScanSource) ?*const ScanEntry {
        if (self.done) return null;
        return if (self.current_entry) |*entry| entry else null;
    }

    // Streams forward until it finds the next entry that falls inside the scan bounds.
    fn advance(self: *SstScanSource) !void {
        self.clearCurrent();
        if (self.done) return;

        while (true) {
            const line = (try self.reader.interface.takeDelimiter('\n')) orelse {
                self.done = true;
                return;
            };
            if (line.len == 0) continue;

            var tok = std.mem.splitScalar(u8, line, '\t');
            const seq_txt = tok.next() orelse continue;
            const tomb_txt = tok.next() orelse continue;
            const key_txt = tok.next() orelse continue;
            const val_txt = tok.next() orelse "";

            if (self.bounds.start) |start| {
                if (std.mem.order(u8, key_txt, start) == .lt) continue;
            }
            if (self.bounds.end) |end| {
                if (std.mem.order(u8, key_txt, end) != .lt) {
                    self.done = true;
                    return;
                }
            }

            const tombstone = std.mem.eql(u8, tomb_txt, "1");
            self.current_entry = .{
                .key = try self.allocator.dupe(u8, key_txt),
                .seq = try std.fmt.parseUnsigned(u64, seq_txt, 10),
                .tombstone = tombstone,
                .value = if (tombstone) null else try self.allocator.dupe(u8, val_txt),
            };
            return;
        }
    }

    // Releases the currently buffered SST entry before moving on.
    fn clearCurrent(self: *SstScanSource) void {
        if (self.current_entry) |entry| {
            self.allocator.free(entry.key);
            if (entry.value) |v| self.allocator.free(v);
            self.current_entry = null;
        }
    }

    // Frees the buffered entry and closes the SST file handle.
    fn deinit(self: *SstScanSource) void {
        self.clearCurrent();
        self.file.close();
    }
};

// Polymorphic scan source used by the merge-style scan implementation.
const ScanSource = union(enum) {
    mem: MemScanSource,
    sst: SstScanSource,

    // Returns the current item regardless of whether the source is memory-backed or file-backed.
    fn current(self: *const ScanSource) ?*const ScanEntry {
        return switch (self.*) {
            .mem => |*src| src.current(),
            .sst => |*src| src.current(),
        };
    }

    // Advances the active source by one logical row.
    fn advance(self: *ScanSource) !void {
        switch (self.*) {
            .mem => |*src| src.advance(),
            .sst => |*src| try src.advance(),
        }
    }

    // Releases resources owned by the active scan source variant.
    fn deinit(self: *ScanSource, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .mem => |*src| src.deinit(allocator),
            .sst => |*src| src.deinit(),
        }
    }
};

const Decoded = struct {
    seq: u64,
    op: Op,
    key: []const u8,
    value: []const u8,
};

// Reads the raw latest value entry for a key from one in-memory map.
fn lookupMap(map: *std.StringHashMap(ValueEntry), key: []const u8) ?ValueEntry {
    return map.get(key);
}

// Applies snapshot filtering on top of lookupMap.
fn lookupVisibleMap(map: *std.StringHashMap(ValueEntry), key: []const u8, snapshot_seq: ?u64) ?ValueEntry {
    const entry = lookupMap(map, key) orelse return null;
    if (snapshot_seq) |snap| {
        if (entry.seq > snap) return null;
    }
    return entry;
}

// Converts an internal value entry into an owned API return buffer unless it is tombstoned.
fn entryToGetResult(self: *Engine, entry: ValueEntry) ?[]u8 {
    if (entry.tombstone) return null;
    return self.allocator.dupe(u8, entry.value orelse "") catch null;
}

// Keeps only the newest sequence for each key while merging SST contents.
fn mergeEntry(allocator: std.mem.Allocator, map: *std.StringHashMap(ValueEntry), key: []const u8, entry: SstEntry) !void {
    if (map.getPtr(key)) |existing| {
        if (entry.seq <= existing.seq) return;
        if (existing.value) |v| allocator.free(v);
        existing.seq = entry.seq;
        existing.tombstone = entry.tombstone;
        if (entry.value) |v| {
            existing.value = try allocator.dupe(u8, v);
        } else {
            existing.value = null;
        }
        return;
    }

    const key_copy = try allocator.dupe(u8, key);
    errdefer allocator.free(key_copy);

    try map.put(key_copy, .{
        .seq = entry.seq,
        .tombstone = entry.tombstone,
        .value = if (entry.value) |v| try allocator.dupe(u8, v) else null,
    });
}

// Merges visible in-memory entries matching a prefix while keeping the newest sequence per key.
fn mergeMapByPrefix(allocator: std.mem.Allocator, dst: *std.StringHashMap(ValueEntry), src: *std.StringHashMap(ValueEntry), prefix: []const u8) !void {
    var it = src.iterator();
    while (it.next()) |entry| {
        if (!std.mem.startsWith(u8, entry.key_ptr.*, prefix)) continue;
        if (dst.getPtr(entry.key_ptr.*)) |existing| {
            if (entry.value_ptr.seq <= existing.seq) continue;
            if (existing.value) |v| allocator.free(v);
            existing.seq = entry.value_ptr.seq;
            existing.tombstone = entry.value_ptr.tombstone;
            existing.value = if (entry.value_ptr.value) |v| try allocator.dupe(u8, v) else null;
            continue;
        }

        const key_copy = try allocator.dupe(u8, entry.key_ptr.*);
        errdefer allocator.free(key_copy);
        try dst.put(key_copy, .{
            .seq = entry.value_ptr.seq,
            .tombstone = entry.value_ptr.tombstone,
            .value = if (entry.value_ptr.value) |v| try allocator.dupe(u8, v) else null,
        });
    }
}

// Merges in-memory entries that fall inside explicit scan bounds.
fn mergeMapByBounds(allocator: std.mem.Allocator, dst: *std.StringHashMap(ValueEntry), src: *std.StringHashMap(ValueEntry), bounds: ScanBounds) !void {
    var it = src.iterator();
    while (it.next()) |entry| {
        if (!keyInBounds(entry.key_ptr.*, bounds)) continue;
        if (dst.getPtr(entry.key_ptr.*)) |existing| {
            if (entry.value_ptr.seq <= existing.seq) continue;
            if (existing.value) |v| allocator.free(v);
            existing.seq = entry.value_ptr.seq;
            existing.tombstone = entry.value_ptr.tombstone;
            existing.value = if (entry.value_ptr.value) |v| try allocator.dupe(u8, v) else null;
            continue;
        }

        const key_copy = try allocator.dupe(u8, entry.key_ptr.*);
        errdefer allocator.free(key_copy);
        try dst.put(key_copy, .{
            .seq = entry.value_ptr.seq,
            .tombstone = entry.value_ptr.tombstone,
            .value = if (entry.value_ptr.value) |v| try allocator.dupe(u8, v) else null,
        });
    }
}

// Materializes one memtable map into a sorted scan source restricted to the requested bounds.
fn collectMemScanSource(allocator: std.mem.Allocator, map: *std.StringHashMap(ValueEntry), bounds: ScanBounds) !?ScanSource {
    var list = std.ArrayList(ScanEntry).empty;
    errdefer {
        for (list.items) |entry| {
            allocator.free(entry.key);
            if (entry.value) |v| allocator.free(v);
        }
        list.deinit(allocator);
    }

    var it = map.iterator();
    while (it.next()) |entry| {
        if (!keyInBounds(entry.key_ptr.*, bounds)) continue;
        try list.append(allocator, .{
            .key = try allocator.dupe(u8, entry.key_ptr.*),
            .seq = entry.value_ptr.seq,
            .tombstone = entry.value_ptr.tombstone,
            .value = if (entry.value_ptr.value) |v| try allocator.dupe(u8, v) else null,
        });
    }

    if (list.items.len == 0) {
        list.deinit(allocator);
        return null;
    }

    std.mem.sort(ScanEntry, list.items, {}, struct {
        fn lessThan(_: void, a: ScanEntry, b: ScanEntry) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.lessThan);

    return .{ .mem = .{ .entries = try list.toOwnedSlice(allocator) } };
}

// Builds a single-entry scan source for exact key lookups from an in-memory map.
fn collectMemPointSource(allocator: std.mem.Allocator, map: *std.StringHashMap(ValueEntry), key: []const u8) !?ScanSource {
    const entry = map.get(key) orelse return null;

    const key_copy = try allocator.dupe(u8, key);
    errdefer allocator.free(key_copy);

    const value_copy = if (entry.value) |v| try allocator.dupe(u8, v) else null;
    errdefer if (value_copy) |v| allocator.free(v);

    const one = try allocator.alloc(ScanEntry, 1);
    one[0] = .{
        .key = key_copy,
        .seq = entry.seq,
        .tombstone = entry.tombstone,
        .value = value_copy,
    };

    return .{ .mem = .{ .entries = one } };
}

// Returns whether a sequence number is visible to the chosen snapshot.
fn entryVisibleAtSnapshot(seq: u64, snapshot_seq: u64) bool {
    return seq <= snapshot_seq;
}

// Chooses the highest-sequence visible version of one key across all active scan sources.
fn selectVisibleWinnerForKey(sources: []ScanSource, key: []const u8, snapshot_seq: u64) ?*const ScanEntry {
    var best: ?*const ScanEntry = null;
    for (sources) |*src| {
        const entry = src.current() orelse continue;
        if (!std.mem.eql(u8, entry.key, key)) continue;
        if (!entryVisibleAtSnapshot(entry.seq, snapshot_seq)) continue;
        if (best == null or entry.seq > best.?.seq) {
            best = entry;
        }
    }
    return best;
}

// Produces a half-open upper bound that captures exactly one key in scan space.
fn makeExactKeyEnd(allocator: std.mem.Allocator, key: []const u8) ![]u8 {
    var out = try allocator.alloc(u8, key.len + 1);
    @memcpy(out[0..key.len], key);
    out[key.len] = 0;
    return out;
}

// Computes the smallest exclusive upper bound for a byte-string prefix.
fn makePrefixEnd(allocator: std.mem.Allocator, prefix: []const u8) !?[]u8 {
    if (prefix.len == 0) return null;

    var i = prefix.len;
    while (i > 0) {
        i -= 1;
        if (prefix[i] == 0xFF) continue;

        var out = try allocator.alloc(u8, i + 1);
        @memcpy(out[0..i], prefix[0..i]);
        out[i] = prefix[i] + 1;
        return out;
    }

    return null;
}

// Opens scan sources only for SST files whose key ranges overlap the requested bounds.
fn collectDiskScanSources(allocator: std.mem.Allocator, dir: std.fs.Dir, version: *manifest.VersionSet, bounds: ScanBounds, out: *std.ArrayList(ScanSource)) !void {
    var l0_idx = version.levels[0].items.len;
    while (l0_idx > 0) {
        l0_idx -= 1;
        const f = version.levels[0].items[l0_idx];
        if (!fileOverlapsBounds(bounds, f.min_key, f.max_key)) continue;
        const src = SstScanSource.init(allocator, dir, f.file_number, bounds) catch continue;
        if (src.current() == null) {
            var tmp = src;
            tmp.deinit();
            continue;
        }
        try out.append(allocator, .{ .sst = src });
    }

    var level: usize = 1;
    while (level < manifest.MAX_LEVELS) : (level += 1) {
        for (version.levels[level].items) |f| {
            if (!fileOverlapsBounds(bounds, f.min_key, f.max_key)) continue;
            const src = SstScanSource.init(allocator, dir, f.file_number, bounds) catch continue;
            if (src.current() == null) {
                var tmp = src;
                tmp.deinit();
                continue;
            }
            try out.append(allocator, .{ .sst = src });
        }
    }
}

// Finds the smallest current key across all scan sources.
fn pickNextScanKey(sources: []ScanSource) ?[]const u8 {
    var min_key: ?[]const u8 = null;
    for (sources) |*src| {
        const entry = src.current() orelse continue;
        if (min_key == null or std.mem.order(u8, entry.key, min_key.?) == .lt) {
            min_key = entry.key;
        }
    }
    return min_key;
}

// Sums the byte sizes of current L0 files as a rough compaction debt estimate.
fn pendingCompactionBytesLocked(self: *const Engine) u64 {
    var total: u64 = 0;
    for (self.version.levels[0].items) |f| total += f.size;
    return total;
}

// Flushes a sorted map into the project's simple line-oriented SST file format.
fn writeSstFromMap(self: *Engine, level: u8, file_number: u64, map: *std.StringHashMap(ValueEntry)) !manifest.FileMeta {
    var items = std.ArrayList(struct { key: []const u8, value: ValueEntry }).empty;
    defer items.deinit(self.allocator);

    var it = map.iterator();
    while (it.next()) |entry| {
        try items.append(self.allocator, .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* });
    }

    std.mem.sort(@TypeOf(items.items[0]), items.items, {}, struct {
        fn lessThan(_: void, a: @TypeOf(items.items[0]), b: @TypeOf(items.items[0])) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.lessThan);

    if (items.items.len == 0) return EngineError.InvalidConfig;

    var name_buf: [64]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&name_buf, "sst-{d}.sst", .{file_number});

    var file = try self.dir.createFile(file_name, .{ .truncate = true });
    defer file.close();

    var wbuf: [1024]u8 = undefined;
    var writer = file.writer(&wbuf);
    const w = &writer.interface;

    try w.print("ZIGGY-SST {d} {d} {d}\n", .{ sst_format_major, sst_format_minor, level });
    for (items.items) |item| {
        const tomb: u8 = if (item.value.tombstone) 1 else 0;
        const val = item.value.value orelse "";
        try w.print("{d}\t{d}\t{s}\t{s}\n", .{ item.value.seq, tomb, item.key, val });
    }
    try w.flush();
    try file.sync();

    const stat = try self.dir.statFile(file_name);
    return .{
        .level = level,
        .file_number = file_number,
        .size = stat.size,
        .min_key = try self.allocator.dupe(u8, items.items[0].key),
        .max_key = try self.allocator.dupe(u8, items.items[items.items.len - 1].key),
    };
}

// Reads an entire SST file into decoded entries for merge-style operations.
fn readSstEntries(allocator: std.mem.Allocator, dir: std.fs.Dir, file_number: u64) ![]SstEntry {
    var name_buf: [64]u8 = undefined;
    const file_name = try std.fmt.bufPrint(&name_buf, "sst-{d}.sst", .{file_number});

    const bytes = try dir.readFileAlloc(allocator, file_name, 128 * 1024 * 1024);
    defer allocator.free(bytes);

    var out = std.ArrayList(SstEntry).empty;
    errdefer {
        for (out.items) |it| {
            allocator.free(it.key);
            if (it.value) |v| allocator.free(v);
        }
        out.deinit(allocator);
    }

    var lines = std.mem.splitScalar(u8, bytes, '\n');
    const header = lines.next() orelse return EngineError.UnsupportedFormat;
    try validateSstHeaderLine(header);

    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var tok = std.mem.splitScalar(u8, line, '\t');
        const seq_txt = tok.next() orelse continue;
        const tomb_txt = tok.next() orelse continue;
        const key_txt = tok.next() orelse continue;
        const val_txt = tok.next() orelse "";

        const tomb = std.mem.eql(u8, tomb_txt, "1");
        try out.append(allocator, .{
            .seq = try std.fmt.parseUnsigned(u64, seq_txt, 10),
            .tombstone = tomb,
            .key = try allocator.dupe(u8, key_txt),
            .value = if (tomb) null else try allocator.dupe(u8, val_txt),
        });
    }

    return out.toOwnedSlice(allocator);
}

// Verifies that every manifest-referenced SST exists and advertises a supported header.
fn validateSstSet(_: std.mem.Allocator, dir: std.fs.Dir, version: *manifest.VersionSet) !void {
    for (0..manifest.MAX_LEVELS) |level| {
        for (version.levels[level].items) |f| {
            var name_buf: [64]u8 = undefined;
            const file_name = try std.fmt.bufPrint(&name_buf, "sst-{d}.sst", .{f.file_number});
            var file = dir.openFile(file_name, .{}) catch |err| switch (err) {
                error.FileNotFound => return EngineError.UnsupportedFormat,
                else => return err,
            };
            defer file.close();

            var rbuf: [1024]u8 = undefined;
            var reader = file.reader(&rbuf);
            const header = (try reader.interface.takeDelimiter('\n')) orelse return EngineError.UnsupportedFormat;
            try validateSstHeaderLine(header);
        }
    }
}

// Parses the project's SST header line and enforces format compatibility.
fn validateSstHeaderLine(line: []const u8) !void {
    var tok = std.mem.tokenizeScalar(u8, line, ' ');
    const magic = tok.next() orelse return EngineError.UnsupportedFormat;
    if (!std.mem.eql(u8, magic, "ZIGGY-SST")) return EngineError.UnsupportedFormat;

    const major_txt = tok.next() orelse return EngineError.UnsupportedFormat;
    const major = std.fmt.parseUnsigned(u64, major_txt, 10) catch return EngineError.UnsupportedFormat;
    if (major != sst_format_major) return EngineError.UnsupportedFormat;

    const token_after_major = tok.next() orelse return EngineError.UnsupportedFormat;
    const maybe_last_token = tok.next();

    var minor: u64 = 0;
    if (maybe_last_token) |_| {
        minor = std.fmt.parseUnsigned(u64, token_after_major, 10) catch return EngineError.UnsupportedFormat;
    }

    if (minor > sst_format_minor_max_compatible) return EngineError.UnsupportedFormat;
}

// Searches SST files in read-precedence order for the newest visible version of one key.
fn lookupDisk(allocator: std.mem.Allocator, dir: std.fs.Dir, version: *manifest.VersionSet, key: []const u8, snapshot_seq: ?u64) !?ValueEntry {
    var best: ?ValueEntry = null;

    var l0_idx = version.levels[0].items.len;
    while (l0_idx > 0) {
        l0_idx -= 1;
        const f = version.levels[0].items[l0_idx];
        if (!keyInRange(key, f.min_key, f.max_key)) continue;
        try mergeBestFromFile(allocator, dir, f.file_number, key, snapshot_seq, &best);
    }

    var level: usize = 1;
    while (level < manifest.MAX_LEVELS) : (level += 1) {
        if (findLevelCandidate(version.levels[level].items, key)) |f| {
            try mergeBestFromFile(allocator, dir, f.file_number, key, snapshot_seq, &best);
        }
    }

    return best;
}

// Merges all disk-resident entries matching a prefix into one newest-version map.
fn mergeDiskByPrefix(allocator: std.mem.Allocator, dir: std.fs.Dir, version: *manifest.VersionSet, prefix: []const u8, out: *std.StringHashMap(ValueEntry)) !void {
    for (0..manifest.MAX_LEVELS) |level| {
        for (version.levels[level].items) |f| {
            const entries = readSstEntries(allocator, dir, f.file_number) catch continue;
            defer {
                for (entries) |entry| {
                    allocator.free(entry.key);
                    if (entry.value) |v| allocator.free(v);
                }
                allocator.free(entries);
            }

            for (entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, prefix)) continue;
                try mergeEntry(allocator, out, entry.key, entry);
            }
        }
    }
}

// Merges all overlapping disk files for an explicit range scan.
fn mergeDiskByBounds(allocator: std.mem.Allocator, dir: std.fs.Dir, version: *manifest.VersionSet, bounds: ScanBounds, out: *std.StringHashMap(ValueEntry)) !void {
    var l0_idx = version.levels[0].items.len;
    while (l0_idx > 0) {
        l0_idx -= 1;
        const f = version.levels[0].items[l0_idx];
        if (!fileOverlapsBounds(bounds, f.min_key, f.max_key)) continue;
        try mergeFileByBounds(allocator, dir, f.file_number, bounds, out);
    }

    var level: usize = 1;
    while (level < manifest.MAX_LEVELS) : (level += 1) {
        for (version.levels[level].items) |f| {
            if (!fileOverlapsBounds(bounds, f.min_key, f.max_key)) continue;
            try mergeFileByBounds(allocator, dir, f.file_number, bounds, out);
        }
    }
}

// Reads one SST file and contributes only entries inside the requested bounds.
fn mergeFileByBounds(allocator: std.mem.Allocator, dir: std.fs.Dir, file_number: u64, bounds: ScanBounds, out: *std.StringHashMap(ValueEntry)) !void {
    const entries = readSstEntries(allocator, dir, file_number) catch return;
    defer {
        for (entries) |entry| {
            allocator.free(entry.key);
            if (entry.value) |v| allocator.free(v);
        }
        allocator.free(entries);
    }

    for (entries) |entry| {
        if (!keyInBounds(entry.key, bounds)) continue;
        try mergeEntry(allocator, out, entry.key, entry);
    }
}

// Returns the first non-L0 file whose recorded key range could contain the key.
fn findLevelCandidate(files: []manifest.FileMeta, key: []const u8) ?manifest.FileMeta {
    for (files) |f| if (keyInRange(key, f.min_key, f.max_key)) return f;
    return null;
}

// Checks inclusive file-range membership for a key.
fn keyInRange(key: []const u8, min: []const u8, max: []const u8) bool {
    if (std.mem.order(u8, key, min) == .lt) return false;
    if (std.mem.order(u8, key, max) == .gt) return false;
    return true;
}

// Checks half-open scan-bound membership for a key.
fn keyInBounds(key: []const u8, bounds: ScanBounds) bool {
    if (bounds.start) |start| {
        if (std.mem.order(u8, key, start) == .lt) return false;
    }
    if (bounds.end) |end| {
        if (std.mem.order(u8, key, end) != .lt) return false;
    }
    return true;
}

// Checks whether a file range intersects a requested scan range.
fn fileOverlapsBounds(bounds: ScanBounds, min: []const u8, max: []const u8) bool {
    if (bounds.end) |end| {
        if (std.mem.order(u8, end, min) != .gt) return false;
    }
    if (bounds.start) |start| {
        if (std.mem.order(u8, start, max) == .gt) return false;
    }
    return true;
}

// Examines one SST and updates the current best visible value for an exact key.
fn mergeBestFromFile(allocator: std.mem.Allocator, dir: std.fs.Dir, file_number: u64, key: []const u8, snapshot_seq: ?u64, best: *?ValueEntry) !void {
    const entries = readSstEntries(allocator, dir, file_number) catch return;
    defer {
        for (entries) |entry| {
            allocator.free(entry.key);
            if (entry.value) |v| allocator.free(v);
        }
        allocator.free(entries);
    }

    for (entries) |entry| {
        if (!std.mem.eql(u8, entry.key, key)) continue;
        if (snapshot_seq) |snap| {
            if (entry.seq > snap) continue;
        }
        if (best.*) |b| {
            if (entry.seq <= b.seq) continue;
            if (b.value) |v| allocator.free(v);
        }
        best.* = .{
            .seq = entry.seq,
            .tombstone = entry.tombstone,
            .value = if (entry.value) |v| try allocator.dupe(u8, v) else null,
        };
    }
}

// Best-effort deletion of an SST file after compaction or cleanup.
fn deleteSstFile(dir: std.fs.Dir, file_number: u64) void {
    var name_buf: [64]u8 = undefined;
    const file_name = std.fmt.bufPrint(&name_buf, "sst-{d}.sst", .{file_number}) catch return;
    dir.deleteFile(file_name) catch {};
}

// Writes one logical mutation to wal.log using the same physical framing rules as wal.zig.
fn appendFramed(self: *Engine, record: []const u8) !void {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(self.allocator);

    var offset: usize = 0;
    var first = true;

    while (offset < record.len) {
        const block_offset = @as(usize, @intCast(self.wal_logical_pos % wal.BLOCK_SIZE));
        const remaining_in_block = wal.BLOCK_SIZE - block_offset;

        if (remaining_in_block < wal.HEADER_SIZE) {
            try out.appendNTimes(self.allocator, 0, remaining_in_block);
            self.wal_logical_pos += @as(u64, @intCast(remaining_in_block));
            continue;
        }

        const max_fragment = remaining_in_block - wal.HEADER_SIZE;
        const fragment_len = @min(max_fragment, record.len - offset);
        const is_last = (offset + fragment_len) == record.len;

        const ty: wal.FragmentType = if (first and is_last)
            .full
        else if (first)
            .first
        else if (is_last)
            .last
        else
            .middle;

        const frag = record[offset .. offset + fragment_len];
        const checksum = walFragmentChecksum(ty, frag);

        var header: [wal.HEADER_SIZE]u8 = undefined;
        header[0] = @intCast(checksum & 0xFF);
        header[1] = @intCast((checksum >> 8) & 0xFF);
        header[2] = @intCast((checksum >> 16) & 0xFF);
        header[3] = @intCast((checksum >> 24) & 0xFF);
        const len16: u16 = @intCast(fragment_len);
        header[4] = @intCast(len16 & 0xFF);
        header[5] = @intCast((len16 >> 8) & 0xFF);
        header[6] = @intFromEnum(ty);

        try out.appendSlice(self.allocator, &header);
        try out.appendSlice(self.allocator, frag);

        self.wal_logical_pos += @as(u64, @intCast(wal.HEADER_SIZE + fragment_len));
        offset += fragment_len;
        first = false;
    }

    try self.wal_file.writeAll(out.items);
    if (self.durability_mode == .fdatasync) try std.posix.fdatasync(self.wal_file.handle);
}

// Encodes the engine's logical WAL mutation record.
fn encodeRecord(allocator: std.mem.Allocator, op: Op, seq: u64, key: []const u8, value: []const u8) ![]u8 {
    if (key.len > std.math.maxInt(u16) or value.len > std.math.maxInt(u32)) return EngineError.InvalidConfig;

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    const w = out.writer(allocator);
    try w.writeInt(u64, seq, .little);
    try w.writeByte(@intFromEnum(op));
    try w.writeInt(u16, @intCast(key.len), .little);
    try w.writeInt(u32, @intCast(value.len), .little);
    try out.appendSlice(allocator, key);
    try out.appendSlice(allocator, value);
    return out.toOwnedSlice(allocator);
}

const WAL_BATCH_TAG: u8 = 3;

fn encodeBatchRecord(allocator: std.mem.Allocator, base_seq: u64, ops: []const BatchOp) ![]u8 {
    if (ops.len == 0 or ops.len > std.math.maxInt(u16)) return EngineError.InvalidConfig;

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    const w= out.writer(allocator);
    try w.writeInt(u64, base_seq, .little);
    try w.writeByte(WAL_BATCH_TAG);
    try w.writeInt(u16, @intCast(ops.len), .little);

    for (ops) |op| {
        if (op.key.len > std.math.maxInt(u16) or op.value.len > std.math.maxInt(u32)) return EngineError.InvalidConfig;
        try w.writeByte(@intFromEnum(op.op));
        try w.writeInt(u16, @intCast(op.key.len), .little);
        try w.writeInt(u32, @intCast(op.value.len), .little);
        try out.appendSlice(allocator, op.key);
        try out.appendSlice(allocator, op.value);
    }

    return out.toOwnedSlice(allocator);
}

// Parses one logical WAL record emitted by encodeRecord().
fn decodeRecord(bytes: []const u8) !Decoded {
    if (bytes.len < 15) return EngineError.CorruptedWalRecord;

    const seq = cbytes.readIntLe(u64, bytes, 0) catch return EngineError.CorruptedWalRecord;

    const op: Op = switch (bytes[8]) {
        1 => .put,
        2 => .delete,
        else => return EngineError.CorruptedWalRecord,
    };

    const key_len = cbytes.readIntLe(u16, bytes, 9) catch return EngineError.CorruptedWalRecord;
    const value_len = cbytes.readIntLe(u32, bytes, 11) catch return EngineError.CorruptedWalRecord;

    const key_start = 15;
    const key_end = key_start + @as(usize, key_len);
    const value_end = key_end + @as(usize, value_len);
    if (value_end > bytes.len) return EngineError.CorruptedWalRecord;

    return .{ .seq = seq, .op = op, .key = bytes[key_start..key_end], .value = bytes[key_end..value_end] };
}

// Replays wal.log into the active memtable map during open().
fn replayWal(
    allocator: std.mem.Allocator,
    dir: std.fs.Dir,
    map: *std.StringHashMap(ValueEntry),
    bytes_out: *u64,
    max_seq_seen: *u64,
) !void {
    const wal_bytes = dir.readFileAlloc(allocator, "wal.log", 128 * 1024 * 1024) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer allocator.free(wal_bytes);

    const records = wal.readAllRecords(allocator, wal_bytes) catch return EngineError.CorruptedWalRecord;
    defer {
        for (records) |record| allocator.free(record);
        allocator.free(records);
    }

    for (records) |record| {
        try replayWalRecord(allocator, map, bytes_out, max_seq_seen, record);
    }
}

fn replayWalRecord(
    allocator: std.mem.Allocator,
    map: *std.StringHashMap(ValueEntry),
    bytes_out: *u64,
    max_seq_seen: *u64,
    record: []const u8,
) !void {
    if (record.len < 9) return EngineError.CorruptedWalRecord;

    const tag = record[8];

    // legacy single-op
    if (tag == 1 or tag == 2) {
        const d = decodeRecord(record) catch return EngineError.CorruptedWalRecord;
        try applyMapMutation(allocator, map, bytes_out, d.seq, d.op, d.key, d.value);
        if (d.seq > max_seq_seen.*) max_seq_seen.* = d.seq;
        return;
    }

    // batch-op
    if (tag != WAL_BATCH_TAG) return EngineError.CorruptedWalRecord;
    if (record.len < 11) return EngineError.CorruptedWalRecord;

    const base_seq = cbytes.readIntLe(u64, record, 0) catch return EngineError.CorruptedWalRecord;
    const count = cbytes.readIntLe(u16, record, 9) catch return EngineError.CorruptedWalRecord;

    var pos: usize = 11;
    var seq = base_seq;
    var i: usize = 0;

    while (i < count) : (i += 1) {
        if (pos + 7 > record.len) return EngineError.CorruptedWalRecord;
        const op_raw = record[pos];
        const op: Op = switch (op_raw) {
            1 => .put,
            2 => .delete,
            else => return EngineError.CorruptedWalRecord,
        };
        const key_len = cbytes.readIntLe(u16, record, pos + 1) catch return EngineError.CorruptedWalRecord;
        const val_len = cbytes.readIntLe(u32, record, pos + 3) catch return EngineError.CorruptedWalRecord;
        pos += 7;

        const key_end = pos + @as(usize, key_len);
        const val_end = key_end + @as(usize, val_len);
        if (val_end > record.len) return EngineError.CorruptedWalRecord;

        const key = record[pos..key_end];
        const value = record[key_end..val_end];
        pos = val_end;

        try applyMapMutation(allocator, map, bytes_out, seq, op, key, value);
        if (seq > max_seq_seen.*) max_seq_seen.* = seq;
        seq += 1;
    }

    if (pos != record.len) return EngineError.CorruptedWalRecord;
}

// Applies a mutation directly into the active memtable state.
fn applyMutation(self: *Engine, seq: u64, op: Op, key: []const u8, value: []const u8) !void {
    try applyMapMutation(self.allocator, &self.active_mem, &self.active_mem_bytes, seq, op, key, value);
}

// Applies a put/delete mutation into an in-memory version map and updates its byte budget.
fn applyMapMutation(
    allocator: std.mem.Allocator,
    map: *std.StringHashMap(ValueEntry),
    bytes_acc: *u64,
    seq: u64,
    op: Op,
    key: []const u8,
    value: []const u8,
) !void {
    var gop = try map.getOrPut(key);
    if (!gop.found_existing) {
        gop.key_ptr.* = try allocator.dupe(u8, key);
        gop.value_ptr.* = .{ .seq = seq, .tombstone = true, .value = null };
        bytes_acc.* += @as(u64, @intCast(key.len));
    } else {
        if (gop.value_ptr.value) |old| {
            bytes_acc.* -= @as(u64, @intCast(old.len));
            allocator.free(old);
        }
    }

    gop.value_ptr.seq = seq;
    gop.value_ptr.tombstone = (op == .delete);

    if (op == .put) {
        gop.value_ptr.value = try allocator.dupe(u8, value);
        bytes_acc.* += @as(u64, @intCast(value.len));
    } else {
        gop.value_ptr.value = null;
    }
}

// Frees all keys and optional values owned by one in-memory version map.
fn deinitMap(allocator: std.mem.Allocator, map: *std.StringHashMap(ValueEntry)) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        if (entry.value_ptr.value) |v| allocator.free(v);
    }
    map.deinit();
}

// Rejects zero or negative compaction bandwidth settings.
fn validateCompactionRate(rate: i64) !u64 {
    if (rate <= 0) return EngineError.InvalidConfig;
    return @intCast(rate);
}

// Computes the same fragment checksum scheme used by wal.zig.
fn walFragmentChecksum(ty: wal.FragmentType, payload: []const u8) u32 {
    var hasher = std.hash.crc.Crc32Iscsi.init();
    const ty_byte: [1]u8 = .{@intFromEnum(ty)};
    hasher.update(&ty_byte);
    hasher.update(payload);
    return hasher.final();
}

// Opens the single-writer LOCK file and acquires a non-blocking exclusive flock.
fn acquireLock(dir: std.fs.Dir) !std.fs.File {
    const file = try dir.createFile("LOCK", .{ .truncate = false, .read = true });
    std.posix.flock(file.handle, std.posix.LOCK.EX | std.posix.LOCK.NB) catch return EngineError.LockHeld;
    return file;
}

// Copies one named file into a checkpoint directory and fsyncs the destination.
fn copyFile(allocator: std.mem.Allocator, src_dir: std.fs.Dir, dst_dir: std.fs.Dir, name: []const u8) !void {
    const bytes = try src_dir.readFileAlloc(allocator, name, 128 * 1024 * 1024);
    defer allocator.free(bytes);

    var file = try dst_dir.createFile(name, .{ .exclusive = true });
    defer file.close();
    try file.writeAll(bytes);
    try file.sync();
}

// Rejects checkpoint paths containing explicit .. traversal segments.
fn containsParentTraversal(path: []const u8) bool {
    var it = std.mem.tokenizeScalar(u8, path, '/');
    while (it.next()) |part| if (std.mem.eql(u8, part, "..")) return true;
    return false;
}

// Opens each checkpoint path component with NOFOLLOW semantics to reject symlink traversal.
fn openDirNoSymlinkChain(allocator: std.mem.Allocator, path: []const u8) !std.fs.Dir {
    const flags = std.posix.O{ .ACCMODE = .RDONLY, .DIRECTORY = true, .NOFOLLOW = true, .CLOEXEC = true };

    var current = if (std.fs.path.isAbsolute(path)) blk: {
        const root_fd = try std.posix.openZ("/", flags, 0);
        break :blk std.fs.Dir{ .fd = root_fd };
    } else blk: {
        const dup_fd = try std.posix.dup(std.fs.cwd().fd);
        break :blk std.fs.Dir{ .fd = dup_fd };
    };
    errdefer current.close();

    var it = std.mem.tokenizeScalar(u8, path, '/');
    while (it.next()) |part| {
        if (part.len == 0 or std.mem.eql(u8, part, ".")) continue;
        if (std.mem.eql(u8, part, "..")) return EngineError.UnsafeCheckpointPath;

        const part_z = try allocator.dupeZ(u8, part);
        defer allocator.free(part_z);

        const next_fd = std.posix.openatZ(current.fd, part_z, flags, 0) catch |err| switch (err) {
            error.SymLinkLoop => return EngineError.UnsafeCheckpointPath,
            else => return err,
        };
        current.close();
        current = .{ .fd = next_fd };
    }

    return current;
}

const checkpoint_rename_noreplace_flag: u32 = 1;

// Renames a checkpoint directory only if the destination name does not already exist.
fn renameNoReplace(allocator: std.mem.Allocator, parent: std.fs.Dir, old_name: []const u8, new_name: []const u8) !void {
    if (builtin.os.tag != .linux) {
        return EngineError.UnsupportedPlatform;
    }

    const old_z = try allocator.dupeZ(u8, old_name);
    defer allocator.free(old_z);
    const new_z = try allocator.dupeZ(u8, new_name);
    defer allocator.free(new_z);

    const linux = std.os.linux;
    const rc = linux.renameat2(parent.fd, old_z, parent.fd, new_z, checkpoint_rename_noreplace_flag);
    switch (linux.E.init(rc)) {
        .SUCCESS => return,
        .EXIST => return EngineError.CheckpointAlreadyExists,
        .NOSYS, .INVAL => return EngineError.UnsupportedPlatform,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .ACCES => return error.AccessDenied,
        else => |errno_code| return std.posix.unexpectedErrno(errno_code),
    }
}

fn writeCheckpointMarker(dir: std.fs.Dir, name: []const u8, content: []const u8) !void {
    var file = try dir.createFile(name, .{ .truncate = true, .exclusive = false });
    defer file.close();
    try file.writeAll(content);
    try file.sync();
}

fn removeCheckpointMarker(dir: std.fs.Dir, name: []const u8) void {
    dir.deleteFile(name) catch {};
}

fn createPortableCheckpoint(
    allocator: std.mem.Allocator,
    src_dir: std.fs.Dir,
    parent: std.fs.Dir,
    dst_name: []const u8,
) !void {
    parent.makeDir(dst_name) catch |err| switch (err) {
        error.PathAlreadyExists => return EngineError.CheckpointAlreadyExists,
        else => return err,
    };
    errdefer parent.deleteTree(dst_name) catch {};

    var dst = try parent.openDir(dst_name, .{});
    defer dst.close();

    try writeCheckpointMarker(dst, checkpoint_incomplete_marker, "copying\n");

    try copyFile(allocator, src_dir, dst, "wal.log");
    try copyFile(allocator, src_dir, dst, "CURRENT");

    var it = src_dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        const is_manifest = std.mem.startsWith(u8, entry.name, "MANIFEST-");
        const is_sst = std.mem.startsWith(u8, entry.name, "sst-") and std.mem.endsWith(u8, entry.name, ".sst");
        if (!is_manifest and !is_sst) continue;
        try copyFile(allocator, src_dir, dst, entry.name);
    }

    try writeCheckpointMarker(dst, checkpoint_complete_marker, "complete\n");
    removeCheckpointMarker(dst, checkpoint_incomplete_marker);
}

test "task 7.1 positive: open put get delete scan close reopen preserves data" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 4 * 1024 * 1024, .compaction_trigger_l0_files = 1000 });
        defer eng.close() catch {};

        var i: usize = 0;
        while (i < 1000) : (i += 1) {
            var kbuf: [32]u8 = undefined;
            var vbuf: [32]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "k-{d}", .{i});
            const v = try std.fmt.bufPrint(&vbuf, "v-{d}", .{i});
            try eng.put(k, v);
        }

        const v42 = try eng.get("k-42");
        defer eng.freeValue(v42);
        try std.testing.expectEqualStrings("v-42", v42);

        const rows = try eng.scanPrefix("k-9");
        defer eng.freeScan(rows);
        try std.testing.expect(rows.len > 0);

        try eng.delete("k-42");
        try std.testing.expectError(EngineError.KeyNotFound, eng.get("k-42"));
    }

    {
        var reopened = try Engine.open(allocator, .{ .path = db_path });
        defer reopened.close() catch {};

        const v999 = try reopened.get("k-999");
        defer reopened.freeValue(v999);
        try std.testing.expectEqualStrings("v-999", v999);
        try std.testing.expectError(EngineError.KeyNotFound, reopened.get("k-42"));
    }
}

test "task 7.1 negative: second writer open fails with lock error" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try std.testing.expectError(EngineError.LockHeld, Engine.open(allocator, .{ .path = db_path }));
}

test "task 7.2 negative: corrupted wal fails open deterministically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path });
        defer eng.close() catch {};
        try eng.put("a", "1");
    }

    var wal_file = try tmp.dir.openFile("wal.log", .{ .mode = .read_write });
    defer wal_file.close();
    var bytes = try wal_file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(bytes);
    if (bytes.len > 8) bytes[8] ^= 0xFF;
    try wal_file.seekTo(0);
    try wal_file.writeAll(bytes);
    try wal_file.setEndPos(bytes.len);

    try std.testing.expectError(EngineError.CorruptedWalRecord, Engine.open(allocator, .{ .path = db_path }));
    try std.testing.expectError(EngineError.CorruptedWalRecord, Engine.open(allocator, .{ .path = db_path }));
}

test "task 7.2 positive: acknowledged writes recover after reopen without duplicate apply" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path });
        defer eng.close() catch {};
        try eng.put("r-a", "1");
        try eng.put("r-a", "2");
    }

    {
        var reopened = try Engine.open(allocator, .{ .path = db_path });
        defer reopened.close() catch {};
        const got = try reopened.get("r-a");
        defer reopened.freeValue(got);
        try std.testing.expectEqualStrings("2", got);

        const next_seq = reopened.next_sequence;
        reopened.close() catch {};

        var reopened2 = try Engine.open(allocator, .{ .path = db_path });
        defer reopened2.close() catch {};
        try std.testing.expectEqual(next_seq, reopened2.next_sequence);
    }
}

test "task 7.4 positive: snapshot sequence hides keys written after snapshot" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try eng.put("snap-base", "v1");
    const snapshot_seq = eng.next_sequence - 1;
    try eng.put("snap-future", "v2");

    try std.testing.expectError(EngineError.KeyNotFound, eng.getAtSnapshot("snap-future", snapshot_seq));

    const latest = try eng.get("snap-future");
    defer eng.freeValue(latest);
    try std.testing.expectEqualStrings("v2", latest);
}

test "task 7.4 negative: close during background activity is deterministic and reopenable" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 24, .compaction_trigger_l0_files = 2 });

    var i: usize = 0;
    while (i < 160) : (i += 1) {
        var kbuf: [32]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "bg-{d}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "vv-{d}", .{i});
        try eng.put(k, v);
    }

    try eng.close();

    var reopened = try Engine.open(allocator, .{ .path = db_path });
    defer reopened.close() catch {};
    const got = try reopened.get("bg-159");
    defer reopened.freeValue(got);
    try std.testing.expectEqualStrings("vv-159", got);
}

test "task 12.1 positive: checkpoint creates reopenable snapshot" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    const cp_path = try std.fmt.allocPrint(allocator, "{s}/checkpoint", .{db_path});
    defer allocator.free(cp_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 24, .compaction_trigger_l0_files = 100 });
        defer eng.close() catch {};

        var i: usize = 0;
        while (i < 20) : (i += 1) {
            var kbuf: [32]u8 = undefined;
            var vbuf: [32]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "cp-{d}", .{i});
            const v = try std.fmt.bufPrint(&vbuf, "vv-{d}", .{i});
            try eng.put(k, v);
        }

        var attempts: usize = 0;
        while (attempts < 200) : (attempts += 1) {
            if ((try eng.property("engine.l0.files")).u64 > 0) break;
            std.Thread.sleep(2 * std.time.ns_per_ms);
        }

        try eng.checkpoint(cp_path);
    }

    var cp = try Engine.open(allocator, .{ .path = cp_path });
    defer cp.close() catch {};
    const got = try cp.get("cp-19");
    defer cp.freeValue(got);
    try std.testing.expectEqualStrings("vv-19", got);
}

test "task 12.1 negative: checkpoint rejects empty path" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try std.testing.expectError(EngineError.InvalidConfig, eng.checkpoint(""));
}

test "task 12.1 safety: checkpoint rejects parent traversal path" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try std.testing.expectError(EngineError.UnsafeCheckpointPath, eng.checkpoint("../bad"));
}

test "task 12.1 safety: checkpoint fails on existing directory without overwrite" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    const cp_path = try std.fmt.allocPrint(allocator, "{s}/checkpoint", .{db_path});
    defer allocator.free(cp_path);
    try std.fs.cwd().makePath(cp_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try std.testing.expectError(EngineError.CheckpointAlreadyExists, eng.checkpoint(cp_path));
}

test "task 12.1 safety: incomplete portable checkpoint fails closed on open" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(root_path);

    const cp_path = try std.fmt.allocPrint(allocator, "{s}/checkpoint-incomplete", .{root_path});
    defer allocator.free(cp_path);
    try std.fs.cwd().makePath(cp_path);

    const marker_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ cp_path, checkpoint_incomplete_marker });
    defer allocator.free(marker_path);

    {
        var file = try std.fs.cwd().createFile(marker_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll("copying\n");
    }

    try std.testing.expectError(EngineError.IncompleteCheckpoint, Engine.open(allocator, .{ .path = cp_path }));
}

test "task 12.1 regression: concurrent checkpoint creators are deterministic and leave no partial output" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(root_path);

    const db_a_path = try std.fmt.allocPrint(allocator, "{s}/db-a", .{root_path});
    defer allocator.free(db_a_path);
    const db_b_path = try std.fmt.allocPrint(allocator, "{s}/db-b", .{root_path});
    defer allocator.free(db_b_path);
    const cp_path = try std.fmt.allocPrint(allocator, "{s}/checkpoint-race", .{root_path});
    defer allocator.free(cp_path);

    try std.fs.cwd().makePath(db_a_path);
    try std.fs.cwd().makePath(db_b_path);

    var eng_a = try Engine.open(allocator, .{ .path = db_a_path, .memtable_max_bytes = 24, .compaction_trigger_l0_files = 100 });
    defer eng_a.close() catch {};
    var eng_b = try Engine.open(allocator, .{ .path = db_b_path, .memtable_max_bytes = 24, .compaction_trigger_l0_files = 100 });
    defer eng_b.close() catch {};

    try eng_a.put("origin", "A");
    try eng_b.put("origin", "B");

    const Outcome = enum {
        success,
        exists,
        unexpected,
    };

    const Ctx = struct {
        eng_a: *Engine,
        eng_b: *Engine,
        cp_path: []const u8,
        outcomes: *[2]Outcome,

        fn run(ctx: *@This(), idx: usize) void {
            const eng = if (idx == 0) ctx.eng_a else ctx.eng_b;
            eng.checkpoint(ctx.cp_path) catch |err| {
                ctx.outcomes[idx] = switch (err) {
                    EngineError.CheckpointAlreadyExists => .exists,
                    else => .unexpected,
                };
                return;
            };
            ctx.outcomes[idx] = .success;
        }
    };

    var outcomes = [_]Outcome{ .unexpected, .unexpected };
    var ctx = Ctx{ .eng_a = &eng_a, .eng_b = &eng_b, .cp_path = cp_path, .outcomes = &outcomes };

    const t1 = try std.Thread.spawn(.{}, Ctx.run, .{ &ctx, @as(usize, 0) });
    const t2 = try std.Thread.spawn(.{}, Ctx.run, .{ &ctx, @as(usize, 1) });
    t1.join();
    t2.join();

    const success_count: usize = @intFromBool(outcomes[0] == .success) + @intFromBool(outcomes[1] == .success);
    const exists_count: usize = @intFromBool(outcomes[0] == .exists) + @intFromBool(outcomes[1] == .exists);
    try std.testing.expectEqual(@as(usize, 1), success_count);
    try std.testing.expectEqual(@as(usize, 1), exists_count);

    var cp = try Engine.open(allocator, .{ .path = cp_path });
    defer cp.close() catch {};
    const got = try cp.get("origin");
    defer cp.freeValue(got);
    try std.testing.expect(std.mem.eql(u8, got, "A") or std.mem.eql(u8, got, "B"));

    var parent = try openDirNoSymlinkChain(allocator, root_path);
    defer parent.close();
    var it = parent.iterate();
    while (try it.next()) |entry| {
        try std.testing.expect(!std.mem.startsWith(u8, entry.name, ".ziggy-checkpoint-"));
    }
}

test "task 8.1 positive: memtable rotation flushes to L0 and survives reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 32, .compaction_trigger_l0_files = 100 });
        defer eng.close() catch {};

        var i: usize = 0;
        while (i < 32) : (i += 1) {
            var kbuf: [32]u8 = undefined;
            var vbuf: [32]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "k-{d}", .{i});
            const v = try std.fmt.bufPrint(&vbuf, "v-{d}", .{i});
            try eng.put(k, v);
        }

        var attempts: usize = 0;
        while (attempts < 200) : (attempts += 1) {
            const l0 = (try eng.property("engine.l0.files")).u64;
            if (l0 > 0) break;
            std.Thread.sleep(2 * std.time.ns_per_ms);
        }
        try std.testing.expect((try eng.property("engine.l0.files")).u64 > 0);
    }

    {
        var reopened = try Engine.open(allocator, .{ .path = db_path });
        defer reopened.close() catch {};
        const v = try reopened.get("k-31");
        defer reopened.freeValue(v);
        try std.testing.expectEqualStrings("v-31", v);
    }
}

test "task 8.1 negative: crash after WAL append before memtable apply replays exactly once" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path, .compaction_trigger_l0_files = 100 });
        defer eng.close() catch {};

        const seq = eng.next_sequence;
        const record = try encodeRecord(allocator, .put, seq, "mid-crash", "v-crash");
        defer allocator.free(record);

        // Simulate a kill window: record is durable in WAL but not applied to the active memtable.
        try appendFramed(&eng, record);
    }

    {
        var reopened = try Engine.open(allocator, .{ .path = db_path });
        defer reopened.close() catch {};

        const got = try reopened.get("mid-crash");
        defer reopened.freeValue(got);
        try std.testing.expectEqualStrings("v-crash", got);

        const seq_after_replay = reopened.next_sequence;
        reopened.close() catch {};

        var reopened_again = try Engine.open(allocator, .{ .path = db_path });
        defer reopened_again.close() catch {};
        try std.testing.expectEqual(seq_after_replay, reopened_again.next_sequence);
    }
}

test "task 8.2 positive: canonical read order with snapshot visibility across mutable immutable L0 L1" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path, .compaction_trigger_l0_files = 100 });
    defer eng.close() catch {};

    const writeTierFile = struct {
        fn call(e: *Engine, level: u8, seq: u64, value: []const u8) !void {
            var m = std.StringHashMap(ValueEntry).init(e.allocator);
            defer deinitMap(e.allocator, &m);
            var bytes: u64 = 0;
            try applyMapMutation(e.allocator, &m, &bytes, seq, .put, "hot", value);

            e.mutex.lock();
            const n = e.version.next_file_number;
            e.version.next_file_number += 1;
            e.mutex.unlock();

            const meta = try writeSstFromMap(e, level, n, &m);
            defer {
                e.allocator.free(meta.min_key);
                e.allocator.free(meta.max_key);
            }

            e.mutex.lock();
            defer e.mutex.unlock();
            try e.manifest_store.applyEditAtomic(&e.version, .{
                .new_files = &.{meta},
                .deleted_files = &.{},
                .next_sequence = e.next_sequence,
                .next_file_number = e.version.next_file_number,
            });
        }
    }.call;

    try writeTierFile(&eng, 1, 10, "v-l1");
    try writeTierFile(&eng, 0, 20, "v-l0");

    var imm_map = std.StringHashMap(ValueEntry).init(allocator);
    var imm_bytes: u64 = 0;
    try applyMapMutation(allocator, &imm_map, &imm_bytes, 30, .put, "hot", "v-imm");

    eng.mutex.lock();
    try eng.immutables.append(allocator, .{ .id = eng.next_mem_id, .map = imm_map, .bytes = imm_bytes });
    eng.next_mem_id += 1;
    try applyMutation(&eng, 40, .put, "hot", "v-active");
    eng.next_sequence = 41;
    eng.mutex.unlock();

    const s15 = try eng.getAtSnapshot("hot", 15);
    defer eng.freeValue(s15);
    try std.testing.expectEqualStrings("v-l1", s15);

    const s25 = try eng.getAtSnapshot("hot", 25);
    defer eng.freeValue(s25);
    try std.testing.expectEqualStrings("v-l0", s25);

    const s35 = try eng.getAtSnapshot("hot", 35);
    defer eng.freeValue(s35);
    try std.testing.expectEqualStrings("v-imm", s35);

    const s45 = try eng.getAtSnapshot("hot", 45);
    defer eng.freeValue(s45);
    try std.testing.expectEqualStrings("v-active", s45);
}

test "task 8.2 negative: newest tombstone in L0 masks older lower-level value" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 24, .compaction_trigger_l0_files = 100 });
    defer eng.close() catch {};

    try eng.put("hot", "v1");
    try eng.put("filler-a", "xxxxxxxx");
    try eng.put("filler-b", "xxxxxxxx");

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        if ((try eng.property("engine.l0.files")).u64 > 0) break;
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }

    try eng.delete("hot");
    try eng.put("filler-c", "xxxxxxxx");
    try eng.put("filler-d", "xxxxxxxx");

    attempts = 0;
    while (attempts < 200) : (attempts += 1) {
        if ((try eng.property("engine.l0.files")).u64 >= 2) break;
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }

    try std.testing.expectError(EngineError.KeyNotFound, eng.get("hot"));
}

test "task 9.1 negative: bounded scheduler queue triggers explicit backpressure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{
        .path = db_path,
        .memtable_max_bytes = 8,
        .stop_l0_files = 1,
        .slowdown_l0_files = 1,
        .compaction_trigger_l0_files = 100,
    });
    defer eng.close() catch {};

    var got_stall = false;
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        var kbuf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&kbuf, "k{d}", .{i});
        if (eng.put(key, "123456789")) |_| {} else |err| {
            if (err == EngineError.WriteStall) {
                got_stall = true;
                break;
            }
        }
    }

    try std.testing.expect(got_stall);
    try std.testing.expectEqualStrings("l0_pressure", (try eng.property("engine.stall.reason")).text);
}

test "task 9.1 positive: dedicated workers drain flush and compaction queues asynchronously" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{
        .path = db_path,
        .memtable_max_bytes = 16,
        .compaction_trigger_l0_files = 2,
    });
    defer eng.close() catch {};

    var i: usize = 0;
    while (i < 80) : (i += 1) {
        var kbuf: [32]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "wk-{d}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "value-{d}", .{i});
        try eng.put(k, v);
    }

    var drained = false;
    var attempts: usize = 0;
    while (attempts < 300) : (attempts += 1) {
        const flush_depth = (try eng.property("engine.scheduler.flush_queue_depth")).u64;
        const compaction_depth = (try eng.property("engine.scheduler.compaction_queue_depth")).u64;
        if (flush_depth == 0 and compaction_depth == 0 and (try eng.property("engine.metrics.flush.count")).u64 > 0) {
            drained = true;
            break;
        }
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }

    try std.testing.expect(drained);
    try std.testing.expect((try eng.property("engine.metrics.compaction.count")).u64 > 0);
}

test "task 9.2 negative: stop threshold fails fast until debt drops" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{
        .path = db_path,
        .memtable_max_bytes = 24,
        .slowdown_l0_files = 1,
        .stop_l0_files = 1,
        .slowdown_immutable_count = 200,
        .stop_immutable_count = 400,
        .compaction_trigger_l0_files = 100,
    });
    defer eng.close() catch {};

    var i: usize = 0;
    while (i < 64) : (i += 1) {
        var kbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "stall-{d}", .{i});
        if (eng.put(k, "xxxxxxxx")) |_| {} else |err| {
            if (err == EngineError.WriteStall) break;
            return err;
        }
    }

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        if ((try eng.property("engine.l0.files")).u64 >= 1) break;
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }

    try std.testing.expectError(EngineError.WriteStall, eng.put("blocked", "v"));
    try std.testing.expectError(EngineError.WriteStall, eng.put("blocked-2", "v"));
    try std.testing.expectEqualStrings("stop", (try eng.property("engine.stall.state")).text);
    try std.testing.expectEqualStrings("l0_pressure", (try eng.property("engine.stall.reason")).text);

    eng.mutex.lock();
    for (eng.version.levels[0].items) |f| {
        eng.allocator.free(f.min_key);
        eng.allocator.free(f.max_key);
        deleteSstFile(eng.dir, f.file_number);
    }
    eng.version.levels[0].clearRetainingCapacity();
    eng.refreshStallStateLocked();
    eng.mutex.unlock();

    try eng.put("after-drop", "ok");
    try std.testing.expectEqualStrings("normal", (try eng.property("engine.stall.state")).text);
}

test "task 9.2 regression: slowdown sleep does not hold global mutex" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{
        .path = db_path,
        .memtable_max_bytes = 1024,
        .slowdown_immutable_count = 0,
        .stop_immutable_count = 100,
    });
    defer eng.close() catch {};

    const Ctx = struct {
        eng: *Engine,
        done: *std.atomic.Value(bool),

        fn writer(ctx: @This()) void {
            var i: usize = 0;
            while (i < 20) : (i += 1) {
                ctx.eng.put("hot", "value") catch {};
            }
            ctx.done.store(true, .release);
        }

        fn reader(ctx: @This()) void {
            while (!ctx.done.load(.acquire)) {
                _ = ctx.eng.property("engine.stall.state") catch {};
            }
        }
    };

    var done = std.atomic.Value(bool).init(false);
    const ctx = Ctx{ .eng = &eng, .done = &done };
    const wt = try std.Thread.spawn(.{}, Ctx.writer, .{ctx});
    const rt = try std.Thread.spawn(.{}, Ctx.reader, .{ctx});
    wt.join();
    rt.join();

    try std.testing.expect((try eng.property("engine.metrics.stall.duration_ms")).u64 > 0);
}

test "task 9.3 positive: compaction limiter is wired and metrics/properties are exposed" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{
        .path = db_path,
        .memtable_max_bytes = 16,
        .compaction_trigger_l0_files = 2,
        .slowdown_l0_files = 200,
        .stop_l0_files = 400,
        .slowdown_immutable_count = 200,
        .stop_immutable_count = 400,
        .compaction_rate_bytes_per_sec = 1024,
    });
    defer eng.close() catch {};

    try std.testing.expectEqual(@as(u64, 1024), (try eng.property("engine.compaction.rate_limit.bytes_per_sec")).u64);

    eng.mutex.lock();
    eng.io_limiter.tokens = 0;
    eng.mutex.unlock();

    var i: usize = 0;
    var retries: usize = 0;
    while (i < 96) {
        var kbuf: [32]u8 = undefined;
        var vbuf: [64]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "rl-{d}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "payload-{d}-abcdefgh", .{i});
        eng.put(k, v) catch |err| switch (err) {
            EngineError.WriteStall => {
                retries += 1;
                if (retries > 2000) return error.TestUnexpectedResult;
                std.Thread.sleep(2 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        i += 1;
    }

    var attempts: usize = 0;
    while (attempts < 400) : (attempts += 1) {
        if ((try eng.property("engine.metrics.compaction.count")).u64 > 0) break;
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }

    try std.testing.expect((try eng.property("engine.metrics.compaction.count")).u64 > 0);
    try std.testing.expect((try eng.property("engine.metrics.compaction.rate_limited.count")).u64 > 0);
}

test "task 9.3 negative: zero/negative compaction rate is rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    try std.testing.expectError(EngineError.InvalidConfig, Engine.open(allocator, .{ .path = db_path, .compaction_rate_bytes_per_sec = 0 }));
    try std.testing.expectError(EngineError.InvalidConfig, validateCompactionRate(0));
    try std.testing.expectError(EngineError.InvalidConfig, validateCompactionRate(-1));
}

test "task 10.2 positive: metrics/properties are real and monotonic" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try eng.put("a", "1");
    try eng.put("b", "2");
    const got = try eng.get("a");
    defer eng.freeValue(got);
    _ = eng.get("missing") catch {};

    try std.testing.expect((try eng.property("engine.metrics.put.count")).u64 >= 2);
    try std.testing.expect((try eng.property("engine.metrics.get.count")).u64 >= 2);
    try std.testing.expect((try eng.property("engine.metrics.get.miss.count")).u64 >= 1);
    try std.testing.expect((try eng.property("engine.memtable.bytes")).u64 > 0);
}

test "task 10.2 positive: latency histogram exports p50/p95/p99 for put/get/delete/scan" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try eng.put("hist-a", "1");
    try eng.put("hist-b", "2");
    const hit = try eng.get("hist-a");
    eng.freeValue(hit);
    _ = eng.get("missing") catch {};
    try eng.delete("hist-b");
    const rows = try eng.scanPrefix("hist-");
    eng.freeScan(rows);

    const put_p50 = (try eng.property("engine.metrics.latency.put.p50_us")).u64;
    const put_p95 = (try eng.property("engine.metrics.latency.put.p95_us")).u64;
    const put_p99 = (try eng.property("engine.metrics.latency.put.p99_us")).u64;

    try std.testing.expect(put_p50 > 0);
    try std.testing.expect(put_p95 >= put_p50);
    try std.testing.expect(put_p99 >= put_p95);

    const get_p50 = (try eng.property("engine.metrics.latency.get.p50_us")).u64;
    const del_p50 = (try eng.property("engine.metrics.latency.delete.p50_us")).u64;
    const scan_p50 = (try eng.property("engine.metrics.latency.scan.p50_us")).u64;
    try std.testing.expect(get_p50 > 0);
    try std.testing.expect(del_p50 > 0);
    try std.testing.expect(scan_p50 > 0);
}

fn forceSstAndMutateHeader(allocator: std.mem.Allocator, dir: std.fs.Dir, db_path: []const u8, header: []const u8) !void {
    {
        var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 24, .compaction_trigger_l0_files = 100 });
        defer eng.close() catch {};
        try eng.put("fmt", "v1");
        try eng.put("fmt-pad-a", "xxxxxxxx");
        try eng.put("fmt-pad-b", "xxxxxxxx");

        var attempts: usize = 0;
        while (attempts < 200) : (attempts += 1) {
            if ((try eng.property("engine.l0.files")).u64 > 0) break;
            std.Thread.sleep(2 * std.time.ns_per_ms);
        }
    }

    var iter_dir = try dir.openDir(".", .{ .iterate = true });
    defer iter_dir.close();

    var it = iter_dir.iterate();
    var sst_name: ?[]u8 = null;
    defer if (sst_name) |name| allocator.free(name);
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        if (std.mem.startsWith(u8, entry.name, "sst-") and std.mem.endsWith(u8, entry.name, ".sst")) {
            sst_name = try allocator.dupe(u8, entry.name);
            break;
        }
    }
    const file_name = sst_name orelse return error.TestUnexpectedResult;

    const bytes = try dir.readFileAlloc(allocator, file_name, 64 * 1024);
    defer allocator.free(bytes);
    const newline = std.mem.indexOfScalar(u8, bytes, '\n') orelse return error.TestUnexpectedResult;
    const remainder = bytes[newline + 1 ..];

    var mutated = std.ArrayList(u8).empty;
    defer mutated.deinit(allocator);
    try mutated.appendSlice(allocator, header);
    try mutated.appendSlice(allocator, "\n");
    try mutated.appendSlice(allocator, remainder);

    var file = try dir.createFile(file_name, .{ .truncate = true });
    defer file.close();
    try file.writeAll(mutated.items);
    try file.sync();
}

test "task 10.3 positive: supported minor SST format opens successfully" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    try forceSstAndMutateHeader(allocator, tmp.dir, db_path, "ZIGGY-SST 1 1 0");

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};
}

test "task 10.3 negative: unsupported minor SST format fails fast on open" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    try forceSstAndMutateHeader(allocator, tmp.dir, db_path, "ZIGGY-SST 1 9 0");
    try std.testing.expectError(EngineError.UnsupportedFormat, Engine.open(allocator, .{ .path = db_path }));
}

test "task 10.3 negative: unsupported major SST format fails fast on open" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    try forceSstAndMutateHeader(allocator, tmp.dir, db_path, "ZIGGY-SST 999 0 0");
    try std.testing.expectError(EngineError.UnsupportedFormat, Engine.open(allocator, .{ .path = db_path }));
}

test "task 12.3 positive: scan range returns sorted keys within [start,end)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try eng.put("a", "1");
    try eng.put("b", "2");
    try eng.put("c", "3");
    try eng.put("d", "4");

    const rows = try eng.scan(.{ .start = "b", .end = "d" });
    defer eng.freeScan(rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("b", rows[0].key);
    try std.testing.expectEqualStrings("2", rows[0].value);
    try std.testing.expectEqualStrings("c", rows[1].key);
    try std.testing.expectEqualStrings("3", rows[1].value);
}

test "task 12.3 positive: start-only scan includes lower bound and is ordered" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 16, .compaction_trigger_l0_files = 100 });
    defer eng.close() catch {};

    try eng.put("a", "1");
    try eng.put("b", "2");
    try eng.put("c", "3");
    try eng.put("d", "4");

    const rows = try eng.scan(.{ .start = "c" });
    defer eng.freeScan(rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("c", rows[0].key);
    try std.testing.expectEqualStrings("d", rows[1].key);
}

test "task 12.3 negative: invalid scan bounds are rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try std.testing.expectError(EngineError.InvalidConfig, eng.scan(.{ .start = "z", .end = "a" }));
}

test "task 7.4 positive: scan keeps stable snapshot under concurrent overlapping writes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 16 * 1024, .compaction_trigger_l0_files = 100 });
    defer eng.close() catch {};

    var i: usize = 0;
    while (i < 2000) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "snap-{d:0>4}", .{i});
        try eng.put(key, "base");
    }

    eng.scan_test_delay_ns = 40 * std.time.ns_per_ms;

    const Ctx = struct {
        eng: *Engine,
        rows: ?[]ScanItem = null,

        fn runScan(ctx: *@This()) void {
            ctx.rows = ctx.eng.scan(.{ .start = "snap-", .end = "snap.~" }) catch null;
        }

        fn runWriter(ctx: *@This()) void {
            var waits: usize = 0;
            while (waits < 500) : (waits += 1) {
                const count_value = ctx.eng.property("engine.metrics.scan.count") catch {
                    std.Thread.sleep(1 * std.time.ns_per_ms);
                    continue;
                };
                if (count_value.u64 > 0) break;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }

            var j: usize = 0;
            while (j < 1000) : (j += 1) {
                var key_buf: [32]u8 = undefined;
                const key = std.fmt.bufPrint(&key_buf, "snap-{d:0>4}", .{j}) catch continue;
                if (j % 2 == 0) {
                    ctx.eng.put(key, "new") catch {};
                } else {
                    ctx.eng.delete(key) catch {};
                }
            }
        }
    };

    var ctx = Ctx{ .eng = &eng };
    const scan_thread = try std.Thread.spawn(.{}, Ctx.runScan, .{&ctx});
    const writer_thread = try std.Thread.spawn(.{}, Ctx.runWriter, .{&ctx});
    scan_thread.join();
    writer_thread.join();

    const rows = ctx.rows orelse return error.TestUnexpectedResult;
    defer eng.freeScan(rows);

    try std.testing.expectEqual(@as(usize, 2000), rows.len);
    for (rows) |row| {
        try std.testing.expectEqualStrings("base", row.value);
    }
}

test "task 8.2 positive: scanPrefix keeps stable snapshot under concurrent overlapping writes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 16 * 1024, .compaction_trigger_l0_files = 100 });
    defer eng.close() catch {};

    var i: usize = 0;
    while (i < 2000) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "pref-{d:0>4}", .{i});
        try eng.put(key, "base");
    }

    eng.scan_test_delay_ns = 40 * std.time.ns_per_ms;

    const Ctx = struct {
        eng: *Engine,
        rows: ?[]ScanItem = null,

        fn runScan(ctx: *@This()) void {
            ctx.rows = ctx.eng.scanPrefix("pref-") catch null;
        }

        fn runWriter(ctx: *@This()) void {
            var waits: usize = 0;
            while (waits < 500) : (waits += 1) {
                const count_value = ctx.eng.property("engine.metrics.scan.count") catch {
                    std.Thread.sleep(1 * std.time.ns_per_ms);
                    continue;
                };
                if (count_value.u64 > 0) break;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }

            var j: usize = 0;
            while (j < 1000) : (j += 1) {
                var key_buf: [32]u8 = undefined;
                const key = std.fmt.bufPrint(&key_buf, "pref-{d:0>4}", .{j}) catch continue;
                if (j % 2 == 0) {
                    ctx.eng.put(key, "new") catch {};
                } else {
                    ctx.eng.delete(key) catch {};
                }
            }
        }
    };

    var ctx = Ctx{ .eng = &eng };
    const scan_thread = try std.Thread.spawn(.{}, Ctx.runScan, .{&ctx});
    const writer_thread = try std.Thread.spawn(.{}, Ctx.runWriter, .{&ctx});
    scan_thread.join();
    writer_thread.join();

    const rows = ctx.rows orelse return error.TestUnexpectedResult;
    defer eng.freeScan(rows);

    try std.testing.expectEqual(@as(usize, 2000), rows.len);
    for (rows) |row| {
        try std.testing.expectEqualStrings("base", row.value);
    }
}

test "task 12.3 stress: large SST range scan stays within bounded allocator budget" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng_seed = try Engine.open(allocator, .{
            .path = db_path,
            .memtable_max_bytes = 64 * 1024,
            .compaction_trigger_l0_files = 1000,
            .slowdown_l0_files = 10_000,
            .stop_l0_files = 20_000,
            .slowdown_immutable_count = 10_000,
            .stop_immutable_count = 20_000,
        });
        defer eng_seed.close() catch {};

        var i: usize = 0;
        while (i < 20000) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            var value_buf: [512]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "bulk-{d:0>5}", .{i});
            const tail = try std.fmt.bufPrint(&value_buf, "{d:0>5}-", .{i});
            @memset(value_buf[tail.len..], 'x');
            try eng_seed.put(key, value_buf[0..]);
        }
    }

    const budget_backing = try allocator.alloc(u8, 4 * 1024 * 1024);
    defer allocator.free(budget_backing);
    var fba = std.heap.FixedBufferAllocator.init(budget_backing);

    tmp.dir.deleteFile("wal.log") catch {};

    var eng = try Engine.open(fba.allocator(), .{ .path = db_path });
    defer eng.close() catch {};

    const rows = try eng.scan(.{ .start = "bulk-12345", .end = "bulk-12346" });
    defer eng.freeScan(rows);

    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("bulk-12345", rows[0].key);
}

test "task 12.3 stress: large SST point get stays within bounded allocator budget" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng_seed = try Engine.open(allocator, .{
            .path = db_path,
            .memtable_max_bytes = 64 * 1024,
            .compaction_trigger_l0_files = 1000,
            .slowdown_l0_files = 10_000,
            .stop_l0_files = 20_000,
            .slowdown_immutable_count = 10_000,
            .stop_immutable_count = 20_000,
        });
        defer eng_seed.close() catch {};

        var i: usize = 0;
        while (i < 20000) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            var value_buf: [512]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "bulk-{d:0>5}", .{i});
            const tail = try std.fmt.bufPrint(&value_buf, "{d:0>5}-", .{i});
            @memset(value_buf[tail.len..], 'y');
            try eng_seed.put(key, value_buf[0..]);
        }
    }

    const budget_backing = try allocator.alloc(u8, 4 * 1024 * 1024);
    defer allocator.free(budget_backing);
    var fba = std.heap.FixedBufferAllocator.init(budget_backing);

    tmp.dir.deleteFile("wal.log") catch {};

    var eng = try Engine.open(fba.allocator(), .{ .path = db_path });
    defer eng.close() catch {};

    const got = try eng.get("bulk-12345");
    defer eng.freeValue(got);

    try std.testing.expect(got.len > 0);
}

test "task 12.3 stress: large SST scanPrefix stays within bounded allocator budget" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng_seed = try Engine.open(allocator, .{
            .path = db_path,
            .memtable_max_bytes = 64 * 1024,
            .compaction_trigger_l0_files = 1000,
            .slowdown_l0_files = 10_000,
            .stop_l0_files = 20_000,
            .slowdown_immutable_count = 10_000,
            .stop_immutable_count = 20_000,
        });
        defer eng_seed.close() catch {};

        var i: usize = 0;
        while (i < 20000) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            var value_buf: [512]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "bulk-{d:0>5}", .{i});
            const tail = try std.fmt.bufPrint(&value_buf, "{d:0>5}-", .{i});
            @memset(value_buf[tail.len..], 'z');
            try eng_seed.put(key, value_buf[0..]);
        }
    }

    const budget_backing = try allocator.alloc(u8, 4 * 1024 * 1024);
    defer allocator.free(budget_backing);
    var fba = std.heap.FixedBufferAllocator.init(budget_backing);

    tmp.dir.deleteFile("wal.log") catch {};

    var eng = try Engine.open(fba.allocator(), .{ .path = db_path });
    defer eng.close() catch {};

    const rows = try eng.scanPrefix("bulk-12345");
    defer eng.freeScan(rows);

    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("bulk-12345", rows[0].key);
}

test "task 10.2 negative: unknown property returns typed error" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try std.testing.expectError(EngineError.UnknownProperty, eng.property("engine.unknown.key"));
}

test "task p1 positive: writeBatch applies multiple ops and survives reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path });
        defer eng.close() catch {};

        const ops = [_]BatchOp{
            .{ .op = .put, .key = "u:1", .value = "alice" },
            .{ .op = .put, .key = "u:2", .value = "bob" },
            .{ .op = .delete, .key = "u:3", .value = "" },
        };
        try eng.writeBatch(&ops);
    }

    {
        var reopened = try Engine.open(allocator, .{ .path = db_path });
        defer reopened.close() catch {};

        const v1 = try reopened.get("u:1");
        defer reopened.freeValue(v1);
        try std.testing.expectEqualStrings("alice", v1);

        const v2 = try reopened.get("u:2");
        defer reopened.freeValue(v2);
        try std.testing.expectEqualStrings("bob", v2);

        try std.testing.expectError(EngineError.KeyNotFound, reopened.get("u:3"));
    }
}

test "task p1 negative: crash after batch WAL append before mem apply replays exactly once" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path, .compaction_trigger_l0_files = 100 });
        defer eng.close() catch {};

        const seq = eng.next_sequence;
        const ops = [_]BatchOp{
            .{ .op = .put, .key = "mid:1", .value = "v1" },
            .{ .op = .put, .key = "mid:2", .value = "v2" },
        };

        const rec = try encodeBatchRecord(allocator, seq, &ops);
        defer allocator.free(rec);

        // Simulate kill window: WAL durable, memtable not applied.
        try appendFramed(&eng, rec);
    }

    {
        var reopened = try Engine.open(allocator, .{ .path = db_path });
        defer reopened.close() catch {};

        const a = try reopened.get("mid:1");
        defer reopened.freeValue(a);
        try std.testing.expectEqualStrings("v1", a);

        const b = try reopened.get("mid:2");
        defer reopened.freeValue(b);
        try std.testing.expectEqualStrings("v2", b);

        const seq_after = reopened.next_sequence;
        reopened.close() catch {};

        var reopened2 = try Engine.open(allocator, .{ .path = db_path });
        defer reopened2.close() catch {};
        try std.testing.expectEqual(seq_after, reopened2.next_sequence);
    }
}

test "task p1 negative: corrupted batch WAL fails open deterministically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try Engine.open(allocator, .{ .path = db_path });
        defer eng.close() catch {};

        const ops = [_]BatchOp{
            .{ .op = .put, .key = "c:1", .value = "x" },
            .{ .op = .put, .key = "c:2", .value = "y" },
        };
        try eng.writeBatch(&ops);
    }

    var wal_file = try tmp.dir.openFile("wal.log", .{ .mode = .read_write });
    defer wal_file.close();

    var bytes = try wal_file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(bytes);

    if (bytes.len > 8) bytes[8] ^= 0xFF;
    try wal_file.seekTo(0);
    try wal_file.writeAll(bytes);
    try wal_file.setEndPos(bytes.len);

    try std.testing.expectError(EngineError.CorruptedWalRecord, Engine.open(allocator, .{ .path = db_path }));
    try std.testing.expectError(EngineError.CorruptedWalRecord, Engine.open(allocator, .{ .path = db_path }));
}

test "task p1 positive: snapshot read remains stable while newer batch commits" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    const snap = try eng.beginSnapshot();

    const ops = [_]BatchOp{
        .{ .op = .put, .key = "snap:key", .value = "v2" },
        .{ .op = .put, .key = "other:key", .value = "x" },
    };
    try eng.writeBatch(&ops);

    try std.testing.expectError(EngineError.KeyNotFound, eng.getSnapshot(snap, "snap:key"));

    const latest = try eng.get("snap:key");
    defer eng.freeValue(latest);
    try std.testing.expectEqualStrings("v2", latest);

    eng.releaseSnapshot(snap);
}

test "task p1 regression: classifyError maps key engine errors" {
    try std.testing.expectEqual(ErrorClass.busy, classifyError(EngineError.WriteStall));
    try std.testing.expectEqual(ErrorClass.busy, classifyError(EngineError.LockHeld));
    try std.testing.expectEqual(ErrorClass.corruption, classifyError(EngineError.CorruptedWalRecord));
    try std.testing.expectEqual(ErrorClass.not_found, classifyError(EngineError.KeyNotFound));
    try std.testing.expectEqual(ErrorClass.retryable, classifyError(EngineError.EngineClosed));
}

test "task 10.2 positive: platform capability properties are exposed" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var eng = try Engine.open(allocator, .{ .path = db_path });
    defer eng.close() catch {};

    try std.testing.expect((try eng.property("engine.platform.os")).text.len > 0);
    try std.testing.expect((try eng.property("engine.platform.arch")).text.len > 0);
    try std.testing.expect((try eng.property("engine.platform.tier")).text.len > 0);
    try std.testing.expect((try eng.property("engine.platform.pointer_bits")).u64 >= 32);
    try std.testing.expect((try eng.property("engine.platform.supported")).u64 <= 1);
}
