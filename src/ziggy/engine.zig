const std = @import("std");
const builtin = @import("builtin");
const wal = @import("wal.zig");
const manifest = @import("manifest.zig");

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
};

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

pub const PropertyValue = union(enum) {
    u64: u64,
    text: []const u8,
};

pub const ScanItem = struct {
    key: []u8,
    value: []u8,
};

pub const EngineStats = struct {
    key_count: usize,
    next_sequence: u64,
    wal_size_bytes: u64,
};

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

    mutex: std.Thread.Mutex,

    pub fn open(allocator: std.mem.Allocator, config: Config) !Engine {
        if (config.path.len == 0) return EngineError.InvalidConfig;
        if (config.memtable_max_bytes == 0) return EngineError.InvalidConfig;
        if (config.scheduler_queue_limit == 0) return EngineError.InvalidConfig;
        _ = try validateCompactionRate(@as(i64, @intCast(config.compaction_rate_bytes_per_sec)));
        if (config.slowdown_l0_files > config.stop_l0_files) return EngineError.InvalidConfig;
        if (config.slowdown_immutable_count > config.stop_immutable_count) return EngineError.InvalidConfig;

        var dir = try std.fs.cwd().makeOpenPath(config.path, .{ .iterate = true });
        errdefer dir.close();

        const lock_file = try acquireLock(dir);
        errdefer lock_file.close();

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
            .mutex = .{},
        };

        return engine;
    }

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

    pub fn put(self: *Engine, key: []const u8, value: []const u8) !void {
        if (key.len > self.max_key_size) return error.KeyTooLarge;
        try self.ensureWorkersStarted();
        const op_start: u64 = @intCast(std.time.nanoTimestamp());

        var slowed_once = false;
        while (true) {
            self.mutex.lock();
            try self.ensureOpenLocked();
            const pre = self.preWriteActionLocked();
            if (pre == .stop) {
                self.metrics.stall_count += 1;
                self.mutex.unlock();
                return EngineError.WriteStall;
            }
            if (pre == .slowdown and !slowed_once) {
                slowed_once = true;
                self.mutex.unlock();
                const start = std.time.nanoTimestamp();
                std.Thread.sleep(1 * std.time.ns_per_ms);
                const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
                self.mutex.lock();
                self.metrics.stall_duration_ns_total += elapsed;
                self.mutex.unlock();
                continue;
            }

            const seq = self.next_sequence;
            const encoded = try encodeRecord(self.allocator, .put, seq, key, value);
            defer self.allocator.free(encoded);

            const start = std.time.nanoTimestamp();
            try appendFramed(self, encoded);
            self.metrics.wal_bytes_appended += @as(u64, @intCast(encoded.len));
            try applyMutation(self, seq, .put, key, value);
            self.next_sequence += 1;
            self.metrics.put_count += 1;
            const elapsed_ns: u64 = @intCast(std.time.nanoTimestamp() - start);
            self.metrics.write_latency_ns_total += elapsed_ns;
            if (elapsed_ns > self.metrics.write_latency_ns_max) self.metrics.write_latency_ns_max = elapsed_ns;
            self.metrics.put_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));

            try self.maybeRotateActiveLocked();
            self.refreshStallStateLocked();

            self.mutex.unlock();
            return;
        }
    }

    pub fn delete(self: *Engine, key: []const u8) !void {
        try self.ensureWorkersStarted();
        const op_start: u64 = @intCast(std.time.nanoTimestamp());
        var slowed_once = false;
        while (true) {
            self.mutex.lock();
            try self.ensureOpenLocked();
            const pre = self.preWriteActionLocked();
            if (pre == .stop) {
                self.metrics.stall_count += 1;
                self.mutex.unlock();
                return EngineError.WriteStall;
            }
            if (pre == .slowdown and !slowed_once) {
                slowed_once = true;
                self.mutex.unlock();
                const start = std.time.nanoTimestamp();
                std.Thread.sleep(1 * std.time.ns_per_ms);
                const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
                self.mutex.lock();
                self.metrics.stall_duration_ns_total += elapsed;
                self.mutex.unlock();
                continue;
            }

            const seq = self.next_sequence;
            const encoded = try encodeRecord(self.allocator, .delete, seq, key, "");
            defer self.allocator.free(encoded);

            const start = std.time.nanoTimestamp();
            try appendFramed(self, encoded);
            self.metrics.wal_bytes_appended += @as(u64, @intCast(encoded.len));
            try applyMutation(self, seq, .delete, key, "");
            self.next_sequence += 1;
            self.metrics.delete_count += 1;
            const elapsed_ns: u64 = @intCast(std.time.nanoTimestamp() - start);
            self.metrics.write_latency_ns_total += elapsed_ns;
            if (elapsed_ns > self.metrics.write_latency_ns_max) self.metrics.write_latency_ns_max = elapsed_ns;
            self.metrics.delete_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));

            try self.maybeRotateActiveLocked();
            self.refreshStallStateLocked();

            self.mutex.unlock();
            return;
        }
    }

    pub fn get(self: *Engine, key: []const u8) ![]u8 {
        return self.getInternal(key, null, true);
    }

    fn getAtSnapshot(self: *Engine, key: []const u8, snapshot_seq: u64) ![]u8 {
        return self.getInternal(key, snapshot_seq, false);
    }

    fn getInternal(self: *Engine, key: []const u8, snapshot_seq: ?u64, count_metrics: bool) ![]u8 {
        const op_start: u64 = @intCast(std.time.nanoTimestamp());
        self.mutex.lock();
        try self.ensureOpenLocked();
        if (count_metrics) self.metrics.get_count += 1;

        if (lookupVisibleMap(&self.active_mem, key, snapshot_seq)) |entry| {
            const res = entryToGetResult(self, entry);
            if (res) |v| {
                if (count_metrics) self.metrics.get_hit_count += 1;
                if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
                self.mutex.unlock();
                return v;
            }
            if (count_metrics) self.metrics.get_miss_count += 1;
            if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
            self.mutex.unlock();
            return EngineError.KeyNotFound;
        }

        var idx = self.immutables.items.len;
        while (idx > 0) {
            idx -= 1;
            if (lookupVisibleMap(&self.immutables.items[idx].map, key, snapshot_seq)) |entry| {
                const res = entryToGetResult(self, entry);
                if (res) |v| {
                    if (count_metrics) self.metrics.get_hit_count += 1;
                    if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
                    self.mutex.unlock();
                    return v;
                }
                if (count_metrics) self.metrics.get_miss_count += 1;
                if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
                self.mutex.unlock();
                return EngineError.KeyNotFound;
            }
        }

        var snapshot = try self.version.clone(self.allocator);
        self.mutex.unlock();
        defer snapshot.deinit();

        const disk_entry = try lookupDisk(self.allocator, self.dir, &snapshot, key, snapshot_seq);
        if (disk_entry) |entry| {
            defer if (entry.value) |v| self.allocator.free(v);
            if (entry.tombstone) {
                self.mutex.lock();
                if (count_metrics) self.metrics.get_miss_count += 1;
                if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
                self.mutex.unlock();
                return EngineError.KeyNotFound;
            }
            const out = try self.allocator.dupe(u8, entry.value orelse "");
            self.mutex.lock();
            if (count_metrics) self.metrics.get_hit_count += 1;
            if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
            self.mutex.unlock();
            return out;
        }

        self.mutex.lock();
        if (count_metrics) self.metrics.get_miss_count += 1;
        if (count_metrics) self.metrics.get_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
        self.mutex.unlock();
        return EngineError.KeyNotFound;
    }

    pub fn freeValue(self: *Engine, value: []u8) void {
        self.allocator.free(value);
    }

    pub fn scanPrefix(self: *Engine, prefix: []const u8) ![]ScanItem {
        const op_start: u64 = @intCast(std.time.nanoTimestamp());
        self.mutex.lock();
        try self.ensureOpenLocked();
        self.metrics.scan_count += 1;

        var selected = std.StringHashMap(ValueEntry).init(self.allocator);
        errdefer deinitMap(self.allocator, &selected);

        try mergeMapByPrefix(self.allocator, &selected, &self.active_mem, prefix);
        for (self.immutables.items) |*im| {
            try mergeMapByPrefix(self.allocator, &selected, &im.map, prefix);
        }

        var snapshot = try self.version.clone(self.allocator);
        self.mutex.unlock();
        defer snapshot.deinit();

        try mergeDiskByPrefix(self.allocator, self.dir, &snapshot, prefix, &selected);

        var out = std.ArrayList(ScanItem).empty;
        errdefer {
            for (out.items) |it| {
                self.allocator.free(it.key);
                self.allocator.free(it.value);
            }
            out.deinit(self.allocator);
        }

        var it = selected.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.tombstone) continue;
            try out.append(self.allocator, .{
                .key = try self.allocator.dupe(u8, entry.key_ptr.*),
                .value = try self.allocator.dupe(u8, entry.value_ptr.value orelse ""),
            });
        }

        std.mem.sort(ScanItem, out.items, {}, struct {
            fn lessThan(_: void, a: ScanItem, b: ScanItem) bool {
                return std.mem.order(u8, a.key, b.key) == .lt;
            }
        }.lessThan);

        deinitMap(self.allocator, &selected);
        const result = try out.toOwnedSlice(self.allocator);
        self.mutex.lock();
        self.metrics.scan_latency.observeNs(@intCast(std.time.nanoTimestamp() - op_start));
        self.mutex.unlock();
        return result;
    }

    pub fn freeScan(self: *Engine, items: []ScanItem) void {
        for (items) |it| {
            self.allocator.free(it.key);
            self.allocator.free(it.value);
        }
        self.allocator.free(items);
    }

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

    pub fn property(self: *Engine, name: []const u8) !PropertyValue {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.ensureOpenLocked();

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
            if (!std.mem.startsWith(u8, entry.name, "MANIFEST-")) continue;
            try copyFile(self.allocator, self.dir, tmp_dst, entry.name);
            if (std.mem.startsWith(u8, entry.name, "sst-")) try copyFile(self.allocator, self.dir, tmp_dst, entry.name);
        }

        try renameNoReplace(self.allocator, parent, tmp_name, dst_name);
    }

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

    fn ensureOpenLocked(self: *Engine) !void {
        if (self.closed) return EngineError.EngineClosed;
    }

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

    fn signalFlushWorker(self: *Engine) void {
        self.flush_worker_mutex.lock();
        self.flush_pending = true;
        self.flush_worker_cond.signal();
        self.flush_worker_mutex.unlock();
    }

    fn signalCompactionWorker(self: *Engine) void {
        self.compaction_worker_mutex.lock();
        self.compaction_pending = true;
        self.compaction_worker_cond.signal();
        self.compaction_worker_mutex.unlock();
    }

    fn enqueueFlushLocked(self: *Engine, mem_id: u64) !void {
        if (self.flush_queue.items.len >= self.queue_limit) {
            self.stall_state = .stop;
            self.stall_reason = .scheduler_queue_full;
            return EngineError.WriteStall;
        }
        try self.flush_queue.append(self.allocator, mem_id);
        self.signalFlushWorker();
    }

    fn enqueueCompactionLocked(self: *Engine, level: u8) !void {
        if (self.compaction_queue.items.len >= self.queue_limit) return;
        for (self.compaction_queue.items) |existing| if (existing == level) return;
        try self.compaction_queue.append(self.allocator, level);
        self.signalCompactionWorker();
    }

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

    fn preWriteActionLocked(self: *Engine) PreWrite {
        self.refreshStallStateLocked();
        return switch (self.stall_state) {
            .normal => .proceed,
            .slowdown => .slowdown,
            .stop => .stop,
        };
    }

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

    fn popFlushJob(self: *Engine) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.flush_queue.items.len == 0) return null;
        return self.flush_queue.orderedRemove(0);
    }

    fn popCompactionJob(self: *Engine) ?u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.compaction_queue.items.len == 0) return null;
        return self.compaction_queue.orderedRemove(0);
    }

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

const SstEntry = struct {
    key: []u8,
    seq: u64,
    tombstone: bool,
    value: ?[]u8,
};

const Decoded = struct {
    seq: u64,
    op: Op,
    key: []const u8,
    value: []const u8,
};

fn lookupMap(map: *std.StringHashMap(ValueEntry), key: []const u8) ?ValueEntry {
    return map.get(key);
}

fn lookupVisibleMap(map: *std.StringHashMap(ValueEntry), key: []const u8, snapshot_seq: ?u64) ?ValueEntry {
    const entry = lookupMap(map, key) orelse return null;
    if (snapshot_seq) |snap| {
        if (entry.seq > snap) return null;
    }
    return entry;
}

fn entryToGetResult(self: *Engine, entry: ValueEntry) ?[]u8 {
    if (entry.tombstone) return null;
    return self.allocator.dupe(u8, entry.value orelse "") catch null;
}

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

fn pendingCompactionBytesLocked(self: *const Engine) u64 {
    var total: u64 = 0;
    for (self.version.levels[0].items) |f| total += f.size;
    return total;
}

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

fn validateSstSet(allocator: std.mem.Allocator, dir: std.fs.Dir, version: *manifest.VersionSet) !void {
    for (0..manifest.MAX_LEVELS) |level| {
        for (version.levels[level].items) |f| {
            var name_buf: [64]u8 = undefined;
            const file_name = try std.fmt.bufPrint(&name_buf, "sst-{d}.sst", .{f.file_number});
            const bytes = dir.readFileAlloc(allocator, file_name, 256) catch |err| switch (err) {
                error.FileNotFound => return EngineError.UnsupportedFormat,
                else => return err,
            };
            defer allocator.free(bytes);

            const eol = std.mem.indexOfScalar(u8, bytes, '\n') orelse bytes.len;
            try validateSstHeaderLine(bytes[0..eol]);
        }
    }
}

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

fn findLevelCandidate(files: []manifest.FileMeta, key: []const u8) ?manifest.FileMeta {
    for (files) |f| if (keyInRange(key, f.min_key, f.max_key)) return f;
    return null;
}

fn keyInRange(key: []const u8, min: []const u8, max: []const u8) bool {
    if (std.mem.order(u8, key, min) == .lt) return false;
    if (std.mem.order(u8, key, max) == .gt) return false;
    return true;
}

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

fn deleteSstFile(dir: std.fs.Dir, file_number: u64) void {
    var name_buf: [64]u8 = undefined;
    const file_name = std.fmt.bufPrint(&name_buf, "sst-{d}.sst", .{file_number}) catch return;
    dir.deleteFile(file_name) catch {};
}

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

fn decodeRecord(bytes: []const u8) !Decoded {
    if (bytes.len < 15) return EngineError.CorruptedWalRecord;

    const seq = @as(u64, bytes[0]) |
        (@as(u64, bytes[1]) << 8) |
        (@as(u64, bytes[2]) << 16) |
        (@as(u64, bytes[3]) << 24) |
        (@as(u64, bytes[4]) << 32) |
        (@as(u64, bytes[5]) << 40) |
        (@as(u64, bytes[6]) << 48) |
        (@as(u64, bytes[7]) << 56);

    const op: Op = switch (bytes[8]) {
        1 => .put,
        2 => .delete,
        else => return EngineError.CorruptedWalRecord,
    };

    const key_len = @as(usize, bytes[9]) | (@as(usize, bytes[10]) << 8);
    const value_len = @as(usize, bytes[11]) |
        (@as(usize, bytes[12]) << 8) |
        (@as(usize, bytes[13]) << 16) |
        (@as(usize, bytes[14]) << 24);

    const key_start = 15;
    const key_end = key_start + key_len;
    const value_end = key_end + value_len;
    if (value_end > bytes.len) return EngineError.CorruptedWalRecord;

    return .{ .seq = seq, .op = op, .key = bytes[key_start..key_end], .value = bytes[key_end..value_end] };
}

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
        const d = decodeRecord(record) catch return EngineError.CorruptedWalRecord;
        try applyMapMutation(allocator, map, bytes_out, d.seq, d.op, d.key, d.value);
        if (d.seq > max_seq_seen.*) max_seq_seen.* = d.seq;
    }
}

fn applyMutation(self: *Engine, seq: u64, op: Op, key: []const u8, value: []const u8) !void {
    try applyMapMutation(self.allocator, &self.active_mem, &self.active_mem_bytes, seq, op, key, value);
}

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

fn deinitMap(allocator: std.mem.Allocator, map: *std.StringHashMap(ValueEntry)) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        if (entry.value_ptr.value) |v| allocator.free(v);
    }
    map.deinit();
}

fn validateCompactionRate(rate: i64) !u64 {
    if (rate <= 0) return EngineError.InvalidConfig;
    return @intCast(rate);
}

fn walFragmentChecksum(ty: wal.FragmentType, payload: []const u8) u32 {
    var hasher = std.hash.crc.Crc32Iscsi.init();
    const ty_byte: [1]u8 = .{@intFromEnum(ty)};
    hasher.update(&ty_byte);
    hasher.update(payload);
    return hasher.final();
}

fn acquireLock(dir: std.fs.Dir) !std.fs.File {
    const file = try dir.createFile("LOCK", .{ .truncate = false, .read = true });
    std.posix.flock(file.handle, std.posix.LOCK.EX | std.posix.LOCK.NB) catch return EngineError.LockHeld;
    return file;
}

fn copyFile(allocator: std.mem.Allocator, src_dir: std.fs.Dir, dst_dir: std.fs.Dir, name: []const u8) !void {
    const bytes = try src_dir.readFileAlloc(allocator, name, 128 * 1024 * 1024);
    defer allocator.free(bytes);

    var file = try dst_dir.createFile(name, .{ .exclusive = true });
    defer file.close();
    try file.writeAll(bytes);
    try file.sync();
}

fn containsParentTraversal(path: []const u8) bool {
    var it = std.mem.tokenizeScalar(u8, path, '/');
    while (it.next()) |part| if (std.mem.eql(u8, part, "..")) return true;
    return false;
}

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

fn renameNoReplace(allocator: std.mem.Allocator, parent: std.fs.Dir, old_name: []const u8, new_name: []const u8) !void {
    if (builtin.os.tag != .linux) return EngineError.UnsupportedPlatform;

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
