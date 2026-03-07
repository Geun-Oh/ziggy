//! Standalone compaction manager used to merge SSTables and test background compaction behavior.
const std = @import("std");
const builtin = @import("builtin");

pub const CompactionError = error{
    DiskFull,
    OutOfMemory,
    WriteFailed,
};

pub const WorkerError = error{
    WorkerAlreadyRunning,
};

// One versioned SSTable entry used by the compaction simulator.
pub const Entry = struct {
    key: []u8,
    value: []u8,
    sequence: u64,
    tombstone: bool,

    // Deep-copies an entry so compaction outputs own their memory.
    pub fn clone(self: Entry, allocator: std.mem.Allocator) !Entry {
        return .{
            .key = try allocator.dupe(u8, self.key),
            .value = try allocator.dupe(u8, self.value),
            .sequence = self.sequence,
            .tombstone = self.tombstone,
        };
    }

    // Releases key and value buffers held by the entry.
    pub fn deinit(self: Entry, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }
};

// In-memory SSTable model with key-range metadata and ordered entries.
pub const SSTable = struct {
    id: u64,
    level: u8,
    min_key: []u8,
    max_key: []u8,
    entries: std.ArrayList(Entry),

    // Creates an empty table shell with owned key-range metadata.
    pub fn init(allocator: std.mem.Allocator, id: u64, level: u8, min_key: []const u8, max_key: []const u8) !SSTable {
        return .{
            .id = id,
            .level = level,
            .min_key = try allocator.dupe(u8, min_key),
            .max_key = try allocator.dupe(u8, max_key),
            .entries = .empty,
        };
    }

    // Frees the range metadata and all owned entries.
    pub fn deinit(self: *SSTable, allocator: std.mem.Allocator) void {
        allocator.free(self.min_key);
        allocator.free(self.max_key);
        for (self.entries.items) |e| e.deinit(allocator);
        self.entries.deinit(allocator);
    }

    // Returns a rough size estimate used for level pressure decisions.
    pub fn estimatedBytes(self: *const SSTable) usize {
        var total: usize = 0;
        for (self.entries.items) |e| total += e.key.len + e.value.len + 16;
        return total;
    }
};

pub const MAX_LEVELS = 7;

// Owns per-level SSTable sets and optional background compaction workers.
pub const CompactionManager = struct {
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
    levels: [MAX_LEVELS]std.ArrayList(SSTable),
    base_level_bytes: usize,
    next_file_id: u64,
    manifest_updates: usize,
    compaction_runs: usize,
    inject_disk_full: bool,
    inject_write_failure: bool,
    allow_env_failure_injection: bool,
    storage_dir: ?std.fs.Dir,
    last_error: ?CompactionError,
    last_error_os: ?anyerror,
    error_log_count: usize,

    worker_thread: ?std.Thread,
    worker_stop: std.atomic.Value(bool),
    worker_running: std.atomic.Value(bool),
    worker_interval_ns: u64,
    worker_wait_mutex: std.Thread.Mutex,
    worker_pending: bool,
    worker_cond: std.Thread.Condition,
    worker_notify_count: std.atomic.Value(usize),
    worker_wake_count: std.atomic.Value(usize),

    // Creates an in-memory manager with no on-disk persistence.
    pub fn init(allocator: std.mem.Allocator, base_level_bytes: usize) CompactionManager {
        return initOptions(allocator, base_level_bytes, null, false);
    }

    // Creates a manager that mirrors SSTables and manifest records into a storage directory.
    pub fn initWithStorage(allocator: std.mem.Allocator, base_level_bytes: usize, storage_dir: std.fs.Dir) CompactionManager {
        return initOptions(allocator, base_level_bytes, storage_dir, false);
    }

    pub fn initWithStorageForTests(allocator: std.mem.Allocator, base_level_bytes: usize, storage_dir: std.fs.Dir) CompactionManager {
        return initOptions(allocator, base_level_bytes, storage_dir, true);
    }

    // Shared constructor that sets up worker state and optional on-disk mirroring.
    fn initOptions(
        allocator: std.mem.Allocator,
        base_level_bytes: usize,
        storage_dir: ?std.fs.Dir,
        allow_env_failure_injection: bool,
    ) CompactionManager {
        var levels: [MAX_LEVELS]std.ArrayList(SSTable) = undefined;
        for (&levels) |*l| l.* = .empty;
        return .{
            .allocator = allocator,
            .mutex = .{},
            .levels = levels,
            .base_level_bytes = base_level_bytes,
            .next_file_id = 1000,
            .manifest_updates = 0,
            .compaction_runs = 0,
            .inject_disk_full = false,
            .inject_write_failure = false,
            .allow_env_failure_injection = allow_env_failure_injection,
            .storage_dir = storage_dir,
            .last_error = null,
            .last_error_os = null,
            .error_log_count = 0,
            .worker_thread = null,
            .worker_stop = std.atomic.Value(bool).init(false),
            .worker_running = std.atomic.Value(bool).init(false),
            .worker_interval_ns = 10 * std.time.ns_per_ms,
            .worker_wait_mutex = .{},
            .worker_pending = false,
            .worker_cond = .{},
            .worker_notify_count = std.atomic.Value(usize).init(0),
            .worker_wake_count = std.atomic.Value(usize).init(0),
        };
    }

    // Stops background work and releases every table in every level.
    pub fn deinit(self: *CompactionManager) void {
        self.stopBackground();

        for (&self.levels) |*level| {
            for (level.items) |*table| table.deinit(self.allocator);
            level.deinit(self.allocator);
        }
    }

    // Allows tests to opt into env-driven fault injection.
    pub fn setEnvFailureInjectionEnabledForTests(self: *CompactionManager, enabled: bool) void {
        if (builtin.is_test) self.allow_env_failure_injection = enabled;
    }

    // Inserts a table into a level and wakes the worker if compaction might now be needed.
    pub fn addTable(self: *CompactionManager, level: usize, table: SSTable) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.levels[level].append(self.allocator, table);
        if (self.storage_dir != null) {
            const table_ref = &self.levels[level].items[self.levels[level].items.len - 1];
            try self.writeTableToFile(table_ref, "sst");
        }

        self.notifyWorker();
    }

    pub fn levelTableCount(self: *CompactionManager, level: usize) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.levels[level].items.len;
    }

    pub fn isWorkerRunning(self: *CompactionManager) bool {
        return self.worker_running.load(.acquire);
    }

    pub fn workerNotifyCount(self: *CompactionManager) usize {
        return self.worker_notify_count.load(.acquire);
    }

    pub fn workerWakeCount(self: *CompactionManager) usize {
        return self.worker_wake_count.load(.acquire);
    }

    pub fn totalTableCount(self: *CompactionManager) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var total: usize = 0;
        for (self.levels) |level| total += level.items.len;
        return total;
    }

    pub fn manifestUpdateCount(self: *CompactionManager) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.manifest_updates;
    }

    pub fn compactionRunCount(self: *CompactionManager) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.compaction_runs;
    }

    pub fn errorLogCount(self: *CompactionManager) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.error_log_count;
    }

    pub fn lastLoggedOsError(self: *CompactionManager) ?anyerror {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.last_error_os;
    }

    // Starts a background thread that periodically or reactively compacts eligible levels.
    pub fn startBackground(self: *CompactionManager, interval_ms: u64) WorkerError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.worker_thread != null) return WorkerError.WorkerAlreadyRunning;

        self.worker_interval_ns = interval_ms * std.time.ns_per_ms;
        self.worker_stop.store(false, .release);
        self.worker_wait_mutex.lock();
        self.worker_pending = false;
        self.worker_wait_mutex.unlock();
        self.worker_thread = std.Thread.spawn(.{}, workerMain, .{self}) catch return WorkerError.WorkerAlreadyRunning;
    }

    // Stops and joins the background worker if it is running.
    pub fn stopBackground(self: *CompactionManager) void {
        self.worker_stop.store(true, .release);
        self.notifyWorker();

        self.mutex.lock();
        const maybe_thread = self.worker_thread;
        self.worker_thread = null;
        self.mutex.unlock();

        if (maybe_thread) |t| t.join();
    }

    // Sets the pending flag and wakes the background compaction worker.
    fn notifyWorker(self: *CompactionManager) void {
        self.worker_wait_mutex.lock();
        self.worker_pending = true;
        _ = self.worker_notify_count.fetchAdd(1, .acq_rel);
        self.worker_cond.signal();
        self.worker_wait_mutex.unlock();
    }

    // Repeatedly attempts compaction and then waits for a signal or timeout.
    fn workerMain(self: *CompactionManager) void {
        self.worker_running.store(true, .release);
        defer self.worker_running.store(false, .release);

        while (!self.worker_stop.load(.acquire)) {
            _ = self.compactOnePass() catch |err| {
                self.mutex.lock();
                self.last_error = err;
                self.mutex.unlock();
            };

            if (self.worker_stop.load(.acquire)) break;

            self.worker_wait_mutex.lock();
            while (!self.worker_stop.load(.acquire) and !self.worker_pending) {
                _ = self.worker_cond.timedWait(&self.worker_wait_mutex, self.worker_interval_ns) catch {};
                if (!self.worker_pending) break;
            }
            const had_pending = self.worker_pending;
            self.worker_pending = false;
            self.worker_wait_mutex.unlock();

            if (had_pending) {
                _ = self.worker_wake_count.fetchAdd(1, .acq_rel);
            }
        }
    }

    // Records the last compaction failure for tests and diagnostics.
    fn logCompactionFailure(self: *CompactionManager, err: CompactionError, os_err: anyerror) void {
        std.log.warn("compaction failed: {s} (os: {s})", .{ @errorName(err), @errorName(os_err) });
        self.last_error = err;
        self.last_error_os = os_err;
        self.error_log_count += 1;
    }

    // Computes the target size budget for one level.
    fn levelTarget(self: *const CompactionManager, level: usize) usize {
        var out = self.base_level_bytes;
        var i: usize = 0;
        while (i < level) : (i += 1) out *= 10;
        return out;
    }

    // Sums the estimated byte sizes of all tables currently in a level.
    fn levelBytes(self: *const CompactionManager, level: usize) usize {
        var total: usize = 0;
        for (self.levels[level].items) |*table| total += table.estimatedBytes();
        return total;
    }

    // Returns whether two key ranges intersect.
    fn overlaps(a: *const SSTable, b: *const SSTable) bool {
        return !(std.mem.order(u8, a.max_key, b.min_key) == .lt or std.mem.order(u8, b.max_key, a.min_key) == .lt);
    }

    // Checks whether a key still exists below the compaction output level before dropping tombstones.
    fn keyExistsDeeper(self: *CompactionManager, start_level: usize, key: []const u8) bool {
        var level = start_level;
        while (level < MAX_LEVELS) : (level += 1) {
            for (self.levels[level].items) |table| {
                for (table.entries.items) |e| {
                    if (std.mem.eql(u8, e.key, key)) return true;
                }
            }
        }
        return false;
    }

    // Enables env-based faults only for test builds that explicitly opt in.
    fn shouldHonorEnvFailureInjection(self: *const CompactionManager) bool {
        return builtin.is_test and self.allow_env_failure_injection;
    }

    // Formats the mirrored on-disk file name for a table id and extension.
    fn tableFileName(table_id: u64, ext: []const u8, out: []u8) ![]const u8 {
        return std.fmt.bufPrint(out, "sst-{d}.{s}", .{ table_id, ext });
    }

    // Mirrors one in-memory table to disk using the simple test-friendly text format.
    fn writeTableToFile(self: *CompactionManager, table: *const SSTable, ext: []const u8) !void {
        const dir = self.storage_dir orelse return;

        var name_buf: [64]u8 = undefined;
        const file_name = try tableFileName(table.id, ext, &name_buf);

        var f = try dir.createFile(file_name, .{ .truncate = true });
        defer f.close();

        var wbuf: [256]u8 = undefined;
        var writer = f.writer(&wbuf);
        const w = &writer.interface;

        try w.print("id={d} level={d}\n", .{ table.id, table.level });
        for (table.entries.items) |e| {
            try w.print("{s}|{s}|{d}|{d}\n", .{ e.key, e.value, e.sequence, @intFromBool(e.tombstone) });
        }
        try w.flush();
        try f.sync();
    }

    // Best-effort removal of one mirrored table file variant.
    fn deleteTableFileExt(self: *CompactionManager, table_id: u64, ext: []const u8) void {
        const dir = self.storage_dir orelse return;
        var name_buf: [64]u8 = undefined;
        const file_name = tableFileName(table_id, ext, &name_buf) catch return;
        dir.deleteFile(file_name) catch {};
    }

    // Deletes the finalized .sst file for a table.
    fn deleteTableFile(self: *CompactionManager, table_id: u64) void {
        self.deleteTableFileExt(table_id, "sst");
    }

    // Renames a temporary compaction output into its finalized SST file name.
    fn finalizeTempTableFile(self: *CompactionManager, table_id: u64) !void {
        const dir = self.storage_dir orelse return;

        var tmp_buf: [64]u8 = undefined;
        var sst_buf: [64]u8 = undefined;
        const tmp_name = try tableFileName(table_id, "tmp", &tmp_buf);
        const sst_name = try tableFileName(table_id, "sst", &sst_buf);
        try dir.rename(tmp_name, sst_name);
    }

    // Appends a human-readable compaction record to the mirrored MANIFEST file.
    fn appendManifestRecord(self: *CompactionManager, level: usize, new_id: u64, old_ids: []const u64) !void {
        const dir = self.storage_dir orelse return;

        var manifest = try dir.createFile("MANIFEST", .{ .read = true });
        defer manifest.close();

        try manifest.seekFromEnd(0);
        var wbuf: [256]u8 = undefined;
        var writer = manifest.writer(&wbuf);
        const w = &writer.interface;

        try w.print("COMPACT L{d} -> L{d} new={d} drop=", .{ level, level + 1, new_id });
        for (old_ids, 0..) |id, idx| {
            if (idx > 0) try w.writeAll(",");
            try w.print("{d}", .{id});
        }
        try w.writeAll("\n");
        try w.flush();
        try manifest.sync();
    }

    // Finds the first oversized level and tries a single compaction pass.
    pub fn compactOnePass(self: *CompactionManager) CompactionError!bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        var level: usize = 0;
        while (level + 1 < MAX_LEVELS) : (level += 1) {
            if (self.levelBytes(level) > self.levelTarget(level) and self.levels[level].items.len > 0) {
                return self.maybeCompactLocked(level);
            }
        }
        return false;
    }

    // Tries to compact one explicit level while holding the manager mutex.
    pub fn maybeCompact(self: *CompactionManager, level: usize) CompactionError!bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.maybeCompactLocked(level);
    }

    // Merges one source table with overlapping next-level tables and atomically swaps them in memory.
    fn maybeCompactLocked(self: *CompactionManager, level: usize) CompactionError!bool {
        if (level + 1 >= MAX_LEVELS) return false;
        if (self.levelBytes(level) <= self.levelTarget(level)) return false;
        if (self.levels[level].items.len == 0) return false;

        const source_index: usize = 0;
        const source = &self.levels[level].items[source_index];

        var overlap_idx = std.ArrayList(usize).empty;
        defer overlap_idx.deinit(self.allocator);
        for (self.levels[level + 1].items, 0..) |*candidate, idx| {
            if (overlaps(source, candidate)) {
                try overlap_idx.append(self.allocator, idx);
            }
        }

        var merged = std.StringHashMap(Entry).init(self.allocator);
        defer {
            var it = merged.iterator();
            while (it.next()) |e| {
                self.allocator.free(e.key_ptr.*);
                e.value_ptr.deinit(self.allocator);
            }
            merged.deinit();
        }

        const insertEntry = struct {
            fn apply(map: *std.StringHashMap(Entry), allocator: std.mem.Allocator, in: Entry) !void {
                if (map.getPtr(in.key)) |existing| {
                    if (in.sequence > existing.sequence) {
                        existing.deinit(allocator);
                        existing.* = try in.clone(allocator);
                    }
                    return;
                }
                const key_copy = try allocator.dupe(u8, in.key);
                errdefer allocator.free(key_copy);
                try map.put(key_copy, try in.clone(allocator));
            }
        }.apply;

        for (source.entries.items) |e| try insertEntry(&merged, self.allocator, e);
        for (overlap_idx.items) |idx| {
            for (self.levels[level + 1].items[idx].entries.items) |e| try insertEntry(&merged, self.allocator, e);
        }

        var keys = std.ArrayList([]u8).empty;
        defer {
            for (keys.items) |k| self.allocator.free(k);
            keys.deinit(self.allocator);
        }

        var it = merged.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.tombstone and !self.keyExistsDeeper(level + 2, entry.key_ptr.*)) {
                continue;
            }
            try keys.append(self.allocator, try self.allocator.dupe(u8, entry.key_ptr.*));
        }

        std.mem.sort([]u8, keys.items, {}, struct {
            fn lessThan(_: void, a: []u8, b: []u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);

        const honor_env_injection = self.shouldHonorEnvFailureInjection();
        const disk_full = if (honor_env_injection) std.process.getEnvVarOwned(self.allocator, "ZIGGY_MOCK_DISK_FULL") catch null else null;
        defer if (disk_full) |v| self.allocator.free(v);

        if (keys.items.len == 0) return false;

        var out = try SSTable.init(self.allocator, self.next_file_id, @intCast(level + 1), keys.items[0], keys.items[keys.items.len - 1]);
        errdefer out.deinit(self.allocator);
        self.next_file_id += 1;

        for (keys.items) |k| {
            const e = merged.get(k).?;
            try out.entries.append(self.allocator, try e.clone(self.allocator));
        }

        if (self.storage_dir != null) {
            self.writeTableToFile(&out, "tmp") catch |os_err| {
                self.logCompactionFailure(CompactionError.WriteFailed, os_err);
                return CompactionError.WriteFailed;
            };
        }
        errdefer self.deleteTableFileExt(out.id, "tmp");

        if (self.inject_disk_full or disk_full != null) {
            self.logCompactionFailure(CompactionError.DiskFull, error.NoSpaceLeft);
            return CompactionError.DiskFull;
        }

        const write_fail = if (honor_env_injection) std.process.getEnvVarOwned(self.allocator, "ZIGGY_MOCK_COMPACTION_WRITE_FAIL") catch null else null;
        defer if (write_fail) |v| self.allocator.free(v);
        if (self.inject_write_failure or write_fail != null) {
            self.logCompactionFailure(CompactionError.WriteFailed, error.InputOutput);
            return CompactionError.WriteFailed;
        }

        if (self.storage_dir != null) {
            self.finalizeTempTableFile(out.id) catch |os_err| {
                self.logCompactionFailure(CompactionError.WriteFailed, os_err);
                return CompactionError.WriteFailed;
            };
        }

        var removed_ids = std.ArrayList(u64).empty;
        defer removed_ids.deinit(self.allocator);
        try removed_ids.append(self.allocator, source.id);
        for (overlap_idx.items) |idx| {
            try removed_ids.append(self.allocator, self.levels[level + 1].items[idx].id);
        }

        self.appendManifestRecord(level, out.id, removed_ids.items) catch |os_err| {
            self.deleteTableFileExt(out.id, "tmp");
            self.deleteTableFile(out.id);
            self.logCompactionFailure(CompactionError.WriteFailed, os_err);
            return CompactionError.WriteFailed;
        };

        // Remove old L table.
        var removed_l = self.levels[level].orderedRemove(source_index);
        defer removed_l.deinit(self.allocator);

        // Remove overlapping L+1 tables from highest index downward.
        var rev = overlap_idx.items.len;
        while (rev > 0) : (rev -= 1) {
            const idx = overlap_idx.items[rev - 1];
            var removed = self.levels[level + 1].orderedRemove(idx);
            defer removed.deinit(self.allocator);
        }

        try self.levels[level + 1].append(self.allocator, out);

        for (removed_ids.items) |id| self.deleteTableFile(id);

        self.manifest_updates += 1;
        self.compaction_runs += 1;
        self.last_error = null;
        self.last_error_os = null;
        return true;
    }
};

test "task 6.1 positive: L0 overflow triggers compaction into L1 and removes old L0" {
    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    var l0 = try SSTable.init(std.testing.allocator, 1, 0, "a", "z");
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "a"), .value = try std.testing.allocator.dupe(u8, "1"), .sequence = 1, .tombstone = false });
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "b"), .value = try std.testing.allocator.dupe(u8, "2"), .sequence = 2, .tombstone = false });
    try cm.addTable(0, l0);

    var l1_overlap = try SSTable.init(std.testing.allocator, 2, 1, "b", "y");
    try l1_overlap.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "b"), .value = try std.testing.allocator.dupe(u8, "old"), .sequence = 1, .tombstone = false });
    try cm.addTable(1, l1_overlap);

    const compacted = try cm.maybeCompact(0);
    try std.testing.expect(compacted);
    try std.testing.expectEqual(@as(usize, 0), cm.levelTableCount(0));
    try std.testing.expectEqual(@as(usize, 1), cm.levelTableCount(1));
    try std.testing.expect(cm.manifest_updates > 0);
}

test "task 6.1 positive: background worker lifecycle compacts and can stop cleanly" {
    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    var l0 = try SSTable.init(std.testing.allocator, 11, 0, "a", "z");
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "a"), .value = try std.testing.allocator.dupe(u8, "1"), .sequence = 1, .tombstone = false });
    try cm.addTable(0, l0);

    try cm.startBackground(1);
    defer cm.stopBackground();

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        if (cm.levelTableCount(0) == 0 and cm.compactionRunCount() > 0) break;
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }

    cm.stopBackground();
    try std.testing.expect(!cm.isWorkerRunning());

    try std.testing.expectEqual(@as(usize, 0), cm.levelTableCount(0));
    try std.testing.expect(cm.totalTableCount() > 0);
    try std.testing.expect(cm.manifestUpdateCount() > 0);
}

test "task 6.1 positive: background worker wakes on new work without waiting full interval" {
    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    try cm.startBackground(2_000);
    defer cm.stopBackground();

    var l0 = try SSTable.init(std.testing.allocator, 12, 0, "a", "z");
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "a"), .value = try std.testing.allocator.dupe(u8, "1"), .sequence = 1, .tombstone = false });
    try cm.addTable(0, l0);

    var attempts: usize = 0;
    while (attempts < 60) : (attempts += 1) {
        if (cm.compactionRunCount() > 0) break;
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }

    try std.testing.expect(cm.compactionRunCount() > 0);
    try std.testing.expectEqual(@as(usize, 0), cm.levelTableCount(0));
}

test "task 6.1 positive: worker pending flag prevents missed signal race" {
    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    try cm.startBackground(2_000);
    defer cm.stopBackground();

    var run_target: usize = 0;
    var id: u64 = 200;
    var round: usize = 0;
    while (round < 6) : (round += 1) {
        const before = cm.compactionRunCount();
        run_target = before + 1;

        var l0 = try SSTable.init(std.testing.allocator, id, 0, "a", "z");
        id += 1;
        try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "k"), .value = try std.testing.allocator.dupe(u8, "v"), .sequence = id, .tombstone = false });
        try cm.addTable(0, l0);

        var attempts: usize = 0;
        while (attempts < 120) : (attempts += 1) {
            if (cm.compactionRunCount() >= run_target) break;
            std.Thread.sleep(2 * std.time.ns_per_ms);
        }

        try std.testing.expect(cm.compactionRunCount() >= run_target);
    }
}

test "task 6.1 negative: disk full abort keeps original state intact" {
    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    var l0 = try SSTable.init(std.testing.allocator, 10, 0, "a", "z");
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "x"), .value = try std.testing.allocator.dupe(u8, "v"), .sequence = 1, .tombstone = false });
    try cm.addTable(0, l0);

    cm.inject_disk_full = true;

    try std.testing.expectError(CompactionError.DiskFull, cm.maybeCompact(0));
    try std.testing.expectEqual(@as(usize, 1), cm.levelTableCount(0));
    try std.testing.expectEqual(@as(usize, 1), cm.errorLogCount());
    try std.testing.expectEqual(error.NoSpaceLeft, cm.lastLoggedOsError().?);
}

test "task 6.1 negative: write failure rolls back and preserves source tables" {
    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    var l0 = try SSTable.init(std.testing.allocator, 20, 0, "a", "z");
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "k"), .value = try std.testing.allocator.dupe(u8, "v1"), .sequence = 1, .tombstone = false });
    try cm.addTable(0, l0);

    var l1 = try SSTable.init(std.testing.allocator, 21, 1, "a", "z");
    try l1.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "k"), .value = try std.testing.allocator.dupe(u8, "old"), .sequence = 0, .tombstone = false });
    try cm.addTable(1, l1);

    cm.inject_write_failure = true;

    try std.testing.expectError(CompactionError.WriteFailed, cm.maybeCompact(0));
    try std.testing.expectEqual(@as(usize, 1), cm.levelTableCount(0));
    try std.testing.expectEqual(@as(usize, 1), cm.levelTableCount(1));
    try std.testing.expectEqual(@as(usize, 0), cm.manifest_updates);
    try std.testing.expectEqual(CompactionError.WriteFailed, cm.last_error.?);
    try std.testing.expectEqual(@as(usize, 1), cm.errorLogCount());
    try std.testing.expectEqual(error.InputOutput, cm.lastLoggedOsError().?);
}

test "task 6.1 durability: compaction updates manifest and deletes old table files on disk" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cm = CompactionManager.initWithStorage(std.testing.allocator, 1, tmp.dir);
    defer cm.deinit();

    var l0 = try SSTable.init(std.testing.allocator, 31, 0, "a", "z");
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "a"), .value = try std.testing.allocator.dupe(u8, "1"), .sequence = 1, .tombstone = false });
    try cm.addTable(0, l0);

    var l1 = try SSTable.init(std.testing.allocator, 32, 1, "a", "z");
    try l1.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "b"), .value = try std.testing.allocator.dupe(u8, "2"), .sequence = 1, .tombstone = false });
    try cm.addTable(1, l1);

    _ = try tmp.dir.statFile("sst-31.sst");
    _ = try tmp.dir.statFile("sst-32.sst");

    try std.testing.expect(try cm.maybeCompact(0));

    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile("sst-31.sst"));
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile("sst-32.sst"));
    _ = try tmp.dir.statFile("sst-1000.sst");

    const manifest = try tmp.dir.readFileAlloc(std.testing.allocator, "MANIFEST", 4096);
    defer std.testing.allocator.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "new=1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "drop=31,32") != null);
}

test "task 6.1 durability negative: failed compaction cleans partial tmp output and keeps source files" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cm = CompactionManager.initWithStorage(std.testing.allocator, 1, tmp.dir);
    defer cm.deinit();

    var l0 = try SSTable.init(std.testing.allocator, 41, 0, "a", "z");
    try l0.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "a"), .value = try std.testing.allocator.dupe(u8, "1"), .sequence = 1, .tombstone = false });
    try cm.addTable(0, l0);

    var l1 = try SSTable.init(std.testing.allocator, 42, 1, "a", "z");
    try l1.entries.append(std.testing.allocator, .{ .key = try std.testing.allocator.dupe(u8, "b"), .value = try std.testing.allocator.dupe(u8, "2"), .sequence = 1, .tombstone = false });
    try cm.addTable(1, l1);

    cm.inject_disk_full = true;
    try std.testing.expectError(CompactionError.DiskFull, cm.maybeCompact(0));

    _ = try tmp.dir.statFile("sst-41.sst");
    _ = try tmp.dir.statFile("sst-42.sst");
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile("sst-1000.tmp"));
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile("sst-1000.sst"));
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile("MANIFEST"));
}

test "task 6.1 safety: env failure-injection knobs are disabled by default" {
    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    try std.testing.expect(!cm.shouldHonorEnvFailureInjection());
    cm.setEnvFailureInjectionEnabledForTests(true);
    try std.testing.expect(cm.shouldHonorEnvFailureInjection());

    cm.setEnvFailureInjectionEnabledForTests(false);
    try std.testing.expect(!cm.shouldHonorEnvFailureInjection());
}

test "task 6.1 stress: multi-producer wakeups avoid full-interval stalls" {
    const Producer = struct {
        cm: *CompactionManager,
        start_id: u64,
        count: usize,
        seed: u64,

        fn run(ctx: *@This()) void {
            var prng = std.Random.DefaultPrng.init(ctx.seed);
            const rand = prng.random();

            var i: usize = 0;
            while (i < ctx.count) : (i += 1) {
                var table = SSTable.init(std.testing.allocator, ctx.start_id + i, 0, "a", "z") catch @panic("table init failed");
                table.entries.append(std.testing.allocator, .{ .key = std.testing.allocator.dupe(u8, "k") catch @panic("oom"), .value = std.testing.allocator.dupe(u8, "v") catch @panic("oom"), .sequence = ctx.start_id + i, .tombstone = false }) catch @panic("append failed");
                ctx.cm.addTable(0, table) catch @panic("addTable failed");

                const jitter_ms = rand.intRangeAtMost(u8, 0, 3);
                std.Thread.sleep(@as(u64, jitter_ms) * std.time.ns_per_ms);
            }
        }
    };

    var cm = CompactionManager.init(std.testing.allocator, 1);
    defer cm.deinit();

    try cm.startBackground(2_000);
    defer cm.stopBackground();

    var producers = [_]Producer{
        .{ .cm = &cm, .start_id = 1000, .count = 25, .seed = 1 },
        .{ .cm = &cm, .start_id = 2000, .count = 25, .seed = 2 },
        .{ .cm = &cm, .start_id = 3000, .count = 25, .seed = 3 },
        .{ .cm = &cm, .start_id = 4000, .count = 25, .seed = 4 },
    };

    const total_enqueues = producers[0].count * producers.len;
    const start_wake_count = cm.workerWakeCount();
    const start_run_count = cm.compactionRunCount();

    var threads: [producers.len]std.Thread = undefined;
    for (&threads, &producers) |*thread, *ctx| {
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{ctx});
    }

    for (threads) |thread| thread.join();

    var wake_wait_ms: usize = 0;
    while (wake_wait_ms < 300) : (wake_wait_ms += 2) {
        if (cm.workerWakeCount() > start_wake_count) break;
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }

    var drain_attempts: usize = 0;
    const max_drain_attempts = total_enqueues * 8;
    while (drain_attempts < max_drain_attempts) : (drain_attempts += 1) {
        if (cm.levelTableCount(0) == 0) break;
        cm.notifyWorker();
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }

    try std.testing.expect(cm.workerNotifyCount() >= total_enqueues);
    try std.testing.expect(cm.workerWakeCount() > start_wake_count);
    try std.testing.expect(cm.compactionRunCount() > start_run_count);
    try std.testing.expectEqual(@as(usize, 0), cm.levelTableCount(0));
}
