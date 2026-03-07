//! Manifest snapshots and CURRENT pointer management for durable metadata versions.
const std = @import("std");

pub const ManifestError = error{
    InvalidManifest,
    MissingCurrent,
};

pub const MAX_LEVELS = 7;

// Metadata describing one SSTable tracked by the manifest.
pub const FileMeta = struct {
    level: u8,
    file_number: u64,
    size: u64,
    min_key: []u8,
    max_key: []u8,

    // Deep-copies file-range metadata so versions can be cloned safely.
    pub fn clone(self: FileMeta, allocator: std.mem.Allocator) !FileMeta {
        const min_copy = try allocator.dupe(u8, self.min_key);
        errdefer allocator.free(min_copy);
        const max_copy = try allocator.dupe(u8, self.max_key);
        return .{
            .level = self.level,
            .file_number = self.file_number,
            .size = self.size,
            .min_key = min_copy,
            .max_key = max_copy,
        };
    }

    // Releases owned min/max key buffers.
    pub fn deinit(self: FileMeta, allocator: std.mem.Allocator) void {
        allocator.free(self.min_key);
        allocator.free(self.max_key);
    }
};

// Reference to an SSTable that should be removed from a version.
pub const DeletedFile = struct {
    level: u8,
    file_number: u64,
};

// Atomic metadata delta applied on top of the current VersionSet.
pub const VersionEdit = struct {
    new_files: []const FileMeta,
    deleted_files: []const DeletedFile,
    next_sequence: ?u64 = null,
    next_file_number: ?u64 = null,
};

// Full in-memory view of the LSM tree state described by the manifest.
pub const VersionSet = struct {
    allocator: std.mem.Allocator,
    levels: [MAX_LEVELS]std.ArrayList(FileMeta),
    next_sequence: u64,
    next_file_number: u64,

    // Creates an empty version set with default monotonic counters.
    pub fn init(allocator: std.mem.Allocator) VersionSet {
        var levels: [MAX_LEVELS]std.ArrayList(FileMeta) = undefined;
        for (&levels) |*level| level.* = .empty;
        return .{
            .allocator = allocator,
            .levels = levels,
            .next_sequence = 1,
            .next_file_number = 1,
        };
    }

    // Releases every tracked file metadata entry in every level.
    pub fn deinit(self: *VersionSet) void {
        for (&self.levels) |*level| {
            for (level.items) |meta| meta.deinit(self.allocator);
            level.deinit(self.allocator);
        }
    }

    // Applies file additions/removals and advances monotonic counters.
    pub fn applyEdit(self: *VersionSet, edit: VersionEdit) !void {
        for (edit.deleted_files) |deleted| {
            if (deleted.level >= MAX_LEVELS) continue;
            var list = &self.levels[deleted.level];
            var i: usize = 0;
            while (i < list.items.len) {
                if (list.items[i].file_number == deleted.file_number) {
                    list.items[i].deinit(self.allocator);
                    _ = list.orderedRemove(i);
                    break;
                }
                i += 1;
            }
        }

        for (edit.new_files) |meta| {
            if (meta.level >= MAX_LEVELS) continue;
            try self.levels[meta.level].append(self.allocator, try meta.clone(self.allocator));
        }

        if (edit.next_sequence) |seq| self.next_sequence = seq;
        if (edit.next_file_number) |num| self.next_file_number = num;
    }

    // Deep-copies the full version so callers can stage atomic updates safely.
    pub fn clone(self: *const VersionSet, allocator: std.mem.Allocator) !VersionSet {
        var out = VersionSet.init(allocator);
        out.next_sequence = self.next_sequence;
        out.next_file_number = self.next_file_number;

        for (0..MAX_LEVELS) |idx| {
            for (self.levels[idx].items) |meta| {
                try out.levels[idx].append(allocator, try meta.clone(allocator));
            }
        }
        return out;
    }
};

// Writes CURRENT.tmp, fsyncs it, and renames it into CURRENT.
fn writeCurrentAtomically(dir: std.fs.Dir, manifest_name: []const u8) !void {
    const tmp_name = "CURRENT.tmp";

    {
        var file = try dir.createFile(tmp_name, .{ .truncate = true });
        defer file.close();
        try file.writeAll(manifest_name);
        try file.writeAll("\n");
        try file.sync();
    }

    try dir.rename(tmp_name, "CURRENT");
}

// Formats the canonical MANIFEST file name for a file number.
fn manifestNameFor(file_number: u64, buf: *[32]u8) []const u8 {
    return std.fmt.bufPrint(buf, "MANIFEST-{d:0>6}", .{file_number}) catch unreachable;
}

// Serializes the full VersionSet into the project's line-oriented manifest snapshot format.
fn serializeSnapshot(allocator: std.mem.Allocator, version: *const VersionSet) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    const w = out.writer(allocator);
    try w.print("SEQ {d}\n", .{version.next_sequence});
    try w.print("FILENO {d}\n", .{version.next_file_number});

    for (0..MAX_LEVELS) |level| {
        for (version.levels[level].items) |meta| {
            try w.print("FILE {d} {d} {d} {s} {s}\n", .{
                meta.level,
                meta.file_number,
                meta.size,
                meta.min_key,
                meta.max_key,
            });
        }
    }

    return out.toOwnedSlice(allocator);
}

// Parses one manifest snapshot file back into an in-memory VersionSet.
fn parseSnapshot(allocator: std.mem.Allocator, bytes: []const u8) !VersionSet {
    var version = VersionSet.init(allocator);
    errdefer version.deinit();

    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;

        var tok = std.mem.tokenizeScalar(u8, line, ' ');
        const tag = tok.next() orelse return ManifestError.InvalidManifest;

        if (std.mem.eql(u8, tag, "SEQ")) {
            const seq_txt = tok.next() orelse return ManifestError.InvalidManifest;
            version.next_sequence = try std.fmt.parseUnsigned(u64, seq_txt, 10);
        } else if (std.mem.eql(u8, tag, "FILENO")) {
            const n_txt = tok.next() orelse return ManifestError.InvalidManifest;
            version.next_file_number = try std.fmt.parseUnsigned(u64, n_txt, 10);
        } else if (std.mem.eql(u8, tag, "FILE")) {
            const level_txt = tok.next() orelse return ManifestError.InvalidManifest;
            const file_txt = tok.next() orelse return ManifestError.InvalidManifest;
            const size_txt = tok.next() orelse return ManifestError.InvalidManifest;
            const min_txt = tok.next() orelse return ManifestError.InvalidManifest;
            const max_txt = tok.next() orelse return ManifestError.InvalidManifest;

            const level = try std.fmt.parseUnsigned(u8, level_txt, 10);
            if (level >= MAX_LEVELS) return ManifestError.InvalidManifest;

            const meta = FileMeta{
                .level = level,
                .file_number = try std.fmt.parseUnsigned(u64, file_txt, 10),
                .size = try std.fmt.parseUnsigned(u64, size_txt, 10),
                .min_key = try allocator.dupe(u8, min_txt),
                .max_key = try allocator.dupe(u8, max_txt),
            };
            try version.levels[level].append(allocator, meta);
        } else {
            return ManifestError.InvalidManifest;
        }
    }

    return version;
}

// Filesystem-backed manifest manager that writes snapshots and swaps CURRENT atomically.
pub const ManifestStore = struct {
    allocator: std.mem.Allocator,
    dir: std.fs.Dir,

    pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir) ManifestStore {
        return .{ .allocator = allocator, .dir = dir };
    }

    // Creates the first manifest snapshot and points CURRENT at it.
    pub fn bootstrap(self: *ManifestStore, initial: *const VersionSet) !void {
        var name_buf: [32]u8 = undefined;
        const name = manifestNameFor(initial.next_file_number, &name_buf);
        try self.writeSnapshot(name, initial);
        try writeCurrentAtomically(self.dir, name);
    }

    // Writes and fsyncs one manifest snapshot file.
    fn writeSnapshot(self: *ManifestStore, name: []const u8, version: *const VersionSet) !void {
        const content = try serializeSnapshot(self.allocator, version);
        defer self.allocator.free(content);

        var file = try self.dir.createFile(name, .{ .truncate = true });
        defer file.close();
        try file.writeAll(content);
        try file.sync();
    }

    // Writes a new snapshot, updates CURRENT, and then swaps the caller's in-memory version.
    pub fn applyEditAtomic(self: *ManifestStore, current: *VersionSet, edit: VersionEdit) !void {
        var next = try current.clone(self.allocator);
        errdefer next.deinit();
        try next.applyEdit(edit);

        var name_buf: [32]u8 = undefined;
        const new_name = manifestNameFor(next.next_file_number, &name_buf);
        try self.writeSnapshot(new_name, &next);
        try writeCurrentAtomically(self.dir, new_name);

        current.deinit();
        current.* = next;
    }

    // Test helper that writes a new manifest file without updating CURRENT.
    pub fn simulateCrashAfterManifestWrite(self: *ManifestStore, current: *VersionSet, edit: VersionEdit) !void {
        var next = try current.clone(self.allocator);
        defer next.deinit();

        try next.applyEdit(edit);

        var name_buf: [32]u8 = undefined;
        const new_name = manifestNameFor(next.next_file_number, &name_buf);
        try self.writeSnapshot(new_name, &next);
    }

    // Loads a specific manifest snapshot file by name.
    fn tryLoadFromManifestName(self: *ManifestStore, name: []const u8) !VersionSet {
        const bytes = try self.dir.readFileAlloc(self.allocator, name, 1024 * 1024);
        defer self.allocator.free(bytes);
        return parseSnapshot(self.allocator, bytes);
    }

    // Loads the version referenced by CURRENT or falls back to the newest valid manifest.
    pub fn loadVersionSet(self: *ManifestStore) !VersionSet {
        const current_name_raw = self.dir.readFileAlloc(self.allocator, "CURRENT", 1024) catch |err| switch (err) {
            error.FileNotFound => return self.loadFallbackManifest(),
            else => return err,
        };
        defer self.allocator.free(current_name_raw);

        const current_name = std.mem.trim(u8, current_name_raw, " \n\r\t");
        if (current_name.len == 0) return self.loadFallbackManifest();

        return self.tryLoadFromManifestName(current_name) catch ManifestError.InvalidManifest;
    }

    // Scans all MANIFEST-* files and returns the newest valid snapshot.
    fn loadFallbackManifest(self: *ManifestStore) !VersionSet {
        var iter = self.dir.iterate();
        var names = std.ArrayList([]u8).empty;
        defer {
            for (names.items) |n| self.allocator.free(n);
            names.deinit(self.allocator);
        }

        while (try iter.next()) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.startsWith(u8, entry.name, "MANIFEST-")) continue;
            try names.append(self.allocator, try self.allocator.dupe(u8, entry.name));
        }

        if (names.items.len == 0) return ManifestError.MissingCurrent;

        std.mem.sort([]u8, names.items, {}, struct {
            fn lessThan(_: void, a: []u8, b: []u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);

        var idx = names.items.len;
        while (idx > 0) : (idx -= 1) {
            const name = names.items[idx - 1];
            if (self.tryLoadFromManifestName(name)) |version| {
                return version;
            } else |_| {}
        }

        return ManifestError.InvalidManifest;
    }
};

test "task 1.2 positive: apply VersionEdit and recover via CURRENT" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = ManifestStore.init(allocator, tmp.dir);
    var version = VersionSet.init(allocator);
    defer version.deinit();

    version.next_sequence = 100;
    version.next_file_number = 1;

    try store.bootstrap(&version);

    const new_file = FileMeta{
        .level = 1,
        .file_number = 42,
        .size = 8192,
        .min_key = @constCast("k001"),
        .max_key = @constCast("k999"),
    };

    try store.applyEditAtomic(&version, .{
        .new_files = &.{new_file},
        .deleted_files = &.{},
        .next_sequence = 101,
        .next_file_number = 2,
    });

    var recovered = try store.loadVersionSet();
    defer recovered.deinit();

    try std.testing.expectEqual(@as(usize, 1), recovered.levels[1].items.len);
    try std.testing.expectEqual(@as(u64, 101), recovered.next_sequence);
    try std.testing.expectEqual(@as(u64, 2), recovered.next_file_number);
    try std.testing.expectEqual(@as(u64, 42), recovered.levels[1].items[0].file_number);
}

test "task 10.1 negative: corrupt CURRENT target fails closed" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = ManifestStore.init(allocator, tmp.dir);
    var version = VersionSet.init(allocator);
    defer version.deinit();

    try store.bootstrap(&version);

    var current = try tmp.dir.createFile("CURRENT", .{ .truncate = true });
    defer current.close();
    try current.writeAll("MANIFEST-DOES-NOT-EXIST\n");
    try current.sync();

    try std.testing.expectError(ManifestError.InvalidManifest, store.loadVersionSet());
}

test "task 1.2 negative: crash before CURRENT rename keeps previous manifest valid" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = ManifestStore.init(allocator, tmp.dir);
    var version = VersionSet.init(allocator);
    defer version.deinit();

    version.next_sequence = 500;
    version.next_file_number = 1;

    try store.bootstrap(&version);

    const old_file = FileMeta{
        .level = 0,
        .file_number = 11,
        .size = 1024,
        .min_key = @constCast("a"),
        .max_key = @constCast("z"),
    };
    try version.applyEdit(.{
        .new_files = &.{old_file},
        .deleted_files = &.{},
        .next_sequence = 501,
        .next_file_number = 1,
    });
    try store.applyEditAtomic(&version, .{
        .new_files = &.{},
        .deleted_files = &.{},
        .next_sequence = 501,
        .next_file_number = 1,
    });

    const pending_file = FileMeta{
        .level = 1,
        .file_number = 99,
        .size = 4096,
        .min_key = @constCast("m"),
        .max_key = @constCast("n"),
    };

    try store.simulateCrashAfterManifestWrite(&version, .{
        .new_files = &.{pending_file},
        .deleted_files = &.{},
        .next_sequence = 900,
        .next_file_number = 3,
    });

    // CURRENT was never renamed, so restart must still reconstruct from old valid manifest.
    var recovered = try store.loadVersionSet();
    defer recovered.deinit();

    try std.testing.expectEqual(@as(u64, 501), recovered.next_sequence);
    try std.testing.expectEqual(@as(usize, 1), recovered.levels[0].items.len);
    try std.testing.expectEqual(@as(usize, 0), recovered.levels[1].items.len);
}
