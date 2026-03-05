const std = @import("std");
const wal = @import("wal.zig");

pub const RecoveryError = anyerror;

pub const Mutation = struct {
    timestamp_micros: i64,
    tombstone: bool,
    key: []const u8,
    value: []const u8,
};

pub fn encodeMutation(allocator: std.mem.Allocator, m: Mutation) ![]u8 {
    if (m.key.len > std.math.maxInt(u16) or m.value.len > std.math.maxInt(u16)) {
        return RecoveryError.InvalidRecord;
    }

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    const w = out.writer(allocator);
    try w.writeInt(i64, m.timestamp_micros, .little);
    try w.writeByte(if (m.tombstone) 1 else 0);
    try w.writeInt(u16, @intCast(m.key.len), .little);
    try w.writeInt(u16, @intCast(m.value.len), .little);
    try out.appendSlice(allocator, m.key);
    try out.appendSlice(allocator, m.value);
    return out.toOwnedSlice(allocator);
}

fn parseWalSequence(name: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, name, "wal-")) return null;
    const suffix = name[4..];
    return std.fmt.parseUnsigned(u64, suffix, 10) catch null;
}

pub fn decodeMutation(bytes: []const u8) RecoveryError!Mutation {
    if (bytes.len < 13) return RecoveryError.InvalidRecord;

    const ts_u = @as(u64, bytes[0]) |
        (@as(u64, bytes[1]) << 8) |
        (@as(u64, bytes[2]) << 16) |
        (@as(u64, bytes[3]) << 24) |
        (@as(u64, bytes[4]) << 32) |
        (@as(u64, bytes[5]) << 40) |
        (@as(u64, bytes[6]) << 48) |
        (@as(u64, bytes[7]) << 56);
    const ts: i64 = @bitCast(ts_u);
    const tombstone = bytes[8] == 1;
    const key_len = @as(u16, bytes[9]) | (@as(u16, bytes[10]) << 8);
    const value_len = @as(u16, bytes[11]) | (@as(u16, bytes[12]) << 8);

    const start = 13;
    const key_end = start + key_len;
    const value_end = key_end + value_len;
    if (value_end > bytes.len) return RecoveryError.InvalidRecord;

    return .{
        .timestamp_micros = ts,
        .tombstone = tombstone,
        .key = bytes[start..key_end],
        .value = bytes[key_end..value_end],
    };
}

pub const ArchiveManager = struct {
    dir: std.fs.Dir,

    pub fn init(dir: std.fs.Dir) ArchiveManager {
        return .{ .dir = dir };
    }

    pub fn archiveSegment(self: *ArchiveManager, name: []const u8, bytes: []const u8) !void {
        var f = try self.dir.createFile(name, .{ .truncate = true });
        defer f.close();
        try f.writeAll(bytes);
        try f.sync();
    }
};

pub const KV = std.StringHashMap([]u8);

fn cloneBaseState(allocator: std.mem.Allocator, base: *const KV) !KV {
    var out = KV.init(allocator);
    errdefer out.deinit();

    var it = base.iterator();
    while (it.next()) |entry| {
        try out.put(try allocator.dupe(u8, entry.key_ptr.*), try allocator.dupe(u8, entry.value_ptr.*));
    }
    return out;
}

pub fn deinitState(allocator: std.mem.Allocator, state: *KV) void {
    var it = state.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        allocator.free(entry.value_ptr.*);
    }
    state.deinit();
}

pub fn restoreUntil(
    allocator: std.mem.Allocator,
    dir: std.fs.Dir,
    base: *const KV,
    recovery_target_time: i64,
) RecoveryError!KV {
    var out = try cloneBaseState(allocator, base);
    errdefer deinitState(allocator, &out);

    var names = std.ArrayList([]u8).empty;
    defer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.startsWith(u8, entry.name, "wal-")) continue;
        try names.append(allocator, try allocator.dupe(u8, entry.name));
    }

    std.mem.sort([]u8, names.items, {}, struct {
        fn lessThan(_: void, a: []u8, b: []u8) bool {
            const a_seq = parseWalSequence(a) orelse std.math.maxInt(u64);
            const b_seq = parseWalSequence(b) orelse std.math.maxInt(u64);
            if (a_seq != b_seq) return a_seq < b_seq;
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);

    var stop = false;

    for (names.items) |name| {
        if (stop) break;

        const bytes = try dir.readFileAlloc(allocator, name, 10 * 1024 * 1024);
        defer allocator.free(bytes);

        const records = wal.readAllRecords(allocator, bytes) catch return RecoveryError.ArchiveCorruption;
        defer {
            for (records) |r| allocator.free(r);
            allocator.free(records);
        }

        for (records) |record| {
            const m = decodeMutation(record) catch return RecoveryError.ArchiveCorruption;

            if (m.timestamp_micros >= recovery_target_time) {
                stop = true;
                break;
            }

            if (m.tombstone) {
                if (out.fetchRemove(m.key)) |removed| {
                    allocator.free(removed.key);
                    allocator.free(removed.value);
                }
            } else {
                if (out.fetchRemove(m.key)) |removed| {
                    allocator.free(removed.key);
                    allocator.free(removed.value);
                }
                try out.put(try allocator.dupe(u8, m.key), try allocator.dupe(u8, m.value));
            }
        }
    }

    return out;
}

test "task 6.2 positive: PITR replays WAL archive and stops at target microsecond" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var arch = ArchiveManager.init(tmp.dir);

    var writer = wal.WalWriter.init();
    defer writer.deinit(allocator);

    const m1 = try encodeMutation(allocator, .{ .timestamp_micros = 100, .tombstone = false, .key = "k1", .value = "v1" });
    defer allocator.free(m1);
    const m2 = try encodeMutation(allocator, .{ .timestamp_micros = 200, .tombstone = false, .key = "k2", .value = "v2" });
    defer allocator.free(m2);
    const m3 = try encodeMutation(allocator, .{ .timestamp_micros = 300, .tombstone = false, .key = "k3", .value = "v3" });
    defer allocator.free(m3);

    try writer.appendRecord(allocator, m1);
    try writer.appendRecord(allocator, m2);
    try writer.appendRecord(allocator, m3);

    try arch.archiveSegment("wal-000001", writer.asSlice());

    var base = KV.init(allocator);
    defer deinitState(allocator, &base);
    try base.put(try allocator.dupe(u8, "base"), try allocator.dupe(u8, "ok"));

    var recovered = try restoreUntil(allocator, tmp.dir, &base, 250);
    defer deinitState(allocator, &recovered);

    try std.testing.expect(recovered.contains("base"));
    try std.testing.expect(recovered.contains("k1"));
    try std.testing.expect(recovered.contains("k2"));
    try std.testing.expect(!recovered.contains("k3"));
}

test "task 6.2 negative: archived WAL corruption yields error.ArchiveCorruption" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var arch = ArchiveManager.init(tmp.dir);

    var writer = wal.WalWriter.init();
    defer writer.deinit(allocator);

    const payload = try encodeMutation(allocator, .{ .timestamp_micros = 100, .tombstone = false, .key = "k", .value = "v" });
    defer allocator.free(payload);
    try writer.appendRecord(allocator, payload);

    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(allocator);
    try bytes.appendSlice(allocator, writer.asSlice());
    bytes.items[wal.HEADER_SIZE + 1] ^= 0xAA;

    try arch.archiveSegment("wal-000001", bytes.items);

    var base = KV.init(allocator);
    defer deinitState(allocator, &base);

    try std.testing.expectError(RecoveryError.ArchiveCorruption, restoreUntil(allocator, tmp.dir, &base, 500));
}
