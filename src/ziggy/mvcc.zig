const std = @import("std");

pub const MvccError = error{
    KeyNotFound,
};

pub const Version = struct {
    sequence: u64,
    tombstone: bool,
    value: []u8,
};

pub const MVCCEngine = struct {
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
    versions: std.StringHashMap(std.ArrayList(Version)),

    pub fn init(allocator: std.mem.Allocator) MVCCEngine {
        return .{
            .allocator = allocator,
            .mutex = .{},
            .versions = std.StringHashMap(std.ArrayList(Version)).init(allocator),
        };
    }

    pub fn deinit(self: *MVCCEngine) void {
        var it = self.versions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            for (entry.value_ptr.items) |ver| self.allocator.free(ver.value);
            entry.value_ptr.deinit(self.allocator);
        }
        self.versions.deinit();
    }

    fn ensureKey(self: *MVCCEngine, key: []const u8) !*std.ArrayList(Version) {
        if (self.versions.getPtr(key)) |list| {
            return list;
        }

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);
        try self.versions.put(key_copy, .empty);
        return self.versions.getPtr(key).?;
    }

    pub fn put(self: *MVCCEngine, sequence: u64, key: []const u8, value: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const list = try self.ensureKey(key);
        try list.append(self.allocator, .{
            .sequence = sequence,
            .tombstone = false,
            .value = try self.allocator.dupe(u8, value),
        });
    }

    pub fn delete(self: *MVCCEngine, sequence: u64, key: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const list = try self.ensureKey(key);
        try list.append(self.allocator, .{
            .sequence = sequence,
            .tombstone = true,
            .value = try self.allocator.dupe(u8, ""),
        });
    }

    pub fn get(self: *MVCCEngine, snapshot_sequence: u64, key: []const u8) MvccError![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const list = self.versions.get(key) orelse return MvccError.KeyNotFound;

        var best_idx: ?usize = null;
        var best_seq: u64 = 0;
        for (list.items, 0..) |ver, idx| {
            if (ver.sequence <= snapshot_sequence and (best_idx == null or ver.sequence > best_seq)) {
                best_idx = idx;
                best_seq = ver.sequence;
            }
        }

        const idx = best_idx orelse return MvccError.KeyNotFound;
        const ver = list.items[idx];
        if (ver.tombstone) return MvccError.KeyNotFound;
        return ver.value;
    }
};

test "task 4.1 positive: snapshot ignores future sequence versions" {
    var mvcc = MVCCEngine.init(std.testing.allocator);
    defer mvcc.deinit();

    try mvcc.put(90, "Key X", "old");
    try mvcc.put(105, "Key X", "new");

    const visible = try mvcc.get(100, "Key X");
    try std.testing.expectEqualStrings("old", visible);
}

test "task 4.1 negative: tombstone visible at snapshot returns KeyNotFound" {
    var mvcc = MVCCEngine.init(std.testing.allocator);
    defer mvcc.deinit();

    try mvcc.put(80, "tombed", "v1");
    try mvcc.delete(95, "tombed");

    try std.testing.expectError(MvccError.KeyNotFound, mvcc.get(100, "tombed"));
}
