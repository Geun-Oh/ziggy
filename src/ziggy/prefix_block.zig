const std = @import("std");

pub const PrefixBlockError = error{
    InvalidInput,
    DecompressionFailure,
    OutOfMemory,
};

fn commonPrefixLen(a: []const u8, b: []const u8) usize {
    const n = @min(a.len, b.len);
    var i: usize = 0;
    while (i < n and a[i] == b[i]) : (i += 1) {}
    return i;
}

fn appendVarint(out: *std.ArrayList(u8), allocator: std.mem.Allocator, value: usize) !void {
    var x = value;
    while (x >= 0x80) {
        try out.append(allocator, @as(u8, @intCast((x & 0x7F) | 0x80)));
        x >>= 7;
    }
    try out.append(allocator, @as(u8, @intCast(x)));
}

fn decodeVarint(bytes: []const u8, cursor: *usize) PrefixBlockError!usize {
    var shift: usize = 0;
    var out: usize = 0;

    while (true) {
        if (cursor.* >= bytes.len) return PrefixBlockError.DecompressionFailure;
        const b = bytes[cursor.*];
        cursor.* += 1;

        out |= (@as(usize, b & 0x7F) << @intCast(shift));
        if ((b & 0x80) == 0) break;

        shift += 7;
        if (shift > (@bitSizeOf(usize) - 1)) return PrefixBlockError.DecompressionFailure;
    }

    return out;
}

fn readU32Le(bytes: []const u8, pos: usize) PrefixBlockError!u32 {
    if (pos + 4 > bytes.len) return PrefixBlockError.DecompressionFailure;
    return @as(u32, bytes[pos]) |
        (@as(u32, bytes[pos + 1]) << 8) |
        (@as(u32, bytes[pos + 2]) << 16) |
        (@as(u32, bytes[pos + 3]) << 24);
}

fn appendU32Le(out: *std.ArrayList(u8), allocator: std.mem.Allocator, value: u32) !void {
    try out.append(allocator, @intCast(value & 0xFF));
    try out.append(allocator, @intCast((value >> 8) & 0xFF));
    try out.append(allocator, @intCast((value >> 16) & 0xFF));
    try out.append(allocator, @intCast((value >> 24) & 0xFF));
}

pub fn encode(
    allocator: std.mem.Allocator,
    keys: []const []const u8,
    values: []const []const u8,
    restart_interval: usize,
) PrefixBlockError![]u8 {
    if (keys.len != values.len) return PrefixBlockError.InvalidInput;
    if (restart_interval == 0) return PrefixBlockError.InvalidInput;

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    var restarts = std.ArrayList(u32).empty;
    defer restarts.deinit(allocator);

    var prev_key: []const u8 = &.{};

    for (keys, values, 0..) |key, value, idx| {
        const restart = (idx % restart_interval) == 0;
        const shared: usize = if (restart) 0 else commonPrefixLen(prev_key, key);
        const unshared = key.len - shared;

        if (restart) {
            try restarts.append(allocator, @intCast(out.items.len));
        }

        try appendVarint(&out, allocator, shared);
        try appendVarint(&out, allocator, unshared);
        try appendVarint(&out, allocator, value.len);
        try out.appendSlice(allocator, key[shared..]);
        try out.appendSlice(allocator, value);

        prev_key = key;
    }

    for (restarts.items) |off| {
        try appendU32Le(&out, allocator, off);
    }
    try appendU32Le(&out, allocator, @intCast(restarts.items.len));

    return out.toOwnedSlice(allocator);
}

fn decodeEntry(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    cursor: *usize,
    limit: usize,
    prev_key: []const u8,
) PrefixBlockError!struct { key: []u8, value: []u8 } {
    if (cursor.* >= limit) return PrefixBlockError.DecompressionFailure;

    const shared = try decodeVarint(bytes[0..limit], cursor);
    const unshared = try decodeVarint(bytes[0..limit], cursor);
    const value_len = try decodeVarint(bytes[0..limit], cursor);

    if (shared > prev_key.len) return PrefixBlockError.DecompressionFailure;
    if (cursor.* + unshared + value_len > limit) return PrefixBlockError.DecompressionFailure;

    const suffix = bytes[cursor.* .. cursor.* + unshared];
    cursor.* += unshared;

    const value_slice = bytes[cursor.* .. cursor.* + value_len];
    cursor.* += value_len;

    const key = try allocator.alloc(u8, shared + unshared);
    @memcpy(key[0..shared], prev_key[0..shared]);
    @memcpy(key[shared..], suffix);

    const value = try allocator.dupe(u8, value_slice);
    return .{ .key = key, .value = value };
}

fn restartTableStart(bytes: []const u8) PrefixBlockError!usize {
    if (bytes.len < 4) return PrefixBlockError.DecompressionFailure;
    const restart_count = try readU32Le(bytes, bytes.len - 4);
    const table_bytes = @as(usize, restart_count) * 4;
    if (bytes.len < 4 + table_bytes) return PrefixBlockError.DecompressionFailure;
    return bytes.len - 4 - table_bytes;
}

pub fn decodeKeyAt(allocator: std.mem.Allocator, bytes: []const u8, target_index: usize) PrefixBlockError![]u8 {
    const limit = try restartTableStart(bytes);
    var cursor: usize = 0;
    var idx: usize = 0;

    var prev_key: []u8 = try allocator.alloc(u8, 0);
    defer allocator.free(prev_key);

    while (cursor < limit) {
        const decoded = try decodeEntry(allocator, bytes, &cursor, limit, prev_key);
        defer allocator.free(decoded.value);

        allocator.free(prev_key);
        prev_key = decoded.key;

        if (idx == target_index) {
            return allocator.dupe(u8, prev_key);
        }
        idx += 1;
    }

    return PrefixBlockError.DecompressionFailure;
}

pub fn getValue(allocator: std.mem.Allocator, bytes: []const u8, target_key: []const u8) PrefixBlockError!?[]u8 {
    const limit = try restartTableStart(bytes);
    var cursor: usize = 0;

    var prev_key: []u8 = try allocator.alloc(u8, 0);
    defer allocator.free(prev_key);

    while (cursor < limit) {
        const decoded = try decodeEntry(allocator, bytes, &cursor, limit, prev_key);
        allocator.free(prev_key);
        prev_key = decoded.key;

        if (std.mem.eql(u8, prev_key, target_key)) {
            return decoded.value;
        }
        allocator.free(decoded.value);
    }

    return null;
}

test "task 3.2 positive: prefix compression shrinks block and random access reconstructs key" {
    const allocator = std.testing.allocator;

    var keys = std.ArrayList([]const u8).empty;
    defer keys.deinit(allocator);
    var values = std.ArrayList([]const u8).empty;
    defer values.deinit(allocator);

    var raw_bytes: usize = 0;
    var i: usize = 1;
    while (i <= 100) : (i += 1) {
        const k = try std.fmt.allocPrint(allocator, "user_profile_data_{d:0>3}", .{i});
        errdefer allocator.free(k);
        const v = try std.fmt.allocPrint(allocator, "value-{d}", .{i});
        errdefer allocator.free(v);

        try keys.append(allocator, k);
        try values.append(allocator, v);
        raw_bytes += k.len + v.len;
    }
    defer {
        for (keys.items) |k| allocator.free(k);
        for (values.items) |v| allocator.free(v);
    }

    const encoded = try encode(allocator, keys.items, values.items, 16);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len < raw_bytes);

    const k73 = try decodeKeyAt(allocator, encoded, 72);
    defer allocator.free(k73);
    try std.testing.expectEqualStrings("user_profile_data_073", k73);

    const v = (try getValue(allocator, encoded, "user_profile_data_100")).?;
    defer allocator.free(v);
    try std.testing.expectEqualStrings("value-100", v);
}

test "task 3.2 negative: malformed unshared length returns error.DecompressionFailure" {
    const allocator = std.testing.allocator;

    const encoded = try encode(allocator, &.{"abc"}, &.{"v"}, 16);
    defer allocator.free(encoded);

    var corrupted = try allocator.dupe(u8, encoded);
    defer allocator.free(corrupted);

    // Corrupt unshared length varint to an absurdly large value.
    if (corrupted.len < 1) return;
    corrupted[1] = 0xFF;

    try std.testing.expectError(PrefixBlockError.DecompressionFailure, decodeKeyAt(allocator, corrupted, 0));
}
