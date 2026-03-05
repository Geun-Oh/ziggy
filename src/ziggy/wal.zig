const std = @import("std");
const builtin = @import("builtin");

pub const BLOCK_SIZE: usize = 32 * 1024;
pub const HEADER_SIZE: usize = 7;

pub const WalError = error{
    CorruptedBlock,
    InvalidFragmentType,
    InvalidFragmentSequence,
};

pub const WalIoError = error{
    OpenFailed,
    DirectIoUnavailable,
    WriteFailed,
    SyncFailed,
    InjectedFailure,
};

pub const DurabilityMode = enum {
    fdatasync,
    o_direct,
};

pub const DurableWalConfig = struct {
    mode: DurabilityMode = .fdatasync,
    inject_sync_failure: bool = false,
    inject_direct_open_failure: bool = false,
};

pub const FragmentType = enum(u8) {
    full = 1,
    first = 2,
    middle = 3,
    last = 4,

    pub fn fromByte(b: u8) WalError!FragmentType {
        return switch (b) {
            1 => .full,
            2 => .first,
            3 => .middle,
            4 => .last,
            else => WalError.InvalidFragmentType,
        };
    }
};

fn crc32c(bytes: []const u8) u32 {
    return std.hash.crc.Crc32Iscsi.hash(bytes);
}

fn fragmentChecksum(ty: FragmentType, payload: []const u8) u32 {
    var hasher = std.hash.crc.Crc32Iscsi.init();
    const ty_byte: [1]u8 = .{@intFromEnum(ty)};
    hasher.update(&ty_byte);
    hasher.update(payload);
    return hasher.final();
}

fn readU16Le(bytes: []const u8, pos: usize) u16 {
    return @as(u16, bytes[pos]) |
        (@as(u16, bytes[pos + 1]) << 8);
}

fn readU32Le(bytes: []const u8, pos: usize) u32 {
    return @as(u32, bytes[pos]) |
        (@as(u32, bytes[pos + 1]) << 8) |
        (@as(u32, bytes[pos + 2]) << 16) |
        (@as(u32, bytes[pos + 3]) << 24);
}

pub const WalWriter = struct {
    bytes: std.ArrayList(u8),

    pub fn init() WalWriter {
        return .{ .bytes = .empty };
    }

    pub fn deinit(self: *WalWriter, allocator: std.mem.Allocator) void {
        self.bytes.deinit(allocator);
    }

    pub fn appendRecord(self: *WalWriter, allocator: std.mem.Allocator, payload: []const u8) !void {
        var offset: usize = 0;
        var first = true;

        while (offset < payload.len) {
            const pos = self.bytes.items.len;
            const block_offset = pos % BLOCK_SIZE;
            const remaining_in_block = BLOCK_SIZE - block_offset;

            if (remaining_in_block < HEADER_SIZE) {
                try self.bytes.appendNTimes(allocator, 0, remaining_in_block);
                continue;
            }

            const max_fragment = remaining_in_block - HEADER_SIZE;
            const fragment_len = @min(max_fragment, payload.len - offset);
            const is_last = (offset + fragment_len) == payload.len;

            const ty: FragmentType = if (first and is_last)
                .full
            else if (first)
                .first
            else if (is_last)
                .last
            else
                .middle;

            const frag = payload[offset .. offset + fragment_len];
            const checksum = fragmentChecksum(ty, frag);

            var header: [HEADER_SIZE]u8 = undefined;
            header[0] = @intCast(checksum & 0xFF);
            header[1] = @intCast((checksum >> 8) & 0xFF);
            header[2] = @intCast((checksum >> 16) & 0xFF);
            header[3] = @intCast((checksum >> 24) & 0xFF);
            const len16: u16 = @intCast(fragment_len);
            header[4] = @intCast(len16 & 0xFF);
            header[5] = @intCast((len16 >> 8) & 0xFF);
            header[6] = @intFromEnum(ty);

            try self.bytes.appendSlice(allocator, &header);
            try self.bytes.appendSlice(allocator, frag);

            offset += fragment_len;
            first = false;
        }
    }

    pub fn asSlice(self: *const WalWriter) []const u8 {
        return self.bytes.items;
    }
};

pub const DurableWal = struct {
    file: std.fs.File,
    config: DurableWalConfig,
    direct_open_used: bool,
    fdatasync_calls: usize,

    pub fn init(dir: std.fs.Dir, path: []const u8, config: DurableWalConfig) WalIoError!DurableWal {
        switch (config.mode) {
            .fdatasync => {
                const file = dir.createFile(path, .{ .truncate = true, .read = true }) catch return WalIoError.OpenFailed;
                return .{
                    .file = file,
                    .config = config,
                    .direct_open_used = false,
                    .fdatasync_calls = 0,
                };
            },
            .o_direct => {
                if (config.inject_direct_open_failure) return WalIoError.DirectIoUnavailable;

                var flags = std.posix.O{
                    .ACCMODE = .RDWR,
                    .CREAT = true,
                    .TRUNC = true,
                    .CLOEXEC = true,
                };
                if (@hasField(std.posix.O, "DIRECT")) {
                    flags.DIRECT = true;
                }

                const fd = std.posix.openat(dir.fd, path, flags, 0o644) catch return WalIoError.DirectIoUnavailable;
                if (!@hasField(std.posix.O, "DIRECT") and builtin.os.tag == .macos) {
                    _ = std.posix.fcntl(fd, std.c.F.NOCACHE, 1) catch {
                        std.posix.close(fd);
                        return WalIoError.DirectIoUnavailable;
                    };
                }

                return .{
                    .file = .{ .handle = fd },
                    .config = config,
                    .direct_open_used = true,
                    .fdatasync_calls = 0,
                };
            },
        }
    }

    pub fn deinit(self: *DurableWal) void {
        self.file.close();
    }

    pub fn appendRecord(self: *DurableWal, allocator: std.mem.Allocator, payload: []const u8) WalIoError!void {
        var framed = WalWriter.init();
        defer framed.deinit(allocator);

        framed.appendRecord(allocator, payload) catch return WalIoError.WriteFailed;
        self.file.writeAll(framed.asSlice()) catch return WalIoError.WriteFailed;
        try self.flush();
    }

    fn flush(self: *DurableWal) WalIoError!void {
        switch (self.config.mode) {
            .fdatasync => {
                if (self.config.inject_sync_failure) return WalIoError.InjectedFailure;
                std.posix.fdatasync(self.file.handle) catch return WalIoError.SyncFailed;
                self.fdatasync_calls += 1;
            },
            .o_direct => {},
        }
    }
};

pub fn decodeFragmentTypes(allocator: std.mem.Allocator, bytes: []const u8) ![]FragmentType {
    var out = std.ArrayList(FragmentType).empty;
    errdefer out.deinit(allocator);

    var pos: usize = 0;
    while (pos < bytes.len) {
        const block_offset = pos % BLOCK_SIZE;
        const remaining = BLOCK_SIZE - block_offset;

        if (remaining < HEADER_SIZE) {
            pos += remaining;
            continue;
        }
        if (pos + HEADER_SIZE > bytes.len) break;

        const checksum = readU32Le(bytes, pos);
        const len = readU16Le(bytes, pos + 4);
        const ty_raw = bytes[pos + 6];

        if (checksum == 0 and len == 0 and ty_raw == 0) {
            pos += remaining;
            continue;
        }

        const ty = try FragmentType.fromByte(ty_raw);
        try out.append(allocator, ty);

        const fragment_len: usize = @intCast(len);
        pos += HEADER_SIZE + fragment_len;
    }

    return out.toOwnedSlice(allocator);
}

pub fn readAllRecords(allocator: std.mem.Allocator, bytes: []const u8) ![][]u8 {
    var records = std.ArrayList([]u8).empty;
    errdefer {
        for (records.items) |rec| allocator.free(rec);
        records.deinit(allocator);
    }

    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);
    var in_split = false;

    var pos: usize = 0;
    while (pos < bytes.len) {
        const block_offset = pos % BLOCK_SIZE;
        const remaining = BLOCK_SIZE - block_offset;

        if (remaining < HEADER_SIZE) {
            pos += remaining;
            continue;
        }
        if (pos + HEADER_SIZE > bytes.len) break;

        const checksum = readU32Le(bytes, pos);
        const len = readU16Le(bytes, pos + 4);
        const ty_raw = bytes[pos + 6];

        if (checksum == 0 and len == 0 and ty_raw == 0) {
            pos += remaining;
            continue;
        }

        const fragment_len: usize = @intCast(len);
        if (fragment_len > remaining - HEADER_SIZE) return WalError.CorruptedBlock;
        if (pos + HEADER_SIZE + fragment_len > bytes.len) return WalError.CorruptedBlock;

        const payload = bytes[pos + HEADER_SIZE .. pos + HEADER_SIZE + fragment_len];
        const ty = try FragmentType.fromByte(ty_raw);

        const actual = fragmentChecksum(ty, payload);
        if (actual != checksum) return WalError.CorruptedBlock;

        switch (ty) {
            .full => {
                if (in_split) return WalError.InvalidFragmentSequence;
                const copy = try allocator.alloc(u8, payload.len);
                @memcpy(copy, payload);
                try records.append(allocator, copy);
            },
            .first => {
                if (in_split) return WalError.InvalidFragmentSequence;
                try scratch.appendSlice(allocator, payload);
                in_split = true;
            },
            .middle => {
                if (!in_split) return WalError.InvalidFragmentSequence;
                try scratch.appendSlice(allocator, payload);
            },
            .last => {
                if (!in_split) return WalError.InvalidFragmentSequence;
                try scratch.appendSlice(allocator, payload);
                const out = try scratch.toOwnedSlice(allocator);
                try records.append(allocator, out);
                scratch = .empty;
                in_split = false;
            },
        }

        pos += HEADER_SIZE + fragment_len;
    }

    if (in_split) return WalError.CorruptedBlock;

    return records.toOwnedSlice(allocator);
}

test "task 1.1 positive: 100KB payload is fragmented and checksummed" {
    const allocator = std.testing.allocator;

    var wal = WalWriter.init();
    defer wal.deinit(allocator);

    const payload = try allocator.alloc(u8, 100 * 1024);
    defer allocator.free(payload);
    @memset(payload, 0xAB);

    try wal.appendRecord(allocator, payload);

    const fragment_types = try decodeFragmentTypes(allocator, wal.asSlice());
    defer allocator.free(fragment_types);

    try std.testing.expect(fragment_types.len == 4);
    try std.testing.expectEqual(FragmentType.first, fragment_types[0]);
    try std.testing.expectEqual(FragmentType.middle, fragment_types[1]);
    try std.testing.expectEqual(FragmentType.middle, fragment_types[2]);
    try std.testing.expectEqual(FragmentType.last, fragment_types[3]);

    const records = try readAllRecords(allocator, wal.asSlice());
    defer {
        for (records) |r| allocator.free(r);
        allocator.free(records);
    }

    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualSlices(u8, payload, records[0]);
}

test "task 1.1 negative: crc mismatch returns error.CorruptedBlock" {
    const allocator = std.testing.allocator;

    var wal = WalWriter.init();
    defer wal.deinit(allocator);

    const payload = "corrupt-me-please";
    try wal.appendRecord(allocator, payload);

    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(allocator);
    try bytes.appendSlice(allocator, wal.asSlice());

    const flip_index = HEADER_SIZE + 2;
    bytes.items[flip_index] ^= 0xFF;

    try std.testing.expectError(WalError.CorruptedBlock, readAllRecords(allocator, bytes.items));
}

test "task 1.1 durability positive: fdatasync mode writes record and flushes" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var wal_file = try DurableWal.init(tmp.dir, "wal.log", .{ .mode = .fdatasync });
    defer wal_file.deinit();

    try wal_file.appendRecord(allocator, "durable-payload");
    try std.testing.expectEqual(@as(usize, 1), wal_file.fdatasync_calls);

    const on_disk = try tmp.dir.readFileAlloc(allocator, "wal.log", 1024 * 1024);
    defer allocator.free(on_disk);

    const records = try readAllRecords(allocator, on_disk);
    defer {
        for (records) |r| allocator.free(r);
        allocator.free(records);
    }

    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings("durable-payload", records[0]);
}

test "task 1.1 durability negative: injected fdatasync failure is returned" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var wal_file = try DurableWal.init(tmp.dir, "wal.log", .{ .mode = .fdatasync, .inject_sync_failure = true });
    defer wal_file.deinit();

    try std.testing.expectError(WalIoError.InjectedFailure, wal_file.appendRecord(std.testing.allocator, "x"));
}

test "task 1.1 durability path: o_direct config uses direct open path or explicit unavailability" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    if (DurableWal.init(tmp.dir, "wal.log", .{ .mode = .o_direct })) |wal_file| {
        var w = wal_file;
        defer w.deinit();
        try std.testing.expect(w.direct_open_used);
    } else |err| {
        try std.testing.expectEqual(WalIoError.DirectIoUnavailable, err);
    }
}
