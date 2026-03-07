//! Small comptime-specialized little-endian readers for hot decode paths.
const std = @import("std");

pub const BytesError = error{OutOfBounds};

pub inline fn readIntLe(comptime T: type, bytes: []const u8, pos: usize) BytesError!T {
    comptime {
        if (@typeInfo(T) != .int) @compileError("readIntLe expects an integer type");
        if (@typeInfo(T).int.signedness != .unsigned) @compileError("readIntLe expects an unsigned integer type");
    }

    const n = @sizeOf(T);
    if (pos + n > bytes.len) return BytesError.OutOfBounds;

    var out: T = 0;
    inline for (0..n) |i| {
        out |= (@as(T, bytes[pos + i]) << @intCast(i * 8));
    }
    return out;
}

test "comptime little-endian reads decode expected values" {
    const bytes: [8]u8 = .{ 0x78, 0x56, 0x34, 0x12, 0xEF, 0xCD, 0xAB, 0x90 };

    try std.testing.expectEqual(@as(u16, 0x5678), try readIntLe(u16, bytes[0..], 0));
    try std.testing.expectEqual(@as(u32, 0x12345678), try readIntLe(u32, bytes[0..], 0));
    try std.testing.expectEqual(@as(u64, 0x90ABCDEF12345678), try readIntLe(u64, bytes[0..], 0));
    try std.testing.expectError(BytesError.OutOfBounds, readIntLe(u32, bytes[0..], 6));
}
