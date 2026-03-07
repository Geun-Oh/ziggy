//! Public package entrypoint that re-exports the storage engine modules.
const std = @import("std");

pub const wal = @import("ziggy/wal.zig");
pub const manifest = @import("ziggy/manifest.zig");
pub const memtable = @import("ziggy/memtable.zig");
pub const slotted_page = @import("ziggy/slotted_page.zig");
pub const prefix_block = @import("ziggy/prefix_block.zig");
pub const mvcc = @import("ziggy/mvcc.zig");
pub const lock_manager = @import("ziggy/lock_manager.zig");
pub const access = @import("ziggy/access.zig");
pub const clock_cache = @import("ziggy/clock_cache.zig");
pub const compaction = @import("ziggy/compaction.zig");
pub const recovery = @import("ziggy/recovery.zig");
pub const platform = @import("ziggy/platform.zig");
pub const comptime_bytes = @import("ziggy/comptime_bytes.zig");
pub const engine = @import("ziggy/engine.zig");

// Small sample helper that prints a test hint using buffered stdout.
pub fn bufferedPrint() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try stdout.print("Run `zig build test` to run the tests.\n", .{});
    try stdout.flush();
}

// Tiny example function kept by the starter template tests.
pub fn add(a: i32, b: i32) i32 {
    return a + b;
}

// Returns a fixed tag used by one regression-style test.
pub fn loopGoalTag() []const u8 {
    return "test loop";
}

test "basic add functionality" {
    try std.testing.expect(add(3, 7) == 10);
}

test "loop goal tag is stable" {
    try std.testing.expectEqualStrings("test loop", loopGoalTag());
}

test {
    _ = wal;
    _ = manifest;
    _ = memtable;
    _ = slotted_page;
    _ = prefix_block;
    _ = mvcc;
    _ = lock_manager;
    _ = access;
    _ = clock_cache;
    _ = compaction;
    _ = recovery;
    _ = platform;
    _ = comptime_bytes;
    _ = engine;
}
