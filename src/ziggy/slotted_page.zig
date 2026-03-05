const std = @import("std");

pub const SlottedPageError = error{
    BlockFull,
    CorruptedBlock,
    InvalidLayout,
};

pub const SlottedPageBuilder = struct {
    allocator: std.mem.Allocator,
    block: []u8,
    block_size: usize,
    header_size: usize,
    data_end: u16,
    slot_count: u16,

    pub fn init(allocator: std.mem.Allocator, block_size: usize, header_size: usize) !SlottedPageBuilder {
        if (block_size > std.math.maxInt(u16)) return SlottedPageError.InvalidLayout;
        if (header_size > std.math.maxInt(u16)) return SlottedPageError.InvalidLayout;
        if (header_size >= block_size) return SlottedPageError.InvalidLayout;

        const block = try allocator.alloc(u8, block_size);
        @memset(block, 0);

        return .{
            .allocator = allocator,
            .block = block,
            .block_size = block_size,
            .header_size = header_size,
            .data_end = @intCast(header_size),
            .slot_count = 0,
        };
    }

    pub fn deinit(self: *SlottedPageBuilder) void {
        self.allocator.free(self.block);
    }

    fn slotPos(self: *const SlottedPageBuilder, idx: usize) usize {
        return self.block_size - ((idx + 1) * @sizeOf(u16));
    }

    fn writeU16(self: *SlottedPageBuilder, pos: usize, v: u16) void {
        self.block[pos] = @intCast(v & 0xFF);
        self.block[pos + 1] = @intCast((v >> 8) & 0xFF);
    }

    fn readU16(self: *const SlottedPageBuilder, pos: usize) u16 {
        return @as(u16, self.block[pos]) | (@as(u16, self.block[pos + 1]) << 8);
    }

    pub fn append(self: *SlottedPageBuilder, key: []const u8, value: []const u8) SlottedPageError!void {
        if (key.len > std.math.maxInt(u16) or value.len > std.math.maxInt(u16)) {
            return SlottedPageError.BlockFull;
        }
        const entry_len = 4 + key.len + value.len;

        const new_data_end_usize = @as(usize, self.data_end) + entry_len;
        const new_slot_count = @as(usize, self.slot_count) + 1;
        const new_slot_start = self.block_size - new_slot_count * @sizeOf(u16);

        if (new_data_end_usize > new_slot_start) return SlottedPageError.BlockFull;

        const tuple_offset: usize = self.data_end;
        if (tuple_offset > std.math.maxInt(u16)) return SlottedPageError.InvalidLayout;
        self.writeU16(tuple_offset, @intCast(key.len));
        self.writeU16(tuple_offset + 2, @intCast(value.len));
        @memcpy(self.block[tuple_offset + 4 .. tuple_offset + 4 + key.len], key);
        @memcpy(self.block[tuple_offset + 4 + key.len .. tuple_offset + 4 + key.len + value.len], value);

        const slot_pos = self.slotPos(self.slot_count);
        self.writeU16(slot_pos, @intCast(tuple_offset));

        self.data_end = @intCast(new_data_end_usize);
        self.slot_count += 1;
    }

    pub fn getSlotOffset(self: *const SlottedPageBuilder, logical_index: usize) SlottedPageError!u16 {
        if (logical_index >= self.slot_count) return SlottedPageError.CorruptedBlock;
        return self.readU16(self.slotPos(logical_index));
    }

    pub fn keyAt(self: *const SlottedPageBuilder, logical_index: usize) SlottedPageError![]const u8 {
        const off = try self.getSlotOffset(logical_index);
        const off_usize: usize = off;
        if (off_usize + 4 > self.block.len) return SlottedPageError.CorruptedBlock;
        const key_len = self.readU16(off_usize);
        const val_len = self.readU16(off_usize + 2);
        const start = off_usize + 4;
        const key_end = start + key_len;
        const value_end = key_end + val_len;
        if (value_end > self.block.len) return SlottedPageError.CorruptedBlock;
        return self.block[start..key_end];
    }

    pub fn valueAt(self: *const SlottedPageBuilder, logical_index: usize) SlottedPageError![]const u8 {
        const off = try self.getSlotOffset(logical_index);
        const off_usize: usize = off;
        if (off_usize + 4 > self.block.len) return SlottedPageError.CorruptedBlock;
        const key_len = self.readU16(off_usize);
        const val_len = self.readU16(off_usize + 2);
        const start = off_usize + 4 + key_len;
        const end = start + val_len;
        if (end > self.block.len) return SlottedPageError.CorruptedBlock;
        return self.block[start..end];
    }

    pub fn binarySearch(self: *const SlottedPageBuilder, key: []const u8) SlottedPageError!?usize {
        var lo: usize = 0;
        var hi: usize = self.slot_count;

        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const mid_key = try self.keyAt(mid);
            switch (std.mem.order(u8, mid_key, key)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return mid,
            }
        }
        return null;
    }

    pub fn freeGap(self: *const SlottedPageBuilder) usize {
        const slot_start = self.block_size - @as(usize, self.slot_count) * @sizeOf(u16);
        return slot_start - self.data_end;
    }
};

test "task 3.1 positive: footer slot offsets and binary search lookup" {
    var builder = try SlottedPageBuilder.init(std.testing.allocator, 8192, 32);
    defer builder.deinit();

    var i: usize = 0;
    while (i < 50) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        var val_buf: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key-{d:0>3}", .{i});
        const value = try std.fmt.bufPrint(&val_buf, "value-{d:0>3}-{d}", .{ i, i * 7 });
        try builder.append(key, value);
    }

    try std.testing.expectEqual(@as(u16, 50), builder.slot_count);

    i = 0;
    while (i < 50) : (i += 1) {
        const off = try builder.getSlotOffset(i);
        try std.testing.expect(off >= builder.header_size);
    }

    const idx = (try builder.binarySearch("key-025")).?;
    try std.testing.expectEqual(@as(usize, 25), idx);
    const val = try builder.valueAt(idx);
    try std.testing.expect(std.mem.startsWith(u8, val, "value-025"));
}

test "task 3.1 negative: oversized payload returns error.BlockFull" {
    var builder = try SlottedPageBuilder.init(std.testing.allocator, 4096, 32);
    defer builder.deinit();

    const free_before = builder.freeGap();

    const big = try std.testing.allocator.alloc(u8, free_before + 128);
    defer std.testing.allocator.free(big);
    @memset(big, 'x');

    try std.testing.expectError(SlottedPageError.BlockFull, builder.append("k", big));
    try std.testing.expectEqual(@as(u16, 0), builder.slot_count);
}
