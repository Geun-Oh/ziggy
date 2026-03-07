//! Read-path helpers: bloom filtering plus a k-way merge iterator for ordered sources.
const std = @import("std");

// Errors returned by lightweight read-path helpers.
pub const AccessError = error{
    IteratorExhausted,
    OutOfMemory,
};

// Probabilistic membership filter used to skip obvious disk misses.
pub const BloomFilter = struct {
    allocator: std.mem.Allocator,
    bits: []u8,
    bit_count: usize,
    hash_count: usize,

    // Allocates the backing bitset and derives a bounded number of hash probes.
    pub fn init(allocator: std.mem.Allocator, key_count: usize, bits_per_key: usize) !BloomFilter {
        const bit_count = @max(64, key_count * bits_per_key);
        const byte_count = std.math.divCeil(usize, bit_count, 8) catch unreachable;
        const bits = try allocator.alloc(u8, byte_count);
        @memset(bits, 0);

        const k = @max(@as(usize, 1), @min(@as(usize, 12), @as(usize, @intFromFloat(@round((@as(f64, @floatFromInt(bits_per_key)) * std.math.ln2))))));

        return .{
            .allocator = allocator,
            .bits = bits,
            .bit_count = bit_count,
            .hash_count = k,
        };
    }

    // Releases the bitset allocated by init.
    pub fn deinit(self: *BloomFilter) void {
        self.allocator.free(self.bits);
    }

    // Derives the idx-th probe position from the key and hash seed.
    fn nthHash(self: *const BloomFilter, key: []const u8, idx: usize) usize {
        const h = std.hash.Wyhash.hash(@as(u64, idx) *% 0x9E3779B185EBCA87, key);
        return @as(usize, @intCast(h % self.bit_count));
    }

    // Sets one bit inside the backing bitset.
    fn setBit(self: *BloomFilter, bit_index: usize) void {
        const byte_idx = bit_index / 8;
        const bit = @as(u3, @intCast(bit_index % 8));
        self.bits[byte_idx] |= (@as(u8, 1) << bit);
    }

    // Tests one bit inside the backing bitset.
    fn testBit(self: *const BloomFilter, bit_index: usize) bool {
        const byte_idx = bit_index / 8;
        const bit = @as(u3, @intCast(bit_index % 8));
        return (self.bits[byte_idx] & (@as(u8, 1) << bit)) != 0;
    }

    // Marks the hash positions for a key.
    pub fn add(self: *BloomFilter, key: []const u8) void {
        for (0..self.hash_count) |idx| {
            self.setBit(self.nthHash(key, idx));
        }
    }

    // Returns false only when the key is definitely absent.
    pub fn mayContain(self: *const BloomFilter, key: []const u8) bool {
        for (0..self.hash_count) |idx| {
            if (!self.testBit(self.nthHash(key, idx))) return false;
        }
        return true;
    }
};

// Test-oriented wrapper that counts how often the bloom filter allows a disk read.
pub const AccessProbe = struct {
    bloom: BloomFilter,
    disk_reads: usize = 0,

    pub fn deinit(self: *AccessProbe) void {
        self.bloom.deinit();
    }

    // Simulates a guarded disk lookup and increments the read counter only on bloom hits.
    pub fn maybeReadFromDisk(self: *AccessProbe, key: []const u8) bool {
        if (!self.bloom.mayContain(key)) return false;
        self.disk_reads += 1;
        return true;
    }
};

// One versioned key/value item flowing through the merge heap.
pub const MergeItem = struct {
    key: []const u8,
    value: []const u8,
    sequence: u64,
    cursor_index: usize,
};

// Per-input cursor used by the merge iterator.
pub const Cursor = struct {
    entries: []const MergeItem,
    index: usize = 0,

    // Returns the current merge item for this input stream.
    fn current(self: *const Cursor) ?MergeItem {
        if (self.index >= self.entries.len) return null;
        return self.entries[self.index];
    }

    // Advances the cursor by one item.
    fn advance(self: *Cursor) void {
        self.index += 1;
    }
};

// Heap ordering: smallest key first, newest sequence first for ties.
fn lessThan(_: void, a: MergeItem, b: MergeItem) std.math.Order {
    const key_order = std.mem.order(u8, a.key, b.key);
    if (key_order != .eq) return key_order;
    if (a.sequence > b.sequence) return .lt;
    if (a.sequence < b.sequence) return .gt;
    return .eq;
}

// Merges multiple already-sorted input streams while preferring newer sequence numbers per key.
pub const KWayMergingIterator = struct {
    allocator: std.mem.Allocator,
    cursors: []Cursor,
    heap: std.PriorityQueue(MergeItem, void, lessThan),

    // Seeds the priority queue with the first item from each input stream.
    pub fn init(allocator: std.mem.Allocator, inputs: []const []const MergeItem) !KWayMergingIterator {
        const cursors = try allocator.alloc(Cursor, inputs.len);
        for (inputs, 0..) |input, idx| {
            cursors[idx] = .{ .entries = input, .index = 0 };
        }

        var heap = std.PriorityQueue(MergeItem, void, lessThan).init(allocator, {});
        errdefer heap.deinit();

        for (cursors, 0..) |*cursor, idx| {
            if (cursor.current()) |item| {
                try heap.add(.{
                    .key = item.key,
                    .value = item.value,
                    .sequence = item.sequence,
                    .cursor_index = idx,
                });
            }
        }

        return .{
            .allocator = allocator,
            .cursors = cursors,
            .heap = heap,
        };
    }

    pub fn deinit(self: *KWayMergingIterator) void {
        self.heap.deinit();
        self.allocator.free(self.cursors);
    }

    // Returns the next globally ordered item and advances only the source that produced it.
    pub fn next(self: *KWayMergingIterator) AccessError!MergeItem {
        const item = self.heap.removeOrNull() orelse return AccessError.IteratorExhausted;

        var cursor = &self.cursors[item.cursor_index];
        cursor.advance();
        if (cursor.current()) |next_item| {
            self.heap.add(.{
                .key = next_item.key,
                .value = next_item.value,
                .sequence = next_item.sequence,
                .cursor_index = item.cursor_index,
            }) catch return AccessError.OutOfMemory;
        }

        return item;
    }
};

test "task 4.3 positive: bloom negative bypasses disk I/O" {
    var bloom = try BloomFilter.init(std.testing.allocator, 1000, 10);
    bloom.add("known-key");

    var probe = AccessProbe{ .bloom = bloom };
    defer probe.deinit();

    const read = probe.maybeReadFromDisk("definitely-absent-key");
    try std.testing.expect(!read);
    try std.testing.expectEqual(@as(usize, 0), probe.disk_reads);
}

test "task 4.3 negative: exhausted merge iterator returns IteratorExhausted" {
    const a = [_]MergeItem{.{ .key = "a", .value = "1", .sequence = 10, .cursor_index = 0 }};
    const b = [_]MergeItem{.{ .key = "b", .value = "2", .sequence = 9, .cursor_index = 1 }};

    var iter = try KWayMergingIterator.init(std.testing.allocator, &.{ a[0..], b[0..] });
    defer iter.deinit();

    _ = try iter.next();
    _ = try iter.next();
    try std.testing.expectError(AccessError.IteratorExhausted, iter.next());
}
