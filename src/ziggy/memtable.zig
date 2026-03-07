//! Lock-free skip-list memtable used as the in-memory write buffer.
const std = @import("std");

// Insert-time validation and allocation failures for the memtable.
pub const MemTableError = error{
    KeyTooLarge,
    OutOfMemory,
};

pub const MAX_LEVELS: usize = 8;

const Node = struct {
    key: []u8,
    value: []u8,
    height: u8,
    next: [MAX_LEVELS]std.atomic.Value(?*Node),

    // Initializes one skip-list node with all next pointers cleared.
    fn init(key: []u8, value: []u8, height: u8) Node {
        var node = Node{
            .key = key,
            .value = value,
            .height = height,
            .next = undefined,
        };
        for (&node.next) |*ptr| ptr.* = std.atomic.Value(?*Node).init(null);
        return node;
    }
};

// Concurrent skip list storing sorted key/value pairs.
pub const MemTable = struct {
    allocator: std.mem.Allocator,
    max_key_size: usize,
    head: *Node,
    node_count: std.atomic.Value(usize),

    // Creates the head tower and records the maximum accepted key length.
    pub fn init(allocator: std.mem.Allocator, max_key_size: usize) !MemTable {
        const head = try allocator.create(Node);
        head.* = Node.init(try allocator.dupe(u8, ""), try allocator.dupe(u8, ""), MAX_LEVELS);
        return .{
            .allocator = allocator,
            .max_key_size = max_key_size,
            .head = head,
            .node_count = std.atomic.Value(usize).init(0),
        };
    }

    pub fn deinit(self: *MemTable) void {
        var curr = self.head.next[0].load(.acquire);
        while (curr) |node| {
            const next = node.next[0].load(.acquire);
            self.allocator.free(node.key);
            self.allocator.free(node.value);
            self.allocator.destroy(node);
            curr = next;
        }

        self.allocator.free(self.head.key);
        self.allocator.free(self.head.value);
        self.allocator.destroy(self.head);
    }

    // Deterministically derives a tower height from the key hash.
    fn pickHeight(key: []const u8) u8 {
        var h: u8 = 1;
        var bits = std.hash.Wyhash.hash(0, key);
        while (h < MAX_LEVELS and (bits & 0x3) == 0) {
            h += 1;
            bits >>= 2;
        }
        return h;
    }

    // Centralized bytewise key ordering helper.
    fn keyOrder(a: []const u8, b: []const u8) std.math.Order {
        return std.mem.order(u8, a, b);
    }

    // Finds predecessor and successor nodes at each level for a prospective insert.
    fn findSplice(self: *MemTable, key: []const u8, preds: *[MAX_LEVELS]*Node, succs: *[MAX_LEVELS]?*Node) bool {
        var pred = self.head;
        var found = false;

        var level: usize = MAX_LEVELS;
        while (level > 0) {
            level -= 1;

            var curr = pred.next[level].load(.acquire);
            while (curr) |node| {
                switch (keyOrder(node.key, key)) {
                    .lt => {
                        pred = node;
                        curr = node.next[level].load(.acquire);
                    },
                    .eq => {
                        found = true;
                        break;
                    },
                    .gt => break,
                }
            }

            preds[level] = pred;
            succs[level] = curr;
        }

        return found;
    }

    // Allocates an owned node copy for insertion.
    fn allocateNode(self: *MemTable, key: []const u8, value: []const u8, height: u8) MemTableError!*Node {
        const key_copy = self.allocator.dupe(u8, key) catch return MemTableError.OutOfMemory;
        errdefer self.allocator.free(key_copy);

        const value_copy = self.allocator.dupe(u8, value) catch return MemTableError.OutOfMemory;
        errdefer self.allocator.free(value_copy);

        const node = self.allocator.create(Node) catch return MemTableError.OutOfMemory;
        node.* = Node.init(key_copy, value_copy, height);
        return node;
    }

    // Inserts a key once and links a new node into the skip list from the bottom up.
    pub fn insert(self: *MemTable, key: []const u8, value: []const u8) MemTableError!void {
        if (key.len > self.max_key_size) return MemTableError.KeyTooLarge;

        const height = pickHeight(key);

        while (true) {
            var preds: [MAX_LEVELS]*Node = undefined;
            var succs: [MAX_LEVELS]?*Node = undefined;
            if (self.findSplice(key, &preds, &succs)) return;

            const node = try self.allocateNode(key, value, height);

            var lvl: usize = 0;
            while (lvl < height) : (lvl += 1) {
                node.next[lvl].store(succs[lvl], .release);
            }

            if (preds[0].next[0].cmpxchgWeak(succs[0], node, .release, .acquire) != null) {
                self.allocator.free(node.key);
                self.allocator.free(node.value);
                self.allocator.destroy(node);
                continue;
            }

            lvl = 1;
            while (lvl < height) : (lvl += 1) {
                while (true) {
                    node.next[lvl].store(succs[lvl], .release);
                    if (preds[lvl].next[lvl].cmpxchgWeak(succs[lvl], node, .release, .acquire) == null) {
                        break;
                    }
                    _ = self.findSplice(key, &preds, &succs);
                }
            }

            _ = self.node_count.fetchAdd(1, .acq_rel);
            return;
        }
    }

    // Traverses the skip list to find an exact key match.
    pub fn get(self: *MemTable, key: []const u8) ?[]const u8 {
        var pred = self.head;

        var level: usize = MAX_LEVELS;
        while (level > 0) {
            level -= 1;

            var curr = pred.next[level].load(.acquire);
            while (curr) |node| {
                switch (keyOrder(node.key, key)) {
                    .lt => {
                        pred = node;
                        curr = node.next[level].load(.acquire);
                    },
                    .eq => return node.value,
                    .gt => break,
                }
            }
        }

        return null;
    }

    // Returns the number of inserted nodes currently visible from level 0.
    pub fn count(self: *MemTable) usize {
        return self.node_count.load(.acquire);
    }

    // Materializes all keys in sorted order for tests and diagnostics.
    pub fn collectKeys(self: *MemTable, allocator: std.mem.Allocator) ![][]u8 {
        var out = std.ArrayList([]u8).empty;
        errdefer {
            for (out.items) |k| allocator.free(k);
            out.deinit(allocator);
        }

        var curr = self.head.next[0].load(.acquire);
        while (curr) |node| {
            try out.append(allocator, try allocator.dupe(u8, node.key));
            curr = node.next[0].load(.acquire);
        }

        return out.toOwnedSlice(allocator);
    }
};

test "task 2.1 positive: concurrent writers/readers preserve sorted visibility" {
    const allocator = std.heap.c_allocator;

    var table = try MemTable.init(allocator, 128);
    defer table.deinit();

    const writers = 12;
    const per_writer = 200;
    const readers = 6;

    const WriterCtx = struct {
        table: *MemTable,
        writer_id: usize,

        fn run(ctx: @This()) void {
            var key_buf: [32]u8 = undefined;
            var val_buf: [32]u8 = undefined;
            var i: usize = 0;
            while (i < per_writer) : (i += 1) {
                const global = ctx.writer_id * per_writer + i;
                const key = std.fmt.bufPrint(&key_buf, "key-{d:0>8}", .{global}) catch unreachable;
                const val = std.fmt.bufPrint(&val_buf, "value-{d:0>8}", .{global}) catch unreachable;
                ctx.table.insert(key, val) catch unreachable;
            }
        }
    };

    const ReaderCtx = struct {
        table: *MemTable,

        fn run(ctx: @This()) void {
            var rounds: usize = 0;
            while (rounds < 2000) : (rounds += 1) {
                _ = ctx.table.get("key-00000010");
                _ = ctx.table.get("key-00010000");
                _ = ctx.table.get("key-00099999");
            }
        }
    };

    var writer_threads: [writers]std.Thread = undefined;
    var reader_threads: [readers]std.Thread = undefined;

    for (0..writers) |idx| {
        writer_threads[idx] = try std.Thread.spawn(.{}, WriterCtx.run, .{WriterCtx{ .table = &table, .writer_id = idx }});
    }
    for (0..readers) |idx| {
        reader_threads[idx] = try std.Thread.spawn(.{}, ReaderCtx.run, .{ReaderCtx{ .table = &table }});
    }

    for (&writer_threads) |*t| t.join();
    for (&reader_threads) |*t| t.join();

    try std.testing.expectEqual(@as(usize, writers * per_writer), table.count());

    const keys = try table.collectKeys(std.testing.allocator);
    defer {
        for (keys) |k| std.testing.allocator.free(k);
        std.testing.allocator.free(keys);
    }

    var i: usize = 1;
    while (i < keys.len) : (i += 1) {
        try std.testing.expect(std.mem.order(u8, keys[i - 1], keys[i]) != .gt);
    }
}

test "task 2.1 negative: oversized key returns error.KeyTooLarge" {
    var table = try MemTable.init(std.testing.allocator, 16);
    defer table.deinit();

    var huge: [128]u8 = undefined;
    @memset(&huge, 'k');

    try std.testing.expectError(MemTableError.KeyTooLarge, table.insert(&huge, "v"));
    try std.testing.expectEqual(@as(usize, 0), table.count());
}
