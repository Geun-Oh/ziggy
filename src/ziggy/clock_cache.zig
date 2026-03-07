//! Lock-free clock-style cache metadata that tracks shared slot ownership by hash.
const std = @import("std");

// Errors surfaced by cache admission and leak checks.
pub const CacheError = error{
    CacheFull,
    ReferenceLeak,
};

const STATE_SHIFT: u6 = 60;
const HIT_BIT: u64 = (@as(u64, 1) << 63);
const RELEASE_INC: u64 = (@as(u64, 1) << 30);
const ACQUIRE_MASK: u64 = 0x3fffffff;
const RELEASE_MASK: u64 = 0x3fffffff << 30;

const SlotState = enum(u3) {
    empty = 0,
    occupied = 1,
};

// One cache metadata slot: key identity plus packed acquire/release counters.
pub const Slot = struct {
    key_hash: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    meta: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

// Borrowed reference returned by fetch; callers must release it to clear the logical pin.
pub const Handle = struct {
    cache: *ClockCache,
    slot_index: usize,
    released: bool = false,

    // Marks the handle as released exactly once.
    pub fn release(self: *Handle) void {
        if (self.released) return;
        self.cache.incrementRelease(&self.cache.slots[self.slot_index]);
        self.released = true;
    }
};

// Fixed-size cache directory that deduplicates hot keys onto shared slots without a global mutex.
pub const ClockCache = struct {
    allocator: std.mem.Allocator,
    slots: []Slot,

    // Allocates and zero-initializes the slot array.
    pub fn init(allocator: std.mem.Allocator, slot_count: usize) !ClockCache {
        const slots = try allocator.alloc(Slot, slot_count);
        for (slots) |*slot| slot.* = .{};
        return .{ .allocator = allocator, .slots = slots };
    }

    pub fn deinit(self: *ClockCache) void {
        self.allocator.free(self.slots);
    }

    fn slotState(meta: u64) SlotState {
        return @enumFromInt((meta >> STATE_SHIFT) & 0x7);
    }

    fn makeOccupied(meta: u64) u64 {
        return (meta & ~(@as(u64, 0x7) << STATE_SHIFT)) | (@as(u64, @intFromEnum(SlotState.occupied)) << STATE_SHIFT);
    }

    fn incrementAcquire(_: *ClockCache, slot: *Slot) void {
        while (true) {
            const meta = slot.meta.load(.acquire);
            const acq = meta & ACQUIRE_MASK;
            const rel = meta & RELEASE_MASK;
            const state = meta & (@as(u64, 0x7) << STATE_SHIFT);
            const hit = meta & HIT_BIT;
            const next_acq = if (acq == ACQUIRE_MASK) acq else acq + 1;
            const next = state | hit | rel | next_acq;
            if (slot.meta.cmpxchgWeak(meta, next | HIT_BIT, .acq_rel, .acquire) == null) break;
        }
    }

    fn incrementRelease(_: *ClockCache, slot: *Slot) void {
        while (true) {
            const meta = slot.meta.load(.acquire);
            const rel = (meta & RELEASE_MASK) >> 30;
            const acq = meta & ACQUIRE_MASK;
            const state = meta & (@as(u64, 0x7) << STATE_SHIFT);
            const hit = meta & HIT_BIT;
            const next_rel = if (rel == ACQUIRE_MASK) rel else rel + 1;
            const next = state | hit | acq | (next_rel << 30);
            if (slot.meta.cmpxchgWeak(meta, next, .acq_rel, .acquire) == null) break;
        }
    }

    // Finds or claims a slot for the key hash and returns a tracked handle.
    pub fn fetch(self: *ClockCache, key: []const u8) CacheError!Handle {
        const key_hash = std.hash.Wyhash.hash(0, key);
        var idx = @as(usize, @intCast(key_hash % self.slots.len));

        var probed: usize = 0;
        while (probed < self.slots.len) {
            var slot = &self.slots[idx];

            while (true) {
                const meta = slot.meta.load(.acquire);
                switch (slotState(meta)) {
                    .empty => {
                        const occupied = makeOccupied(meta);
                        if (slot.meta.cmpxchgWeak(meta, occupied, .acq_rel, .acquire) == null) {
                            _ = slot.key_hash.cmpxchgWeak(0, key_hash, .acq_rel, .acquire);
                            break;
                        }
                    },
                    .occupied => break,
                }
            }

            const stored_hash = slot.key_hash.load(.acquire);
            if (stored_hash == 0) {
                if (slot.key_hash.cmpxchgWeak(0, key_hash, .acq_rel, .acquire) == null) {
                    self.incrementAcquire(slot);
                    return .{ .cache = self, .slot_index = idx };
                }
                continue;
            }

            if (stored_hash == key_hash) {
                self.incrementAcquire(slot);
                return .{ .cache = self, .slot_index = idx };
            }

            idx = (idx + 1) % self.slots.len;
            probed += 1;
        }

        return CacheError.CacheFull;
    }

    // Exposes packed acquire/release counters for tests and diagnostics.
    pub fn inspectCounts(self: *ClockCache, slot_index: usize) struct { acquire: u32, release: u32, refs: u32 } {
        const meta = self.slots[slot_index].meta.load(.acquire);
        const acq: u32 = @intCast(meta & ACQUIRE_MASK);
        const rel: u32 = @intCast((meta & RELEASE_MASK) >> 30);
        return .{ .acquire = acq, .release = rel, .refs = acq - rel };
    }

    // Verifies that every acquired handle has been released.
    pub fn assertNoLeaks(self: *ClockCache) CacheError!void {
        for (self.slots, 0..) |_, idx| {
            const c = self.inspectCounts(idx);
            if (c.refs != 0) return CacheError.ReferenceLeak;
        }
    }

    // Counts how many slots currently advertise the same hashed key identity.
    pub fn countSlotsWithHash(self: *ClockCache, key_hash: u64) usize {
        var total: usize = 0;
        for (self.slots) |slot| {
            if (slot.key_hash.load(.acquire) == key_hash) total += 1;
        }
        return total;
    }
};

test "task 5.1 positive: contested fetch increments acquire count without mutex bottleneck" {
    var cache = try ClockCache.init(std.heap.c_allocator, 8);
    defer cache.deinit();

    const threads_n = 100;

    const Ctx = struct {
        cache: *ClockCache,
        fn run(ctx: @This()) void {
            var h = ctx.cache.fetch("shared-block") catch unreachable;
            h.release();
        }
    };

    var threads: [threads_n]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .cache = &cache }}) catch unreachable;
    }
    for (&threads) |*t| t.join();

    const idx = @as(usize, @intCast(std.hash.Wyhash.hash(0, "shared-block") % cache.slots.len));
    const counts = cache.inspectCounts(idx);
    try std.testing.expectEqual(@as(u32, threads_n), counts.acquire);
    try std.testing.expectEqual(@as(u32, threads_n), counts.release);
    try std.testing.expectEqual(@as(u32, 0), counts.refs);
}

test "task 5.1 stress: high-iteration contention keeps a single shared slot identity" {
    var cache = try ClockCache.init(std.heap.c_allocator, 64);
    defer cache.deinit();

    const threads_n = 48;
    const iterations = 1500;

    var start = std.atomic.Value(bool).init(false);

    const Ctx = struct {
        cache: *ClockCache,
        start: *std.atomic.Value(bool),

        fn run(ctx: @This()) void {
            while (!ctx.start.load(.acquire)) {
                std.Thread.yield() catch {};
            }

            var i: usize = 0;
            while (i < iterations) : (i += 1) {
                var h = ctx.cache.fetch("shared-hot-key") catch unreachable;
                h.release();
            }
        }
    };

    var threads: [threads_n]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .cache = &cache, .start = &start }}) catch unreachable;
    }

    start.store(true, .release);

    for (&threads) |*t| t.join();

    const hash = std.hash.Wyhash.hash(0, "shared-hot-key");
    try std.testing.expectEqual(@as(usize, 1), cache.countSlotsWithHash(hash));

    const idx = @as(usize, @intCast(hash % cache.slots.len));
    const c = cache.inspectCounts(idx);
    try std.testing.expectEqual(@as(u32, threads_n * iterations), c.acquire);
    try std.testing.expectEqual(@as(u32, threads_n * iterations), c.release);

    try cache.assertNoLeaks();
}

test "task 5.1 fault injection: pre-occupied metadata with zero hash does not split counts" {
    var cache = try ClockCache.init(std.heap.c_allocator, 16);
    defer cache.deinit();

    const hash = std.hash.Wyhash.hash(0, "shared-race-key");
    const idx = @as(usize, @intCast(hash % cache.slots.len));

    // Simulate race window: slot is occupied but hash is not initialized yet.
    cache.slots[idx].meta.store(ClockCache.makeOccupied(0), .release);
    cache.slots[idx].key_hash.store(0, .release);

    const threads_n = 32;

    const Ctx = struct {
        cache: *ClockCache,

        fn run(ctx: @This()) void {
            var h = ctx.cache.fetch("shared-race-key") catch unreachable;
            h.release();
        }
    };

    var threads: [threads_n]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .cache = &cache }}) catch unreachable;
    }
    for (&threads) |*t| t.join();

    try std.testing.expectEqual(@as(usize, 1), cache.countSlotsWithHash(hash));
    const c = cache.inspectCounts(idx);
    try std.testing.expectEqual(@as(u32, threads_n), c.acquire);
    try std.testing.expectEqual(@as(u32, threads_n), c.release);
    try cache.assertNoLeaks();
}

test "task 5.1 negative: missing handle release is detected as reference leak" {
    var cache = try ClockCache.init(std.testing.allocator, 4);
    defer cache.deinit();

    _ = try cache.fetch("leaky-block");
    try std.testing.expectError(CacheError.ReferenceLeak, cache.assertNoLeaks());
}
