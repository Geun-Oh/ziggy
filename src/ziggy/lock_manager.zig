const std = @import("std");

pub const LockError = error{
    WouldBlock,
    DeadlockDetected,
};

const LockEntry = struct {
    owner: u64,
    waiters: std.ArrayList(u64),
};

const Stripe = struct {
    mutex: std.Thread.Mutex = .{},
    locks: std.StringHashMap(LockEntry),

    fn init(allocator: std.mem.Allocator) Stripe {
        return .{ .locks = std.StringHashMap(LockEntry).init(allocator) };
    }
};

pub const LockManager = struct {
    allocator: std.mem.Allocator,
    stripes: []Stripe,
    wait_for_mutex: std.Thread.Mutex,
    wait_for: std.AutoHashMap(u64, u64), // waiter -> owner

    pub fn init(allocator: std.mem.Allocator, stripe_count: usize) !LockManager {
        const stripes = try allocator.alloc(Stripe, stripe_count);
        for (stripes) |*s| s.* = Stripe.init(allocator);

        return .{
            .allocator = allocator,
            .stripes = stripes,
            .wait_for_mutex = .{},
            .wait_for = std.AutoHashMap(u64, u64).init(allocator),
        };
    }

    pub fn deinit(self: *LockManager) void {
        for (self.stripes) |*stripe| {
            var it = stripe.locks.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                entry.value_ptr.waiters.deinit(self.allocator);
            }
            stripe.locks.deinit();
        }
        self.allocator.free(self.stripes);
        self.wait_for.deinit();
    }

    pub fn stripeFor(self: *const LockManager, key: []const u8) usize {
        return @as(usize, @intCast(std.hash.Wyhash.hash(0, key) % self.stripes.len));
    }

    fn detectCycleVictim(self: *LockManager, start: u64) !?u64 {
        var queue = std.ArrayList(u64).empty;
        defer queue.deinit(self.allocator);

        var seen = std.AutoHashMap(u64, void).init(self.allocator);
        defer seen.deinit();

        try queue.append(self.allocator, start);
        var q_idx: usize = 0;

        while (q_idx < queue.items.len) : (q_idx += 1) {
            const txn = queue.items[q_idx];
            if (seen.contains(txn)) continue;
            try seen.put(txn, {});

            if (self.wait_for.get(txn)) |next| {
                if (next == start) {
                    var victim = start;
                    var cursor = start;
                    var guard: usize = 0;
                    while (guard < 1024) : (guard += 1) {
                        victim = @max(victim, cursor);
                        const next_txn = self.wait_for.get(cursor) orelse break;
                        if (next_txn == start) {
                            victim = @max(victim, next_txn);
                            break;
                        }
                        cursor = next_txn;
                    }
                    return victim;
                }
                try queue.append(self.allocator, next);
            }
        }
        return null;
    }

    pub fn acquire(self: *LockManager, txn_id: u64, key: []const u8) !void {
        const stripe_id = self.stripeFor(key);
        var stripe = &self.stripes[stripe_id];

        stripe.mutex.lock();
        defer stripe.mutex.unlock();

        const existing = stripe.locks.getPtr(key);
        if (existing == null) {
            const key_copy = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(key_copy);
            try stripe.locks.put(key_copy, .{
                .owner = txn_id,
                .waiters = .empty,
            });

            self.wait_for_mutex.lock();
            defer self.wait_for_mutex.unlock();
            _ = self.wait_for.remove(txn_id);
            return;
        }

        const entry = existing.?;
        if (entry.owner == txn_id) return;

        self.wait_for_mutex.lock();
        defer self.wait_for_mutex.unlock();

        try self.wait_for.put(txn_id, entry.owner);
        if (try self.detectCycleVictim(txn_id)) |victim| {
            if (victim == txn_id) {
                _ = self.wait_for.remove(txn_id);
                return LockError.DeadlockDetected;
            }
        }

        try entry.waiters.append(self.allocator, txn_id);
        return LockError.WouldBlock;
    }

    pub fn release(self: *LockManager, txn_id: u64, key: []const u8) !void {
        const stripe_id = self.stripeFor(key);
        var stripe = &self.stripes[stripe_id];

        stripe.mutex.lock();
        defer stripe.mutex.unlock();

        const entry = stripe.locks.getPtr(key) orelse return;
        if (entry.owner != txn_id) return;

        if (entry.waiters.items.len == 0) {
            const removed = stripe.locks.fetchRemove(key) orelse return;
            self.allocator.free(removed.key);
            removed.value.waiters.deinit(self.allocator);
            return;
        }

        const new_owner = entry.waiters.orderedRemove(0);
        entry.owner = new_owner;

        self.wait_for_mutex.lock();
        defer self.wait_for_mutex.unlock();
        _ = self.wait_for.remove(new_owner);
    }
};

fn findDistinctStripeKeys(manager: *LockManager, allocator: std.mem.Allocator) !struct { a: []u8, b: []u8 } {
    const first_key = try std.fmt.allocPrint(allocator, "k-{d}", .{0});
    const first_stripe = manager.stripeFor(first_key);

    var i: usize = 1;
    while (i < 10000) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "k-{d}", .{i});
        const stripe = manager.stripeFor(key);
        if (stripe != first_stripe) {
            return .{ .a = first_key, .b = key };
        }
        allocator.free(key);
    }

    allocator.free(first_key);
    return error.OutOfMemory;
}

test "task 4.2 positive: independent stripes lock concurrently without blocking" {
    var manager = try LockManager.init(std.testing.allocator, 256);
    defer manager.deinit();

    const keys = try findDistinctStripeKeys(&manager, std.testing.allocator);
    defer std.testing.allocator.free(keys.a);
    defer std.testing.allocator.free(keys.b);

    try manager.acquire(1, keys.a);
    try manager.acquire(2, keys.b);
}

test "task 4.2 negative: wait-for cycle returns DeadlockDetected for newest txn" {
    var manager = try LockManager.init(std.testing.allocator, 64);
    defer manager.deinit();

    try manager.acquire(1, "Key 1");
    try manager.acquire(2, "Key 2");

    try std.testing.expectError(LockError.WouldBlock, manager.acquire(1, "Key 2"));
    try std.testing.expectError(LockError.DeadlockDetected, manager.acquire(2, "Key 1"));
}
