//! CLI entrypoint that exposes the embedded engine through small operator-facing commands.
const std = @import("std");
const ziggy = @import("ziggy");

const CliError = error{
    InvalidArguments,
};

// Wraps the CLI runner and converts failures into human-readable or JSON errors.
pub fn main() !void {
    run() catch |err| {
        var stderr_buffer: [2048]u8 = undefined;
        var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);
        const stderr = &stderr_writer.interface;

        const use_json = processHasFlag("--json");
        if (use_json) {
            try stderr.writeAll("{\"ok\":false,\"error\":");
            try writeJsonString(stderr, errorMessage(err));
            try stderr.writeAll("}\n");
        } else {
            switch (err) {
                CliError.InvalidArguments => {
                    try printUsage(stderr);
                },
                else => {
                    try stderr.print("error: {s}\n", .{errorMessage(err)});
                },
            }
        }

        try stderr.flush();
        std.process.exit(1);
    };
}

// Collects process arguments, prepares buffered stdout, and dispatches the command.
fn run() !void {
    const allocator = std.heap.page_allocator;

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try runWithArgs(allocator, args, stdout);
    try stdout.flush();
}

// Validates arguments, opens the database, and executes one CLI command.
fn runWithArgs(allocator: std.mem.Allocator, args: []const []const u8, stdout: anytype) !void {
    try validateArgs(args);

    const cmd = args[1];
    const json = hasFlag(args, "--json");
    const path = valueFor(args, "--path") orelse return CliError.InvalidArguments;

    var eng = try ziggy.engine.Engine.open(allocator, .{ .path = path });
    defer eng.close() catch {};

    if (std.mem.eql(u8, cmd, "open")) {
        if (json) {
            try stdout.writeAll("{\"ok\":true}\n");
        } else {
            try stdout.print("open ok\n", .{});
        }
    } else if (std.mem.eql(u8, cmd, "put")) {
        const key = valueFor(args, "--key") orelse return CliError.InvalidArguments;
        const value = valueFor(args, "--value") orelse return CliError.InvalidArguments;
        try eng.put(key, value);
        if (json) {
            try stdout.writeAll("{\"ok\":true,\"op\":\"put\"}\n");
        } else {
            try stdout.print("ok\n", .{});
        }
    } else if (std.mem.eql(u8, cmd, "get")) {
        const key = valueFor(args, "--key") orelse return CliError.InvalidArguments;
        const value = try eng.get(key);
        defer eng.freeValue(value);

        if (json) {
            try stdout.writeAll("{\"key\":");
            try writeJsonString(stdout, key);
            try stdout.writeAll(",\"value\":");
            try writeJsonString(stdout, value);
            try stdout.writeAll("}\n");
        } else {
            try stdout.print("{s}\n", .{value});
        }
    } else if (std.mem.eql(u8, cmd, "delete")) {
        const key = valueFor(args, "--key") orelse return CliError.InvalidArguments;
        try eng.delete(key);
        if (json) {
            try stdout.writeAll("{\"ok\":true,\"op\":\"delete\"}\n");
        } else {
            try stdout.print("ok\n", .{});
        }
    } else if (std.mem.eql(u8, cmd, "scan")) {
        const prefix = valueFor(args, "--prefix");
        const start = valueFor(args, "--start");
        const end = valueFor(args, "--end");

        if (prefix != null and (start != null or end != null)) return CliError.InvalidArguments;

        const rows = if (prefix) |p|
            try eng.scanPrefix(p)
        else
            try eng.scan(.{ .start = start, .end = end });
        defer eng.freeScan(rows);

        if (json) {
            try stdout.writeAll("[");
            for (rows, 0..) |row, idx| {
                if (idx != 0) try stdout.writeAll(",");
                try stdout.writeAll("{\"key\":");
                try writeJsonString(stdout, row.key);
                try stdout.writeAll(",\"value\":");
                try writeJsonString(stdout, row.value);
                try stdout.writeAll("}");
            }
            try stdout.writeAll("]\n");
        } else {
            for (rows) |row| {
                try stdout.print("{s}\t{s}\n", .{ row.key, row.value });
            }
        }
    } else if (std.mem.eql(u8, cmd, "stats")) {
        const s = try eng.stats();
        if (json) {
            try stdout.print("{{\"key_count\":{d},\"next_sequence\":{d},\"wal_size_bytes\":{d}}}\n", .{ s.key_count, s.next_sequence, s.wal_size_bytes });
        } else {
            try stdout.print("keys: {d}\nnext_sequence: {d}\nwal_size_bytes: {d}\n", .{ s.key_count, s.next_sequence, s.wal_size_bytes });
        }
    } else if (std.mem.eql(u8, cmd, "property")) {
        const name = valueFor(args, "--name") orelse return CliError.InvalidArguments;
        const value = try eng.property(name);
        if (json) {
            try stdout.writeAll("{\"name\":");
            try writeJsonString(stdout, name);
            switch (value) {
                .u64 => |v| try stdout.print(",\"type\":\"u64\",\"value\":{d}}}\n", .{v}),
                .text => |v| {
                    try stdout.writeAll(",\"type\":\"text\",\"value\":");
                    try writeJsonString(stdout, v);
                    try stdout.writeAll("}\n");
                },
            }
        } else {
            switch (value) {
                .u64 => |v| try stdout.print("{s}: {d}\n", .{ name, v }),
                .text => |v| try stdout.print("{s}: {s}\n", .{ name, v }),
            }
        }
    } else if (std.mem.eql(u8, cmd, "doctor")) {
        try eng.doctor();
        if (json) {
            try stdout.writeAll("{\"ok\":true,\"checks\":[\"manifest\",\"wal\"]}\n");
        } else {
            try stdout.print("doctor ok (manifest + wal)\n", .{});
        }
    } else {
        return CliError.InvalidArguments;
    }
}

// Returns true when a flag token appears anywhere in the argument list.
fn hasFlag(args: []const []const u8, flag: []const u8) bool {
    for (args) |a| if (std.mem.eql(u8, a, flag)) return true;
    return false;
}

// Returns the value paired with a named option like --path or --key.
fn valueFor(args: []const []const u8, name: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], name)) return args[i + 1];
    }
    return null;
}

// Enforces command shape, required options, and scan bound invariants before execution.
fn validateArgs(args: []const []const u8) !void {
    if (args.len < 2) return CliError.InvalidArguments;

    const cmd = args[1];
    if (!isCommand(cmd)) return CliError.InvalidArguments;

    var i: usize = 2;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            i += 1;
            continue;
        }
        if (isValueFlag(arg)) {
            if (i + 1 >= args.len) return CliError.InvalidArguments;
            i += 2;
            continue;
        }
        return CliError.InvalidArguments;
    }

    const path = valueFor(args, "--path") orelse return CliError.InvalidArguments;
    if (path.len == 0) return CliError.InvalidArguments;

    if (std.mem.eql(u8, cmd, "put")) {
        _ = valueFor(args, "--key") orelse return CliError.InvalidArguments;
        _ = valueFor(args, "--value") orelse return CliError.InvalidArguments;
    } else if (std.mem.eql(u8, cmd, "get") or std.mem.eql(u8, cmd, "delete")) {
        _ = valueFor(args, "--key") orelse return CliError.InvalidArguments;
    } else if (std.mem.eql(u8, cmd, "property")) {
        _ = valueFor(args, "--name") orelse return CliError.InvalidArguments;
    } else if (std.mem.eql(u8, cmd, "scan")) {
        const prefix = valueFor(args, "--prefix");
        const start = valueFor(args, "--start");
        const end = valueFor(args, "--end");
        if (prefix != null and (start != null or end != null)) return CliError.InvalidArguments;
        if (end != null and start == null) return CliError.InvalidArguments;
        if (start) |s| {
            if (end) |e| {
                if (std.mem.order(u8, s, e) == .gt) return CliError.InvalidArguments;
            }
        }
    }
}

// Returns whether the token names a supported top-level command.
fn isCommand(cmd: []const u8) bool {
    return std.mem.eql(u8, cmd, "open") or
        std.mem.eql(u8, cmd, "put") or
        std.mem.eql(u8, cmd, "get") or
        std.mem.eql(u8, cmd, "delete") or
        std.mem.eql(u8, cmd, "scan") or
        std.mem.eql(u8, cmd, "stats") or
        std.mem.eql(u8, cmd, "doctor") or
        std.mem.eql(u8, cmd, "property");
}

// Returns whether the option expects a following value token.
fn isValueFlag(flag: []const u8) bool {
    return std.mem.eql(u8, flag, "--path") or
        std.mem.eql(u8, flag, "--key") or
        std.mem.eql(u8, flag, "--value") or
        std.mem.eql(u8, flag, "--prefix") or
        std.mem.eql(u8, flag, "--start") or
        std.mem.eql(u8, flag, "--end") or
        std.mem.eql(u8, flag, "--name");
}

fn processHasFlag(flag: []const u8) bool {
    const allocator = std.heap.page_allocator;
    const args = std.process.argsAlloc(allocator) catch return false;
    defer std.process.argsFree(allocator, args);
    return hasFlag(args, flag);
}

// Maps internal errors onto stable user-facing CLI messages.
fn errorMessage(err: anyerror) []const u8 {
    return switch (err) {
        CliError.InvalidArguments => "invalid arguments",
        ziggy.engine.EngineError.KeyNotFound => "key not found",
        ziggy.engine.EngineError.LockHeld => "database is locked by another writer",
        ziggy.engine.EngineError.CorruptedWalRecord => "wal corruption detected",
        ziggy.engine.EngineError.UnknownProperty => "unknown property",
        ziggy.engine.EngineError.WriteStall => "write stalled",
        ziggy.engine.EngineError.UnsupportedFormat => "unsupported on-disk format version",
        else => @errorName(err),
    };
}

// Emits a JSON-safe string literal without depending on a full serializer.
fn writeJsonString(w: anytype, value: []const u8) !void {
    try w.writeByte('"');
    for (value) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try w.print("\\u00{x:0>2}", .{c});
                } else {
                    try w.writeByte(c);
                }
            },
        }
    }
    try w.writeByte('"');
}

// Prints the supported CLI commands and their required flags.
fn printUsage(w: *std.Io.Writer) !void {
    try w.print(
        "usage:\n" ++
            "  ziggy open --path <db_dir> [--json]\n" ++
            "  ziggy put --path <db_dir> --key <k> --value <v> [--json]\n" ++
            "  ziggy get --path <db_dir> --key <k> [--json]\n" ++
            "  ziggy delete --path <db_dir> --key <k> [--json]\n" ++
            "  ziggy scan --path <db_dir> [--prefix <p> | --start <k> [--end <k>]] [--json]\n" ++
            "  ziggy stats --path <db_dir> [--json]\n" ++
            "  ziggy property --path <db_dir> --name <prop> [--json]\n" ++
            "  ziggy doctor --path <db_dir> [--json]\n",
        .{},
    );
}

test "task 7.3 negative: malformed flags and missing values are rejected" {
    try std.testing.expectError(CliError.InvalidArguments, validateArgs(&.{ "ziggy", "put", "--path", "/tmp/db", "--key", "k", "--oops" }));
    try std.testing.expectError(CliError.InvalidArguments, validateArgs(&.{ "ziggy", "put", "--path", "/tmp/db", "--key" }));
    try std.testing.expectError(CliError.InvalidArguments, validateArgs(&.{ "ziggy", "get", "--path" }));
    try std.testing.expectError(CliError.InvalidArguments, validateArgs(&.{ "ziggy", "scan", "--path", "/tmp/db", "--prefix", "a", "--start", "b" }));
    try std.testing.expectError(CliError.InvalidArguments, validateArgs(&.{ "ziggy", "scan", "--path", "/tmp/db", "--start", "z", "--end", "a" }));
    try std.testing.expectError(CliError.InvalidArguments, validateArgs(&.{ "ziggy", "scan", "--path", "/tmp/db", "--end", "user:200" }));
}

test "task 7.3 positive: valid command shape passes argument validation" {
    try validateArgs(&.{ "ziggy", "put", "--path", "/tmp/db", "--key", "a", "--value", "b", "--json" });
    try validateArgs(&.{ "ziggy", "scan", "--path", "/tmp/db", "--prefix", "user:" });
    try validateArgs(&.{ "ziggy", "scan", "--path", "/tmp/db", "--start", "user:100" });
    try validateArgs(&.{ "ziggy", "scan", "--path", "/tmp/db", "--start", "user:100", "--end", "user:200" });
}

test "task 7.3 negative: invalid path surfaces explicit filesystem error" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var block = try tmp.dir.createFile("not-a-dir", .{ .truncate = true });
    block.close();

    const file_path = try tmp.dir.realpathAlloc(allocator, "not-a-dir");
    defer allocator.free(file_path);

    const bad_path = try std.fmt.allocPrint(allocator, "{s}/child", .{file_path});
    defer allocator.free(bad_path);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    var writer = out.writer(allocator);

    try std.testing.expectError(error.NotDir, runWithArgs(allocator, &.{ "ziggy", "open", "--path", bad_path }, &writer));
}

test "task 12.3 positive: scan command supports --start-only" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try ziggy.engine.Engine.open(allocator, .{ .path = db_path });
        defer eng.close() catch {};
        try eng.put("a", "1");
        try eng.put("b", "2");
        try eng.put("c", "3");
    }

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    var writer = out.writer(allocator);

    const args = [_][]const u8{
        "ziggy",
        "scan",
        "--path",
        db_path,
        "--start",
        "b",
        "--json",
    };

    try runWithArgs(allocator, &args, &writer);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"key\":\"a\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"key\":\"b\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"key\":\"c\"") != null);
}

test "task 12.3 positive: scan command supports --start plus --end" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try ziggy.engine.Engine.open(allocator, .{ .path = db_path });
        defer eng.close() catch {};
        try eng.put("a", "1");
        try eng.put("b", "2");
        try eng.put("c", "3");
    }

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    var writer = out.writer(allocator);

    const args = [_][]const u8{
        "ziggy",
        "scan",
        "--path",
        db_path,
        "--start",
        "b",
        "--end",
        "d",
        "--json",
    };

    try runWithArgs(allocator, &args, &writer);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"key\":\"b\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"key\":\"c\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"key\":\"a\"") == null);
}

test "task 12.3 negative: scan command rejects --end without --start" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    var writer = out.writer(allocator);

    try std.testing.expectError(
        CliError.InvalidArguments,
        runWithArgs(allocator, &.{ "ziggy", "scan", "--path", db_path, "--end", "d", "--json" }, &writer),
    );
}

test "task 8.2 integration: CLI get/scan/prefix align with engine read visibility" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try ziggy.engine.Engine.open(allocator, .{ .path = db_path, .memtable_max_bytes = 16, .compaction_trigger_l0_files = 100 });
        defer eng.close() catch {};

        try eng.put("user:001", "v1");
        try eng.put("user:002", "v2");
        try eng.put("user:003", "v3");
        try eng.delete("user:002");
        try eng.put("user:001", "v1-new");
    }

    var get_out = std.ArrayList(u8).empty;
    defer get_out.deinit(allocator);
    var get_writer = get_out.writer(allocator);
    try runWithArgs(allocator, &.{ "ziggy", "get", "--path", db_path, "--key", "user:001", "--json" }, &get_writer);

    var scan_out = std.ArrayList(u8).empty;
    defer scan_out.deinit(allocator);
    var scan_writer = scan_out.writer(allocator);
    try runWithArgs(allocator, &.{ "ziggy", "scan", "--path", db_path, "--start", "user:", "--end", "user;", "--json" }, &scan_writer);

    var prefix_out = std.ArrayList(u8).empty;
    defer prefix_out.deinit(allocator);
    var prefix_writer = prefix_out.writer(allocator);
    try runWithArgs(allocator, &.{ "ziggy", "scan", "--path", db_path, "--prefix", "user:", "--json" }, &prefix_writer);

    try std.testing.expect(std.mem.indexOf(u8, get_out.items, "\"value\":\"v1-new\"") != null);

    try std.testing.expect(std.mem.indexOf(u8, scan_out.items, "\"key\":\"user:001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan_out.items, "\"value\":\"v1-new\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan_out.items, "\"key\":\"user:003\"") != null);

    try std.testing.expect(std.mem.eql(u8, scan_out.items, prefix_out.items));
}

test "task 7.3 regression: json escaping covers quotes control chars and slashes" {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(std.testing.allocator);

    const raw = "line\n\"quoted\"\\tab\x01";
    var writer = out.writer(std.testing.allocator);
    try writeJsonString(&writer, raw);

    try std.testing.expectEqualStrings("\"line\\n\\\"quoted\\\"\\\\tab\\u0001\"", out.items);
}

test "task 10.2 positive: property command returns typed json payload" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    var writer = out.writer(allocator);

    const args = [_][]const u8{
        "ziggy",
        "property",
        "--path",
        db_path,
        "--name",
        "engine.stall.state",
        "--json",
    };

    try runWithArgs(allocator, &args, &writer);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"name\":\"engine.stall.state\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"type\":\"text\"") != null);
}

test "task 10.2 positive: latency percentile property prints typed json" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(db_path);

    {
        var eng = try ziggy.engine.Engine.open(allocator, .{ .path = db_path });
        defer eng.close() catch {};
        try eng.put("k", "v");
    }

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    var writer = out.writer(allocator);

    const args = [_][]const u8{
        "ziggy",
        "property",
        "--path",
        db_path,
        "--name",
        "engine.metrics.latency.put.p99_us",
        "--json",
    };

    try runWithArgs(allocator, &args, &writer);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "\"type\":\"u64\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "engine.metrics.latency.put.p99_us") != null);
}

test "task 10.2 negative: property command requires --name" {
    try std.testing.expectError(CliError.InvalidArguments, validateArgs(&.{ "ziggy", "property", "--path", "/tmp/db" }));
}
