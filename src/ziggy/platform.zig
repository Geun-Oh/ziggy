//! Compile-time platform capability model used by engine policy and diagnostics.
const std = @import("std");
const builtin = @import("builtin");

pub const SupportTier = enum {
    tier1,
    tier2,
    experimental,
    unsupported,
};

pub const Capabilities = struct {
    os_tag: std.Target.Os.Tag,
    arch: std.Target.Cpu.Arch,
    pointer_bits: u16,
    tier: SupportTier,
    supported: bool,
    has_flock: bool,
    has_linux_renameat2_noreplace: bool,
    has_direct_io_path: bool,
    has_posix_nofollow_open: bool,
};

pub fn capabilities() Capabilities {
    const os = builtin.os.tag;
    const arch = builtin.cpu.arch;
    const bits: u16 = @intCast(@bitSizeOf(usize));

    const posix_like = switch (os) {
        .linux, .macos, .freebsd => true,
        else => false,
    };

    const tier: SupportTier = switch (os) {
        .linux, .macos => switch (arch) {
            .x86_64, .aarch64 => .tier1,
            else => .experimental,
        },
        .freebsd => switch (arch) {
            .x86_64, .aarch64 => .tier2,
            else => .experimental,
        },
        else => .unsupported,
    };

    const has_direct_io = switch (os) {
        .linux => true, // O_DIRECT path
        .macos => true, // fcntl(F_NOCACHE) fallback path
        else => @hasField(std.posix.O, "DIRECT"),
    };

    const supported = posix_like and bits >= 64 and tier != .unsupported;

    return .{
        .os_tag = os,
        .arch = arch,
        .pointer_bits = bits,
        .tier = tier,
        .supported = supported,
        .has_flock = posix_like,
        .has_linux_renameat2_noreplace = os == .linux,
        .has_direct_io_path = has_direct_io,
        .has_posix_nofollow_open = posix_like,
    };
}

test "platform capabilities are internally consistent" {
    const caps = capabilities();
    if (!caps.supported) {
        try std.testing.expect(caps.tier == .unsupported or caps.tier == .experimental);
    }

    if (caps.has_linux_renameat2_noreplace) {
        try std.testing.expectEqual(std.Target.Os.Tag.linux, caps.os_tag);
    }
}
