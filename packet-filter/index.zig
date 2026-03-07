//! Small experiment that queries the IPv4 address of a named network interface via ioctl.
const std = @import("std");
const libc = @cImport({
    @cInclude("sys/ioctl.h");
    @cInclude("net/if.h");
    @cInclude("netinet/in.h");
});

// Opens a socket, fills ifreq, and prints the address assigned to en0.
pub fn main() !void {
    var ifr: libc.ifreq = undefined;
    const interface_name = "en0";

    // Copy the interface name into ifr.ifr_name
    std.mem.copyForwards(u8, &ifr.ifr_name, interface_name);
    ifr.ifr_name[interface_name.len] = 0; // Null-terminate

    const sock = libc.socket(libc.AF_INET, libc.SOCK_STREAM, 0);
    if (sock < 0) {
        std.debug.print("socket error\n", .{});
        return;
    }

    const res = libc.ioctl(sock, libc.SIOCGIFADDR, &ifr);
    if (res < 0) {
        std.debug.print("failed to get interface\n", .{});
        return;
    }

    const addr_ptr = &ifr.ifr_ifru.ifru_addr;
    const sockaddr: *libc.sockaddr_in = @ptrCast(@alignCast(addr_ptr));
    const ip_bytes = sockaddr.sin_addr.s_addr;
    std.debug.print("IP Address: {d}.{d}.{d}.{d}", .{ ip_bytes >> 24, (ip_bytes >> 16) & 0xFF, (ip_bytes >> 8) & 0xFF, ip_bytes & 0xFF });
}
