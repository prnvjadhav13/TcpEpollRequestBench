// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#include "tcp_server_runtime.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>

#include <sys/file.h>
#include <sys/eventfd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tcp_server_runtime {

std::atomic<ShutdownState> g_shutdown_state{ShutdownState::Running};
std::atomic<std::int64_t> g_drain_deadline_ns{0};

std::int64_t steady_clock_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

UniqueFd create_event_handle() {
    const int event_fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (event_fd < 0) {
        throw std::runtime_error(std::string("eventfd failed: ") + std::strerror(errno));
    }
    return UniqueFd(event_fd);
}

UniqueFd acquire_port_instance_lock(int listen_port) {
    const std::string lock_path =
        "/tmp/tcp_server_epoll." + std::to_string(static_cast<unsigned long>(::getuid())) +
        "." + std::to_string(listen_port) + ".lock";
    UniqueFd lock_fd(::open(lock_path.c_str(),
                            O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW,
                            S_IRUSR | S_IWUSR));
    if (!lock_fd) {
        throw std::runtime_error("failed to open per-port instance lock " + lock_path +
                                 ": " + std::strerror(errno));
    }
    if (::flock(lock_fd.fd(), LOCK_EX | LOCK_NB) < 0) {
        const int lock_errno = errno;
        if (lock_errno == EWOULDBLOCK || lock_errno == EAGAIN) {
            throw std::runtime_error(
                "another tcp_server_epoll instance is already using port " +
                std::to_string(listen_port));
        }
        throw std::runtime_error("failed to lock port " + std::to_string(listen_port) +
                                 ": " + std::strerror(lock_errno));
    }
    return lock_fd;
}

} // namespace tcp_server_runtime
