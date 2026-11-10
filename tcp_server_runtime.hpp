// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#pragma once

#include "tcp_server_fd.hpp"

#include <atomic>
#include <cstdint>

namespace tcp_server_runtime {

// Process-wide state for the running, draining, and stopping server phases.
enum class ShutdownState : std::uint8_t { Running, Draining, Stopping };

extern std::atomic<ShutdownState> g_shutdown_state;
extern std::atomic<std::int64_t> g_drain_deadline_ns;

// Returns monotonic time in nanoseconds for shutdown deadlines.
[[nodiscard]] std::int64_t steady_clock_ns() noexcept;

// Creates a nonblocking eventfd used for worker wakeups and completion signals.
UniqueFd create_event_handle();

// Acquires the per-port process lock held for the server lifetime.
UniqueFd acquire_port_instance_lock(int listen_port);

} // namespace tcp_server_runtime
