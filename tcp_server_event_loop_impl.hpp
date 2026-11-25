// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#pragma once

#include "tcp_server_connection.hpp"
#include "tcp_server_event_loop.hpp"
#include "tcp_server_fd.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stop_token>
#include <string>

#include <sys/epoll.h>

// Private Linux-specific implementation details for EventLoop.
class EventLoop::Impl {
public:
    Impl(std::size_t loop_id,
         int listen_port,
         std::chrono::seconds idle_timeout,
         std::size_t max_connections);
    ~Impl() noexcept;
    void run(std::stop_token stop_token);
    void wake() noexcept;
    [[nodiscard]] EventLoopStats stats() const noexcept;

private:
    static constexpr std::size_t kTimingWheelSlots = 256;

    void register_fd(int fd, std::uint32_t events);
    bool try_register_connection_fd(int fd, std::uint32_t events) noexcept;
    bool try_modify_fd(int fd, std::uint32_t events) noexcept;
    void remove_fd(int fd) noexcept;
    void drain_wake_fd() noexcept;
    void pause_accepting(std::chrono::milliseconds duration) noexcept;
    void maybe_resume_accepting();
    void stop_accepting() noexcept;
    static UniqueFd open_reserve_fd();
    void recover_from_emfile() noexcept;
    static std::uint64_t current_tick() noexcept;
    void unschedule_timeout(ConnectionState& connection) noexcept;
    void schedule_timeout(ConnectionState& connection) noexcept;
    void touch(ConnectionState& connection) noexcept;
    void handle_event(const epoll_event& event);
    bool update_connection_interest(ConnectionState& connection) noexcept;
    void accept_ready_connections();
    bool handle_read(ConnectionState& connection);
    bool handle_write(ConnectionState& connection);
    void expire_idle_connections();
    void close_connection(int fd) noexcept;
    void shutdown_all_connections() noexcept;

    std::size_t loop_id_;
    std::string name_;
    std::chrono::seconds idle_timeout_;
    std::size_t max_connections_;
    EventLoopStats stats_;
    UniqueFd epoll_fd_;
    UniqueFd listen_socket_;
    UniqueFd wake_fd_;
    UniqueFd reserve_fd_;
    bool listener_registered_ = false;
    std::chrono::steady_clock::time_point accept_resume_time_{};
    ConnectionTable connections_;
    std::array<int, kTimingWheelSlots> timing_wheel_{};
    std::uint64_t last_expired_tick_ = 0;
};
