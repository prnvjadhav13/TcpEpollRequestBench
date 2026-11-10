// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#include "tcp_server_event_loop.hpp"
#include "tcp_server_fd.hpp"
#include "tcp_server_connection.hpp"
#include "tcp_server_mapping.hpp"
#include "tcp_server_runtime.hpp"
#include "tcp_common_config.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <unistd.h>

namespace {
constexpr std::size_t kMaxRequestSize = 1024;
constexpr std::size_t kAcceptBatchSize = 256;
constexpr std::size_t kMaxEpollEvents = 4096;
constexpr std::size_t kMaxReadBytesPerEvent = 64 * 1024;
constexpr std::size_t kMaxWriteBytesPerEvent = 64 * 1024;
constexpr std::size_t kTimingWheelSlots = 256;
constexpr int kClientReceiveBufferBytes = 4096;
constexpr auto kEpollWaitTimeout = std::chrono::milliseconds(250);
constexpr auto kAcceptResourceBackoff = std::chrono::milliseconds(100);
constexpr auto kAcceptCapacityBackoff = std::chrono::milliseconds(10);
constexpr auto kRequestCompletionTimeout = std::chrono::seconds(10);
constexpr auto kResponseDrainTimeout = std::chrono::seconds(30);

bool is_resource_exhaustion_error(int error_code) {
    switch (error_code) {
    case EMFILE:
    case ENFILE:
    case ENOMEM:
    case ENOBUFS:
        return true;
    default:
        return false;
    }
}
} // namespace

using namespace tcp_server_runtime;

// Creates the close-on-exec epoll descriptor used by one event-loop worker.
UniqueFd create_epoll_handle() {
    const int epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        throw std::runtime_error(std::string("epoll_create1 failed: ") + std::strerror(errno));
    }
    return UniqueFd(epoll_fd);
}

// Creates, configures, binds, and listens on a nonblocking IPv4 socket for
// listen_port; SO_REUSEPORT permits one listener per event-loop worker.
UniqueFd create_reuseport_listener(int listen_port) {
    const int listen_fd =
        ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (listen_fd < 0) {
        throw std::runtime_error(std::string("socket failed: ") + std::strerror(errno));
    }

    UniqueFd listen_socket(listen_fd);

    int enable = 1;
    if (::setsockopt(listen_socket.fd(), SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0) {
        throw std::runtime_error(std::string("setsockopt(SO_REUSEADDR) failed: ") +
                                 std::strerror(errno));
    }
    if (::setsockopt(listen_socket.fd(), SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable)) < 0) {
        throw std::runtime_error(std::string("setsockopt(SO_REUSEPORT) failed: ") +
                                 std::strerror(errno));
    }
    if (::setsockopt(listen_socket.fd(), SOL_SOCKET, SO_KEEPALIVE, &enable, sizeof(enable)) < 0) {
        throw std::runtime_error(std::string("setsockopt(SO_KEEPALIVE) failed: ") +
                                 std::strerror(errno));
    }
    if (::setsockopt(listen_socket.fd(), IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable)) < 0) {
        throw std::runtime_error(std::string("setsockopt(TCP_NODELAY) failed: ") +
                                 std::strerror(errno));
    }

    sockaddr_in listen_addr{};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(static_cast<std::uint16_t>(listen_port));
    listen_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(listen_socket.fd(),
               reinterpret_cast<sockaddr*>(&listen_addr),
               sizeof(listen_addr)) < 0) {
        throw std::runtime_error(std::string("bind failed: ") + std::strerror(errno));
    }

    if (::listen(listen_socket.fd(), SOMAXCONN) < 0) {
        throw std::runtime_error(std::string("listen failed: ") + std::strerror(errno));
    }

    return listen_socket;
}

// Applies memory-conscious options to an accepted client socket. Linux doubles
// SO_RCVBUF internally for bookkeeping, but the requested value still bounds
// the advertised receive window far below the distribution default.
void configure_client_socket(int fd) {
    const int enable = 1;
    const int receive_buffer = kClientReceiveBufferBytes;
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVBUF,
                     &receive_buffer, sizeof(receive_buffer)) < 0) {
        throw std::runtime_error(std::string("setsockopt(SO_RCVBUF) failed: ") +
                                 std::strerror(errno));
    }
    if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enable, sizeof(enable)) < 0) {
        throw std::runtime_error(std::string("setsockopt(SO_KEEPALIVE) failed: ") +
                                 std::strerror(errno));
    }
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable)) < 0) {
        throw std::runtime_error(std::string("setsockopt(TCP_NODELAY) failed: ") +
                                 std::strerror(errno));
    }
}

// Linux-specific implementation of one isolated epoll worker, including its
// listener, wake channel, connection table, and idle-timeout wheel.
class EventLoop::Impl {
public:
    // Builds worker loop_id with a reuse-port listener on listen_port, the
    // requested idle_timeout, and a hard max_connections capacity.
    Impl(std::size_t loop_id,
         int listen_port,
         std::chrono::seconds idle_timeout,
         std::size_t max_connections)
        : loop_id_(loop_id),
          name_("EpollLoop-" + std::to_string(loop_id)),
          idle_timeout_(idle_timeout),
          max_connections_(max_connections),
          epoll_fd_(create_epoll_handle()),
          listen_socket_(create_reuseport_listener(listen_port)),
          wake_fd_(create_event_handle()),
          reserve_fd_(open_reserve_fd()) {
        connections_.reserve(max_connections_);
        timing_wheel_.fill(-1);
        last_expired_tick_ = current_tick();
        register_fd(listen_socket_.fd(), EPOLLIN);
        listener_registered_ = true;
        register_fd(wake_fd_.fd(), EPOLLIN);
    }

    ~Impl() noexcept {
        shutdown_all_connections();
        if (listen_socket_.fd() >= 0) {
            remove_fd(listen_socket_.fd());
        }
        remove_fd(wake_fd_.fd());
    }

    // Dispatches epoll events until stop_token or global shutdown completes.
    void run(std::stop_token stop_token) {
        std::vector<epoll_event> events(kMaxEpollEvents);

        while (true) {
            if (stop_token.stop_requested()) {
                g_shutdown_state.store(ShutdownState::Stopping,
                                       std::memory_order_release);
            }
            const ShutdownState shutdown = g_shutdown_state.load(std::memory_order_acquire);
            if (shutdown != ShutdownState::Running) {
                stop_accepting();
                if (connections_.size() == 0 || shutdown == ShutdownState::Stopping ||
                    steady_clock_ns() >= g_drain_deadline_ns.load(std::memory_order_acquire)) {
                    shutdown_all_connections();
                    return;
                }
            } else {
                maybe_resume_accepting();
            }
            const int ready = ::epoll_wait(
                epoll_fd_.fd(),
                events.data(),
                static_cast<int>(events.size()),
                static_cast<int>(kEpollWaitTimeout.count()));

            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::runtime_error(std::string("epoll_wait failed: ") + std::strerror(errno));
            }

            for (int i = 0; i < ready; ++i) {
                handle_event(events[static_cast<std::size_t>(i)]);
            }

            expire_idle_connections();
        }
    }

    // Signals the worker's eventfd so a blocked epoll_wait returns promptly.
    void wake() noexcept {
        const std::uint64_t one = 1;
        for (;;) {
            const ssize_t written = ::write(wake_fd_.fd(), &one, sizeof(one));
            if (written == static_cast<ssize_t>(sizeof(one))) {
                return;
            }
            if (written < 0 && errno == EINTR) {
                continue;
            }
            // EAGAIN means the eventfd counter is already nonzero, so the loop
            // is guaranteed to wake. Other failures can only be logged safely
            // by the coordinator; wake itself is intentionally noexcept.
            return;
        }
    }

    [[nodiscard]] EventLoopStats stats() const noexcept {
        return stats_;
    }

private:
    // Adds fd to this worker's epoll set with the requested event mask.
    void register_fd(int fd, std::uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd_.fd(), EPOLL_CTL_ADD, fd, &ev) < 0) {
            throw std::runtime_error(std::string("epoll_ctl(ADD) failed: ") + std::strerror(errno));
        }
    }

    // Adds client fd with events, logging and returning false on failure.
    bool try_register_connection_fd(int fd, std::uint32_t events) noexcept {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd_.fd(), EPOLL_CTL_ADD, fd, &ev) == 0) {
            return true;
        }
        const int error = errno;
        std::cerr << name_ << " epoll_ctl(ADD) failed for fd " << fd
                  << ": " << std::strerror(error) << std::endl;
        return false;
    }

    // Replaces fd's epoll interest mask with events, returning success status.
    bool try_modify_fd(int fd, std::uint32_t events) noexcept {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd_.fd(), EPOLL_CTL_MOD, fd, &ev) == 0) {
            return true;
        }
        const int error = errno;
        std::cerr << name_ << " epoll_ctl(MOD) failed for fd " << fd
                  << ": " << std::strerror(error) << std::endl;
        return false;
    }

    // Best-effort removal of fd from this worker's epoll set.
    void remove_fd(int fd) noexcept {
        ::epoll_ctl(epoll_fd_.fd(), EPOLL_CTL_DEL, fd, nullptr);
    }

    // Consumes accumulated eventfd notifications after a wake event.
    void drain_wake_fd() noexcept {
        std::uint64_t value = 0;
        for (;;) {
            const ssize_t result = ::read(wake_fd_.fd(), &value, sizeof(value));
            if (result == static_cast<ssize_t>(sizeof(value))) {
                continue;
            }
            if (result < 0 && errno == EINTR) {
                continue;
            }
            return;
        }
    }

    // Temporarily removes the listener from epoll for duration after capacity
    // or resource pressure.
    void pause_accepting(std::chrono::milliseconds duration) noexcept {
        if (listen_socket_.fd() < 0) {
            return;
        }
        if (listener_registered_) {
            remove_fd(listen_socket_.fd());
            listener_registered_ = false;
        }
        const auto requested_deadline = std::chrono::steady_clock::now() + duration;
        accept_resume_time_ = std::max(accept_resume_time_, requested_deadline);
    }

    // Re-registers the listener after its backoff deadline, if still running.
    void maybe_resume_accepting() {
        if (listen_socket_.fd() < 0 || listener_registered_ ||
            std::chrono::steady_clock::now() < accept_resume_time_) {
            return;
        }
        register_fd(listen_socket_.fd(), EPOLLIN);
        listener_registered_ = true;
    }

    // Permanently closes this worker's listener during shutdown.
    void stop_accepting() noexcept {
        if (listen_socket_.fd() < 0) {
            return;
        }
        if (listener_registered_) {
            remove_fd(listen_socket_.fd());
            listener_registered_ = false;
        }
        listen_socket_.reset();
    }

    // Opens the emergency descriptor reserved for recovery from EMFILE.
    static UniqueFd open_reserve_fd() {
        const int fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            throw std::runtime_error(std::string("open(/dev/null) failed: ") + std::strerror(errno));
        }
        return UniqueFd(fd);
    }

    // Frees the reserve descriptor, accepts and rejects one queued client, then
    // restores the reserve so future descriptor exhaustion remains recoverable.
    void recover_from_emfile() noexcept {
        reserve_fd_.reset();
        sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);
        UniqueFd rejected_client(::accept4(listen_socket_.fd(),
                                           reinterpret_cast<sockaddr*>(&client_addr),
                                           &client_addr_len,
                                           SOCK_NONBLOCK | SOCK_CLOEXEC));
        if (rejected_client) {
            ++stats_.rejected;
        }
        try {
            reserve_fd_ = open_reserve_fd();
        } catch (const std::exception& ex) {
            std::cerr << name_ << " cannot restore emergency file descriptor: " << ex.what()
                      << std::endl;
        }
    }

    static std::uint64_t current_tick() noexcept {
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }

    // Removes a connection from its intrusive timing-wheel list in O(1).
    void unschedule_timeout(ConnectionState& connection) noexcept {
        if (!connection.wheel_scheduled) {
            return;
        }
        if (connection.wheel_previous >= 0) {
            ConnectionState* previous = connections_.find(connection.wheel_previous);
            if (previous != nullptr) {
                previous->wheel_next = connection.wheel_next;
            }
        } else {
            timing_wheel_[connection.wheel_slot] = connection.wheel_next;
        }
        if (connection.wheel_next >= 0) {
            ConnectionState* next = connections_.find(connection.wheel_next);
            if (next != nullptr) {
                next->wheel_previous = connection.wheel_previous;
            }
        }
        connection.wheel_previous = -1;
        connection.wheel_next = -1;
        connection.wheel_scheduled = false;
    }

    // Tracks the earliest inactivity or absolute protocol-progress deadline.
    void schedule_timeout(ConnectionState& connection) noexcept {
        unschedule_timeout(connection);
        connection.expiration_tick = connection.idle_deadline_tick;
        if (connection.request_deadline_tick != 0) {
            connection.expiration_tick = std::min(connection.expiration_tick,
                                                  connection.request_deadline_tick);
        }
        if (connection.response_deadline_tick != 0) {
            connection.expiration_tick = std::min(connection.expiration_tick,
                                                  connection.response_deadline_tick);
        }
        connection.wheel_slot = static_cast<std::uint8_t>(
            connection.expiration_tick % kTimingWheelSlots);
        connection.wheel_next = timing_wheel_[connection.wheel_slot];
        if (connection.wheel_next >= 0) {
            ConnectionState* next = connections_.find(connection.wheel_next);
            if (next != nullptr) {
                next->wheel_previous = connection.fd();
            }
        }
        timing_wheel_[connection.wheel_slot] = connection.fd();
        connection.wheel_scheduled = true;
    }

    void touch(ConnectionState& connection) noexcept {
        connection.idle_deadline_tick = current_tick() +
            static_cast<std::uint64_t>(idle_timeout_.count()) + 1U;
        schedule_timeout(connection);
    }

    // Routes one epoll event to wake, accept, read, write, error, or hangup logic.
    void handle_event(const epoll_event& event) {
        if (event.data.fd == wake_fd_.fd()) {
            drain_wake_fd();
            return;
        }

        if (event.data.fd == listen_socket_.fd()) {
            accept_ready_connections();
            return;
        }

        ConnectionState* connection = connections_.find(event.data.fd);
        if (connection == nullptr) {
            return;
        }

        if ((event.events & EPOLLERR) != 0U) {
            int socket_error = 0;
            socklen_t length = sizeof(socket_error);
            if (::getsockopt(connection->fd(), SOL_SOCKET, SO_ERROR,
                             &socket_error, &length) == 0 && socket_error != 0) {
                std::cerr << name_ << " socket error on fd " << connection->fd()
                          << ": " << std::strerror(socket_error) << std::endl;
            }
            close_connection(connection->fd());
            return;
        }

        const bool full_hangup = (event.events & EPOLLHUP) != 0U;
        if ((event.events & EPOLLRDHUP) != 0U) {
            connection->peer_write_closed = true;
        }
        bool keep_open = true;
        if ((event.events & EPOLLIN) != 0U) {
            keep_open = handle_read(*connection);
            connection = connections_.find(event.data.fd);
        }

        if (!keep_open || connection == nullptr) {
            close_connection(event.data.fd);
            return;
        }

        if (connection->has_responses() &&
            (((event.events & EPOLLOUT) != 0U) || (event.events & EPOLLIN) != 0U)) {
            keep_open = handle_write(*connection);
        }

        connection = connections_.find(event.data.fd);
        if (!keep_open || connection == nullptr) {
            close_connection(event.data.fd);
            return;
        }

        if (full_hangup || (connection->peer_write_closed && !connection->has_responses())) {
            close_connection(event.data.fd);
            return;
        }

        if (!update_connection_interest(*connection)) {
            close_connection(event.data.fd);
        }
    }

    // Uses level-triggered readiness so bounded per-event work remains fair and
    // any unread/unwritten data is reported again without relying on ET sizing.
    bool update_connection_interest(ConnectionState& connection) noexcept {
        std::uint32_t events = EPOLLRDHUP | EPOLLERR;
        if (!connection.peer_write_closed) {
            events |= EPOLLIN;
        }
        if (connection.has_responses()) {
            events |= EPOLLOUT;
        }
        if (events == connection.registered_events) {
            return true;
        }
        if (!try_modify_fd(connection.fd(), events)) {
            return false;
        }
        connection.registered_events = events;
        return true;
    }

    // Accepts a bounded batch from the ready listener, registers each client,
    // and applies backpressure at resource or connection limits.
    void accept_ready_connections() {
        if (connections_.size() >= max_connections_) {
            pause_accepting(kAcceptCapacityBackoff);
            return;
        }

        for (std::size_t accepted = 0; accepted < kAcceptBatchSize; ++accepted) {
            sockaddr_in client_addr{};
            socklen_t client_addr_len = sizeof(client_addr);
            UniqueFd client_socket(::accept4(
                listen_socket_.fd(),
                reinterpret_cast<sockaddr*>(&client_addr),
                &client_addr_len,
                SOCK_NONBLOCK | SOCK_CLOEXEC));

            if (!client_socket) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return;
                }
                if (is_resource_exhaustion_error(errno)) {
                    std::cerr << name_ << " accept resource exhaustion: " << std::strerror(errno)
                              << std::endl;
                    if (errno == EMFILE) {
                        recover_from_emfile();
                    }
                    pause_accepting(kAcceptResourceBackoff);
                    return;
                }
                throw std::runtime_error(std::string("accept4 failed: ") + std::strerror(errno));
            }

            if (connections_.size() >= max_connections_) {
                ++stats_.rejected;
                pause_accepting(kAcceptCapacityBackoff);
                return;
            }

            const int client_fd = client_socket.get();
            bool connection_created = false;
            try {
                configure_client_socket(client_fd);
                ConnectionState& connection =
                    connections_.emplace(std::move(client_socket));
                connection_created = true;

                // Register the socket before publishing its idle deadline.  If epoll
                // registration fails, the whole connection transaction is rolled back.
                constexpr std::uint32_t initial_events =
                    EPOLLIN | EPOLLRDHUP | EPOLLERR;
                if (!try_register_connection_fd(client_fd, initial_events)) {
                    connections_.erase(client_fd);
                    continue;
                }
                connection.registered_events = initial_events;

                touch(connection);
                ++stats_.accepted;
            } catch (const std::bad_alloc&) {
                if (connection_created) {
                    remove_fd(client_fd);
                    connections_.erase(client_fd);
                }
                std::cerr << name_ << " unable to allocate connection state for fd "
                          << client_fd << std::endl;
                continue;
            } catch (const std::exception& ex) {
                if (connection_created) {
                    remove_fd(client_fd);
                    connections_.erase(client_fd);
                }
                std::cerr << name_ << " failed to initialize fd " << client_fd
                          << ": " << ex.what() << std::endl;
                continue;
            } catch (...) {
                if (connection_created) {
                    remove_fd(client_fd);
                    connections_.erase(client_fd);
                }
                std::cerr << name_ << " failed to initialize fd " << client_fd
                          << ": unknown exception" << std::endl;
                continue;
            }
        }
    }

    // Reads bounded newline-delimited requests. Multiple lines may be queued on
    // one keep-alive connection; the fixed response ring bounds memory usage.
    bool handle_read(ConnectionState& connection) {
        std::array<char, 512> buffer{};
        std::size_t bytes_read = 0;

        while (bytes_read < kMaxReadBytesPerEvent) {
            const std::size_t read_size = std::min(buffer.size(), kMaxReadBytesPerEvent - bytes_read);
            const ssize_t n = ::recv(connection.fd(), buffer.data(), read_size, 0);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return true;
                }
                return false;
            }

            if (n == 0) {
                connection.peer_write_closed = true;
                return true;
            }

            const char* chunk = buffer.data();
            const std::size_t chunk_size = static_cast<std::size_t>(n);
            bytes_read += chunk_size;
            std::size_t consumed = 0;
            while (consumed < chunk_size) {
                const void* newline_ptr =
                    std::memchr(chunk + consumed, '\n', chunk_size - consumed);
                const std::size_t fragment_size = newline_ptr == nullptr
                    ? chunk_size - consumed
                    : static_cast<std::size_t>(static_cast<const char*>(newline_ptr) -
                                               (chunk + consumed));
                if (connection.request_line.size() + fragment_size > kMaxRequestSize) {
                    return false;
                }
                if (fragment_size != 0 && connection.request_deadline_tick == 0) {
                    connection.request_deadline_tick = current_tick() +
                        static_cast<std::uint64_t>(kRequestCompletionTimeout.count()) + 1U;
                }
                connection.request_line.append(chunk + consumed, fragment_size);
                consumed += fragment_size;
                if (newline_ptr == nullptr) {
                    break;
                }
                ++consumed;
                if (!connection.request_line.empty() &&
                    connection.request_line.back() == '\r') {
                    connection.request_line.pop_back();
                }
                if (connection.request_line.empty() || connection.response_queue_full()) {
                    return false;
                }
                const std::optional<ResponseBufferView> response =
                    find_client_response(connection.request_line);
                if (!response.has_value()) {
                    return false;
                }
                const bool starting_response = !connection.has_responses();
                if (!connection.push_response(*response)) {
                    return false;
                }
                if (starting_response) {
                    connection.response_deadline_tick = current_tick() +
                        static_cast<std::uint64_t>(kResponseDrainTimeout.count()) + 1U;
                }
                connection.request_line.clear();
                connection.request_deadline_tick = 0;
            }
            touch(connection);
        }

        return true;
    }

    // Sends queued responses in request order while bounding work per event.
    bool handle_write(ConnectionState& connection) {
        std::size_t bytes_written = 0;
        while (connection.has_responses() && bytes_written < kMaxWriteBytesPerEvent) {
            const ResponseBufferView& response = connection.current_response();
            const std::size_t budget = kMaxWriteBytesPerEvent - bytes_written;
            const std::size_t header_remaining =
                connection.response_header.size() - connection.response_header_sent;
            const std::size_t header_size = std::min(header_remaining, budget);
            const std::size_t payload_budget = budget - header_size;
            const std::size_t payload_remaining = response.size - connection.response_sent;
            const std::size_t payload_size = std::min(payload_remaining, payload_budget);

            // A single vectored write normally emits the framing header and payload
            // together. This preserves zero-copy views into the immutable mapping
            // while avoiding a syscall and a small TCP segment per response.
            std::array<iovec, 2> vectors{};
            int vector_count = 0;
            if (header_size != 0) {
                vectors[static_cast<std::size_t>(vector_count++)] = iovec{
                    .iov_base = connection.response_header.data() +
                                connection.response_header_sent,
                    .iov_len = header_size};
            }
            if (payload_size != 0) {
                vectors[static_cast<std::size_t>(vector_count++)] = iovec{
                    .iov_base = const_cast<std::uint8_t*>(response.data +
                                                         connection.response_sent),
                    .iov_len = payload_size};
            }

            msghdr message{};
            message.msg_iov = vectors.data();
            message.msg_iovlen = static_cast<std::size_t>(vector_count);
            const ssize_t n = ::sendmsg(connection.fd(),
                                        &message,
                                        MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return true;
                }
                return false;
            }
            if (n == 0) {
                return false;
            }

            const std::size_t sent = static_cast<std::size_t>(n);
            const std::size_t header_sent = std::min(sent, header_size);
            connection.response_header_sent += header_sent;
            connection.response_sent += sent - header_sent;
            bytes_written += sent;
            if (connection.response_header_sent == connection.response_header.size() &&
                connection.response_sent == response.size) {
                connection.pop_response();
                if (!connection.has_responses()) {
                    connection.response_deadline_tick = 0;
                }
            }
            // Progress refreshes inactivity only. The absolute response deadline
            // prevents a slow reader from retaining this connection indefinitely.
            if (!connection.peer_write_closed) {
                touch(connection);
            }
        }

        return true;
    }

    // Advances the one-second timing wheel and closes expired connections.
    void expire_idle_connections() {
        const std::uint64_t now = current_tick();
        while (last_expired_tick_ < now) {
            ++last_expired_tick_;
            const std::size_t slot = static_cast<std::size_t>(
                last_expired_tick_ % kTimingWheelSlots);
            int fd = timing_wheel_[slot];
            while (fd >= 0) {
                ConnectionState* connection = connections_.find(fd);
                if (connection == nullptr) {
                    break;
                }
                const int next_fd = connection->wheel_next;
                if (connection->expiration_tick <= now) {
                    close_connection(fd);
                }
                fd = next_fd;
            }
        }
    }

    // Removes fd from epoll and releases its state, updating close statistics.
    void close_connection(int fd) noexcept {
        ConnectionState* connection = connections_.find(fd);
        if (connection == nullptr) {
            return;
        }

        unschedule_timeout(*connection);
        remove_fd(fd);
        connections_.erase(fd);
        ++stats_.closed;
    }

    // Removes and releases every client owned by this worker during teardown.
    void shutdown_all_connections() noexcept {
        for (auto it = connections_.begin(); it != connections_.end();) {
            const int fd = it->first;
            remove_fd(fd);
            it = connections_.erase(it);
            ++stats_.closed;
        }
    }

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

// Constructs the public worker facade from its identifier, shared port,
// per-connection idle timeout, and per-worker connection limit.
EventLoop::EventLoop(std::size_t loop_id,
                     int listen_port,
                     std::chrono::seconds idle_timeout,
                     std::size_t max_connections)
    : impl_(std::make_unique<Impl>(loop_id, listen_port, idle_timeout, max_connections)) {}

EventLoop::~EventLoop() noexcept = default;

// Delegates event processing to the implementation until stop_token is set.
void EventLoop::run(std::stop_token stop_token) {
    impl_->run(stop_token);
}

// Delegates a thread-safe wake notification to the implementation.
void EventLoop::wake() noexcept {
    impl_->wake();
}

// Returns a snapshot of this worker's lifecycle counters.
EventLoopStats EventLoop::stats() const noexcept {
    return impl_->stats();
}
