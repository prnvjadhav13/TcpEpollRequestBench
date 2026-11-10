// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#pragma once

#include "tcp_server_fd.hpp"
#include "tcp_server_mapping.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <boost/unordered/unordered_flat_map.hpp>

// Bounded overflow ring for pipelined responses beyond the inline response.
struct PipelinedResponseOverflow {
    static constexpr std::size_t kMaxPipelinedResponses = 16;
    static constexpr std::size_t kCapacity = kMaxPipelinedResponses - 1;
    std::array<ResponseBufferView, kCapacity> responses{};
    std::size_t head = 0;
    std::size_t count = 0;
};

// Holds a client's socket, request parser, response queue, and timeout state.
struct ConnectionState {
    explicit ConnectionState(UniqueFd socket_value);

    [[nodiscard]] int fd() const noexcept;
    [[nodiscard]] bool has_responses() const noexcept;
    [[nodiscard]] bool response_queue_full() const noexcept;
    [[nodiscard]] ResponseBufferView& current_response() noexcept;
    void set_current_response(ResponseBufferView next_response) noexcept;
    [[nodiscard]] bool push_response(ResponseBufferView next_response) noexcept;
    void pop_response() noexcept;

    UniqueFd socket;
    std::string request_line;
    ResponseBufferView response{};
    std::array<std::uint8_t, sizeof(std::uint32_t)> response_header{};
    std::unique_ptr<PipelinedResponseOverflow> response_overflow;
    std::size_t response_header_sent = 0;
    std::size_t response_sent = 0;
    std::uint64_t expiration_tick = 0;
    std::uint64_t idle_deadline_tick = 0;
    std::uint64_t request_deadline_tick = 0;
    std::uint64_t response_deadline_tick = 0;
    int wheel_previous = -1;
    int wheel_next = -1;
    std::uint8_t wheel_slot = 0;
    bool wheel_scheduled = false;
    bool peer_write_closed = false;
    std::uint32_t registered_events = 0;
};

// Owns live per-descriptor connection state in a cache-local hash table.
class ConnectionTable {
public:
    using Storage = boost::unordered_flat_map<int, ConnectionState>;
    using Iterator = Storage::iterator;

    ConnectionState& emplace(UniqueFd socket);
    [[nodiscard]] ConnectionState* find(int fd) noexcept;
    [[nodiscard]] const ConnectionState* find(int fd) const noexcept;
    void erase(int fd) noexcept;
    void clear() noexcept;
    [[nodiscard]] std::size_t size() const noexcept;
    void reserve(std::size_t count);
    Iterator begin() noexcept;
    Iterator end() noexcept;
    Iterator erase(Iterator it) noexcept;

private:
    Storage entries_;
};
