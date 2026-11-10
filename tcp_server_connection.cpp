// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#include "tcp_server_connection.hpp"

#include <stdexcept>
#include <type_traits>
#include <utility>

ConnectionState::ConnectionState(UniqueFd socket_value)
    : socket(std::move(socket_value)) {}

int ConnectionState::fd() const noexcept {
    return socket.get();
}

bool ConnectionState::has_responses() const noexcept {
    return response.size != 0;
}

bool ConnectionState::response_queue_full() const noexcept {
    return has_responses() && response_overflow != nullptr &&
           response_overflow->count == PipelinedResponseOverflow::kCapacity;
}

ResponseBufferView& ConnectionState::current_response() noexcept {
    return response;
}

void ConnectionState::set_current_response(ResponseBufferView next_response) noexcept {
    response = next_response;
    const auto size = static_cast<std::uint32_t>(next_response.size);
    response_header = {
        static_cast<std::uint8_t>(size >> 24U),
        static_cast<std::uint8_t>(size >> 16U),
        static_cast<std::uint8_t>(size >> 8U),
        static_cast<std::uint8_t>(size)};
    response_header_sent = 0;
    response_sent = 0;
}

bool ConnectionState::push_response(ResponseBufferView next_response) noexcept {
    if (!has_responses()) [[likely]] {
        set_current_response(next_response);
        return true;
    }
    if (response_overflow == nullptr) {
        try {
            response_overflow = std::make_unique<PipelinedResponseOverflow>();
        } catch (const std::bad_alloc&) {
            return false;
        }
    }
    if (response_overflow->count == PipelinedResponseOverflow::kCapacity) {
        return false;
    }
    const std::size_t tail =
        (response_overflow->head + response_overflow->count) %
        PipelinedResponseOverflow::kCapacity;
    response_overflow->responses[tail] = next_response;
    ++response_overflow->count;
    return true;
}

void ConnectionState::pop_response() noexcept {
    if (response_overflow == nullptr || response_overflow->count == 0) [[likely]] {
        response = {};
        response_header_sent = 0;
        response_sent = 0;
        response_overflow.reset();
        return;
    }
    const ResponseBufferView next_response =
        response_overflow->responses[response_overflow->head];
    response_overflow->head =
        (response_overflow->head + 1) % PipelinedResponseOverflow::kCapacity;
    --response_overflow->count;
    if (response_overflow->count == 0) {
        response_overflow.reset();
    }
    set_current_response(next_response);
}

static_assert(!std::is_copy_constructible_v<ConnectionState>);
static_assert(!std::is_copy_assignable_v<ConnectionState>);
static_assert(std::is_nothrow_move_constructible_v<ConnectionState>);
static_assert(std::is_nothrow_move_assignable_v<ConnectionState>);

ConnectionState& ConnectionTable::emplace(UniqueFd socket) {
    const int fd = socket.get();
    if (fd < 0) {
        throw std::runtime_error("cannot store negative file descriptor");
    }
    auto [it, inserted] = entries_.try_emplace(fd, std::move(socket));
    if (!inserted) {
        throw std::runtime_error("duplicate connection state for fd " + std::to_string(fd));
    }
    return it->second;
}

ConnectionState* ConnectionTable::find(int fd) noexcept {
    if (fd < 0) {
        return nullptr;
    }
    const auto it = entries_.find(fd);
    return it == entries_.end() ? nullptr : &it->second;
}

const ConnectionState* ConnectionTable::find(int fd) const noexcept {
    if (fd < 0) {
        return nullptr;
    }
    const auto it = entries_.find(fd);
    return it == entries_.end() ? nullptr : &it->second;
}

void ConnectionTable::erase(int fd) noexcept {
    entries_.erase(fd);
}

void ConnectionTable::clear() noexcept {
    entries_.clear();
}

std::size_t ConnectionTable::size() const noexcept {
    return entries_.size();
}

void ConnectionTable::reserve(std::size_t count) {
    entries_.reserve(count);
}

ConnectionTable::Iterator ConnectionTable::begin() noexcept {
    return entries_.begin();
}

ConnectionTable::Iterator ConnectionTable::end() noexcept {
    return entries_.end();
}

ConnectionTable::Iterator ConnectionTable::erase(Iterator it) noexcept {
    return entries_.erase(it);
}
