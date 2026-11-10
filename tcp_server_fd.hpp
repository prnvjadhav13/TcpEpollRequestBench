// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#pragma once

#include <signal.h>

// Move-only RAII owner for a file descriptor.
class UniqueFd {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept;
    ~UniqueFd() noexcept;

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& other) noexcept;
    UniqueFd& operator=(UniqueFd&& other) noexcept;

    [[nodiscard]] int get() const noexcept;
    [[nodiscard]] int fd() const noexcept;
    [[nodiscard]] explicit operator bool() const noexcept;
    [[nodiscard]] int release() noexcept;
    // On Linux, close() must not be retried after EINTR because the descriptor
    // number may already have been reused.
    int close() noexcept;
    void reset(int replacement = -1) noexcept;

private:
    int fd_ = -1;
};

// Blocks termination signals and exposes them through a nonblocking signalfd.
class SignalFd {
public:
    SignalFd();
    ~SignalFd() noexcept;

    SignalFd(const SignalFd&) = delete;
    SignalFd& operator=(const SignalFd&) = delete;
    SignalFd(SignalFd&&) = delete;
    SignalFd& operator=(SignalFd&&) = delete;

    [[nodiscard]] int fd() const noexcept;

private:
    UniqueFd fd_;
    sigset_t previous_mask_{};
    bool mask_installed_ = false;
};
