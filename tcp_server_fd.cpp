// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#include "tcp_server_fd.hpp"

#include <cerrno>
#include <csignal>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

#include <sys/signalfd.h>
#include <unistd.h>

UniqueFd::UniqueFd(int fd) noexcept : fd_(fd) {}

UniqueFd::~UniqueFd() noexcept {
    reset();
}

UniqueFd::UniqueFd(UniqueFd&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)) {}

UniqueFd& UniqueFd::operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
        reset(std::exchange(other.fd_, -1));
    }
    return *this;
}

int UniqueFd::get() const noexcept {
    return fd_;
}

int UniqueFd::fd() const noexcept {
    return get();
}

UniqueFd::operator bool() const noexcept {
    return fd_ >= 0;
}

int UniqueFd::release() noexcept {
    return std::exchange(fd_, -1);
}

int UniqueFd::close() noexcept {
    const int old_fd = release();
    return old_fd < 0 ? 0 : ::close(old_fd);
}

void UniqueFd::reset(int replacement) noexcept {
    if (fd_ == replacement) {
        return;
    }
    const int old_fd = std::exchange(fd_, replacement);
    if (old_fd >= 0) {
        ::close(old_fd);
    }
}

SignalFd::SignalFd() {
    sigset_t signals{};
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGINT);
    ::sigaddset(&signals, SIGTERM);
    ::sigaddset(&signals, SIGHUP);
    ::sigaddset(&signals, SIGQUIT);
    const int mask_error =
        ::pthread_sigmask(SIG_BLOCK, &signals, &previous_mask_);
    if (mask_error != 0) {
        throw std::runtime_error(std::string("pthread_sigmask failed while blocking ") +
                                 "termination signals: " + std::strerror(mask_error));
    }
    mask_installed_ = true;
    const int signal_fd =
        ::signalfd(-1, &signals, SFD_CLOEXEC | SFD_NONBLOCK);
    if (signal_fd < 0) {
        const int signal_error = errno;
        (void)::pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr);
        mask_installed_ = false;
        throw std::runtime_error(std::string("signalfd failed: ") +
                                 std::strerror(signal_error));
    }
    fd_.reset(signal_fd);
}

SignalFd::~SignalFd() noexcept {
    fd_.reset();
    if (mask_installed_) {
        (void)::pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr);
    }
}

int SignalFd::fd() const noexcept {
    return fd_.get();
}
