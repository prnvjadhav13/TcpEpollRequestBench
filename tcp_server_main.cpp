// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#include <iostream>
#include <string>
#include <thread>
#include <string_view>
#include <stdexcept>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <csignal>
#include <vector>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <exception>
#include <fstream>
#include <functional>
#include <random>
#include <filesystem>
#include <array>
#include <bit>
#include <optional>
#include <span>
#include <memory>
#include <limits>
#include <type_traits>
#include <utility>
#include <poll.h>
#include <pthread.h>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/signalfd.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/random.h>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

#include <boost/unordered/unordered_flat_map.hpp>


#include "tcp_common_config.h"
#include "tcp_server_event_loop.hpp"
#include "tcp_server_fd.hpp"
#include "tcp_server_mapping.hpp"
#include "tcp_server_runtime.hpp"

using namespace tcp_server_runtime;

namespace {
constexpr auto kDefaultIdleTimeout = std::chrono::seconds(15);
constexpr auto kGraceShutdownTimeout = std::chrono::seconds(30);
constexpr std::size_t kDefaultMappingEntries = 500000;
static_assert(tcp_common::kMaxPayloadSize <=
              static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()));

// Chooses a conservative default worker count from available CPU concurrency.
std::size_t default_event_loop_count() {
    const unsigned int hardware_threads = std::thread::hardware_concurrency();
    return hardware_threads == 0 ? 1U : hardware_threads;
}

// Pins a worker to one CPU from the process's allowed affinity set. Failure is
// non-fatal because containers and restricted service managers may forbid it.
void pin_current_thread(std::size_t worker_index) noexcept {
    cpu_set_t allowed{};
    if (::sched_getaffinity(0, sizeof(allowed), &allowed) < 0) {
        return;
    }
    const int allowed_count = CPU_COUNT(&allowed);
    if (allowed_count <= 0) {
        return;
    }
    const std::size_t target_index = worker_index % static_cast<std::size_t>(allowed_count);
    std::size_t seen = 0;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &allowed)) {
            continue;
        }
        if (seen++ != target_index) {
            continue;
        }
        cpu_set_t target{};
        CPU_SET(cpu, &target);
        (void)::pthread_setaffinity_np(::pthread_self(), sizeof(target), &target);
        return;
    }
}

// Rejects configurations that cannot fit their requested sockets in the
// process descriptor limit, including conservative per-worker overhead.
void validate_file_descriptor_capacity(std::size_t event_loop_count,
                                       std::size_t max_connections_per_loop) {
    if (max_connections_per_loop >
        (std::numeric_limits<std::size_t>::max() - 32U) / event_loop_count) {
        throw std::invalid_argument("configured connection capacity overflows size_t");
    }
    const std::size_t connections = event_loop_count * max_connections_per_loop;
    constexpr std::size_t kDescriptorsPerLoop = 4;
    if (event_loop_count >
        (std::numeric_limits<std::size_t>::max() - connections - 32U) /
            kDescriptorsPerLoop) {
        throw std::invalid_argument("configured descriptor capacity overflows size_t");
    }
    const std::size_t required = connections + event_loop_count * kDescriptorsPerLoop + 32U;
    struct rlimit limit {};
    if (::getrlimit(RLIMIT_NOFILE, &limit) < 0) {
        throw std::runtime_error(std::string("getrlimit(RLIMIT_NOFILE) failed: ") +
                                 std::strerror(errno));
    }
    if (limit.rlim_cur != RLIM_INFINITY &&
        required > static_cast<std::size_t>(limit.rlim_cur)) {
        throw std::runtime_error(
            "RLIMIT_NOFILE soft limit " + std::to_string(limit.rlim_cur) +
            " is below required descriptor capacity " + std::to_string(required) +
            "; raise ulimit -n or reduce --event-loops/--max-connections-per-loop");
    }
}

// Parses a base-10 CLI value; text is the raw input and arg_name labels errors.
int parse_int_arg(const char* text, const char* arg_name) {
    try {
        std::size_t pos = 0;
        const std::string value(text);
        const int parsed = std::stoi(value, &pos, 10);
        if (pos != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception&) {
        throw std::invalid_argument(std::string(arg_name) + " must be a valid integer");
    }
}

// Advances global shutdown state on each termination signal: the first starts
// draining and a later signal requests immediate stopping.
void begin_graceful_shutdown() noexcept {
    ShutdownState expected = ShutdownState::Running;
    if (g_shutdown_state.compare_exchange_strong(expected, ShutdownState::Draining,
                                                  std::memory_order_acq_rel)) {
        const auto deadline = steady_clock_ns() +
            std::chrono::duration_cast<std::chrono::nanoseconds>(kGraceShutdownTimeout).count();
        g_drain_deadline_ns.store(deadline, std::memory_order_release);
    } else {
        g_shutdown_state.store(ShutdownState::Stopping, std::memory_order_release);
    }
}

} // namespace

namespace {
struct WorkerThreadContext {
    const std::vector<std::unique_ptr<EventLoop>>& event_loops;
    std::exception_ptr& worker_exception;
    std::mutex& worker_exception_mutex;
    int completion_fd;
};

void wake_event_loop_workers(const WorkerThreadContext& context) noexcept {
    for (const auto& event_loop : context.event_loops) {
        event_loop->wake();
    }
}

void stop_and_join_workers(std::vector<std::jthread>& workers,
                           const WorkerThreadContext& context) noexcept {
    g_shutdown_state.store(ShutdownState::Stopping, std::memory_order_release);
    for (auto& worker : workers) {
        worker.request_stop();
    }
    wake_event_loop_workers(context);
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void run_event_loop_worker(std::stop_token stop_token,
                           std::size_t worker_index,
                           EventLoop& event_loop,
                           WorkerThreadContext& context) {
    try {
        pin_current_thread(worker_index);
        event_loop.run(stop_token);
    } catch (...) {
        {
            std::lock_guard lock(context.worker_exception_mutex);
            if (!context.worker_exception) {
                context.worker_exception = std::current_exception();
            }
        }
        g_shutdown_state.store(ShutdownState::Stopping, std::memory_order_release);
        wake_event_loop_workers(context);
    }

    const std::uint64_t one = 1;
    for (;;) {
        const ssize_t result = ::write(context.completion_fd, &one, sizeof(one));
        if (result == static_cast<ssize_t>(sizeof(one))) {
            break;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}
} // namespace

// Coordinates event_loop_count workers on listen_port, applying idle_timeout
// and max_connections_per_loop; port_instance_lock is retained for exclusivity.
void start_listen(int listen_port,
                  std::size_t event_loop_count,
                  std::chrono::seconds idle_timeout,
                  std::size_t max_connections_per_loop,
                  UniqueFd port_instance_lock) {
    // Ownership of this descriptor keeps the advisory lock for the full
    // lifetime of the listening server.
    (void)port_instance_lock;
    g_shutdown_state.store(ShutdownState::Running, std::memory_order_release);
    g_drain_deadline_ns.store(0, std::memory_order_release);
    SignalFd termination_signals;
    UniqueFd worker_completion = create_event_handle();
    std::cout << "Listening on port " << listen_port
              << " using " << event_loop_count
              << " epoll event loop(s) with SO_REUSEPORT; maximum active connections: "
              << (event_loop_count * max_connections_per_loop) << std::endl;

    // Construct every event loop before starting threads. Listener or epoll
    // creation failures therefore cannot leave a partially running server.
    std::vector<std::unique_ptr<EventLoop>> event_loops;
    event_loops.reserve(event_loop_count);
    for (std::size_t i = 0; i < event_loop_count; ++i) {
        event_loops.push_back(std::make_unique<EventLoop>(
            i + 1, listen_port, idle_timeout, max_connections_per_loop));
    }

    std::exception_ptr worker_exception;
    std::mutex worker_exception_mutex;
    WorkerThreadContext worker_context{
        event_loops, worker_exception, worker_exception_mutex, worker_completion.fd()};
    std::vector<std::jthread> workers;
    workers.reserve(event_loop_count);

    try {
        for (std::size_t i = 0; i < event_loop_count; ++i) {
            workers.emplace_back(run_event_loop_worker,
                                 i,
                                 std::ref(*event_loops[i]),
                                 std::ref(worker_context));
        }
    } catch (...) {
        stop_and_join_workers(workers, worker_context);
        throw;
    }

    int last_termination_signal = 0;
    std::size_t remaining_workers = workers.size();

    try {
        while (remaining_workers != 0) {
            if (g_shutdown_state.load(std::memory_order_acquire) ==
                    ShutdownState::Draining &&
                steady_clock_ns() >=
                    g_drain_deadline_ns.load(std::memory_order_acquire)) {
                g_shutdown_state.store(ShutdownState::Stopping,
                                       std::memory_order_release);
                wake_event_loop_workers(worker_context);
            }

            std::array<pollfd, 2> poll_fds{{
                {termination_signals.fd(), POLLIN, 0},
                {worker_completion.fd(), POLLIN, 0},
            }};
            const int ready = ::poll(poll_fds.data(), poll_fds.size(), 250);
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::runtime_error(std::string("coordinator poll failed: ") +
                                         std::strerror(errno));
            }

            for (const pollfd& descriptor : poll_fds) {
                if ((descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                    throw std::runtime_error("coordinator descriptor became invalid");
                }
            }

            if ((poll_fds[0].revents & POLLIN) != 0) {
                for (;;) {
                    signalfd_siginfo signal_info{};
                    const ssize_t count = ::read(termination_signals.fd(),
                                                 &signal_info,
                                                 sizeof(signal_info));
                    if (count == static_cast<ssize_t>(sizeof(signal_info))) {
                        last_termination_signal =
                            static_cast<int>(signal_info.ssi_signo);
                        begin_graceful_shutdown();
                        wake_event_loop_workers(worker_context);
                        continue;
                    }
                    if (count < 0 && errno == EINTR) {
                        continue;
                    }
                    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        break;
                    }
                    throw std::runtime_error("short or failed read from signalfd");
                }
            }

            if ((poll_fds[1].revents & POLLIN) != 0) {
                for (;;) {
                    std::uint64_t completed = 0;
                    const ssize_t count = ::read(worker_completion.fd(),
                                                 &completed,
                                                 sizeof(completed));
                    if (count == static_cast<ssize_t>(sizeof(completed))) {
                        const std::size_t completed_count =
                            completed > remaining_workers
                                ? remaining_workers
                                : static_cast<std::size_t>(completed);
                        remaining_workers -= completed_count;
                        continue;
                    }
                    if (count < 0 && errno == EINTR) {
                        continue;
                    }
                    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        break;
                    }
                    throw std::runtime_error("short or failed read from worker eventfd");
                }
            }
        }
    } catch (...) {
        stop_and_join_workers(workers, worker_context);
        throw;
    }

    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    if (worker_exception) {
        try {
            std::rethrow_exception(worker_exception);
        } catch (const std::exception& ex) {
            throw std::runtime_error(std::string("event-loop worker failed: ") + ex.what());
        } catch (...) {
            throw std::runtime_error("event-loop worker failed with an unknown exception");
        }
    }

    EventLoopStats totals;
    for (const auto& event_loop : event_loops) {
        const EventLoopStats loop_stats = event_loop->stats();
        totals.accepted += loop_stats.accepted;
        totals.rejected += loop_stats.rejected;
        totals.closed += loop_stats.closed;
    }

    std::cout << "Shutdown"
              << (last_termination_signal == 0
                      ? " completed"
                      : " completed after signal " +
                            std::to_string(last_termination_signal))
              << ". accepted=" << totals.accepted
              << " rejected=" << totals.rejected
              << " closed=" << totals.closed << std::endl;
}

// Parses server and mapping options, initializes the shared request mapping,
// optionally exports keys, and starts listeners. argc/argv are the CLI inputs.
void print_server_usage(std::ostream& output, std::string_view program) {
    output
        << "Usage:\n"
        << "  " << program << " --listen <port> [options]\n"
        << "  " << program << " --export-keys <count> [options]\n\n"
        << "Options:\n"
        << "  -h, --help                         Show this help and exit\n"
        << "  --listen <port>                    Listen on TCP port 1..65535\n"
        << "  --mapping-file <path>              Mapping file (default: "
        << tcp_common::kDefaultServerMappingFile << ")\n"
        << "  --mapping-loader <mode>            ifstream, mmap, or mmap-view\n"
        << "                                      (default: mmap-view)\n"
        << "  --regen-mapping                    Regenerate the mapping file\n"
        << "  --mapping-entries <count>          Entries generated with --regen-mapping\n"
        << "  --event-loops <count>              Epoll worker count (default: CPU count)\n"
        << "  --max-connections-per-loop <count> Connection limit per epoll worker\n"
        << "                                      (default: 10000)\n"
        << "  --idle-timeout-seconds <seconds>   Positive idle timeout (default: "
        << kDefaultIdleTimeout.count() << ")\n"
        << "  --export-keys <count>              Export request keys for tcp_client\n"
        << "  --keys-output <path>               Export destination (default: "
        << tcp_common::kDefaultClientKeysFile << ")\n\n"
        << "Examples:\n"
        << "  " << program << " --listen 23456 --mapping-file "
        << tcp_common::kDefaultServerMappingFile << "\n"
        << "  " << program
        << " --listen 23456 --mapping-file server --mapping-loader mmap-view\n"
        << "  " << program
        << " --regen-mapping --mapping-entries 500000 --export-keys 500000\n\n"
        << "Options use a space between the option and its value; for example,\n"
        << "--mapping-file server (not --mapping-file=server).\n";
}

int main(int argc, char* argv[]) {
    try {
        int listen_port = 0;
        bool has_listen = false;
        bool force_regen_mapping = false;
        bool print_keys_requested = false;
        bool keys_output_requested = false;
        bool mapping_entries_requested = false;
        std::size_t print_keys_count = 0;
        std::size_t mapping_entries = kDefaultMappingEntries;
        std::size_t event_loop_count = default_event_loop_count();
        std::size_t max_connections_per_loop = 10000;
        std::string mapping_file = tcp_common::kDefaultServerMappingFile;
        std::string keys_output_file = tcp_common::kDefaultClientKeysFile;
        MappingLoadMode mapping_load_mode = MappingLoadMode::MMapView;
        std::chrono::seconds idle_timeout = kDefaultIdleTimeout;

        if (argc < 2) {
            throw std::invalid_argument("no mode or option was provided");
        }

        for (int i = 1; i < argc; ++i) {
            const std::string_view arg = argv[i];
            if (arg == "--help" || arg == "-h") {
                print_server_usage(std::cout, argv[0]);
                return 0;
            } else if (arg == "--listen") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--listen requires a port value");
                }
                listen_port = parse_int_arg(argv[i + 1], "--listen");
                has_listen = true;
                ++i;
            } else if (arg == "--mapping-file") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--mapping-file requires a file path");
                }
                mapping_file = argv[i + 1];
                ++i;
            } else if (arg == "--mapping-loader") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--mapping-loader requires one value: ifstream, mmap, or mmap-view");
                }
                const std::string_view loader = argv[i + 1];
                if (loader == "ifstream") {
                    mapping_load_mode = MappingLoadMode::Ifstream;
                } else if (loader == "mmap") {
                    mapping_load_mode = MappingLoadMode::MMap;
                } else if (loader == "mmap-view") {
                    mapping_load_mode = MappingLoadMode::MMapView;
                } else {
                    throw std::invalid_argument("--mapping-loader must be one of: ifstream, mmap, mmap-view");
                }
                ++i;
            } else if (arg == "--regen-mapping") {
                force_regen_mapping = true;
            } else if (arg == "--mapping-entries") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--mapping-entries requires a numeric value");
                }
                const int requested = parse_int_arg(argv[i + 1], "--mapping-entries");
                if (requested <= 0 ||
                    static_cast<std::uint32_t>(requested) > tcp_common::kMaxMappingEntries) {
                    throw std::invalid_argument(
                        "--mapping-entries must be in range 1.." +
                        std::to_string(tcp_common::kMaxMappingEntries));
                }
                mapping_entries = static_cast<std::size_t>(requested);
                mapping_entries_requested = true;
                ++i;
            } else if (arg == "--event-loops") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--event-loops requires a numeric value");
                }
                const int requested = parse_int_arg(argv[i + 1], "--event-loops");
                if (requested <= 0) {
                    throw std::invalid_argument("--event-loops must be positive");
                }
                event_loop_count = static_cast<std::size_t>(requested);
                ++i;
            } else if (arg == "--max-connections-per-loop") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--max-connections-per-loop requires a numeric value");
                }
                const int requested = parse_int_arg(argv[i + 1], "--max-connections-per-loop");
                if (requested <= 0) {
                    throw std::invalid_argument("--max-connections-per-loop must be positive");
                }
                max_connections_per_loop = static_cast<std::size_t>(requested);
                ++i;
            } else if (arg == "--idle-timeout-seconds") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--idle-timeout-seconds requires a numeric value");
                }
                const int requested = parse_int_arg(argv[i + 1], "--idle-timeout-seconds");
                if (requested <= 0) {
                    throw std::invalid_argument("--idle-timeout-seconds must be positive");
                }
                idle_timeout = std::chrono::seconds(requested);
                ++i;
            } else if (arg == "--export-keys" || arg == "--print-keys") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--export-keys requires a numeric value");
                }
                const int requested = parse_int_arg(argv[i + 1], "--export-keys");
                if (requested <= 0) {
                    throw std::invalid_argument("--export-keys must be positive");
                }
                print_keys_count = static_cast<std::size_t>(requested);
                print_keys_requested = true;
                ++i;
            } else if (arg == "--keys-output") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument("--keys-output requires a file path");
                }
                keys_output_file = argv[i + 1];
                if (keys_output_file.empty()) {
                    throw std::invalid_argument("--keys-output path must not be empty");
                }
                keys_output_requested = true;
                ++i;
            } else {
                throw std::invalid_argument("Unknown argument: " + std::string(arg));
            }
        }

        if (!has_listen && !print_keys_requested) {
            throw std::invalid_argument(
                "Provide at least one mode: --listen <port> and/or --export-keys <N>");
        }

        if (keys_output_requested && !print_keys_requested) {
            throw std::invalid_argument("--keys-output requires --export-keys");
        }

        if (mapping_entries_requested && !force_regen_mapping) {
            throw std::invalid_argument("--mapping-entries requires --regen-mapping");
        }

        if (has_listen && (listen_port <= 0 || listen_port > 65535)) {
            throw std::invalid_argument("Port must be in range 1..65535");
        }

        if (has_listen) {
            validate_file_descriptor_capacity(event_loop_count,
                                              max_connections_per_loop);
        }

        UniqueFd port_instance_lock;
        if (has_listen) {
            // Acquire this before loading or exporting so a conflicting server
            // cannot leave behind a key file for a mapping it never served.
            port_instance_lock = acquire_port_instance_lock(listen_port);
        }

        initialize_request_mapping(mapping_file,
                                   force_regen_mapping,
                                   mapping_load_mode,
                                   mapping_entries);

        if (print_keys_requested) {
            print_request_keys(print_keys_count, keys_output_file);
        }

        if (!has_listen) {
            return 0;
        }

        start_listen(listen_port,
                     event_loop_count,
                     idle_timeout,
                     max_connections_per_loop,
                     std::move(port_instance_lock));
        return 0;
    } catch (const std::invalid_argument& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        std::cerr << '\n';
        print_server_usage(std::cerr, argc > 0 ? argv[0] : "tcp_server_epoll");
        return 1;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }
}
