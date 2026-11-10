# TcpEpollRequestBench

TcpEpollRequestBench is a C++23/Linux project for learning, validating, and
measuring a scalable TCP request/response design. It includes an `epoll`-based
server and a concurrent load-test client that can run on the same machine or on
different machines.

## Purpose

The project demonstrates how to build a TCP server intended to scale as the
number of open connections grows, including experiments targeting millions of
connections when the host and operating-system resource limits permit. It uses
Linux's event-driven `epoll` API and C++ `std::jthread` worker threads to handle
many mostly-idle connections without dedicating one server thread to each
connection. It also provides a client for measuring request throughput and
response bandwidth as concurrency changes.

The million-connection figure is an experiment target, not a result or a
guarantee: actual capacity depends on memory, file-descriptor limits, kernel
settings, hardware, workload, and the number and capacity of event loops. The
bundled client supports at most 4,096 concurrent workers/connections, so
million-connection tests require external or distributed load generation.

The project demonstrates how to:

- handle many TCP connections with multiple independent `epoll` event loops on
  C++ `std::jthread` workers;
- distribute accepted connections across server workers with `SO_REUSEPORT`;
- process fragmented and pipelined requests and partial nonblocking writes safely;
- enforce connection, request-size, and idle-time limits;
- keep file descriptors and memory mappings under RAII ownership;
- separate private server response data from a small client test-key file; and
- measure request throughput, response bandwidth, and connection failures at
  different concurrency levels.

It is intended for systems-programming education, functional testing, and
controlled performance experiments. It is not, by itself, an authenticated or
encrypted Internet-facing service.

## High-level design

```text
       client machine                       server machine

 client_request_keys.txt          +-----------------------------+
          |                       | tcp_server_epoll            |
          v                       |                             |
 +----------------+  requests     | worker 1: epoll event loop  |
 | tcp_client     |-------------->| worker 2: epoll event loop  |
 | worker threads |<--------------| worker N: epoll event loop  |
 +----------------+  responses    +--------------+--------------+
                                                  |
                                                  v
                              server_request_response_mapping.bin
                                      request key -> binary payload
```

The main components are:

- `tcp_server_epoll` starts one `std::jthread` and one `epoll` event loop per
  worker. Each worker owns a `SO_REUSEPORT` listening socket, its active
  connection states, and its allocation-free timeout wheel. Connections use
  level-triggered notifications and remain assigned to one event loop for their
  lifetime. Bounded work per event provides fairness, and the configured
  connection capacity is `--event-loops` multiplied by
  `--max-connections-per-loop`.
- The server loads `server_request_response_mapping.bin` once at startup. The
  default `mmap-view` loader indexes keys and keeps response views in a
  read-only anonymous snapshot, avoiding per-payload allocations without
  retaining a vulnerable file-backed mapping.
- `tcp_client` opens one persistent connection per worker and reports success,
  request throughput, response bandwidth, and categorized connection failures.
  It creates a fixed worker-thread set once, feeds it through a bounded
  condition-variable queue,
  and reuses those threads across all rounds. Each worker performs one complete
  framed request/response transaction at a time on that connection and sleeps
  without busy-spinning when the queue is empty. A stale connection is replaced
  automatically and the read-only request is retried once.
- `server_request_response_mapping.bin` stays on the server and contains keys
  plus binary response payloads.
- `client_request_keys.txt` contains only valid keys and can be copied to a
  test-client machine.

The deliberately small protocol accepts newline-terminated request keys. A
known key returns a four-byte big-endian payload length followed by its binary
payload. Request lines are limited to 1,024 bytes, and generated response
payloads are 2,000-4,000 bytes. The server supports keep-alive and up to 16
pipelined responses per connection; responses are emitted in request order.
An empty, oversized, unknown, or excessively pipelined request closes the
connection. The bundled client reuses one connection per worker across requests
and rounds. This is not HTTP; the binary length prefix is the response-framing
contract and is included in network bytes but excluded from payload statistics.

Server implementation is split by responsibility: `tcp_server_main.cpp`
contains command-line handling and worker coordination,
`tcp_server_event_loop.cpp` implements each epoll worker declared in
`tcp_server_event_loop.hpp`,
`tcp_server_mapping.cpp` handles request/response mappings,
`tcp_server_connection.cpp` manages per-client state, and
`tcp_server_fd.cpp` owns descriptor and signal RAII utilities. Their internal
interfaces are declared in the corresponding headers.

Client rounds have an intentional completion barrier: all requests in a round
finish before its consolidated statistics and optional ordered verbose results
are printed, and only then may the next `--forever` round begin. This makes
rounds independently comparable. It introduces a small between-round barrier;
a continuous interval-reporting load generator would be preferable when the
only objective is maximum uninterrupted throughput.

## Requirements

- Linux server with a C++23-capable GCC toolchain
- Boost 1.81 or newer development headers for `boost::unordered_flat_map`
- Linux or another POSIX client supported by this source
- Network access from the client to the chosen server TCP port
- `scp` for copying the key file, or an equivalent file-transfer tool

## Build

The included `Makefile` builds the server and client together. Run one of these
targets from the repository root:

```bash
make debug
make sanitizer
make production
make performance
```

The variants are kept in separate directories:

- `build/debug`: `-Og -g3`, frame pointers, and libstdc++ assertions for GDB.
- `build/sanitizer`: AddressSanitizer and UndefinedBehaviorSanitizer for finding
  memory and undefined-behavior bugs. Do not benchmark this build.
- `build/production`: portable `-O2` build with stack protection, fortified libc
  calls, PIE, RELRO, and immediate symbol binding. This is the default target.
- `build/performance`: `-O3 -march=native -flto` for benchmark measurements on
  the machine where it was built. A `-march=native` binary may not run on an
  older or different CPU.

Build every variant or display the available targets with:

```bash
make all
make help
```

The examples below use the production binaries. Substitute
`build/debug`, `build/sanitizer`, or `build/performance` when appropriate.

## Quick local test

First create a small mapping. Creation is explicit so starting the server from
the wrong directory cannot silently generate a large file:

```bash
./build/production/tcp_server_epoll \
    --mapping-file server_request_response_mapping.bin \
    --mapping-entries 10000 \
    --regen-mapping \
    --export-keys 10000 \
    --keys-output client_request_keys.txt
```

Start the server in the first terminal. `--regen-mapping` is intentionally
omitted so this run reuses the exact mapping from which the keys were exported:

```bash
./build/production/tcp_server_epoll \
    --listen 8080 \
    --event-loops 4 \
    --max-connections-per-loop 10000 \
    --idle-timeout-seconds 15 \
    --mapping-file server_request_response_mapping.bin \
    --mapping-loader mmap-view
```

Run a smoke test in a second terminal:

```bash
./build/production/tcp_client 127.0.0.1 8080 \
    --keys-file client_request_keys.txt \
    --request-count 100 \
    --max-concurrency 10
```

A healthy run reports `success=100 failed=0 connect_failed=0`. Stop the server
with `Ctrl+C`. The first termination signal drains existing connections; a
second signal forces shutdown.

## Test from a different machine

### 1. Prepare the server dataset and client keys

On the server machine, generate the mapping and key file once. For an initial
remote test, 10,000 entries is sufficient:

```bash
cd /path/to/TcpEpollRequestBench

./build/production/tcp_server_epoll \
    --mapping-file server_request_response_mapping.bin \
    --mapping-entries 10000 \
    --regen-mapping \
    --export-keys 10000 \
    --keys-output client_request_keys.txt
```

For a larger working set, increase `--mapping-entries` up to 500,000. The full
500,000-entry mapping is approximately 1.5 GB and requires additional memory,
disk space, and generation time.

### 2. Copy only the keys to the client

From the client machine, pull the file from the server:

```bash
scp USER@SERVER_IP:/path/to/TcpEpollRequestBench/client_request_keys.txt \
    /path/to/TcpEpollRequestBench/
```

Do not copy `server_request_response_mapping.bin`; it is server-only data.
Optionally run `sha256sum client_request_keys.txt` on both machines to confirm
that the transfer produced identical files.

### 3. Start exactly one server

Exporting keys and listening can be combined in one invocation. This guarantees
that the exported keys and active mapping come from the same loaded dataset:

```bash
./build/production/tcp_server_epoll \
    --listen 8080 \
    --event-loops 8 \
    --max-connections-per-loop 10000 \
    --idle-timeout-seconds 15 \
    --mapping-file server_request_response_mapping.bin \
    --mapping-loader mmap-view \
    --export-keys 10000 \
    --keys-output client_request_keys.txt
```

If this command exports a new key file, copy that file to the client before
testing. The server has a per-user, per-port lock that prevents another
current-version server process from silently joining the same `SO_REUSEPORT`
group. Also check for older builds that do not have this protection:

```bash
ps -ef | grep '[t]cp_server_epoll'
ss -ltnp 'sport = :8080'
```

### 4. Confirm network access

The server listens on all IPv4 interfaces. Confirm its address and port:

```bash
ip -brief address
ss -ltnp 'sport = :8080'
```

Permit the port only from the intended client IP using the host firewall and,
when applicable, the cloud security group. Prefer a private network or VPN.

### 5. Run the remote client

On the client machine:

```bash
cd /path/to/TcpEpollRequestBench

./build/production/tcp_client SERVER_IP 8080 \
    --keys-file client_request_keys.txt \
    --request-count 1000 \
    --max-concurrency 32
```

Replace `SERVER_IP` with an IPv4 address reachable from the client. A successful
run reports zero failures. The client exits with status `2` if any request fails,
which makes it usable in automated test scripts.

Connection failures are reported with the server IP and port. An immediate
`connection refused` normally means the host responded but nothing is listening
on that port. A timeout or unreachable message normally indicates an incorrect
IP address, missing route, firewall/security-group filtering, or an unavailable
server. The connection timeout is five seconds.

## Scalability and performance testing

Build the `performance` target for measurements (`-O3 -flto` with local CPU
optimization), use summary mode, and run the client from another machine so
server and client CPU usage do not compete. Do not use `--verbose` for
benchmarks because terminal output overwhelms the workload.

The server's configured connection ceiling is
`--event-loops * --max-connections-per-loop`; for example, 16 loops with 62,500
connections per loop configure a ceiling of one million. This is only a
configuration target. The server checks that the requested descriptors fit the
process's soft `RLIMIT_NOFILE`, but it cannot ensure the host has enough memory,
kernel capacity, CPU, or network bandwidth to reach that ceiling. Check and
raise applicable per-process and system limits before testing, and observe
actual accepted connections and resource use. The server prints accepted,
rejected, and closed connection totals when it shuts down.

The server uses level-triggered epoll notifications and bounded accept/read/write
work per event so a busy connection does not monopolize a worker. `SO_REUSEPORT`
allows each worker to accept connections on its own listener. These design
choices support experiments at increasing connection counts; they do not
replace measuring behavior on the target machine.

Run a concurrency sweep instead of testing only one value:

```bash
./build/performance/tcp_client SERVER_IP 8080 --request-count 100000 --max-concurrency 1
./build/performance/tcp_client SERVER_IP 8080 --request-count 100000 --max-concurrency 8
./build/performance/tcp_client SERVER_IP 8080 --request-count 100000 --max-concurrency 32
./build/performance/tcp_client SERVER_IP 8080 --request-count 100000 --max-concurrency 128
./build/performance/tcp_client SERVER_IP 8080 --request-count 100000 --max-concurrency 256
```

Record these fields for every run:

- `success`, `failed`, and `connect_failed`
- `elapsed_ms`
- `requests_per_second` (requests attempted during the round divided by elapsed
  time)
- `successful_MiB_per_second`
- server and client CPU utilization
- server resident memory and network utilization

Increase concurrency until throughput stops improving or failures become
unacceptable. For higher load, run clients from multiple machines. This client
uses a closed-loop workload: each worker starts another request after its
previous request finishes. Therefore, `--max-concurrency` is a maximum number of
simultaneous requests, not a guaranteed arrival rate.

Before a high-concurrency test, inspect file-descriptor limits on both machines:

```bash
ulimit -n
```

Choose `--event-loops` based on measurement. The number of available CPU cores
is a reasonable starting point, but more loops are not automatically faster.
Each worker reuses one TCP connection. Connection setup and teardown are
amortized across its requests; a reconnect is performed after a stale socket or
I/O failure.

## Useful server options

- `--event-loops N`: number of event-loop workers (default: available CPU count)
- `--max-connections-per-loop N`: per-worker active connection ceiling (default:
  10,000); total configured capacity is the product of this and `--event-loops`
- `--idle-timeout-seconds N`: close connections that remain idle for this many
  seconds (default: 15)
- `--mapping-file PATH`: server-side request/response mapping (default:
  `server_request_response_mapping.bin`)
- `--mapping-loader MODE`: choose `ifstream`, `mmap`, or `mmap-view` (default:
  `mmap-view`)
- `--regen-mapping --mapping-entries N`: explicitly generate a mapping with up
  to 1,000,000 entries; `--mapping-entries` requires `--regen-mapping`
- `--export-keys N --keys-output PATH`: export valid request keys for the
  bundled client without copying the server mapping file

Run `tcp_server_epoll --help` and `tcp_client --help` for the full command-line
usage and option limits. The server accepts IPv4 connections on all interfaces;
restrict access with a firewall and use a trusted network.

## Useful client options

- `--request-count N`: requests sent in each round (default: 8)
- `--max-concurrency N`: maximum worker threads and in-flight requests; the
  maximum is 4,096; the default is one worker per CPU reported to the process
  (or one worker if CPU detection is unavailable)
- `--queue-capacity N`: bounded number of requests waiting for workers; the
  default is twice the effective worker count, capped by the requests in one
  round
- `--round-delay-ms N`: interruptible delay between `--forever` rounds; useful
  for pacing sustained tests and reducing reconnect pressure
- `--forever`: repeat rounds until interrupted
- `--verbose`: print each binary response as a hex dump; debugging only
- `--keys-file PATH`: use a non-default key file

The client creates its worker threads and per-worker connections once and joins
them at shutdown. The CPU-aware default is deliberately conservative. For
example, a two-CPU machine defaults to two workers. Higher concurrency can
improve throughput when workers spend most of their time waiting on a remote
network, but should be enabled explicitly after measuring CPU use, latency, and
failures.
The first SIGINT or SIGTERM stops submission, discards queued work, allows
active socket operations to finish within their five-second timeout, closes
each worker's persistent socket, and joins the workers. A second signal exits
immediately.

## Mapping loaders

- `ifstream`: copies all keys and payloads into owned memory.
- `mmap`: reads the file into an anonymous mapping, marks it read-only, parses
  it, and copies entries into owned containers.
- `mmap-view`: reads the file into an anonymous mapping, marks it read-only,
  and keeps validated key/payload views into that immutable snapshot.

Neither mmap mode retains a live file-backed mapping, so truncating or replacing
the source file after startup cannot cause `SIGBUS` or mutate active views.
Regenerate and re-export keys together; keys from another mapping are rejected.

## Security and scope

This is a benchmarking/example server, not an Internet-facing application
protocol. It has no TLS, authentication, authorization, request-level error
response, or application rate limiting. Run it only on a trusted network or
behind a secured proxy/load balancer. Restrict firewall access to designated
test clients.

Request lookup uses process-randomized SipHash. Partial request lines must be
completed within 10 seconds, and queued responses must drain within 30 seconds;
ordinary socket activity cannot extend those absolute progress deadlines.

The client validates response size but does not verify payload contents. A
content-integrity test would require a manifest containing each key's expected
payload size and cryptographic digest.
