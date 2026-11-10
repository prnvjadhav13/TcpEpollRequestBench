// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#include "tcp_server_mapping.hpp"
#include "tcp_server_fd.hpp"
#include "tcp_common_config.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/random.h>
#include <fcntl.h>
#include <unistd.h>

namespace {
constexpr std::size_t kMaxRequestSize = 1024;
}

// Owns a generated response payload associated with one request key.
struct ClientResponse {
    std::vector<std::uint8_t> payload;
};

// Hashes string-like request keys without allocating a temporary std::string.
struct TransparentStringHash {
    using is_transparent = void;

    static const std::array<std::uint64_t, 2>& secrets() {
        static const std::array<std::uint64_t, 2> values = [] {
            std::array<std::uint64_t, 2> random_values{};
            std::size_t offset = 0;
            while (offset < sizeof(random_values)) {
                const ssize_t count = ::getrandom(
                    reinterpret_cast<std::uint8_t*>(random_values.data()) + offset,
                    sizeof(random_values) - offset, 0);
                if (count > 0) {
                    offset += static_cast<std::size_t>(count);
                    continue;
                }
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                throw std::runtime_error(
                    std::string("getrandom failed while seeding request hash: ") +
                    std::strerror(errno));
            }
            return random_values;
        }();
        return values;
    }

    static void sip_round(std::uint64_t& v0, std::uint64_t& v1,
                          std::uint64_t& v2, std::uint64_t& v3) noexcept {
        v0 += v1; v1 = std::rotl(v1, 13); v1 ^= v0; v0 = std::rotl(v0, 32);
        v2 += v3; v3 = std::rotl(v3, 16); v3 ^= v2;
        v0 += v3; v3 = std::rotl(v3, 21); v3 ^= v0;
        v2 += v1; v1 = std::rotl(v1, 17); v1 ^= v2; v2 = std::rotl(v2, 32);
    }

    std::size_t operator()(std::string_view value) const {
        const auto& key = secrets();
        std::uint64_t v0 = 0x736f6d6570736575ULL ^ key[0];
        std::uint64_t v1 = 0x646f72616e646f6dULL ^ key[1];
        std::uint64_t v2 = 0x6c7967656e657261ULL ^ key[0];
        std::uint64_t v3 = 0x7465646279746573ULL ^ key[1];
        const auto* bytes = reinterpret_cast<const std::uint8_t*>(value.data());
        std::size_t offset = 0;
        while (value.size() - offset >= sizeof(std::uint64_t)) {
            std::uint64_t message = 0;
            std::memcpy(&message, bytes + offset, sizeof(message));
            if constexpr (std::endian::native == std::endian::big) {
                message = std::byteswap(message);
            }
            v3 ^= message;
            sip_round(v0, v1, v2, v3); sip_round(v0, v1, v2, v3);
            v0 ^= message;
            offset += sizeof(message);
        }
        std::uint64_t tail = static_cast<std::uint64_t>(value.size()) << 56U;
        for (std::size_t i = 0; i < value.size() - offset; ++i) {
            tail |= static_cast<std::uint64_t>(bytes[offset + i]) << (i * 8U);
        }
        v3 ^= tail;
        sip_round(v0, v1, v2, v3); sip_round(v0, v1, v2, v3);
        v0 ^= tail;
        v2 ^= 0xffU;
        for (int i = 0; i < 4; ++i) {
            sip_round(v0, v1, v2, v3);
        }
        return static_cast<std::size_t>(v0 ^ v1 ^ v2 ^ v3);
    }

    std::size_t operator()(const std::string& value) const {
        return (*this)(std::string_view(value));
    }

    std::size_t operator()(const char* value) const {
        return (*this)(std::string_view(value));
    }
};

// Enables heterogeneous equality checks for owned and borrowed request keys.
struct TransparentStringEqual {
    using is_transparent = void;

    bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
        return lhs == rhs;
    }
};

// Open addressing keeps lookup metadata and entries in contiguous allocations,
// improving cache locality for the immutable, lookup-heavy request index.
using RequestMap = boost::unordered_flat_map<std::string,
                                             ClientResponse,
                                             TransparentStringHash,
                                             TransparentStringEqual>;

using MappedPayloadView = std::span<const std::uint8_t>;
using MappedRequestMap = boost::unordered_flat_map<std::string_view,
                                                   MappedPayloadView,
                                                   TransparentStringHash,
                                                   TransparentStringEqual>;

class MappedRegion;

// Formats the configured response payload bounds for diagnostics.
std::string payload_range_string() {
    return "[" + std::to_string(tcp_common::kMinPayloadSize) + ", " +
           std::to_string(tcp_common::kMaxPayloadSize) + "]";
}

// Generates count unique request keys and random payloads within protocol size
// limits for creating a benchmark mapping file.
RequestMap generate_random_requests(std::size_t count) {
    static_assert(tcp_common::kMinPayloadSize >= sizeof(std::uint64_t),
                  "payload must have room for its unique response ID");

    RequestMap data;
    data.reserve(count);

    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<std::size_t> payload_len_dist(
        tcp_common::kMinPayloadSize,
        tcp_common::kMaxPayloadSize);
    std::uniform_int_distribution<int> suffix_dist(100000, 999999);

    // Generate random-looking response content once. Each response still owns
    // separate contiguous storage, while copying this template is much cheaper
    // than invoking a random distribution for every payload byte.
    std::vector<std::uint8_t> payload_template(tcp_common::kMaxPayloadSize);
    std::size_t template_offset = 0;
    while (template_offset < payload_template.size()) {
        const std::uint64_t random_bits = rng();
        const std::size_t remaining = payload_template.size() - template_offset;
        const std::size_t bytes_to_copy =
            remaining < sizeof(random_bits) ? remaining : sizeof(random_bits);
        std::memcpy(payload_template.data() + template_offset,
                    &random_bits,
                    bytes_to_copy);
        template_offset += bytes_to_copy;
    }

    for (std::size_t i = 0; i < count; ++i) {
        const std::size_t payload_len = payload_len_dist(rng);
        std::string request =
            "GET /page_" + std::to_string(i) + "_" + std::to_string(suffix_dist(rng));
        auto [it, inserted] = data.emplace(std::move(request), ClientResponse{});
        if (!inserted) {
            throw std::logic_error("generated duplicate request key");
        }

        auto& payload = it->second.payload;
        using PayloadDifference =
            std::vector<std::uint8_t>::const_iterator::difference_type;
        payload.assign(payload_template.begin(),
                       payload_template.begin() +
                           static_cast<PayloadDifference>(payload_len));

        // The loop index makes uniqueness deterministic rather than
        // probabilistic. Explicit little-endian encoding also makes generated
        // payloads reproducible across CPU architectures for a fixed template.
        const std::uint64_t response_id = static_cast<std::uint64_t>(i);
        for (std::size_t byte = 0; byte < sizeof(response_id); ++byte) {
            payload[byte] = static_cast<std::uint8_t>(response_id >> (byte * 8U));
        }
    }

    return data;
}

// Writes exactly size bytes from data to fd, retrying interrupted/partial I/O.
void write_all_fd(int fd, const void* data, std::size_t size) {
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    std::size_t written = 0;
    while (written < size) {
        const ssize_t n = ::write(fd, bytes + written, size - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error(std::string("write failed: ") + std::strerror(errno));
        }
        if (n == 0) {
            throw std::runtime_error("write returned 0 while writing mapping file");
        }
        written += static_cast<std::size_t>(n);
    }
}

// Serializes value to fd as one big-endian 32-bit mapping-file field.
void write_u32_fd(int fd, std::uint32_t value) {
    // MAP1 integers are always little-endian, independent of host CPU endian.
    const std::array<std::uint8_t, 4> bytes{
        static_cast<std::uint8_t>(value),
        static_cast<std::uint8_t>(value >> 8U),
        static_cast<std::uint8_t>(value >> 16U),
        static_cast<std::uint8_t>(value >> 24U)};
    write_all_fd(fd, bytes.data(), bytes.size());
}

// Reads exactly size bytes from fd into data; context identifies the field in
// truncation and I/O errors.
void read_exact_fd(int fd, void* data, std::size_t size, const char* context) {
    auto* bytes = static_cast<std::uint8_t*>(data);
    std::size_t total = 0;
    while (total < size) {
        const ssize_t n = ::read(fd, bytes + total, size - total);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error(std::string("read failed while ") + context + ": " +
                                     std::strerror(errno));
        }
        if (n == 0) {
            throw std::runtime_error(std::string("mapping file is truncated while ") + context);
        }
        total += static_cast<std::size_t>(n);
    }
}

// Reads and converts one big-endian 32-bit field from fd; context labels errors.
std::uint32_t read_u32_fd(int fd, const char* context) {
    std::array<std::uint8_t, 4> bytes{};
    read_exact_fd(fd, bytes.data(), bytes.size(), context);
    return static_cast<std::uint32_t>(bytes[0]) |
           (static_cast<std::uint32_t>(bytes[1]) << 8U) |
           (static_cast<std::uint32_t>(bytes[2]) << 16U) |
           (static_cast<std::uint32_t>(bytes[3]) << 24U);
}

// Validates the mapping file's magic identifier and supported format version.
void validate_mapping_file_header(std::uint32_t magic, std::uint32_t version) {
    if (magic != tcp_common::kMappingFileMagic) {
        throw std::runtime_error("invalid mapping file magic; expected MAP1 format");
    }
    if (version != tcp_common::kMappingFileVersion) {
        throw std::runtime_error("unsupported mapping file version: " + std::to_string(version));
    }
}

// Ensures file_size for path fits the minimum header and configured safety cap.
void validate_mapping_file_size(std::size_t file_size, const std::string& path) {
    if (file_size < tcp_common::kMappingFileHeaderBytes) {
        throw std::runtime_error("mapping file is too small to contain required header: " + path);
    }
    if (file_size > tcp_common::kMaxMappingFileSizeBytes) {
        throw std::runtime_error("mapping file exceeds max allowed size (" +
                                 std::to_string(tcp_common::kMaxMappingFileSizeBytes) + " bytes): " + path);
    }
}

// Reads one big-endian integer at cursor, advances it, and uses end to reject
// truncated memory-mapped input.
std::uint32_t read_u32_from_memory(const std::uint8_t*& cursor, const std::uint8_t* end) {
    if (static_cast<std::size_t>(end - cursor) < sizeof(std::uint32_t)) {
        throw std::runtime_error("mapping file is truncated while reading u32");
    }

    const std::uint32_t value = static_cast<std::uint32_t>(cursor[0]) |
                                (static_cast<std::uint32_t>(cursor[1]) << 8U) |
                                (static_cast<std::uint32_t>(cursor[2]) << 16U) |
                                (static_cast<std::uint32_t>(cursor[3]) << 24U);
    cursor += sizeof(std::uint32_t);
    return value;
}

// Narrows value for the on-disk format and uses field to describe overflow.
std::uint32_t checked_u32(std::size_t value, const char* field) {
    if (value > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error(std::string(field) + " exceeds MAP1 u32 limit");
    }
    return static_cast<std::uint32_t>(value);
}

// Persists data to path using the versioned binary format and an atomic file
// replacement so readers never observe a partial mapping.
void save_mapping_file(const std::string& path, const RequestMap& data) {
    std::string tmp_template = path + ".tmp.XXXXXX";
    std::vector<char> tmp_buffer(tmp_template.begin(), tmp_template.end());
    tmp_buffer.push_back('\0');

    UniqueFd tmp_file(::mkostemp(tmp_buffer.data(), O_CLOEXEC));
    if (!tmp_file) {
        throw std::runtime_error("failed to create temporary mapping file: " + path);
    }
    const std::string tmp_path(tmp_buffer.data());

    try {
        write_u32_fd(tmp_file.get(), tcp_common::kMappingFileMagic);
        write_u32_fd(tmp_file.get(), tcp_common::kMappingFileVersion);
        if (data.size() > tcp_common::kMaxMappingEntries) {
            throw std::runtime_error("mapping entry count exceeds configured limit");
        }
        write_u32_fd(tmp_file.get(), checked_u32(data.size(), "mapping entry count"));

        for (const auto& [request, response] : data) {
            write_u32_fd(tmp_file.get(), checked_u32(request.size(), "request length"));
            if (!request.empty()) {
                write_all_fd(tmp_file.get(), request.data(), request.size());
            }

            write_u32_fd(tmp_file.get(), checked_u32(response.payload.size(), "payload length"));
            if (!response.payload.empty()) {
                write_all_fd(tmp_file.get(), response.payload.data(), response.payload.size());
            }
        }

        if (::fsync(tmp_file.get()) < 0) {
            throw std::runtime_error(std::string("fsync failed for mapping temp file: ") +
                                     std::strerror(errno));
        }
        if (tmp_file.close() < 0) {
            throw std::runtime_error(std::string("close failed for mapping temp file: ") +
                                     std::strerror(errno));
        }

        if (::rename(tmp_path.c_str(), path.c_str()) < 0) {
            throw std::runtime_error(std::string("atomic rename failed for mapping file: ") +
                                     std::strerror(errno));
        }
        const std::filesystem::path parent = std::filesystem::path(path).parent_path();
        const std::string parent_path = parent.empty() ? "." : parent.string();
        UniqueFd directory_fd(
            ::open(parent_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        if (!directory_fd) {
            throw std::runtime_error(std::string("open mapping parent directory failed: ") +
                                     std::strerror(errno));
        }
        const int fsync_result = ::fsync(directory_fd.get());
        const int fsync_errno = errno;
        if (fsync_result < 0) {
            throw std::runtime_error(std::string("fsync mapping parent directory failed: ") +
                                     std::strerror(fsync_errno));
        }
    } catch (...) {
        ::unlink(tmp_path.c_str());
        throw;
    }
}

void save_request_keys_file(const std::string& path,
                            const std::vector<std::string_view>& keys) {
    std::string tmp_template = path + ".tmp.XXXXXX";
    std::vector<char> tmp_buffer(tmp_template.begin(), tmp_template.end());
    tmp_buffer.push_back('\0');

    UniqueFd tmp_file(::mkostemp(tmp_buffer.data(), O_CLOEXEC));
    if (!tmp_file) {
        throw std::runtime_error("failed to create temporary request-keys file: " + path);
    }
    const std::string tmp_path(tmp_buffer.data());

    try {
        for (const std::string_view key : keys) {
            if (key.empty() || key.size() > tcp_common::kMaxRequestKeySize) {
                throw std::runtime_error("request key length is outside the supported range");
            }
            if (key.find_first_of("\r\n") != std::string_view::npos ||
                key.find('\0') != std::string_view::npos) {
                throw std::runtime_error("request key contains a character invalid in a text key file");
            }
            write_all_fd(tmp_file.get(), key.data(), key.size());
            constexpr char newline = '\n';
            write_all_fd(tmp_file.get(), &newline, 1);
        }

        if (::fsync(tmp_file.get()) < 0) {
            throw std::runtime_error(std::string("fsync failed for request-keys temp file: ") +
                                     std::strerror(errno));
        }
        if (tmp_file.close() < 0) {
            throw std::runtime_error(std::string("close failed for request-keys temp file: ") +
                                     std::strerror(errno));
        }
        if (::rename(tmp_path.c_str(), path.c_str()) < 0) {
            throw std::runtime_error(std::string("atomic rename failed for request-keys file: ") +
                                     std::strerror(errno));
        }

        const std::filesystem::path parent = std::filesystem::path(path).parent_path();
        const std::string parent_path = parent.empty() ? "." : parent.string();
        UniqueFd directory_fd(::open(parent_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        if (!directory_fd) {
            throw std::runtime_error(std::string("open request-keys parent directory failed: ") +
                                     std::strerror(errno));
        }
        if (::fsync(directory_fd.get()) < 0) {
            throw std::runtime_error(std::string("fsync request-keys parent directory failed: ") +
                                     std::strerror(errno));
        }
    } catch (...) {
        ::unlink(tmp_path.c_str());
        throw;
    }
}

// Opens path read-only and holds a shared advisory lock while its mapping is
// inspected or loaded.
UniqueFd open_locked_mapping_file(const std::string& path) {
    UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!fd) {
        throw std::runtime_error("failed to open mapping file for reading: " + path);
    }
    if (::flock(fd.get(), LOCK_SH) < 0) {
        const int lock_errno = errno;
        throw std::runtime_error("failed to acquire shared lock on mapping file: " +
                                 path + ": " + std::strerror(lock_errno));
    }
    return fd;
}

// Validates and loads the mapping at path through descriptor reads into owned
// request and response buffers.
RequestMap load_mapping_file(const std::string& path) {
    // Open -> lock -> fstat removes the size-check TOCTOU window for this loader.
    UniqueFd mapping_fd = open_locked_mapping_file(path);

    struct stat st {};
    if (::fstat(mapping_fd.get(), &st) < 0) {
        const int stat_errno = errno;
        throw std::runtime_error(std::string("fstat failed: ") + std::strerror(stat_errno));
    }
    if (st.st_size <= 0) {
        throw std::runtime_error("mapping file is empty: " + path);
    }
    validate_mapping_file_size(static_cast<std::size_t>(st.st_size), path);

    if (::lseek(mapping_fd.get(), 0, SEEK_SET) < 0) {
        const int seek_errno = errno;
        throw std::runtime_error(std::string("lseek failed: ") + std::strerror(seek_errno));
    }

    const std::uint32_t magic =
        read_u32_fd(mapping_fd.get(), "reading mapping header magic");
    const std::uint32_t version =
        read_u32_fd(mapping_fd.get(), "reading mapping header version");
    validate_mapping_file_header(magic, version);

    const std::uint32_t count =
        read_u32_fd(mapping_fd.get(), "reading mapping entry count");
    if (count > tcp_common::kMaxMappingEntries) {
        throw std::runtime_error(
            "mapping file entry count exceeds limit: " + std::to_string(count));
    }

    RequestMap data;
    data.reserve(count);

    for (std::uint32_t i = 0; i < count; ++i) {
        const std::uint32_t req_len =
            read_u32_fd(mapping_fd.get(), "reading request length");
        if (req_len == 0 || req_len > kMaxRequestSize) {
            throw std::runtime_error("mapping file request length out of range [1, " +
                                     std::to_string(kMaxRequestSize) + "]");
        }

        std::string req(req_len, '\0');
        read_exact_fd(mapping_fd.get(), req.data(), req_len, "reading request string");

        const std::uint32_t payload_len =
            read_u32_fd(mapping_fd.get(), "reading payload length");
        if (payload_len < tcp_common::kMinPayloadSize ||
            payload_len > tcp_common::kMaxPayloadSize) {
            throw std::runtime_error("mapping file payload size out of allowed range " +
                                     payload_range_string());
        }

        auto [it, inserted] = data.emplace(std::move(req), ClientResponse{});
        if (!inserted) {
            throw std::runtime_error("duplicate request key found in mapping file");
        }
        std::vector<std::uint8_t>& payload = it->second.payload;
        payload.resize(payload_len);
        read_exact_fd(mapping_fd.get(), payload.data(), payload_len,
                      "reading payload bytes");
    }

    std::uint8_t trailing = 0;
    ssize_t trailing_n = -1;
    for (;;) {
        trailing_n = ::read(mapping_fd.get(), &trailing, 1);
        if (trailing_n < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    if (trailing_n < 0) {
        throw std::runtime_error(std::string("read failed while checking trailing bytes: ") +
                                 std::strerror(errno));
    }
    if (trailing_n > 0) {
        throw std::runtime_error("mapping file has unexpected trailing bytes");
    }

    return data;
}

// Owns an anonymous snapshot populated from fd and made read-only. The mapping
// has no live dependency on the source inode, so later truncation cannot SIGBUS.
class MappedRegion {
public:
    MappedRegion(int fd, std::size_t length) : length_(length) {
        addr_ = ::mmap(nullptr, length_, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (addr_ == MAP_FAILED) {
            throw std::runtime_error(std::string("mmap failed: ") + std::strerror(errno));
        }
        try {
            read_exact_fd(fd, addr_, length_, "creating immutable mapping snapshot");
            if (::mprotect(addr_, length_, PROT_READ) < 0) {
                throw std::runtime_error(std::string("mprotect failed: ") +
                                         std::strerror(errno));
            }
        } catch (...) {
            ::munmap(addr_, length_);
            addr_ = MAP_FAILED;
            throw;
        }
    }

    ~MappedRegion() noexcept {
        if (addr_ != MAP_FAILED) {
            ::munmap(addr_, length_);
        }
    }

    MappedRegion(const MappedRegion&) = delete;
    MappedRegion& operator=(const MappedRegion&) = delete;
    MappedRegion(MappedRegion&&) = delete;
    MappedRegion& operator=(MappedRegion&&) = delete;

    const std::uint8_t* data() const noexcept {
        return static_cast<const std::uint8_t*>(addr_);
    }

    std::size_t size() const noexcept {
        return length_;
    }

private:
    void* addr_ = MAP_FAILED;
    std::size_t length_ = 0;
};

// Keeps an mmap alive alongside an index whose keys and payloads borrow bytes
// directly from that mapped region.
struct MMapViewLoadedData {
    std::unique_ptr<MappedRegion> region;
    MappedRequestMap data;
};

// Provides one lookup interface over either fully owned mappings or zero-copy
// mmap-backed views, and owns whichever backing storage is active.
class MappingStore {
public:
    // Replaces current storage with an owned request map supplied in data.
    void set_owned(RequestMap&& data) {
        mapped_view_data_.clear();
        mapped_region_.reset();
        owned_data_ = std::move(data);
    }

    // Replaces current storage with loaded's mmap-backed region and index.
    void set_mmap_view(MMapViewLoadedData&& loaded) {
        owned_data_.clear();
        owned_data_.rehash(0);
        // Borrowed keys and payloads must be destroyed before their old mapped
        // region is released, even though their destructors are currently trivial.
        mapped_view_data_.clear();
        mapped_view_data_.rehash(0);
        mapped_region_.reset();
        mapped_region_ = std::move(loaded.region);
        mapped_view_data_ = std::move(loaded.data);
    }

    // Returns the entry count for the active owned or mmap-backed representation.
    std::size_t size() const {
        if (mapped_region_ != nullptr) {
            return mapped_view_data_.size();
        }
        return owned_data_.size();
    }

    // Finds request without allocation and returns its payload byte range.
    std::optional<ResponseBufferView> find(std::string_view request) const {
        if (mapped_region_ != nullptr) {
            auto it = mapped_view_data_.find(request);
            if (it == mapped_view_data_.end()) {
                return std::nullopt;
            }
            return ResponseBufferView{it->second.data(), it->second.size()};
        }

        auto it = owned_data_.find(request);
        if (it == owned_data_.end()) {
            return std::nullopt;
        }
        return ResponseBufferView{it->second.payload.data(), it->second.payload.size()};
    }

    // Writes at most max_keys sorted request keys to output_path for clients.
    void export_request_keys(std::size_t max_keys,
                             const std::string& output_path) const {
        const std::size_t limit = std::min(max_keys, size());
        std::vector<std::string_view> exported_keys;
        exported_keys.reserve(limit);

        if (mapped_region_ != nullptr) {
            for (const auto& [request, payload] : mapped_view_data_) {
                (void)payload;
                if (exported_keys.size() >= limit) {
                    break;
                }
                exported_keys.push_back(request);
            }
        } else {
            for (const auto& [request, response] : owned_data_) {
                (void)response;
                if (exported_keys.size() >= limit) {
                    break;
                }
                exported_keys.push_back(request);
            }
        }

        save_request_keys_file(output_path, exported_keys);
        std::cout << "Exported " << exported_keys.size() << " request key(s) to "
                  << output_path << ". Copy this file to the directory where "
                     "tcp_client will be executed."
                  << std::endl;
    }

private:
    RequestMap owned_data_;
    std::unique_ptr<MappedRegion> mapped_region_;
    MappedRequestMap mapped_view_data_;
};

MappingStore g_mapping_store;

// Installs data as the process-wide owned mapping after validation/loading.
void set_owned_mapping_data(RequestMap&& data) {
    g_mapping_store.set_owned(std::move(data));
}

// Installs loaded's zero-copy index and mapped backing region globally.
void set_mmap_view_mapping_data(MMapViewLoadedData&& loaded) {
    g_mapping_store.set_mmap_view(std::move(loaded));
}

// Returns the number of request/response entries in the active mapping store.
std::size_t request_mapping_size() {
    return g_mapping_store.size();
}

// Parses an anonymous read-only snapshot and copies keys and payloads into an
// owned RequestMap before releasing the snapshot.
RequestMap load_mapping_file_mmap(const std::string& path) {
    UniqueFd fd = open_locked_mapping_file(path);

    struct stat st {};
    if (::fstat(fd.get(), &st) < 0) {
        throw std::runtime_error(std::string("fstat failed: ") + std::strerror(errno));
    }
    if (st.st_size <= 0) {
        throw std::runtime_error("mapping file is empty: " + path);
    }

    const std::size_t file_size = static_cast<std::size_t>(st.st_size);
    validate_mapping_file_size(file_size, path);
    MappedRegion map(fd.get(), file_size);

    const std::uint8_t* cursor = map.data();
    const std::uint8_t* end = map.data() + map.size();

    const std::uint32_t magic = read_u32_from_memory(cursor, end);
    const std::uint32_t version = read_u32_from_memory(cursor, end);
    validate_mapping_file_header(magic, version);

    const std::uint32_t count = read_u32_from_memory(cursor, end);
    if (count > tcp_common::kMaxMappingEntries) {
        throw std::runtime_error(
            "mapping file entry count exceeds limit: " + std::to_string(count));
    }

    RequestMap data;
    data.reserve(count);

    for (std::uint32_t i = 0; i < count; ++i) {
        const std::uint32_t req_len = read_u32_from_memory(cursor, end);
        if (req_len == 0 || req_len > kMaxRequestSize) {
            throw std::runtime_error("mapping file request length out of range [1, " +
                                     std::to_string(kMaxRequestSize) + "]");
        }
        if (static_cast<std::size_t>(end - cursor) < req_len) {
            throw std::runtime_error("mapping file is truncated while reading request string");
        }

        std::string req(reinterpret_cast<const char*>(cursor), req_len);
        cursor += req_len;

        const std::uint32_t payload_len = read_u32_from_memory(cursor, end);
        if (payload_len < tcp_common::kMinPayloadSize || payload_len > tcp_common::kMaxPayloadSize) {
            throw std::runtime_error("mapping file payload size out of allowed range " +
                                     payload_range_string());
        }
        if (static_cast<std::size_t>(end - cursor) < payload_len) {
            throw std::runtime_error("mapping file is truncated while reading payload bytes");
        }

        auto [it, inserted] = data.emplace(std::move(req), ClientResponse{});
        if (!inserted) {
            throw std::runtime_error("duplicate request key found in mapping file");
        }
        std::vector<std::uint8_t>& payload = it->second.payload;
        payload.resize(payload_len);
        if (payload_len > 0) {
            std::memcpy(payload.data(), cursor, payload_len);
            cursor += payload_len;
        }
    }

    if (cursor != end) {
        throw std::runtime_error("mapping file has unexpected trailing bytes");
    }

    return data;
}

// Maps path and builds a zero-copy index of string and payload views whose
// lifetime is tied to the returned MMapViewLoadedData.
MMapViewLoadedData load_mapping_file_mmap_view(const std::string& path) {
    UniqueFd fd = open_locked_mapping_file(path);

    struct stat st {};
    if (::fstat(fd.get(), &st) < 0) {
        throw std::runtime_error(std::string("fstat failed: ") + std::strerror(errno));
    }
    if (st.st_size <= 0) {
        throw std::runtime_error("mapping file is empty: " + path);
    }

    const std::size_t file_size = static_cast<std::size_t>(st.st_size);
    validate_mapping_file_size(file_size, path);
    auto region = std::make_unique<MappedRegion>(fd.get(), file_size);

    const std::uint8_t* cursor = region->data();
    const std::uint8_t* end = region->data() + region->size();

    const std::uint32_t magic = read_u32_from_memory(cursor, end);
    const std::uint32_t version = read_u32_from_memory(cursor, end);
    validate_mapping_file_header(magic, version);

    const std::uint32_t count = read_u32_from_memory(cursor, end);
    if (count > tcp_common::kMaxMappingEntries) {
        throw std::runtime_error(
            "mapping file entry count exceeds limit: " + std::to_string(count));
    }

    MappedRequestMap data;
    data.reserve(count);

    for (std::uint32_t i = 0; i < count; ++i) {
        const std::uint32_t req_len = read_u32_from_memory(cursor, end);
        if (req_len == 0 || req_len > kMaxRequestSize) {
            throw std::runtime_error("mapping file request length out of range [1, " +
                                     std::to_string(kMaxRequestSize) + "]");
        }
        if (static_cast<std::size_t>(end - cursor) < req_len) {
            throw std::runtime_error("mapping file is truncated while reading request string");
        }

        const char* request_ptr = reinterpret_cast<const char*>(cursor);
        std::string_view request_key(request_ptr, req_len);
        cursor += req_len;

        const std::uint32_t payload_len = read_u32_from_memory(cursor, end);
        if (payload_len < tcp_common::kMinPayloadSize || payload_len > tcp_common::kMaxPayloadSize) {
            throw std::runtime_error("mapping file payload size out of allowed range " +
                                     payload_range_string());
        }
        if (static_cast<std::size_t>(end - cursor) < payload_len) {
            throw std::runtime_error("mapping file is truncated while reading payload bytes");
        }

        const std::uint8_t* payload_ptr = cursor;
        cursor += payload_len;
        auto [it, inserted] = data.emplace(request_key, MappedPayloadView(payload_ptr, payload_len));
        if (!inserted) {
            throw std::runtime_error("duplicate request key found in mapping file");
        }
    }

    if (cursor != end) {
        throw std::runtime_error("mapping file has unexpected trailing bytes");
    }

    return MMapViewLoadedData{std::move(region), std::move(data)};
}

// Generates or loads path according to force_regenerate and load_mode;
// mapping_entries controls the generated entry count when regeneration is set.
void initialize_request_mapping(const std::string& path,
                                bool force_regenerate,
                                MappingLoadMode load_mode,
                                std::size_t mapping_entries) {
    if (!std::filesystem::exists(path) && !force_regenerate) {
        throw std::runtime_error(
            "mapping file does not exist: " + path +
            "; create it explicitly with --regen-mapping");
    }

    if (force_regenerate) {
        std::size_t generated_count = 0;
        {
            RequestMap generated = generate_random_requests(mapping_entries);
            generated_count = generated.size();
            save_mapping_file(path, generated);

            if (load_mode != MappingLoadMode::MMapView) {
                set_owned_mapping_data(std::move(generated));
                std::cout << "Generated mapping file with " << request_mapping_size()
                          << " random entries at " << path << std::endl;
                return;
            }
        }

        if (load_mode == MappingLoadMode::MMapView) {
            set_mmap_view_mapping_data(load_mapping_file_mmap_view(path));
            std::cout << "Generated mapping file with " << generated_count
                      << " random entries at " << path
                      << " and loaded using an immutable mmap-view snapshot"
                      << std::endl;
            return;
        }
    }

    if (load_mode == MappingLoadMode::MMap) {
        set_owned_mapping_data(load_mapping_file_mmap(path));
        std::cout << "Loaded " << request_mapping_size() << " mapping entries from " << path
                  << " using mmap" << std::endl;
        return;
    }

    if (load_mode == MappingLoadMode::MMapView) {
        set_mmap_view_mapping_data(load_mapping_file_mmap_view(path));
        std::cout << "Loaded " << request_mapping_size() << " mapping entries from " << path
                  << " using an immutable mmap-view snapshot" << std::endl;
        return;
    }

    set_owned_mapping_data(load_mapping_file(path));
    std::cout << "Loaded " << request_mapping_size() << " mapping entries from " << path
              << " using ifstream" << std::endl;
}

// Exports up to max_keys from the active mapping to newline-delimited output_path.
void print_request_keys(std::size_t max_keys, const std::string& output_path) {
    g_mapping_store.export_request_keys(max_keys, output_path);
}

// Looks up request in the active store and returns a non-owning payload view.
std::optional<ResponseBufferView> find_client_response(std::string_view request) {
    return g_mapping_store.find(request);
}
