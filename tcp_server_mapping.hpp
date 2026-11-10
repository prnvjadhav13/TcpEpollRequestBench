// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for performance evaluation and educational purposes.
// See the repository LICENSE file for reuse terms.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

// Selects how mapping-file payloads are loaded into server memory.
enum class MappingLoadMode {
    Ifstream,
    MMap,
    MMapView
};

// Non-owning response bytes whose lifetime is managed by the mapping store.
struct ResponseBufferView {
    const std::uint8_t* data = nullptr;
    std::size_t size = 0;
};

// Loads or regenerates the request/response map using the selected loader.
void initialize_request_mapping(const std::string& path,
                                bool force_regen,
                                MappingLoadMode load_mode,
                                std::size_t mapping_entries);

// Writes up to max_keys request keys to a client-compatible text file.
void print_request_keys(std::size_t max_keys, const std::string& output_path);

// Looks up a request without copying its response payload.
[[nodiscard]] std::optional<ResponseBufferView>
find_client_response(std::string_view request);

// Returns the number of entries currently held by the process mapping store.
[[nodiscard]] std::size_t request_mapping_size();
