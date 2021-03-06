#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "dictionary_segment.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size{chunk_size} { create_new_chunk(); }

void Table::add_column_definition(const std::string& name, const std::string& type) {
  _column_names.push_back(name);
  _column_types.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(chunk_count() == ChunkID{1}, "not exactly 1 chnunk in table");
  DebugAssert(row_count() == 0, "can not add column, since there is already a row");
  DebugAssert(std::find(_column_names.begin(), _column_names.end(), name) == _column_names.end(),
              "column name already exists");

  add_column_definition(name, type);
  _chunks.front().add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_is_latest_chunk_full()) {
    create_new_chunk();
  }

  _chunks.back().append(values);
}

bool Table::_is_latest_chunk_full() const {
  DebugAssert(chunk_count() >= ChunkID{1}, "less than 1 chunk in table");

  return _is_chunk_full(ChunkID{static_cast<uint32_t>(_chunks.size() - 1)});
}

bool Table::_is_chunk_full(const ChunkID chunk_id) const {
  const auto& chunk = _chunks.at(chunk_id);
  return chunk.size() >= chunk_size();
}

void Table::create_new_chunk() {
  Chunk chunk;
  for (const auto& column_type : _column_types) {
    chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(column_type));
  }

  _chunks.emplace_back(std::move(chunk));
}

uint16_t Table::column_count() const {
  DebugAssert(_column_names.size() == _column_types.size(), "Size of names and types does not match");
  return static_cast<uint16_t>(_column_names.size());
}

uint64_t Table::row_count() const {
  uint64_t count = 0;
  for (const auto& chunk : _chunks) {
    count += chunk.size();
  }

  return count;
}

ChunkID Table::chunk_count() const { return ChunkID{static_cast<const uint32_t&>(_chunks.size())}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto it = std::find(_column_names.begin(), _column_names.end(), column_name);
  DebugAssert(it != _column_names.end(), "Name is not in columns");

  const auto id = static_cast<const uint16_t>(std::distance(_column_names.begin(), it));

  return ColumnID{id};
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) {
  std::shared_lock<std::shared_mutex> lock(_chunks_mutex);
  return _chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  std::shared_lock<std::shared_mutex> lock(const_cast<std::shared_mutex&>(_chunks_mutex));
  return _chunks.at(chunk_id);
}

void Table::emplace_chunk(Chunk&& chunk) {
  if (_chunks.back().size() == 0) {
    _chunks.back() = std::move(chunk);
  } else {
    _chunks.emplace_back(std::move(chunk));
  }
}

void Table::compress_chunk(ChunkID chunk_id) {
  Assert(_is_chunk_full(chunk_id), "chunk is not full");

  const auto& uncompressed_chunk = _chunks.at(chunk_id);
  auto compressed_chunk = Chunk();
  for (ColumnID column_id = ColumnID{0}; column_id < uncompressed_chunk.column_count(); ++column_id) {
    const auto uncompressed_segment = uncompressed_chunk.get_segment(column_id);
    const auto compressed_segment =
        make_shared_by_data_type<BaseSegment, DictionarySegment>(column_type(column_id), uncompressed_segment);
    compressed_chunk.add_segment(compressed_segment);
  }

  std::unique_lock<std::shared_mutex> lock(_chunks_mutex);
  _chunks.at(chunk_id) = std::move(compressed_chunk);
}

}  // namespace opossum
