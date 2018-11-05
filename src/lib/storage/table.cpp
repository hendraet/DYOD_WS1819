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

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size{chunk_size} { open_new_chunk(); }

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(chunk_count() == ChunkID{1}, "not exactly 1 chnunk in table");
  DebugAssert(row_count() == 0, "can not add column, since there is already a row");
  DebugAssert(std::find(_column_names.begin(), _column_names.end(), name) == _column_names.end(),
              "column name already exists");

  _column_names.push_back(name);
  _column_types.push_back(type);
  _chunks.at(0)->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (should_open_new_chunk()) {
    open_new_chunk();
  }

  _chunks.back()->append(values);
}

bool Table::should_open_new_chunk() const {
  DebugAssert(chunk_count() >= ChunkID{1}, "less than 1 chunk in table");

  return _chunks.back()->size() >= chunk_size();
}

void Table::open_new_chunk() {
  auto chunk = std::make_shared<Chunk>();
  for (const auto& column_type : _column_types) {
    chunk->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(column_type));
  }

  _chunks.push_back(chunk);
}

uint16_t Table::column_count() const {
  DebugAssert(_column_names.size() == _column_types.size(), "Size of names and types does not match");
  return static_cast<uint16_t>(_column_names.size());
}

uint64_t Table::row_count() const {
  int num_of_filled_chunks = chunk_count() - 1;
  uint64_t row_count = num_of_filled_chunks * _chunk_size + _chunks.back()->size();

  return row_count;
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

Chunk& Table::get_chunk(ChunkID chunk_id) { return *_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *_chunks.at(chunk_id); }

void Table::compress_chunk(ChunkID chunk_id) { throw std::runtime_error("Implement Table::compress_chunk"); }

}  // namespace opossum
