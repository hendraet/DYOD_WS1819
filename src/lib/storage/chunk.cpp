#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  DebugAssert(column_count() < std::numeric_limits<std::uint16_t>::max(), "max number of segments reached");

  _columns.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(), "Number of values doesn't match number of columns.");

  for (size_t i = 0; i < values.size(); ++i) {
    _columns[i]->append(values[i]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const { return _columns.at(column_id); }

uint16_t Chunk::column_count() const { return static_cast<uint16_t>(_columns.size()); }

uint32_t Chunk::size() const { return column_count() > 0 ? static_cast<uint32_t>(_columns[0]->size()) : 0; }

}  // namespace opossum
