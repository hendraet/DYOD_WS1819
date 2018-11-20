#include <memory>

#include "reference_segment.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const opossum::Table> referenced_table,
                                   const opossum::ColumnID referenced_column_id,
                                   const std::shared_ptr<const opossum::PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {}

const AllTypeVariant ReferenceSegment::operator[](const size_t i) const {
  PerformanceWarning("operator[] used");

  const auto& row_id = _pos_list->at(i);
  const auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);
  const auto& segment = chunk.get_segment(_referenced_column_id);

  return segment->operator[](row_id.chunk_offset);
}

size_t ReferenceSegment::size() const { return _pos_list->size(); }

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _pos_list; }

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

}  //  namespace opossum
