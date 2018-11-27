#include <memory>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "table_scan.hpp"
#include "types.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto table = _input_table_left();
  const auto table_scan_impl = make_unique_by_data_type<BaseTableScanImpl, TableScanImpl>(
      table->column_type(_column_id), table, _column_id, _scan_type, _search_value);
  return table_scan_impl->execute();
}

template <typename T>
TableScan::TableScanImpl<T>::TableScanImpl(const std::shared_ptr<const Table> table, ColumnID column_id,
                                           const ScanType scan_type, const AllTypeVariant search_value)
    : _table(table), _column_id(column_id), _scan_type(scan_type), _search_value(type_cast<T>(search_value)) {}

template <typename T>
const std::shared_ptr<const Table> TableScan::TableScanImpl<T>::execute() const {
  // First create an empty result_table with the column definitions copied from the input _table.
  // This does not add segments to the table.
  const auto result_table = std::make_shared<Table>();
  for (auto column_idx = ColumnID(0); column_idx < _table->column_count(); ++column_idx) {
    result_table->add_column_definition(_table->column_name(column_idx), _table->column_type(column_idx));
  }

  // During the execution of this method, we will create multiple ReferenceSegments. Because a ReferenceSegment can only
  // reference rows of a single table, we create a new ReferenceSegment whenever the nth segment of the incoming table
  // references a different table than the (n-1)th segment.
  // The result_pos_list holds the PosList to of the currently being filled ReferenceSegment.
  auto result_pos_list = std::make_shared<PosList>();

  // This points to the table which was referenced by the last ReferenceSegment we added to the result table.
  // It allows us to detect when we need to add another ReferenceSegment, because the referenced table changed.
  std::shared_ptr<const Table> last_referenced_table = nullptr;

  // Here, we iterate through all chunks of the input table. Within the loop, we only retrieve one segment per chunk,
  // more specifically, the segment corresponding to the column we want to filter on.
  for (auto chunk_index = ChunkID(0); chunk_index < _table->chunk_count(); ++chunk_index) {
    const auto& chunk = _table->get_chunk(chunk_index);
    const auto& segment_to_scan = chunk.get_segment(_column_id);

    // The following three blocks work similarly. They first check the type of the segment_to_scan. Then, they add
    // a new chunk including ReferenceSegments to the result table if the last referenced table is different from the
    // current table and at least one valid row has been found.
    // There is a slight difference between ValueSegments/DictionarySegments and ReferenceSegments: The former compare
    // the _table with the last_referenced_table, whereas the latter needs to compare the table referenced by the
    // ReferenceSegment with the last_referenced_table.

    const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment_to_scan);
    if (value_segment != nullptr) {
      if (last_referenced_table != nullptr && last_referenced_table != _table && !result_pos_list->empty()) {
        _add_chunk(result_table, result_pos_list, last_referenced_table);
      }
      last_referenced_table = _table;
      _scan_segment(chunk_index, result_pos_list, value_segment);
      continue;
    }

    const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment_to_scan);
    if (dict_segment != nullptr) {
      if (last_referenced_table != nullptr && last_referenced_table != _table && !result_pos_list->empty()) {
        _add_chunk(result_table, result_pos_list, last_referenced_table);
      }
      last_referenced_table = _table;
      _scan_segment(chunk_index, result_pos_list, dict_segment);
      continue;
    }

    const auto& reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment_to_scan);
    if (reference_segment != nullptr) {
      if (last_referenced_table != nullptr && last_referenced_table != reference_segment->referenced_table() &&
          !result_pos_list->empty()) {
        _add_chunk(result_table, result_pos_list, last_referenced_table);
      }
      last_referenced_table = reference_segment->referenced_table();
      _scan_segment(result_pos_list, reference_segment);
      continue;
    }

    Fail("Type mismatch: Cannot cast table segments to type of search_value.");
  }

  if (!result_pos_list->empty()) {
    _add_chunk(result_table, result_pos_list, last_referenced_table);
  } else {
    auto& last_chunk = result_table->get_chunk(ChunkID(result_table->chunk_count() - 1));
    if (last_chunk.column_count() == 0) {
      // This only happens when no result has been found. In this case, the result_table already has the column
      // definitions set correctly as well as an empty chunk. We need to make sure to add one empty ValueSegment per
      // column now.
      DebugAssert(result_table->chunk_count() == 1, "Only when the table has just one chunk it can be empty.");
      for (auto column_idx = ColumnID(0); column_idx < result_table->column_count(); ++column_idx) {
        last_chunk.add_segment(
            make_shared_by_data_type<BaseSegment, ValueSegment>(result_table->column_type(column_idx)));
      }
    }
  }

  return result_table;
}

template <typename T>
void TableScan::TableScanImpl<T>::_add_chunk(const std::shared_ptr<Table>& result_table,
                                             std::shared_ptr<PosList>& result_pos_list,
                                             const std::shared_ptr<const Table>& referenced_table) const {
  // The last referenced table is different than the current table and at least one row valid row has been found.
  // Thus, we need to add a new chunk to the result_table and add one ReferenceSegment per column to it.
  Chunk result_chunk;
  for (auto column_idx = ColumnID(0); column_idx < referenced_table->column_count(); ++column_idx) {
    const auto segment = std::make_shared<ReferenceSegment>(referenced_table, column_idx, result_pos_list);
    result_chunk.add_segment(segment);
  }
  result_table->emplace_chunk(std::move(result_chunk));
  result_pos_list = std::make_shared<PosList>();
}

template <typename T>
void TableScan::TableScanImpl<T>::_scan_segment(const ChunkID current_chunk_id, std::shared_ptr<PosList> pos_list,
                                                const std::shared_ptr<ValueSegment<T>> segment) const {
  const auto& values = segment->values();
  for (auto i = ChunkOffset(0); i < values.size(); ++i) {
    if (_matches_search_value(values[i])) {
      auto row_id = RowID();
      row_id.chunk_offset = i;
      row_id.chunk_id = current_chunk_id;
      pos_list->emplace_back(std::move(row_id));
    }
  }
}

template <typename T>
void TableScan::TableScanImpl<T>::_scan_segment(const ChunkID current_chunk_id, std::shared_ptr<PosList> pos_list,
                                                const std::shared_ptr<DictionarySegment<T>> segment) const {
//  const auto& value_id_pair = _matches_value_id(ValueID(), segment);
  const auto& attribute_vector = segment->attribute_vector();
  for (size_t i = 0; i < segment->size(); ++i) {
    if(_matches_value_id(attribute_vector->get(i), segment)) {
      auto row_id = RowID();
      row_id.chunk_offset = ChunkOffset(i);
      row_id.chunk_id = current_chunk_id;
      pos_list->emplace_back(std::move(row_id));
    }
  }
}

template <typename T>
bool TableScan::TableScanImpl<T>::_matches_value_id(const ValueID& valueID,
        const std::shared_ptr<DictionarySegment<T>> segment) const {
  switch (_scan_type) {
    case ScanType::OpEquals: {
      return valueID >= segment->lower_bound(_search_value) && valueID < segment->upper_bound(_search_value);
    }
    case ScanType::OpNotEquals: {
      return valueID < segment->lower_bound(_search_value) || valueID >= segment->upper_bound(_search_value);
    }
    case ScanType::OpGreaterThan: {
      return valueID >= segment->upper_bound(_search_value) && valueID < ValueID(segment->dictionary()->size());
    }
    case ScanType::OpGreaterThanEquals: {
      return valueID >= segment->lower_bound(_search_value) && valueID < ValueID(segment->dictionary()->size());
    }
    case ScanType::OpLessThan: {
      return valueID < segment->lower_bound(_search_value) && valueID >= ValueID(0);
    }
    case ScanType::OpLessThanEquals: {
      return valueID < segment->upper_bound(_search_value) && valueID >= ValueID(0);
    }
    default: { Fail("Unknown scan type operator"); }
  }
  return false;
}

template <typename T>
void TableScan::TableScanImpl<T>::_scan_segment(std::shared_ptr<PosList> pos_list,
                                                const std::shared_ptr<ReferenceSegment> segment) const {
  const auto& ref_table = segment->referenced_table();
  const auto& ref_pos_list = segment->pos_list();

  for (const auto& pos : *ref_pos_list) {
    const auto chunk_id = pos.chunk_id;
    const auto chunk_offset = pos.chunk_offset;

    const auto& referenced_chunk = ref_table->get_chunk(chunk_id);
    const auto& referenced_segment = referenced_chunk.get_segment(segment->referenced_column_id());

    bool match = false;

    const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(referenced_segment);
    const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(referenced_segment);
    if (value_segment != nullptr) {
      const auto& value = value_segment->values()[chunk_offset];
      match = _matches_search_value(value);
    } else if (dict_segment != nullptr) {
      const auto& value = dict_segment->get(chunk_offset);
      match = _matches_search_value(value);
    } else {
      Fail("ReferenceSegment did not point to either a ValueSegment or a DictionarySegment.");
    }

    if (match) {
      auto row_id = RowID();
      row_id.chunk_offset = chunk_offset;
      row_id.chunk_id = chunk_id;
      pos_list->emplace_back(std::move(row_id));
    }
  }
}

template <typename T>
bool TableScan::TableScanImpl<T>::_matches_search_value(const T& value) const {
  switch (_scan_type) {
    case ScanType::OpEquals: {
      return value == _search_value;
    }
    case ScanType::OpNotEquals: {
      return value != _search_value;
    }
    case ScanType::OpGreaterThan: {
      return value > _search_value;
    }
    case ScanType::OpGreaterThanEquals: {
      return value >= _search_value;
    }
    case ScanType::OpLessThan: {
      return value < _search_value;
    }
    case ScanType::OpLessThanEquals: {
      return value <= _search_value;
    }
    default: { Fail("Unknown scan type operator"); }
  }
  return false;
}

}  // namespace opossum
