#include <memory>
#include <utility>
#include <vector>

#include "../resolve_type.hpp"
#include "../storage/chunk.hpp"
#include "../storage/dictionary_segment.hpp"
#include "../storage/reference_segment.hpp"
#include "../storage/table.hpp"
#include "../storage/value_segment.hpp"
#include "../types.hpp"
#include "table_scan.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

TableScan::~TableScan() {
  // TODO
}

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
    : _table(table), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

template <typename T>
std::shared_ptr<const Table> TableScan::TableScanImpl<T>::execute() {
  const auto result_table = std::make_shared<Table>();
  for (auto column_idx = ColumnID(0); column_idx < _table->column_count(); ++column_idx) {
    result_table->add_column_definition(_table->column_name(column_idx), _table->column_type(column_idx));
  }
  const auto result_pos_list = std::make_shared<PosList>();

  for (auto chunk_index = ChunkID(0); chunk_index < _table->chunk_count(); ++chunk_index) {
    const auto& chunk = _table->get_chunk(chunk_index);
    const auto& segment_to_scan = chunk.get_segment(_column_id);

    const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment_to_scan);
    if (value_segment != nullptr) {
      const auto matches = _scan_segment(value_segment);
      for (const auto matchedChunkOffset : matches) {
        auto row_id = RowID();
        row_id.chunk_offset = matchedChunkOffset;
        row_id.chunk_id = chunk_index;
        result_pos_list->emplace_back(std::move(row_id));
      }
      continue;
    }
    /*
    const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (dict_segment != nullptr) {
      // TODO: Smartly iterate the dictionary.

      continue;
    }
    const auto& reference_segment = std::dynamic_pointer_cast<ReferenceSegment<T>>(segment);
    if (reference_segment != nullptr) {

      continue;
    }
    */

    Fail("Type mismatch: Cannot cast table segments to type of search_value.");
  }

  Chunk result_chunk;
  for (auto column_idx = ColumnID(0); column_idx < _table->column_count(); ++column_idx) {
    const auto segment = std::make_shared<ReferenceSegment>(_table, column_idx, result_pos_list);
    result_chunk.add_segment(segment);
  }
  result_table->emplace_chunk(std::move(result_chunk));

  return result_table;
}

template <typename T>
const std::vector<ChunkOffset> TableScan::TableScanImpl<T>::_scan_segment(
    std::shared_ptr<ValueSegment<T>> segment) const {
  auto matchedRows = std::vector<ChunkOffset>();
  const auto& values = segment->values();
  for (size_t i = 0; i < values.size(); ++i) {
    if (_matches_search_value(values[i])) {
      matchedRows.emplace_back(i);
    }
  }
  return matchedRows;
}

template <typename T>
bool TableScan::TableScanImpl<T>::_matches_search_value(const T& value) const {
  const auto& search_value = type_cast<T>(_search_value);  // TODO: Don't type_cast every time!
  switch (_scan_type) {
    case ScanType::OpEquals: {
      return value == search_value;
    }
    case ScanType::OpNotEquals: {
      return value != search_value;
    }
    case ScanType::OpGreaterThan: {
      return value > search_value;
    }
    case ScanType::OpGreaterThanEquals: {
      return value >= search_value;
    }
    case ScanType::OpLessThan: {
      return value < search_value;
    }
    case ScanType::OpLessThanEquals: {
      return value <= search_value;
    }
    default: { Fail("Unknown scan type operator"); }
  }
  return false;  // TODO: Does this really need to be here?
}

}  // namespace opossum
