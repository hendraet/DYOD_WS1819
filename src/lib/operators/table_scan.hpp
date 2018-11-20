#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "../storage/value_segment.hpp"

namespace opossum {

class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;


  class BaseTableScanImpl {
  public:
    BaseTableScanImpl() = default;
  };

  std::unique_ptr<BaseTableScanImpl> _table_scan_impl;

  template <typename T>
  class TableScanImpl : public BaseTableScanImpl {
  public:
    TableScanImpl(const std::shared_ptr<const Table> table, ColumnID column_id, const ScanType scan_type,
                      const AllTypeVariant search_value);

    std::shared_ptr<const Table> execute() const;

  protected:
    void _scan_segment(std::shared_ptr<Table> table, std::shared_ptr<ValueSegment<T>> segment) const;

    bool _matches_search_value(const T& value) const;

    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;
    const ScanType _scan_type;
    const T _search_value;
  };
};

}  // namespace opossum
