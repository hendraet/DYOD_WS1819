#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  DebugAssert(!has_table(name), "can not add table: table name is already in use");
  _table_mapping.emplace(std::make_pair(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  DebugAssert(has_table(name), "can not drop table: table name is not in use");
  _table_mapping.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  DebugAssert(has_table(name), "can not retrieve table: table name is not in use");
  return _table_mapping.find(name)->second;
}

bool StorageManager::has_table(const std::string& name) const {
  return _table_mapping.find(name) != _table_mapping.end();
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  for (auto table_pair : _table_mapping) {
    table_names.push_back(table_pair.first);
  }
  return table_names;
}

const size_t C_PRINT_COLUMN_WIDTH = 25;

void StorageManager::print(std::ostream& out) const {
  print_header(out);
  for (auto it : _table_mapping) {
    auto table_name = it.first;
    auto table = it.second;

    print_table(out, table_name, table);
  }
}

void StorageManager::print_header(std::ostream& out) const {
  out << pad_right("table_name", C_PRINT_COLUMN_WIDTH);
  out << pad_right("#columns", C_PRINT_COLUMN_WIDTH);
  out << pad_right("#rows", C_PRINT_COLUMN_WIDTH);
  out << pad_right("#chunks", C_PRINT_COLUMN_WIDTH);
  out << std::endl;
}

void StorageManager::print_table(std::ostream& out, const std::string& table_name, std::shared_ptr<Table> table) const {
  out << pad_right(table_name, C_PRINT_COLUMN_WIDTH);
  out << pad_right(std::to_string(table->column_count()), C_PRINT_COLUMN_WIDTH);
  out << pad_right(std::to_string(table->row_count()), C_PRINT_COLUMN_WIDTH);
  out << pad_right(std::to_string(table->chunk_count()), C_PRINT_COLUMN_WIDTH);
  out << std::endl;
}

std::string StorageManager::pad_right(std::string input, size_t total_width, char pad_with) const {
  if (input.size() < total_width) {
    input.insert(input.end(), total_width - input.size(), pad_with);
  }

  return input;
}

void StorageManager::reset() { get() = StorageManager(); }

}  // namespace opossum
