#pragma once

#include <type_cast.hpp>

#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_segment.hpp"
#include "fitted_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    auto unique_values = std::set<T>();

    for (size_t value_index = 0; value_index < base_segment->size(); ++value_index) {
      unique_values.emplace(type_cast<T>(base_segment->operator[](value_index)));
    }
    _dictionary = std::make_shared<std::vector<T>>(unique_values.cbegin(), unique_values.cend());
    _attribute_vector =
        make_shared_attribute_vector(base_segment->size(), ValueID{static_cast<uint32_t>(unique_values.size())});

    for (size_t value_index = 0; value_index < base_segment->size(); ++value_index) {
      const auto value = type_cast<T>(base_segment->operator[](value_index));
      auto dictionary_it = unique_values.find(value);
      auto dictionary_index = std::distance(unique_values.cbegin(), dictionary_it);
      _attribute_vector->set(value_index, ValueID{static_cast<uint32_t>(dictionary_index)});
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override { return get(i); }

  // return the value at a certain position.
  const T get(const size_t i) const {
    const auto dictionary_index = _attribute_vector->get(i);
    return _dictionary->at(dictionary_index);
  }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override { Fail("can not append value to DictionarySegment"); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    const auto lower_bound_it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (lower_bound_it == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(static_cast<uint32_t>(std::distance(_dictionary->cbegin(), lower_bound_it)));
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto upper_bound_it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (upper_bound_it == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(static_cast<uint32_t>(std::distance(_dictionary->cbegin(), upper_bound_it)));
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); };

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
