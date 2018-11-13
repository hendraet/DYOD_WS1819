#pragma once

#include <utils/assert.hpp>

#include <memory>
#include <vector>

#include "base_attribute_vector.hpp"

namespace opossum {

class BaseAttributeVector;

template <typename T>
class FittedAttributeVector : public BaseAttributeVector {
  static_assert(std::is_integral<T>::value,
                "VariableAttributeVector can only be instantiated using an integral data type (such as uint8_t).");
  static_assert(std::is_unsigned<T>::value,
                "VariableAttributeVector can only be instantiated using an unsigned data type (such as uint8_t).");

 public:
  explicit FittedAttributeVector(const size_t size, const ValueID max_value);

  ValueID get(const size_t i) const override;

  void set(const size_t i, const ValueID value_id) override;

  size_t size() const override;

  AttributeVectorWidth width() const override;

 protected:
  std::vector<T> _values;
};

std::shared_ptr<BaseAttributeVector> make_shared_attribute_vector(const size_t size, const ValueID max_value);

}  // namespace opossum
