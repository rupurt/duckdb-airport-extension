#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"


#define AIRPORT_RETURN_IF_(condition, status, _) \
  do {                                         \
    if (ARROW_PREDICT_FALSE(condition)) {      \
        throw InvalidInputException("airport - Arrow Flight Exception: " + status.message()); \
    }                                          \
  } while (0)


#define AIRPORT_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)                            \
  auto&& result_name = (rexpr);                                                          \
  AIRPORT_RETURN_IF_(!(result_name).ok(), (result_name).status(), ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_ASSERT_OK(expr)                                                      \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();) \
  throw InvalidInputException("airport - Arrow Flight Exception: " + _st.message());


#define AIRPORT_ASSIGN_OR_RAISE(lhs, rexpr)                                              \
  AIRPORT_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                             lhs, rexpr);

