#pragma once

#define AIRPORT_RETURN_IF_(error_prefix, condition, status, description)                                                                            \
  do                                                                                                                                                \
  {                                                                                                                                                 \
    if (ARROW_PREDICT_FALSE(condition))                                                                                                             \
    {                                                                                                                                               \
      throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + status.message() + " " + description); \
    }                                                                                                                                               \
  } while (0)

#define AIRPORT_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr, error_prefix)                              \
  auto &&result_name = (rexpr);                                                                          \
  AIRPORT_RETURN_IF_(error_prefix, !(result_name).ok(), (result_name).status(), ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_ASSERT_OK(expr, error_prefix)                                        \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();) \
    throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + _st.message());

#define AIRPORT_ASSIGN_OR_RAISE(lhs, rexpr, error_prefix) \
  AIRPORT_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, error_prefix);

#define AIRPORT_ARROW_ASSIGN_OR_RAISE(lhs, rexpr, error_prefix) \
  AIRPORT_ARROW_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, error_prefix);

#define AIRPORT_ARROW_RETURN_IF_(error_prefix, condition, status, _)                                                            \
  do                                                                                                                            \
  {                                                                                                                             \
    if (ARROW_PREDICT_FALSE(condition))                                                                                         \
    {                                                                                                                           \
      throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + status.message()); \
    }                                                                                                                           \
  } while (0)

#define AIRPORT_ARROW_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr, error_prefix)                              \
  auto &&result_name = (rexpr);                                                                                \
  AIRPORT_ARROW_RETURN_IF_(error_prefix, !(result_name).ok(), (result_name).status(), ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_ARROW_ASSERT_OK(expr, error_prefix)                                  \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();) \
    throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + _st.message());
