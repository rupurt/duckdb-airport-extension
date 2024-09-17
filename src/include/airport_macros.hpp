#include "airport_exception.hpp"

#pragma once

// #define AIRPORT_RETURN_IF_(error_prefix, condition, status, description)                                                                            \
//   do                                                                                                                                                \
//   {                                                                                                                                                 \
//     if (ARROW_PREDICT_FALSE(condition))                                                                                                             \
//     {                                                                                                                                               \
//       throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + status.message() + " " + description); \
//     }                                                                                                                                               \
//   } while (0)

// #define AIRPORT_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr, error_prefix)                              \
//   auto &&result_name = (rexpr);                                                                          \
//   AIRPORT_RETURN_IF_(error_prefix, !(result_name).ok(), (result_name).status(), ARROW_STRINGIFY(rexpr)); \
//   lhs = std::move(result_name).ValueUnsafe();

// #define AIRPORT_ASSERT_OK(expr, error_prefix)                                        \
//   for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();) \
//     throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + _st.message());

// #define AIRPORT_ASSIGN_OR_RAISE(lhs, rexpr, error_prefix) \
//   AIRPORT_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, error_prefix);

// #define AIRPORT_ARROW_ASSIGN_OR_RAISE(lhs, rexpr, error_prefix) \
//   AIRPORT_ARROW_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, error_prefix);

// #define AIRPORT_ARROW_RETURN_IF_(error_prefix, condition, status, _)                                                            \
//   do                                                                                                                            \
//   {                                                                                                                             \
//     if (ARROW_PREDICT_FALSE(condition))                                                                                         \
//     {                                                                                                                           \
//       throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + status.message()); \
//     }                                                                                                                           \
//   } while (0)

// #define AIRPORT_ARROW_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr, error_prefix)                              \
//   auto &&result_name = (rexpr);                                                                                \
//   AIRPORT_ARROW_RETURN_IF_(error_prefix, !(result_name).ok(), (result_name).status(), ARROW_STRINGIFY(rexpr)); \
//   lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_ARROW_ASSERT_OK_LOCATION(expr, location, message)                    \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();) \
    throw AirportFlightException(location, _st, "");

#define AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(expr, location, descriptor, message) \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();)     \
    throw AirportFlightException(location, descriptor, _st, message);

//    throw InvalidInputException("airport - " + std::string(error_prefix) + " - Arrow Flight Exception: " + _st.message());

#define AIRPORT_FLIGHT_RETURN_IF_LOCATION_DESCRIPTOR(error_prefix, condition, status, location, descriptor, message, extra_message) \
  do                                                                                                                                \
  {                                                                                                                                 \
    if (ARROW_PREDICT_FALSE(condition))                                                                                             \
    {                                                                                                                               \
      throw AirportFlightException(location, descriptor, status, message, {{"extra_details", string(extra_message)}});              \
    }                                                                                                                               \
  } while (0)

#define AIRPORT_FLIGHT_RETURN_IF_LOCATION(error_prefix, condition, status, location, message, extra_message) \
  do                                                                                                         \
  {                                                                                                          \
    if (ARROW_PREDICT_FALSE(condition))                                                                      \
    {                                                                                                        \
      throw AirportFlightException(location, status, message, {{"extra_details", string(extra_message)}});   \
    }                                                                                                        \
  } while (0)

#define AIRPORT_FLIGHT_ASSIGN_OR_RAISE_IMPL_LOCATION_DESCRIPTOR(result_name, lhs, rexpr, location, descriptor, message)                                           \
  auto &&result_name = (rexpr);                                                                                                                                   \
  AIRPORT_FLIGHT_RETURN_IF_LOCATION_DESCRIPTOR(error_prefix, !(result_name).ok(), (result_name).status(), location, descriptor, message, ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_FLIGHT_ASSIGN_OR_RAISE_IMPL_LOCATION(result_name, lhs, rexpr, location, message)                                           \
  auto &&result_name = (rexpr);                                                                                                            \
  AIRPORT_FLIGHT_RETURN_IF_LOCATION(error_prefix, !(result_name).ok(), (result_name).status(), location, message, ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(lhs, rexpr, location, descriptor, message) \
  AIRPORT_FLIGHT_ASSIGN_OR_RAISE_IMPL_LOCATION_DESCRIPTOR(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, location, descriptor, message);

#define AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(lhs, rexpr, location, message) \
  AIRPORT_FLIGHT_ASSIGN_OR_RAISE_IMPL_LOCATION(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, location, message);
