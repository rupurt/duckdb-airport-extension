#pragma once

#include <vector>
#include <string>
#include "arrow/flight/client.h"

namespace duckdb
{
  void airport_add_headers(std::vector<std::pair<std::string, std::string>> &headers, const std::string &server_location);
  void airport_add_standard_headers(arrow::flight::FlightCallOptions &options, const std::string &server_location);
}