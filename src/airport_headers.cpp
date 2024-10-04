#include "airport_headers.hpp"
#include <string.h>

#define AIRPORT_USER_AGENT "airport/20240820-01"

void airport_add_headers(std::vector<std::pair<std::string, std::string>> &headers, const std::string &server_location)
{
  headers.emplace_back("airport-user-agent", AIRPORT_USER_AGENT);
  headers.emplace_back("authority", server_location);
  headers.emplace_back("arrow-flight-user-agent", "duckdb-airport/0.0.1");
}

void airport_add_standard_headers(arrow::flight::FlightCallOptions &options, const std::string &server_location)
{
  airport_add_headers(options.headers, server_location);
}