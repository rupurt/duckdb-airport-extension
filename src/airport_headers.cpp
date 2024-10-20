#include "airport_headers.hpp"
#include <string.h>
#include "duckdb/common/types/uuid.hpp"

#define AIRPORT_USER_AGENT "airport/20240820-01"

namespace duckdb
{
  static std::string airport_session_id = "";

  static std::string get_airport_session_id()
  {
    if (airport_session_id.empty())
    {
      airport_session_id = UUID::ToString(UUID::GenerateRandomUUID());
    }
    return airport_session_id;
  }

  void airport_add_headers(std::vector<std::pair<std::string, std::string>> &headers, const std::string &server_location)
  {
    headers.emplace_back("airport-user-agent", AIRPORT_USER_AGENT);
    headers.emplace_back("authority", server_location);
    headers.emplace_back("airport-client-session-id", get_airport_session_id());
  }

  void airport_add_standard_headers(arrow::flight::FlightCallOptions &options, const std::string &server_location)
  {
    airport_add_headers(options.headers, server_location);
  }
}