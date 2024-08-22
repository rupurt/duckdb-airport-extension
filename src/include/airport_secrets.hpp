#pragma once
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb
{
  unique_ptr<SecretEntry> AirportGetSecretByName(ClientContext &context, const string &secret_name);

  SecretMatch AirportGetSecretByPath(ClientContext &context, const string &path);
}
