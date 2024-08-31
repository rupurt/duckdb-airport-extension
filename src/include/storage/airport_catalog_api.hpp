#pragma once

#include "duckdb/common/types.hpp"

#include <arrow/flight/client.h>
#include <curl/curl.h>

namespace duckdb
{
  struct AirportCredentials;

  struct AirportAPITable
  {
    string location;
    std::shared_ptr<arrow::flight::FlightInfo> flight_info;

    string catalog_name;
    string schema_name;
    string name;
    string comment;
  };

  struct AirportAPISchema
  {
    string schema_name;
    string catalog_name;
    string comment;
    unordered_map<string, string> tags;

    // An optional URL to the contents of the schema.
    string contents_url;
    // The SHA256 hash of the contents of the schema.
    string contents_sha256;
  };

  struct AirportSchemaCollection
  {
    // An optional URL that contains all of the contents for all of the schemas.
    string contents_url;
    // The SHA256 of the contents url.
    string contents_sha256;
    vector<AirportAPISchema> schemas;
  };

  class AirportAPI
  {
  public:
    static vector<string> GetCatalogs(const string &catalog, AirportCredentials credentials);
    static vector<AirportAPITable> GetTables(CURL *curl,
                                             const string &catalog,
                                             const string &schema,
                                             const string &schema_contents_url,
                                             const string &schema_contents_sha256,
                                             const string &cache_base_dir,
                                             AirportCredentials credentials);
    static unique_ptr<AirportSchemaCollection> GetSchemas(const string &catalog, AirportCredentials credentials);

    static void PopulateURLCacheUsingContainerURL(CURL *curl, const string &url, const string &expected_sha256, const string &baseDir);
  };
} // namespace duckdb
