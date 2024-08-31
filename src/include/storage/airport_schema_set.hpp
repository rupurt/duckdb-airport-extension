#pragma once

#include "airport_catalog_set.hpp"
#include "airport_curl_pool.hpp"

namespace duckdb
{
  struct CreateSchemaInfo;

  class AirportSchemaSet : public AirportCatalogSet
  {
  public:
    explicit AirportSchemaSet(Catalog &catalog);

  public:
    optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

    // Load the schemas of the entire set from a cached url if possible, useful for scans
    // when all schemas are requested.
    void LoadEntireSet(ClientContext &context);

  protected:
    void LoadEntries(ClientContext &context) override;

  private:
    AirportCurlPool connection_pool;
    string contents_url;
    string contents_sha256;
    bool populated_entire_set = false;
    bool called_load_entries = false;

  };

} // namespace duckdb
