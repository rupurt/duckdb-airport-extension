#include "storage/airport_schema_set.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb
{
  // Set the connection pool size.
  AirportSchemaSet::AirportSchemaSet(Catalog &catalog) : AirportCatalogSet(catalog), connection_pool(32)
  {
  }

  static bool IsInternalTable(const string &catalog, const string &schema)
  {
    if (schema == "information_schema")
    {
      return true;
    }
    return false;
  }

  static string DuckDBHomeDirectory(ClientContext &context)
  {
    auto &fs = FileSystem::GetFileSystem(context);

    string home_directory = fs.GetHomeDirectory();
    // exception if the home directory does not exist, don't create whatever we think is home
    if (!fs.DirectoryExists(home_directory))
    {
      throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
                        "home_directory='/path/to/dir' option.",
                        home_directory);
    }
    string cache_path = home_directory;
    cache_path = fs.JoinPath(cache_path, ".duckdb");
    return cache_path;
  }

  void AirportSchemaSet::LoadEntireSet(ClientContext &context)
  {
    if (called_load_entries == false)
    {
      // We haven't called load entries yet.
      LoadEntries(context);
      called_load_entries = true;
    }
    // If there isn't both a contents_url and contents_sha256 don't do anything.
    // also if everything has been previously populated, just return early.
    if (contents_url.empty() || contents_sha256.empty() || populated_entire_set)
    {
      return;
    }

    auto cache_path = DuckDBHomeDirectory(context);

    // Populate the on-disk schema cache from the catalog while contents_url.
    auto curl = connection_pool.acquire();
    AirportAPI::PopulateURLCacheUsingContainerURL(curl, contents_url, contents_sha256, cache_path);
    connection_pool.release(curl);
    populated_entire_set = true;
  }

  void AirportSchemaSet::LoadEntries(ClientContext &context)
  {
    if (called_load_entries)
    {
      return;
    }

    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    string cache_path = DuckDBHomeDirectory(context);

    auto schema_collection = AirportAPI::GetSchemas(catalog.GetName(), airport_catalog.credentials);

    contents_url = schema_collection->contents_url;
    contents_sha256 = schema_collection->contents_sha256;

    for (const auto &schema : schema_collection->schemas)
    {
      CreateSchemaInfo info;
      info.schema = schema.schema_name;
      info.internal = IsInternalTable(schema.catalog_name, schema.schema_name);
      auto schema_entry = make_uniq<AirportSchemaEntry>(catalog, info, connection_pool, cache_path);
      schema_entry->schema_data = make_uniq<AirportAPISchema>(schema);
      schema_entry->comment = schema.comment;
      schema_entry->tags = schema.tags;
      CreateEntry(std::move(schema_entry));
    }

    called_load_entries = true;
  }

  optional_ptr<CatalogEntry> AirportSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info)
  {
    throw NotImplementedException("Schema creation");
  }

} // namespace duckdb
