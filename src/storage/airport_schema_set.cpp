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
    catalog_name = catalog.GetName();
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
    lock_guard<mutex> l(entry_lock);

    if (called_load_entries == false)
    {
      // We haven't called load entries yet.
      LoadEntries(context);
      called_load_entries = true;
    }
  }

  void AirportSchemaSet::LoadEntries(ClientContext &context)
  {
    if (called_load_entries)
    {
      return;
    }
    //    printf("Calling LoadEntries on AirportSchemaSet, catalog basically\n");

    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    string cache_path = DuckDBHomeDirectory(context);

    auto returned_collection = AirportAPI::GetSchemas(catalog.GetName(), airport_catalog.credentials);

    collection = std::move(returned_collection);

    std::unordered_set<string> seen_schema_names;

    // So the database can have all of its schemas sent at the top level.
    //
    // It can return a URL or an inline serialization of all saved schemas
    //
    // When the individual schemas are loaded they will be loaded through the
    // cached content that is already present on the disk, or if the schema
    // is serialized inline that will be used.
    //
    if (!populated_entire_set && !collection->contents_sha256.empty() &&
        !(collection->contents_serialized.empty() && collection->schema_collection_contents_url.empty()))
    {
      auto cache_path = DuckDBHomeDirectory(context);

      // Populate the on-disk schema cache from the catalog while contents_url.
      auto curl = connection_pool.acquire();
      AirportAPI::PopulateCatalogSchemaCacheFromURLorContent(curl, *collection, catalog_name, cache_path);
      connection_pool.release(curl);
    }
    populated_entire_set = true;

    for (const auto &schema : collection->schemas)
    {
      CreateSchemaInfo info;

      if (schema.schema_name.empty())
      {
        throw InvalidInputException("Schema name is empty when loading entries");
      }
      if (!(seen_schema_names.find(schema.schema_name) == seen_schema_names.end()))
      {
        throw InvalidInputException("Schema name %s is not unique when loading entries", schema.schema_name.c_str());
      }

      seen_schema_names.insert(schema.schema_name);

      info.schema = schema.schema_name;
      info.internal = IsInternalTable(schema.catalog_name, schema.schema_name);
      auto schema_entry = make_uniq<AirportSchemaEntry>(catalog, info, connection_pool, cache_path);
      schema_entry->schema_data = make_uniq<AirportAPISchema>(schema);

      // Since these are DuckDB attributes, we need to copy them manually.
      schema_entry->comment = schema.comment;
      schema_entry->tags = schema.tags;
      // printf("Creating schema %s\n", schema.schema_name.c_str());
      CreateEntry(std::move(schema_entry));
    }

    called_load_entries = true;
  }

  optional_ptr<CatalogEntry> AirportSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info)
  {
    throw NotImplementedException("Schema creation");
  }

} // namespace duckdb
