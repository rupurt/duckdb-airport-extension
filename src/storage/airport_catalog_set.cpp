#include "storage/airport_catalog_set.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/airport_schema_entry.hpp"

namespace duckdb
{

  AirportCatalogSet::AirportCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false)
  {
  }

  optional_ptr<CatalogEntry> AirportCatalogSet::GetEntry(ClientContext &context, const string &name)
  {
    lock_guard<mutex> l(entry_lock);
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }
    auto entry = entries.find(name);
    if (entry == entries.end())
    {
      return nullptr;
    }
    return entry->second.get();
  }

  void AirportCatalogSet::DropEntry(ClientContext &context, DropInfo &info)
  {
    throw NotImplementedException("AirportCatalogSet::DropEntry");
  }

  void AirportCatalogSet::EraseEntryInternal(const string &name)
  {
    lock_guard<mutex> l(entry_lock);
    entries.erase(name);
  }

  void AirportCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback)
  {
    lock_guard<mutex> l(entry_lock);
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }
    for (auto &entry : entries)
    {
      callback(*entry.second);
    }
  }

  optional_ptr<CatalogEntry> AirportCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry)
  {
    //    lock_guard<mutex> l(entry_lock);
    auto result = entry.get();
    if (result->name.empty())
    {
      throw InternalException("AirportCatalogSet::CreateEntry called with empty name");
    }
    entries.insert(make_pair(result->name, std::move(entry)));
    return result;
  }

  void AirportCatalogSet::ClearEntries()
  {
    lock_guard<mutex> l(entry_lock);
    entries.clear();
    is_loaded = false;
  }

  AirportInSchemaSet::AirportInSchemaSet(AirportSchemaEntry &schema) : AirportCatalogSet(schema.ParentCatalog()), schema(schema)
  {
  }

  optional_ptr<CatalogEntry> AirportInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry)
  {
    if (!entry->internal)
    {
      entry->internal = schema.internal;
    }
    return AirportCatalogSet::CreateEntry(std::move(entry));
  }

} // namespace duckdb
