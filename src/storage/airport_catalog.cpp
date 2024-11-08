#include "storage/airport_catalog.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/airport_delete.hpp"

namespace duckdb
{

  AirportCatalog::AirportCatalog(AttachedDatabase &db_p, const string &internal_name, AccessMode access_mode,
                                 AirportCredentials credentials)
      : Catalog(db_p), internal_name(internal_name), access_mode(access_mode), credentials(std::move(credentials)),
        schemas(*this)
  {
  }

  AirportCatalog::~AirportCatalog() = default;

  void AirportCatalog::Initialize(bool load_builtin)
  {
  }

  optional_idx AirportCatalog::GetCatalogVersion(ClientContext &context)
  {
    // These catalogs generally don't change so just return 1 for now, if we were
    // creating dynamic tables or other changes this will have to change.
    //
    // If we don't do this all statements get rebound twice.
    return 1;
  }

  optional_ptr<CatalogEntry> AirportCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info)
  {
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT)
    {
      DropInfo try_drop;
      try_drop.type = CatalogType::SCHEMA_ENTRY;
      try_drop.name = info.schema;
      try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
      try_drop.cascade = false;
      schemas.DropEntry(transaction.GetContext(), try_drop);
    }
    return schemas.CreateSchema(transaction.GetContext(), info);
  }

  void AirportCatalog::DropSchema(ClientContext &context, DropInfo &info)
  {
    return schemas.DropEntry(context, info);
  }

  void AirportCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback)
  {
    // If there is a contents_url for all schemas make sure it is present and decompressed on the disk, so that the
    // schema loaders will grab it.

    schemas.LoadEntireSet(context);

    schemas.Scan(context, [&](CatalogEntry &schema)
                 { callback(schema.Cast<AirportSchemaEntry>()); });
  }

  optional_ptr<SchemaCatalogEntry> AirportCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                             OnEntryNotFound if_not_found, QueryErrorContext error_context)
  {
    if (schema_name == DEFAULT_SCHEMA)
    {
      if (if_not_found == OnEntryNotFound::RETURN_NULL)
      {
        // There really isn't a default way to handle this, so just return null.
        return nullptr;
      }
      throw BinderException("Schema with name \"%s\" not found", schema_name);
    }
    auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
    if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL)
    {
      throw BinderException("Schema with name \"%s\" not found", schema_name);
    }
    return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
  }

  bool AirportCatalog::InMemory()
  {
    return false;
  }

  string AirportCatalog::GetDBPath()
  {
    return internal_name;
  }

  DatabaseSize AirportCatalog::GetDatabaseSize(ClientContext &context)
  {
    DatabaseSize size;
    return size;
  }

  void AirportCatalog::ClearCache()
  {
    schemas.ClearEntries();
  }

  unique_ptr<PhysicalOperator> AirportCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                          unique_ptr<PhysicalOperator> plan)
  {

    throw NotImplementedException("AirportCatalog PlanInsert");
  }
  unique_ptr<PhysicalOperator> AirportCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                                 unique_ptr<PhysicalOperator> plan)
  {
    throw NotImplementedException("AirportCatalog PlanCreateTableAs");
  }

  unique_ptr<PhysicalOperator> AirportCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                          unique_ptr<PhysicalOperator> plan)
  {
    throw NotImplementedException("AirportCatalog PlanUpdate");
  }
  unique_ptr<LogicalOperator> AirportCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                              unique_ptr<LogicalOperator> plan)
  {
    throw NotImplementedException("AirportCatalog BindCreateIndex");
  }

} // namespace duckdb
