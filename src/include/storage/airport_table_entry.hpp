#pragma once

#include "airport_catalog_api.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb
{

  struct AirportTableInfo
  {
    AirportTableInfo()
    {
      create_info = make_uniq<CreateTableInfo>();
    }
    AirportTableInfo(const string &schema, const string &table)
    {
      create_info = make_uniq<CreateTableInfo>(string(), schema, table);
    }
    AirportTableInfo(const SchemaCatalogEntry &schema, const string &table)
    {
      create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table);
    }

    const string &GetTableName() const
    {
      return create_info->table;
    }

    unique_ptr<CreateTableInfo> create_info;
  };

  class AirportTableEntry : public TableCatalogEntry
  {
  public:
    AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
    AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AirportTableInfo &info);

    unique_ptr<AirportAPITable> table_data;

  public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                               ClientContext &context) override;
  };

} // namespace duckdb
