#include "storage/airport_catalog_api.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_table_set.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/airport_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb.hpp"
#include "airport_macros.hpp"
#include <arrow/c/bridge.h>
#include "duckdb/function/table/arrow.hpp"
#include "storage/airport_curl_pool.hpp"

namespace duckdb
{

  AirportTableSet::AirportTableSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory_) : AirportInSchemaSet(schema), connection_pool(connection_pool), cache_directory(cache_directory_)
  {
  }

  AirportTableSet::~AirportTableSet()
  {
  }

  void AirportTableSet::LoadEntries(ClientContext &context)
  {
    // auto &transaction = AirportTransaction::Get(context, catalog);

    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    // TODO: handle out-of-order columns using position property
    auto curl = connection_pool.acquire();
    auto tables = AirportAPI::GetTables(
        curl,
        catalog.GetDBPath(),
        schema.name,
        schema.schema_data->contents_url,
        schema.schema_data->contents_sha256,
        cache_directory,
        airport_catalog.credentials);
    connection_pool.release(curl);

    for (auto &table : tables)
    {
      //      D_ASSERT(schema.name == table.schema_name);
      CreateTableInfo info;

      info.table = table.name;
      info.comment = table.comment;

      std::shared_ptr<arrow::Schema> info_schema;
      arrow::ipc::DictionaryMemo dictionary_memo;
      string error_location = "(" + airport_catalog.credentials.location + ")";
      AIRPORT_ARROW_ASSIGN_OR_RAISE(info_schema, table.flight_info->GetSchema(&dictionary_memo), error_location);

      ArrowSchema arrow_schema;

      AIRPORT_ARROW_ASSERT_OK(ExportSchema(*info_schema, &arrow_schema), error_location);

      vector<string> column_names;
      vector<duckdb::LogicalType> return_types;

      for (idx_t col_idx = 0;
           col_idx < (idx_t)arrow_schema.n_children; col_idx++)
      {
        auto &column = *arrow_schema.children[col_idx];
        if (!column.release)
        {
          throw InvalidInputException("AirportTableSet::LoadEntries: released schema passed");
        }

        auto column_name = string(column.name);
        if (column_name.empty())
        {
          column_name = string("v") + to_string(col_idx);
        }

        column_names.emplace_back(column_name);

        auto arrow_type = ArrowTableFunction::GetArrowLogicalType(column);
        if (column.dictionary)
        {
          auto dictionary_type = ArrowTableFunction::GetArrowLogicalType(*column.dictionary);
          return_types.emplace_back(dictionary_type->GetDuckType());
        }
        else
        {
          return_types.emplace_back(arrow_type->GetDuckType());
        }
      }
      arrow_schema.release(&arrow_schema);

      QueryResult::DeduplicateColumns(column_names);
      for (idx_t col_idx = 0;
           col_idx < (idx_t)arrow_schema.n_children; col_idx++)
      {
        info.columns.AddColumn({column_names[col_idx], return_types[col_idx]});
      }

      auto table_entry = make_uniq<AirportTableEntry>(catalog, schema, info);
      table_entry->table_data = make_uniq<AirportAPITable>(table);

      CreateEntry(std::move(table_entry));
    }
  }

  optional_ptr<CatalogEntry> AirportTableSet::RefreshTable(ClientContext &context, const string &table_name)
  {
    auto table_info = GetTableInfo(context, schema, table_name);
    auto table_entry = make_uniq<AirportTableEntry>(catalog, schema, *table_info);
    auto table_ptr = table_entry.get();
    CreateEntry(std::move(table_entry));
    return table_ptr;
  }

  unique_ptr<AirportTableInfo> AirportTableSet::GetTableInfo(ClientContext &context, AirportSchemaEntry &schema,
                                                             const string &table_name)
  {
    throw NotImplementedException("AirportTableSet::GetTableInfo");
  }

  optional_ptr<CatalogEntry> AirportTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info)
  {
    throw NotImplementedException("AirportTableSet::CreateTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, RenameTableInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, AddColumnInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

} // namespace duckdb
