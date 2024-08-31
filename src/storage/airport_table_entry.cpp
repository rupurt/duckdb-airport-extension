#include "storage/airport_catalog.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_table_entry.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "storage/airport_catalog_api.hpp"
#include "../../duckdb/third_party/catch/catch.hpp"

namespace flight = arrow::flight;

namespace duckdb
{

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
      : TableCatalogEntry(catalog, schema, info)
  {
    this->internal = false;
  }

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AirportTableInfo &info)
      : TableCatalogEntry(catalog, schema, *info.create_info)
  {
    this->internal = false;
  }

  unique_ptr<BaseStatistics> AirportTableEntry::GetStatistics(ClientContext &context, column_t column_id)
  {
    // TODO: Rusty implement this from the flight server.
    return nullptr;
  }

  void AirportTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                                ClientContext &)
  {
    throw NotImplementedException("BindUpdateConstraints");
  }

  TableFunction AirportTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data)
  {
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &airport_take_flight_function_set = ExtensionUtil::GetTableFunction(db, "airport_take_flight");
    auto airport_take_flight_function = airport_take_flight_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR, LogicalType::POINTER});

    D_ASSERT(table_data);
    D_ASSERT(table_data->flight_info);

    auto descriptor = table_data->flight_info->descriptor();

    // Rusty: this is the place where the transformation happens between table functions and tables.

    vector<Value> inputs = {table_data->location, Value::POINTER((uintptr_t)&table_data->flight_info->descriptor())};

    // if (table_data->storage_location.find("file://") != 0)
    // {
    //   auto &secret_manager = SecretManager::Get(context);
    //   // Get Credentials from AirportAPI
    //   auto table_credentials = AirportAPI::GetTableCredentials(table_data->table_id, airport_catalog.credentials);

    //   // Inject secret into secret manager scoped to this path
    //   CreateSecretInfo info(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);
    //   info.name = "__internal_uc_" + table_data->table_id;
    //   info.type = "s3";
    //   info.provider = "config";
    //   info.options = {
    //       {"key_id", table_credentials.key_id},
    //       {"secret", table_credentials.secret},
    //       {"session_token", table_credentials.session_token},
    //       {"region", airport_catalog.credentials.aws_region},
    //   };
    //   info.scope = {table_data->storage_location};
    //   secret_manager.CreateSecret(context, info);
    // }
    named_parameter_map_t param_map;
    vector<LogicalType> return_types;
    vector<string> names;
    TableFunctionRef empty_ref;

    TableFunctionBindInput bind_input(inputs,
    param_map,
    return_types,
    names,
    nullptr,
    nullptr,
    airport_take_flight_function,
    empty_ref);

    auto result = airport_take_flight_function.bind(context, bind_input, return_types, names);
    bind_data = std::move(result);

    return airport_take_flight_function;
  }

  TableStorageInfo AirportTableEntry::GetStorageInfo(ClientContext &context)
  {
    TableStorageInfo result;
    // TODO fill info
    return result;
  }

} // namespace duckdb
