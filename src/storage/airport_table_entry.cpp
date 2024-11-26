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

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, LogicalType rowid_type)
      : TableCatalogEntry(catalog, schema, info, rowid_type)
  {
    this->internal = false;
  }

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AirportTableInfo &info, LogicalType rowid_type)
      : TableCatalogEntry(catalog, schema, *info.create_info, rowid_type)
  {
    this->internal = false;
  }

  unique_ptr<BaseStatistics> AirportTableEntry::GetStatistics(ClientContext &context, column_t column_id)
  {
    // TODO: Rusty implement this from the flight server.
    // printf("Getting column statistics for column %d\n", column_id);
    return nullptr;
  }

  // void AirportTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
  //                                               ClientContext &)
  // {
  //   	// check the constraints and indexes of the table to see if we need to project any additional columns
  // // we do this for indexes with multiple columns and CHECK constraints in the UPDATE clause
  // // suppose we have a constraint CHECK(i + j < 10); now we need both i and j to check the constraint
  // // if we are only updating one of the two columns we add the other one to the UPDATE set
  // // with a "useless" update (i.e. i=i) so we can verify that the CHECK constraint is not violated
  // auto bound_constraints = binder.BindConstraints(constraints, name, columns);
  // for (auto &constraint : bound_constraints) {
  // 	if (constraint->type == ConstraintType::CHECK) {
  // 		auto &check = constraint->Cast<BoundCheckConstraint>();
  // 		// check constraint! check if we need to add any extra columns to the UPDATE clause
  // 		BindExtraColumns(*this, get, proj, update, check.bound_columns);
  // 	}
  // }
  // if (update.return_chunk) {
  // 	physical_index_set_t all_columns;
  // 	for (auto &column : GetColumns().Physical()) {
  // 		all_columns.insert(column.Physical());
  // 	}
  // 	BindExtraColumns(*this, get, proj, update, all_columns);
  // }
  // // for index updates we always turn any update into an insert and a delete
  // // we thus need all the columns to be available, hence we check if the update touches any index columns
  // // If the returning keyword is used, we need access to the whole row in case the user requests it.
  // // Therefore switch the update to a delete and insert.
  // update.update_is_del_and_insert = false;
  // TableStorageInfo table_storage_info = GetStorageInfo(context);
  // for (auto index : table_storage_info.index_info) {
  // 	for (auto &column : update.columns) {
  // 		if (index.column_set.find(column.index) != index.column_set.end()) {
  // 			update.update_is_del_and_insert = true;
  // 			break;
  // 		}
  // 	}
  // };
  // }

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
