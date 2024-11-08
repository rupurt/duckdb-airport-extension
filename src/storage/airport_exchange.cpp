#include "duckdb.hpp"
#include "storage/airport_delete.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "airport_macros.hpp"
#include "airport_headers.hpp"
#include "airport_exception.hpp"
#include "airport_secrets.hpp"

#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/type_fwd.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/c/bridge.h"
#include "duckdb/common/arrow/schema_metadata.hpp"

#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_exchange.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb
{
  static std::string join_vector_of_strings(const std::vector<std::string> &vec, const char joiner)
  {
    if (vec.empty())
      return "";

    return std::accumulate(
        std::next(vec.begin()), vec.end(), vec.front(),
        [joiner](const std::string &a, const std::string &b)
        {
          return a + joiner + b;
        });
  }

  static int findIndex(const std::vector<std::string> &vec, const std::string &target)
  {
    auto it = std::find(vec.begin(), vec.end(), target);

    if (it == vec.end())
    {
      throw std::runtime_error("String not found in vector");
    }

    return std::distance(vec.begin(), it);
  }

  void AirportExchangeGetGlobalSinkState(ClientContext &context,
                                         TableCatalogEntry &table,
                                         AirportTableEntry &airport_table,
                                         AirportExchangeGlobalState *global_state,
                                         ArrowSchema &send_schema,
                                         bool return_chunk,
                                         string exchange_operation)
  {
    //    auto global_state = make_uniq<AirportDeleteGlobalState>(context, airport_table, GetTypes(), return_chunk);

    //    global_state->send_types = {airport_table.GetRowIdType()};
    //    vector<string> send_names = {"row_id"};
    //   ArrowSchema send_schema;
    //  ArrowConverter::ToArrowSchema(&send_schema, global_state->send_types, send_names,
    //                                 context.GetClientProperties());
    // Create the C++ schema.
    global_state->schema = arrow::ImportSchema(&send_schema).ValueOrDie();
    //    global_state->return_chunk = return_chunk;
    global_state->flight_descriptor = airport_table.table_data->flight_info->descriptor();

    auto auth_token = AirportAuthTokenForLocation(context, airport_table.table_data->location, "", "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_location, flight::Location::Parse(airport_table.table_data->location),
                                                       airport_table.table_data->location,
                                                       global_state->flight_descriptor, "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_client,
                                                       flight::FlightClient::Connect(flight_location),
                                                       airport_table.table_data->location,
                                                       global_state->flight_descriptor, "");

    auto trace_uuid = UUID::ToString(UUID::GenerateRandomUUID());

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, airport_table.table_data->location);

    if (!auth_token.empty())
    {
      std::stringstream ss;
      ss << "Bearer " << auth_token;
      call_options.headers.emplace_back("authorization", ss.str());
    }

    call_options.headers.emplace_back("airport-trace-id", trace_uuid);

    // Indicate that we are doing a delete.
    call_options.headers.emplace_back("airport-operation", exchange_operation);

    // Indicate if the caller is interested in data being returned.
    call_options.headers.emplace_back("return-chunks", return_chunk ? "1" : "0");

    if (global_state->flight_descriptor.type == arrow::flight::FlightDescriptor::PATH)
    {
      auto path_parts = global_state->flight_descriptor.path;
      std::string joined_path_parts = join_vector_of_strings(path_parts, '/');
      call_options.headers.emplace_back("airport-flight-path", joined_path_parts);
    }

    // Need to make this call so its possible to be stored in the scan data below.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_info,
                                                       flight_client->GetFlightInfo(call_options, global_state->flight_descriptor),
                                                       airport_table.table_data->location,
                                                       global_state->flight_descriptor,
                                                       "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto exchange_result,
        flight_client->DoExchange(call_options, global_state->flight_descriptor),
        airport_table.table_data->location,
        global_state->flight_descriptor, "");

    // Tell the server the schema that we will be using to write data.
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        exchange_result.writer->Begin(global_state->schema),
        airport_table.table_data->location,
        global_state->flight_descriptor,
        "Begin schema");

    // Now that there is a reader stream and a writer stream, we want to reuse the Arrow
    // scan code as much as possible, but the problem is it assumes its being called as
    // part of a table returning function, with the life cycle of bind, init global, init local
    // and scan.
    //
    // But we can simulate most of that here.
    auto scan_data = make_uniq<AirportTakeFlightScanData>(
        airport_table.table_data->location,
        std::move(flight_info),
        std::move(exchange_result.reader));

    auto scan_bind_data = make_uniq<AirportExchangeTakeFlightBindData>(
        (stream_factory_produce_t)&AirportFlightStreamReader::CreateStream,
        (uintptr_t)scan_data.get());

    scan_bind_data->scan_data = std::move(scan_data);
    scan_bind_data->flight_client = std::move(flight_client);
    scan_bind_data->server_location = airport_table.table_data->location;
    scan_bind_data->trace_id = trace_uuid;

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto read_schema,
                                                       scan_bind_data->scan_data->stream_->GetSchema(),
                                                       airport_table.table_data->location,
                                                       global_state->flight_descriptor, "");

    auto &data = *scan_bind_data;
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        ExportSchema(*read_schema, &data.schema_root.arrow_schema),
        airport_table.table_data->location,
        global_state->flight_descriptor,
        "ExportSchema");

    // Get a list of the real columns on the table, so that their
    // offsets can be looked up from how the schema is returned.
    vector<string> real_table_names;
    for (auto &cd : table.GetColumns().Logical())
    {
      real_table_names.push_back(cd.GetName());
    }

    vector<column_t> column_ids;

    idx_t row_id_col_idx = -1;
    for (idx_t col_idx = 0;
         col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *data.schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_delete: released schema passed");
      }
      auto arrow_type = ArrowTableFunction::GetArrowLogicalType(schema);

      // Determine if the column is the row_id column by looking at the metadata
      // on the column.
      bool is_row_id_column = false;
      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);

        auto comment = column_metadata.GetOption("is_row_id");
        if (!comment.empty())
        {
          is_row_id_column = true;
          scan_bind_data->row_id_column_index = col_idx;
        }
      }

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowTableFunction::GetArrowLogicalType(*schema.dictionary);
        if (!is_row_id_column)
        {
          scan_bind_data->return_types.emplace_back(dictionary_type->GetDuckType());
        }
        arrow_type->SetDictionary(std::move(dictionary_type));
      }
      else
      {
        if (!is_row_id_column)
        {
          scan_bind_data->return_types.emplace_back(arrow_type->GetDuckType());
        }
      }

      scan_bind_data->arrow_table.AddColumn(is_row_id_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx, std::move(arrow_type));

      // FIXME: determine if bind should include the row_id column.

      auto format = string(schema.format);
      auto name = string(schema.name);
      if (name.empty())
      {
        name = string("v") + to_string(col_idx);
      }

      if (!is_row_id_column)
      {
        scan_bind_data->names.push_back(name);
        column_ids.emplace_back(findIndex(real_table_names, name));
      }
      else
      {
        row_id_col_idx = col_idx;
      }
    }

    // Since the server may provide row_id but returning doesn't include it we need to
    // adjust the index of the data columns to be shifted by 1.
    if (row_id_col_idx != -1)
    {
      for (size_t i = row_id_col_idx; i < column_ids.size(); ++i)
      {
        column_ids[i] += 1;
      }
    }

    // For each index in the arrow table, the column_ids is asked what
    // where to map that column, the row id can be expressed there.

    // There shouldn't be any projection ids.
    vector<idx_t> projection_ids;

    // Now to initialize the Arrow scan from the reader stream we need to do the steps
    // that the normal table returning function does.

    // bind
    // init global state
    // init local state
    // scan...

    // Init the global state.
    auto scan_global_state = make_uniq<ArrowScanGlobalState>();
    scan_global_state->stream = AirportProduceArrowScan(scan_bind_data->CastNoConst<ArrowScanFunctionData>(), column_ids, nullptr);
    scan_global_state->max_threads = 1;

    // Retain the global state.
    global_state->scan_global_state = std::move(scan_global_state);

    // Now simulate the init input.
    auto fake_init_input = TableFunctionInitInput(
        &scan_bind_data->Cast<FunctionData>(),
        column_ids,
        projection_ids,
        nullptr);

    // Local init.

    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto scan_local_state = make_uniq<ArrowScanLocalState>(std::move(current_chunk));
    scan_local_state->column_ids = fake_init_input.column_ids;
    scan_local_state->filters = fake_init_input.filters.get();

    global_state->scan_local_state = std::move(scan_local_state);

    // Create a parameter is the commonly passed to the other functions.
    global_state->scan_bind_data = std::move(scan_bind_data);
    global_state->writer = std::move(exchange_result.writer);

    global_state->scan_table_function_input = make_uniq<TableFunctionInput>(
        global_state->scan_bind_data.get(),
        global_state->scan_local_state.get(),
        global_state->scan_global_state.get());
  }

}