#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>

#include "duckdb/main/extension_util.hpp"

#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"
#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"

namespace flight = arrow::flight;

namespace duckdb
{

  // Create a FlightDescriptor from a DuckDB value which can be one of a few different
  // types.
  static flight::FlightDescriptor flight_descriptor_from_value(duckdb::Value &flight_descriptor)
  {
    switch (flight_descriptor.type().id())
    {
    case LogicalTypeId::BLOB:
    case LogicalTypeId::VARCHAR:
      return flight::FlightDescriptor::Command(flight_descriptor.ToString());
    case LogicalTypeId::LIST:
    {
      auto &list_values = ListValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < list_values.size(); i++)
      {
        auto &child = list_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    case LogicalTypeId::ARRAY:
    {
      auto &array_values = ArrayValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < array_values.size(); i++)
      {
        auto &child = array_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    // FIXME: deal with the union type returned by Arrow list flights.
    default:
      throw InvalidInputException("airport_take_flight: unknown descriptor type passed");
    }
  }

  static unique_ptr<FunctionData> take_flight_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    string auth_token = "";

    for (auto &kv : input.named_parameters)
    {
      auto loption = StringUtil::Lower(kv.first);
      if (loption == "auth_token")
      {
        auth_token = StringValue::Get(kv.second);
      }
    }

    auto server_location = input.inputs[0].ToString();
    AIRPORT_ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::Parse(server_location));

    auto descriptor = flight_descriptor_from_value(input.inputs[1]);

    AIRPORT_ARROW_ASSIGN_OR_RAISE(auto flight_client, flight::FlightClient::Connect(location));

    arrow::flight::FlightCallOptions call_options;
    call_options.headers.emplace_back("arrow-flight-user-agent", "duckdb-airport/0.0.1");
    if (!auth_token.empty())
    {
      std::stringstream ss;
      ss << "Bearer " << auth_token;
      call_options.headers.emplace_back("authorization", ss.str());
    }

    // Get the information about the flight, this will allow the
    // endpoint information to be returned.
    AIRPORT_ARROW_ASSIGN_OR_RAISE(auto flight_info, flight_client->GetFlightInfo(call_options, descriptor));

    // Store the flight info on the bind data.

    // After doing a little bit of examination of the DuckDb sources, I learned that
    // that DuckDb supports the "C" interface of Arrow, this means that DuckDB doens't
    // actually have a dependency on Arrow.
    //
    // Arrow Flight requires a dependency on the full Arrow library, because of all of
    // the dependencies.
    //
    // Thankfully there is a "bridge" interface between the C++ based Arrow types returned
    // by the C++ Arrow library and the C based Arrow types that DuckDB already knows how to
    // consume.

    auto scan_data = make_uniq<AirportTakeFlightScanData>(
        std::move(flight_info),
        nullptr);

    auto ret = make_uniq<AirportTakeFlightBindData>(
        (stream_factory_produce_t)&AirportFlightStreamReader::CreateStream,
        (uintptr_t)scan_data.get());

    ret->scan_data = std::move(scan_data);
    ret->flight_client = std::move(flight_client);
    ret->auth_token = auth_token;

    auto &data = *ret;

    // Convert the C++ schema into the C format schema, but store it on the bind
    // information
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_ARROW_ASSIGN_OR_RAISE(info_schema, ret->scan_data->flight_info_->GetSchema(&dictionary_memo));

    AIRPORT_ARROW_ASSERT_OK(ExportSchema(*info_schema, &data.schema_root.arrow_schema));

    for (idx_t col_idx = 0;
         col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *data.schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_take_flight: released schema passed");
      }
      auto arrow_type = ArrowTableFunction::GetArrowLogicalType(schema);
      if (schema.dictionary)
      {
        auto dictionary_type = ArrowTableFunction::GetArrowLogicalType(*schema.dictionary);
        return_types.emplace_back(dictionary_type->GetDuckType());
        arrow_type->SetDictionary(std::move(dictionary_type));
      }
      else
      {
        return_types.emplace_back(arrow_type->GetDuckType());
      }
      ret->arrow_table.AddColumn(col_idx, std::move(arrow_type));
      auto format = string(schema.format);
      auto name = string(schema.name);
      if (name.empty())
      {
        name = string("v") + to_string(col_idx);
      }
      names.push_back(name);
    }
    QueryResult::DeduplicateColumns(names);
    return std::move(ret);
  }

  static void take_flight(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    if (!data_p.local_state)
    {
      return;
    }
    auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
    auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

    //! Out of tuples in this chunk
    if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length)
    {
      if (!ArrowTableFunction::ArrowScanParallelStateNext(
              context,
              data_p.bind_data.get(),
              state,
              global_state))
      {
        return;
      }
    }
    int64_t output_size =
        MinValue<int64_t>(STANDARD_VECTOR_SIZE,
                          state.chunk->arrow_array.length - state.chunk_offset);
    data.lines_read += output_size;

    if (global_state.CanRemoveFilterColumns())
    {
      state.all_columns.Reset();
      state.all_columns.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns,
                                        data.lines_read - output_size, false);
      output.ReferenceColumns(state.all_columns, global_state.projection_ids);
    }
    else
    {
      output.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), output,
                                        data.lines_read - output_size, false);
    }

    output.Verify();
    state.chunk_offset += output.size();
  }

  static unique_ptr<NodeStatistics> take_flight_cardinality(ClientContext &context, const FunctionData *data)
  {
    // To estimate the cardinality of the flight, we can peek at the flight information
    // that was retrieved during the bind function.
    //
    // This estimate does not take into account any filters that may have been applied
    //
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    auto flight_estimated_records = bind_data.scan_data.get()->flight_info_->total_records();

    if (flight_estimated_records != -1)
    {
      return make_uniq<NodeStatistics>(flight_estimated_records);
    }
    return make_uniq<NodeStatistics>();
  }

  static void take_flight_complex_filter_pushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                                  vector<unique_ptr<Expression>> &filters)
  {
    auto allocator = AirportJSONAllocator(BufferAllocator::Get(context));

    auto alc = allocator.GetYYAlc();

    auto doc = AirportJSONCommon::CreateDocument(alc);
    auto result_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, result_obj);

    auto filters_arr = yyjson_mut_arr(doc);

    for (auto &f : filters)
    {
      auto serializer = AirportJsonSerializer(doc, true, true, true);
      f->Serialize(serializer);
      yyjson_mut_arr_append(filters_arr, serializer.GetRootObject());
    }

    yyjson_mut_obj_add_val(doc, result_obj, "filters", filters_arr);
    idx_t len;
    auto data = yyjson_mut_val_write_opts(
        result_obj,
        AirportJSONCommon::WRITE_FLAG,
        alc, reinterpret_cast<size_t *>(&len), nullptr);

    if (data == nullptr)
    {
      throw SerializationException(
          "Failed to serialize json, perhaps the query contains invalid utf8 characters?");
    }

    auto json_result = string(data, (size_t)len);

    auto &bind_data = bind_data_p->Cast<AirportTakeFlightBindData>();

    bind_data.json_filters = json_result;
  }

  static unique_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(const ArrowScanFunctionData &function,
                                                                     const vector<column_t> &column_ids, TableFilterSet *filters)
  {
    //! Generate Projection Pushdown Vector
    ArrowStreamParameters parameters;
    D_ASSERT(!column_ids.empty());
    for (idx_t idx = 0; idx < column_ids.size(); idx++)
    {
      auto col_idx = column_ids[idx];
      if (col_idx != COLUMN_IDENTIFIER_ROW_ID)
      {
        auto &schema = *function.schema_root.arrow_schema.children[col_idx];
        parameters.projected_columns.projection_map[idx] = schema.name;
        parameters.projected_columns.columns.emplace_back(schema.name);
        parameters.projected_columns.filter_to_col[idx] = col_idx;
      }
    }
    parameters.filters = filters;
    return function.scanner_producer(function.stream_factory_ptr, parameters);
  }

  static unique_ptr<GlobalTableFunctionState> AirportArrowScanInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input)
  {
    auto &bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();

    auto result = make_uniq<ArrowScanGlobalState>();

    std::unique_ptr<flight::FlightStreamReader> flight_stream;

    arrow::flight::FlightCallOptions call_options;
    call_options.headers.emplace_back("arrow-flight-user-agent", "duckdb-airport/0.0.1");
    // printf("Calling with filters: %s\n", bind_data.json_filters.c_str());
    call_options.headers.emplace_back("airport-duckdb-json-filters", bind_data.json_filters);

    if (!bind_data.auth_token.empty())
    {
      std::stringstream ss;
      ss << "Bearer " << bind_data.auth_token;
      call_options.headers.emplace_back("authorization", ss.str());
    }

    AIRPORT_ARROW_ASSIGN_OR_RAISE(
        flight_stream,
        bind_data.flight_client->DoGet(
            call_options,
            bind_data.scan_data->flight_info_->endpoints()[0].ticket));

    bind_data.scan_data->stream_ = std::move(flight_stream);

    result->stream = AirportProduceArrowScan(bind_data, input.column_ids, input.filters.get());

    // Since we're single threaded, we can only really use a single thread at a time.
    result->max_threads = 1;
    if (input.CanRemoveFilterColumns())
    {
      result->projection_ids = input.projection_ids;
      for (const auto &col_idx : input.column_ids)
      {
        if (col_idx == COLUMN_IDENTIFIER_ROW_ID)
        {
          result->scanned_types.emplace_back(LogicalTypeId::BIGINT);
        }
        else
        {
          result->scanned_types.push_back(bind_data.all_types[col_idx]);
        }
      }
    }
    return std::move(result);
  }

  void AddTakeFlightFunction(DatabaseInstance &instance)
  {
    auto take_flight_function = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::ANY},
        take_flight,
        take_flight_bind,
        AirportArrowScanInitGlobal,
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    // Add support for optional named paraemters that would be appended to the descriptor
    // of the flight, ideally parameters would be JSON encoded.

    take_flight_function.cardinality = take_flight_cardinality;
    take_flight_function.get_batch_index = nullptr;
    take_flight_function.projection_pushdown = true;
    take_flight_function.filter_pushdown = false;

    ExtensionUtil::RegisterFunction(instance, take_flight_function);
  }
}