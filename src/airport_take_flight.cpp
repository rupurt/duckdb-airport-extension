#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>

#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"
#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"
#include "airport_secrets.hpp"
#include "airport_exception.hpp"

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

  struct TakeFlightParameters
  {
    string server_location;
    string auth_token;
    string secret_name;
  };

  static TakeFlightParameters ParseTakeFlightParameters(
      string &server_location,
      ClientContext &context,
      TableFunctionBindInput &input)
  {
    TakeFlightParameters params;

    params.server_location = server_location;

    for (auto &kv : input.named_parameters)
    {
      auto loption = StringUtil::Lower(kv.first);
      if (loption == "auth_token")
      {
        params.auth_token = StringValue::Get(kv.second);
      }
      else if (loption == "secret")
      {
        params.secret_name = StringValue::Get(kv.second);
      }
    }

    params.auth_token = AirportAuthTokenForLocation(context, params.server_location, params.secret_name, params.auth_token);
    return params;
  }

  static unique_ptr<FunctionData> take_flight_bind_with_descriptor(
      TakeFlightParameters &take_flight_params,
      flight::FlightDescriptor &descriptor,
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto location, flight::Location::Parse(take_flight_params.server_location),
                                                       take_flight_params.server_location,
                                                       descriptor,
                                                       "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_client,
                                                       flight::FlightClient::Connect(location),
                                                       take_flight_params.server_location,
                                                       descriptor,
                                                       "");

    arrow::flight::FlightCallOptions call_options;
    call_options.headers.emplace_back("airport-user-agent", AIRPORT_USER_AGENT);
    if (!take_flight_params.auth_token.empty())
    {
      std::stringstream ss;
      ss << "Bearer " << take_flight_params.auth_token;
      call_options.headers.emplace_back("authorization", ss.str());
    }

    // Get the information about the flight, this will allow the
    // endpoint information to be returned.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_info,
                                                       flight_client->GetFlightInfo(call_options, descriptor),
                                                       take_flight_params.server_location,
                                                       descriptor,
                                                       "");

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
        take_flight_params.server_location,
        std::move(flight_info),
        nullptr);

    auto ret = make_uniq<AirportTakeFlightBindData>(
        (stream_factory_produce_t)&AirportFlightStreamReader::CreateStream,
        (uintptr_t)scan_data.get());

    ret->scan_data = std::move(scan_data);
    ret->flight_client = std::move(flight_client);
    ret->auth_token = take_flight_params.auth_token;
    ret->server_location = take_flight_params.server_location;

    auto &data = *ret;

    // Convert the C++ schema into the C format schema, but store it on the bind
    // information
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema,
                                                       ret->scan_data->flight_info_->GetSchema(&dictionary_memo),
                                                       take_flight_params.server_location,
                                                       descriptor,
                                                       "");

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(ExportSchema(*info_schema, &data.schema_root.arrow_schema), take_flight_params.server_location, descriptor, "ExportSchema");

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

  static unique_ptr<FunctionData> take_flight_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    auto params = ParseTakeFlightParameters(server_location, context, input);
    auto descriptor = flight_descriptor_from_value(input.inputs[1]);

    return take_flight_bind_with_descriptor(
        params,
        descriptor,
        context,
        input, return_types, names);
  }

  static unique_ptr<FunctionData> take_flight_bind_with_pointer(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    auto params = ParseTakeFlightParameters(server_location, context, input);

    if (input.inputs[1].IsNull())
    {
      throw BinderException("airport: take_flight_with_pointer, pointers to flight descriptor cannot be null");
    }
    auto descriptor = *reinterpret_cast<flight::FlightDescriptor *>(input.inputs[1].GetPointer());

    return take_flight_bind_with_descriptor(
        params,
        descriptor,
        context,
        input,
        return_types,
        names);
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
    return make_uniq<NodeStatistics>(100000);
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
      auto serializer = AirportJsonSerializer(doc, false, false, false);
      f->Serialize(serializer);
      yyjson_mut_arr_append(filters_arr, serializer.GetRootObject());
    }

    yyjson_mut_val *column_id_names = yyjson_mut_arr(doc);
    for (auto id : get.GetColumnIds())
    {
      yyjson_mut_arr_add_str(doc, column_id_names, get.names[id].c_str());
    }

    yyjson_mut_obj_add_val(doc, result_obj, "filters", filters_arr);
    yyjson_mut_obj_add_val(doc, result_obj, "column_binding_names_by_index", column_id_names);
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

    if (filters)
    {
      auto allocator = AirportJSONAllocator(Allocator::DefaultAllocator());
      auto alc = allocator.GetYYAlc();

      auto doc = AirportJSONCommon::CreateDocument(alc);
      auto result_obj = yyjson_mut_obj(doc);
      yyjson_mut_doc_set_root(doc, result_obj);
      auto filters_arr = yyjson_mut_arr(doc);

      auto serializer = AirportJsonSerializer(doc, true, true, true);
      filters->Serialize(serializer);
      yyjson_mut_arr_append(filters_arr, serializer.GetRootObject());

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
    }

    return function.scanner_producer(function.stream_factory_ptr, parameters);
  }

  std::string join_vector(const std::vector<idx_t> &vec)
  {
    if (vec.empty())
      return "";

    std::ostringstream oss;
    auto it = vec.begin();
    oss << *it; // Add the first element

    for (++it; it != vec.end(); ++it)
    {
      oss << ',' << *it;
    }

    return oss.str();
  }

  static string CompressString(const string &input, const string &location, const flight::FlightDescriptor &descriptor)
  {
    auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 1).ValueOrDie();

    // Estimate the maximum compressed size (usually larger than original size)
    int64_t max_compressed_len = codec->MaxCompressedLen(input.size(), reinterpret_cast<const uint8_t *>(input.data()));

    // Allocate a buffer to hold the compressed data

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_buffer, arrow::AllocateBuffer(max_compressed_len), location, descriptor, "");

    // Perform the compression
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_size,
                                                       codec->Compress(
                                                           input.size(),
                                                           reinterpret_cast<const uint8_t *>(input.data()),
                                                           max_compressed_len,
                                                           compressed_buffer->mutable_data()),
                                                       location, descriptor, "");

    // If you want to write the compressed data to a string
    std::string compressed_str(reinterpret_cast<const char *>(compressed_buffer->data()), compressed_size);
    return compressed_str;
  }

  static string BuildDynamicTicketData(const string &json_filters, const string &column_ids, uint32_t *uncompressed_length, const string &location, const flight::FlightDescriptor &descriptor)
  {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    // Add key-value pairs to the JSON object
    if (!json_filters.empty())
    {
      yyjson_mut_obj_add_str(doc, root, "airport-duckdb-json-filters", json_filters.c_str());
    }
    if (!column_ids.empty())
    {
      yyjson_mut_obj_add_str(doc, root, "airport-duckdb-column-ids", column_ids.c_str());
    }

    // Serialize the JSON document to a string
    char *metadata_str = yyjson_mut_write(doc, 0, nullptr);

    auto metadata_doc_string = string(metadata_str);

    *uncompressed_length = metadata_doc_string.size();

    auto compressed_metadata = CompressString(metadata_doc_string, location, descriptor);
    free(metadata_str);
    yyjson_mut_doc_free(doc);

    return compressed_metadata;
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
    //    call_options.headers.emplace_back("airport-duckdb-json-filters", bind_data.json_filters);

    auto joined_column_ids = join_vector(input.column_ids);
    //    call_options.headers.emplace_back("airport-duckdb-column-ids", joined_column_ids);

    if (!bind_data.auth_token.empty())
    {
      std::stringstream ss;
      ss << "Bearer " << bind_data.auth_token;
      call_options.headers.emplace_back("authorization", ss.str());
    }

    // Rather than using the headers, check to see if the ticket starts with <TICKET_ALLOWS_METADATA>

    auto server_ticket = bind_data.scan_data->flight_info_->endpoints()[0].ticket;
    auto server_ticket_contents = server_ticket.ticket;
    if (server_ticket_contents.find("<TICKET_ALLOWS_METADATA>") == 0)
    {
      // This ticket allows metadata to be supplied in the ticket.

      auto ticket_without_preamble = server_ticket_contents.substr(strlen("<TICKET_ALLOWS_METADATA>"));

      // encode the length as a unsigned int32 in network byte order
      uint32_t ticket_length = ticket_without_preamble.size();
      auto ticket_length_bytes = std::string((char *)&ticket_length, sizeof(ticket_length));

      uint32_t uncompressed_length;
      auto dynamic_ticket = BuildDynamicTicketData(bind_data.json_filters, joined_column_ids, &uncompressed_length, bind_data.server_location,
                                                   bind_data.scan_data->flight_info_->descriptor());

      auto compressed_length_bytes = std::string((char *)&uncompressed_length, sizeof(uncompressed_length));

      auto manipulated_ticket_data = "<TICKET_WITH_METADATA>" + ticket_length_bytes + ticket_without_preamble + compressed_length_bytes + dynamic_ticket;

      server_ticket = flight::Ticket{manipulated_ticket_data};
    }

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        flight_stream,
        bind_data.flight_client->DoGet(
            call_options,
            server_ticket),
        bind_data.server_location,
        bind_data.scan_data->flight_info_->descriptor(),
        "");

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

  static double take_flight_scan_progress(ClientContext &, const FunctionData *data, const GlobalTableFunctionState *global_state)
  {
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    lock_guard<mutex> guard(bind_data.lock);

    return bind_data.scan_data->progress_ * 100.0;
  }

  void AddTakeFlightFunction(DatabaseInstance &instance)
  {

    auto take_flight_function_set = TableFunctionSet("airport_take_flight");

    auto take_flight_function_with_descriptor = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::ANY},
        take_flight,
        take_flight_bind,
        AirportArrowScanInitGlobal,
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function_with_descriptor.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    // Add support for optional named paraemters that would be appended to the descriptor
    // of the flight, ideally parameters would be JSON encoded.

    take_flight_function_with_descriptor.cardinality = take_flight_cardinality;
    take_flight_function_with_descriptor.get_batch_index = nullptr;
    take_flight_function_with_descriptor.projection_pushdown = true;
    take_flight_function_with_descriptor.filter_pushdown = false;
    take_flight_function_with_descriptor.table_scan_progress = take_flight_scan_progress;
    take_flight_function_set.AddFunction(take_flight_function_with_descriptor);

    auto take_flight_function_with_pointer = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::POINTER},
        take_flight,
        take_flight_bind_with_pointer,
        AirportArrowScanInitGlobal,
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function_with_pointer.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    // Add support for optional named paraemters that would be appended to the descriptor
    // of the flight, ideally parameters would be JSON encoded.

    take_flight_function_with_pointer.cardinality = take_flight_cardinality;
    take_flight_function_with_pointer.get_batch_index = nullptr;
    take_flight_function_with_pointer.projection_pushdown = true;
    take_flight_function_with_pointer.filter_pushdown = false;
    take_flight_function_with_pointer.table_scan_progress = take_flight_scan_progress;

    take_flight_function_set.AddFunction(take_flight_function_with_pointer);

    ExtensionUtil::RegisterFunction(instance, take_flight_function_set);
  }
}