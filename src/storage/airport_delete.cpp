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
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

// Some improvements to make
//
// The global state needs to accumulate data chunks that are returned by the local
// returned delete calls. This is because the data is returned in chunks and we need
// to process it.
//
// It seems that upon delete all columns of the table are returned, but it seems reasonable.
//
// We need to keep a local state and a global state.
//
// Reference physical_delete.cpp for ideas around the implementation.
//
// Need to add the code to read the returned chunks for the DoExchange call, which means we'll
// be dealing with ArrowScan again, but hopefully in a more limited way since we're just
// dealing with DataChunks, but it could be more since we aren't just faking a function call.
//
// Transactional Guarantees:
//
// There really won't be many guarantees - since all row ids can't be pushed in one call
// it could really be up to the server to determine if the operation succeeded.
//
// DoExchange could just be used for a chunked delete, and then finally a commit or rollback
// action is sent at the end of the calls. But this could be really hard on the remote server to
// implement, since it would have to deal with transactional problems.
//
// Is there some way to keep at the flight stream to determine if there is data to read on the stream?
// If so it could be a single DoExchange call.
//
// Could be simulated with flow control with metadata messages, but need to cast a metadata reader
// rather than just a stream reader.
//
//

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

  AirportDelete::AirportDelete(LogicalOperator &op, TableCatalogEntry &table, idx_t row_id_index, bool return_chunk)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table), row_id_index(row_id_index), return_chunk(return_chunk)
  {
  }

  class AirportDeleteLocalState : public LocalSinkState
  {
  public:
    AirportDeleteLocalState(ClientContext &context, TableCatalogEntry &table)
    //                            const vector<unique_ptr<BoundConstraint>> &bound_constraints)
    {
      delete_chunk.Initialize(Allocator::Get(context), table.GetTypes());
      //		delete_state = table.GetStorage().InitializeDelete(table, context, bound_constraints);
    }
    DataChunk delete_chunk;
    //    unique_ptr<TableDeleteState> delete_state;
  };

  struct AirportDeleteTakeFlightBindData : public ArrowScanFunctionData
  {
  public:
    using ArrowScanFunctionData::ArrowScanFunctionData;
    std::unique_ptr<AirportTakeFlightScanData> scan_data = nullptr;
    std::unique_ptr<arrow::flight::FlightClient> flight_client = nullptr;

    string server_location;
    string json_filters;

    // This is the trace id so that calls to GetFlightInfo and DoGet can be traced.
    string trace_id;

    idx_t row_id_column_index = COLUMN_IDENTIFIER_ROW_ID;

    // This is the auth token.
    string auth_token;
    mutable mutex lock;

    vector<string> names;
    vector<LogicalType> return_types;
  };

  class AirportDeleteGlobalState : public GlobalSinkState
  {
  public:
    explicit AirportDeleteGlobalState(
        ClientContext &context,
        AirportTableEntry &table,
        const vector<LogicalType> &return_types,
        bool return_chunk) : deleted_count(0), return_chunk(return_chunk), table(table),
                             return_collection(context, return_types)
    {
    }

    mutex delete_lock;
    idx_t deleted_count;

    // Is there any data requested to be returned.
    bool return_chunk;

    AirportTableEntry &table;
    std::shared_ptr<arrow::Schema> schema;
    arrow::flight::FlightDescriptor flight_descriptor;

    // These are the streams that talk to the Flight server.
    // std::unique_ptr<arrow::flight::FlightStreamReader> reader;

    std::unique_ptr<AirportDeleteTakeFlightBindData> scan_bind_data;
    std::unique_ptr<ArrowArrayStreamWrapper> reader;
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;

    duckdb::unique_ptr<TableFunctionInput> scan_table_function_input;

    duckdb::unique_ptr<GlobalTableFunctionState> scan_global_state;
    duckdb::unique_ptr<LocalTableFunctionState> scan_local_state;

    vector<LogicalType> send_types;

    ColumnDataCollection return_collection;

    void Flush(ClientContext &context)
    {
    }
  };

  int findIndex(const std::vector<std::string> &vec, const std::string &target)
  {
    auto it = std::find(vec.begin(), vec.end(), target);

    if (it == vec.end())
    {
      throw std::runtime_error("String not found in vector");
    }

    return std::distance(vec.begin(), it);
  }

  unique_ptr<GlobalSinkState> AirportDelete::GetGlobalSinkState(ClientContext &context) const
  {
    auto &airport_table = table.Cast<AirportTableEntry>();
    // auto &transaction = AirportTransaction::Get(context, airport_table.catalog);

    auto delete_global_state = make_uniq<AirportDeleteGlobalState>(context, airport_table, GetTypes(), return_chunk);

    delete_global_state->send_types = {airport_table.GetRowIdType()};
    vector<string> send_names = {"row_id"};
    ArrowSchema send_schema;
    ArrowConverter::ToArrowSchema(&send_schema, delete_global_state->send_types, send_names,
                                  context.GetClientProperties());
    // Create the C++ schema.
    delete_global_state->schema = arrow::ImportSchema(&send_schema).ValueOrDie();
    delete_global_state->return_chunk = return_chunk;
    delete_global_state->flight_descriptor = airport_table.table_data->flight_info->descriptor();

    auto auth_token = AirportAuthTokenForLocation(context, airport_table.table_data->location, "", "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_location, flight::Location::Parse(airport_table.table_data->location),
                                                       airport_table.table_data->location,
                                                       delete_global_state->flight_descriptor, "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_client,
                                                       flight::FlightClient::Connect(flight_location),
                                                       airport_table.table_data->location,
                                                       delete_global_state->flight_descriptor, "");

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
    call_options.headers.emplace_back("airport-operation", "delete");

    // Indicate if the caller is interested in data being returned.
    call_options.headers.emplace_back("delete-return-chunks", return_chunk ? "1" : "0");

    if (delete_global_state->flight_descriptor.type == arrow::flight::FlightDescriptor::PATH)
    {
      auto path_parts = delete_global_state->flight_descriptor.path;
      std::string joined_path_parts = join_vector_of_strings(path_parts, '/');
      call_options.headers.emplace_back("airport-flight-path", joined_path_parts);
    }

    // Need to make this call so its possible to be stored in the scan data below.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_info,
                                                       flight_client->GetFlightInfo(call_options, delete_global_state->flight_descriptor),
                                                       airport_table.table_data->location,
                                                       delete_global_state->flight_descriptor,
                                                       "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto exchange_result,
        flight_client->DoExchange(call_options, delete_global_state->flight_descriptor),
        airport_table.table_data->location,
        delete_global_state->flight_descriptor, "");

    // Tell the server the schema that we will be using to write data.
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        exchange_result.writer->Begin(delete_global_state->schema),
        airport_table.table_data->location,
        delete_global_state->flight_descriptor,
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

    auto scan_bind_data = make_uniq<AirportDeleteTakeFlightBindData>(
        (stream_factory_produce_t)&AirportFlightStreamReader::CreateStream,
        (uintptr_t)scan_data.get());

    scan_bind_data->scan_data = std::move(scan_data);
    scan_bind_data->flight_client = std::move(flight_client);
    scan_bind_data->server_location = airport_table.table_data->location;
    scan_bind_data->trace_id = trace_uuid;

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto read_schema,
                                                       scan_bind_data->scan_data->stream_->GetSchema(),
                                                       airport_table.table_data->location,
                                                       delete_global_state->flight_descriptor, "");

    auto &data = *scan_bind_data;
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        ExportSchema(*read_schema, &data.schema_root.arrow_schema),
        airport_table.table_data->location,
        delete_global_state->flight_descriptor,
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
    delete_global_state->scan_global_state = std::move(scan_global_state);

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

    delete_global_state->scan_local_state = std::move(scan_local_state);

    // Create a parameter is the commonly passed to the other functions.
    delete_global_state->scan_bind_data = std::move(scan_bind_data);
    delete_global_state->writer = std::move(exchange_result.writer);

    delete_global_state->scan_table_function_input = make_uniq<TableFunctionInput>(
        delete_global_state->scan_bind_data.get(),
        delete_global_state->scan_local_state.get(),
        delete_global_state->scan_global_state.get());

    return std::move(delete_global_state);
  }

  unique_ptr<LocalSinkState> AirportDelete::GetLocalSinkState(ExecutionContext &context) const
  {
    return make_uniq<AirportDeleteLocalState>(context.client, table);
  }

  //===--------------------------------------------------------------------===//
  // Sink
  //===--------------------------------------------------------------------===//
  SinkResultType AirportDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportDeleteGlobalState>();
    auto &ustate = input.local_state.Cast<AirportDeleteLocalState>();

    // Since we need to return the data from the rows that we're deleting.
    // we need do exchanges with the server chunk by chunk because if we batch everything
    // up it could use a lot of memory and we wouldn't be able to return the data
    // to the user.
    auto appender = make_uniq<ArrowAppender>(gstate.send_types, chunk.size(), context.client.GetClientProperties());
    appender->Append(chunk, 0, chunk.size(), chunk.size());
    ArrowArray arr = appender->Finalize();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, gstate.schema),
        gstate.table.table_data->location,
        gstate.flight_descriptor, "");

    // Acquire a lock because we don't want other threads to be writing to the same streams
    // at the same time.
    lock_guard<mutex> delete_guard(gstate.delete_lock);

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        gstate.writer->WriteRecordBatch(*record_batch),
        gstate.table.table_data->location,
        gstate.flight_descriptor, "");

    // Since we wrote a batch I'd like to read the data returned if we are returning chunks.
    if (gstate.return_chunk)
    {
      ustate.delete_chunk.Reset();

      {
        auto &data = gstate.scan_table_function_input->bind_data->CastNoConst<ArrowScanFunctionData>(); // FIXME
        auto &state = gstate.scan_table_function_input->local_state->Cast<ArrowScanLocalState>();
        auto &global_state = gstate.scan_table_function_input->global_state->Cast<ArrowScanGlobalState>();

        state.Reset();

        auto current_chunk = global_state.stream->GetNextChunk();
        state.chunk = std::move(current_chunk);

        auto output_size =
            MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
        data.lines_read += output_size;
        ustate.delete_chunk.SetCardinality(state.chunk->arrow_array.length);

        // Assume that the data returned is the same size as the table.
        //        D_ASSERT(data.arrow_table.GetColumns().size() == ustate.delete_chunk.ColumnCount());

        ArrowTableFunction::ArrowToDuckDB(state,
                                          data.arrow_table.GetColumns(), ustate.delete_chunk, data.lines_read - output_size, false);
        ustate.delete_chunk.Verify();
        gstate.return_collection.Append(ustate.delete_chunk);
      }
    }
    return SinkResultType::NEED_MORE_INPUT;
  }

  //===--------------------------------------------------------------------===//
  // Finalize
  //===--------------------------------------------------------------------===//
  SinkFinalizeType AirportDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportDeleteGlobalState>();

    // printf("AirportDelete::Finalize started, indicating that writing is done\n");
    auto flight_descriptor = gstate.table.table_data->flight_info->descriptor();

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        gstate.writer->DoneWriting(),
        gstate.table.table_data->location,
        gstate.flight_descriptor, "");

    // There should be a metadata message in the reader stream
    // but the problem is the current interface just reads data
    // chunks, and drops the metadata silently.
    //

    {
      //      auto &data = gstate.scan_table_function_input->bind_data->CastNoConst<ArrowScanFunctionData>(); // FIXME
      auto &state = gstate.scan_table_function_input->local_state->Cast<ArrowScanLocalState>();
      auto &global_state = gstate.scan_table_function_input->global_state->Cast<ArrowScanGlobalState>();

      state.Reset();

      auto current_chunk = global_state.stream->GetNextChunk();
      state.chunk = std::move(current_chunk);

      if (!gstate.scan_bind_data->scan_data->last_app_metadata_.empty())
      {
        auto metadata = *&gstate.scan_bind_data->scan_data->last_app_metadata_;

        // Try to parse the metadata
        // Try to parse out a JSON document that contains a progress indicator
        // that will update the scan data.

        yyjson_doc *doc = yyjson_read((const char *)metadata.data(), metadata.size(), 0);
        if (doc)
        {
          // Get the root object
          yyjson_val *root = yyjson_doc_get_root(doc);
          if (root && yyjson_is_obj(root))
          {
            yyjson_val *total_deleted_val = yyjson_obj_get(root, "total_deleted");
            if (total_deleted_val && yyjson_is_int(total_deleted_val))
            {
              gstate.deleted_count = yyjson_get_int(total_deleted_val);
            }
          }
        }
        // Free the JSON document
        yyjson_doc_free(doc);
      }
    }

    gstate.Flush(context);
    return SinkFinalizeType::READY;
  }

  //===--------------------------------------------------------------------===//
  // Source
  //===--------------------------------------------------------------------===//
  class AirportDeleteSourceState : public GlobalSourceState
  {
  public:
    explicit AirportDeleteSourceState(const AirportDelete &op)
    {
      if (op.return_chunk)
      {
        D_ASSERT(op.sink_state);
        auto &g = op.sink_state->Cast<AirportDeleteGlobalState>();
        g.return_collection.InitializeScan(scan_state);
      }
    }

    ColumnDataScanState scan_state;
  };

  unique_ptr<GlobalSourceState> AirportDelete::GetGlobalSourceState(ClientContext &context) const
  {
    return make_uniq<AirportDeleteSourceState>(*this);
  }

  //===--------------------------------------------------------------------===//
  // GetData
  //===--------------------------------------------------------------------===//
  SourceResultType AirportDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                          OperatorSourceInput &input) const
  {
    auto &state = input.global_state.Cast<AirportDeleteSourceState>();
    auto &g = sink_state->Cast<AirportDeleteGlobalState>();
    if (!return_chunk)
    {
      chunk.SetCardinality(1);
      chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.deleted_count)));
      return SourceResultType::FINISHED;
    }

    g.return_collection.Scan(state.scan_state, chunk);

    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
  }

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  string AirportDelete::GetName() const
  {
    return "AIRPORT_DELETE";
  }

  InsertionOrderPreservingMap<string> AirportDelete::ParamsToString() const
  {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table.name;
    return result;
  }

  //===--------------------------------------------------------------------===//
  // Plan
  //===--------------------------------------------------------------------===//
  unique_ptr<PhysicalOperator> AirportCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                          unique_ptr<PhysicalOperator> plan)
  {
    auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
    // AirportCatalog::MaterializeAirportScans(*plan);
    auto del = make_uniq<AirportDelete>(op, op.table, bound_ref.index, op.return_chunk);
    del->children.push_back(std::move(plan));
    return std::move(del);
  }

} // namespace duckdb
