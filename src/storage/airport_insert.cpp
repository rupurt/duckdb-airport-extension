#include "storage/airport_insert.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "airport_extension.hpp"

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
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"

#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "yyjson.hpp"
#include "storage/airport_exchange.hpp"
#include "airport_macros.hpp"
#include "airport_headers.hpp"
#include "airport_exception.hpp"
#include "airport_secrets.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb
{

  AirportInsert::AirportInsert(LogicalOperator &op, TableCatalogEntry &table,
                               physical_index_vector_t<idx_t> column_index_map_p,
                               bool return_chunk)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
        column_index_map(std::move(column_index_map_p)), return_chunk(return_chunk)
  {
  }

  AirportInsert::AirportInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info,
                               bool return_chunk)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
        info(std::move(info)), return_chunk(return_chunk)
  {
  }

  //===--------------------------------------------------------------------===//
  // States
  //===--------------------------------------------------------------------===//
  class AirportInsertGlobalState : public GlobalSinkState, public AirportExchangeGlobalState
  {
  public:
    explicit AirportInsertGlobalState(
        ClientContext &context,
        AirportTableEntry *table,
        const vector<LogicalType> &return_types,
        bool return_chunk)
        : table(table), insert_count(0),
          return_collection(context, return_types), return_chunk(return_chunk)
    {
      if (table)
      {
        insert_chunk.Initialize(Allocator::Get(context), table->GetTypes());
      }
      else
      {
        throw NotImplementedException("AirportInsertGlobalState: table is null");
      }
    }

    AirportTableEntry *table;
    DataChunk insert_chunk;
    idx_t insert_count;
    mutex insert_lock;

    ColumnDataCollection return_collection;

    bool return_chunk;
  };

  class AirportInsertLocalState : public LocalSinkState
  {
  public:
    AirportInsertLocalState(ClientContext &context, const TableCatalogEntry &table)
    {
      insert_chunk.Initialize(Allocator::Get(context), table.GetTypes());
    }
    DataChunk insert_chunk;
  };

  static pair<vector<string>, vector<LogicalType>> AirportGetInsertColumns(const AirportInsert &insert, AirportTableEntry &entry)
  {
    vector<string> column_names;
    vector<LogicalType> column_types;
    auto &columns = entry.GetColumns();
    idx_t column_count;
    if (!insert.column_index_map.empty())
    {
      column_count = 0;
      vector<PhysicalIndex> column_indexes;
      column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
      for (idx_t c = 0; c < insert.column_index_map.size(); c++)
      {
        auto column_index = PhysicalIndex(c);
        auto mapped_index = insert.column_index_map[column_index];
        if (mapped_index == DConstants::INVALID_INDEX)
        {
          // column not specified
          continue;
        }
        column_indexes[mapped_index] = column_index;
        column_count++;
      }
      for (idx_t c = 0; c < column_count; c++)
      {
        auto &col = columns.GetColumn(column_indexes[c]);

        // Not only do I need the names and I need the types.
        column_types.push_back(col.Type());
        column_names.push_back(col.GetName());
      }
    }
    return make_pair(column_names, column_types);
  }

  unique_ptr<GlobalSinkState> AirportInsert::GetGlobalSinkState(ClientContext &context) const
  {
    AirportTableEntry *insert_table;
    if (!table)
    {
      auto &schema_ref = *schema.get_mutable();
      insert_table =
          &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<AirportTableEntry>();
    }
    else
    {
      insert_table = &table.get_mutable()->Cast<AirportTableEntry>();
    }

    auto insert_global_state = make_uniq<AirportInsertGlobalState>(context, insert_table, GetTypes(), return_chunk);

    // auto &transaction = AirportTransaction::Get(context, insert_table->catalog);
    // auto &connection = transaction.GetConnection();
    auto [send_names, send_types] = AirportGetInsertColumns(*this, *insert_table);

    insert_global_state->send_types = send_types;
    ArrowSchema send_schema;
    ArrowConverter::ToArrowSchema(&send_schema, insert_global_state->send_types, send_names,
                                  context.GetClientProperties());

    D_ASSERT(table != nullptr);

    AirportExchangeGetGlobalSinkState(context, *table.get(), *insert_table, insert_global_state.get(),
                                      send_schema, return_chunk, "insert");

    // auto format = insert_table->GetCopyFormat(context);
    // vector<string> insert_column_names;
    // if (!insert_columns.empty())
    // {
    //   for (auto &str : insert_columns)
    //   {
    //     auto index = insert_table->GetColumnIndex(str, true);
    //     if (!index.IsValid())
    //     {
    //       insert_column_names.push_back(str);
    //     }
    //     else
    //     {
    //       insert_column_names.push_back(insert_table->Airport_names[index.index]);
    //     }
    //   }
    // }

    // connection.BeginCopyTo(context, insert_global_state->copy_state, format, insert_table->schema.name, insert_table->name,
    //                        insert_column_names);
    return std::move(insert_global_state);
  }

  unique_ptr<LocalSinkState> AirportInsert::GetLocalSinkState(ExecutionContext &context) const
  {
    return make_uniq<AirportInsertLocalState>(context.client, *table.get());
  }

  //===--------------------------------------------------------------------===//
  // Sink
  //===--------------------------------------------------------------------===//
  SinkResultType AirportInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportInsertGlobalState>();
    auto &ustate = input.local_state.Cast<AirportInsertLocalState>();

    auto appender = make_uniq<ArrowAppender>(gstate.send_types, chunk.size(), context.client.GetClientProperties());
    appender->Append(chunk, 0, chunk.size(), chunk.size());
    ArrowArray arr = appender->Finalize();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, gstate.schema),
        gstate.table->table_data->location,
        gstate.flight_descriptor, "");

    // Acquire a lock because we don't want other threads to be writing to the same streams
    // at the same time.
    lock_guard<mutex> delete_guard(gstate.insert_lock);

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        gstate.writer->WriteRecordBatch(*record_batch),
        gstate.table->table_data->location,
        gstate.flight_descriptor, "");

    // Since we wrote a batch I'd like to read the data returned if we are returning chunks.
    if (gstate.return_chunk)
    {
      ustate.insert_chunk.Reset();

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
        ustate.insert_chunk.SetCardinality(state.chunk->arrow_array.length);

        ArrowTableFunction::ArrowToDuckDB(state,
                                          data.arrow_table.GetColumns(), ustate.insert_chunk, data.lines_read - output_size, false);
        ustate.insert_chunk.Verify();
        gstate.return_collection.Append(ustate.insert_chunk);
      }
    }

    return SinkResultType::NEED_MORE_INPUT;
  }

  //===--------------------------------------------------------------------===//
  // Finalize
  //===--------------------------------------------------------------------===//
  SinkFinalizeType AirportInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportInsertGlobalState>();

    // printf("AirportDelete::Finalize started, indicating that writing is done\n");
    auto flight_descriptor = gstate.table->table_data->flight_info->descriptor();

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        gstate.writer->DoneWriting(),
        gstate.table->table_data->location,
        gstate.flight_descriptor, "");

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
            yyjson_val *total_val = yyjson_obj_get(root, "total_inserted");
            if (total_val && yyjson_is_int(total_val))
            {
              gstate.insert_count = yyjson_get_int(total_val);
            }
          }
        }
        // Free the JSON document
        yyjson_doc_free(doc);
      }
    }

    return SinkFinalizeType::READY;
  }

  //===--------------------------------------------------------------------===//
  // Source
  //===--------------------------------------------------------------------===//
  class AirportInsertSourceState : public GlobalSourceState
  {
  public:
    explicit AirportInsertSourceState(const AirportInsert &op)
    {
      if (op.return_chunk)
      {
        D_ASSERT(op.sink_state);
        auto &g = op.sink_state->Cast<AirportInsertGlobalState>();
        g.return_collection.InitializeScan(scan_state);
      }
    }

    ColumnDataScanState scan_state;
  };

  unique_ptr<GlobalSourceState> AirportInsert::GetGlobalSourceState(ClientContext &context) const
  {
    return make_uniq<AirportInsertSourceState>(*this);
  }

  //===--------------------------------------------------------------------===//
  // GetData
  //===--------------------------------------------------------------------===//
  SourceResultType AirportInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                          OperatorSourceInput &input) const
  {
    auto &state = input.global_state.Cast<AirportInsertSourceState>();
    auto &g = sink_state->Cast<AirportInsertGlobalState>();
    if (!return_chunk)
    {
      chunk.SetCardinality(1);
      chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.insert_count)));
      return SourceResultType::FINISHED;
    }

    g.return_collection.Scan(state.scan_state, chunk);

    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
  }

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  string AirportInsert::GetName() const
  {
    return table ? "AIRPORT_INSERT" : "AIRPORT_CREATE_TABLE_AS";
  }

  InsertionOrderPreservingMap<string> AirportInsert::ParamsToString() const
  {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table ? table->name : info->Base().table;
    return result;
  }

  //===--------------------------------------------------------------------===//
  // Plan
  //===--------------------------------------------------------------------===//
  // unique_ptr<PhysicalOperator> AddCastToAirportTypes(ClientContext &context, unique_ptr<PhysicalOperator> plan)
  // {
  //   // check if we need to cast anything
  //   bool require_cast = false;
  //   auto &child_types = plan->GetTypes();
  //   for (auto &type : child_types)
  //   {
  //     auto Airport_type = AirportUtils::ToAirportType(type);
  //     if (Airport_type != type)
  //     {
  //       require_cast = true;
  //       break;
  //     }
  //   }
  //   if (require_cast)
  //   {
  //     vector<LogicalType> Airport_types;
  //     vector<unique_ptr<Expression>> select_list;
  //     for (idx_t i = 0; i < child_types.size(); i++)
  //     {
  //       auto &type = child_types[i];
  //       unique_ptr<Expression> expr;
  //       expr = make_uniq<BoundReferenceExpression>(type, i);

  //       auto Airport_type = AirportUtils::ToAirportType(type);
  //       if (Airport_type != type)
  //       {
  //         // add a cast
  //         expr = BoundCastExpression::AddCastToType(context, std::move(expr), Airport_type);
  //       }
  //       Airport_types.push_back(std::move(Airport_type));
  //       select_list.push_back(std::move(expr));
  //     }
  //     // we need to cast: add casts
  //     auto proj = make_uniq<PhysicalProjection>(std::move(Airport_types), std::move(select_list),
  //                                               plan->estimated_cardinality);
  //     proj->children.push_back(std::move(plan));
  //     plan = std::move(proj);
  //   }

  //   return plan;
  // }

  unique_ptr<PhysicalOperator> AirportCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                          unique_ptr<PhysicalOperator> plan)
  {
    // if (op.return_chunk)
    //{
    //   throw BinderException("RETURNING clause not yet supported for insertion into Airport table");
    // }

    if (op.action_type != OnConflictAction::THROW)
    {
      throw BinderException("ON CONFLICT clause not yet supported for insertion into Airport table");
    }

    //    plan = AddCastToAirportTypes(context, std::move(plan));

    auto insert = make_uniq<AirportInsert>(op, op.table, op.column_index_map, op.return_chunk);
    insert->children.push_back(std::move(plan));
    return std::move(insert);
  }

  // unique_ptr<PhysicalOperator> AirportCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
  //                                                                unique_ptr<PhysicalOperator> plan)
  // {
  //   // plan = AddCastToAirportTypes(context, std::move(plan));

  //   auto insert = make_uniq<AirportInsert>(op, op.schema, std::move(op.info));
  //   insert->children.push_back(std::move(plan));
  //   return std::move(insert);
  // }

}
