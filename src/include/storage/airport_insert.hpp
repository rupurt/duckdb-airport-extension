#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb
{

  class AirportInsert : public PhysicalOperator
  {
  public:
    //! INSERT INTO
    AirportInsert(LogicalOperator &op, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map, bool return_chunk);
    //! CREATE TABLE AS
    AirportInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info, bool return_chunk);

    //! The table to insert into
    optional_ptr<TableCatalogEntry> table;
    //! Table schema, in case of CREATE TABLE AS
    optional_ptr<SchemaCatalogEntry> schema;
    //! Create table info, in case of CREATE TABLE AS
    unique_ptr<BoundCreateTableInfo> info;
    //! column_index_map
    physical_index_vector_t<idx_t> column_index_map;

    bool return_chunk;

  public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override
    {
      return true;
    }

  public:
    // Sink interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    bool IsSink() const override
    {
      return true;
    }

    bool ParallelSink() const override
    {
      return false;
    }

    string GetName() const override;
    InsertionOrderPreservingMap<string> ParamsToString() const override;
  };

}
