#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/c/bridge.h"

#include "arrow/flight/client.h"

#include "duckdb/function/table/arrow.hpp"

namespace flight = arrow::flight;

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/include/duckdb/web/arrow_stream_buffer.h
namespace duckdb
{

  struct AirportTakeFlightScanData
  {
  public:
    AirportTakeFlightScanData(
        const string &flight_server_location,
        std::shared_ptr<flight::FlightInfo> flight_info,
        std::shared_ptr<flight::FlightStreamReader> stream) : flight_server_location_(flight_server_location), flight_info_(flight_info), stream_(stream), progress_(0) {}

    string flight_server_location_;
    std::shared_ptr<flight::FlightInfo> flight_info_;
    std::shared_ptr<arrow::flight::FlightStreamReader> stream_;
    double progress_;
  };

  struct AirportTakeFlightBindData : public ArrowScanFunctionData
  {
  public:
    using ArrowScanFunctionData::ArrowScanFunctionData;
    std::unique_ptr<AirportTakeFlightScanData> scan_data = nullptr;
    std::unique_ptr<arrow::flight::FlightClient> flight_client = nullptr;

    string server_location;
    string json_filters;

    // This is the trace id so that calls to GetFlightInfo and DoGet can be traced.
    string trace_id;

    // This is the auth token.
    string auth_token;
    mutable mutex lock;
  };

  struct AirportFlightStreamReader : public arrow::RecordBatchReader
  {
  protected:
    /// The buffer
    string flight_server_location_;
    std::shared_ptr<flight::FlightInfo> flight_info_;
    std::shared_ptr<flight::FlightStreamReader> flight_stream_;

  public:
    /// Constructor
    AirportFlightStreamReader(
        const string &flight_server_location,
        std::shared_ptr<flight::FlightInfo> flight_info,
        std::shared_ptr<flight::FlightStreamReader> flight_stream);

    /// Destructor
    ~AirportFlightStreamReader() = default;

    /// Get the schema
    std::shared_ptr<arrow::Schema> schema() const override;

    /// Read the next record batch in the stream. Return null for batch when
    /// reaching end of stream
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override;

    /// Create arrow array stream wrapper
    static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>

    CreateStream(uintptr_t buffer_ptr, duckdb::ArrowStreamParameters &parameters);

    /// Create arrow array stream wrapper
    static void GetSchema(uintptr_t buffer_ptr, duckdb::ArrowSchemaWrapper &schema);
  };

  class AirportArrowArrayStreamWrapper : public duckdb::ArrowArrayStreamWrapper
  {
  public:
    AirportArrowArrayStreamWrapper(const string &flight_server_location, const flight::FlightDescriptor &flight_descriptor) : flight_server_location_(flight_server_location), flight_descriptor_(flight_descriptor) {}

    shared_ptr<ArrowArrayWrapper> GetNextChunk();

  private:
    string flight_server_location_;
    flight::FlightDescriptor flight_descriptor_;
  };

} // namespace duckdb
