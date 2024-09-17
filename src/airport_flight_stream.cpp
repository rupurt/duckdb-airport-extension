#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include <arrow/c/bridge.h>

#include <arrow/flight/client.h>
#include <arrow/flight/types.h>

#include <iostream>
#include <memory>

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/src/arrow_stream_buffer.cc

namespace duckdb
{

  /// Constructor
  AirportFlightStreamReader::AirportFlightStreamReader(
      const string &flight_server_location,
      std::shared_ptr<flight::FlightInfo> flight_info,
      std::shared_ptr<flight::FlightStreamReader> flight_stream)
      : flight_server_location_(flight_server_location), flight_info_(flight_info), flight_stream_(flight_stream) {}

  /// Get the schema
  std::shared_ptr<arrow::Schema> AirportFlightStreamReader::schema() const
  {
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema, flight_info_->GetSchema(&dictionary_memo), flight_server_location_, flight_info_->descriptor(), "");
    return info_schema;
  }

  /// Read the next record batch in the stream. Return null for batch when
  /// reaching end of stream
  arrow::Status AirportFlightStreamReader::ReadNext(
      std::shared_ptr<arrow::RecordBatch> *batch)
  {
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto chunk, flight_stream_.get()->Next(), flight_server_location_, flight_info_->descriptor(), "");
    if (!chunk.data)
    {
      // End of the stream has been reached.
      *batch = nullptr;
      return arrow::Status::OK();
    }

    *batch = chunk.data;
    return arrow::Status::OK();
  }

  /// Arrow array stream factory function
  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  AirportFlightStreamReader::CreateStream(uintptr_t buffer_ptr,
                                          ArrowStreamParameters &parameters)
  {
    assert(buffer_ptr != 0);

    auto buffer_data = reinterpret_cast<AirportTakeFlightScanData *>(buffer_ptr);

    // We're playing a trick here to recast the FlightStreamReader as a RecordBatchReader,
    // I'm not sure how else to do this.

    // If this doesn't work I can re-implement the ArrowArrayStreamWrapper
    // to take a FlightStreamReader instead of a RecordBatchReader.

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto reader, flight::MakeRecordBatchReader(buffer_data->stream_), buffer_data->flight_server_location_, buffer_data->flight_info_->descriptor(), "");

    // Create arrow stream
    auto stream_wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream.release = nullptr;

    auto maybe_ok = arrow::ExportRecordBatchReader(
        reader, &stream_wrapper->arrow_array_stream);

    if (!maybe_ok.ok())
    {
      if (stream_wrapper->arrow_array_stream.release)
      {
        stream_wrapper->arrow_array_stream.release(
            &stream_wrapper->arrow_array_stream);
      }
      return nullptr;
    }

    // Release the stream
    return stream_wrapper;
  }

  void AirportFlightStreamReader::GetSchema(uintptr_t buffer_ptr,
                                            duckdb::ArrowSchemaWrapper &schema)
  {
    assert(buffer_ptr != 0);
    // Rusty: this cast needs to be checked to make sure its valid.
    auto reader = reinterpret_cast<std::shared_ptr<AirportTakeFlightScanData> *>(buffer_ptr);

    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    const auto actual_reader = reader->get();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema, actual_reader->flight_info_->GetSchema(&dictionary_memo), actual_reader->flight_server_location_, actual_reader->flight_info_->descriptor(), "");

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(ExportSchema(*info_schema, &schema.arrow_schema), actual_reader->flight_server_location_, actual_reader->flight_info_->descriptor(), "ExportSchema");
  }
} // namespace duckdb
