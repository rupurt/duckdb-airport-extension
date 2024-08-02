#define DUCKDB_EXTENSION_MAIN

#include "airport_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "duckdb/function/table/arrow.hpp"

#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/c/bridge.h>

#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"


namespace flight = arrow::flight;

namespace duckdb {

struct ListFlightsBindData : public TableFunctionData {
//    unique_ptr<string_t> connection_details;
    string json_filters;
    string criteria;
    std::unique_ptr<flight::FlightClient> flight_client;
};

struct ListFlightsGlobalState : public GlobalTableFunctionState {
public:
    std::unique_ptr<flight::FlightListing> listing;

    ListFlightsGlobalState() {

    };

    idx_t MaxThreads() const override {
        return 1;
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
        return make_uniq<ListFlightsGlobalState>();
    }
};


static unique_ptr<FunctionData> take_flight_bind(
        ClientContext &context,
        TableFunctionBindInput &input,
        vector<LogicalType> &return_types,
        vector<string> &names) {

    // FIXME: make the location variable.
    auto server_location = input.inputs[0].ToString();
    AIRPORT_ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::Parse(server_location));

    auto flight_descriptor = input.inputs[1];

    arrow::flight::FlightDescriptor descriptor;

    switch (flight_descriptor.type().id()) {
        case LogicalTypeId::BLOB:
        case LogicalTypeId::VARCHAR: {
            descriptor = arrow::flight::FlightDescriptor::Command(flight_descriptor.ToString());
        }
        break;
        case LogicalTypeId::LIST: {
            auto &list_values = ListValue::GetChildren(flight_descriptor);
            vector<string> components;
            for (idx_t i = 0; i < list_values.size(); i++)
            {
                auto &child = list_values[i];
                if (child.type().id() != LogicalTypeId::VARCHAR) {
                    throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
                }
                components.emplace_back(child.ToString());
            }
            descriptor = arrow::flight::FlightDescriptor::Path(components);
        }
        break;
        case LogicalTypeId::ARRAY:
        {
            auto &array_values = ArrayValue::GetChildren(flight_descriptor);
            vector<string> components;
            for (idx_t i = 0; i < array_values.size(); i++) {
                auto &child = array_values[i];
                if (child.type().id() != LogicalTypeId::VARCHAR) {
                    throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
                }
                components.emplace_back(child.ToString());
            }
            descriptor = arrow::flight::FlightDescriptor::Path(components);
        }
        break;
        // FIXME: deal with the union type returned by Arrow list flights.
        default:
            throw InvalidInputException("airport_take_flight: unknown descriptor type passed");
    }


    // To actually get the information about the flight, we need to either call
    // GetFlightInfo or DoGet.
    AIRPORT_ARROW_ASSIGN_OR_RAISE(auto flight_client, flight::FlightClient::Connect(location));

    // Get the information about the flight, this will allow the
    // endpoint information to be returned.
    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    AIRPORT_ARROW_ASSIGN_OR_RAISE(flight_info, flight_client->GetFlightInfo(descriptor));

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

    // FIXME: need to move this call to getting the flight info after the bind
    // because the filters won't be populated until the bind is complete.

    // Start the stream here on the bind.
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    AIRPORT_ARROW_ASSIGN_OR_RAISE(stream, flight_client->DoGet(flight_info->endpoints()[0].ticket));

    auto scan_data = make_uniq<AirportTakeFlightScanData>(
        std::move(flight_info),
        std::move(stream)
    );

    assert(!stream);
    assert(!flight_info);

    auto stream_factory_produce =
        (stream_factory_produce_t)&AirportFlightStreamReader::CreateStream;

    auto stream_factory_ptr = (uintptr_t)scan_data.get();

    auto ret = make_uniq<AirportTakeFlightScanFunctionData>(stream_factory_produce,
                                                            stream_factory_ptr);

    // The flight_data now owns the scan_data.
    ret->flight_data = std::move(scan_data);

    assert(!scan_data);

    auto &data = *ret;

    // Convert the C++ schema into the C format schema, but store it on the bind
    // information
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_ARROW_ASSIGN_OR_RAISE(info_schema, ret->flight_data->flight_info_->GetSchema(&dictionary_memo));

    AIRPORT_ARROW_ASSERT_OK(ExportSchema(*info_schema, &data.schema_root.arrow_schema));

    for (idx_t col_idx = 0;
        col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
        auto &schema = *data.schema_root.arrow_schema.children[col_idx];
        if (!schema.release) {
            throw InvalidInputException("airport_take_flight: released schema passed");
        }
        auto arrow_type = ArrowTableFunction::GetArrowLogicalType(schema);
        if (schema.dictionary) {
            auto dictionary_type = ArrowTableFunction::GetArrowLogicalType(*schema.dictionary);
            return_types.emplace_back(dictionary_type->GetDuckType());
            arrow_type->SetDictionary(std::move(dictionary_type));
        } else {
            return_types.emplace_back(arrow_type->GetDuckType());
        }
        ret->arrow_table.AddColumn(col_idx, std::move(arrow_type));
        auto format = string(schema.format);
        auto name = string(schema.name);
        if (name.empty()) {
            name = string("v") + to_string(col_idx);
        }
        names.push_back(name);
    }
    QueryResult::DeduplicateColumns(names);
    return std::move(ret);
}

static void take_flight(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  if (!data_p.local_state) {
      return;
  }
  auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
  auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
  auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

  //! Out of tuples in this chunk
  if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
    if (!ArrowTableFunction::ArrowScanParallelStateNext(context, data_p.bind_data.get(), state,
                                    global_state)) {
      return;
    }
  }
  int64_t output_size =
      MinValue<int64_t>(STANDARD_VECTOR_SIZE,
                        state.chunk->arrow_array.length - state.chunk_offset);
  data.lines_read += output_size;

  if (global_state.CanRemoveFilterColumns()) {
    state.all_columns.Reset();
    state.all_columns.SetCardinality(output_size);
    ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns,
                  data.lines_read - output_size, false);
    output.ReferenceColumns(state.all_columns, global_state.projection_ids);
  } else {
    output.SetCardinality(output_size);
    ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), output,
                  data.lines_read - output_size, false);
  }

  output.Verify();
  state.chunk_offset += output.size();
}

static unique_ptr<FunctionData> list_flights_bind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {

    auto server_location = input.inputs[0].ToString();
    auto criteria = input.inputs[1].ToString();

    auto ret = make_uniq<ListFlightsBindData>();

    AIRPORT_ARROW_ASSIGN_OR_RAISE(auto location,
                          flight::Location::Parse(server_location));

    AIRPORT_ARROW_ASSIGN_OR_RAISE(ret->flight_client, flight::FlightClient::Connect(location));
    ret->criteria = criteria;

    // ordered - boolean
    // total_records - BIGINT
    // total_bytes - BIGINT
    // metadata - bytes

    child_list_t<LogicalType> flight_descriptor_members = {
        {"cmd", LogicalType::BLOB},
        {"path", LogicalType::LIST(LogicalType::VARCHAR)}
    };

    auto endpoint_type = LogicalType::STRUCT({
        {"ticket", LogicalType::BLOB},
        {"location", LogicalType::LIST(LogicalType::VARCHAR)},
        {"expiration_time", LogicalType::TIMESTAMP},
        {"app_metadata", LogicalType::BLOB}
    });


    std::initializer_list<duckdb::LogicalType> table_types = {
        LogicalType::UNION(flight_descriptor_members),
        LogicalType::LIST(endpoint_type),
        LogicalType::BOOLEAN,
        LogicalType::BIGINT,
        LogicalType::BIGINT,
        LogicalType::BLOB,
        LogicalType::VARCHAR
    };
    return_types.insert(return_types.end(), table_types.begin(), table_types.end());

    auto list_flights_field_names = {
        "flight_descriptor",
        "endpoint",
        "ordered",
        "total_records",
        "total_bytes",
        "app_metadata",
        "schema"
    };
    names.insert(names.end(), list_flights_field_names.begin(), list_flights_field_names.end());

	return std::move(ret);
}


static void list_flights(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<ListFlightsBindData>();
	auto &global_state = data.global_state->Cast<ListFlightsGlobalState>();

    if(global_state.listing == nullptr) {
        // Now send a list flights request.
        arrow::flight::FlightCallOptions call_options;
        call_options.headers.emplace_back("arrow-flight-user-agent", "duckdb-airport/0.0.1");
        call_options.headers.emplace_back("airport-duckdb-json-filters", bind_data.json_filters);
        printf("Calling with filters: %s\n", bind_data.json_filters.c_str());

        AIRPORT_ARROW_ASSIGN_OR_RAISE(global_state.listing, bind_data.flight_client->ListFlights(call_options, {bind_data.criteria}));
    }

    std::unique_ptr<flight::FlightInfo> flight_info;
    AIRPORT_ARROW_ASSIGN_OR_RAISE(flight_info, global_state.listing->Next());

    if(flight_info == nullptr) {
        // There are no more flights to return.
        output.SetCardinality(0);
        return;
    }

    const auto max_rows = STANDARD_VECTOR_SIZE;


    auto &descriptor_entries = StructVector::GetEntries(output.data[0]);
	auto descriptor_type_tag_data = FlatVector::GetData<uint8_t>(*descriptor_entries[0]);
	auto descriptor_cmd_data = FlatVector::GetData<string_t>(*descriptor_entries[1]);

    // Flat vector of list entries.
    auto descriptor_path_data = ListVector::GetData(*descriptor_entries[2]);

    auto endpoint_data = ListVector::GetData(output.data[1]);

    int output_row_index = 0;
    while (flight_info != nullptr && output_row_index < max_rows) {
        auto descriptor = flight_info->descriptor();

        switch(descriptor.type) {
        case flight::FlightDescriptor::CMD: {
            descriptor_type_tag_data[output_row_index] = 0;
            descriptor_cmd_data[output_row_index] = StringVector::AddStringOrBlob(*descriptor_entries[1], descriptor.cmd);
            FlatVector::Validity(*descriptor_entries[2]).SetInvalid(output_row_index);
            descriptor_path_data[output_row_index].length = 0;
            descriptor_path_data[output_row_index].offset = 0;
        };
        break;
        case flight::FlightDescriptor::PATH: {
            descriptor_type_tag_data[output_row_index] = 1;
            FlatVector::Validity(*descriptor_entries[1]).SetInvalid(output_row_index);

            auto current_size = ListVector::GetListSize(*descriptor_entries[2]);
            auto new_size = current_size + descriptor.path.size();

            if (ListVector::GetListCapacity(*descriptor_entries[2]) < new_size)
            {
                ListVector::Reserve(*descriptor_entries[2], new_size);
            }

            auto path_values = ListVector::GetEntry(*descriptor_entries[2]);
            auto path_parts = FlatVector::GetData<string_t>(path_values);

            for(size_t i = 0; i < descriptor.path.size(); i++) {
                path_parts[current_size + i] = StringVector::AddString(ListVector::GetEntry(*descriptor_entries[2]), descriptor.path[i]);
            }

            descriptor_path_data[output_row_index].length = descriptor.path.size();
            descriptor_path_data[output_row_index].offset = current_size;

            ListVector::SetListSize(*descriptor_entries[2], new_size);
        }
        break;
        default:
            throw InvalidInputException("Unknown Arrow Flight descriptor type encountered.");
        }

        // Now lets make a fake endpoint struct.
        auto endpoint_list_current_size = ListVector::GetListSize(output.data[1]);
        auto endpoint_list_new_size = endpoint_list_current_size + flight_info->endpoints().size();

        if (ListVector::GetListCapacity(output.data[1]) < endpoint_list_new_size)
        {
            ListVector::Reserve(output.data[1], endpoint_list_new_size);
        }

        auto &endpoint_entries = StructVector::GetEntries(ListVector::GetEntry(output.data[1]));

        auto endpoint_ticket_data = FlatVector::GetData<string_t>(*endpoint_entries[0]);
        auto endpoint_location_data = ListVector::GetData(*endpoint_entries[1]);
        auto endpoint_expiration_data = FlatVector::GetData<int64_t>(*endpoint_entries[2]);
        auto endpoint_metadata_data = FlatVector::GetData<string_t>(*endpoint_entries[3]);

        // Lets deal with the endpoints.
        for (size_t endpoint_index = 0; endpoint_index < flight_info->endpoints().size(); endpoint_index++) {
            auto endpoint = flight_info->endpoints()[endpoint_index];
            endpoint_ticket_data[endpoint_list_current_size + endpoint_index] = StringVector::AddStringOrBlob(*endpoint_entries[0], endpoint.ticket.ticket);
            if(endpoint.expiration_time.has_value()) {
                endpoint_expiration_data[endpoint_list_current_size + endpoint_index] = endpoint.expiration_time.value().time_since_epoch().count();
            }
            else
            {
                // No expiration is set.
                FlatVector::Validity(*endpoint_entries[2]).SetInvalid(endpoint_list_current_size + endpoint_index);
            }
            endpoint_metadata_data[endpoint_list_current_size + endpoint_index] = StringVector::AddStringOrBlob(*endpoint_entries[3], endpoint.app_metadata);

            // Now deal with the locations of this endpoint.
            auto endpoint_location_current_size = ListVector::GetListSize(*endpoint_entries[1]);
            auto endpoint_location_new_size = endpoint_location_current_size + endpoint.locations.size();
            if (ListVector::GetListCapacity(*endpoint_entries[1]) < endpoint_location_new_size)
            {
                ListVector::Reserve(*endpoint_entries[1], endpoint_location_new_size);
            }

            auto endpoint_location_parts = FlatVector::GetData<string_t>(ListVector::GetEntry(*endpoint_entries[1]));

            for(size_t location_index = 0; location_index < endpoint.locations.size(); location_index++) {
                endpoint_location_parts[endpoint_location_current_size + location_index] = StringVector::AddStringOrBlob(ListVector::GetEntry(*endpoint_entries[1]), endpoint.locations[location_index].ToString());
            }

            endpoint_location_data[endpoint_list_current_size].length = endpoint.locations.size();
            endpoint_location_data[endpoint_list_current_size].offset = endpoint_location_current_size;

            ListVector::SetListSize(*endpoint_entries[1], endpoint_location_new_size);
        }


        endpoint_data[output_row_index].length = flight_info->endpoints().size();
        endpoint_data[output_row_index].offset = endpoint_list_current_size;

        ListVector::SetListSize(output.data[1], endpoint_list_new_size);

        FlatVector::GetData<bool>(output.data[2])[output_row_index] = flight_info->ordered();
        FlatVector::GetData<uint64_t>(output.data[3])[output_row_index] = flight_info->total_records();
        FlatVector::GetData<uint64_t>(output.data[4])[output_row_index] = flight_info->total_bytes();
        FlatVector::GetData<string_t>(output.data[5])[output_row_index] = StringVector::AddStringOrBlob(output.data[5], flight_info->app_metadata());

        std::shared_ptr<arrow::Schema> info_schema;
        arrow::ipc::DictionaryMemo dictionary_memo;
        AIRPORT_ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
        FlatVector::GetData<string_t>(output.data[6])[output_row_index] = StringVector::AddStringOrBlob(output.data[6], info_schema->ToString());

        AIRPORT_ARROW_ASSIGN_OR_RAISE(flight_info, global_state.listing->Next());
        output_row_index++;
    }

    output.SetCardinality(output_row_index);
}

unique_ptr<NodeStatistics> take_flight_cardinality(ClientContext &context, const FunctionData *data) {
    // We can ask look at the flight info's number of estimaE
    auto d = reinterpret_cast<const AirportTakeFlightScanFunctionData *>(data);

    auto flight_estimated_records = d->flight_data.get()->flight_info_->total_records();

    if(flight_estimated_records != -1) {
        return make_uniq<NodeStatistics>(flight_estimated_records);
    }
    return make_uniq<NodeStatistics>();
}


void list_flights_complex_filter_pushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                          vector<unique_ptr<Expression>> &filters) {
    //	auto &data = bind_data_p->Cast<JSONScanData>();

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

    if (data == nullptr) {
        throw SerializationException(
            "Failed to serialize json, perhaps the query contains invalid utf8 characters?");
    }

    auto json_result = string(data, (size_t)len);

    auto &bind_data = bind_data_p->Cast<ListFlightsBindData>();

    bind_data.json_filters = json_result;

    // Now how do I pass that as a header on the flight request to the flight server?
}


void take_flight_complex_filter_pushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                        vector<unique_ptr<Expression>> &filters) {
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

    if (data == nullptr) {
        throw SerializationException(
            "Failed to serialize json, perhaps the query contains invalid utf8 characters?");
    }

    auto json_result = string(data, (size_t)len);

    auto &bind_data = bind_data_p->Cast<AirportTakeFlightScanFunctionData>();

    bind_data.json_filters = json_result;
}



unique_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(const ArrowScanFunctionData &function,
                                                     const vector<column_t> &column_ids, TableFilterSet *filters) {
	//! Generate Projection Pushdown Vector
	ArrowStreamParameters parameters;
	D_ASSERT(!column_ids.empty());
	for (idx_t idx = 0; idx < column_ids.size(); idx++) {
		auto col_idx = column_ids[idx];
		if (col_idx != COLUMN_IDENTIFIER_ROW_ID) {
			auto &schema = *function.schema_root.arrow_schema.children[col_idx];
			parameters.projected_columns.projection_map[idx] = schema.name;
			parameters.projected_columns.columns.emplace_back(schema.name);
			parameters.projected_columns.filter_to_col[idx] = col_idx;
		}
	}
	parameters.filters = filters;
	return function.scanner_producer(function.stream_factory_ptr, parameters);
}


unique_ptr<GlobalTableFunctionState> AirportArrowScanInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ArrowScanFunctionData>();
    auto result = make_uniq<ArrowScanGlobalState>();
    result->stream = AirportProduceArrowScan(bind_data, input.column_ids, input.filters.get());

    // Since we're single threaded, we can only really use a single thread at a time.
	result->max_threads = 1;
	if (input.CanRemoveFilterColumns()) {
		result->projection_ids = input.projection_ids;
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				result->scanned_types.push_back(bind_data.all_types[col_idx]);
			}
		}
	}
	return std::move(result);
}


static void LoadInternal(DatabaseInstance &instance) {

    // We could add some parameter here for authentication
    // and extra headers.
    auto list_flights_function = TableFunction(
        "airport_list_flights",
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        list_flights,
        list_flights_bind,
        ListFlightsGlobalState::Init);

    list_flights_function.pushdown_complex_filter = list_flights_complex_filter_pushdown;
    list_flights_function.filter_pushdown = false;

    // Need a named parameter for the criteria.

    ExtensionUtil::RegisterFunction(instance, list_flights_function);

    auto take_flight_function = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::ANY},
        take_flight,
        take_flight_bind,
        AirportArrowScanInitGlobal,
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    take_flight_function.cardinality = take_flight_cardinality;
    take_flight_function.get_batch_index = nullptr;
    take_flight_function.projection_pushdown = true;
    take_flight_function.filter_pushdown = false;

    ExtensionUtil::RegisterFunction(instance, take_flight_function);
}

void AirportExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string AirportExtension::Name() {
	return "airport";
}

std::string AirportExtension::Version() const {
#ifdef EXT_VERSION_airport
	return EXT_VERSION_airport;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void airport_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::AirportExtension>();
}

DUCKDB_EXTENSION_API const char *airport_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
