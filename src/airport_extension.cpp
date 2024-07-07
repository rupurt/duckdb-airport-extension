#define DUCKDB_EXTENSION_MAIN

#include "airport_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// Arrow includes.
#include <arrow/flight/client.h>

namespace flight = arrow::flight;



#define ARROW_RETURN_IF_(condition, status, _) \
  do {                                         \
    if (ARROW_PREDICT_FALSE(condition)) {      \
        throw InvalidInputException("Runway Arrow Flight Exception: " + status.message()); \
    }                                          \
  } while (0)


#define ARROW_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)                              \
  auto&& result_name = (rexpr);                                                          \
  ARROW_RETURN_IF_(!(result_name).ok(), (result_name).status(), ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();



namespace duckdb {

inline void airportScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "airport "+name.GetString()+" 🐥");;
        });
}


struct ListFlightsBindData : public TableFunctionData {
    unique_ptr<string_t> connection_details;

    std::unique_ptr<flight::FlightListing> flight_listing;
};

struct ListFlightsGlobalState : public GlobalTableFunctionState {
public:
    ListFlightsGlobalState() {

    };

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
        printf("Init\n");
        return make_uniq<ListFlightsGlobalState>();
    }
};

static unique_ptr<FunctionData> list_flights_bind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    printf("Calling bind\n");
	auto ret = make_uniq<ListFlightsBindData>();

    // The data is going to be returned in the format of

    ARROW_ASSIGN_OR_RAISE(auto location,
                        flight::Location::ForGrpcTcp("127.0.0.1", 8815));

    std::unique_ptr<flight::FlightClient> flight_client;
    ARROW_ASSIGN_OR_RAISE(flight_client, flight::FlightClient::Connect(location));

    // Now send a list flights request.
    printf("Connected to %s\n", location.ToString().c_str());

    ARROW_ASSIGN_OR_RAISE(ret->flight_listing, flight_client->ListFlights());

    // std::unique_ptr<flight::FlightInfo> flight_info;
    // while(true) {
    //     ARROW_ASSIGN_OR_RAISE(flight_info, listing->Next());

    //     if(flight_info == nullptr) {
    //         break;
    //     }
    //     printf("Got flight info\n");
    //     printf("Flight info: %s\n", flight_info->ToString().c_str());
    // }


    //ARROW_ASSIGN_OR_RAISE(auto location,
    //                    flight::Location::ForGrpcTcp("127.0.0.1", 8815));


    // path

    // ordered - boolean
    // total_records - BIGINT
    // total_bytes - BIGINT
    // metadata - bytes


    child_list_t<LogicalType> flight_descriptor_members = {{"cmd", LogicalType::BLOB}, {"path", LogicalType::LIST(LogicalType::VARCHAR)}};

    auto endpoint_type = LogicalType::STRUCT({{"ticket", LogicalType::BLOB},
                                               {"location", LogicalType::LIST(LogicalType::VARCHAR)},
                                               {"expiration_time", LogicalType::TIMESTAMP},
                                               {"app_metadata", LogicalType::BLOB}});


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

    auto table_names = {"flight_descriptor", "endpoint", "ordered", "total_records", "total_bytes", "app_metadata", "schema"};
    names.insert(names.end(), table_names.begin(), table_names.end());

	return std::move(ret);
}


static void list_flights(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<ListFlightsBindData>();
	auto &global_state = data.global_state->Cast<ListFlightsGlobalState>();

    std::unique_ptr<flight::FlightInfo> flight_info;

    ARROW_ASSIGN_OR_RAISE(flight_info, bind_data.flight_listing->Next());

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
        ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
        FlatVector::GetData<string_t>(output.data[6])[output_row_index] = StringVector::AddStringOrBlob(output.data[6], info_schema->ToString());

        ARROW_ASSIGN_OR_RAISE(flight_info, bind_data.flight_listing->Next());
        output_row_index++;
    }

    output.SetCardinality(output_row_index);
}

static void LoadInternal(DatabaseInstance &instance) {
    auto list_flights_function = TableFunction("airport_list_flights", {LogicalType::VARCHAR}, list_flights, list_flights_bind, ListFlightsGlobalState::Init);
    ExtensionUtil::RegisterFunction(instance, list_flights_function);

//    auto get_flight_function = TableFunction("airport_take_flight", {LogicalType::VARCHAR}, take_flights, take_flight-bind, TakeFlightGlobalState::Init);
//    ExtensionUtil::RegisterFunction(instance, take_flight_function);

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
