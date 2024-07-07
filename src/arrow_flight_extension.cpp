#define DUCKDB_EXTENSION_MAIN

#include "arrow_flight_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void arrow_flightScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "arrow_flight "+name.GetString()+" 🐥");;
        });
}


struct ListFlightsBindData : public TableFunctionData {
    unique_ptr<string_t> connection_details;
};

struct ListFlightsGlobalState : public GlobalTableFunctionState {
public:
    ListFlightsGlobalState() {

    };

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
        printf("Init\n");
        return make_uniq<ListFlightsGlobalState>();
    }
    idx_t call_count = 0;
};

static unique_ptr<FunctionData> list_flights_bind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    printf("Calling bind\n");
	auto ret = make_uniq<ListFlightsBindData>();

    // The data is going to be returned in the format of

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
        LogicalType::BLOB
    };
    return_types.insert(return_types.end(), table_types.begin(), table_types.end());

    auto table_names = {"flight_descriptor", "endpoint", "ordered", "total_records", "total_bytes", "app_metadata"};
    names.insert(names.end(), table_names.begin(), table_names.end());

	return std::move(ret);
}

static void list_flights(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    printf("Calling list_flights\n");
	auto &bind_data = data.bind_data->Cast<ListFlightsBindData>();
	auto &global_state = data.global_state->Cast<ListFlightsGlobalState>();

    if(global_state.call_count > 0) {
        output.SetCardinality(0);
        return;
    }


    auto &descriptor_entries = StructVector::GetEntries(output.data[0]);
	auto descriptor_tag_data = FlatVector::GetData<uint8_t>(*descriptor_entries[0]);
	auto descriptor_cmd_data = FlatVector::GetData<string_t>(*descriptor_entries[1]);

    // Flat vector of list entries.
    auto descriptor_path_data = ListVector::GetData(*descriptor_entries[2]);

    printf("Getting list size\n");
    printf("List current size: %lld", ListVector::GetListSize(*descriptor_entries[2]));

    auto endpoint_data = ListVector::GetData(output.data[1]);


    idx_t out = 0;
    for (int i = 0; i < 10; i++) {

        // The first element is a union of the command and the path
        // first lets just return cmd.
        if(i % 2 == 1) {
            descriptor_tag_data[i] = 0;
            descriptor_cmd_data[i] = "example command";
            FlatVector::Validity(*descriptor_entries[2]).SetInvalid(i);
            descriptor_path_data[i].length = 0;
            descriptor_path_data[i].offset = 0;
        } else {
            descriptor_tag_data[i] = 1;
            FlatVector::Validity(*descriptor_entries[1]).SetInvalid(i);


            auto current_size = ListVector::GetListSize(*descriptor_entries[2]);
            auto new_size = current_size + 2;
            printf("Current list value size=%d new size=%d\n", current_size, new_size);

            if(ListVector::GetListCapacity(*descriptor_entries[2]) < new_size) {
                ListVector::Reserve(*descriptor_entries[2], new_size);
            }

            auto path_parts = FlatVector::GetData<string_t>(ListVector::GetEntry(*descriptor_entries[2]));

            path_parts[current_size] = "example_part_1";
            path_parts[current_size + 1] = "example_part_2";

            descriptor_path_data[i].length = 2;
            descriptor_path_data[i].offset = current_size;

            ListVector::SetListSize(*descriptor_entries[2], new_size);

            printf("Finished %d\n", i);
        }


        // Now lets make a fake endpoint struct.
        auto endpoint_list_current_size = ListVector::GetListSize(output.data[1]);
        auto endpoint_list_new_size = endpoint_list_current_size + 1;

        if(ListVector::GetListCapacity(output.data[1]) < endpoint_list_new_size) {
            ListVector::Reserve(output.data[1], endpoint_list_new_size);
        }

        auto &endpoint_entries = StructVector::GetEntries(ListVector::GetEntry(output.data[1]));

        auto endpoint_ticket_data = FlatVector::GetData<string_t>(*endpoint_entries[0]);
        auto endpoint_location_data = ListVector::GetData(*endpoint_entries[1]);
        auto endpoint_expiration_data = FlatVector::GetData<int64_t>(*endpoint_entries[2]);
        auto endpoint_metadata_data = FlatVector::GetData<string_t>(*endpoint_entries[3]);

        endpoint_ticket_data[endpoint_list_current_size] = "example ticket";
        endpoint_expiration_data[endpoint_list_current_size] = 1234567890;
        endpoint_metadata_data[endpoint_list_current_size] = "example metadata";

        auto endpoint_location_current_size = ListVector::GetListSize(*endpoint_entries[1]);
        auto endpoint_location_new_size = endpoint_location_current_size + 1;
        if (ListVector::GetListCapacity(*endpoint_entries[1]) < endpoint_location_new_size) {
            ListVector::Reserve(*endpoint_entries[1], endpoint_location_new_size);
        }

        auto endpoint_location_parts = FlatVector::GetData<string_t>(ListVector::GetEntry(*endpoint_entries[1]));
        endpoint_location_parts[endpoint_location_current_size] = "example_location_1";

        endpoint_location_data[endpoint_list_current_size].length = 1;
        endpoint_location_data[endpoint_list_current_size].offset = endpoint_location_current_size;

        ListVector::SetListSize(*endpoint_entries[1], endpoint_location_new_size);

        endpoint_data[out].length = 1;
        endpoint_data[out].offset = out;

        ListVector::SetListSize(output.data[1], endpoint_list_new_size);

        FlatVector::GetData<bool>(output.data[2])[out] = false;

        FlatVector::GetData<uint64_t>(output.data[3])[out] = -1;
        FlatVector::GetData<uint64_t>(output.data[4])[out] = 1000;

        FlatVector::GetData<string_t>(output.data[5])[out] =
            StringVector::AddStringOrBlob(output.data[5], "example metadata");

        out++;
    }
    global_state.call_count = global_state.call_count + 1;
	output.SetCardinality(out);
}


static void LoadInternal(DatabaseInstance &instance) {

    // Register a table function that enumerates the flights available on a server.
    auto list_flights_function = TableFunction("arrow_flight_list_flights", {LogicalType::VARCHAR}, list_flights, list_flights_bind, ListFlightsGlobalState::Init);
    ExtensionUtil::RegisterFunction(instance, list_flights_function);

}

void ArrowFlightExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ArrowFlightExtension::Name() {
	return "arrow_flight";
}

std::string ArrowFlightExtension::Version() const {
#ifdef EXT_VERSION_arrow_flight
	return EXT_VERSION_arrow_flight;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void arrow_flight_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ArrowFlightExtension>();
}

DUCKDB_EXTENSION_API const char *arrow_flight_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
