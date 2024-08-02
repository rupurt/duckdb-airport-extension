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

namespace duckdb
{

    static void LoadInternal(DatabaseInstance &instance)
    {

        // We could add some parameter here for authentication
        // and extra headers.
        AddListFlightsFunction(instance);

        AddTakeFlightFunction(instance);
    }

    void AirportExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string AirportExtension::Name()
    {
        return "airport";
    }

    std::string AirportExtension::Version() const
    {
#ifdef EXT_VERSION_airport
        return EXT_VERSION_airport;
#else
        return "";
#endif
    }

} // namespace duckdb

extern "C"
{
    DUCKDB_EXTENSION_API void airport_init(duckdb::DatabaseInstance &db)
    {
        duckdb::DuckDB db_wrapper(db);
        db_wrapper.LoadExtension<duckdb::AirportExtension>();
    }

    DUCKDB_EXTENSION_API const char *airport_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
