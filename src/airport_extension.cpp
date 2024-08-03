#define DUCKDB_EXTENSION_MAIN

#include "airport_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

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
