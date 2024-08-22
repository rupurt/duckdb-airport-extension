#define DUCKDB_EXTENSION_MAIN

#include "airport_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb
{

    static unique_ptr<BaseSecret> CreateAirportSecretFunction(ClientContext &, CreateSecretInput &input)
    {
        // apply any overridden settings
        vector<string> prefix_paths;

        auto scope = input.scope;
        if (scope.empty())
        {
            throw InternalException("No scope set Airport create secret (should start with grpc://): '%s'", input.type);
        }

        auto result = make_uniq<KeyValueSecret>(scope, "airport", "config", input.name);
        for (const auto &named_param : input.options)
        {
            auto lower_name = StringUtil::Lower(named_param.first);

            if (lower_name == "token")
            {
                result->secret_map["token"] = named_param.second.ToString();
            }
            else
            {
                throw InternalException("Unknown named parameter passed to CreateAirportSecretFunction: " + lower_name);
            }
        }

        //! Set redact keys
        result->redact_keys = {"token"};

        return std::move(result);
    }

    static void SetAirportSecretParameters(CreateSecretFunction &function)
    {
        function.named_parameters["token"] = LogicalType::VARCHAR;
    }

    static void LoadInternal(DatabaseInstance &instance)
    {

        // We could add some parameter here for authentication
        // and extra headers.
        AddListFlightsFunction(instance);

        AddTakeFlightFunction(instance);

        SecretType secret_type;
        secret_type.name = "airport";
        secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
        secret_type.default_provider = "config";

        ExtensionUtil::RegisterSecretType(instance, secret_type);

        CreateSecretFunction airport_secret_function = {"airport", "config", CreateAirportSecretFunction};
        SetAirportSecretParameters(airport_secret_function);
        ExtensionUtil::RegisterFunction(instance, airport_secret_function);
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
