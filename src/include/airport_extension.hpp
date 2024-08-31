#pragma once

#include "duckdb.hpp"

#define AIRPORT_USER_AGENT "airport/20240820-01"

namespace duckdb
{

	class AirportExtension : public Extension
	{
	public:
		void Load(DuckDB &db) override;
		std::string Name() override;
		std::string Version() const override;
	};

	void AddListFlightsFunction(DatabaseInstance &instance);

	void AddTakeFlightFunction(DatabaseInstance &instance);

} // namespace duckdb
