#pragma once

#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/function/table/arrow.hpp"

namespace duckdb
{
  unique_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(const ArrowScanFunctionData &function,
                                                              const vector<column_t> &column_ids,
                                                              TableFilterSet *filters);
}
