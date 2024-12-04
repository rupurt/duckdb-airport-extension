#pragma once
#include "duckdb/main/config.hpp"

namespace duckdb
{
  class AirportOptimizer
  {
  public:
    static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
  };

}
