
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/storage/table/append_state.hpp"

#include "airport_extension.hpp"
#include "storage/airport_insert.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "storage/airport_table_entry.hpp"

namespace duckdb
{
  void AirportVerifyAppendConstraints(ConstraintState &state, ClientContext &context, DataChunk &chunk,
                                      optional_ptr<ConflictManager> conflict_manager,
                                      vector<string> column_names);
}
