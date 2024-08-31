#include "storage/airport_transaction.hpp"
#include "storage/airport_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb
{

  AirportTransaction::AirportTransaction(AirportCatalog &airport_catalog, TransactionManager &manager, ClientContext &context)
      : Transaction(manager, context), access_mode(airport_catalog.access_mode)
  {
  }

  AirportTransaction::~AirportTransaction() = default;

  void AirportTransaction::Start()
  {
    transaction_state = AirportTransactionState::TRANSACTION_NOT_YET_STARTED;
  }
  void AirportTransaction::Commit()
  {
    if (transaction_state == AirportTransactionState::TRANSACTION_STARTED)
    {
      transaction_state = AirportTransactionState::TRANSACTION_FINISHED;
    }
  }
  void AirportTransaction::Rollback()
  {
    if (transaction_state == AirportTransactionState::TRANSACTION_STARTED)
    {
      transaction_state = AirportTransactionState::TRANSACTION_FINISHED;
    }
  }

  AirportTransaction &AirportTransaction::Get(ClientContext &context, Catalog &catalog)
  {
    return Transaction::Get(context, catalog).Cast<AirportTransaction>();
  }

} // namespace duckdb
