#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"

namespace duckdb
{

  class AirportTransactionManager : public TransactionManager
  {
  public:
    AirportTransactionManager(AttachedDatabase &db_p, AirportCatalog &airport_catalog);

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;

    void Checkpoint(ClientContext &context, bool force = false) override;

  private:
    AirportCatalog &airport_catalog;
    mutex transaction_lock;
    reference_map_t<Transaction, unique_ptr<AirportTransaction>> transactions;
  };

} // namespace duckdb
