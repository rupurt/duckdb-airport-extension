#include "storage/airport_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb
{

  AirportTransactionManager::AirportTransactionManager(AttachedDatabase &db_p, AirportCatalog &airport_catalog)
      : TransactionManager(db_p), airport_catalog(airport_catalog)
  {
  }

  Transaction &AirportTransactionManager::StartTransaction(ClientContext &context)
  {
    auto transaction = make_uniq<AirportTransaction>(airport_catalog, *this, context);
    transaction->Start();
    auto &result = *transaction;
    lock_guard<mutex> l(transaction_lock);
    transactions[result] = std::move(transaction);
    return result;
  }

  ErrorData AirportTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction)
  {
    auto &airport_transaction = transaction.Cast<AirportTransaction>();
    airport_transaction.Commit();
    lock_guard<mutex> l(transaction_lock);
    transactions.erase(transaction);
    return ErrorData();
  }

  void AirportTransactionManager::RollbackTransaction(Transaction &transaction)
  {
    auto &airport_transaction = transaction.Cast<AirportTransaction>();
    airport_transaction.Rollback();
    lock_guard<mutex> l(transaction_lock);
    transactions.erase(transaction);
  }

  void AirportTransactionManager::Checkpoint(ClientContext &context, bool force)
  {
    // auto &transaction = AirportTransaction::Get(context, db.GetCatalog());
    // auto &db = transaction.GetConnection();
    // db.Execute("CHECKPOINT");
  }

} // namespace duckdb
