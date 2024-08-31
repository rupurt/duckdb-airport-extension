#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb
{
  class AirportCatalog;
  class AirportSchemaEntry;
  class AirportTableEntry;

  enum class AirportTransactionState
  {
    TRANSACTION_NOT_YET_STARTED,
    TRANSACTION_STARTED,
    TRANSACTION_FINISHED
  };

  class AirportTransaction : public Transaction
  {
  public:
    AirportTransaction(AirportCatalog &airport_catalog, TransactionManager &manager, ClientContext &context);
    ~AirportTransaction() override;

    void Start();
    void Commit();
    void Rollback();

    //	UCConnection &GetConnection();
    //	unique_ptr<UCResult> Query(const string &query);
    static AirportTransaction &Get(ClientContext &context, Catalog &catalog);
    AccessMode GetAccessMode() const
    {
      return access_mode;
    }

  private:
    AirportTransactionState transaction_state;
    AccessMode access_mode;
  };

} // namespace duckdb
