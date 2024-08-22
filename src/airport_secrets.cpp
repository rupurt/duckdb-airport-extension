#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/main/secret/secret_manager.hpp"
#include "airport_secrets.hpp"

namespace duckdb
{

  unique_ptr<SecretEntry> AirportGetSecretByName(ClientContext &context, const string &secret_name)
  {
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    // FIXME: this should be adjusted once the `GetSecretByName` API supports this
    // use case
    auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
    if (secret_entry)
    {
      return secret_entry;
    }
    secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
    if (secret_entry)
    {
      return secret_entry;
    }
    return nullptr;
  }

  SecretMatch AirportGetSecretByPath(ClientContext &context, const string &path)
  {
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    return secret_manager.LookupSecret(transaction, path, "airport");
  }

}