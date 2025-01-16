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

  string AirportAuthTokenForLocation(ClientContext &context, const string &server_location, const string &secret_name, const string &auth_token)
  {
    if (!auth_token.empty())
    {
      return auth_token;
    }

    if (!secret_name.empty())
    {
      auto secret_entry = AirportGetSecretByName(context, secret_name);
      if (!secret_entry)
      {
        throw BinderException("Secret with name \"%s\" not found", secret_name);
      }

      const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

      if (auth_token.empty())
      {
        Value input_val = kv_secret.TryGetValue("auth_token");
        if (!input_val.IsNull())
        {
          return input_val.ToString();
        }
        return "";
      }
    }

    auto secret_match = AirportGetSecretByPath(context, server_location);
    if (secret_match.HasMatch())
    {
      const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);

      Value input_val = kv_secret.TryGetValue("auth_token");
      if (!input_val.IsNull())
      {
        return input_val.ToString();
      }
      return "";
    }
    return "";
  }
}