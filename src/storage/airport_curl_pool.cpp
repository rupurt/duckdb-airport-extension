#include "duckdb.hpp"

#include <curl/curl.h>
#include <vector>
#include <mutex>
#include <condition_variable>
#include "storage/airport_curl_pool.hpp"

namespace duckdb
{

  AirportCurlPool::AirportCurlPool(size_t size)
  {
    for (size_t i = 0; i < size; ++i)
    {
      CURL *handle = curl_easy_init();
      if (handle)
      {
        _pool.push_back(handle);
      }
    }
  }

  AirportCurlPool::~AirportCurlPool()
  {
    for (auto handle : _pool)
    {
      curl_easy_cleanup(handle);
    }
  }

  CURL *AirportCurlPool::acquire()
  {
    std::unique_lock<std::mutex> lock(_mutex);
    _cv.wait(lock, [this]()
             { return !_pool.empty(); });
    CURL *handle = _pool.back();
    _pool.pop_back();
    return handle;
  }

  void AirportCurlPool::release(CURL *handle)
  {
    std::unique_lock<std::mutex> lock(_mutex);
    _pool.push_back(handle);
    _cv.notify_one();
  }

}