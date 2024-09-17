#include "duckdb.hpp"
#include "airport_exception.hpp"

#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include "yyjson.hpp"
#include "airport_secrets.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb
{

  static std::string joinWithSlash(const std::vector<std::string> &vec)
  {
    if (vec.empty())
      return ""; // Handle empty vector case

    std::ostringstream result;
    for (size_t i = 0; i < vec.size(); ++i)
    {
      result << vec[i];
      if (i != vec.size() - 1)
      {
        result << '/';
      }
    }
    return result.str();
  }

  // Function to split string based on a delimiter
  static std::vector<std::string> split(const std::string &input, const std::string &delimiter)
  {
    std::vector<std::string> parts;
    size_t pos = 0;
    size_t start = 0;

    while ((pos = input.find(delimiter, start)) != std::string::npos)
    {
      parts.push_back(input.substr(start, pos - start));
      start = pos + delimiter.length();
    }

    // Add the last part after the final delimiter
    parts.push_back(input.substr(start));

    return parts;
  }

  unordered_map<string, string> AirportFlightException::extract_extra_info(const arrow::Status &status, const unordered_map<string, string> &extra_info)
  {
    auto arrow_status_string = status.message();
    std::shared_ptr<flight::FlightStatusDetail> flight_status = flight::FlightStatusDetail::UnwrapStatus(status);

    // Create an unordered_map to store the key-value pairs
    std::unordered_map<std::string, std::string> result = extra_info;

    if (arrow_status_string.find("gRPC client debug context: ") != std::string::npos)
    {
      // Extract the gRPC context from the error message
      std::vector<std::string> parts = split(arrow_status_string, "gRPC client debug context: ");
      if (parts.size() > 1)
      {
        result["grpc_context"] = parts[1]; // Store the gRPC context in the result map
      }
    }

    // The extra info is just some bytes, but in my servers its going to be a json document
    // that can only have string values
    if (flight_status != nullptr)
    {
      auto extra_info_string = flight_status->extra_info();
      if (!extra_info_string.empty())
      {
        // Parse the JSON string
        yyjson_doc *doc = yyjson_read(extra_info_string.c_str(), extra_info_string.size(), 0);
        if (doc)
        {
          // Get the root of the JSON document
          yyjson_val *root = yyjson_doc_get_root(doc);

          // Ensure the root is an object (dictionary)
          if (root && yyjson_is_obj(root))
          {
            size_t idx, max;
            yyjson_val *key, *value;
            yyjson_obj_foreach(root, idx, max, key, value)
            {
              if (yyjson_is_str(key) && yyjson_is_str(value))
              {
                // Add the key-value pair to the unordered_map
                result[yyjson_get_str(key)] = yyjson_get_str(value);
              }
            }
          }
        }
        // Free the JSON document after parsing
        yyjson_doc_free(doc);
      }
    }
    return result;
  }

  string AirportFlightException::produce_flight_error_message(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg)
  {
    // So its kind of silly to have the grpc context
    auto arrow_status_string = status.message();

    if (arrow_status_string.find("gRPC client debug context:") != std::string::npos)
    {
      // Extract the gRPC context from the error message
      std::vector<std::string> parts = split(arrow_status_string, "gRPC client debug context:");
      if (parts.size() > 1)
      {
        arrow_status_string = parts[0]; // Keep the part before the gRPC context
      }
    }

    string extra_msg = "";
    if (!msg.empty())
    {
      extra_msg = msg + " ";
    }

    string descriptor_string;
    if (descriptor.type == flight::FlightDescriptor::DescriptorType::PATH)
    {
      descriptor_string = joinWithSlash(descriptor.path);
    }
    else if (descriptor.type == flight::FlightDescriptor::DescriptorType::CMD)
    {
      descriptor_string = "'" + descriptor.cmd + "'";
    }
    else
    {
      throw IOException("Unsupported Arrow Flight descriptor type");
    }

    // Handle other descriptor types if needed
    string descriptor_joiner = "/";
    if (location.back() == '/')
    {
      descriptor_joiner = "";
    }

    return "Airport @ " + location + descriptor_joiner + descriptor_string + ": " + extra_msg + arrow_status_string;
  }

  string AirportFlightException::produce_flight_error_message(const string &location, const arrow::Status &status, const string &msg)
  {
    // So its kind of silly to have the grpc context
    auto arrow_status_string = status.message();

    if (arrow_status_string.find("gRPC client debug context:") != std::string::npos)
    {
      // Extract the gRPC context from the error message
      std::vector<std::string> parts = split(arrow_status_string, "gRPC client debug context:");
      if (parts.size() > 1)
      {
        arrow_status_string = parts[0]; // Keep the part before the gRPC context
      }
    }

    string extra_msg = "";
    if (!msg.empty())
    {
      extra_msg = msg + " ";
    }

    return "Airport @ " + location + ": " + extra_msg + arrow_status_string;
  }

  AirportFlightException::AirportFlightException(const string &location, const arrow::Status &status, const string &msg) : Exception(ExceptionType::IO, produce_flight_error_message(location, status, msg), extract_extra_info(status, {}))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const arrow::Status &status, const string &msg, const unordered_map<string, string> &extra_info)
      : Exception(ExceptionType::IO, produce_flight_error_message(location, status, msg), extract_extra_info(status, extra_info))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg) : Exception(ExceptionType::IO, produce_flight_error_message(location, descriptor, status, msg), extract_extra_info(status, {}))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg, const unordered_map<string, string> &extra_info)
      : Exception(ExceptionType::IO, produce_flight_error_message(location, descriptor, status, msg), extract_extra_info(status, extra_info))
  {
  }
}