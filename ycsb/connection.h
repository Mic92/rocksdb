#pragma once

#include "queue.h"
#include <string_view>

class Request {
 public:
  enum Type { READ = 0, SCAN = 1, UPDATE = 2, INSERT = 3, DELETE = 4 } type;

  size_t HEADER_SIZE = sizeof(int32_t) * 2;

  Request(std::vector<char> &data, size_t keyLength, size_t valueLength)
      : data(std::move(data)), keyLength_(keyLength), valueLength_(valueLength) {}

  std::string_view getKey() {
    return std::basic_string_view(&data[HEADER_SIZE], keyLength_);
  }

  std::string_view getValue() {
    assert(valueLength_ != 0);
    return std::string_view(&data[HEADER_SIZE + keyLength_], valueLength_);
  }

 private:
  std::vector<char> data;
  size_t keyLength_, valueLength_;
};

Request::Type getRequestType(char typeField) {
  switch (typeField) {
    case 0:
      return Request::Type::READ;
    case 1:
      return Request::Type::SCAN;
    case 2:
      return Request::Type::UPDATE;
    case 3:
      return Request::Type::INSERT;
    case 4:
      return Request::Type::DELETE;
    default: {
      std::cerr << "Invalid request type: " << typeField << std::endl;
      exit(1);
    }
  };
}

class Connection {
 public:
  Connection()
      : requests(QueueType::REQUESTS, 4096),
        responses(QueueType::RESPONSE, 4096) {}

  Request receive() {
    auto req = requests.pop();
    Request::Type type = getRequestType(req.at(0));
    int32_t keyLength, valueLength = 0;

    switch (type) {
      case Request::Type::READ:
      case Request::Type::SCAN:
      case Request::Type::DELETE: {
        keyLength = reinterpret_cast<const int32_t *>(&req.at(0))[0];

        break;
      };
      case Request::Type::UPDATE:
      case Request::Type::INSERT: {
        keyLength = reinterpret_cast<const int32_t *>(&req.at(0))[0];
        valueLength = reinterpret_cast<const int32_t *>(&req.at(0))[1];
        break;
      };
    }

    return Request(req, keyLength, valueLength);
  }

 private:
  Queue requests, responses;
};
