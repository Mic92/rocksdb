#pragma once

#include <errno.h>
#include <string.h>
#include <iostream>

struct die {
  bool valid{true};
  int err{errno};

  die(){};

  die(die const &) = delete;

  die(die &&other) : valid(other.valid), err(other.err) { other.valid = false; }

  template <typename T>
  die &operator<<(T const &val) {
    std::cerr << val;
    return *this;
  }

  ~die() {
    if (valid) {
      std::cerr << ": " << strerror(err) << std::endl;
      exit(1);
    }
  }
};
