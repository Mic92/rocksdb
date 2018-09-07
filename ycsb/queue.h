#pragma once

#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "die.h"

enum QueueType { REQUESTS, RESPONSE };

std::string queuePath(QueueType type) {
  auto prefix =
      type == REQUESTS ? "rocksdb-request-queue." : "rocksdb-response-queue.";
  return std::string("/dev/shm/") + prefix + std::to_string(getpid());
}

class Queue {
 public:
  Queue(QueueType type, size_t limit_) : path(queuePath(type)), limit(limit_) {
    fd = open(path.c_str(), O_CLOEXEC | O_CREAT | O_RDWR | O_TRUNC, 0700);

    if (fd < 0) {
      die() << "open queue '" << path << "' failed";
    }

    if (ftruncate(fd, limit) < 0) {
      die() << "ftruncate";
    }

    auto addr = mmap(nullptr, limit + HEADER_SIZE, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
      die() << "mmap";
    }

    head_ = reinterpret_cast<int32_t volatile*>(addr);
    tail_ = &head_[1];

    buffer = reinterpret_cast<char volatile*>(&head_[2]);
  }

  ~Queue() {
    if (fd != -1) {
      if (close(fd) < 0) {
        perror("close");
      }
      if (unlink(path.c_str()) < 0) {
        perror("unlink");
      }
    }
    if (buffer != MAP_FAILED) {
      auto addr =
          reinterpret_cast<char volatile*>(buffer) - HEADER_SIZE;
      if (munmap((void*)(addr), limit + HEADER_SIZE) < 0) {
        perror("munmap");
      }
    }
  }

  void push(std::vector<char> bytes) {
    const size_t msgLen = bytes.size() + INT_SIZE;
    while (size() + msgLen > limit) {
    };

    int head = getHead();
    putSize(head, bytes.size());
    head += INT_SIZE;

    for (size_t i = 0; i < bytes.size(); i++) {
      put(head, bytes[i]);
      head++;
      head %= limit;
    }

    setHead(head);
  }

  std::vector<char> pop() {
    while (size() == 0) {
    };

    size_t tail = getTail();
    const size_t elemLen = getSize(tail);
    tail += INT_SIZE;

    std::vector<char> msg;
    msg.resize(elemLen);

    for (size_t i = 0; i < elemLen; i++) {
      msg[i] = get(i + tail);
      tail++;
      tail %= limit;
    }

    setTail(tail);

    return msg;
  }

 private:
  const size_t INT_SIZE = sizeof(int32_t);

  int32_t volatile* head_ = nullptr;
  int32_t volatile* tail_ = nullptr;

  const size_t HEADER_SIZE = INT_SIZE * 2;

  char volatile* buffer =
      reinterpret_cast<char volatile*>(MAP_FAILED);
  std::string path;
  int fd = -1;
  const size_t limit;

  void put(size_t index, char c) {
    assert(index < limit);
    buffer[index + HEADER_SIZE] = c;
  }

  char get(size_t index) {
    assert(index < limit);
    return buffer[index + HEADER_SIZE];
  }

  void putSize(size_t index, size_t size) {
    assert((index + sizeof(int32_t)) < limit);

    *reinterpret_cast<int32_t volatile*>(&buffer[index]) =
        static_cast<int32_t>(size);
  }

  size_t getSize(int index) {
    assert((index + sizeof(int32_t)) < limit);

    int32_t val = *reinterpret_cast<int32_t* volatile>(buffer[index]);
    assert(val >= 0);

    return val;
  }

  size_t getHead() {
    int32_t v = *head_;
    assert(v >= 0 && static_cast<size_t>(v) < limit);
    return v;
  }

  void setHead(size_t value) {
    assert(value < limit);
    *head_ = value;
  }

  size_t getTail() {
    int32_t v = *head_;
    assert(v >= 0 && static_cast<size_t>(v) < limit);

    return v;
  }

  void setTail(size_t value) {
    assert(value < limit);

    *tail_ = value;
  }

  size_t size() {
    const size_t head = getHead();
    const size_t tail = getTail();
    if (head >= tail) {
      return head - tail;
    } else {
      return (limit - tail) + head;
    }
  }
};
