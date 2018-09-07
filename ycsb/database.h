#pragma once

#include <rocksdb/db.h>
#include <iostream>
#include <string>

rocksdb::Status checkOk(rocksdb::Status s) {
  if (!s.ok()) {
    std::cerr << "database error: " << s.getState() << std::endl;
    exit(1);
  }
  return s;
}

class UpdateResult {
 public:
  enum { OK, NOTFOUND } status;
  std::string value;
};

class Database {
 public:
  Database(const char* dbPath, rocksdb::Options options) {
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    checkOk(rocksdb::DB::Open(options, dbPath, &db_));
  }

  std::string read(const std::string key) {
    std::string value;
    checkOk(db_->Get(rocksdb::ReadOptions(), key, &value));
    return value;
  }

  // TODO
  // std::string scan(const std::string key, std::function<void(std::string)>&&
  // callback) {
  //  std::string value;
  //  checkOk(db_->Get(rocksdb::ReadOptions(), key, &value));
  //  return value;
  //}

  UpdateResult update(const std::string key, std::string newValue) {
    std::string oldValue;
    auto s = db_->Get(rocksdb::ReadOptions(), key, &oldValue);
    if (s.IsNotFound()) {
      return UpdateResult{UpdateResult::OK, ""};
    } else {
      checkOk(s);
      checkOk(db_->Put(rocksdb::WriteOptions(), key, newValue));

      return UpdateResult{UpdateResult::NOTFOUND, oldValue};
    }
  }

  void insert(const std::string key, const std::string value) {
    checkOk(db_->Put(rocksdb::WriteOptions(), key, value));
  }

  void deleteKey(const std::string key) {
    checkOk(db_->Delete(rocksdb::WriteOptions(), key));
  }

 private:
  rocksdb::DB* db_ = nullptr;
};
