#define _POSIX_C_SOURCE 200809L

#include <unistd.h>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <thread>

#include "database.h"
#include "server.h"
#include "connection.h"

// TODO
const char* SPDK = "";
const char* SPDK_BDEV = "";
const uint64_t SPDK_CACHE_SIZE = 0;

rocksdb::Env* getSpdkEnv(std::string dbPath) {
  auto env = rocksdb::NewSpdkEnv(rocksdb::Env::Default(), dbPath, SPDK,
                                 SPDK_BDEV, SPDK_CACHE_SIZE);

  if (env == NULL) {
    fprintf(stderr,
            "Could not load SPDK blobfs - check that SPDK mkfs was run "
            "against block device %s.\n",
            SPDK_BDEV);
    exit(1);
  }

  return env;
}

rocksdb::Options getOptions(const char* dbPath, bool spdk) {
  rocksdb::Options options;
  auto numThreads = std::thread::hardware_concurrency();
  options.IncreaseParallelism(numThreads);
  options.max_background_compactions = numThreads;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.info_log_level = rocksdb::InfoLogLevel::INFO_LEVEL;

  if (spdk) {
    options.env = getSpdkEnv(dbPath);
  } else {
    options.env = rocksdb::Env::Default();
  }

  return std::move(options);
}

int main(int argc, char** argv) {
  int opt;
  bool spdk = false;
  while ((opt = getopt(argc, argv, "s")) != -1) {
    switch (opt) {
      case 's':
        spdk = true;
        break;
      default: /* '?' */
        fprintf(stderr, "Usage: %s [-s] database\n", argv[0]);
        exit(EXIT_FAILURE);
    }
  }

  if (optind >= argc) {
    fprintf(stderr, "Expected argument after options\n");
    exit(EXIT_FAILURE);
  }

  auto dbPath = argv[optind];
  auto options = getOptions(dbPath, spdk);

  std::cout << "<OK>" << std::endl;

  std::cerr << "open db" << dbPath << std::endl;
  Database db(dbPath, options);

  Connection conn;

  serveRequests(db, conn);
}
