#ifndef SKYSYNC_F_H_1
#define SKYSYNC_F_H_1

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <zlib.h>
#include <openssl/md5.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/sha.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <libgen.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <mimalloc-2.1/mimalloc.h>
#include <string>
#include <queue>
#include <atomic>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <cctype>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <limits>
#include <memory>

#include "fastcdc.h"
#include "skysync_c.h"
#include "sync_common.h"
#include "blake3.h"
#include "crc32/crc32.h"
#include "crc32/crc32c.h"
#include "dsync.h"
#include "fsc_hash.h"
#include "hash7.hpp"

struct one_fsc {
    uint64_t offset;
    uint64_t length;
    uint32_t weak_hash;
    std::string strong_hash;
};

struct file_fsc {
    uint64_t chunk_num;
    one_fsc *fsc_array;
};

file_fsc* calc_fsc_hw(int fd);
void write_fsc(const char *fsc_file, file_fsc *fsc);
file_fsc* read_fsc(const char *fsc_file);

// Helper functions for file_fsc management
file_fsc* create_file_fsc(uint64_t chunk_count);
void free_file_fsc(file_fsc* fsc);

const uint64_t DefaultWindowSize = AvgChunkSize;

typedef std::function<void(int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue)> rolling_func_ptr;

class SkySyncFWorker {
public:

    SkySyncFWorker();
    ~SkySyncFWorker();
};

class ClientSkySyncFWorker : public SkySyncFWorker {
public:
    file_fsc *new_csums;
    file_fsc *old_csums;
    DataQueue<data_cmd> data_cmd_queue;

    // Replace std::unordered_map with efficient hash tables
    WeakHashTable *weak_hash_table;
    StrongHashTable *strong_hash_table;
    emhash7::HashMap<uint32_t, bool> *weak_hash7_table;
    emhash7::HashMap<std::string, ol> *strong_hash7_table;

    ClientSkySyncFWorker(uint8_t whashing = 0);
    ~ClientSkySyncFWorker();

    rolling_func_ptr rolling_fsc;

    void rolling_fsc_sw(int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue);
    void rolling_fsc_hw(int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue);

    void chash_builder(file_fsc *old_csums);
    void hash7_builder(file_fsc *old_csums);

    data_cmd create_data_cmd(int fd, int cmd_flag, uint64_t offset, uint64_t length);
};

class ServerSkySyncFWorker : public SkySyncFWorker {
public:
    file_fsc *old_csums;
    DataQueue<data_cmd> data_cmd_queue;

    ServerSkySyncFWorker(uint8_t whashing = 0);
    ~ServerSkySyncFWorker();

    void patch_delta(int old_fd, int out_fd, DataQueue<data_cmd> &data_cmd_queue);
};

#endif // SKYSYNC_F_H