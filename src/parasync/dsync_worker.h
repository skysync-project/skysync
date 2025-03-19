#ifndef DSYNC_WORKER_H
#define DSYNC_WORKER_H

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <zlib.h>
#include <openssl/md5.h>
#include <openssl/evp.h>
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

#include "uthash.h"
#include "fastcdc.h"
#include "skysync_c.h"
#include "parasync_common.h"

// #define SIZE_TEST

#ifdef SIZE_TEST
    extern uint64_t matching_tokens_size;
    extern uint64_t patch_commands_size;  
    extern uint64_t literal_bytes_size;
    extern std::vector<uint64_t> same_crc32c_chunks;
#endif

typedef struct one_cdc one_cdc;
typedef struct cdc_matched_chunks cdc_matched_chunks;
typedef struct real_matched real_matched;

using crc_to_chunks_map = std::unordered_map<uint32_t, std::vector<ol>>;
using old_ol = ol;
using new_ol = ol;
using same_chunks = std::vector<std::pair<old_ol, new_ol>>;

using strong_hash_to_chunk_map = std::unordered_map<std::string, ol>;

typedef struct {
    uint8_t cmd;
    uint64_t offset;
    uint64_t length;
    uint8_t *data;
    bool end_of_stream;
} data_cmd;

struct item {
        uint32_t weak_hash;
        uint64_t offset;
        uint64_t length;
};

class SyncWorker {
public:
    crc_to_chunks_map *chash_table;
    SyncWorker();
    ~SyncWorker();

    void serial_cdc(int fd, DataQueue<one_cdc> &csums_queue);
    void serial_cdc_isal(int fd, DataQueue<one_cdc> &csums_queue);
};

class ClientSyncWorker : public SyncWorker {
public:
    DataQueue<one_cdc> new_csums_queue;
    DataQueue<uint32_t> new_crc32_queue;
    
    DataQueue<matched_item_rpc> weak_matched_chunks_queue;
    DataQueue<matched_item_rpc> strong_matched_chunks_queue;
    DataQueue<data_cmd> data_cmd_queue;

    DataQueue<matched_item_rpc_1> weak_matched_chunks_queue_1;
    
    ClientSyncWorker();
    ~ClientSyncWorker();

    using SyncWorker::serial_cdc;
    void chash_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue);

    void compare_sha1(int fd, DataQueue<matched_item_rpc> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    void compare_sha1_1(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    data_cmd create_data_cmd(int fd, int cmd_flag, uint64_t offset, uint64_t length);

    void sort_matched_chunks(DataQueue<matched_item_rpc> &matched_chunks_queue);

    void generate_delta(int new_fd, DataQueue<matched_item_rpc> &strong_matched_chunks_queue, DataQueue<data_cmd> &data_cmd_queue);
    
};

class ServerSyncWorker : public SyncWorker {
public:
    DataQueue<one_cdc> old_csums_queue;
    DataQueue<one_cdc> new_csums_queue;
    DataQueue<uint32_t> new_crc32_queue;

    DataQueue<matched_item_rpc> weak_matched_chunks_queue;
    DataQueue<matched_item_rpc_1> weak_matched_chunks_queue_1;

    DataQueue<data_cmd> data_cmd_queue;

    struct cdc_uthash *hash_table;

    ServerSyncWorker();
    ~ServerSyncWorker();

    using SyncWorker::serial_cdc;

    // build a uthash table for the checksums of the old file
    void uthash_builder(DataQueue<one_cdc> &old_csums_queue);

    void chash_builder(DataQueue<one_cdc> &csums_queue);

    void compare_weak_uthash(int fd, DataQueue<one_cdc> &new_csums_queue, DataQueue<matched_item_rpc> &matched_chunks_queue);

    void compare_weak_chash(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue);

    // void compare_weak_chash(int fd, DataQueue<one_cdc> &new_csums_queue, DataQueue<matched_item_rpc> &matched_chunks_queue);

    void patch_delta(int old_fd, int out_fd, DataQueue<data_cmd> &data_cmd_queue);
};

#endif // DSYNC_WORKER_H