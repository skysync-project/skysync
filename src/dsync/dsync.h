#ifndef DSYNC_H
#define DSYNC_H

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

#include "uthash.h"
#include "fastcdc.h"
#include "skysync_c.h"
#include "sync_common.h"
#include "linked_hash.h"
#include "blake3.h"
#include "crc32/crc32.h"
#include "crc32/crc32c.h"

// #define SIZE_TEST

enum class StrongHashingAlgorithm {
    SHA1,
    SHA256,
    BLAKE3
};

#define SHA1_OUT_LEN 20
#define SHA256_OUT_LEN 32
#define BLAKE3_OUT_LEN 32

typedef void (*shash_func_ptr)(uint8_t *hash, uint8_t *buf, uint32_t len);

void calc_sha1(uint8_t *sha1_hash, uint8_t *buf, uint32_t len);
void cal_sha256(uint8_t *sha256_hash, uint8_t *buf, uint32_t len);
void calc_blake3(uint8_t *b3_hash, uint8_t *buf, uint32_t len);

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

const uint64_t MinChunkSize = 4096; // 4 KB
const uint64_t AvgChunkSize = 8192; // 8 KB
const uint64_t MaxChunkSize = 12288; // 12 KB

const int mask_bits = int(round(log2(AvgChunkSize)));
const int small_mask = (1 << mask_bits) - 1;
const int large_mask = (1 << mask_bits) - 1;

uint64_t find_cutpoint(const char* data, uint64_t len);

uint64_t find_cutpoint_2(const char* data, uint64_t len);

typedef std::function<void(int fd, DataQueue<one_cdc> &csums_queue)> whash_func_ptr;

class SyncWorker {
public:
    crc_to_chunks_map *chash_table;
    LinkedHashTable *lhash_table;

    shash_func_ptr calc_hash_func;
    uint32_t shash_length;

    whash_func_ptr serial_cdc;

    SyncWorker();
    ~SyncWorker();

    void serial_cdc_sw(int fd, DataQueue<one_cdc> &csums_queue);
    void serial_cdc_hw(int fd, DataQueue<one_cdc> &csums_queue);
    void serial_cdc_isal(int fd, DataQueue<one_cdc> &csums_queue);
    void init_shash_functions(StrongHashingAlgorithm algorithm);
};

class ClientSyncWorker : public SyncWorker {
public:
    DataQueue<one_cdc> new_csums_queue;
    DataQueue<uint32_t> new_crc32_queue;
    
    DataQueue<matched_item_rpc> weak_matched_chunks_queue;
    DataQueue<matched_item_rpc> strong_matched_chunks_queue;
    DataQueue<data_cmd> data_cmd_queue;

    DataQueue<matched_item_rpc_1> weak_matched_chunks_queue_1;
    
    ClientSyncWorker(StrongHashingAlgorithm shashing = StrongHashingAlgorithm::SHA1, uint8_t whashing = 0);
    ~ClientSyncWorker();

    using SyncWorker::serial_cdc;
    void chash_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue);

    void compare_sha1_chash(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    void lhash_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue, uint64_t chunk_nums);
    
    void compare_sha1_lhash(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

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

    ServerSyncWorker(StrongHashingAlgorithm shashing = StrongHashingAlgorithm::SHA1, uint8_t whashing = 0);
    ~ServerSyncWorker();

    using SyncWorker::serial_cdc;

    // build a uthash table for the checksums of the old file
    void uthash_builder(DataQueue<one_cdc> &old_csums_queue);

    void chash_builder(DataQueue<one_cdc> &csums_queue);

    void lhash_builder(DataQueue<one_cdc> &csums_queue, uint64_t chunk_nums);

    void compare_weak_uthash(int fd, DataQueue<one_cdc> &new_csums_queue, DataQueue<matched_item_rpc> &matched_chunks_queue);

    void compare_weak_chash(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue);

    void compare_weak_lhash(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue);

    // void compare_weak_chash(int fd, DataQueue<one_cdc> &new_csums_queue, DataQueue<matched_item_rpc> &matched_chunks_queue);

    void patch_delta(int old_fd, int out_fd, DataQueue<data_cmd> &data_cmd_queue);
};

#endif // DSYNC_H