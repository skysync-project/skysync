#ifndef SMATCHER_H
#define SMATCHER_H

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <libgen.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <atomic>
#include <thread>
#include <mimalloc-2.1/mimalloc.h>

#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/task_arena.h>
#include <oneapi/tbb/concurrent_queue.h>
#include <oneapi/tbb/concurrent_unordered_map.h>
#include <oneapi/tbb/flow_graph.h>

#include "skysync_f.h"
#include "skysync_c.h"
#include "parasync_common.h"
#include "dsync_worker.h"
#include "wmatcher.h"

typedef struct smatched_item {
    uint32_t weak_hash;
    ol old_ol;
    ol new_ol;
    uint8_t hash[BLAKE3_OUT_LEN];
} smatched_item;

struct smatcher_task {
    int fd;
    crc_to_chunks_map *chash_table;
    uint32_t *matched_weak_hash;
    size_t matched_size;
    strong_hash_to_chunk_map *strong_hash_table;
    DataQueue<matched_item_rpc> *strong_matched_chunks_queue;
};

class Smatcher
{
public:
    explicit Smatcher() {}

    virtual ~Smatcher() {}
};

class PDSyncSmatcher : public Smatcher
{
private:
    thread_local static uint8_t* tls_buffer;
    thread_local static size_t tls_buffer_size;

public:
    using Smatcher::Smatcher;

    void compare_strong_hash(char *map, std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *matched_weak_hash, size_t matched_size, std::unique_ptr<tbb_blake_to_chunk_map> &strong_hash_table, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);
};

class ParaSyncSmatcher : public Smatcher
{
private:
    thread_local static uint8_t* tls_buffer;
    thread_local static size_t tls_buffer_size;
    
public:
    using Smatcher::Smatcher;

    void compare_strong_hash(char* map, std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *matched_weak_hash, size_t matched_size, std::unique_ptr<tbb_blake_to_chunk_map> &strong_hash_table, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    void calculate(char* map, ol *new_ols, size_t size, std::shared_ptr<tbb_blake_to_chunk_map> strong_hash_table, oneapi::tbb::concurrent_vector<matched_item_rpc> results);
};

#endif // SMATCHER_H