#ifndef WMATCHER_H
#define WMATCHER_H

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
#include "skysync_f.h"
#include "skysync_c.h"
#include "parasync_common.h"
#include "dsync_worker.h"
#include "blake3.h"

#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/task_arena.h>
#include <oneapi/tbb/concurrent_queue.h>
#include <oneapi/tbb/concurrent_unordered_map.h>
#include <oneapi/tbb/concurrent_vector.h>
#include <oneapi/tbb/flow_graph.h>

using tbb_crc_to_chunks_map = oneapi::tbb::concurrent_unordered_map<uint32_t, std::vector<ol>>;

using tbb_blake_to_chunk_map = oneapi::tbb::concurrent_unordered_map<std::string, ol>;

using post_calc_map = std::unordered_map<uint32_t, std::unordered_map<std::string, ol>>;

const size_t BATCH_SIZE = 4096;
const size_t NUM_TOKENS = 4;

typedef struct wmatched_item {
    uint32_t weak_hash;
    ol old_ol;
    uint8_t hash[BLAKE3_OUT_LEN];
} wmatched_item;

struct wmatcher_task {
    int fd;
    crc_to_chunks_map *chash_table;
    uint32_t *weak_hash;
    size_t size;
    DataQueue<matched_item_rpc_1> *matched_chunks_queue;
};

class Wmatcher
{
public:
    explicit Wmatcher() {}

    virtual ~Wmatcher() {}
};

class PDSyncWmatcher : public Wmatcher
{
private:
    thread_local static uint8_t* tls_buffer;
    thread_local static size_t tls_buffer_size;

public:
    using Wmatcher::Wmatcher;

    void compare_weak_hash(char *map, std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *weak_hash, size_t size, DataQueue<matched_item_rpc_1> &matched_chunks_queue);
};

class ParaSyncWmatcher : public Wmatcher
{
private:
    thread_local static uint8_t* tls_buffer;
    thread_local static size_t tls_buffer_size;

public:
    using Wmatcher::Wmatcher;
    
    void compare(std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *weak_hash, size_t size, oneapi::tbb::concurrent_vector<wmatched_item> &matched_chunks);
    void compare(std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *weak_hash, size_t size, std::vector<wmatched_item> &matched_chunks);
    
    void calculate(char* map, wmatched_item *wmatched_chunks, size_t size, DataQueue<wmatched_item> &wmatched_item_queue);

    void post_calculate(DataQueue<wmatched_item> *wmatched_item_queue, int thread_num, DataQueue<matched_item_rpc_1> &matched_chunks_queue);
    void post_calculate(DataQueue<wmatched_item> &wmatched_item_queue, int thread_num, DataQueue<matched_item_rpc_1> &matched_chunks_queue);
};

#endif // WMATCHER_H