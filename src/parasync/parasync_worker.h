#ifndef PARASYNC_WORKER_H
#define PARASYNC_WORKER_H

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

#include "fastcdc.h"
#include "skysync_c.h"
#include "parasync_common.h"
#include "dsync_worker.h"
#include "chunker.h"
#include "wmatcher.h"
#include "smatcher.h"
#include "pdsync_worker.h"

#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/task_arena.h>
#include <oneapi/tbb/concurrent_queue.h>
#include <oneapi/tbb/concurrent_unordered_map.h>
#include <oneapi/tbb/flow_graph.h>
#include <tbb/parallel_pipeline.h>

class ParaSyncWorker {
public:
    std::unique_ptr<tbb_crc_to_chunks_map> weak_hash_table;
    oneapi::tbb::task_arena arena;
public:
    
    ParaSyncWorker();
    ~ParaSyncWorker();

    void init(int thread_num);

    void detector(std::vector<csegment*> &seg_array, char *map, uint64_t thread_num, uint64_t fs, DataQueue<one_cdc> &csums_queue);

    void parallel_cdc(int fd, const uint32_t thread_num, DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue);

    virtual void weak_hash_table_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) = 0;
};

class ClientParaSyncWorker : public ParaSyncWorker {
public:
    std::unique_ptr<tbb_blake_to_chunk_map> strong_hash_table;
    
    DataQueue<one_cdc> new_csums_queue;
    DataQueue<uint32_t> new_crc32_queue;

    DataQueue<matched_item_rpc_1> weak_matched_chunks_queue;
    DataQueue<matched_item_rpc> strong_matched_chunks_queue;
    DataQueue<data_cmd> data_cmd_queue;

    ClientParaSyncWorker();
    ~ClientParaSyncWorker();

    using ParaSyncWorker::init;
    using ParaSyncWorker::parallel_cdc;
    using ParaSyncWorker::detector;

    void weak_hash_table_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) override;

    size_t strong_hash_table_builder(DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, std::vector<uint32_t> &matched_weak_hash, std::unique_ptr<tbb_blake_to_chunk_map>& hash_table);

    void parallel_smatcher_v1(int fd, const uint32_t thread_num, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    void parallel_smatcher_v2(int fd, const uint32_t thread_num, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    void parallel_smatcher_v3(int fd, const uint32_t thread_num, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);
};

class ServerParaSyncWorker : public ParaSyncWorker {
public:
    DataQueue<one_cdc> old_csums_queue;
    DataQueue<uint32_t> old_crc32_queue;
    DataQueue<uint32_t> new_crc32_queue;

    DataQueue<matched_item_rpc_1> weak_matched_chunks_queue;

    DataQueue<data_cmd> data_cmd_queue;

    ServerParaSyncWorker();
    ~ServerParaSyncWorker();

    using ParaSyncWorker::init;
    using ParaSyncWorker::parallel_cdc;
    using ParaSyncWorker::detector;

    void weak_hash_table_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) override;

    void parallel_wmatcher(int fd, const uint32_t thread_num, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue);

    void parallel_wmatcher_pipe(int fd, const uint32_t thread_num, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue);
};

#endif // PARASYNC_WORKER_H