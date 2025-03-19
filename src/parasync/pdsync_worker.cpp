#include "pdsync_worker.h"

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

PDSyncWorker::PDSyncWorker() 
    : weak_hash_table(std::make_unique<tbb_crc_to_chunks_map>()) {}

PDSyncWorker::~PDSyncWorker() {}

ClientPDSyncWorker::ClientPDSyncWorker()
    : strong_hash_table(std::make_unique<tbb_blake_to_chunk_map>()) {}

ClientPDSyncWorker::~ClientPDSyncWorker() {}

ServerPDSyncWorker::ServerPDSyncWorker() {}

ServerPDSyncWorker::~ServerPDSyncWorker() {}

void PDSyncWorker::detector(std::vector<csegment*> &seg_array, char *map, uint64_t fs, struct bitmap *bm, DataQueue<one_cdc> &csums_queue) {

    uint64_t chunk_num = 0;
    uint64_t offset = 0;
    uint64_t i = MinSize1;
    char *map_t = map;

    // Push chunks
    while (i < fs) {
        if (bitmap_test(bm, i) && (i - offset) >= MinSize1) {
            struct one_cdc cdc = {
                .offset = offset,
                .length = i - offset,
                .weak_hash = crc32_isal((uint8_t *)map_t + offset, i - offset, 0)
            };
            csums_queue.push(cdc);
            chunk_num += 1;
            offset = i;
            i += MinSize1;
        } else {
            i++;
        }
    }

    // Handle remaining data
    if (offset < fs) {
        struct one_cdc cdc = {
            .offset = offset,
            .length = fs - offset,
            .weak_hash = crc32_isal((uint8_t *)map_t + offset, fs - offset, 0)
        };
        csums_queue.push(cdc);
        chunk_num += 1;
    }

    csums_queue.setDone();
}

void PDSyncWorker::parallel_cdc(int fd, const uint32_t thread_num, DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) {
    uint64_t fs = file_size(fd);
    char* map = (char*)map_file(fd);
    struct bitmap* bm = bitmap_create(fs);

    // Configure TBB task arena and task group
    // const uint32_t thread_num = std::thread::hardware_concurrency();
    oneapi::tbb::task_arena arena(thread_num);
    tbb::task_group group;
    
    // Calculate segment sizes 
    uint64_t seg_size = fs / thread_num;
    std::vector<csegment*> seg_array(thread_num);

    // Stage 1: Parallel chunking using TBB parallel_for
    auto start = std::chrono::high_resolution_clock::now();
    
    arena.execute([&]() {
        oneapi::tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
            uint64_t offset = i * seg_size;
            uint64_t end = (i == thread_num - 1) ? fs : offset + seg_size;
            uint64_t length = end - offset;

            PDSyncChunker chunker;
            chunker.init(map, offset, length);
            seg_array[i] = chunker.task(bm);
        });
    });

    auto chunking_time = std::chrono::high_resolution_clock::now() - start;
    printf("Parallel CDC Stage 1,%f\n", 
           std::chrono::duration<double>(chunking_time).count());

    // Stage 2: Serial detection and building weak hash table
    start = std::chrono::high_resolution_clock::now();

    arena.execute([&]() {
        group.run([&]() {
            this->detector(seg_array, map, fs, bm, csums_queue);
        });
        
        group.run([&]() {
            this->weak_hash_table_builder(csums_queue, crc32_queue);
        });
        
        group.wait();
    });
    // this->detector(seg_array, map, fs, bm, csums_queue);
    // this->weak_hash_table_builder(csums_queue, crc32_queue);
        
    auto detection_time = std::chrono::high_resolution_clock::now() - start;
    printf("Parallel CDC Stage 2,%f\n\n", 
           std::chrono::duration<double>(detection_time).count());

    // Cleanup
    for (auto* seg : seg_array) {
        if (seg) {
            mi_free(seg->map);
            mi_free(seg);
        }
    }

    bitmap_destroy(bm);
    unmap_file(fd, map);
}

void ClientPDSyncWorker::weak_hash_table_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) {

    if (!this->weak_hash_table) {
        this->weak_hash_table = std::make_unique<tbb_crc_to_chunks_map>();
    }

    // Pre-allocate space for estimated number of chunks
    const size_t estimated_chunks = 1024 * 1024;  // 1M chunks initial estimate
    this->weak_hash_table->rehash(estimated_chunks);

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        auto it = this->weak_hash_table->find(cdc.weak_hash);
        if (it != this->weak_hash_table->end()) {
            // Add to existing vector
            struct ol ol_item = {cdc.offset, cdc.length};
            it->second.push_back(ol_item);
        } else {
            // Create new vector and insert
            std::vector<ol> ol_vec;
            ol_vec.reserve(4);
            ol_vec.push_back({cdc.offset, cdc.length});
            this->weak_hash_table->insert({cdc.weak_hash, std::move(ol_vec)});
                
            // Push CRC32 value to queue
            crc32_queue.push(cdc.weak_hash);
        }
    }
    crc32_queue.setDone();
}

void ServerPDSyncWorker::weak_hash_table_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) {

    if (!this->weak_hash_table) {
        this->weak_hash_table = std::make_unique<tbb_crc_to_chunks_map>();
    }

    // Pre-allocate space for estimated number of chunks
    const size_t estimated_chunks = 1024 * 1024;  // 1M chunks initial estimate
    this->weak_hash_table->rehash(estimated_chunks);

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        auto it = this->weak_hash_table->find(cdc.weak_hash);
        if (it != this->weak_hash_table->end()) {
            // Add to existing vector
            struct ol ol_item = {cdc.offset, cdc.length};
            it->second.push_back(ol_item);
        } else {
            // Create new vector and insert
            std::vector<ol> ol_vec;
            ol_vec.push_back({cdc.offset, cdc.length});
            this->weak_hash_table->insert({cdc.weak_hash, std::move(ol_vec)});
        }
    }
}

void ServerPDSyncWorker::parallel_wmatcher(int fd, const uint32_t thread_num, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue) {
    
    // Create task arena for parallel processing
    oneapi::tbb::task_arena arena(thread_num);

    std::vector<uint32_t> new_crc32_array;
    new_crc32_array.reserve(1024 * 1024);
    
    while (!new_crc32_queue.isDone()) {
        uint32_t hash = new_crc32_queue.pop();
        new_crc32_array.push_back(hash);
    }

    const size_t total_size = new_crc32_array.size();
    if (total_size == 0) {
        return;
    }
    uint64_t per_size = total_size / thread_num;
    
    auto &hash_table = this->weak_hash_table;

    std::vector<DataQueue<matched_item_rpc_1>> local_queues(thread_num);

    char* map = (char*)map_file(fd);

    arena.execute([&] {
        oneapi::tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
            size_t start = i * per_size;
            size_t end = (i == thread_num - 1) ? total_size : start + per_size;
            size_t chunk_size = end - start;
            
            PDSyncWmatcher matcher;
            
            char* cp_map = map;
            
            matcher.compare_weak_hash(
                cp_map,
                hash_table,  // Use the proper hash table reference
                new_crc32_array.data() + start,
                chunk_size,
                local_queues[i]
            );
        });
    });

    // Merge local queues
    for (auto& local_queue : local_queues) {
        while (!local_queue.isDone()) {
            matched_item_rpc_1 item = local_queue.pop();
            weak_matched_chunks_queue.push(item);
        }
    }

    weak_matched_chunks_queue.setDone();
    unmap_file(fd, map);
}

size_t ClientPDSyncWorker::strong_hash_table_builder(DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, std::vector<uint32_t> &matched_weak_hash, std::unique_ptr<tbb_blake_to_chunk_map>& hash_table) {

    if (hash_table == nullptr)
        hash_table = std::make_unique<tbb_blake_to_chunk_map>();

    size_t matched_size = 0;

     while (!weak_matched_chunks_queue.isDone()) {
        matched_item_rpc_1 mc_item = weak_matched_chunks_queue.pop();
        if (mc_item.weak_hash == 0) {
            continue;
        }

        matched_weak_hash.push_back(mc_item.weak_hash);
        matched_size++;

        for (auto& item : mc_item.sha_to_chunk_map) {
            if (!hash_table->contains(item.first)) {
                hash_table->emplace(item.first, std::move(item.second));
            }
        }
    }

    return matched_size;
}

void ClientPDSyncWorker::parallel_smatcher(int fd, const uint32_t thread_num, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {
    
    // Pre-allocate vector and initialize hash table
    std::vector<uint32_t> matched_weak_hash;
    matched_weak_hash.reserve(1024 * 1024);
    
    if (!this->strong_hash_table) {
        this->strong_hash_table = std::make_unique<tbb_blake_to_chunk_map>();
    }

    size_t matched_size = strong_hash_table_builder(
        weak_matched_chunks_queue, 
        matched_weak_hash, 
        this->strong_hash_table
    );

    if (matched_size == 0) {
        return;
    }

    uint64_t per_size = matched_size / thread_num;
    std::vector<DataQueue<matched_item_rpc>> local_queues(thread_num);
    char *map = (char*)map_file(fd);

    // Create task arena for parallel processing
    oneapi::tbb::task_arena arena(thread_num);

    // Distribute work for strong matching
    arena.execute([&] {
        oneapi::tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
            size_t start = i * per_size;
            size_t end = (i == thread_num - 1) ? matched_size : start + per_size;
            size_t chunk_size = end - start;
            
            PDSyncSmatcher matcher;
            char* cp_map = map;

            matcher.compare_strong_hash(
                cp_map,
                this->weak_hash_table,
                matched_weak_hash.data() + start,
                chunk_size,
                this->strong_hash_table,
                local_queues[i]
            );
        });
    });

    // Merge local queues
    for (auto& local_queue : local_queues) {
        while (!local_queue.isDone()) {
            matched_item_rpc item = local_queue.pop();
            strong_matched_chunks_queue.push(item);
        }
    }

    strong_matched_chunks_queue.setDone();
    unmap_file(fd, map);
}