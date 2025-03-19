#include "parasync_worker.h"
#include <oneapi/tbb/global_control.h>

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

ParaSyncWorker::ParaSyncWorker()
    : weak_hash_table(std::make_unique<tbb_crc_to_chunks_map>()) {}

ParaSyncWorker::~ParaSyncWorker() {}

void ParaSyncWorker::init(int thread_num) {
    arena.initialize(thread_num);
}

ClientParaSyncWorker::ClientParaSyncWorker()
    : strong_hash_table(std::make_unique<tbb_blake_to_chunk_map>()) {}

ClientParaSyncWorker::~ClientParaSyncWorker() {}

ServerParaSyncWorker::ServerParaSyncWorker() {}

ServerParaSyncWorker::~ServerParaSyncWorker() {}

void ParaSyncWorker::detector(std::vector<csegment*> &seg_array, char *map, uint64_t thread_num, uint64_t fs, DataQueue<one_cdc> &total_csums_queue) {

    uint64_t chunk_num = 0;
    uint32_t combined_hash = 0;
    uint64_t total_length = 0;
    uint64_t start_offset = 0;
    bool first = true;

    for (int i = 0; i < thread_num; i++) {
        DataQueue<one_cdc> *csums_queue = seg_array[i]->csums_queue;
        
        while (!csums_queue->isDone()) {
            struct one_cdc curr_cdc = csums_queue->pop();

            if (first) {
                start_offset = curr_cdc.offset;
                first = false;
            }

            total_length += curr_cdc.length;
            combined_hash = crc32_comb(combined_hash, curr_cdc.weak_hash, curr_cdc.length);
            
            // Push combined chunk only when size threshold met
            if (total_length >= MinSize) {
                struct one_cdc combined = {
                    .offset = start_offset,
                    .length = total_length,
                    .weak_hash = combined_hash
                };
                total_csums_queue.push(combined);
                chunk_num += 1;

                // Reset for the next chunk
                total_length = 0;
                combined_hash = 0;
                first = true;
            }
        }

        delete csums_queue;
    }

    // Handle any remaining data after all segments processed
    if (total_length > 0) {
        struct one_cdc combined = {
            .offset = start_offset,
            .length = total_length,
            .weak_hash = combined_hash
        };
        total_csums_queue.push(combined);
        chunk_num += 1;
    }

    total_csums_queue.setDone();
}

void ParaSyncWorker::parallel_cdc(int fd, const uint32_t thread_num, DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) {
    uint64_t fs = file_size(fd);
    char* map = (char*)map_file(fd);

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

            ParaSyncChunker chunker;
            chunker.init(map, offset, length);
            seg_array[i] = chunker.task();
        });
    });

    auto chunking_time = std::chrono::high_resolution_clock::now() - start;
    printf("Parallel CDC Stage 1,%f\n", 
           std::chrono::duration<double>(chunking_time).count());

    // Stage 2: Serial detection and building weak hash table
    start = std::chrono::high_resolution_clock::now();

    arena.execute([&]() {
        group.run([&]() {
            this->detector(seg_array, map, thread_num, fs, csums_queue);
        });
        
        group.run([&]() {
            this->weak_hash_table_builder(csums_queue, crc32_queue);
        });
        
        group.wait();
    });
    // this->detector(seg_array, map, thread_num, fs, csums_queue);
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

    unmap_file(fd, map);
}

void ClientParaSyncWorker::weak_hash_table_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) {

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

void ServerParaSyncWorker::weak_hash_table_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) {

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

void ServerParaSyncWorker::parallel_wmatcher(int fd, const uint32_t thread_num, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue) {

    std::vector<uint32_t> new_crc32_array;
    new_crc32_array.reserve(1024 * 1024);
    
    while (!new_crc32_queue.isDone())
        new_crc32_array.push_back(new_crc32_queue.pop());

    if (new_crc32_array.empty())
        return;
    
    auto &hash_table = this->weak_hash_table;

    oneapi::tbb::concurrent_vector<wmatched_item> matched_chunks;

    auto start = std::chrono::high_resolution_clock::now();

    arena.execute([&] {
        oneapi::tbb::parallel_for(
            oneapi::tbb::blocked_range<size_t>(0, new_crc32_array.size()),
            [&](const oneapi::tbb::blocked_range<size_t> &range) {

                std::vector<wmatched_item> local_matches;
                ParaSyncWmatcher wmatcher;
                wmatcher.compare(hash_table, new_crc32_array.data() + range.begin(), range.size(), local_matches);
                matched_chunks.grow_by(local_matches.begin(), local_matches.end());
            },
            oneapi::tbb::auto_partitioner() // Use auto_partitioner for load balancing
        );
    });

    auto end = std::chrono::high_resolution_clock::now();
    printf("Stage 1, %f\n", std::chrono::duration<double>(end - start).count());

    auto total_size = matched_chunks.size();
    if (total_size == 0)
        return;
    auto per_size = total_size / thread_num;

    // Convert concurrent_vector to regular vector for processing
    std::vector<wmatched_item> matched_chunks_vec(matched_chunks.begin(), matched_chunks.end());

    std::vector<DataQueue<wmatched_item>> local_queues(thread_num);

    char* map = (char*)map_file(fd);

    start = std::chrono::high_resolution_clock::now();
    arena.execute([&] {
        oneapi::tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
            size_t start = i * per_size;
            size_t end = (i == thread_num - 1) ? total_size : start + per_size;
            size_t chunk_size = end - start;
            
            ParaSyncWmatcher wmatcher;
            char* cp_map = map;
            wmatcher.calculate(cp_map, matched_chunks_vec.data() + start, chunk_size, local_queues[i]);
        });
    });
    end = std::chrono::high_resolution_clock::now();
    printf("Stage 2, %f\n", std::chrono::duration<double>(end - start).count());

    unmap_file(fd, map);

    start = std::chrono::high_resolution_clock::now();
    ParaSyncWmatcher wmatcher;
    wmatcher.post_calculate(local_queues.data(), thread_num, weak_matched_chunks_queue);
    end = std::chrono::high_resolution_clock::now();
    printf("Stage 3, %f\n", std::chrono::duration<double>(end - start).count());

    weak_matched_chunks_queue.setDone();
    
}

void ServerParaSyncWorker::parallel_wmatcher_pipe(int fd, const uint32_t thread_num, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue) {

    struct BatchData {
        std::vector<uint32_t> weak_hashes;
        std::vector<wmatched_item> matched_chunks;
        oneapi::tbb::concurrent_unordered_map<uint32_t, 
            std::unordered_map<std::string, ol>> results;
    };
    
    auto &hash_table = this->weak_hash_table;

    char* map = (char*)map_file(fd);

    arena.execute([&] {
        tbb::parallel_pipeline(
            thread_num * NUM_TOKENS,
            // Stage 1: Split input into chunks
            tbb::make_filter<void, std::shared_ptr<BatchData>>(
                tbb::filter_mode::serial_in_order,
                [&](tbb::flow_control& fc) -> std::shared_ptr<BatchData> {

                    if (new_crc32_queue.isDone()) {
                        fc.stop();
                        return nullptr;
                    }

                    auto batch = std::make_shared<BatchData>();
                    batch->weak_hashes.reserve(BATCH_SIZE);
                    // insert new_crc32_queue into weak_hashes
                    while (!new_crc32_queue.empty() && batch->weak_hashes.size() < BATCH_SIZE) {
                        batch->weak_hashes.push_back(new_crc32_queue.pop());
                    }

                    return batch->weak_hashes.empty() ? nullptr : batch;
                }
            ) &

            // Stage 2: Weak hash matching
            tbb::make_filter<std::shared_ptr<BatchData>, std::shared_ptr<BatchData>>(
                tbb::filter_mode::parallel,
                [&](std::shared_ptr<BatchData> batch) -> std::shared_ptr<BatchData> {
                    if (!batch) return nullptr;
                    batch->matched_chunks.reserve(batch->weak_hashes.size());
                    ParaSyncWmatcher wmatcher;
                    wmatcher.compare(hash_table, batch->weak_hashes.data(), batch->weak_hashes.size(), batch->matched_chunks);
                    return batch;
                }
            ) &

            // Stage 3: Strong hash calculation
            tbb::make_filter<std::shared_ptr<BatchData>, void>(
                tbb::filter_mode::parallel,
                [&](std::shared_ptr<BatchData> batch) {
                    if (!batch || batch->matched_chunks.empty()) return;
                    
                    const size_t items_per_thread = 
                            (batch->matched_chunks.size() + thread_num - 1) / thread_num;

                    tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
                        size_t start = i * items_per_thread;
                        if (start >= batch->matched_chunks.size()) return;

                        size_t end = std::min(start + items_per_thread, 
                            batch->matched_chunks.size());

                        ParaSyncWmatcher wmatcher;
                        DataQueue<wmatched_item> local_queue;
                            
                        char* cp_map = map;
                        wmatcher.calculate(cp_map, batch->matched_chunks.data() + start, end - start, local_queue);

                        // Process results thread-safely
                        while (!local_queue.isDone()) {
                            wmatched_item item = local_queue.pop();
                            auto it = batch->results.find(item.weak_hash);
                            if (it != batch->results.end()) {
                                auto sha_it = it->second.find(
                                    std::string((char *)item.hash, BLAKE3_OUT_LEN));
                                if (sha_it == it->second.end()) {
                                    it->second.emplace(
                                        std::string((char *)item.hash, BLAKE3_OUT_LEN),
                                        std::move(item.old_ol)
                                    );
                                }
                            } else {
                                std::unordered_map<std::string, ol> sha_map;
                                sha_map.emplace(
                                    std::string((char *)item.hash, BLAKE3_OUT_LEN),
                                    std::move(item.old_ol)
                                );
                                batch->results.insert({item.weak_hash, std::move(sha_map)});
                            }
                        }
                    });

                    // Push results to next stage
                    for (auto& item : batch->results) {
                        matched_item_rpc_1 mc_item;
                        mc_item.weak_hash = item.first;
                        mc_item.item_nums = item.second.size();
                        mc_item.end_of_stream = false;
                        mc_item.sha_to_chunk_map = std::move(item.second);
                        weak_matched_chunks_queue.push(std::move(mc_item));
                    }
                }
            )
        );
    });

    unmap_file(fd, map);
    weak_matched_chunks_queue.setDone();
}

size_t ClientParaSyncWorker::strong_hash_table_builder(DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, std::vector<uint32_t> &matched_weak_hash, std::unique_ptr<tbb_blake_to_chunk_map>& hash_table) {

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

void ClientParaSyncWorker::parallel_smatcher_v1(int fd, const uint32_t thread_num, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {

    struct BatchData {
        std::unique_ptr<tbb_blake_to_chunk_map> strong_hash_table;
        std::vector<uint32_t> weak_hashes;
        // std::vector<matched_item_rpc> results;
        oneapi::tbb::concurrent_vector<matched_item_rpc> results;
        size_t size = 0;
    };
    
    char* map = (char*)map_file(fd);

    auto &weak_hash_table = this->weak_hash_table;
    tbb::concurrent_queue<matched_item_rpc> global_results_queue;

    auto start = std::chrono::high_resolution_clock::now();
    arena.execute([&] {
        tbb::parallel_pipeline(
            thread_num * NUM_TOKENS,
            // Stage 1: Build strong hash table
            tbb::make_filter<void, std::shared_ptr<BatchData>>(
                tbb::filter_mode::parallel,
                [&](tbb::flow_control& fc) -> std::shared_ptr<BatchData> {
                    if (weak_matched_chunks_queue.isDone()) {
                        fc.stop();
                        return nullptr;
                    }

                    auto batch = std::make_shared<BatchData>();
                    batch->strong_hash_table = std::make_unique<tbb_blake_to_chunk_map>();
                    batch->weak_hashes.reserve(BATCH_SIZE);

                    while (!weak_matched_chunks_queue.isDone() && batch->size < BATCH_SIZE) {
                        auto mc_item = weak_matched_chunks_queue.pop();
                        if (mc_item.weak_hash == 0) continue;

                        batch->weak_hashes.push_back(mc_item.weak_hash);
                        for (auto& item : mc_item.sha_to_chunk_map) {
                            if (!batch->strong_hash_table->contains(item.first)) {
                                batch->strong_hash_table->emplace(item.first, std::move(item.second));
                            }
                        }
                        batch->size++;
                    }

                    return batch->size > 0 ? batch : nullptr;
                }
            ) &

            // Stage 2: Processing
            tbb::make_filter<std::shared_ptr<BatchData>, void>(
                tbb::filter_mode::parallel,
                [&](std::shared_ptr<BatchData> batch) {
                    if (!batch) return;

                    const size_t items_per_thread = batch->size / thread_num;
                    std::vector<DataQueue<matched_item_rpc>> local_queues(thread_num);

                    tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
                        size_t start = i * items_per_thread;
                        if (start >= batch->size) return;

                        size_t end = (i == thread_num - 1) ? batch->size : start + items_per_thread;

                        char* cp_map = map;

                        ParaSyncSmatcher matcher;
                        matcher.compare_strong_hash(
                            cp_map,
                            weak_hash_table,
                            batch->weak_hashes.data() + start,
                            end - start,
                            batch->strong_hash_table,
                            local_queues[i]
                        );
                    });

                    // Collect results
                    for (auto& queue : local_queues) {
                        while (!queue.isDone()) {
                            global_results_queue.push(queue.pop());
                        }
                    }
                }
            )
        );
    });
    auto end = std::chrono::high_resolution_clock::now();
    printf("Stage 1, %f\n", std::chrono::duration<double>(end - start).count());

    // Process results
    start = std::chrono::high_resolution_clock::now();
    matched_item_rpc mc_item;
    while (global_results_queue.try_pop(mc_item)) {
        strong_matched_chunks_queue.push(std::move(mc_item));
    }
    end = std::chrono::high_resolution_clock::now();
    printf("Stage 2, %f\n", std::chrono::duration<double>(end - start).count());
    
    strong_matched_chunks_queue.setDone();
}

void ClientParaSyncWorker::parallel_smatcher_v2(int fd, const uint32_t thread_num, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {

    struct BatchData {
        std::vector<uint32_t> weak_hashes;
        std::vector<std::pair<std::string, ol>> old_strong_hashes;
        std::vector<ol> new_ols;
        std::shared_ptr<tbb_blake_to_chunk_map> strong_hash_table;
        oneapi::tbb::concurrent_vector<matched_item_rpc> results;
    };

    tbb::flow::graph g;

    // 1. input node for splitting weak_hashes based on BATCH_SIZE
    tbb::flow::input_node<std::shared_ptr<BatchData>>
    weak_hashes_node(g, [&](tbb::flow_control& fc) -> std::shared_ptr<BatchData> {
        auto batch = std::make_shared<BatchData>();
        batch->weak_hashes.reserve(BATCH_SIZE);
        batch->old_strong_hashes.reserve(BATCH_SIZE);

        while (!weak_matched_chunks_queue.isDone() && batch->weak_hashes.size() < BATCH_SIZE) {
            auto mc_item = weak_matched_chunks_queue.pop();
            if (mc_item.weak_hash == 0) continue;

            batch->weak_hashes.push_back(mc_item.weak_hash);
            for (auto& item : mc_item.sha_to_chunk_map) {
                batch->old_strong_hashes.push_back({item.first, item.second});
            }
        }

        if (batch->weak_hashes.empty()) {
            fc.stop();
            return nullptr;
        }

        return batch;
    });

    // 2. processing node for building strong hash table
    tbb::flow::function_node<std::shared_ptr<BatchData>,
    std::shared_ptr<BatchData>> build_strong_hash_table_node(
        g,
        tbb::flow::unlimited,
        [&](std::shared_ptr<BatchData> batch) -> std::shared_ptr<BatchData> {
            if (!batch) return nullptr;

            batch->strong_hash_table = std::make_shared<tbb_blake_to_chunk_map>();
            
            for (auto& hash : batch->old_strong_hashes) {
                batch->strong_hash_table->emplace(hash.first, hash.second);
            }
            return batch;
        }
    );

    // 3. processing node for searching chash table to find weak hash matches
    tbb::flow::function_node<std::shared_ptr<BatchData>,
    std::shared_ptr<BatchData>> weak_hash_match_node(
        g,
        tbb::flow::unlimited,
        [&](std::shared_ptr<BatchData> batch) -> std::shared_ptr<BatchData> {
            if (!batch) return nullptr;
            batch->new_ols.reserve(BATCH_SIZE);
            for (auto& hash : batch->weak_hashes) {
                auto it = this->weak_hash_table->find(hash);
                if (it != this->weak_hash_table->end()) {
                        batch->new_ols.insert(batch->new_ols.end(), it->second.begin(), it->second.end());
                }
            }
            return batch;
        }
    );

    // 4. sync nodes for processing results
    using tuple_type = std::tuple<std::shared_ptr<BatchData>, std::shared_ptr<BatchData>>;
    tbb::flow::join_node<tuple_type, tbb::flow::queueing> sync_node(g);

    // 5. processing node for calculating strong hash and comparing
    char* map = (char*)map_file(fd);

    tbb::flow::function_node<tuple_type,
    std::shared_ptr<BatchData>> strong_hash_match_node(
        g,
        tbb::flow::unlimited,
        [&](tuple_type tuple) -> std::shared_ptr<BatchData> {
            if (!std::get<0>(tuple) || !std::get<1>(tuple)) return nullptr;
            auto batch = std::get<0>(tuple);
            try {
                arena.execute([&] {
                    tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
                        size_t start = i * batch->new_ols.size() / thread_num;
                        size_t end = (i == thread_num - 1) ? batch->new_ols.size() : start + batch->new_ols.size() / thread_num;
                        size_t chunk_size = end - start;

                        char* cp_map = map;
                        ParaSyncSmatcher smatcher;
                        smatcher.calculate(cp_map, batch->new_ols.data() + start,
                        chunk_size, batch->strong_hash_table, batch->results);
                    });
                });
            } catch (...) {
                printf("Error in strong hash match\n");
                return nullptr;
            }
            return batch;
        }
    );

    // 6. processing node for collecting results
    tbb::flow::function_node<std::shared_ptr<BatchData>> collect_results_node(
        g,
        tbb::flow::serial,
        [&](std::shared_ptr<BatchData> batch) -> tbb::flow::continue_msg {
            if (batch) {
                for (auto& item : batch->results) {
                    strong_matched_chunks_queue.push(item);
                }
            }
            return tbb::flow::continue_msg();
        }
    );

    tbb::flow::make_edge(weak_hashes_node, build_strong_hash_table_node);
    tbb::flow::make_edge(weak_hashes_node, weak_hash_match_node);
    tbb::flow::make_edge(build_strong_hash_table_node, std::get<0>(sync_node.input_ports()));
    tbb::flow::make_edge(weak_hash_match_node, std::get<1>(sync_node.input_ports()));
    tbb::flow::make_edge(sync_node, strong_hash_match_node);
    tbb::flow::make_edge(strong_hash_match_node, collect_results_node);

    oneapi::tbb::global_control control(oneapi::tbb::global_control::max_allowed_parallelism, thread_num);

    arena.execute([&] {
        weak_hashes_node.activate();
        g.wait_for_all();
    });

    strong_matched_chunks_queue.setDone();
    unmap_file(fd, map);
}

void ClientParaSyncWorker::parallel_smatcher_v3(int fd, const uint32_t thread_num, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {

    struct BatchData {
        std::vector<uint32_t> weak_hashes;
        std::vector<ol> new_ols;
        std::shared_ptr<tbb_blake_to_chunk_map> strong_hash_table;
        oneapi::tbb::concurrent_vector<matched_item_rpc> results;
    };

    char* map = (char*)map_file(fd);

    arena.execute([&] {
        tbb::parallel_pipeline(
            thread_num * NUM_TOKENS,
            
            // Stage 1: Split weak hashes into batches
            tbb::make_filter<void, std::shared_ptr<BatchData>>(
                tbb::filter_mode::serial_in_order,
                [&](tbb::flow_control& fc) -> std::shared_ptr<BatchData> {
                    if (weak_matched_chunks_queue.isDone()) {
                        fc.stop();
                        return nullptr;
                    }

                    auto batch = std::make_shared<BatchData>();
                    batch->weak_hashes.reserve(BATCH_SIZE);
                    batch->new_ols.reserve(BATCH_SIZE);
                    batch->strong_hash_table = std::make_shared<tbb_blake_to_chunk_map>();

                    while (!weak_matched_chunks_queue.isDone() && 
                           batch->weak_hashes.size() < BATCH_SIZE) {
                        auto mc_item = weak_matched_chunks_queue.pop();
                        if (mc_item.weak_hash == 0) continue;

                        // Store weak hash
                        batch->weak_hashes.push_back(mc_item.weak_hash);

                        // Build strong hash table
                        for (auto& item : mc_item.sha_to_chunk_map) {
                            batch->strong_hash_table->emplace(item.first, item.second);
                        }
                    }

                    return batch->weak_hashes.empty() ? nullptr : batch;
                }
            ) &

            // Stage 2: Process weak hashes and find matches
            tbb::make_filter<std::shared_ptr<BatchData>, std::shared_ptr<BatchData>>(
                tbb::filter_mode::parallel,
                [&](std::shared_ptr<BatchData> batch) -> std::shared_ptr<BatchData> {
                    if (!batch) return nullptr;

                    // Find weak hash matches
                    for (auto& hash : batch->weak_hashes) {
                        auto it = this->weak_hash_table->find(hash);
                        if (it != this->weak_hash_table->end()) {
                            batch->new_ols.insert(batch->new_ols.end(), it->second.begin(), it->second.end());
                        }
                    }
                    return batch;
                }
            ) &

            // Stage 3: Calculate strong hashes and compare
            tbb::make_filter<std::shared_ptr<BatchData>, void>(
                tbb::filter_mode::parallel,
                [&](std::shared_ptr<BatchData> batch) {
                    if (!batch || batch->new_ols.empty()) return;

                    const size_t items_per_thread = 
                        (batch->new_ols.size() + thread_num - 1) / thread_num;

                    tbb::parallel_for(0U, thread_num, [&](uint32_t i) {
                        size_t start = i * items_per_thread;
                        if (start >= batch->new_ols.size()) return;

                        size_t end = std::min(start + items_per_thread, 
                            batch->new_ols.size());
                        size_t chunk_size = end - start;

                        ParaSyncSmatcher smatcher;
                        smatcher.calculate(map, 
                            batch->new_ols.data() + start,
                            chunk_size, 
                            batch->strong_hash_table, 
                            batch->results);
                    });

                    // Push results
                    for (auto& item : batch->results) {
                        strong_matched_chunks_queue.push(item);
                    }
                }
            )
        );
    });

    strong_matched_chunks_queue.setDone();
    unmap_file(fd, map);
}