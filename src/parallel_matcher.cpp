#include <assert.h>
#include <chrono>
#include "thpool.h"
#include "parallel_matcher.h"

void wmatcher_thread_worker(void *arg) {
    wmatcher_task *task = (wmatcher_task *)arg;
    PDSyncWmatcher matcher;

    matcher.compare_weak_hash(task->fd, task->chash_table, task->weak_hash, task->size, *(task->matched_chunks_queue));
    close(task->fd);
}

void smatcher_thread_worker(void *arg) {
    smatcher_task *task = (smatcher_task *)arg;
    PDSyncSmatcher matcher;

    matcher.compare_strong_hash(task->fd, task->chash_table, task->matched_weak_hash, task->matched_size, task->strong_hash_table, *(task->strong_matched_chunks_queue));
    close(task->fd);
}

size_t process_crc32_queue (DataQueue<uint32_t> &new_crc32_queue, uint32_t *weak_hash) {
    weak_hash = (uint32_t *) mi_malloc(new_crc32_queue.size() * sizeof(uint32_t));

    size_t i = 0;
    while (!new_crc32_queue.isDone()) {
        weak_hash[i] = new_crc32_queue.pop();
        i++;
    }

    return i;
}

void parallel_matcher_pdsync(int old_fd, int new_fd, uint32_t thread_num, ClientSyncWorker &client_worker, ServerSyncWorker &server_worker) {

    threadpool thpool = thpool_init(thread_num);

    uint32_t *new_crc32 = nullptr;

    size_t new_crc32_size = process_crc32_queue(client_worker.new_crc32_queue, new_crc32);
    
    size_t wmatcher_per_size = new_crc32_size / thread_num;
    std::vector<wmatcher_task> wmatcher_tasks(thread_num);
    std::vector<DataQueue<matched_item_rpc_1>> wmatcher_queues(thread_num);

    // Distribute work for weak matching
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < thread_num; i++) {
        size_t start_idx = i * wmatcher_per_size;
        size_t task_size = (i == thread_num - 1) ? 
                          new_crc32_size - start_idx : wmatcher_per_size;
        
        wmatcher_tasks[i] = {
            .fd = dup(old_fd),
            .chash_table = server_worker.chash_table,
            .weak_hash = new_crc32 + start_idx,
            .size = task_size,
            .matched_chunks_queue = &wmatcher_queues[i]
        };
        
        thpool_add_work(thpool, wmatcher_thread_worker, &wmatcher_tasks[i]);
    }

    thpool_wait(thpool);
    std::chrono::duration<double> diff = std::chrono::high_resolution_clock::now() - start;
    printf("Server Compare Weak Hash time,%f\n", diff.count());

    // Merge results from weak matching
    start = std::chrono::high_resolution_clock::now();
    for (auto& queue : wmatcher_queues) {
        while (!queue.isDone()) {
            server_worker.weak_matched_chunks_queue_1.push(queue.pop());
        }
    }
    server_worker.weak_matched_chunks_queue_1.setDone();
    diff = std::chrono::high_resolution_clock::now() - start;
    printf("Server Merge Matched Chunks time,%f\n", diff.count());

    uint32_t *matched_weak_hash = nullptr;
    strong_hash_to_chunk_map *hash_table = new strong_hash_to_chunk_map();

    start = std::chrono::high_resolution_clock::now();
    size_t matched_size = strong_hash_table_builder(server_worker.weak_matched_chunks_queue_1, matched_weak_hash, hash_table);
    diff = std::chrono::high_resolution_clock::now() - start;
    printf("Client Build Strong Hash Table time,%f\n", diff.count());

    size_t smatcher_per_size = matched_size / thread_num;
    std::vector<smatcher_task> smatcher_tasks(thread_num);
    std::vector<DataQueue<matched_item_rpc>> smatcher_queues(thread_num);

    // Distribute work for strong matching
    start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < thread_num; i++) {
        size_t start_idx = i * smatcher_per_size;
        size_t task_size = (i == thread_num - 1) ? 
                            matched_size - start_idx : smatcher_per_size;
        
        smatcher_tasks[i] = {
            .fd = dup(new_fd),
            .chash_table = client_worker.chash_table,
            .matched_weak_hash = matched_weak_hash + start_idx,
            .matched_size = task_size,
            .strong_hash_table = hash_table,
            .strong_matched_chunks_queue = &smatcher_queues[i]
        };
        
        thpool_add_work(thpool, smatcher_thread_worker, &smatcher_tasks[i]);
    }

    thpool_wait(thpool);
    diff = std::chrono::high_resolution_clock::now() - start;
    printf("Client Compare Strong Hash time,%f\n", diff.count());

    // Merge results from strong matching
    start = std::chrono::high_resolution_clock::now();
    for (auto& queue : smatcher_queues) {
        while (!queue.isDone()) {
            client_worker.strong_matched_chunks_queue.push(queue.pop());
        }
    }
    client_worker.strong_matched_chunks_queue.setDone();
    diff = std::chrono::high_resolution_clock::now() - start;
    printf("Client Merge Strong Matched Chunks time,%f\n", diff.count());
}