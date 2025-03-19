#include <chrono>
#include "skysync_c.h"
#include "parallel_cdc.h"
#include "dsync_worker.h"

off_t file_size(int fd);

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <old_file> <new_file>\n", argv[0]);
        exit(1);
    }

    // struct file_cdc *old_fc = parallel_run_cdc(argv[1], thread_num, threads, &stats, whichone);

    // struct file_cdc *new_fc = parallel_run_cdc(argv[2], thread_num, threads, &stats);

    // printf("Parallel CDC Stage 1: %f\n", (double)stats.cdc_stage_1 / CLOCKS_PER_SEC);
    // printf("Serial CDC Stage 2: %f\n", (double)stats.cdc_stage_2 / CLOCKS_PER_SEC);

    int old_fd = open(argv[1], O_RDONLY);
    if (old_fd == -1) {
        printf("open file %s failed\n", argv[1]);
        exit(1);
    }

    int new_fd = open(argv[2], O_RDONLY);
    if (new_fd == -1) {
        printf("open file %s failed\n", argv[2]);
        exit(1);
    }

    char output_file[strlen(argv[1]) + 20];
    sprintf(output_file, "%s.patch", argv[1]);

    int output_fd = open(output_file, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (output_fd == -1) {
        printf("open file %s failed\n", output_file);
        exit(1);
    }

    ClientSyncWorker client_worker;
    ServerSyncWorker server_worker;

    /* server */
    auto start = std::chrono::high_resolution_clock::now();
    server_worker.serial_cdc(old_fd, server_worker.old_csums_queue);
    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Serial CDC time,%f\n", diff_1.count());

    /* client */
    start = std::chrono::high_resolution_clock::now();
    client_worker.serial_cdc(new_fd, client_worker.new_csums_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Serial CDC time,%f\n", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    client_worker.chash_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Build Hash Table time,%f\n", diff_1.count());

    #ifdef SIZE_TEST
        double list_size = client_worker.new_crc32_queue.size() * sizeof(uint32_t) / 1024.0 / 1024.0;
        printf("Checksum List Size,%f MB\n", list_size);
        // 500 Mbps = 62.5 MB/s
        printf("Network Transfer Time,%f\n", list_size / 62.5);
    #endif

    /* server */
    start = std::chrono::high_resolution_clock::now();
    // build uthash table for the checksums of the old file
    // server_worker.uthash_builder(server_worker.old_csums_queue);
    // build chash table for the checksums of the old file
    server_worker.chash_builder(server_worker.old_csums_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Build Hash Table time,%f\n", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    // use uthash to compare weak hash
    // server_worker.compare_weak_uthash(old_fd, client_worker.new_csums_queue, server_worker.weak_matched_chunks_queue);
    // use light hash to compare weak hash
    server_worker.compare_weak_chash(old_fd, client_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Compare Weak Hash time,%f\n", diff_1.count());

    #ifdef SIZE_TEST
        double token_size = matching_tokens_size / 1024.0 / 1024.0;
        printf("Matching Tokens Size,%f MB\n", token_size);
        printf("Network Transfer Time,%f\n", token_size / 62.5);
    #endif

    /* client */
    start = std::chrono::high_resolution_clock::now();
    // client_worker.compare_sha1(new_fd, server_worker.weak_matched_chunks_queue, client_worker.strong_matched_chunks_queue);
    client_worker.compare_sha1_1(new_fd, server_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Compare SHA1 time,%f\n", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    client_worker.generate_delta(new_fd, client_worker.strong_matched_chunks_queue, client_worker.data_cmd_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Generate Delta time,%f\n", diff_1.count());

    #ifdef SIZE_TEST
        double delta_size = (patch_commands_size + literal_bytes_size) / 1024.0 / 1024.0;
        printf("Patch Commands Size,%f MB\n", patch_commands_size / 1024.0 / 1024.0);
        printf("Literal Bytes Size,%f MB\n", literal_bytes_size / 1024.0 / 1024.0);
        printf("Delta Size,%f MB\n", delta_size);
        printf("Network Transfer Time,%f\n", delta_size / 62.5);
    #endif

    /* server */
    start = std::chrono::high_resolution_clock::now();
    server_worker.patch_delta(old_fd, output_fd, client_worker.data_cmd_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Patch Delta time,%f\n\n", diff_1.count());

    #ifdef SIZE_TEST
        printf("Same CRC32c Chunks,%ld\n", same_crc32c_chunks.size());
        for (auto a : same_crc32c_chunks) {
            printf("%ld,", a);
        }
        printf("\n");
    #endif

    // /* Pipeline Sync Task Time Test */
    // printf("Pipeline Sync Task Time Test: 3 Threads\n");

    // std::vector<std::thread> threads;
    // threads.resize(3);

    // start = std::chrono::high_resolution_clock::now();
    // threads[0] = std::thread([&client_worker, new_fd]() {
    //     client_worker.serial_cdc(new_fd, client_worker.new_csums_queue);
    // });
    // threads[1] = std::thread([&server_worker, old_fd]() {
    //     server_worker.serial_cdc(old_fd, server_worker.old_csums_queue);
    // });
    // threads[2] = std::thread([&server_worker]() {
    //     server_worker.uthash_builder(server_worker.old_csums_queue);
    // });

    // // Thread barrier
    // for (auto &t : threads) {
    //     t.join();
    // }

    // threads[0] = std::thread([&server_worker, old_fd, &client_worker]() {
    //     server_worker.compare_weak_hash(old_fd, client_worker.new_csums_queue, server_worker.weak_matched_chunks_queue);
    // });

    // threads[1] = std::thread([&client_worker, new_fd, &server_worker]() {
    //     client_worker.compare_sha1(new_fd, server_worker.weak_matched_chunks_queue, client_worker.strong_matched_chunks_queue);
    // });

    // // Thread barrier
    // for (int i = 0; i < 2; i++) {
    //     threads[i].join();
    // }

    // threads[0] = std::thread([&client_worker, new_fd, &server_worker]() {
    //     client_worker.generate_delta(new_fd, client_worker.strong_matched_chunks_queue, client_worker.data_cmd_queue);
    // });

    // threads[1] = std::thread([&server_worker, old_fd, output_fd, &client_worker]() {
    //     server_worker.patch_delta(old_fd, output_fd, client_worker.data_cmd_queue);
    // });

    // // Thread barrier
    // for (int i = 0; i < 2; i++) {
    //     threads[i].join();
    // }

    // diff_1 = std::chrono::high_resolution_clock::now() - start;
    // printf("Pipeline Sync Task time,%f\n", diff_1.count());

    // if (old_fc != NULL) {
    //     mi_free(old_fc->cdc_array);
    //     mi_free(old_fc);
    // }

    close(old_fd);
    close(new_fd);
    return 0;
}

