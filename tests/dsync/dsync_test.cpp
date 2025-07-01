#include <chrono>
#include "skysync_c.h"
#include "dsync.h"

off_t file_size(int fd);

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <old_file> <new_file> [hw]\n", argv[0]);
        printf("  hw: 0 for non-hardware accelerated, 1 for hardware accelerated (default is 0)\n");
        exit(1);
    }

    StrongHashingAlgorithm shash_algorithm = StrongHashingAlgorithm::SHA1;
    uint8_t whash_algorithm = 0;
    if (argc >= 4) {
        int algo = atoi(argv[3]);
        switch (algo) {
            case 0:
                shash_algorithm = StrongHashingAlgorithm::SHA1;
                whash_algorithm = 0;
                printf("Using non-hardware accelerated hashing algorithm\n");
                break;
            case 1:
                shash_algorithm = StrongHashingAlgorithm::SHA256;
                whash_algorithm = 1;
                printf("Using hardware accelerated hashing algorithm\n");
                break;
            default:
                printf("Invalid hash algorithm %d, using non-hardware accelerated hashing algorithm (default)\n", algo);
                break;
        }
    } else {
        printf("Using non-hardware accelerated hashing algorithm (default)\n");
    }

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

    ClientSyncWorker client_worker(shash_algorithm, whash_algorithm);
    ServerSyncWorker server_worker(shash_algorithm, whash_algorithm);

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
    // client_worker.lhash_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue, file_size(new_fd) / 8192 / 2);
    client_worker.chash_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue);
    std::chrono::duration<double> diff_client_build = std::chrono::high_resolution_clock::now() - start;
    // printf("Client Build Hash Table time,%f\n", diff_client_build.count());

    /* server */
    start = std::chrono::high_resolution_clock::now();
    // server_worker.lhash_builder(server_worker.old_csums_queue, file_size(old_fd) / 8192 / 2);
    server_worker.chash_builder(server_worker.old_csums_queue);
    std::chrono::duration<double> diff_server_build = std::chrono::high_resolution_clock::now() - start;
    // printf("Server Build Hash Table time,%f\n", diff_server_build.count());

    start = std::chrono::high_resolution_clock::now();
    // server_worker.compare_weak_lhash(old_fd, client_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
    server_worker.compare_weak_chash(old_fd, client_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Compare Weak Hash time,%f\n", diff_1.count() + diff_server_build.count());

    /* client */
    start = std::chrono::high_resolution_clock::now();
    // client_worker.compare_sha1_lhash(new_fd, server_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);
    client_worker.compare_sha1_chash(new_fd, server_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Calculate Strong Hash time,%f\n", diff_1.count() + diff_client_build.count());

    start = std::chrono::high_resolution_clock::now();
    client_worker.generate_delta(new_fd, client_worker.strong_matched_chunks_queue, client_worker.data_cmd_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Generate Delta time,%f\n", diff_1.count());

    /* server */
    start = std::chrono::high_resolution_clock::now();
    server_worker.patch_delta(old_fd, output_fd, client_worker.data_cmd_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Patch Delta time,%f\n\n", diff_1.count());

    close(old_fd);
    close(new_fd);
    return 0;
}

