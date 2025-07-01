#include <chrono>
#include "skysync_c.h"
#include "skysync_c_worker.h"

off_t file_size(int fd);

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <old_file> <new_file> [hw]\n", argv[0]);
        printf("  hw: 0 for non-hardware accelerated, 1 for hardware accelerated (default is 0)\n");
        exit(1);
    }

    uint8_t whash_algorithm = 0;
    if (argc >= 4) {
        int algo = atoi(argv[3]);
        switch (algo) {
            case 0:
                whash_algorithm = 0;
                printf("Using non-hardware accelerated hashing algorithm\n");
                break;
            case 1:
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

    ClientSkySyncCWorker client_worker(whash_algorithm);
    ServerSkySyncCWorker server_worker(whash_algorithm);

    /* server */
    server_worker.fsc_csums = calc_fsc_hw(old_fd);
    const char *fsc_file = "server_fsc.bin";
    write_fsc(fsc_file, server_worker.fsc_csums);
    free_file_fsc(server_worker.fsc_csums);
    server_worker.fsc_csums = nullptr;
    server_worker.fsc_csums = read_fsc(fsc_file);

    auto start = std::chrono::high_resolution_clock::now();
    server_worker.serial_cdc(old_fd, server_worker.old_csums_queue, server_worker.fsc_csums);
    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Serial CDC time,%f\n", diff_1.count());

    /* client */
    client_worker.fsc_csums = calc_fsc_hw(new_fd);
    const char* fsc_file_client = "client_fsc.bin";
    write_fsc(fsc_file_client, client_worker.fsc_csums);
    free_file_fsc(client_worker.fsc_csums);
    client_worker.fsc_csums = nullptr;
    client_worker.fsc_csums = read_fsc(fsc_file_client);

    start = std::chrono::high_resolution_clock::now();
    client_worker.serial_cdc(new_fd, client_worker.new_csums_queue, client_worker.fsc_csums);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Serial CDC time,%f\n", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    // client_worker.lhash_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue, file_size(new_fd) / 8192);
    client_worker.hash7_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue, file_size(new_fd) / 8192);
    std::chrono::duration<double> diff_client_build = std::chrono::high_resolution_clock::now() - start;
    // printf("Client Build Hash Table time,%f\n", diff_client_build.count());

    /* server */
    start = std::chrono::high_resolution_clock::now();
    // server_worker.lhash_builder(server_worker.old_csums_queue, file_size(old_fd) / 8192);
    server_worker.hash7_builder(server_worker.old_csums_queue, file_size(old_fd) / 8192);
    std::chrono::duration<double> diff_server_build = std::chrono::high_resolution_clock::now() - start;
    // printf("Server Build Hash Table time,%f\n", diff_server_build.count());

    start = std::chrono::high_resolution_clock::now();
    // server_worker.compare_weak_lhash(old_fd, client_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
    server_worker.compare_weak_hash7(old_fd, client_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Compare Weak Hash time,%f\n", diff_1.count() + diff_server_build.count());

    /* client */
    start = std::chrono::high_resolution_clock::now();
    // client_worker.compare_sha1_lhash(new_fd, server_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);
    client_worker.compare_sha1_hash7(new_fd, server_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);
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