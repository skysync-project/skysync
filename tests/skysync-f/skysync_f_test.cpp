#include <chrono>
#include "skysync_c.h"
#include "skysync_f_worker.h"

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

    ClientSkySyncFWorker client_worker(whash_algorithm);
    ServerSkySyncFWorker server_worker(whash_algorithm);

    /* server */
    server_worker.old_csums = calc_fsc_hw(old_fd);
    const char *fsc_file = "server_fsc.bin";
    write_fsc(fsc_file, server_worker.old_csums);
    free_file_fsc(server_worker.old_csums);
    server_worker.old_csums = nullptr;

    auto start = std::chrono::high_resolution_clock::now();
    server_worker.old_csums = read_fsc(fsc_file);
    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Signature generation completed in %f seconds\n", diff_1.count());

    /* client */
    client_worker.new_csums = calc_fsc_hw(new_fd);
    const char* fsc_file_client = "client_fsc.bin";
    write_fsc(fsc_file_client, client_worker.new_csums);
    free_file_fsc(client_worker.new_csums);
    client_worker.new_csums = nullptr;

    start = std::chrono::high_resolution_clock::now();
    client_worker.new_csums = read_fsc(fsc_file_client);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    // printf("Client FSC time,%f\n", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    // client_worker.chash_builder(server_worker.old_csums);
    client_worker.hash7_builder(server_worker.old_csums);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    // printf("Client Build Hash Table time,%f\n", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    client_worker.rolling_fsc(new_fd, server_worker.old_csums, client_worker.new_csums, client_worker.data_cmd_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Client Rolling and Delta generation completed in %f seconds\n", diff_1.count());

    /* server */
    start = std::chrono::high_resolution_clock::now();
    server_worker.patch_delta(old_fd, output_fd, client_worker.data_cmd_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Server Patch delta applied in %f seconds\n", diff_1.count());

    close(old_fd);
    close(new_fd);
    return 0;
}

