#include <chrono>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <libgen.h>
#include <string>
#include <cctype>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <limits>
#include <memory>
#include "rsync_http.h"

#define BUFFER_SIZE (4 * 1024 * 1024)

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <old_file> <new_file> [hw]\n", argv[0]);
        printf("  hw: 0 for non-hardware accelerated, 1 for hardware accelerated (default is 0)\n");
        exit(1);
    }

    int hw = 0; // Default to non-hardware accelerated
    if (argc >= 4) {
        int algo = atoi(argv[3]);
        switch (algo) {
            case 0:
                printf("Using non-hardware accelerated hashing algorithm\n");
                break;
            case 1:
                hw = 1; // Use hardware accelerated hashing
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

    /* server */
    char* mapped_file_content = (char*)map_file(old_fd);
    size_t file_s = file_size(old_fd);
    char* sig_data = nullptr;
    size_t sig_len = 0;

    auto start1 = std::chrono::high_resolution_clock::now();
    rs_result ret = rsyncx_signature_mem((char*)mapped_file_content, file_s, &sig_data, &sig_len, hw);
    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start1;
    printf("Server Signature generation completed in %f seconds\n", diff_1.count());

    /* client */
    char* new_file_content = (char*)map_file(new_fd);
    size_t new_file_len = file_size(new_fd);
    char* delta_data = nullptr;
    size_t delta_len = 0;

    auto start2 = std::chrono::high_resolution_clock::now();
    rs_result rsync_ret = rsyncx_delta_mem(sig_data, sig_len, new_file_content, new_file_len, &delta_data, &delta_len);
    std::chrono::duration<double> diff_2 = std::chrono::high_resolution_clock::now() - start2;
    printf("Client Rolling and Delta generation completed in %f seconds\n", diff_2.count());

    /* server */
    char* new_file_data = nullptr;
    size_t reconstructed_len = 0;
    auto start3 = std::chrono::high_resolution_clock::now();
    rs_result patch_ret = rsyncx_patch_mem((char*)mapped_file_content, file_s,
                                        delta_data, delta_len,
                                        &new_file_data, &reconstructed_len);
    // Write the new file data to the output file
    size_t remaining = reconstructed_len;
    char* write_ptr = new_file_data;
    while (remaining > 0) {
        size_t chunk_size = (remaining < BUFFER_SIZE) ? remaining : BUFFER_SIZE;
        ssize_t bytes_written = write(output_fd, write_ptr, chunk_size);
        if (bytes_written <= 0) break;
        write_ptr += bytes_written;
        remaining -= bytes_written;
    }
    std::chrono::duration<double> diff_3 = std::chrono::high_resolution_clock::now() - start3;
    printf("Server Patch delta applied in %f seconds\n", diff_3.count());

    close(old_fd);
    close(new_fd);
    close(output_fd);
    return 0;
}