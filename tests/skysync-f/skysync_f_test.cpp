#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <memory>
#include "skysync_f.h"

void *map_file(int fd);
void unmap_file(int fd, void *map);

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <old_file> <new_file>\n", argv[0]);
        exit(1);
    }

    clock_t start, end;
    clock_t client_total_time = 0;
    clock_t server_total_time = 0;

    /* Legacy C-style implementation */
    printf("=== Testing Legacy C-style Implementation ===\n");
    
    /* client read old file and calculate checksums */
    int old_fd = open(argv[1], O_RDONLY);
    if (old_fd == -1) {
        perror("open old file");
        exit(1);
    }

    void *old_file_map = map_file(old_fd);

    start = clock();
    struct crr_csums* old_csums = crr_calc_csums_1(old_fd, (char *)old_file_map);
    end = clock();
    printf("client: calculate old file checksums time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time += end - start;

    /* client read new file and calculate checksums */
    int new_fd = open(argv[2], O_RDONLY);
    if (new_fd == -1) {
        perror("open new file");
        exit(1);
    }

    void *new_file_map = map_file(new_fd);

    start = clock();
    struct crr_csums* new_csums = crr_calc_csums_1(new_fd, (char *)new_file_map);
    end = clock();
    printf("client: calculate new file checksums time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time += end - start;

    /* client compare checksums and generate delta */
    char delta_file[strlen(argv[2]) + 16];
    strcpy(delta_file, argv[2]);
    strcat(delta_file, "_delta_skysync_f");

    start = clock();
    crr_compare_csums_1(new_fd, delta_file, old_csums, new_csums);
    end = clock();
    printf("client: compare checksums time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time += end - start;

    /* server read delta file and patch it */
    char out_file[strlen(argv[2]) + 18];
    strcpy(out_file, argv[2]);
    strcat(out_file, "_patch_skysync_f");

    start = clock();
    crr_patch_delta(argv[1], delta_file, out_file);
    end = clock();
    
    printf("server: patch delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    server_total_time = end - start;

    printf("client total time: %f\n", (double)client_total_time / CLOCKS_PER_SEC);
    printf("server total time: %f\n", (double)server_total_time / CLOCKS_PER_SEC);

    /* Modern C++ class-based implementation */
    printf("\n=== Testing Modern C++ Class-based Implementation ===\n");
    
    // // Reset timers
    // client_total_time = 0;
    // server_total_time = 0;
    
    // // Create client and server instances
    // SkySyncClient client;
    // SkySyncServer server;
    
    // // Client operations
    // start = clock();
    // std::shared_ptr<crr_csums> old_csums_cpp = client.calculateChecksums(old_fd, (char *)old_file_map);
    // end = clock();
    // printf("client: calculate old file checksums time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    // client_total_time += end - start;
    
    // start = clock();
    // std::shared_ptr<crr_csums> new_csums_cpp = client.calculateChecksums(new_fd, (char *)new_file_map);
    // end = clock();
    // printf("client: calculate new file checksums time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    // client_total_time += end - start;
    
    // char delta_file_cpp[strlen(argv[2]) + 20];
    // strcpy(delta_file_cpp, argv[2]);
    // strcat(delta_file_cpp, "_delta_skysync_f_cpp");
    
    // start = clock();
    // client.generateDelta(new_fd, delta_file_cpp, old_csums_cpp, new_csums_cpp);
    // end = clock();
    // printf("client: generate delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    // client_total_time += end - start;
    
    // // Server operations
    // char out_file_cpp[strlen(argv[2]) + 22];
    // strcpy(out_file_cpp, argv[2]);
    // strcat(out_file_cpp, "_patch_skysync_f_cpp");
    
    // start = clock();
    // server.patchDelta(argv[1], delta_file_cpp, out_file_cpp);
    // end = clock();
    // printf("server: patch delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    // server_total_time = end - start;
    
    // printf("client total time (C++): %f\n", (double)client_total_time / CLOCKS_PER_SEC);
    // printf("server total time (C++): %f\n", (double)server_total_time / CLOCKS_PER_SEC);
    
    // // Clean up
    // close(old_fd);
    // close(new_fd);
    // unmap_file(old_fd, old_file_map);
    // unmap_file(new_fd, new_file_map);
    
    return 0;
}
