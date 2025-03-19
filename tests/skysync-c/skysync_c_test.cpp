#include <time.h>
#include "skysync_c.h"

void *map_file(int fd);
void unmap_file(int fd, void *map);

int main(int argc, char** argv) {
    if (argc != 3) {
        printf("Usage: %s <old_file> <new_file> \n", argv[0]);
        exit(1);
    }

    printf("SkySync-C test: \n");

    clock_t server_total_time, client_total_time;
    clock_t start, end;

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

    /* server */
    char* old_file_map = (char *)map_file(old_fd);
    start = clock();
    struct file_cdc *old_fc = serial_cdc_1(old_fd, old_file_map);
    end = clock();
    unmap_file(old_fd, old_file_map);

    printf("server: generate CRC32C time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    server_total_time = end - start;

    /* client */
    char* new_file_map = (char *)map_file(new_fd);
    start = clock();
    struct file_cdc *new_fc = serial_cdc_1(new_fd, new_file_map);
    end = clock();
    unmap_file(new_fd, new_file_map);

    printf("client: generate CRC32C time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time = end - start;

    /* server */
    start = clock();
    struct cdc_matched_chunks *mc = compare_weak_hash(old_fd, old_fc, new_fc);
    end = clock();

    printf("server: compare CRC32C and generate SHA1 time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    server_total_time += end - start;

    mi_free(old_fc->cdc_array); mi_free(old_fc);
    mi_free(new_fc->cdc_array); mi_free(new_fc);

    /* client */
    start = clock();
    cdc_compare_sha1(mc, new_fd);
    end = clock();
    printf("client: generate and compare SHA1 time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time += end - start;

    /* generate delta file */
    char delta_file_path[strlen(argv[2]) + 20];
    strcpy(delta_file_path, argv[2]);
    strcat(delta_file_path, "_delta_skysync_c");

    start = clock();
    cdc_generate_delta(mc, new_fd, delta_file_path);
    end = clock();
    printf("client: generate delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time += end - start;

    /* server */
    char output_file[strlen(argv[1]) + 20];
    strcpy(output_file, argv[1]);
    strcat(output_file, "_patch_skysync_c");

    start = clock();
    fastcdc_patch_delta(argv[1], delta_file_path, output_file);
    end = clock();
    printf("server: patch delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    server_total_time += end - start;

    printf("client total time: %f\n", (double)client_total_time / CLOCKS_PER_SEC);
    printf("server total time: %f\n", (double)server_total_time / CLOCKS_PER_SEC);

    close(old_fd);
    close(new_fd);

    return 0;
}

