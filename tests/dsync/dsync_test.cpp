#include <time.h>
#include "fastcdc.h"

void *map_file(int fd);
void unmap_file(int fd, void *map);

int main(int argc, char** argv) {
    if (argc < 4) {
        printf("Usage: %s <old_file> <new_file> <chunking method>: \n"
            "\t 1: origin fastcdc \n"
            "\t 2: rolling 2 bytes \n"
            "\t 3: normalized fastcdc \n"
            "\t 4: normalized 2 bytes\n", argv[0]);
        exit(1);
    }

    printf("DSync test: \n");

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
    struct file_fastcdc *old_ff = run_fastfp_1(old_fd, old_file_map, atoi(argv[3]));
    end = clock();
    unmap_file(old_fd, old_file_map);
    printf("server: generate fastfp time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    server_total_time = end - start;

    /* client */
    char* new_file_map = (char *)map_file(new_fd);
    start = clock();
    struct file_fastcdc *new_ff = run_fastfp_1(new_fd, new_file_map, atoi(argv[3]));
    end = clock();
    unmap_file(new_fd, new_file_map);
    printf("client: generate fastfp time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time = end - start;
    /* post fastfp file to server */

    /* server */
    start = clock();
    /* compare two files' fastfp */
    struct matched_chunks *mc_w = compare_fastfp(old_fd, old_ff, new_ff);
    end = clock();
    printf("server: compare fastfp and generate SHA1 time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    server_total_time += end - start;
    /* post matched file to client */

    free(old_ff->fastcdc_array);free(old_ff);
    free(new_ff->fastcdc_array);free(new_ff);

    /* client */
    start = clock();
    /* compare two matched chunks' sha1 */
    compare_sha1(mc_w, new_fd);
    end = clock();
    printf("client: generate and compare sha1 time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time += end - start;

    /* generate delta file */
    char delta_file_path[strlen(argv[2]) + 20];
    strcpy(delta_file_path, argv[2]);
    strcat(delta_file_path, "_delta_dsync");

    start = clock();
    generate_delta(mc_w, new_fd, delta_file_path);
    end = clock();
    printf("client: generate delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    client_total_time += end - start;

    /* post delta file to server */
    /* server */
    char output_file[strlen(argv[1]) + 20];
    strcpy(output_file, argv[1]);
    strcat(output_file, "_patch_dsync");

    start = clock();
    fastcdc_patch_delta(argv[1], delta_file_path, output_file);
    end = clock();
    printf("server: patch delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    server_total_time += end - start;

    printf("client: total time: %f\n", (double)(client_total_time) / CLOCKS_PER_SEC);
    printf("server: total time: %f\n", (double)(server_total_time) / CLOCKS_PER_SEC);
    
    close(old_fd);
    close(new_fd);

    return 0;
}
