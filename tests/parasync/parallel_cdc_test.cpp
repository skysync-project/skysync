#include <chrono>
#include "skysync_c.h"
#include "parallel_cdc.h"

int main(int argc, char** argv) {
    if (argc < 5) {
        printf("Usage: %s <old_file> <new_file> <thread> <whichone>\n", argv[0]);
        exit(1);
    }

    int thread_num = atoi(argv[3]);
    int whichone = atoi(argv[4]);

    struct stats stats = {
        .read_io_bytes = 0,
        .write_io_bytes = 0,
        .cdc_stage_1 = 0,
        .cdc_stage_2 = 0,
    };

    std::vector<std::thread> threads;
    threads.resize(thread_num);

    printf("file: %s\n", argv[1]);
    struct file_cdc *old_fc = parallel_run_cdc(argv[1], thread_num, threads, &stats, whichone);
    printf("file: %s\n", argv[2]);
    struct file_cdc *new_fc = parallel_run_cdc(argv[2], thread_num, threads, &stats, whichone);

    // printf("Parallel CDC Stage 1: %f\n", (double)stats.cdc_stage_1 / CLOCKS_PER_SEC);
    // printf("Serial CDC Stage 2: %f\n", (double)stats.cdc_stage_2 / CLOCKS_PER_SEC);

    if (old_fc != NULL) {
        mi_free(old_fc->cdc_array);
        mi_free(old_fc);
    }
    if (new_fc != NULL) {
        mi_free(new_fc->cdc_array);
        mi_free(new_fc);
    }
    return 0;
}