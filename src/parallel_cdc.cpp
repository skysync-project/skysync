#include <assert.h>
#include <isa-l/crc.h>
#include "parallel_cdc.h"

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

struct file_cdc* parallel_run_cdc(char *file_path, uint32_t thread_num,
                                std::vector<std::thread> &threads,
                                struct stats *st, int whichone) {

    int fd = open(file_path, O_RDONLY);
    if (fd == -1) {
        printf("open file %s failed\n", file_path);
        exit(1);
    }

    std::unique_ptr<DataQueue<one_cdc>> csums_queue(new DataQueue<one_cdc>());
    std::unique_ptr<thread_args[]> targs((thread_args*)mi_zalloc(sizeof(thread_args) * thread_num));
    std::unique_ptr<csegment*[]> seg_array((csegment**)mi_zalloc(sizeof(csegment*) * thread_num));

    char *map = (char *)map_file(fd);

    fastCDC_init();

    st->cdc_stage_1 = 0;
    st->cdc_stage_2 = 0;

    uint64_t fs = file_size(fd);
    uint64_t seg_size = 0;
    uint64_t last_seg_size = 0;

    if (thread_num == 1) {
        seg_size = fs;
        last_seg_size = fs;
    }
    else if (thread_num > 1) {
        seg_size = fs / thread_num;
        /* the last thread may read more data */
        last_seg_size = seg_size + fs % thread_num;
    }
    else {
        printf("thread_num should be greater than 0\n");
        close(fd);
        exit(1);
    }

    for (uint32_t i = 0; i < thread_num; i++) {
        targs[i].offset = i * seg_size;
        targs[i].length = (i == thread_num - 1) ? last_seg_size : seg_size;
    }

    struct bitmap* bm = bitmap_create(fs);

    auto start = std::chrono::high_resolution_clock::now();

    /*=========================== std::thread ===========================*/
    cpu_set_t cpuset;
    for (uint32_t i = 0; i < thread_num; i++) {
        CPU_ZERO(&cpuset);
        CPU_SET(adj_core_set[i], &cpuset);
        // CPU_SET(cross_core_set[i], &cpuset);
        // CPU_SET(cross_cpu_set[i], &cpuset);

        if (whichone == 1) {
            threads[i] = std::thread([&targs, i, map, bm]() {
                PDSyncChunker *c1 = new PDSyncChunker();
                c1->init(map, targs[i].offset, targs[i].length);
                csegment* tmp_cseg = c1->task(bm);
                targs[i].seg.store(tmp_cseg);
                delete c1;
            });
        }
        else if (whichone == 2) {
            threads[i] = std::thread([&targs, i, map]() {
                ParaSyncChunker *ccrc = new ParaSyncChunker();
                ccrc->init(map, targs[i].offset, targs[i].length);
                csegment* tmp_cseg = ccrc->task();
                targs[i].seg.store(tmp_cseg);
                delete ccrc;
            });
        }
        else {
            printf("Invalid chunker function\n");
            exit(1);
        }
        int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            printf("Error calling pthread_setaffinity_np: %d\n", rc);
        }
    }

    for (auto &t : threads) {
        t.join();
    }

    for (int i = 0; i < thread_num; i++) {
        seg_array[i] = targs[i].seg.load();
    }

    /*=========================== std::thread ===========================*/

    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
    printf("Parallel CDC Stage 1,%f\n", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    if (whichone == 1) {
        pdsync_detector(seg_array, (char*) map, fs, bm, csums_queue);
    }
    else if (whichone == 2) {
        parasync_detector(seg_array, (char*) map, thread_num, fs, csums_queue);
    }
    
    std::chrono::duration<double> diff_2 = std::chrono::high_resolution_clock::now() - start;
    printf("Serial CDC Stage 2,%f\n\n", diff_2.count());

    for(int i = 0; i < thread_num; i++) {
        if (seg_array[i])
            mi_free(seg_array[i]->map);
    }

    bitmap_destroy(bm);
    unmap_file(fd, map);
    close(fd);
    return nullptr;
}