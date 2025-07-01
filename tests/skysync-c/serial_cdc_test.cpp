#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "skysync_c_worker.h"
#include "fsc_hash.h"
#include "crc32.h"
#include "sync_common.h"
#include "dsync.h"


off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

// Reference implementation of serial_cdc
void serial_cdc_simple(int fd, DataQueue<one_cdc>& csums_queue) {
    uint64_t fs = file_size(fd);
    char *map = (char *)map_file(fd);
    uint64_t offset = 0;

    for (;;) {
        uint64_t chunk_length = find_cutpoint_2(map + offset, fs - offset);

        struct one_cdc cdc = {
            .offset = offset,
            .length = chunk_length,
            .weak_hash = crc32_fast(map + offset, chunk_length, 0)
        };

        csums_queue.push(cdc);
        offset += chunk_length;

        if (offset >= fs) {
            break;
        }

        if (offset + MinSize > fs) {
            struct one_cdc last_cdc = {
                .offset = offset,
                .length = fs - offset,
                .weak_hash = crc32_fast(map + offset, fs - offset, 0)
            };
            csums_queue.push(last_cdc);
            break;
        }
    }

    unmap_file(fd, map);
    csums_queue.setDone();
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <test_file>" << std::endl;
        return 1;
    }

    const char *test_file = argv[1];
    int fd = open(test_file, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open test file");
        return 1;
    }

    // FSC-Optimized Run
    DataQueue<one_cdc> fsc_queue;
    fsc_queue.init();
    ServerSkySyncCWorker fsc_worker(0); // 0 for SW implementation
    file_fsc* fsc_data = calc_fsc_hw(fd);
    fsc_worker.serial_cdc(fd, fsc_queue, fsc_data);

    // Simple Run
    DataQueue<one_cdc> simple_queue;
    simple_queue.init();
    serial_cdc_simple(fd, simple_queue);

    close(fd);

    // Verify the results
    std::vector<one_cdc> fsc_results;
    while (!fsc_queue.isDone()) {
        fsc_results.push_back(fsc_queue.pop());
    }

    std::vector<one_cdc> simple_results;
    while (!simple_queue.isDone()) {
        simple_results.push_back(simple_queue.pop());
    }

    assert(fsc_results.size() == simple_results.size());

    for (size_t i = 0; i < fsc_results.size(); ++i) {
        assert(fsc_results[i].offset == simple_results[i].offset);
        assert(fsc_results[i].length == simple_results[i].length);
        printf("FSC offset: %lu, Simple offset: %lu\n",
               fsc_results[i].offset, simple_results[i].offset);
        printf("FSC weak hash: %u, Simple weak hash: %u\n",
               fsc_results[i].weak_hash, simple_results[i].weak_hash);
        if (fsc_results[i].weak_hash != simple_results[i].weak_hash) {
            std::cerr << "Mismatch at index " << i << ": "
                      << "FSC weak hash = " << fsc_results[i].weak_hash
                      << ", Simple weak hash = " << simple_results[i].weak_hash
                      << std::endl;
            return 1;
        }
    }

    free(fsc_data->fsc_array);
    free(fsc_data);


    return 0;
}