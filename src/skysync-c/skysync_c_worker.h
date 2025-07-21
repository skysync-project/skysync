#ifndef SKYSYNC_C_H_1
#define SKYSYNC_C_H_1

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <zlib.h>
#include <openssl/md5.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/sha.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <libgen.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <mimalloc-2.1/mimalloc.h>
#include <string>
#include <queue>
#include <atomic>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <cctype>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <limits>
#include <memory>

#include "fastcdc.h"
#include "skysync_c.h"
#include "sync_common.h"
#include "blake3.h"
#include "crc32/crc32.h"
#include "crc32/crc32c.h"
#include "dsync.h"
#include "fsc_hash.h"
#include "hash7.hpp"
#include "skysync_f_worker.h"

typedef std::function<void(int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc)> skysync_c_whash_func_ptr;

class SkySyncCWorker {
public:
    LinkedHashTable *lhash_table;
    skysync_c_whash_func_ptr serial_cdc;
    emhash7::HashMap<uint32_t, std::vector<ol>> *strong_hash7_table;

    SkySyncCWorker();
    ~SkySyncCWorker();

    void serial_cdc_sw(int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc);
    void serial_cdc_hw(int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc);

private:
    // Helper functions for serial_cdc_sw
    void process_chunk_with_fsc(char *map, uint64_t offset, uint64_t chunk_length,
                               file_fsc *fsc, uint64_t fsc_index,
                               uint64_t mid_window_size, uint32_t &cached_crc32,
                               struct one_cdc &cdc, uint64_t file_size);
    
    void handle_exact_offset_match(char *map, uint64_t offset, uint64_t chunk_length,
                                  file_fsc *fsc, uint64_t fsc_index,
                                  uint32_t &cached_crc32, struct one_cdc &cdc, uint64_t file_size);
    
    void handle_first_half_window(char *map, uint64_t offset, uint64_t chunk_length,
                                 file_fsc *fsc, uint64_t fsc_index,
                                 uint64_t mid_window_size, uint32_t &cached_crc32,
                                 struct one_cdc &cdc, uint64_t file_size);
    
    void handle_second_half_window(char *map, uint64_t offset, uint64_t chunk_length,
                                  file_fsc *fsc, uint64_t fsc_index,
                                  uint64_t mid_window_size, uint32_t &cached_crc32,
                                  struct one_cdc &cdc, uint64_t file_size);
   void calculate_weak_hash_for_extended_chunk(char *map, uint64_t offset, uint64_t chunk_length,
                                             file_fsc *fsc, uint64_t fsc_index,
                                             uint64_t mid_window_size, uint32_t &cached_crc32,
                                             struct one_cdc &cdc, uint64_t file_size);
   void calculate_weak_hash_for_second_half(char *map, uint64_t offset, uint64_t chunk_length,
                                          file_fsc *fsc, uint64_t fsc_index,
                                          uint64_t mid_window_size, uint32_t &cached_crc32,
                                          struct one_cdc &cdc, uint64_t next_fsc_offset,
                                          uint64_t chunk_end, uint64_t file_size);
};

class ClientSkySyncCWorker : public SkySyncCWorker {
public:
    DataQueue<one_cdc> new_csums_queue;
    DataQueue<uint32_t> new_crc32_queue;

    file_fsc *fsc_csums;
    
    DataQueue<matched_item_rpc> weak_matched_chunks_queue;
    DataQueue<matched_item_rpc> strong_matched_chunks_queue;
    DataQueue<data_cmd> data_cmd_queue;

    DataQueue<matched_item_rpc_1> weak_matched_chunks_queue_1;
    
    ClientSkySyncCWorker(uint8_t whashing = 0);
    ~ClientSkySyncCWorker();

    using SkySyncCWorker::serial_cdc;

    void lhash_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue, uint64_t chunk_nums);

    void hash7_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue, uint64_t chunk_nums);
    
    void compare_sha1_lhash(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    void compare_sha1_hash7(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue);

    data_cmd create_data_cmd(int fd, int cmd_flag, uint64_t offset, uint64_t length);

    void sort_matched_chunks(DataQueue<matched_item_rpc> &matched_chunks_queue);

    void generate_delta(int new_fd, DataQueue<matched_item_rpc> &strong_matched_chunks_queue, DataQueue<data_cmd> &data_cmd_queue);
    
};

class ServerSkySyncCWorker : public SkySyncCWorker {
public:
    DataQueue<one_cdc> old_csums_queue;
    DataQueue<one_cdc> new_csums_queue;
    DataQueue<uint32_t> new_crc32_queue;

    file_fsc *fsc_csums;

    DataQueue<matched_item_rpc> weak_matched_chunks_queue;
    DataQueue<matched_item_rpc_1> weak_matched_chunks_queue_1;

    DataQueue<data_cmd> data_cmd_queue;

    ServerSkySyncCWorker(uint8_t whashing = 0);
    ~ServerSkySyncCWorker();

    using SkySyncCWorker::serial_cdc;

    void lhash_builder(DataQueue<one_cdc> &csums_queue, uint64_t chunk_nums);

    void hash7_builder(DataQueue<one_cdc> &csums_queue, uint64_t chunk_nums);

    void compare_weak_lhash(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue);

    void compare_weak_hash7(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue);

    void patch_delta(int old_fd, int out_fd, DataQueue<data_cmd> &data_cmd_queue);
};

#endif // SKYSYNC_C_H_1