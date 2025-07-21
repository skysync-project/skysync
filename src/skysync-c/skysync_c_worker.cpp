#include <assert.h>
#include <isa-l_crypto/rolling_hashx.h>
#include "dsync.h"
#include "blake3.h"
#include "crc32c.h"
#include "crc32/crc32.h"
#include "skysync_c_worker.h"

off_t file_size(int fd);
int compare_offset(const void *a, const void *b);
void *map_file(int fd);
void unmap_file(int fd, void *map);

SkySyncCWorker::SkySyncCWorker() {}

SkySyncCWorker::~SkySyncCWorker() {}

ClientSkySyncCWorker::ClientSkySyncCWorker(uint8_t whashing) {
    this->new_csums_queue.init();
    this->weak_matched_chunks_queue.init();
    this->strong_matched_chunks_queue.init();
    this->data_cmd_queue.init();

    switch (whashing) {
        case 0:
            this->serial_cdc = [this](int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {
                this->serial_cdc_sw(fd, csums_queue, fsc);
            };
            break;
        case 1:
            this->serial_cdc = [this](int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {
                this->serial_cdc_hw(fd, csums_queue, fsc);
            };
            break;
        default:
            this->serial_cdc = [this](int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {
                this->serial_cdc_sw(fd, csums_queue, fsc);
            };
            break;
    }
}

ClientSkySyncCWorker::~ClientSkySyncCWorker() {}

ServerSkySyncCWorker::ServerSkySyncCWorker(uint8_t whashing) {
    this->old_csums_queue.init();
    this->new_csums_queue.init();
    this->weak_matched_chunks_queue.init();
    this->data_cmd_queue.init();
  
    switch (whashing) {
        case 0:
            this->serial_cdc = [this](int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {
                this->serial_cdc_sw(fd, csums_queue, fsc);
            };
            break;
        case 1:
            this->serial_cdc = [this](int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {
                this->serial_cdc_hw(fd, csums_queue, fsc);
            };
            break;
        default:
            this->serial_cdc = [this](int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {
                this->serial_cdc_sw(fd, csums_queue, fsc);
            };
            break;
    }
}

ServerSkySyncCWorker::~ServerSkySyncCWorker() {}

void SkySyncCWorker::serial_cdc_sw(int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {

    uint64_t fs = file_size(fd);

    char *map = (char *)map_file(fd);
    uint64_t offset = 0;
    uint32_t cached_crc32 = 0;  // Renamed for clarity
    const uint64_t mid_window_size = DefaultWindowSize / 2;

    for (;;) {
        uint64_t chunk_length = find_cutpoint_2(map + offset, fs - offset);

        struct one_cdc cdc = {
            .offset = offset,
            .length = chunk_length,
            .weak_hash = 0
        };

        // Calculate FSC index with bounds checking
        uint64_t fsc_index = offset / DefaultWindowSize;
        if (fsc_index >= fsc->chunk_num) {
            // Fallback to direct CRC calculation if FSC index is out of bounds
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
        } else {
            // Process chunk using FSC optimization
            process_chunk_with_fsc(map, offset, chunk_length, fsc, fsc_index,
                                 mid_window_size, cached_crc32, cdc, fs);
        }

        offset += chunk_length;
        csums_queue.push(cdc);

        if (offset >= fs)
            break;

        // Handle remaining small chunk at end of file
        if (offset + MinSize > fs) {
            struct one_cdc last_cdc = {
                .offset = offset,
                .length = fs - offset,
                .weak_hash = crc32_isal(map + offset, fs - offset, 0)
            };
            csums_queue.push(last_cdc);
            break;
        }
    }

    unmap_file(fd, map);
    csums_queue.setDone();
}

/**
 * Helper function to process a chunk using FSC (Fixed-Size Chunking) optimization
 * This function handles the complex logic of calculating weak hashes using pre-computed
 * FSC values and CRC32 operations for content-defined chunking.
 */
void SkySyncCWorker::process_chunk_with_fsc(char *map, uint64_t offset, uint64_t chunk_length,
                                           file_fsc *fsc, uint64_t fsc_index,
                                           uint64_t mid_window_size, uint32_t &cached_crc32,
                                           struct one_cdc &cdc, uint64_t file_size) {
    const uint64_t fsc_offset = fsc->fsc_array[fsc_index].offset;
    const uint64_t mid_fsc_offset = fsc_offset + mid_window_size;
    
    // Case 1: Offset exactly matches FSC entry
    if (fsc_offset == offset) {
        handle_exact_offset_match(map, offset, chunk_length, fsc, fsc_index, cached_crc32, cdc, file_size);
    }
    // Case 2: Offset is within first half of FSC window
    else if (offset > fsc_offset && offset < mid_fsc_offset) {
        handle_first_half_window(map, offset, chunk_length, fsc, fsc_index,
                                mid_window_size, cached_crc32, cdc, file_size);
    }
    // Case 3: Offset is in second half or beyond FSC window
    else if (offset >= mid_fsc_offset) {
        handle_second_half_window(map, offset, chunk_length, fsc, fsc_index,
                                 mid_window_size, cached_crc32, cdc, file_size);
    }
    // Case 4: Fallback - calculate CRC directly
    else {
        cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
    }
}

/**
 * Handle case where chunk offset exactly matches FSC entry offset
 */
void SkySyncCWorker::handle_exact_offset_match(char *map, uint64_t offset, uint64_t chunk_length,
                                              file_fsc *fsc, uint64_t fsc_index,
                                              uint32_t &cached_crc32, struct one_cdc &cdc, uint64_t file_size) {
    if (DefaultWindowSize == chunk_length) {
        // Direct match - use pre-computed hash
        cdc.weak_hash = fsc->fsc_array[fsc_index].weak_hash;
    } else if (DefaultWindowSize > chunk_length) {
        // FSC window is larger than chunk - remove trailing bytes
        uint64_t gap = DefaultWindowSize - chunk_length;
        uint64_t read_end = offset + chunk_length + gap;
        
        // BOUNDS CHECK: Ensure we don't read beyond file size
        if (read_end > file_size) {
            // fprintf(stderr, "BOUNDS VIOLATION in handle_exact_offset_match (line 167):\n");
            // fprintf(stderr, "  offset=%lu, chunk_length=%lu, gap=%lu, read_end=%lu, file_size=%lu\n",
            //         offset, chunk_length, gap, read_end, file_size);
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
            return;
        }
        
        cached_crc32 = crc32_isal(map + offset + chunk_length, gap, 0);
        cdc.weak_hash = crc32_remove0(fsc->fsc_array[fsc_index].weak_hash ^ cached_crc32, gap);
    } else {
        // Chunk is larger than FSC window - combine with additional bytes
        uint64_t gap = chunk_length - DefaultWindowSize;
        uint64_t read_end = offset + DefaultWindowSize + gap;
        
        // BOUNDS CHECK: Ensure we don't read beyond file size
        if (read_end > file_size) {
            // fprintf(stderr, "BOUNDS VIOLATION in handle_exact_offset_match (line 172):\n");
            // fprintf(stderr, "  offset=%lu, chunk_length=%lu, gap=%lu, read_end=%lu, file_size=%lu\n",
            //         offset, chunk_length, gap, read_end, file_size);
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
            return;
        }
        
        cached_crc32 = crc32_isal(map + offset + DefaultWindowSize, gap, 0);
        cdc.weak_hash = crc32_comb(fsc->fsc_array[fsc_index].weak_hash, cached_crc32, gap);
    }
}

/**
 * Handle case where chunk offset is in first half of FSC window
 */
void SkySyncCWorker::handle_first_half_window(char *map, uint64_t offset, uint64_t chunk_length,
                                             file_fsc *fsc, uint64_t fsc_index,
                                             uint64_t mid_window_size, uint32_t &cached_crc32,
                                             struct one_cdc &cdc, uint64_t file_size) {
    const uint64_t fsc_offset = fsc->fsc_array[fsc_index].offset;
    uint64_t gap = offset - fsc_offset;
    uint32_t tmp_crc32 = crc32_add0(cached_crc32, DefaultWindowSize - gap);
    cdc.weak_hash = fsc->fsc_array[fsc_index].weak_hash ^ tmp_crc32;

    if ((fsc_offset + DefaultWindowSize) == (offset + chunk_length)) {
        cached_crc32 = tmp_crc32;
    } else if ((fsc_offset + DefaultWindowSize) > (offset + chunk_length)) {
        gap = fsc_offset + DefaultWindowSize - (offset + chunk_length);
        
        // BOUNDS CHECK: Ensure we don't read beyond file size
        uint64_t read_end = offset + chunk_length + gap;
        if (read_end > file_size) {
            // Log the bounds violation for diagnosis
            // fprintf(stderr, "BOUNDS VIOLATION DETECTED at line 193:\n");
            // fprintf(stderr, "  offset=%lu, chunk_length=%lu, gap=%lu\n", offset, chunk_length, gap);
            // fprintf(stderr, "  fsc_offset=%lu, DefaultWindowSize=%lu\n", fsc_offset, DefaultWindowSize);
            // fprintf(stderr, "  read_end=%lu, file_size=%lu\n", read_end, file_size);
            // fprintf(stderr, "  Attempting to read %lu bytes beyond file end\n", read_end - file_size);
            
            // Safe fallback: calculate CRC directly for the chunk
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
            return;
        }
        
        cached_crc32 = crc32_isal(map + offset + chunk_length, gap, 0);
        cdc.weak_hash = (cdc.weak_hash ^ cached_crc32);
        cdc.weak_hash = crc32_remove0(cdc.weak_hash, gap);
    } else {
        if (fsc_index + 1 < fsc->chunk_num) {
            calculate_weak_hash_for_extended_chunk(map, offset, chunk_length, fsc, fsc_index,
                                                   mid_window_size, cached_crc32, cdc, file_size);
        } else {
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
        }
    }
}

/**
 * Handle case where chunk offset is in second half or beyond FSC window
 */
void SkySyncCWorker::handle_second_half_window(char *map, uint64_t offset, uint64_t chunk_length,
                                             file_fsc *fsc, uint64_t fsc_index,
                                             uint64_t mid_window_size, uint32_t &cached_crc32,
                                             struct one_cdc &cdc, uint64_t file_size) {
    if (fsc_index + 1 >= fsc->chunk_num) {
        cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
        return;
    }

    const uint64_t next_fsc_offset = fsc->fsc_array[fsc_index + 1].offset;
    const uint64_t chunk_end = offset + chunk_length;

    calculate_weak_hash_for_second_half(map, offset, chunk_length, fsc, fsc_index,
                                        mid_window_size, cached_crc32, cdc,
                                        next_fsc_offset, chunk_end, file_size);
}

void SkySyncCWorker::calculate_weak_hash_for_extended_chunk(char *map, uint64_t offset, uint64_t chunk_length,
                                                           file_fsc *fsc, uint64_t fsc_index,
                                                           uint64_t mid_window_size, uint32_t &cached_crc32,
                                                           struct one_cdc &cdc, uint64_t file_size) {
    const uint64_t chunk_end = offset + chunk_length;
    const uint64_t next_fsc_offset = fsc->fsc_array[fsc_index + 1].offset;
    uint64_t gap = chunk_end - next_fsc_offset;

    if (chunk_end < next_fsc_offset + mid_window_size) {
        // BOUNDS CHECK: Ensure we don't read beyond file size
        uint64_t read_end = next_fsc_offset + gap;
        if (read_end > file_size) {
            // fprintf(stderr, "BOUNDS VIOLATION in calculate_weak_hash_for_extended_chunk (line 273):\n");
            // fprintf(stderr, "  next_fsc_offset=%lu, gap=%lu, read_end=%lu, file_size=%lu\n",
            //         next_fsc_offset, gap, read_end, file_size);
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
            return;
        }
        
        cached_crc32 = crc32_isal(map + next_fsc_offset, gap, 0);
        cdc.weak_hash = crc32_comb(cdc.weak_hash, cached_crc32, gap);
    } else {
        cdc.weak_hash = crc32_comb(cdc.weak_hash, fsc->fsc_array[fsc_index + 1].weak_hash, DefaultWindowSize);
        gap = next_fsc_offset + DefaultWindowSize - chunk_end;
        
        // BOUNDS CHECK: Ensure we don't read beyond file size
        uint64_t read_end = chunk_end + gap;
        if (read_end > file_size) {
            // fprintf(stderr, "BOUNDS VIOLATION in calculate_weak_hash_for_extended_chunk (line 278):\n");
            // fprintf(stderr, "  chunk_end=%lu, gap=%lu, read_end=%lu, file_size=%lu\n",
            //         chunk_end, gap, read_end, file_size);
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
            return;
        }
        
        cached_crc32 = crc32_isal(map + chunk_end, gap, 0);
        cdc.weak_hash = (cdc.weak_hash ^ cached_crc32);
        cdc.weak_hash = crc32_remove0(cdc.weak_hash, gap);
    }
}

void SkySyncCWorker::calculate_weak_hash_for_second_half(char *map, uint64_t offset, uint64_t chunk_length,
                                                        file_fsc *fsc, uint64_t fsc_index,
                                                        uint64_t mid_window_size, uint32_t &cached_crc32,
                                                        struct one_cdc &cdc, uint64_t next_fsc_offset,
                                                        uint64_t chunk_end, uint64_t file_size) {
    uint32_t tmp_crc32 = cached_crc32;
    uint64_t gap;

    if (chunk_end < (next_fsc_offset + mid_window_size)) {
        gap = chunk_end - next_fsc_offset;
        
        // BOUNDS CHECK: Ensure we don't read beyond file size
        uint64_t read_end = next_fsc_offset + gap;
        if (read_end > file_size) {
            // fprintf(stderr, "BOUNDS VIOLATION in calculate_weak_hash_for_second_half (line 294):\n");
            // fprintf(stderr, "  next_fsc_offset=%lu, gap=%lu, read_end=%lu, file_size=%lu\n",
            //         next_fsc_offset, gap, read_end, file_size);
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
            return;
        }
        
        cached_crc32 = crc32_isal(map + next_fsc_offset, gap, 0);
        cdc.weak_hash = crc32_comb(tmp_crc32, cached_crc32, gap);
    } else if (chunk_end < (next_fsc_offset + DefaultWindowSize)) {
        gap = next_fsc_offset + DefaultWindowSize - chunk_end;
        
        // BOUNDS CHECK: Ensure we don't read beyond file size
        uint64_t read_end = chunk_end + gap;
        if (read_end > file_size) {
            // fprintf(stderr, "BOUNDS VIOLATION in calculate_weak_hash_for_second_half (line 298):\n");
            // fprintf(stderr, "  chunk_end=%lu, gap=%lu, read_end=%lu, file_size=%lu\n",
            //         chunk_end, gap, read_end, file_size);
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
            return;
        }
        
        cached_crc32 = crc32_isal(map + chunk_end, gap, 0);
        cdc.weak_hash = crc32_comb(tmp_crc32, fsc->fsc_array[fsc_index + 1].weak_hash, DefaultWindowSize);
        cdc.weak_hash = (cdc.weak_hash ^ cached_crc32);
        cdc.weak_hash = crc32_remove0(cdc.weak_hash, gap);
    } else if (chunk_end == (next_fsc_offset + DefaultWindowSize)) {
        cdc.weak_hash = crc32_comb(tmp_crc32, fsc->fsc_array[fsc_index + 1].weak_hash, DefaultWindowSize);
    } else {
        if (fsc_index + 2 < fsc->chunk_num) {
            cdc.weak_hash = crc32_comb(tmp_crc32, fsc->fsc_array[fsc_index + 1].weak_hash, DefaultWindowSize);
            gap = chunk_end - fsc->fsc_array[fsc_index + 2].offset;
            
            // BOUNDS CHECK: Ensure we don't read beyond file size
            uint64_t read_end = fsc->fsc_array[fsc_index + 2].offset + gap;
            if (read_end > file_size) {
                // fprintf(stderr, "BOUNDS VIOLATION in calculate_weak_hash_for_second_half (line 308):\n");
                // fprintf(stderr, "  fsc_offset=%lu, gap=%lu, read_end=%lu, file_size=%lu\n",
                //         fsc->fsc_array[fsc_index + 2].offset, gap, read_end, file_size);
                cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
                return;
            }
            
            cached_crc32 = crc32_isal(map + fsc->fsc_array[fsc_index + 2].offset, gap, 0);
            cdc.weak_hash = crc32_comb(cdc.weak_hash, cached_crc32, gap);
        } else {
            cdc.weak_hash = crc32_isal(map + offset, chunk_length, 0);
        }
    }
}

void SkySyncCWorker::serial_cdc_hw(int fd, DataQueue<one_cdc> &csums_queue, file_fsc *fsc) {
    uint64_t chunk_num = 0;

    uint64_t fs = file_size(fd);

    char *map = (char *)map_file(fd);

    uint64_t offset = 0;

    for(;;) {
        uint64_t tmp_length = find_cutpoint_2(map + offset, fs - offset);

        struct one_cdc cdc = {
            .offset = offset,
            .length = tmp_length,
            .weak_hash = crc32_isal((map + offset), tmp_length, 0)
        };

        offset += tmp_length;
        csums_queue.push(cdc);

        if (offset >= fs)
            break;

        if (offset + MinSize > fs) {
            one_cdc cdc = {
                .offset = offset,
                .length = fs - offset,
                .weak_hash = crc32_isal((map + offset), fs - offset, 0)
            };
            csums_queue.push(cdc);
            break;
        }
    }

    unmap_file(fd, map);
    csums_queue.setDone();
}

void ServerSkySyncCWorker::patch_delta(int old_fd, int out_fd, DataQueue<data_cmd> &data_cmd_queue) {
    
    uint64_t bytes_write = 0;
    uint64_t total_delta = 0;

    while(!data_cmd_queue.isDone()) {
        data_cmd cmd = data_cmd_queue.pop();
        
        switch (cmd.cmd)
        {
            case CMD_LITERAL:
            {
                bytes_write = write(out_fd, cmd.data, cmd.length);
                assert(bytes_write == cmd.length);
                total_delta += bytes_write;
                mi_free(cmd.data);
                break;
            }
            case CMD_COPY:
            {
                off_t send_offset = cmd.offset;
                uint64_t bytes_sent = 0;
                uint64_t total_bytes_sent = 0;

                while (total_bytes_sent < cmd.length) {
                    bytes_sent = sendfile(out_fd, old_fd, &send_offset, cmd.length - total_bytes_sent);
                    if (bytes_sent <= 0) {
                        perror("sendfile");
                        return;
                    }
                    total_bytes_sent += bytes_sent;
                }
                break;
            }
            default:
                fprintf(stderr, "Unknown command\n");
                break;
        }
    }
    // printf("Total bytes written: %lu\n", total_delta);
}

data_cmd ClientSkySyncCWorker::create_data_cmd(int fd, int cmd_flag, uint64_t offset, uint64_t length) {
    data_cmd cmd;
    cmd.cmd = cmd_flag;
    cmd.length = length;
    cmd.offset = offset;
    cmd.data = NULL;
    if (cmd_flag == CMD_LITERAL) {
        cmd.data = (uint8_t *)mi_malloc(length);
        assert(cmd.data != NULL);

        lseek(fd, offset, SEEK_SET);
        uint64_t bytes_read = read(fd, cmd.data, length);
        assert(bytes_read == length);
    }

    #ifdef SIZE_TEST
        patch_commands_size += sizeof(data_cmd);
        if (cmd_flag == CMD_LITERAL) {
            literal_bytes_size += length;
        }
    #endif

    return cmd;
}

void ClientSkySyncCWorker::sort_matched_chunks(DataQueue<matched_item_rpc> &matched_chunks_queue) {
    std::vector<struct matched_item_rpc> matched_chunks;
    while(!matched_chunks_queue.isDone()) {
        struct matched_item_rpc mc_item = matched_chunks_queue.pop();
        matched_chunks.push_back(mc_item);
    }

    std::sort(matched_chunks.begin(), matched_chunks.end(),
        [](const struct matched_item_rpc &a, const struct matched_item_rpc &b) {
            return a.new_ol.offset < b.new_ol.offset;
        }
    );

    for (auto mc_item : matched_chunks) {
        matched_chunks_queue.push(mc_item);
    }

    matched_chunks_queue.setDone();
}

void ClientSkySyncCWorker::generate_delta(int new_fd, DataQueue<matched_item_rpc> &strong_matched_chunks_queue, DataQueue<data_cmd> &data_cmd_queue) {
    uint64_t offset = 0;
    uint64_t copy_offset = 0;
    uint64_t copy_length = 0;

    uint64_t new_fs = file_size(new_fd);

    // sort the matched chunks by the new offset
    this->sort_matched_chunks(strong_matched_chunks_queue);

    while(!strong_matched_chunks_queue.isDone()) {
        struct matched_item_rpc mc_item = strong_matched_chunks_queue.pop();

        if(mc_item.item_nums == 0) {
            data_cmd_queue.push(create_data_cmd(new_fd, CMD_LITERAL, offset, new_fs - offset));
            offset += new_fs - offset;
            continue;
        }

        if(mc_item.new_ol.offset > offset) {
            if(copy_length != 0) {
                data_cmd_queue.push(create_data_cmd(new_fd, CMD_COPY, copy_offset, copy_length));
                copy_length = 0; // Reset copy_length after pushing the command
            }
            data_cmd_queue.push(create_data_cmd(new_fd, CMD_LITERAL, offset, mc_item.new_ol.offset - offset));
            offset = mc_item.new_ol.offset;
            copy_offset = mc_item.old_ol.offset;
        }

        if(copy_length == 0) {
            copy_offset = mc_item.old_ol.offset;
        }

        if (mc_item.old_ol.offset == copy_offset + copy_length) {
            copy_length += mc_item.old_ol.length;
        } else {
            data_cmd_queue.push(create_data_cmd(new_fd, CMD_COPY, copy_offset, copy_length));
            copy_offset = mc_item.old_ol.offset;
            copy_length = mc_item.old_ol.length;
        }

        offset += mc_item.new_ol.length;
    }
    
    if (copy_length != 0) {
        data_cmd_queue.push(create_data_cmd(new_fd, CMD_COPY, copy_offset, copy_length));
    }
    if(offset < new_fs) {
        data_cmd_queue.push(create_data_cmd(new_fd, CMD_LITERAL, offset, new_fs - offset));
    }

    data_cmd_queue.setDone();
}

void ServerSkySyncCWorker::lhash_builder(DataQueue<one_cdc> &csums_queue, uint64_t chunk_nums) {
    
    this->lhash_table = new LinkedHashTable(chunk_nums / 2);
    if (this->lhash_table == nullptr) {
        fprintf(stderr, "Error: Failed to allocate memory for hash table\n");
        return;
    }

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        this->lhash_table->insert(cdc.weak_hash, {cdc.offset, cdc.length});
    }
}

void ServerSkySyncCWorker::hash7_builder(DataQueue<one_cdc> &csums_queue, uint64_t chunk_nums) {
    this->strong_hash7_table = new emhash7::HashMap<uint32_t, std::vector<ol>>(chunk_nums);
    if (this->strong_hash7_table == nullptr) {
        fprintf(stderr, "Error: Failed to allocate memory for hash7 table\n");
        return;
    }

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        (*this->strong_hash7_table)[cdc.weak_hash].push_back({cdc.offset, cdc.length});
    }
}

void ServerSkySyncCWorker::compare_weak_lhash(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue) {
    uint64_t matched_nums = 0;

    // Check if hash table exists
    if (this->lhash_table == nullptr) {
        fprintf(stderr, "Error: Hash table not initialized\n");
        matched_chunks_queue.setDone();
        return;
    }

    while (!new_crc32_queue.isDone()) {
        uint32_t weak_hash = new_crc32_queue.pop();

        // printf("weak hash: %u\n", weak_hash);
        // check if the weak hash is already in the hash table of the old file (this->lhash_table)
        if (this->lhash_table->contains(weak_hash)) {
            std::vector<ol> ol_vec = this->lhash_table->find(weak_hash);
            struct matched_item_rpc_1 mc_item;
            mc_item.weak_hash = weak_hash;

            #ifdef SIZE_TEST
                // record the crc32c and the same chunks which have the same crc32c
                same_crc32c_chunks.push_back(ol_vec.size());
            #endif
            
            /* iterate the vector of the matched weak hash to get the offset and length
            of the matched chunks, and calculate the sha hash for them */
            for (const auto &ol_item : ol_vec) {
                uint64_t offset = ol_item.offset;
                uint64_t length = ol_item.length;
                uint8_t hash[SHA256_OUT_LEN];
                uint8_t *file_buf = (uint8_t *)mi_malloc(length);

                lseek(fd, offset, SEEK_SET);

                ssize_t bytes_read = read(fd, file_buf, length);
                if (bytes_read != (ssize_t)length) {
                    fprintf(stderr, "Error: Failed to read %lu bytes, got %ld\n", length, bytes_read);
                    mi_free(file_buf);
                    continue;
                }

                cal_sha256(hash, file_buf, length);

                /* check if the sha hash is already in the map
                if not, insert the new sha hash to the map if yes, do nothing */
                auto sha_it = mc_item.sha_to_chunk_map.find(std::string((char *)hash, SHA256_OUT_LEN));
                if (sha_it == mc_item.sha_to_chunk_map.end()) {
                    // insert the new sha hash to the map
                    struct ol tmp_ol_item = {offset, length};
                    mc_item.sha_to_chunk_map.emplace(
                        std::string((char *)hash, SHA256_OUT_LEN),
                        std::move(tmp_ol_item)
                    );
                }
                mi_free(file_buf);
            }
            mc_item.item_nums = mc_item.sha_to_chunk_map.size();
            mc_item.end_of_stream = false;
            matched_chunks_queue.push(mc_item);
            matched_nums += 1;

            #ifdef SIZE_TEST
                matching_tokens_size += mc_item.sha_to_chunk_map.size() * (get_hash_length(this->shashing_algorithm) + sizeof(ol));
                matching_tokens_size += sizeof(mc_item);
            #endif
        }
    }

    if (matched_nums == 0) {
        // push empty matched item
        struct matched_item_rpc_1 mc_item;
        mc_item.item_nums = 0;
        mc_item.weak_hash = 0;
        mc_item.end_of_stream = false;
        matched_chunks_queue.push(mc_item);
    }

    matched_chunks_queue.setDone();
    
    // Clean up hash table memory
    if (this->lhash_table != nullptr) {
        delete this->lhash_table;
        this->lhash_table = nullptr;
    }

    // printf("Weak Matched chunks: %lu\n", strong_matched_nums);
}

void ClientSkySyncCWorker::lhash_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue, uint64_t chunk_nums) {
    
    this->lhash_table = new LinkedHashTable(chunk_nums / 2);

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        if (!this->lhash_table->contains(cdc.weak_hash)) {
            crc32_queue.push(cdc.weak_hash);

            // printf("weak hash: %u\n", cdc.weak_hash);
        }
        this->lhash_table->insert(cdc.weak_hash, {cdc.offset, cdc.length});
    }
    crc32_queue.setDone();
}

void ClientSkySyncCWorker::hash7_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue, uint64_t chunk_nums) {
    this->strong_hash7_table = new emhash7::HashMap<uint32_t, std::vector<ol>>(chunk_nums);

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        if (!this->strong_hash7_table->contains(cdc.weak_hash)) {
            crc32_queue.push(cdc.weak_hash);
        }
        (*this->strong_hash7_table)[cdc.weak_hash].push_back({cdc.offset, cdc.length});
    }
    crc32_queue.setDone();
}

void ClientSkySyncCWorker::compare_sha1_lhash(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {
    uint64_t strong_matched_nums = 0;

    while (!weak_matched_chunks_queue.isDone()) {
        struct matched_item_rpc_1 mc_item = weak_matched_chunks_queue.pop();
        if (mc_item.weak_hash == 0)
            continue;
        
        // Process each SHA hash in the map to find matches in the new file
        for (const auto &sha_pair : mc_item.sha_to_chunk_map) {
            const std::string &sha_hash = sha_pair.first;
            const ol &old_chunk = sha_pair.second;
            
            // Read the corresponding chunk from the new file using the hash table
            if (this->lhash_table != nullptr && this->lhash_table->contains(mc_item.weak_hash)) {
                std::vector<ol> new_chunks = this->lhash_table->find(mc_item.weak_hash);

                #ifdef SIZE_TEST
                    // record the crc32c and the same chunks which have the same crc32c
                    same_crc32c_chunks.push_back(new_chunks.size());
                #endif

                // Check each potential new chunk with the same weak hash
                for (const auto &new_chunk : new_chunks) {
                    uint64_t offset = new_chunk.offset;
                    uint64_t length = new_chunk.length;
                    
                    // Skip if lengths don't match (optimization)
                    if (length != old_chunk.length) {
                        continue;
                    }

                    uint8_t hash[SHA256_OUT_LEN];
                    uint8_t *file_buf = (uint8_t *)mi_malloc(length);
                    if (file_buf == nullptr) {
                        fprintf(stderr, "Error: Failed to allocate memory for file buffer\n");
                        continue;
                    }

                    lseek(fd, offset, SEEK_SET) == -1;

                    uint64_t bytes_read = read(fd, file_buf, length);
                    assert(bytes_read == length);

                    cal_sha256(hash, file_buf, length);

                    std::string new_hash((char *)hash, SHA256_OUT_LEN);

                    // Check if the SHA hashes match
                    if (new_hash == sha_hash) {
                        matched_item_rpc mc_item_1;
                        mc_item_1.item_nums = 1;
                        mc_item_1.new_ol = {offset, length};
                        mc_item_1.old_ol = {old_chunk.offset, old_chunk.length};
                        memset(mc_item_1.hash, 0, SHA256_OUT_LEN);
                        mc_item_1.end_of_stream = false;
                        strong_matched_chunks_queue.push(mc_item_1);
                        strong_matched_nums += 1;
                    }
                    mi_free(file_buf);
                }
            }
        }
    }

    if (strong_matched_nums == 0) {
        struct matched_item_rpc mc_item;
        mc_item.item_nums = 0;
        mc_item.new_ol = {0, 0};
        mc_item.end_of_stream = false;
        strong_matched_chunks_queue.push(mc_item);
    }

    strong_matched_chunks_queue.setDone();
    
    // Clean up hash table memory
    if (this->lhash_table != nullptr) {
        delete this->lhash_table;
        this->lhash_table = nullptr;
    }

    // printf("Strong Matched chunks: %lu\n", strong_matched_nums);
}

void ClientSkySyncCWorker::compare_sha1_hash7(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {
    uint64_t strong_matched_nums = 0;

    while (!weak_matched_chunks_queue.isDone()) {
        struct matched_item_rpc_1 mc_item = weak_matched_chunks_queue.pop();
        if (mc_item.weak_hash == 0)
            continue;
        
        // Process each SHA hash in the map to find matches in the new file
        for (const auto &sha_pair : mc_item.sha_to_chunk_map) {
            const std::string &sha_hash = sha_pair.first;
            const ol &old_chunk = sha_pair.second;
            
            // Read the corresponding chunk from the new file using the hash table
            if (this->strong_hash7_table->contains(mc_item.weak_hash)) {
                std::vector<ol> new_chunks = (*this->strong_hash7_table)[mc_item.weak_hash];

                #ifdef SIZE_TEST
                    // record the crc32c and the same chunks which have the same crc32c
                    same_crc32c_chunks.push_back(new_chunks.size());
                #endif

                // Check each potential new chunk with the same weak hash
                for (const auto &new_chunk : new_chunks) {
                    uint64_t offset = new_chunk.offset;
                    uint64_t length = new_chunk.length;
                    
                    // Skip if lengths don't match (optimization)
                    if (length != old_chunk.length) {
                        continue;
                    }

                    uint8_t hash[SHA256_OUT_LEN];
                    uint8_t *file_buf = (uint8_t *)mi_malloc(length);
                    if (file_buf == nullptr) {
                        fprintf(stderr, "Error: Failed to allocate memory for file buffer\n");
                        continue;
                    }

                    lseek(fd, offset, SEEK_SET) == -1;

                    uint64_t bytes_read = read(fd, file_buf, length);
                    assert(bytes_read == length);

                    cal_sha256(hash, file_buf, length);

                    std::string new_hash((char *)hash, SHA256_OUT_LEN);

                    // Check if the SHA hashes match
                    if (new_hash == sha_hash) {
                        matched_item_rpc mc_item_1;
                        mc_item_1.item_nums = 1;
                        mc_item_1.new_ol = {offset, length};
                        mc_item_1.old_ol = {old_chunk.offset, old_chunk.length};
                        memset(mc_item_1.hash, 0, SHA256_OUT_LEN);
                        mc_item_1.end_of_stream = false;
                        strong_matched_chunks_queue.push(mc_item_1);
                        strong_matched_nums += 1;
                    }
                    mi_free(file_buf);
                }
            }
        }
    }

    if (strong_matched_nums == 0) {
        struct matched_item_rpc mc_item;
        mc_item.item_nums = 0;
        mc_item.new_ol = {0, 0};
        mc_item.end_of_stream = false;
        strong_matched_chunks_queue.push(mc_item);
    }

    strong_matched_chunks_queue.setDone();
    
    // Clean up hash table memory
    if (this->strong_hash7_table != nullptr) {
        delete this->strong_hash7_table;
        this->strong_hash7_table = nullptr;
    }
}

void ServerSkySyncCWorker::compare_weak_hash7(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue) {
    uint64_t matched_nums = 0;

    while (!new_crc32_queue.isDone()) {
        uint32_t weak_hash = new_crc32_queue.pop();

        // check if the weak hash is already in the hash table of the old file (this->strong_hash7_table)
        if (this->strong_hash7_table->contains(weak_hash)) {
            std::vector<ol> ol_vec = (*this->strong_hash7_table)[weak_hash];
            struct matched_item_rpc_1 mc_item;
            mc_item.weak_hash = weak_hash;

            #ifdef SIZE_TEST
                // record the crc32c and the same chunks which have the same crc32c
                same_crc32c_chunks.push_back(ol_vec.size());
            #endif
            
            /* iterate the vector of the matched weak hash to get the offset and length
            of the matched chunks, and calculate the sha hash for them */
            for (const auto &ol_item : ol_vec) {
                uint64_t offset = ol_item.offset;
                uint64_t length = ol_item.length;
                uint8_t hash[SHA256_OUT_LEN];
                uint8_t *file_buf = (uint8_t *)mi_malloc(length);

                lseek(fd, offset, SEEK_SET);

                ssize_t bytes_read = read(fd, file_buf, length);
                if (bytes_read != (ssize_t)length) {
                    fprintf(stderr, "Error: Failed to read %lu bytes, got %ld\n", length, bytes_read);
                    mi_free(file_buf);
                    continue;
                }

                cal_sha256(hash, file_buf, length);

                /* check if the sha hash is already in the map
                if not, insert the new sha hash to the map if yes, do nothing */
                auto sha_it = mc_item.sha_to_chunk_map.find(std::string((char *)hash, SHA256_OUT_LEN));
                if (sha_it == mc_item.sha_to_chunk_map.end()) {
                    // insert the new sha hash to the map
                    struct ol tmp_ol_item = {offset, length};
                    mc_item.sha_to_chunk_map.emplace(
                        std::string((char *)hash, SHA256_OUT_LEN),
                        std::move(tmp_ol_item)
                    );
                }
                mi_free(file_buf);
            }
            mc_item.item_nums = mc_item.sha_to_chunk_map.size();
            mc_item.end_of_stream = false;
            matched_chunks_queue.push(mc_item);
            matched_nums += 1;

            #ifdef SIZE_TEST
                matching_tokens_size += mc_item.sha_to_chunk_map.size() * (get_hash_length(this->shashing_algorithm) + sizeof(ol));
                matching_tokens_size += sizeof(mc_item);
            #endif
        }
    }

    if (matched_nums == 0) {
        // push empty matched item
        struct matched_item_rpc_1 mc_item;
        mc_item.item_nums = 0;
        mc_item.weak_hash = 0;
        mc_item.end_of_stream = false;
        matched_chunks_queue.push(mc_item);
    }

    matched_chunks_queue.setDone();
    
    // Clean up hash table memory
    if (this->strong_hash7_table != nullptr) {
        delete this->strong_hash7_table;
        this->strong_hash7_table = nullptr;
    }
}