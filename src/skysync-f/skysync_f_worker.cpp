#include <assert.h>
#include <isa-l_crypto/rolling_hashx.h>
#include "skysync_f_worker.h"
#include "blake3.h"
#include "crc32c.h"
#include "crc32/crc32.h"

off_t file_size(int fd);
int compare_offset(const void *a, const void *b);
void *map_file(int fd);
void unmap_file(int fd, void *map);

file_fsc* calc_fsc_hw(int fd) {
    uint64_t fs = file_size(fd);
    uint64_t chunk_num = (fs + DefaultWindowSize - 1) / DefaultWindowSize;
    file_fsc *csums = create_file_fsc(chunk_num);

    if (!csums) {
        fprintf(stderr, "Failed to allocate file_fsc structure\n");
        return nullptr;
    }

    char *map = (char *)map_file(fd);
    if (!map) {
        fprintf(stderr, "Failed to map file\n");
        free_file_fsc(csums);
        return nullptr;
    }

    for (uint64_t i = 0; i < chunk_num; i++) {
        uint64_t offset = i * DefaultWindowSize;
        uint64_t length = std::min((uint64_t)DefaultWindowSize, fs - offset);

        csums->fsc_array[i].offset = offset;
        csums->fsc_array[i].length = length;
        csums->fsc_array[i].weak_hash = crc32_isal((const unsigned char*)(map + offset), length, 0);

        uint8_t hash[SHA256_OUT_LEN];
        cal_sha256(hash, (uint8_t*)(map + offset), length);
        csums->fsc_array[i].strong_hash.assign(reinterpret_cast<char*>(hash), SHA256_OUT_LEN);
    }

    unmap_file(fd, map);
    return csums;
}

// Helper functions for file_fsc management
file_fsc* create_file_fsc(uint64_t chunk_count) {
    file_fsc* fsc = (file_fsc*)mi_malloc(sizeof(file_fsc));
    if (!fsc) {
        return nullptr;
    }
    
    fsc->chunk_num = chunk_count;
    fsc->fsc_array = new one_fsc[chunk_count];
    if (!fsc->fsc_array && chunk_count > 0) {
        mi_free(fsc);
        return nullptr;
    }
    
    return fsc;
}

void free_file_fsc(file_fsc* fsc) {
    if (fsc) {
        if (fsc->fsc_array) {
            delete[] fsc->fsc_array;
        }
        mi_free(fsc);
    }
}

void write_fsc(const char *fsc_file, file_fsc *fsc) {
    FILE *fp = fopen(fsc_file, "wb");
    if (!fp) {
        perror("Failed to open file for writing");
        return;
    }
    uint64_t write_bytes = 0;

    write_bytes = fwrite(&fsc->chunk_num, sizeof(uint64_t), 1, fp);

    for (uint64_t i = 0; i < fsc->chunk_num; ++i) {
        write_bytes = fwrite(&fsc->fsc_array[i].offset, sizeof(uint64_t), 1, fp);
        write_bytes = fwrite(&fsc->fsc_array[i].length, sizeof(uint64_t), 1, fp);
        write_bytes = fwrite(&fsc->fsc_array[i].weak_hash, sizeof(uint32_t), 1, fp);

        size_t strong_hash_len = fsc->fsc_array[i].strong_hash.length();
        write_bytes = fwrite(&strong_hash_len, sizeof(size_t), 1, fp);
        write_bytes = fwrite(fsc->fsc_array[i].strong_hash.data(), 1, strong_hash_len, fp);
    }

    fclose(fp);
}

file_fsc* read_fsc(const char *fsc_file) {
    FILE *fp = fopen(fsc_file, "rb");
    if (!fp) {
        perror("Failed to open file for reading");
        return nullptr;
    }

    uint64_t read_bytes = 0;
    uint64_t chunk_num;
    read_bytes = fread(&chunk_num, sizeof(uint64_t), 1, fp);
    file_fsc* fsc = create_file_fsc(chunk_num);

    if (!fsc) {
        perror("Failed to allocate file_fsc");
        fclose(fp);
        return nullptr;
    }

    for (uint64_t i = 0; i < fsc->chunk_num; ++i) {
        read_bytes = fread(&fsc->fsc_array[i].offset, sizeof(uint64_t), 1, fp);
        read_bytes = fread(&fsc->fsc_array[i].length, sizeof(uint64_t), 1, fp);
        read_bytes = fread(&fsc->fsc_array[i].weak_hash, sizeof(uint32_t), 1, fp);

        size_t strong_hash_len;
        read_bytes = fread(&strong_hash_len, sizeof(size_t), 1, fp);

        std::vector<char> strong_hash_data(strong_hash_len);
        read_bytes = fread(strong_hash_data.data(), 1, strong_hash_len, fp);
        fsc->fsc_array[i].strong_hash.assign(strong_hash_data.data(), strong_hash_len);
    }

    fclose(fp);
    return fsc;
}

SkySyncFWorker::SkySyncFWorker() {}

SkySyncFWorker::~SkySyncFWorker() {}

ClientSkySyncFWorker::ClientSkySyncFWorker(uint8_t whashing) {
    // Initialize member variables
    this->old_csums = nullptr;
    this->new_csums = nullptr;
    this->weak_hash_table = nullptr;
    this->strong_hash_table = nullptr;
    
    this->data_cmd_queue.init();
    switch (whashing) {
        case 0:
            this->rolling_fsc = [this](int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue) {
                this->rolling_fsc_sw(fd, old_csums, new_csums, data_cmd_queue);
            };
            break;
        case 1:
            this->rolling_fsc = [this](int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue) {
                this->rolling_fsc_hw(fd, old_csums, new_csums, data_cmd_queue);
            };
            break;
        default:
            this->rolling_fsc = [this](int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue) {
                this->rolling_fsc_sw(fd, old_csums, new_csums, data_cmd_queue);
            };
            break;
    }
}

ClientSkySyncFWorker::~ClientSkySyncFWorker() {
    // Clean up allocated memory
    if (this->old_csums) {
        free_file_fsc(this->old_csums);
        this->old_csums = nullptr;
    }
    if (this->new_csums) {
        free_file_fsc(this->new_csums);
        this->new_csums = nullptr;
    }
    if (this->weak_hash_table) {
        delete this->weak_hash_table;
        this->weak_hash_table = nullptr;
    }
    if (this->strong_hash_table) {
        delete this->strong_hash_table;
        this->strong_hash_table = nullptr;
    }
}

ServerSkySyncFWorker::ServerSkySyncFWorker(uint8_t whashing) {
    this->data_cmd_queue.init();
}

ServerSkySyncFWorker::~ServerSkySyncFWorker() {}

void ServerSkySyncFWorker::patch_delta(int old_fd, int out_fd, DataQueue<data_cmd> &data_cmd_queue) {
    
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

void ClientSkySyncFWorker::chash_builder(file_fsc *old_csums) {
    // Estimate good table sizes based on chunk count
    uint64_t table_size = old_csums->chunk_num;
    
    this->weak_hash_table = new WeakHashTable(table_size);
    this->strong_hash_table = new StrongHashTable(table_size);
    
    for (uint64_t i = 0; i < old_csums->chunk_num; i++) {
        const one_fsc &fsc = old_csums->fsc_array[i];
        
        // Insert weak hash for existence checking
        this->weak_hash_table->insert(fsc.weak_hash);
        
        // Insert strong hash mapping to chunk location
        this->strong_hash_table->insert(fsc.strong_hash, ol{fsc.offset, fsc.length});
    }
}

void ClientSkySyncFWorker::hash7_builder(file_fsc *old_csums) {
    uint64_t table_size = old_csums->chunk_num;
    this->weak_hash7_table = new emhash7::HashMap<uint32_t, bool>(table_size);
    // this->strong_hash7_table = new emhash7::HashMap<std::string, ol>(table_size);
    this->strong_hash_table = new StrongHashTable(table_size);

    for (uint64_t i = 0; i < old_csums->chunk_num; i++) {
        const one_fsc &fsc = old_csums->fsc_array[i];
        this->weak_hash7_table->emplace(fsc.weak_hash, true);
        this->strong_hash_table->insert(fsc.strong_hash, ol{fsc.offset, fsc.length});
    }
}

void ClientSkySyncFWorker::rolling_fsc_sw(int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue) {
    uint64_t fs = file_size(fd);

    char *map = (char*)map_file(fd);

    // Handle small files
    if (fs < DefaultWindowSize) {
        data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, 0, fs);
        data_cmd_queue.push(literal_cmd);
        unmap_file(fd, map);
        data_cmd_queue.setDone();
        return;
    }

    // Phase 1: Sequential comparison to find first mismatch
    uint64_t first_mismatch_idx = 0;
    uint64_t min_chunks = std::min(old_csums->chunk_num, new_csums->chunk_num);
    
    for (uint64_t i = 0; i < min_chunks; i++) {
        const one_fsc &old_chunk = old_csums->fsc_array[i];
        const one_fsc &new_chunk = new_csums->fsc_array[i];
        
        if (old_chunk.weak_hash != new_chunk.weak_hash ||
            old_chunk.strong_hash != new_chunk.strong_hash) {
            break;
        }
        first_mismatch_idx++;
    }
    
    // If all chunks match, generate single copy command
    if (first_mismatch_idx == min_chunks && old_csums->chunk_num == new_csums->chunk_num) {
        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, 0, fs);
        data_cmd_queue.push(copy_cmd);
        unmap_file(fd, map);
        data_cmd_queue.setDone();
        return;
    }
    
    // Generate copy command for matched prefix
    if (first_mismatch_idx > 0) {
        uint64_t prefix_length = first_mismatch_idx * DefaultWindowSize;
        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, 0, prefix_length);
        data_cmd_queue.push(copy_cmd);
    }
    
    // Phase 2: Rolling hash from first mismatch point
    uint64_t file_offset = first_mismatch_idx * DefaultWindowSize;
    uint64_t literal_start = file_offset;
    
    // Variables for batching copy commands
    uint64_t copy_offset = 0;
    uint64_t copy_length = 0;
    bool in_copy_sequence = false;
    
    // Initialize rolling CRC with first window at current position
    if (file_offset + DefaultWindowSize > fs) {
        // Handle remaining data as literal
        if (file_offset < fs) {
            data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, file_offset, fs - file_offset);
            data_cmd_queue.push(literal_cmd);
        }
        unmap_file(fd, map);
        data_cmd_queue.setDone();
        return;
    }
    
    uint32_t rolling_crc = crc32_fast((const unsigned char*)(map + file_offset), DefaultWindowSize, 0);

    // Rolling hash loop - handle all possible windows including the last one
    while (file_offset + DefaultWindowSize <= fs) {
        // if (this->weak_hash_table->contains(rolling_crc)) {
        if (this->weak_hash7_table->contains(rolling_crc)) {
            // Calculate strong hash for verification
            uint8_t strong_hash_bytes[SHA256_OUT_LEN];
            cal_sha256(strong_hash_bytes, (uint8_t*)(map + file_offset), DefaultWindowSize);
            std::string strong_hash_str(reinterpret_cast<char*>(strong_hash_bytes), SHA256_OUT_LEN);
            
            if (this->strong_hash_table->contains(strong_hash_str)) {
                // const ol match_info = this->strong_hash7_table->at(strong_hash_str);
                const ol match_info = this->strong_hash_table->find(strong_hash_str);
                
                // Generate literal command for unmatched data before this match
                if (file_offset > literal_start) {
                    // Flush any pending copy command first
                    if (in_copy_sequence) {
                        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
                        data_cmd_queue.push(copy_cmd);
                        in_copy_sequence = false;
                        copy_length = 0;
                    }
                    
                    data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, literal_start, file_offset - literal_start);
                    data_cmd_queue.push(literal_cmd);
                }
                
                // Handle copy command batching
                if (!in_copy_sequence) {
                    // Start new copy sequence
                    copy_offset = match_info.offset;
                    copy_length = match_info.length;
                    in_copy_sequence = true;
                } else {
                    // Check if this match is consecutive with the previous one
                    if (match_info.offset == copy_offset + copy_length) {
                        // Extend current copy sequence
                        copy_length += match_info.length;
                    } else {
                        // Non-consecutive match, flush previous copy and start new one
                        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
                        data_cmd_queue.push(copy_cmd);
                        
                        copy_offset = match_info.offset;
                        copy_length = match_info.length;
                    }
                }
                
                // Advance past matched region
                file_offset += DefaultWindowSize;
                literal_start = file_offset;
                
                // Recalculate rolling CRC for new position if there's another window
                if (file_offset + DefaultWindowSize <= fs) {
                    rolling_crc = crc32_fast((const unsigned char*)(map + file_offset), DefaultWindowSize, 0);
                }
                continue;
            }
        }

        // No match found, flush any pending copy command
        if (in_copy_sequence) {
            data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
            data_cmd_queue.push(copy_cmd);
            in_copy_sequence = false;
            copy_length = 0;
        }
        
        // Update rolling hash if we can still form a complete window
        file_offset++;
        if (file_offset + DefaultWindowSize <= fs) {
            rolling_crc = rolling_crc32_1byte_8KB(rolling_crc, map[file_offset + DefaultWindowSize - 1], map[file_offset - 1]);
        }
    }
    
    // Flush any pending copy command before handling remaining literal data
    if (in_copy_sequence) {
        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
        data_cmd_queue.push(copy_cmd);
    }
    
    // Handle remaining data as literal
    if (literal_start < fs) {
        data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, literal_start, fs - literal_start);
        data_cmd_queue.push(literal_cmd);
    }
    
    unmap_file(fd, map);
    data_cmd_queue.setDone();
}

void ClientSkySyncFWorker::rolling_fsc_hw(int fd, file_fsc *old_csums, file_fsc *new_csums, DataQueue<data_cmd> &data_cmd_queue) {
    uint64_t fs = file_size(fd);

    char *map = (char*)map_file(fd);

    // Handle small files
    if (fs < DefaultWindowSize) {
        data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, 0, fs);
        data_cmd_queue.push(literal_cmd);
        unmap_file(fd, map);
        data_cmd_queue.setDone();
        return;
    }

    // Phase 1: Sequential comparison to find first mismatch
    uint64_t first_mismatch_idx = 0;
    uint64_t min_chunks = std::min(old_csums->chunk_num, new_csums->chunk_num);
    
    for (uint64_t i = 0; i < min_chunks; i++) {
        const one_fsc &old_chunk = old_csums->fsc_array[i];
        const one_fsc &new_chunk = new_csums->fsc_array[i];
        
        if (old_chunk.weak_hash != new_chunk.weak_hash ||
            old_chunk.strong_hash != new_chunk.strong_hash) {
            break;
        }
        first_mismatch_idx++;
    }
    
    // If all chunks match, generate single copy command
    if (first_mismatch_idx == min_chunks && old_csums->chunk_num == new_csums->chunk_num) {
        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, 0, fs);
        data_cmd_queue.push(copy_cmd);
        unmap_file(fd, map);
        data_cmd_queue.setDone();
        return;
    }
    
    // Generate copy command for matched prefix
    if (first_mismatch_idx > 0) {
        uint64_t prefix_length = first_mismatch_idx * DefaultWindowSize;
        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, 0, prefix_length);
        data_cmd_queue.push(copy_cmd);
    }
    
    // Phase 2: Rolling hash from first mismatch point
    uint64_t file_offset = first_mismatch_idx * DefaultWindowSize;
    uint64_t literal_start = file_offset;
    
    // Variables for batching copy commands
    uint64_t copy_offset = 0;
    uint64_t copy_length = 0;
    bool in_copy_sequence = false;
    
    // Initialize rolling CRC with first window at current position
    if (file_offset + DefaultWindowSize > fs) {
        // Handle remaining data as literal
        if (file_offset < fs) {
            data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, file_offset, fs - file_offset);
            data_cmd_queue.push(literal_cmd);
        }
        unmap_file(fd, map);
        data_cmd_queue.setDone();
        return;
    }
    
    uint32_t rolling_crc = crc32_isal((const unsigned char*)(map + file_offset), DefaultWindowSize, 0);

    // Rolling hash loop - handle all possible windows including the last one
    while (file_offset + DefaultWindowSize <= fs) {
        if (this->weak_hash7_table->contains(rolling_crc)) {
            // Calculate strong hash for verification
            uint8_t strong_hash_bytes[SHA256_OUT_LEN];
            cal_sha256(strong_hash_bytes, (uint8_t*)(map + file_offset), DefaultWindowSize);
            std::string strong_hash_str(reinterpret_cast<char*>(strong_hash_bytes), SHA256_OUT_LEN);
            
            if (this->strong_hash_table->contains(strong_hash_str)) {
                const ol match_info = this->strong_hash_table->find(strong_hash_str);
                
                // Generate literal command for unmatched data before this match
                if (file_offset > literal_start) {
                    // Flush any pending copy command first
                    if (in_copy_sequence) {
                        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
                        data_cmd_queue.push(copy_cmd);
                        in_copy_sequence = false;
                        copy_length = 0;
                    }
                    
                    data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, literal_start, file_offset - literal_start);
                    data_cmd_queue.push(literal_cmd);
                }
                
                // Handle copy command batching
                if (!in_copy_sequence) {
                    // Start new copy sequence
                    copy_offset = match_info.offset;
                    copy_length = match_info.length;
                    in_copy_sequence = true;
                } else {
                    // Check if this match is consecutive with the previous one
                    if (match_info.offset == copy_offset + copy_length) {
                        // Extend current copy sequence
                        copy_length += match_info.length;
                    } else {
                        // Non-consecutive match, flush previous copy and start new one
                        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
                        data_cmd_queue.push(copy_cmd);
                        
                        copy_offset = match_info.offset;
                        copy_length = match_info.length;
                    }
                }
                
                // Advance past matched region
                file_offset += DefaultWindowSize;
                literal_start = file_offset;
                
                // Recalculate rolling CRC for new position if there's another window
                if (file_offset + DefaultWindowSize <= fs) {
                    rolling_crc = crc32_isal((const unsigned char*)(map + file_offset), DefaultWindowSize, 0);
                }
                continue;
            }
        }

        // No match found, flush any pending copy command
        if (in_copy_sequence) {
            data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
            data_cmd_queue.push(copy_cmd);
            in_copy_sequence = false;
            copy_length = 0;
        }
        
        // Update rolling hash if we can still form a complete window
        file_offset++;
        if (file_offset + DefaultWindowSize <= fs) {
            rolling_crc = rolling_crc32_1byte_8KB(rolling_crc, map[file_offset + DefaultWindowSize - 1], map[file_offset - 1]);
        }
    }
    
    // Flush any pending copy command before handling remaining literal data
    if (in_copy_sequence) {
        data_cmd copy_cmd = create_data_cmd(fd, CMD_COPY, copy_offset, copy_length);
        data_cmd_queue.push(copy_cmd);
    }
    
    // Handle remaining data as literal
    if (literal_start < fs) {
        data_cmd literal_cmd = create_data_cmd(fd, CMD_LITERAL, literal_start, fs - literal_start);
        data_cmd_queue.push(literal_cmd);
    }
    
    unmap_file(fd, map);
    data_cmd_queue.setDone();
}

data_cmd ClientSkySyncFWorker::create_data_cmd(int fd, int cmd_flag, uint64_t offset, uint64_t length) {
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