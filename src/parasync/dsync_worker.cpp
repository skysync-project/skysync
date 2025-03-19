#include <assert.h>
#include <isa-l_crypto/rolling_hashx.h>
#include "dsync_worker.h"
#include "blake3.h"
#include "crc32c.h"

#ifdef SIZE_TEST
    uint64_t matching_tokens_size = 0;
    uint64_t patch_commands_size = 0;
    uint64_t literal_bytes_size = 0;
    std::vector<uint64_t> same_crc32c_chunks;
#endif

off_t file_size(int fd);
int compare_offset(const void *a, const void *b);
void *map_file(int fd);
void unmap_file(int fd, void *map);

void calc_blake3(uint8_t *b3_hash, uint8_t *buf, uint32_t len) {
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, buf, len);
    blake3_hasher_finalize(&hasher, b3_hash, BLAKE3_OUT_LEN);
}

SyncWorker::SyncWorker() {}

SyncWorker::~SyncWorker() {}

ClientSyncWorker::ClientSyncWorker() {
    this->new_csums_queue.init();
    this->weak_matched_chunks_queue.init();
    this->strong_matched_chunks_queue.init();
    this->data_cmd_queue.init();
}

ClientSyncWorker::~ClientSyncWorker() {}

ServerSyncWorker::ServerSyncWorker() {
    this->old_csums_queue.init();
    this->new_csums_queue.init();
    this->weak_matched_chunks_queue.init();
    this->data_cmd_queue.init();
    this->hash_table = NULL;
}

ServerSyncWorker::~ServerSyncWorker() {}

void SyncWorker::serial_cdc(int fd, DataQueue<one_cdc> &csums_queue) {
    uint64_t chunk_num = 0;
    uint64_t bytes_write = 0;
    fastCDC_init();

    uint64_t fs = file_size(fd);

    char *map = (char *)map_file(fd);

    uint64_t offset = 0;

    for(;;) {
        uint64_t tmp_length = cdc_origin_64_skysync((unsigned char *)map + offset, fs - offset);

        struct one_cdc cdc = {
            .offset = offset,
            .length = tmp_length,
            // .weak_hash = crc32c_hw(0, map + offset, tmp_length)
            .weak_hash = crc32_isal(map + offset, tmp_length, 0)
        };

        offset += tmp_length;
        csums_queue.push(cdc);

        chunk_num += 1;

        if (offset >= fs)
            break;

        if (offset + MinSize > fs) {
            one_cdc cdc = {
                .offset = offset,
                .length = fs - offset,
                .weak_hash = crc32_isal(map + offset, fs - offset, 0)
                // .weak_hash = crc32c_hw(0, map + offset, fs - offset)
            };
            csums_queue.push(cdc);
            chunk_num += 1;
            break;
        }
    }

    unmap_file(fd, map);
    csums_queue.setDone();
}

void SyncWorker::serial_cdc_isal(int fd, DataQueue<one_cdc> &csums_queue) {
    uint64_t chunk_num = 0;
    uint64_t bytes_write = 0;

    struct isal_rh_state2 *state;
    int ret;
    uint32_t w = 32;
    uint32_t mask, trigger, offset = 0;
    uint32_t min_chunk, max_chunk, mean_chunk;
    min_chunk = 4096;
    mean_chunk = 8192;
    max_chunk = 12288;
    isal_rolling_hashx_mask_gen(mean_chunk, 0, &mask);
    trigger = rand() & mask;

    // posix_memalign((void **)&state, 16, sizeof(struct isal_rh_state2));
    state = (struct isal_rh_state2 *)mi_malloc(sizeof(struct isal_rh_state2));
    isal_rolling_hash2_init(state, w);

    uint64_t fs = file_size(fd);

    char *map = (char *)map_file(fd);
    uint64_t remain;
    remain = fs;

    uint64_t pre_offset = 0;
    uint8_t *p = (uint8_t *)map;

    while (remain > max_chunk) {
        isal_rolling_hash2_reset(state, p + min_chunk - w);
        isal_rolling_hash2_run(state, p + min_chunk, max_chunk - min_chunk, mask, trigger, &offset, &ret);
        
        struct one_cdc cdc = {
            .offset = pre_offset,
            .length = offset + min_chunk,
        };
        cdc.weak_hash = crc32_isal(map + cdc.offset, cdc.length, 0);
        csums_queue.push(cdc);
        
        chunk_num += 1;
        p += (offset + min_chunk);
        pre_offset += (offset + min_chunk);
        remain -= (offset + min_chunk);
    }

    while (remain > min_chunk) {
        isal_rolling_hash2_reset(state, p + min_chunk - w);
        isal_rolling_hash2_run(state, p + min_chunk, remain - min_chunk, mask, trigger, &offset, &ret);
        
        struct one_cdc cdc = {
            .offset = pre_offset,
            .length = offset + min_chunk
        };
        cdc.weak_hash = crc32_isal(map + cdc.offset, cdc.length, 0);
        csums_queue.push(cdc);
        
        chunk_num += 1;
        p += (offset + min_chunk);
        pre_offset += (offset + min_chunk);
        remain -= (offset + min_chunk);
    }

    if (remain > 0) {
        struct one_cdc cdc = {
            .offset = pre_offset,
            .length = remain
        };
        cdc.weak_hash = crc32_isal(map + cdc.offset, cdc.length, 0);
        csums_queue.push(cdc);
        chunk_num += 1;
    }
    
    unmap_file(fd, map);
    mi_free(state);
    csums_queue.setDone();
}

// build uthash table for the checksums of the old file
void ServerSyncWorker::uthash_builder(DataQueue<one_cdc> &old_csums_queue) {
    this->hash_table = NULL;
    struct cdc_uthash *s;

    while (!old_csums_queue.isDone()) {
        one_cdc cdc = old_csums_queue.pop();
        s = NULL;
        HASH_FIND(hh, this->hash_table, &cdc.weak_hash, sizeof(uint32_t), s);

        if(s == NULL) {
            s = (struct cdc_uthash *)mi_malloc(sizeof(struct cdc_uthash));
            memcpy(&s->weak_hash, &cdc.weak_hash, sizeof(uint32_t));

            s->cdc_item_array = (struct ol*)mi_malloc(sizeof(struct ol) * ITEMNUMS);
            s->cdc_item_array[0].offset = cdc.offset;
            s->cdc_item_array[0].length = cdc.length;

            s->item_nums = 1;
            s->remalloc = 1;

            HASH_ADD(hh, this->hash_table, weak_hash, sizeof(uint32_t), s);
        } else {
            if(s->item_nums >= (s->remalloc * ITEMNUMS)) {
                s->remalloc += 1;
                struct ol *tmp = (struct ol *)mi_malloc(sizeof(struct ol) * ITEMNUMS * s->remalloc);
                memcpy(tmp, s->cdc_item_array, sizeof(struct ol) * s->item_nums);
                mi_free(s->cdc_item_array);
                s->cdc_item_array = tmp;
            }
            s->cdc_item_array[s->item_nums].offset = cdc.offset;
            s->cdc_item_array[s->item_nums].length = cdc.length;
            s->item_nums += 1;
        }
    }
}

void ServerSyncWorker::chash_builder(DataQueue<one_cdc> &csums_queue) {
    this->chash_table = new crc_to_chunks_map();

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        if (this->chash_table->contains(cdc.weak_hash)) {
            // insert the new cdc to the vector
            struct ol ol_item = {cdc.offset, cdc.length};
            (*this->chash_table)[cdc.weak_hash].push_back(ol_item);
        } else {
            // create a new vector and insert it
            std::vector<ol> ol_vec{{cdc.offset, cdc.length}};
            this->chash_table->emplace(cdc.weak_hash, std::move(ol_vec));
        }
    }
}

void ServerSyncWorker::compare_weak_uthash(int fd, DataQueue<one_cdc> &new_csums_queue, DataQueue<matched_item_rpc> &matched_chunks_queue) {
    uint64_t matched_nums = 0;

    while(!new_csums_queue.isDone()) {
        struct one_cdc cdc = new_csums_queue.pop();
        struct cdc_uthash *s = NULL;
        HASH_FIND(hh, this->hash_table, &cdc.weak_hash, sizeof(uint32_t), s);

        if(s != NULL) {
            struct matched_item_rpc mc_item;
            mc_item.item_nums = s->item_nums;
            mc_item.new_ol.offset = cdc.offset;
            mc_item.new_ol.length = cdc.length;

            mc_item.old_ol.offset = s->cdc_item_array[0].offset;
            mc_item.old_ol.length = s->cdc_item_array[0].length;

            uint8_t hash[BLAKE3_OUT_LEN];
            uint8_t old_file_buf[mc_item.old_ol.length];
            
            lseek(fd, mc_item.old_ol.offset, SEEK_SET);

            uint64_t old_bytes_read = read(fd, old_file_buf, mc_item.old_ol.length);
            assert(old_bytes_read == mc_item.old_ol.length);
            calc_blake3(hash, old_file_buf, mc_item.old_ol.length);

            memcpy(mc_item.hash, hash, BLAKE3_OUT_LEN);

            mc_item.end_of_stream = false;
            matched_chunks_queue.push(mc_item);
            matched_nums += 1;
        }
    }

    if(matched_nums == 0) {
        // push empty matched item
        struct matched_item_rpc mc_item;
        mc_item.item_nums = 0;
        mc_item.new_ol = {0, 0};
        matched_chunks_queue.push(mc_item);
    }

    matched_chunks_queue.setDone();
    cdc_delete_uthash(&this->hash_table);
}

void ServerSyncWorker::compare_weak_chash(int fd, DataQueue<uint32_t> &new_crc32_queue, DataQueue<matched_item_rpc_1> &matched_chunks_queue) {
    uint64_t matched_nums = 0;

    while (!new_crc32_queue.isDone()) {
        uint32_t weak_hash = new_crc32_queue.pop();

        // check if the weak hash is already in the hash table of the old file (this->chash_table)
        auto it = this->chash_table->find(weak_hash);
        if (it != this->chash_table->end()) {
            struct matched_item_rpc_1 mc_item;
            mc_item.weak_hash = weak_hash;

            #ifdef SIZE_TEST
                // record the crc32c and the same chunks which have the same crc32c
                same_crc32c_chunks.push_back(it->second.size());
            #endif
            
            /* iterate the vector of the matched weak hash to get the offset and length
            of the matched chunks, and calculate the sha hash for them */
            for (auto &ol_item : it->second) {
                uint64_t offset = ol_item.offset;
                uint64_t length = ol_item.length;
                uint8_t hash[BLAKE3_OUT_LEN];
                uint8_t *file_buf = (uint8_t *)mi_malloc(length);

                lseek(fd, offset, SEEK_SET);

                uint64_t bytes_read = read(fd, file_buf, length);
                assert(bytes_read == length);
                calc_blake3(hash, file_buf, length);

                /* check if the sha hash is already in the map
                if not, insert the new sha hash to the map if yes, do nothing */
                auto sha_it = mc_item.sha_to_chunk_map.find(std::string((char *)hash, BLAKE3_OUT_LEN));
                if (sha_it == mc_item.sha_to_chunk_map.end()) {
                    // insert the new sha hash to the map
                    struct ol tmp_ol_item = {offset, length};
                    mc_item.sha_to_chunk_map.emplace(
                        std::string((char *)hash, BLAKE3_OUT_LEN), 
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
                matching_tokens_size += mc_item.sha_to_chunk_map.size() * (BLAKE3_OUT_LEN + sizeof(ol));
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
    this->chash_table->clear();
}

void ServerSyncWorker::patch_delta(int old_fd, int out_fd, DataQueue<data_cmd> &data_cmd_queue) {
    
    uint64_t bytes_write = 0;

    while(!data_cmd_queue.isDone()) {
        data_cmd cmd = data_cmd_queue.pop();
        
        switch (cmd.cmd)
        {
            case CMD_LITERAL:
            {
                bytes_write = write(out_fd, cmd.data, cmd.length);
                assert(bytes_write == cmd.length);
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
}

void ClientSyncWorker::chash_builder(DataQueue<one_cdc> &csums_queue, DataQueue<uint32_t> &crc32_queue) {
    this->chash_table = new crc_to_chunks_map();

    while (!csums_queue.isDone()) {
        one_cdc cdc = csums_queue.pop();
        if (this->chash_table->contains(cdc.weak_hash)) {
            // insert the new cdc to the vector
            struct ol ol_item = {cdc.offset, cdc.length};
            (*this->chash_table)[cdc.weak_hash].push_back(ol_item);
        } else {
            // create a new vector and insert it
            std::vector<ol> ol_vec{{cdc.offset, cdc.length}};
            this->chash_table->emplace(cdc.weak_hash, std::move(ol_vec));
            crc32_queue.push(cdc.weak_hash);
        }
    }
    crc32_queue.setDone();
}

void ClientSyncWorker::compare_sha1(int fd, DataQueue<matched_item_rpc> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {
    uint64_t strong_matched_nums = 0;
    while(!weak_matched_chunks_queue.isDone()) {
        struct matched_item_rpc mc_item = weak_matched_chunks_queue.pop();
        if (mc_item.new_ol.length == 0 && mc_item.new_ol.offset == 0)
                continue;
        uint64_t bytes_read = 0;
        uint8_t hash[BLAKE3_OUT_LEN];
        
        uint8_t *file_buf = (uint8_t *)mi_malloc(mc_item.new_ol.length);
        assert(file_buf != NULL);

        lseek(fd, mc_item.new_ol.offset, SEEK_SET);

        bytes_read = read(fd, file_buf, mc_item.new_ol.length);
        assert(bytes_read == mc_item.new_ol.length);

        calc_blake3(hash, file_buf, mc_item.new_ol.length);
        if(memcmp(hash, mc_item.hash, BLAKE3_OUT_LEN) == 0) {
            mc_item.item_nums = 1;
            memset(mc_item.hash, 0, BLAKE3_OUT_LEN);
            strong_matched_chunks_queue.push(mc_item);
            strong_matched_nums += 1;
        }
        mi_free(file_buf);
    }
    if (strong_matched_nums == 0) {
        struct matched_item_rpc mc_item;
        mc_item.item_nums = 0;
        mc_item.new_ol = {0, 0};
        strong_matched_chunks_queue.push(mc_item);
    }

    strong_matched_chunks_queue.setDone();
}

void ClientSyncWorker::compare_sha1_1(int fd, DataQueue<matched_item_rpc_1> &weak_matched_chunks_queue, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {
    uint64_t strong_matched_nums = 0;

    while (!weak_matched_chunks_queue.isDone()) {
        struct matched_item_rpc_1 mc_item = weak_matched_chunks_queue.pop();
        if (mc_item.weak_hash == 0)
            continue;
        
        // the weak hash is the key of the hash table of new file, so we can find the matched
        auto it = this->chash_table->find(mc_item.weak_hash);

        #ifdef SIZE_TEST
            // record the crc32c and the same chunks which have the same crc32c
            same_crc32c_chunks.push_back(it->second.size());
        #endif

        // iterate the vector of the matched weak hash to get the offset and length
        for (auto &ol_item : it->second) {
            uint64_t offset = ol_item.offset;
            uint64_t length = ol_item.length;
            uint8_t hash[BLAKE3_OUT_LEN];
            uint8_t *file_buf = (uint8_t *)mi_malloc(length);

            lseek(fd, offset, SEEK_SET);

            uint64_t bytes_read = read(fd, file_buf, length);
            assert(bytes_read == length);
            calc_blake3(hash, file_buf, length);

            auto sha_it = mc_item.sha_to_chunk_map.find(std::string((char *)hash, BLAKE3_OUT_LEN));

            /* check if the sha hash is already in the map
            if yes, there is a matched chunk (identified by the sha hash) */
            if (sha_it != mc_item.sha_to_chunk_map.end()) {
                matched_item_rpc mc_item_1;
                mc_item_1.item_nums = 1;
                mc_item_1.new_ol = {offset, length};
                mc_item_1.old_ol = {sha_it->second.offset, sha_it->second.length};
                memset(mc_item_1.hash, 0, BLAKE3_OUT_LEN);
                mc_item_1.end_of_stream = false;
                strong_matched_chunks_queue.push(mc_item_1);
                strong_matched_nums += 1;
            }
            mi_free(file_buf);
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
}

data_cmd ClientSyncWorker::create_data_cmd(int fd, int cmd_flag, uint64_t offset, uint64_t length) {
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

void ClientSyncWorker::sort_matched_chunks(DataQueue<matched_item_rpc> &matched_chunks_queue) {
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

void ClientSyncWorker::generate_delta(int new_fd, DataQueue<matched_item_rpc> &strong_matched_chunks_queue, DataQueue<data_cmd> &data_cmd_queue) {
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