#include <assert.h>
#include <chrono>
#include "blake3.h"
#include "smatcher.h"

thread_local uint8_t* PDSyncSmatcher::tls_buffer = nullptr;
thread_local size_t PDSyncSmatcher::tls_buffer_size = 0;
thread_local uint8_t* ParaSyncSmatcher::tls_buffer = nullptr;
thread_local size_t ParaSyncSmatcher::tls_buffer_size = 0;

void PDSyncSmatcher::compare_strong_hash(char *map, std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *matched_weak_hash, size_t matched_size, std::unique_ptr<tbb_blake_to_chunk_map> &strong_hash_table, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {
    uint64_t strong_matched_nums = 0;

    for (size_t i = 0; i < matched_size; i++) {
        uint32_t wh = matched_weak_hash[i];

        auto it = chash_table->find(wh);

        for (auto &item : it->second) {
            uint64_t offset = item.offset;
            uint64_t length = item.length;
            uint8_t hash[BLAKE3_OUT_LEN];
            
            if (tls_buffer_size < length) {
                tls_buffer = (uint8_t *)mi_realloc(tls_buffer, length);
                tls_buffer_size = length;
            }

            memcpy(tls_buffer, map + offset, length);
            calc_blake3(hash, tls_buffer, length);

            auto strong_it = strong_hash_table->find(std::string((char *)hash, BLAKE3_OUT_LEN));

            if (strong_it != strong_hash_table->end()) {
                struct matched_item_rpc mc_item;
                mc_item.item_nums = 1;
                mc_item.new_ol = {offset, length};
                mc_item.old_ol = {strong_it->second.offset, strong_it->second.length};
                memset(mc_item.hash, 0, BLAKE3_OUT_LEN);
                mc_item.end_of_stream = false;
                strong_matched_chunks_queue.push(mc_item);
                strong_matched_nums += 1;
            }
        }
    }

    strong_matched_chunks_queue.setDone();
}

void ParaSyncSmatcher::compare_strong_hash(char* map, std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *matched_weak_hash, size_t matched_size, std::unique_ptr<tbb_blake_to_chunk_map> &strong_hash_table, DataQueue<matched_item_rpc> &strong_matched_chunks_queue) {
    uint64_t strong_matched_nums = 0;

    for (size_t i = 0; i < matched_size; i++) {
        uint32_t wh = matched_weak_hash[i];

        auto it = chash_table->find(wh);

        for (auto &item : it->second) {
            uint64_t offset = item.offset;
            uint64_t length = item.length;
            uint8_t hash[BLAKE3_OUT_LEN];
            if (tls_buffer_size < length) {
                tls_buffer = (uint8_t *)mi_realloc(tls_buffer, length);
                tls_buffer_size = length;
            }
            memcpy(tls_buffer, map + offset, length);
            calc_blake3(hash, tls_buffer, length);

            auto strong_it = strong_hash_table->find(std::string((char *)hash, BLAKE3_OUT_LEN));

            if (strong_it != strong_hash_table->end()) {
                struct matched_item_rpc mc_item;
                mc_item.item_nums = 1;
                mc_item.new_ol = {offset, length};
                mc_item.old_ol = {strong_it->second.offset, strong_it->second.length};
                memset(mc_item.hash, 0, BLAKE3_OUT_LEN);
                mc_item.end_of_stream = false;
                strong_matched_chunks_queue.push(mc_item);
                strong_matched_nums += 1;
            }
        }
    }

    strong_matched_chunks_queue.setDone();
}

void ParaSyncSmatcher::calculate(char* map, ol *new_ols, size_t size, std::shared_ptr<tbb_blake_to_chunk_map> strong_hash_table, oneapi::tbb::concurrent_vector<matched_item_rpc> results) {
    
    for (size_t i = 0; i < size; i++) {
        auto item = new_ols[i];
        uint64_t offset = item.offset;
        uint64_t length = item.length;
        uint8_t hash[BLAKE3_OUT_LEN];
        if (tls_buffer_size < length) {
            tls_buffer = (uint8_t *)mi_realloc(tls_buffer, length);
            tls_buffer_size = length;
        }

        memcpy(tls_buffer, map + offset, length);
        calc_blake3(hash, tls_buffer, length);

        auto strong_it = strong_hash_table->find(std::string((char *)hash, BLAKE3_OUT_LEN));

        if (strong_it != strong_hash_table->end()) {
            struct matched_item_rpc mc_item;
            mc_item.item_nums = 1;
            mc_item.new_ol = {offset, length};
            mc_item.old_ol = {strong_it->second.offset, strong_it->second.length};
            memset(mc_item.hash, 0, BLAKE3_OUT_LEN);
            mc_item.end_of_stream = false;
            results.push_back(mc_item);
        }
    }
}