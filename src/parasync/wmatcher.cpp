#include <assert.h>
#include <chrono>
#include "blake3.h"
#include "wmatcher.h"

thread_local uint8_t* PDSyncWmatcher::tls_buffer = nullptr;
thread_local size_t PDSyncWmatcher::tls_buffer_size = 0;
thread_local uint8_t* ParaSyncWmatcher::tls_buffer = nullptr;
thread_local size_t ParaSyncWmatcher::tls_buffer_size = 0;

void PDSyncWmatcher::compare_weak_hash(char *map, std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *weak_hash, size_t size, DataQueue<matched_item_rpc_1> &matched_chunks_queue) {
    uint64_t matched_nums = 0;

    for (size_t i = 0; i < size; i++) {
        uint32_t wh = weak_hash[i];

        // check if the weak hash is already in the hash table of the old file (this->chash_table)
        auto it = chash_table->find(wh);
        if (it != chash_table->end()) {
            struct matched_item_rpc_1 mc_item;
            mc_item.weak_hash = wh;

            for (auto &ol_item : it->second) {
                uint64_t offset = ol_item.offset;
                uint64_t length = ol_item.length;
                uint8_t hash[BLAKE3_OUT_LEN];
                if (tls_buffer_size < length) {
                    tls_buffer = (uint8_t *)mi_realloc(tls_buffer, length);
                    tls_buffer_size = length;
                }

                memcpy(tls_buffer, map + offset, length);
                calc_blake3(hash, tls_buffer, length);

                auto sha_it = mc_item.sha_to_chunk_map.find(std::string((char *)hash, BLAKE3_OUT_LEN));

                /* check if the stong hash is already in the map
                if not, insert the new strong hash to the map if yes, do nothing */
                if (sha_it == mc_item.sha_to_chunk_map.end()) {
                    struct ol tmp_ol = {offset, length};
                    mc_item.sha_to_chunk_map.emplace(
                        std::string((char *)hash, BLAKE3_OUT_LEN), 
                        std::move(tmp_ol)
                    );
                }
            }

            mc_item.item_nums = mc_item.sha_to_chunk_map.size();
            mc_item.end_of_stream = false;
            matched_chunks_queue.push(mc_item);
            matched_nums += 1;
        }
    }

    matched_chunks_queue.setDone();
}

void ParaSyncWmatcher::compare(std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *weak_hash, size_t size, oneapi::tbb::concurrent_vector<wmatched_item> &matched_chunks) {

    for (size_t i = 0; i < size; i++) {
        uint32_t wh = weak_hash[i];

        auto it = chash_table->find(wh);
        if (it != chash_table->end()) {
            for (auto &ol_item : it->second) {
                ol tmp_ol = {
                    .offset = ol_item.offset,
                    .length = ol_item.length,
                };
                matched_chunks.push_back({
                    .weak_hash = wh,
                    .old_ol = tmp_ol,
                });
            }
        }
    }
}

void ParaSyncWmatcher::compare(std::unique_ptr<tbb_crc_to_chunks_map> &chash_table, uint32_t *weak_hash, size_t size, std::vector<wmatched_item> &matched_chunks) {

    for (size_t i = 0; i < size; i++) {
        uint32_t wh = weak_hash[i];

        auto it = chash_table->find(wh);
        if (it != chash_table->end()) {
            for (auto &ol_item : it->second) {
                matched_chunks.push_back({
                    .weak_hash = wh,
                    .old_ol = {
                        .offset = ol_item.offset,
                        .length = ol_item.length,
                    },
                });
            }
        }
    }
}

void ParaSyncWmatcher::calculate(char* map, wmatched_item *wmatched_chunks, size_t size, DataQueue<wmatched_item> &wmatched_item_queue) {

    for (size_t i = 0; i < size; i++) {
        uint64_t offset = wmatched_chunks[i].old_ol.offset;
        uint64_t length = wmatched_chunks[i].old_ol.length;
        if (tls_buffer_size < length) {
            tls_buffer = (uint8_t *)mi_realloc(tls_buffer, length);
            tls_buffer_size = length;
        }
        memcpy(tls_buffer, map + offset, length);
        calc_blake3(wmatched_chunks[i].hash, tls_buffer, length);
        wmatched_item_queue.push(wmatched_chunks[i]);
    }

    wmatched_item_queue.setDone();
}

void ParaSyncWmatcher::post_calculate(DataQueue<wmatched_item> *wmatched_item_queue, int thread_num, DataQueue<matched_item_rpc_1> &matched_chunks_queue) {

    post_calc_map tmp_table;

    for (int i = 0; i < thread_num; i++) {
        DataQueue<wmatched_item> *local_queue = wmatched_item_queue + i;
        while (!local_queue->isDone()) {
            wmatched_item item = local_queue->pop();

            auto it = tmp_table.find(item.weak_hash);
            if (it != tmp_table.end()) {
                auto sha_it = it->second.find(std::string((char *)item.hash, BLAKE3_OUT_LEN));
                if (sha_it == it->second.end()) {
                    it->second.emplace(
                        std::string((char *)item.hash, BLAKE3_OUT_LEN), 
                        std::move(item.old_ol)
                    );
                }
            } else {
                std::unordered_map<std::string, ol> sha_map;
                sha_map.emplace(
                    std::string((char *)item.hash, BLAKE3_OUT_LEN), 
                    std::move(item.old_ol)
                );
                tmp_table.insert({item.weak_hash, std::move(sha_map)});
            }
        }
    }

    for (auto &item : tmp_table) {
        matched_item_rpc_1 mc_item;
        mc_item.weak_hash = item.first;
        mc_item.item_nums = item.second.size();
        mc_item.end_of_stream = false;
        mc_item.sha_to_chunk_map = std::move(item.second);
        matched_chunks_queue.push(std::move(mc_item));
    }
}

void ParaSyncWmatcher::post_calculate(DataQueue<wmatched_item> &wmatched_item_queue, int thread_num, DataQueue<matched_item_rpc_1> &matched_chunks_queue) {

    post_calc_map tmp_table;

    while (!wmatched_item_queue.isDone()) {
        wmatched_item item = wmatched_item_queue.pop();

        auto it = tmp_table.find(item.weak_hash);
        if (it != tmp_table.end()) {
            auto sha_it = it->second.find(std::string((char *)item.hash, BLAKE3_OUT_LEN));
            if (sha_it == it->second.end()) {
                it->second.emplace(
                    std::string((char *)item.hash, BLAKE3_OUT_LEN), 
                    std::move(item.old_ol)
                );
            }
        } else {
            std::unordered_map<std::string, ol> sha_map;
            sha_map.emplace(
                std::string((char *)item.hash, BLAKE3_OUT_LEN), 
                std::move(item.old_ol)
            );
            tmp_table.insert({item.weak_hash, std::move(sha_map)});
        }
    }

    for (auto &item : tmp_table) {
        matched_item_rpc_1 mc_item;
        mc_item.weak_hash = item.first;
        mc_item.item_nums = item.second.size();
        mc_item.end_of_stream = false;
        mc_item.sha_to_chunk_map = std::move(item.second);
        matched_chunks_queue.push(std::move(mc_item));
    }
}