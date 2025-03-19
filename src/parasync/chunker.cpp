#include <assert.h>
#include <nmmintrin.h>
#include <chrono>
#include <isa-l/crc.h>
#include "chunker.h"

// Define thread-local variables
thread_local uint64_t Chunker::offset;
thread_local uint64_t Chunker::start_offset;
thread_local uint64_t Chunker::end_offset;

thread_local uint64_t Chunker::tls_fingerprint;
thread_local unsigned char* Chunker::tls_map_t;
thread_local unsigned char* Chunker::tls_pre;
thread_local uint64_t Chunker::tls_FING_GEAR_08KB_64;
thread_local uint64_t Chunker::tls_GEARv2_tmp[256];

struct bitmap* bitmap_create(uint64_t fs) {
    struct bitmap* bm = (struct bitmap*)mi_zalloc(sizeof(struct bitmap));
    assert(bm);
    bm->size = fs;
    bm->bits = (uint8_t*)mi_zalloc((fs + 7) / 8);
    assert(bm->bits);
    return bm;
}

void bitmap_set(struct bitmap* bm, uint64_t index) {
    if (!bm || !bm->bits || index >= bm->size) return;
    uint64_t byte_index = index >> 3;
    uint64_t bit_index = 7 - (index % 8);  // Reverse bit order within byte
    bm->bits[byte_index] |= (1U << bit_index);
}

void bitmap_clear(struct bitmap* bm, uint64_t index) {
    if (!bm || !bm->bits || index >= bm->size) return;
    uint64_t byte_index = index / 8;
    uint64_t bit_index = 7 - (index % 8);  // Reverse bit order within byte
    bm->bits[byte_index] &= ~(1U << bit_index);
}

uint8_t bitmap_test(struct bitmap* bm, uint64_t index) {
    if (!bm || !bm->bits || index >= bm->size) return 0;
    uint64_t byte_index = index / 8;
    uint64_t bit_index = 7 - (index % 8);  // Reverse bit order within byte
    return (bm->bits[byte_index] >> bit_index) & 1U;
}

void bitmap_destroy(struct bitmap* bm) {
    if (bm) {
        mi_free(bm->bits);
        mi_free(bm);
    }
}

void PDSyncChunker::init(const char *map, uint64_t off_t, uint64_t length) {
    seg = (csegment *)mi_zalloc(sizeof(csegment));
    seg->offset = off_t;
    seg->length = length;
    seg->map = (char *)mi_zalloc(seg->length);
    memcpy(seg->map, map + seg->offset, seg->length);

    offset = seg->offset;
    start_offset = seg->offset;
    end_offset = seg->offset + seg->length;

    tls_fingerprint = 0;
    tls_map_t = (unsigned char*)seg->map;
    tls_pre = (unsigned char*)seg->map;
    tls_FING_GEAR_08KB_64 = 0x0000d93003530000ULL;
    for (int i = 0; i < 256; i++) {
        tls_GEARv2_tmp[i] = GEARv2[i];
    }
}

void PDSyncChunker::run(struct bitmap* bm) {
    auto start_1 = std::chrono::high_resolution_clock::now();
    while (offset < end_offset) {
        tls_fingerprint = (tls_fingerprint << 1) + (tls_GEARv2_tmp[tls_map_t[0]]);
        if (!(tls_fingerprint & tls_FING_GEAR_08KB_64)) {
            // bitmap_set(seg->bm, offset - start_offset);
            bitmap_set(bm, offset);
        }
        offset += 1;
        tls_map_t += 1;
    }

    std::chrono::duration<double> diff = std::chrono::high_resolution_clock::now() - start_1;
    printf("core used,%d\n", sched_getcpu());
    printf("PDSyncChunker time,%f\n", diff.count());
}

csegment* PDSyncChunker::task(struct bitmap* bm) {
    this->run(bm);
    return seg;
}

void ParaSyncChunker::init(const char *map, uint64_t off_t, uint64_t length) {
    seg = (csegment *)mi_zalloc(sizeof(csegment));
    seg->offset = off_t;
    seg->length = length;
    seg->map = (char *)mi_zalloc(seg->length);
    memcpy(seg->map, map + seg->offset, seg->length);
        
    seg->csums_queue = new DataQueue<one_cdc>();
        
    offset = seg->offset;
    start_offset = seg->offset;
    end_offset = seg->offset + seg->length;
    
    tls_fingerprint = 0;
    tls_map_t = (unsigned char*)seg->map;
    tls_pre = (unsigned char*)seg->map;
    tls_FING_GEAR_08KB_64 = 0x0000d93003530000ULL;
    for (int i = 0; i < 256; i++) {
        tls_GEARv2_tmp[i] = GEARv2[i];
    }
}

void ParaSyncChunker::run() {
    auto start_1 = std::chrono::high_resolution_clock::now();
    DataQueue<one_cdc> *csums_queue = seg->csums_queue;
    uint64_t pre_offset = offset;

    while (offset < end_offset) {
        tls_fingerprint = (tls_fingerprint << 1) + (tls_GEARv2_tmp[tls_map_t[0]]);
        if (!(tls_fingerprint & tls_FING_GEAR_08KB_64)) {
            struct one_cdc cdc = {
                .offset = pre_offset,
                .length = offset - pre_offset,
                .weak_hash = crc32_isal((uint8_t *)tls_pre, offset - pre_offset, 0)
            };
            tls_pre = tls_map_t;
            pre_offset = offset;
            csums_queue->push(cdc);
        }
        offset += 1;
        tls_map_t += 1;
    }

    if (pre_offset < end_offset) {
        struct one_cdc cdc = {
            .offset = pre_offset,
            .length = end_offset - pre_offset,
            .weak_hash = crc32_isal((uint8_t *)tls_pre, end_offset - pre_offset, 0)
        };
        csums_queue->push(cdc);
    }
    csums_queue->setDone();
    
    std::chrono::duration<double> diff = std::chrono::high_resolution_clock::now() - start_1;
    printf("core used,%d\n", sched_getcpu());
    printf("ParaSyncChunker time,%f\n", diff.count());
}

csegment* ParaSyncChunker::task() {
    this->run();
    return seg;
}

void pdsync_detector(std::unique_ptr<csegment*[]>& seg_array, char *map, uint64_t fs, struct bitmap *bm, std::unique_ptr<DataQueue<one_cdc>>& csums_queue) {

    uint64_t chunk_num = 0;
    uint64_t offset = 0;
    uint64_t i = MinSize;

    char *map_t = map;
    
    while (i < fs) {
            if (bitmap_test(bm, i) && (i - offset) >= MinSize) {
                struct one_cdc cdc = {
                    .offset = offset,
                    .length = i - offset,
                    .weak_hash = crc32_isal((uint8_t *)map_t + offset, i - offset, 0)
                };

                // printf("offset: %ld, length: %ld\n", offset, i - offset);
                csums_queue->push(cdc);
                chunk_num += 1;
                offset = i;
                i += MinSize;
            }
            else
                i += 1;
    }

    if (offset < fs) {
        struct one_cdc cdc = {
            .offset = offset,
            .length = fs - offset,
            .weak_hash = crc32_isal((uint8_t *)map_t + offset, fs - offset, 0)
        };
        csums_queue->push(cdc);
        chunk_num += 1;
    }

    csums_queue->setDone();
}

void parasync_detector(std::unique_ptr<csegment*[]>& seg_array, char *map, int thread_num, uint64_t fs, std::unique_ptr<DataQueue<one_cdc>>& total_csums_queue) {
    uint64_t chunk_num = 0;
    uint32_t combined_hash = 0;
    uint64_t total_length = 0;
    uint64_t start_offset = 0;
    bool first = true;

    for (int i = 0; i < thread_num; i++) {
        DataQueue<one_cdc> *csums_queue = seg_array[i]->csums_queue;
        
        while (!csums_queue->isDone()) {
            struct one_cdc curr_cdc = csums_queue->pop();

            if (first) {
                start_offset = curr_cdc.offset;
                first = false;
            }

            total_length += curr_cdc.length;
            combined_hash = crc32_comb(combined_hash, curr_cdc.weak_hash, curr_cdc.length);
            
            // Push combined chunk only when size threshold met
            if (total_length >= MinSize) {
                struct one_cdc combined = {
                    .offset = start_offset,
                    .length = total_length,
                    .weak_hash = combined_hash
                };
                total_csums_queue->push(combined);
                chunk_num += 1;

                // Reset for the next chunk
                total_length = 0;
                combined_hash = 0;
                first = true;
            }
        }

        delete csums_queue;
    }

    // Handle any remaining data after all segments processed
    if (total_length > 0) {
        struct one_cdc combined = {
            .offset = start_offset,
            .length = total_length,
            .weak_hash = combined_hash
        };
        total_csums_queue->push(combined);
        chunk_num += 1;
    }

    total_csums_queue->setDone();
}