#include <stdlib.h>
#include <stdio.h>
#include <isa-l/crc.h>
#include <isa-l/crc64.h>
#include <isa-l_crypto/rolling_hashx.h>
#include <nmmintrin.h>
#include <chrono>
#include "fastcdc.h"
#include "crc32.h"
#include "crc32c.h"

#include <string.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <isa-l_crypto.h>

#include "blake3.h"
#include "blake2.h"
#include "adler_rollsum.h"
#include "sync_common.h"

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

const uint32_t DefaultMinChunkSize = 4*1024;
const uint32_t DefaultChunkSize = 8*1024;
const uint32_t DefaultMaxChunkSize = 12*1024;

const uint32_t DefaultWindowSize = 4*1024;

// SS-CDC
// static uint32_t DefaultWindowSize = 64;
static uint32_t magic_number = 0;
// static uint32_t break_mask_bit = 14; // 14-bit mask for 16 KB
static uint32_t break_mask_bit = 13; // 13-bit mask for 8 KB
static uint32_t break_mask = ((1u << break_mask_bit) - 1) << (32 - break_mask_bit);
// (fingerprint & break_mask) == magic_number -> break point

// Seafile
// #define BREAK_VALUE     0x0013    ///0x0513 // 8 KB
// #define BLOCK_SZ        (1024*1024*1)
// #define BLOCK_MASK      (BLOCK_SZ - 1)
// Determine the break point
// (fingerprint & BlOCK_MASK) == (BREAK_VALUE & BLOCK_MASK) -> break point

void drop_cache()
{
    int r = system("sync && sysctl -w vm.drop_caches=3");
    // int cache_fd;
    // sync();
    // cache_fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
    // write(cache_fd, "3", 1);
    // close(cache_fd);
}

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <file> <1 for mmap and 0 for read>\n", argv[0]);
        return 1;
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }

    int is_mmap = atoi(argv[2]);

    uint64_t NumBytes = file_size(fd);

    char *map = NULL;

    if (is_mmap) {
        printf("Using mmap\n");
        map = (char *)map_file(fd);
    }
    else {
        printf("Reading file\n");
        map = (char *)malloc(NumBytes);
        if (!map) {
            perror("malloc");
            close(fd);
            return 1;
        }
        ssize_t bytes_read = 0;
        uint64_t total_read = 0;
        while (total_read < NumBytes) {
            bytes_read = read(fd, map + total_read, NumBytes - total_read);
            if (bytes_read < 0) {
                perror("read");
                free(map);
                close(fd);
                return 1;
            }
            if (bytes_read == 0) break;
            total_read += bytes_read;
        }
    }
    
    DataQueue<uint32_t> queue;

    printf("File size: %lu MB\n", NumBytes / (1024 * 1024));

    // printf("Weak Hash Tests:\n");
    // printf("Fixed chunk size: %u KB\n", DefaultChunkSize / 1024);

    // auto test_checksum = [NumBytes, map](const char* name, auto checksum_fn) {
    //     auto start = std::chrono::high_resolution_clock::now();
    //     uint32_t crc = 0;
    //     for (uint64_t i = 0; i < NumBytes; i += DefaultChunkSize) {
    //         uint32_t chunk_size = (i + DefaultChunkSize <= NumBytes) ? 
    //                              DefaultChunkSize : (NumBytes - i);
    //         crc = checksum_fn(crc, (const unsigned char*)map + i, chunk_size);
    //     }
    //     auto end = std::chrono::high_resolution_clock::now();
    //     auto duration = std::chrono::duration<double>(end - start).count();
    //     printf("%s: CRC32=%08X, %.3fs, %.3f MB/s\n",
    //            name, crc, duration, (NumBytes / (1024*1024)) / duration);
    //     return crc;
    // };

    // // Test different checksum implementations
    // test_checksum("ISA-L  gzip ", crc32_gzip_refl);
    // test_checksum("Adler  HW   ", crc32c_hw);

    printf("\nRolling Tests:\n");
    printf("Window size: %u bytes\n", DefaultWindowSize);
    printf("MinChunkSize, AverageChunkSize, MaxChunkSize: %u, %u, %u\n", 
           DefaultMinChunkSize, DefaultChunkSize, DefaultMaxChunkSize);

    // SS-CDC rolling hash
    drop_cache();
    queue.init();
    auto start = std::chrono::high_resolution_clock::now();
    uint32_t ss_crc = crc32c_hw(0, (const unsigned char*)map, DefaultWindowSize);
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        ss_crc = doCrc_rolling(ss_crc, map[i + DefaultWindowSize], map[i]);
        if ((ss_crc & break_mask) == magic_number) {
            // i += DefaultMinChunkSize;
            queue.push(ss_crc);
            continue;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto diff = std::chrono::duration<double>(end - start).count();
    printf("CRC :  hash=%08X, %.3fs, %.3f MB/s\n",
           ss_crc, diff, (NumBytes / (1024*1024)) / diff);
    
    // Rolling rsync checksum
    drop_cache();
    queue.init();
    start = std::chrono::high_resolution_clock::now();
    Rollsum sum;
    RollsumInit(&sum);
    uint32_t rollsum = 0;

    RollsumUpdate(&sum, (const unsigned char*)map, DefaultWindowSize);
    rollsum = RollsumDigest(&sum);

    // Main rolling loop 
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        RollsumRotate(&sum, map[i], map[i + DefaultWindowSize]);
        rollsum = RollsumDigest(&sum);
        if ((rollsum & break_mask) == magic_number) {
            // i += DefaultMinChunkSize;
            queue.push(rollsum);
            continue;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("Adler :  hash=%08X, %.3fs, %.3f MB/s\n",
           rollsum, diff, (NumBytes / (1024*1024)) / diff);

    // Gear hash
    drop_cache();
    DataQueue<uint64_t> gear_q;
    gear_q.init();
    start = std::chrono::high_resolution_clock::now();
    uint64_t fastfp = 0;
    for (uint64_t i = 0; i < DefaultWindowSize; i++) {
        fastfp += GEARv2[static_cast<uint8_t>(map[i])];
    }
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        fastfp = (fastfp << 1) + GEARv2[static_cast<uint8_t>(map[i + DefaultWindowSize])];
        if ((fastfp & break_mask) == magic_number) {
            // i += DefaultMinChunkSize;
            gear_q.push(fastfp);
            continue;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    uint32_t gear = fastfp;
    printf("Gear  :  hash=%08X, %.3fs, %.3f MB/s\n",
        gear, diff, (NumBytes / (1024*1024)) / diff);
    
    // isal_rolling_hash2
    // struct isal_rh_state2 *state;
    // uint32_t mask, trigger, offset = 0;
    // int ret = 0;
    // state = (struct isal_rh_state2 *)malloc(sizeof(struct isal_rh_state2));
    // isal_rolling_hash2_init(state, 0);
    // for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
    //     isal_rolling_hash2_run(state, (const unsigned char*)map + i, DefaultWindowSize, 0, 0, &offset, &ret);
    //     if ((ret & break_mask) == magic_number) {
    //         // i += DefaultMinChunkSize;
    //         continue;
    //     }
    // }
    // end = std::chrono::high_resolution_clock::now();
    // diff = std::chrono::duration<double>(end - start).count();
    // printf("isal_rolling_hash2      :  %.3fs, %.3f MB/s\n",
    //     diff, (NumBytes / (1024*1024)) / diff);

    // Intel ISA-L rolling hash
    drop_cache();
    // queue.init();
    struct isal_rh_state2 *state;
    int ret;
    uint64_t remain;
    uint32_t mask, trigger, offset = 0;
    uint32_t min_chunk, max_chunk, mean_chunk;
    min_chunk = 4096;
    mean_chunk = 8192;
    max_chunk = 12288;
    ret = isal_rolling_hashx_mask_gen(mean_chunk, 0, &mask);
    if (ret != 0)
    {
        printf("Error: isal_rolling_hashx_mask_gen failed\n");
        return 1;
    }
    trigger = rand() & mask;

    if (posix_memalign((void **)&state, 16, sizeof(struct isal_rh_state2)) != 0)
    {
        printf("Error: posix_memalign failed\n");
        return 1;
    }
    isal_rolling_hash2_init(state, DefaultWindowSize);

    start = std::chrono::high_resolution_clock::now();
    remain = NumBytes;
    const uint8_t* data_ptr = (const uint8_t*)map;
    while (remain > 0)
    {
        uint32_t chunk = (remain > max_chunk) ? max_chunk : remain;
        isal_rolling_hash2_reset(state, data_ptr + min_chunk - DefaultWindowSize);
        isal_rolling_hash2_run(state, data_ptr + min_chunk, chunk - min_chunk, mask, trigger, &offset, &ret);
        data_ptr += (offset + min_chunk);
        remain -= (offset + min_chunk);
        queue.push(1);
    }
    free(state);
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("Hardware rolling ISA-L :  %.3fs, %.3f MB/s\n",
            diff, (NumBytes / (1024*1024)) / diff);
    // drop_cache();

    printf("\nStrong Hash Tests:\n");
    printf("Fixed chunk size: %u KB\n", DefaultChunkSize / 1024);

    uint8_t hash_sha1[SHA256_DIGEST_LENGTH];
    // OpenSSL old SHA1 for fixed 8 KB chunks
    // start = std::chrono::high_resolution_clock::now();
    // for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    //     memset(hash_sha1, 0, SHA256_DIGEST_LENGTH);
    //     SHA1((const unsigned char*)map + i, DefaultChunkSize, hash_sha1);
    //     // SHA_CTX sha_ctx;
    //     // SHA1_Init(&sha_ctx);
    //     // SHA1_Update(&sha_ctx, (const unsigned char*)map + i, DefaultChunkSize);
    //     // SHA1_Final(hash_sha1, &sha_ctx);
    // }
    // end = std::chrono::high_resolution_clock::now();
    // diff = std::chrono::duration<double>(end - start).count();
    // printf("OpenSSL old SHA1       :  %.3fs, %.3f MB/s\n",
    //        diff, (NumBytes / (1024*1024)) / diff);

    // OpenSSL new SHA1 for fixed 8 KB chunks
    drop_cache();
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(hash_sha1, 0, SHA256_DIGEST_LENGTH);
        EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
        EVP_DigestInit_ex(mdctx, EVP_sha1(), NULL);
        EVP_DigestUpdate(mdctx, (const unsigned char*)map + i, DefaultChunkSize);
        EVP_DigestFinal_ex(mdctx, hash_sha1, NULL);
        EVP_MD_CTX_free(mdctx);
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("OpenSSL new SHA1       :  %.3fs, %.3f MB/s\n",
           diff, (NumBytes / (1024*1024)) / diff);

    uint8_t hash[SHA256_DIGEST_LENGTH];

    // OpenSSL old SHA256 for fixed 8 KB chunks
    // start = std::chrono::high_resolution_clock::now();
    // for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    //     memset(hash, 0, SHA256_DIGEST_LENGTH);
    //     SHA256_CTX sha256_ctx;
    //     SHA256_Init(&sha256_ctx);
    //     SHA256_Update(&sha256_ctx, (const unsigned char*)map + i, DefaultChunkSize);
    //     SHA256_Final(hash, &sha256_ctx);
    // }
    // end = std::chrono::high_resolution_clock::now();
    // diff = std::chrono::duration<double>(end - start).count();
    // printf("OpenSSL old SHA256     :  %.3fs, %.3f MB/s\n",
    //         diff, (NumBytes / (1024*1024)) / diff);

    // OpenSSL new SHA256 for fixed 8 KB chunks
    drop_cache();
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(hash, 0, SHA256_DIGEST_LENGTH);
        EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
        EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);
        EVP_DigestUpdate(mdctx, (const unsigned char*)map + i, DefaultChunkSize);
        EVP_DigestFinal_ex(mdctx, hash, NULL);
        EVP_MD_CTX_free(mdctx);
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("OpenSSL new SHA256     :  %.3fs, %.3f MB/s\n",
            diff, (NumBytes / (1024*1024)) / diff);
    
    // ISA-L SHA256 for fixed 8 KB chunks
    drop_cache();
    uint32_t mh_sha256_digest[ISAL_SHA256_DIGEST_WORDS];
    // struct isal_mh_sha256_ctx *ctx = nullptr;
    struct isal_mh_sha256_ctx ctx;

    start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(mh_sha256_digest, 0, ISAL_SHA256_DIGEST_WORDS);
        // ctx = (struct isal_mh_sha256_ctx *)malloc(sizeof(struct isal_mh_sha256_ctx));
        isal_mh_sha256_init(&ctx);
        isal_mh_sha256_update(&ctx, (const unsigned char*)map + i, DefaultChunkSize);
        isal_mh_sha256_finalize(&ctx, mh_sha256_digest);
        // free(ctx);
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("ISA-L SHA256 mh_sha256 :  %.3fs, %.3f MB/s\n",
           diff, (NumBytes / (1024 * 1024)) / diff);
    
    // ISA-L SHA256 multi-buffer manager for fixed 8 KB chunks
    ISAL_SHA256_HASH_CTX_MGR mgr;
    ISAL_SHA256_HASH_CTX ctx_2[4];
    ISAL_SHA256_HASH_CTX *ctx_out = NULL;

    // Initialize the SHA256 multi-buffer manager
    // isal_sha256_ctx_mgr_init(&mgr);
    // uint64_t half_default_chunk_size = DefaultChunkSize / 2;
    // start = std::chrono::high_resolution_clock::now();
    // uint64_t i = 0;
    // while (i + DefaultChunkSize < NumBytes) {
    //     isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[0], &ctx_out, (const unsigned char*)map + i, half_default_chunk_size, ISAL_HASH_FIRST);
    //     isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[1], &ctx_out, (const unsigned char*)map + i + half_default_chunk_size, half_default_chunk_size, ISAL_HASH_ENTIRE);
    //     isal_sha256_ctx_mgr_flush(&mgr, &ctx_out);
    //     i += DefaultChunkSize;
    // }
    // end = std::chrono::high_resolution_clock::now();
    // diff = std::chrono::duration<double>(end - start).count();
    // printf("ISA-L SHA256 ctx_mgr   :  %.3fs, %.3f MB/s\n",
    //           diff, (NumBytes / (1024 * 1024)) / diff);
    
    // Blake3 for fixed 8 KB chunks
    // alignas(32) uint8_t b3_hash[SHA256_DIGEST_LENGTH];
    // start = std::chrono::high_resolution_clock::now();
    // for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    //     blake3_hasher hasher;
    //     blake3_hasher_init(&hasher);
    //     blake3_hasher_update(&hasher, (const unsigned char*)map + i, DefaultChunkSize);
    //     blake3_hasher_finalize(&hasher, b3_hash, SHA256_DIGEST_LENGTH);
    // }
    // end = std::chrono::high_resolution_clock::now();
    // diff = std::chrono::duration<double>(end - start).count();
    // printf("Blake3                 :  %.3fs, %.3f MB/s\n",
    //           diff, (NumBytes / (1024 * 1024)) / diff);

    if (is_mmap) {
        unmap_file(fd, map);
    } else {
        free(map);
    }
    // unmap_file(fd, map);
    close(fd);
    return 0;
}