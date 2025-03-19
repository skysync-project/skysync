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
#include "simd_checksum_avx2.h"

#include <string.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <isa-l_crypto.h>
#include <stdexcept>

#include "blake3.h"
#include "blake2.h"

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

const uint64_t DefaultChunkSize = 8*1024;
const uint32_t DefaultWindowSize = 4*1024;

static uint32_t magic_number = 0;
static uint32_t break_mask_bit = 14;
static uint32_t break_mask = (1u<<break_mask_bit)-1;

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
    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s <file>\n", argv[0]);
        return 1;
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }

    

    char *map = (char *)map_file(fd);
    uint64_t NumBytes = file_size(fd);

    printf("File size: %lu MB\n", NumBytes / (1024 * 1024));

    printf("Weak Hash Tests:\n");
    printf("Fixed chunk size: %lu KB\n", DefaultChunkSize / 1024);

    auto test_checksum = [NumBytes, map](const char* name, auto checksum_fn) {
        auto start = std::chrono::high_resolution_clock::now();
        uint32_t crc = 0;
        for (uint64_t i = 0; i < NumBytes; i += DefaultChunkSize) {
            uint32_t chunk_size = (i + DefaultChunkSize <= NumBytes) ? 
                                 DefaultChunkSize : (NumBytes - i);
            crc = checksum_fn(crc, (const unsigned char*)map + i, chunk_size);
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration<double>(end - start).count();
        printf("%s: CRC32=%08X, %.3fs, %.3f MB/s\n",
               name, crc, duration, (NumBytes / (1024*1024)) / duration);
        return crc;
    };

    // Test different checksum implementations
    test_checksum("ISA-L  gzip ", crc32_gzip_refl);
    drop_cache();
    test_checksum("ISA-L  iscsi", crc32c_isal);
    drop_cache();
    test_checksum("Adler  HW   ", crc32c_hw);
    drop_cache();

    auto start = std::chrono::high_resolution_clock::now();
    uint32_t crc = 0;
    for (uint64_t i = 0; i < NumBytes; i += DefaultChunkSize) {
        uint32_t chunk_size = (i + DefaultChunkSize <= NumBytes) ? 
                             DefaultChunkSize : (NumBytes - i);
        crc = get_checksum1((char*)map + i, chunk_size);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    printf("rsync       : CRC32=%08X, %.3fs, %.3f MB/s\n",
           crc, duration, (NumBytes / (1024*1024)) / duration);
    drop_cache();           

    printf("\nRolling Tests:\n");
    printf("Window size: %u bytes\n", DefaultWindowSize);

    // 1-byte rolling CRC32
    start = std::chrono::high_resolution_clock::now();
    crc = crc32_gzip_refl(0, (const unsigned char*)map, DefaultWindowSize);
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        crc = rolling_crc32_1byte(crc, map[i + DefaultWindowSize], map[i]);
        if ((crc & break_mask) == magic_number) {
            continue;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    auto diff = std::chrono::duration<double>(end - start).count();
    printf("rolling crc32 1byte  :  CRC32=%08X, %.3fs, %.3f MB/s\n",
           crc, diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();
    
    // Builtin rolling CRC32
    start = std::chrono::high_resolution_clock::now();
    crc = crc32_gzip_refl(0, (const unsigned char*)map, DefaultWindowSize);
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        crc = remove_crc32_1byte(crc, map[i]);
        crc = __builtin_ia32_crc32qi(crc, map[i + DefaultWindowSize]);
        if ((crc & break_mask) == magic_number) {
            continue;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("rolling crc32 builtin:  CRC32=%08X, %.3fs, %.3f MB/s\n",
           crc, diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();

    // Intel ISA-L rolling hash
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
    }
    free(state);
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("rolling ISA-L hash  :  %.3fs, %.3f MB/s\n",
            diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();

    // Rolling rsync checksum
    start = std::chrono::high_resolution_clock::now();
    uint32 s1 = 0, s2 = 0;
    
    if (get_checksum1_avx2_asm((schar*)map, DefaultWindowSize, 0, &s1, &s2) < 0) {
        close(fd);
        return 1;
    }
    
    crc = (s1 & 0xffff) + (s2 << 16);
    
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        s1 -= (map[i] + CHAR_OFFSET);
        s2 -= s1;
        s1 += (map[i + DefaultWindowSize] + CHAR_OFFSET);
        s2 += s1;
        crc = (s1 & 0xffff) + (s2 << 16);
        if ((crc & break_mask) == magic_number) {
            continue;
        }
    }
    
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("rolling rsync       :  CRC32=%08X, %.3fs, %.3f MB/s\n",
           crc, diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();

    // SS-CDC rolling hash
    start = std::chrono::high_resolution_clock::now();
    uint32_t ss_crc = crc32c_hw(0, (const unsigned char*)map, DefaultWindowSize);
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        doCrc_rolling(ss_crc, map[i + DefaultWindowSize], map[i]);
        if ((ss_crc & break_mask) == magic_number) {
            continue;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("rolling SS-CDC hash :  CRC32=%08X, %.3fs, %.3f MB/s\n",
           ss_crc, diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();

    // Gear hash
    start = std::chrono::high_resolution_clock::now();
    uint64_t fastfp = 0;
    for (uint64_t i = 0; i < DefaultWindowSize; i++) {
        fastfp += GEARv2[static_cast<uint8_t>(map[i])];
    }
    for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
        fastfp = (fastfp << 1) + GEARv2[static_cast<uint8_t>(map[i + DefaultWindowSize])];
        if ((fastfp & break_mask) == magic_number) {
            continue;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("rolling Gear Hash   :  %.3fs, %.3f MB/s\n",
           diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();

    printf("\nStrong Hash Tests:\n");
    printf("Fixed chunk size: %u bytes\n", DefaultWindowSize);

    // OpenSSL old SHA256 for fixed 8 KB chunks
    uint8_t hash[SHA256_DIGEST_LENGTH];
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(hash, 0, SHA256_DIGEST_LENGTH);
        SHA256((const unsigned char*)map + i, DefaultChunkSize, hash);
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("OpenSSL old SHA256  :  %.3fs, %.3f MB/s\n",
            diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();

    // OpenSSL new SHA256 for fixed 8 KB chunks
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
    printf("OpenSSL new SHA256  :  %.3fs, %.3f MB/s\n",
            diff, (NumBytes / (1024*1024)) / diff);
    drop_cache();

    // ISA-L SHA256 for fixed 8 KB chunks
    uint32_t mh_sha256_digest[ISAL_SHA256_DIGEST_WORDS];
    struct isal_mh_sha256_ctx *ctx = nullptr;

    start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(mh_sha256_digest, 0, ISAL_SHA256_DIGEST_WORDS);
        ctx = (struct isal_mh_sha256_ctx *)malloc(sizeof(struct isal_mh_sha256_ctx));
        isal_mh_sha256_init(ctx);
        isal_mh_sha256_update(ctx, (const unsigned char*)map + i, DefaultChunkSize);
        isal_mh_sha256_finalize(ctx, mh_sha256_digest);
        free(ctx);
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("ISA-L SHA256 mh_sha256 :  %.3fs, %.3f MB/s\n",
           diff, (NumBytes / (1024 * 1024)) / diff);
    drop_cache();

    // ISA-L SHA256 multi-buffer manager for fixed 8 KB chunks
    ISAL_SHA256_HASH_CTX_MGR mgr;
    ISAL_SHA256_HASH_CTX ctx_2[2];
    ISAL_SHA256_HASH_CTX *ctx_out = NULL;

    // Initialize the SHA256 multi-buffer manager
    isal_sha256_ctx_mgr_init(&mgr);
    uint64_t half_default_chunk_size = DefaultChunkSize / 2;
    start = std::chrono::high_resolution_clock::now();
    uint64_t i = 0;
    while (i + DefaultChunkSize < NumBytes)
    {
        isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[0], &ctx_out, (const unsigned char*)map + i, half_default_chunk_size, ISAL_HASH_FIRST);
        isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[1], &ctx_out, (const unsigned char*)map + i + half_default_chunk_size, half_default_chunk_size, ISAL_HASH_ENTIRE);
        isal_sha256_ctx_mgr_flush(&mgr, &ctx_out);
        i += DefaultChunkSize;
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("ISA-L SHA256 ctx_mgr  :  %.3fs, %.3f MB/s\n",
              diff, (NumBytes / (1024 * 1024)) / diff);
    drop_cache();

    // Blake3 for fixed 8 KB chunks
    alignas(32) uint8_t b3_hash[SHA256_DIGEST_LENGTH];
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, (const unsigned char*)map + i, DefaultChunkSize);
        blake3_hasher_finalize(&hasher, b3_hash, SHA256_DIGEST_LENGTH);
    }
    end = std::chrono::high_resolution_clock::now();
    diff = std::chrono::duration<double>(end - start).count();
    printf("Blake3               :  %.3fs, %.3f MB/s\n",
              diff, (NumBytes / (1024 * 1024)) / diff);

    unmap_file(fd, map);
    close(fd);
    return 0;
}