#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <nmmintrin.h>
#include <chrono>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <isa-l_crypto.h>
// #include <intel-ipsec-mb.h>

#include "blake3.h"
#include "blake2.h"

/// 1 gigabyte
const uint32_t NumBytes = 1024ULL * 1024ULL * 1024ULL;
/// 4k chunks during last test
const uint64_t DefaultChunkSize = 8*1024;


#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#else
#include <time.h>
#endif

// timing
static double seconds()
{
#if defined(_WIN32) || defined(_WIN64)
  LARGE_INTEGER frequency, now;
  QueryPerformanceFrequency(&frequency);
  QueryPerformanceCounter  (&now);
  return now.QuadPart / double(frequency.QuadPart);
#else
//   struct timespec time;
//   clock_gettime(CLOCK_REALTIME, &time);
//   return time.tv_sec + time.tv_nsec / 1000000000.0;
    return clock() * 1.0 / CLOCKS_PER_SEC;
#endif
}

uint8_t* calc_sha256(uint8_t *buf, uint32_t len) {
    uint8_t *crr_sha256 = (uint8_t *) malloc(SHA256_DIGEST_LENGTH);
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, buf, len);
    EVP_DigestFinal_ex(ctx, crr_sha256, NULL);
    EVP_MD_CTX_free(ctx);
    return crr_sha256;
}

uint8_t* crr_calc_blake3(uint8_t *buf, uint32_t len) {
    uint8_t *crr_blake3 = (uint8_t *) malloc(BLAKE3_OUT_LEN);
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, buf, len);
    blake3_hasher_finalize(&hasher, crr_blake3, BLAKE3_OUT_LEN);
    return crr_blake3;
}

void handleErrors() {
    ERR_print_errors_fp(stderr);
    abort();
}

void sha256_hardware_accelerated(const uint8_t* data, size_t data_len, uint8_t* hash) {
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if (mdctx == NULL) handleErrors();

    if (1 != EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL)) handleErrors();
    if (1 != EVP_DigestUpdate(mdctx, data, data_len)) handleErrors();
    if (1 != EVP_DigestFinal_ex(mdctx, hash, NULL)) handleErrors();

    EVP_MD_CTX_free(mdctx);
}

int main(int argc, char** argv)
{
    printf("Please wait ...\n");

    uint32_t randomNumber = 0x27121978;
    // initialize data
    char* data = (char*)malloc(NumBytes);
    for (uint64_t i = 0; i < NumBytes; i++)
    {
        data[i] = (char)(randomNumber & 0xFF);
        randomNumber = 1664525 * randomNumber + 1013904223;
    }

    // re-use variables
    double startTime, duration;

    printf("SHA256 speed test of fixed 8 KB chunks for %u MB\n", NumBytes / (1024*1024));
    // Openssl SHA256 for fixed 8 KB chunks
    uint8_t hash[SHA256_DIGEST_LENGTH];
    startTime = seconds();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(hash, 0, SHA256_DIGEST_LENGTH);
        SHA256((const unsigned char*)data + i, DefaultChunkSize, hash);
    }
    duration = seconds() - startTime;
    printf("OpenSSL SHA256      :  %.3fs, %.3f MB/s\n",
            duration, (NumBytes / (1024*1024)) / duration);
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        printf("%02x", hash[i]);
    }
    printf("\n\n");

    uint8_t* crr_sha256 = (uint8_t*)malloc(SHA256_DIGEST_LENGTH);
    startTime = seconds();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        free(crr_sha256);
        uint8_t* crr_sha256 = calc_sha256((uint8_t*)data + i, DefaultChunkSize);
    }
    duration = seconds() - startTime;
    printf("OpenSSL SHA256  2   :  %.3fs, %.3f MB/s\n",
            duration, (NumBytes / (1024*1024)) / duration);

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        printf("%02x", crr_sha256[i]);
    }
    printf("\n\n");
    free(crr_sha256);

    // Blake3 for fixed 8 KB chunks
    uint8_t* crr_blake3 = (uint8_t*)malloc(BLAKE3_OUT_LEN);
    startTime = seconds();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        free(crr_blake3);
        uint8_t* crr_blake3 = crr_calc_blake3((uint8_t*)data + i, DefaultChunkSize);
    }
    duration = seconds() - startTime;
    printf("Blake3              :  %.3fs, %.3f MB/s\n",
            duration, (NumBytes / (1024*1024)) / duration);
    for (int i = 0; i < BLAKE3_OUT_LEN; i++)
    {
        printf("%02x", crr_blake3[i]);
    }
    printf("\n\n");

    // OpenSSL hardware-accelerated SHA256 for fixed 8 KB chunks
    startTime = seconds();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(hash, 0, SHA256_DIGEST_LENGTH);
        sha256_hardware_accelerated((const uint8_t*)data + i, DefaultChunkSize, hash);
    }
    duration = seconds() - startTime;
    printf("OpenSSL SHA256 HW   :  %.3fs, %.3f MB/s\n",
            duration, (NumBytes / (1024*1024)) / duration);
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        printf("%02x", hash[i]);
    }
    printf("\n\n");

    uint32_t mh_sha256_digest[ISAL_SHA256_DIGEST_WORDS];
    struct isal_mh_sha256_ctx *ctx = nullptr;

    startTime = seconds();
    for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
        memset(mh_sha256_digest, 0, ISAL_SHA256_DIGEST_WORDS);
        ctx = (struct isal_mh_sha256_ctx *)malloc(sizeof(struct isal_mh_sha256_ctx));
        isal_mh_sha256_init(ctx);
        isal_mh_sha256_update(ctx, data + i, DefaultChunkSize);
        isal_mh_sha256_finalize(ctx, mh_sha256_digest);
        free(ctx);
    }
    duration = seconds() - startTime;
    printf("ISA-L SHA256 mh_sha256 :  %.3fs, %.3f MB/s\n",
           duration, (NumBytes / (1024 * 1024)) / duration);
    for (int i = 0; i < ISAL_SHA256_DIGEST_WORDS; i++)
    {
        printf("%08x", mh_sha256_digest[i]);
    }
    printf("\n\n");
    
    ISAL_SHA256_HASH_CTX_MGR mgr;
    ISAL_SHA256_HASH_CTX ctx_2[2];
    ISAL_SHA256_HASH_CTX *ctx_out = NULL;

    // Initialize the SHA256 multi-buffer manager
    isal_sha256_ctx_mgr_init(&mgr);
    uint64_t half_default_chunk_size = DefaultChunkSize / 2;
    startTime = seconds();
    uint64_t i = 0;
    while (i + DefaultChunkSize < NumBytes)
    {
        isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[0], &ctx_out, data + i, half_default_chunk_size, ISAL_HASH_FIRST);
        isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[1], &ctx_out, data + i + half_default_chunk_size, half_default_chunk_size, ISAL_HASH_ENTIRE);
        isal_sha256_ctx_mgr_flush(&mgr, &ctx_out);
        i += DefaultChunkSize;
    }
    duration = seconds() - startTime;
    printf("ISA-L SHA256 ctx_mgr  :  %.3fs, %.3f MB/s\n",
           duration, (NumBytes / (1024 * 1024)) / duration);
    for (int i = 0; i < ISAL_SHA256_DIGEST_WORDS; i++)
    {
        printf("%08x", ctx_out->job.result_digest[i]);
    }
    printf("\n\n");

    printf("SHA256 speed test for total %u MB\n", NumBytes / (1024*1024));
    // OpenSSL SHA256 for total data
    startTime = seconds();
    memset(hash, 0, SHA256_DIGEST_LENGTH);
    SHA256((const unsigned char*)data, NumBytes, hash);
    duration = seconds() - startTime;
    printf("OpenSSL SHA256      :  %.3fs, %.3f MB/s\n",
            duration, (NumBytes / (1024*1024)) / duration);
    
    startTime = seconds();
    crr_sha256 = calc_sha256((uint8_t*)data, NumBytes);
    free(crr_sha256);
    duration = seconds() - startTime;
    printf("OpenSSL SHA256  2   :  %.3fs, %.3f MB/s\n",
            duration, (NumBytes / (1024*1024)) / duration);

    // OpenSSL hardware-accelerated SHA256 for total data
    startTime = seconds();
    memset(hash, 0, SHA256_DIGEST_LENGTH);
    sha256_hardware_accelerated((const uint8_t*)data, NumBytes, hash);
    duration = seconds() - startTime;
    printf("OpenSSL SHA256 HW   :  %.3fs, %.3f MB/s\n",
            duration, (NumBytes / (1024*1024)) / duration);
    
    startTime = seconds();
    memset(mh_sha256_digest, 0, ISAL_SHA256_DIGEST_WORDS);
    ctx = (struct isal_mh_sha256_ctx *)malloc(sizeof(struct isal_mh_sha256_ctx));
    isal_mh_sha256_init(ctx);
    isal_mh_sha256_update(ctx, data, NumBytes);
    isal_mh_sha256_finalize(ctx, mh_sha256_digest);
    free(ctx);
    duration = seconds() - startTime;
    printf("ISA-L SHA256 mh_sha256 :  %.3fs, %.3f MB/s\n",
           duration, (NumBytes / (1024 * 1024)) / duration);
    
    // Initialize the SHA256 multi-buffer manager
    isal_sha256_ctx_mgr_init(&mgr);

    startTime = seconds();
    isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[0], &ctx_out, data, NumBytes/2, ISAL_HASH_FIRST);
    isal_sha256_ctx_mgr_submit(&mgr, &ctx_2[1], &ctx_out, data, NumBytes/2, ISAL_HASH_ENTIRE);
    isal_sha256_ctx_mgr_flush(&mgr, &ctx_out);
    duration = seconds() - startTime;
    printf("ISA-L SHA256 ctx_mgr  :  %.3fs, %.3f MB/s\n",
           duration, (NumBytes / (1024 * 1024)) / duration);

    free(data);
    return 0;
}