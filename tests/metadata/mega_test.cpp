#include <stdlib.h>
#include <stdio.h>
#include <isa-l/crc.h>
#include <nmmintrin.h>
#include <chrono>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <iostream>

#include "crc32.h"
#include "crc32c.h"
#include "simd_checksum_avx2.h"

#define CHUNK_SIZE 8192

int main(int argc, char **argv) {

    if (argc < 2) {
        printf("Usage: %s <filename>\n", argv[0]);
        exit(1);
    }

    // char *filename = argv[1];
    std::string filename = argv[1];
    std::string crc32c_filename = filename + ".crc32c";
    std::string sha256_filename = filename + ".sha256";

    FILE *file = fopen(filename.c_str(), "rb");
    if (!file) {
        perror("Failed to open file");
        return EXIT_FAILURE;
    }

    FILE *crc32c_file = fopen(crc32c_filename.c_str(), "wb");
    if (!crc32c_file) {
        perror("Failed to open crc32c output file");
        fclose(file);
        return EXIT_FAILURE;
    }

    FILE *sha256_file = fopen(sha256_filename.c_str(), "wb");
    if (!sha256_file) {
        perror("Failed to open sha256 output file");
        fclose(file);
        fclose(crc32c_file);
        return EXIT_FAILURE;
    }

    uint8_t *buffer = (uint8_t *)malloc(CHUNK_SIZE);
    uint64_t total_chunk = 0;

    while (1) {
        size_t bytes_read = fread(buffer, 1, CHUNK_SIZE, file);
        if (bytes_read == 0) {
            if (feof(file)) {
                break; // End of file
            } else {
                perror("Error reading file");
                free(buffer);
                fclose(file);
                fclose(crc32c_file);
                fclose(sha256_file);
                return EXIT_FAILURE;
            }
        }

        // Calculate CRC32C
        uint32_t crc32c_value = crc32c(0, buffer, bytes_read);
        fwrite(&crc32c_value, sizeof(crc32c_value), 1, crc32c_file);

        // Calculate SHA-256
        uint8_t sha256_hash[SHA256_DIGEST_LENGTH];
        memset(sha256_hash, 0, SHA256_DIGEST_LENGTH);
        EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
        EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);
        EVP_DigestUpdate(mdctx, buffer, bytes_read);
        EVP_DigestFinal_ex(mdctx, sha256_hash, NULL);
        fwrite(sha256_hash, SHA256_DIGEST_LENGTH, 1, sha256_file);
        EVP_MD_CTX_free(mdctx);

        total_chunk++;
    }

    fwrite(&total_chunk, sizeof(total_chunk), 1, crc32c_file);
    fwrite(&total_chunk, sizeof(total_chunk), 1, sha256_file);
    // Ensure all data is written to disk
    fsync(fileno(crc32c_file));
    fsync(fileno(sha256_file));
    fclose(file);
    fclose(crc32c_file);
    fclose(sha256_file);
    free(buffer);

    // Measure the time taken for loading the CRC32C and SHA-256 files
    crc32c_file = fopen(crc32c_filename.c_str(), "rb");
    if (!crc32c_file) {
        perror("Failed to open crc32c output file for reading");
        return EXIT_FAILURE;
    }
    sha256_file = fopen(sha256_filename.c_str(), "rb");
    if (!sha256_file) {
        perror("Failed to open sha256 output file for reading");
        fclose(crc32c_file);
        return EXIT_FAILURE;
    }

    uint32_t* crc32c_loaded = (uint32_t *)malloc(total_chunk * sizeof(uint32_t));
    uint8_t* sha256_loaded = (uint8_t *)malloc(total_chunk * SHA256_DIGEST_LENGTH);

    auto start = std::chrono::high_resolution_clock::now();
    uint64_t count = 0;
    uint8_t batch_read = 8; // Read 8 entries at a time

    while (count < total_chunk) {
        size_t crc32c_read = fread(&crc32c_loaded[count], sizeof(uint32_t) * batch_read, 1, crc32c_file);
        
        if (crc32c_read == 0) {
            if (feof(crc32c_file)) {
                break; // End of file
            } else {
                perror("Error reading crc32c file");
                free(crc32c_loaded);
                free(sha256_loaded);
                fclose(crc32c_file);
                fclose(sha256_file);
                return EXIT_FAILURE;
            }
        }
        count += crc32c_read;
    }

    count = 0;
    while (count < total_chunk) {
        size_t sha256_read = fread(&sha256_loaded[count * SHA256_DIGEST_LENGTH], SHA256_DIGEST_LENGTH * batch_read, 1, sha256_file);
        
        if (sha256_read == 0) {
            if (feof(sha256_file)) {
                break; // End of file
            } else {
                perror("Error reading sha256 file");
                free(crc32c_loaded);
                free(sha256_loaded);
                fclose(crc32c_file);
                fclose(sha256_file);
                return EXIT_FAILURE;
            }
        }
        count += sha256_read;
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    printf("Time taken to load CRC32C and SHA-256 files: %.9f seconds\n", duration.count());
    printf("Total chunks processed: %lu\n", total_chunk);
}