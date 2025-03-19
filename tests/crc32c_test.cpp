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

// the slicing-by-4/8/16 tests are only performed if the corresponding
// preprocessor symbol is defined in Crc32.h
// simpler algorithms can be enabled/disabled right here:
// #define CRC32_TEST_BITWISE
// #define CRC32_TEST_HALFBYTE
// #define CRC32_TEST_TABLELESS

// //////////////////////////////////////////////////////////
// test code

/// 1 gigabyte
const uint64_t NumBytes = 1024ULL * 1024ULL * 10240ULL;
/// 4k chunks during last test
const uint64_t DefaultChunkSize = 8*1024;

const uint32_t DefaultWindowSize = 4*1024;

static uint32_t magic_number = 0;
static uint32_t break_mask_bit = 14;
static uint32_t break_mask = (1u<<break_mask_bit)-1;


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

// Byte-boundary alignment issues
#define ALIGN_SIZE      0x08UL
#define ALIGN_MASK      (ALIGN_SIZE - 1)
#define CALC_CRC(op, crc, type, buf, len)                               \
  do {                                                                  \
    for (; (len) >= sizeof (type); (len) -= sizeof(type), buf += sizeof (type)) { \
      (crc) = op((crc), *(type *) (buf));                               \
    }                                                                   \
  } while(0)


// /* Compute CRC-32C using the Intel hardware instruction. */
// /* for better parallelization with bigger buffers see 
//    http://www.drdobbs.com/parallel/fast-parallelized-crc-computation-using/229401411 */
// uint32_t crc32c_hw_1(const void *input, int len, uint32_t crc)
// {
//     const char* buf = (const char*)input;

//     // XOR the initial CRC with INT_MAX
//     crc ^= 0xFFFFFFFF;

//     // Align the input to the word boundary
//     for (; (len > 0) && ((size_t)buf & ALIGN_MASK); len--, buf++) {
//         crc = _mm_crc32_u8(crc, *buf);
//     }

//     // Blast off the CRC32 calculation
// #if defined(__x86_64__) || defined(__aarch64__)
//     CALC_CRC(_mm_crc32_u64, crc, uint64_t, buf, len);
// #endif
//     CALC_CRC(_mm_crc32_u32, crc, uint32_t, buf, len);
//     CALC_CRC(_mm_crc32_u16, crc, uint16_t, buf, len);
//     CALC_CRC(_mm_crc32_u8, crc, uint8_t, buf, len);

//     // Post-process the crc
//     return (crc ^ 0xFFFFFFFF);
// }

uint64_t crc64c_hw_1(const void *input, int len, uint32_t seed)
{
    const char* buf = (const char*)input;
    uint64_t crc = (uint64_t)seed;

    // Align the input to the word boundary
    for (; (len > 0) && ((size_t)buf & ALIGN_MASK); len--, buf++) {
        crc = _mm_crc32_u8(crc, *buf);
    }

    // Blast off the CRC32 calculation
#if defined(__x86_64__) || defined(__aarch64__)
    CALC_CRC(_mm_crc32_u64, crc, uint64_t, buf, len);
#endif
    CALC_CRC(_mm_crc32_u32, crc, uint32_t, buf, len);
    CALC_CRC(_mm_crc32_u16, crc, uint16_t, buf, len);
    CALC_CRC(_mm_crc32_u8, crc, uint8_t, buf, len);

    // Post-process the crc
    return crc;
}


int main(int argc, char** argv)
{
  printf("Please wait ...\n");

  uint32_t randomNumber = 0x27121978;
  // initialize
//   char* data = new char[NumBytes];
  char* data = (char*)malloc(NumBytes);
  for (uint64_t i = 0; i < NumBytes; i++)
  {
    // data[i] = char(randomNumber & 0xFF);
    data[i] = (char)(randomNumber & 0xFF);
    randomNumber = 1664525 * randomNumber + 1013904223;
  }

  // re-use variables
  double startTime, duration;
  uint32_t crc;

#ifdef CRC32_TEST_BITWISE
  // bitwise
  startTime = seconds();
  crc = crc32_bitwise(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("bitwise          : CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);
#endif // CRC32_TEST_BITWISE

#ifdef CRC32_TEST_HALFBYTE
  // half-byte
  startTime = seconds();
  crc = crc32_halfbyte(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("half-byte        : CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);
#endif // CRC32_TEST_HALFBYTE

#ifdef CRC32_TEST_TABLELESS
  // one byte at once (without lookup tables)
  startTime = seconds();
  crc = crc32_1byte_tableless(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("tableless (byte) : CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // one byte at once (without lookup tables)
  startTime = seconds();
  crc = crc32_1byte_tableless2(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("tableless (byte2): CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);
#endif // CRC32_TEST_TABLELESS

#ifdef CRC32_USE_LOOKUP_TABLE_BYTE
  // one byte at once
  startTime = seconds();
  crc = crc32_1byte(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("  1 byte  at once: CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);
#endif // CRC32_USE_LOOKUP_TABLE_BYTE

#ifdef CRC32_USE_LOOKUP_TABLE_SLICING_BY_4
  // four bytes at once
  startTime = seconds();
  crc = crc32_4bytes(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("  4 bytes at once: CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);
#endif // CRC32_USE_LOOKUP_TABLE_SLICING_BY_4

#ifdef CRC32_USE_LOOKUP_TABLE_SLICING_BY_8
  // eight bytes at once
  startTime = seconds();
  crc = crc32_8bytes(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("  8 bytes at once: CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // eight bytes at once, unrolled 4 times (=> 32 bytes per loop)
  startTime = seconds();
  crc = crc32_4x8bytes(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf("4x8 bytes at once: CRC=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);
#endif // CRC32_USE_LOOKUP_TABLE_SLICING_BY_8

#ifdef CRC32_USE_LOOKUP_TABLE_SLICING_BY_16
  // sixteen bytes at once
  startTime = seconds();
  crc = crc32_16bytes(data, NumBytes, 0);
  duration  = seconds() - startTime;
  printf(" 16 bytes at once: CRC32=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // sixteen bytes at once
  // startTime = seconds();
  // crc = crc32_16bytes_prefetch(data, NumBytes, 0, 256);
  // duration  = seconds() - startTime;
  // printf(" 16 bytes at once: CRC32=%08X, %.3fs, %.3f MB/s (including prefetching)\n",
  //        crc, duration, (NumBytes / (1024*1024)) / duration);
#endif // CRC32_USE_LOOKUP_TABLE_SLICING_BY_16

  printf("\nFixed 8 KB chunk:\n");
  // process in 8k chunks
  startTime = seconds();
  crc = 0; // also default parameter of crc32_xx functions
  uint64_t bytesProcessed = 0;
  while (bytesProcessed < NumBytes)
  {
    uint64_t bytesLeft = NumBytes - bytesProcessed;
    uint64_t chunkSize = (DefaultChunkSize < bytesLeft) ? DefaultChunkSize : bytesLeft;
    crc = 0;
    crc = crc32_fast(data + bytesProcessed, chunkSize, crc);

    bytesProcessed += chunkSize;
  }
  duration  = seconds() - startTime;
  printf("    16 bytes     : CRC32=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // Intel ISA-L CRC32 gzip for fixed 8 KB chunks
  startTime = seconds();
  for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    crc = 0;
    crc = crc32_gzip_refl(crc, (const unsigned char*)data + i, DefaultChunkSize);
  }
  duration  = seconds() - startTime;
  printf("    ISA-L  gzip  : CRC32=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // Intel ISA-L CRC32 iscsi for fixed 8 KB chunks
  startTime = seconds();
  for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    crc = 0;
    crc = crc32c_isal(crc, (const unsigned char*)data + i, DefaultChunkSize);
  }
  duration  = seconds() - startTime;
  printf("    ISA-L iscsi  : CRC32C=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // Mark Adler CRC32-SW for fixed 8 KB chunks
  // startTime = seconds();
  // for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
  //   crc = 0;
  //   crc = crc32c_sw(crc, (const unsigned char*)data + i, DefaultChunkSize);
  // }
  // duration  = seconds() - startTime;
  // printf("    Adler  SW     : CRC32C=%08X, %.3fs, %.3f MB/s\n",
  //        crc, duration, (NumBytes / (1024*1024)) / duration);

  // Mark Adler CRC32-HW for fixed 8 KB chunks
  startTime = seconds();
  for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    crc = 0;
    crc = crc32c_hw(crc, (const unsigned char*)data + i, DefaultChunkSize);
  }
  duration  = seconds() - startTime;
  printf("    Adler  HW    : CRC32C=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // Intel ISA-L CRC64C for fixed 8 KB chunks
  startTime = seconds();
  for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    crc = 0;
    crc = crc64_ecma_norm(crc, (const unsigned char*)data + i, DefaultChunkSize);
  }
  duration  = seconds() - startTime;
  printf("    ISA-L        : CRC64=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // smhasher crc64c_hw_1 for fixed 8 KB chunks
  startTime = seconds();
  for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    crc = 0;
    crc = crc64c_hw_1((const unsigned char*)data + i, DefaultChunkSize, crc);
  }
  duration  = seconds() - startTime;
  printf(" smhasher  HW    : CRC64=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  // // smhasher crc32c_hw_1 for fixed 8 KB chunks
  // startTime = seconds();
  // for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
  //   crc = 0;
  //   crc = crc32c_hw_1((const unsigned char*)data + i, DefaultChunkSize, crc);
  // }
  // duration  = seconds() - startTime;
  // printf(" smhasher  HW_1   : CRC32C=%08X, %.3fs, %.3f MB/s\n",
  //        crc, duration, (NumBytes / (1024*1024)) / duration);

  //rsync get_checksum1 for fixed 8 KB chunks
  startTime = seconds();
  for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i += DefaultChunkSize) {
    crc = 0;
    crc = get_checksum1((char*)data + i, DefaultChunkSize);
  }
  duration  = seconds() - startTime;
  printf("    rsync         : CRC32=%08X, %.3fs, %.3f MB/s\n",
         crc, duration, (NumBytes / (1024*1024)) / duration);

  printf("\nrolling:\n");

  // rolling_crc32_1byte using a DefaultWindowSize window
  auto start_1 = std::chrono::high_resolution_clock::now();
  crc = crc32_fast(data, DefaultWindowSize , 0);
  for (uint64_t i = DefaultWindowSize; i < NumBytes; i++)
  {
    crc = rolling_crc32_1byte(crc, data[i], data[i - DefaultWindowSize]);
    if((crc & break_mask) == magic_number)
      continue;
  }
  std::chrono::duration<double> diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling crc32 1byte  :  CRC32=%08X, %.3fs, %.3f MB/s\n",
          crc, diff.count(), (NumBytes / (1024*1024)) / diff.count());
  
  // rolling byte to byte using a DefaultWindowSize window and calculating the CRC32 using crc32_fast
  start_1 = std::chrono::high_resolution_clock::now();
  crc = crc32_fast(data, DefaultWindowSize , 0);
  for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++)
  {
    crc = remove_crc32_1byte(crc, data[i]);
    crc = crc32_fast(data + i + DefaultWindowSize, 1, crc);
    if ((crc & break_mask) == magic_number)
      continue;
  }
  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling crc32 fast   :  CRC32=%08X, %.3fs, %.3f MB/s\n",
          crc, diff.count(), (NumBytes / (1024*1024)) / diff.count());

  // rolling byte to byte using a DefaultWindowSize window and calculating the CRC32 using crc32_gzip_refl
  start_1 = std::chrono::high_resolution_clock::now();
  crc = crc32_fast(data, DefaultWindowSize, 0);
  for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++)
  { 
    crc = remove_crc32_1byte(crc, data[i]);
    crc = crc32_gzip_refl(crc, (const unsigned char*)data + i + DefaultWindowSize, 1);
    if ((crc & break_mask) == magic_number)
      continue;
  }
  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling crc32 ISA-L  :  CRC32=%08X, %.3fs, %.3f MB/s\n",
         crc, diff.count(), (NumBytes / (1024*1024)) / diff.count());

  // rolling byte to byte using a DefaultWindowSize window and calculating the CRC32 using __builtin_ia32_crc32qi
  start_1 = std::chrono::high_resolution_clock::now();
  crc = crc32_gzip_refl(0, (const unsigned char*)data, DefaultWindowSize);
  for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++)
  { 
    crc = remove_crc32_1byte(crc, data[i]);
    crc = __builtin_ia32_crc32qi(crc, data[i + DefaultWindowSize]);
    if ((crc & break_mask) == magic_number)
      continue;
  }
  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling crc32 builtin:  CRC32=%08X, %.3fs, %.3f MB/s\n",
         crc, diff.count(), (NumBytes / (1024*1024)) / diff.count());

  // rolling byte to byte using a DefaultWindowSize window and calculating the CRC32 using crc32c_hw
  start_1 = std::chrono::high_resolution_clock::now();
  crc = crc32c_hw(0, (const unsigned char*)data, DefaultWindowSize);
  for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++)
  { 
    crc = remove_crc32_1byte(crc, data[i]);
    crc = crc32c_hw(crc, (const unsigned char*)data + i + DefaultWindowSize, 1);
    if ((crc & break_mask) == magic_number)
      continue;
  }
  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling crc32 HW     :  CRC32C=%08X, %.3fs, %.3f MB/s\n",
         crc, diff.count(), (NumBytes / (1024*1024)) / diff.count());

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

  start_1 = std::chrono::high_resolution_clock::now();
  remain = NumBytes;
  const uint8_t* data_ptr = (const uint8_t*)data;
  while (remain > 0)
  {
    uint32_t chunk = (remain > max_chunk) ? max_chunk : remain;
    isal_rolling_hash2_reset(state, data_ptr + min_chunk - DefaultWindowSize);
    isal_rolling_hash2_run(state, data_ptr + min_chunk, chunk - min_chunk, mask, trigger, &offset, &ret);
    data_ptr += (offset + min_chunk);
    remain -= (offset + min_chunk);
  }
  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling ISA-L hash  :  %.3fs, %.3f MB/s\n",
         diff.count(), (NumBytes / (1024*1024)) / (diff.count()));
  
  // rolling rsync get_checksum1
  start_1 = std::chrono::high_resolution_clock::now();
  crc = 0;
  uint32 s1 = 0, s2 = 0;

  // Initialize first window using SIMD optimization
  int32 i = get_checksum1_avx2_asm((schar*)data, DefaultWindowSize, 0, &s1, &s2); 
  crc = (s1 & 0xffff) + (s2 << 16);

  // Roll the window using incremental updates
  for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
      // Remove oldest byte
      s1 -= (data[i] + CHAR_OFFSET);
      s2 -= s1;
      
      // Add newest byte 
      s1 += (data[i + DefaultWindowSize] + CHAR_OFFSET);
      s2 += s1;
      
      crc = (s1 & 0xffff) + (s2 << 16);
      if ((crc & break_mask) == magic_number)
        continue;
  }

  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling rsync       :  CRC32=%08X, %.3fs, %.3f MB/s\n",
        crc, diff.count(), (NumBytes / (1024*1024)) / diff.count());

  // SS-CDC rolling hash
  start_1 = std::chrono::high_resolution_clock::now();
  uint32_t ss_crc = crc32c_hw(0, (const unsigned char*)data, DefaultWindowSize);
  for (uint64_t i = 0; i + DefaultWindowSize < NumBytes; i++) {
    doCrc_rolling(ss_crc, data[i + DefaultWindowSize], data[i]);
    if ((ss_crc & break_mask) == magic_number)
      continue;
  }
  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling SS-CDC hash :  CRC32=%08X, %.3fs, %.3f MB/s\n",
         ss_crc, diff.count(), (NumBytes / (1024*1024)) / diff.count());

  // rolling byte to byte using Gear Hash
  start_1 = std::chrono::high_resolution_clock::now();
  uint64_t fastfp = 0;
  for (uint64_t i = 0; i < DefaultChunkSize; i++)
    fastfp += (GEARv2[data[i]]);
  for (uint64_t i = 0; i + DefaultChunkSize < NumBytes; i++) {
    fastfp = (fastfp << 1) + (GEARv2[data[i + DefaultChunkSize]]);
    if ((fastfp & break_mask) == magic_number)
      continue;
  }
  diff = std::chrono::high_resolution_clock::now() - start_1;
  printf("rolling Gear Hash    :  %.3fs, %.3f MB/s\n",
          diff.count(), (NumBytes / (1024*1024)) / (diff.count()));

  free(data);
  return 0;
}
