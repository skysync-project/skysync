#ifndef CRC32_H
#define CRC32_H

#ifdef  __cplusplus
extern "C" {
#endif

// if running on an embedded system, you might consider shrinking the
// big Crc32Lookup table by undefining these lines:
#define CRC32_USE_LOOKUP_TABLE_BYTE
#define CRC32_USE_LOOKUP_TABLE_SLICING_BY_4
#define CRC32_USE_LOOKUP_TABLE_SLICING_BY_8
#define CRC32_USE_LOOKUP_TABLE_SLICING_BY_16
// - crc32_bitwise  doesn't need it at all
// - crc32_halfbyte has its own small lookup table
// - crc32_1byte_tableless and crc32_1byte_tableless2 don't need it at all
// - crc32_1byte    needs only Crc32Lookup[0]
// - crc32_4bytes   needs only Crc32Lookup[0..3]
// - crc32_8bytes   needs only Crc32Lookup[0..7]
// - crc32_4x8bytes needs only Crc32Lookup[0..7]
// - crc32_16bytes  needs all of Crc32Lookup
// using the aforementioned #defines the table is automatically fitted to your needs

// uint8_t, uint32_t, int32_t
#include <stdint.h>
// size_t
#include <stddef.h>
#include <stdio.h>

#define LONG 8192 
#define SHORT 256

// crc32_fast selects the fastest algorithm depending on flags (CRC32_USE_LOOKUP_...)
/// compute CRC32 using the fastest algorithm for large datasets on modern CPUs
uint32_t crc32_fast    (const void* data, size_t length, uint32_t previousCrc32);

/// merge two CRC32 such that result = crc32(dataB, lengthB, crc32(dataA, lengthA))
uint32_t crc32_combine1 (uint32_t crcA, uint32_t crcB, size_t lengthB);

uint32_t crc32_combine_return_crcA(uint32_t crcA, uint32_t crcB, size_t lengthB);

// pigz crc32_combine
uint32_t multmodp(uint32_t a, uint32_t b);
uint32_t x2nmodp(size_t n, unsigned k);
unsigned long crc32_comb(unsigned long crc1, unsigned long crc2,
                               size_t len2);

/// compute CRC32 (bitwise algorithm)
uint32_t crc32_bitwise (const void* data, size_t length, uint32_t previousCrc32);
/// compute CRC32 (half-byte algoritm)
uint32_t crc32_halfbyte(const void* data, size_t length, uint32_t previousCrc32);

#ifdef CRC32_USE_LOOKUP_TABLE_BYTE
/// compute CRC32 (standard algorithm)
uint32_t crc32_1byte   (const void* data, size_t length, uint32_t previousCrc32);

uint32_t rolling_crc32_1byte(uint32_t crc, const uint8_t cur, const uint8_t pre);

uint32_t remove_crc32_1byte(uint32_t crc, const uint8_t pre);

#endif

/// compute CRC32 (byte algorithm) without lookup tables
uint32_t crc32_1byte_tableless (const void* data, size_t length, uint32_t previousCrc32);
/// compute CRC32 (byte algorithm) without lookup tables
uint32_t crc32_1byte_tableless2(const void* data, size_t length, uint32_t previousCrc32);

#ifdef CRC32_USE_LOOKUP_TABLE_SLICING_BY_4
/// compute CRC32 (Slicing-by-4 algorithm)
uint32_t crc32_4bytes  (const void* data, size_t length, uint32_t previousCrc32);
#endif

#ifdef CRC32_USE_LOOKUP_TABLE_SLICING_BY_8
/// compute CRC32 (Slicing-by-8 algorithm)
uint32_t crc32_8bytes  (const void* data, size_t length, uint32_t previousCrc32);
/// compute CRC32 (Slicing-by-8 algorithm), unroll inner loop 4 times
uint32_t crc32_4x8bytes(const void* data, size_t length, uint32_t previousCrc32);
#endif

#ifdef CRC32_USE_LOOKUP_TABLE_SLICING_BY_16
/// compute CRC32 (Slicing-by-16 algorithm)
uint32_t crc32_16bytes (const void* data, size_t length, uint32_t previousCrc32);
/// compute CRC32 (Slicing-by-16 algorithm, prefetch upcoming data blocks)
uint32_t crc32_16bytes_prefetch(const void* data, size_t length, uint32_t previousCrc32, size_t prefetchAhead);
#endif

// Intel ISA-L CRC32
uint32_t crc32_isal(const void* data, size_t length, uint32_t previousCrc32);

#ifdef  __cplusplus
}
#endif

#endif // CRC32_H
