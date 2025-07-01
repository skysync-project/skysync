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

uint32_t crc32_combine_return_crcA0(uint32_t crcA, uint32_t crcB, size_t lengthB);
uint32_t crc32_remove0(uint32_t crc1, size_t len2);

// pigz crc32_combine
uint32_t multmodp(uint32_t a, uint32_t b);
uint32_t x2nmodp(size_t n, unsigned k);

uint32_t crc32_comb(uint32_t crc1, uint32_t crc2, size_t len2);
uint32_t crc32_add0(uint32_t crc1, size_t len2);

uint32_t operator_pow(uint32_t base, size_t exp);
uint32_t crc32_rem0(uint32_t crc1, size_t len2);


/// compute CRC32 (bitwise algorithm)
uint32_t crc32_bitwise (const void* data, size_t length, uint32_t previousCrc32);
/// compute CRC32 (half-byte algoritm)
uint32_t crc32_halfbyte(const void* data, size_t length, uint32_t previousCrc32);

#ifdef CRC32_USE_LOOKUP_TABLE_BYTE
/// compute CRC32 (standard algorithm)
uint32_t crc32_1byte   (const void* data, size_t length, uint32_t previousCrc32);

uint32_t rolling_crc32_1byte_4KB(uint32_t crc, const uint8_t cur, const uint8_t pre);
uint32_t rolling_crc32_1byte_8KB(uint32_t crc, const uint8_t cur, const uint8_t pre);

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


// static uint32_t odd[32] = {
// 0x1db71064, 0x3b6e20c8, 0x76dc4190, 0xedb88320, 0x00000001, 0x00000002, 0x00000004, 0x00000008, 0x00000010, 0x00000020, 0x00000040, 0x00000080, 0x00000100, 0x00000200, 0x00000400, 0x00000800, 0x00001000, 0x00002000, 0x00004000, 0x00008000, 0x00010000, 0x00020000, 0x00040000, 0x00080000, 0x00100000, 0x00200000, 0x00400000, 0x00800000, 0x01000000, 0x02000000, 0x04000000, 0x08000000,
// };

// static uint32_t even[32] = {
// 0x76dc4190, 0xedb88320, 0x00000001, 0x00000002, 0x00000004, 0x00000008, 0x00000010, 0x00000020, 0x00000040, 0x00000080, 0x00000100, 0x00000200, 0x00000400, 0x00000800, 0x00001000, 0x00002000, 0x00004000, 0x00008000, 0x00010000, 0x00020000, 0x00040000, 0x00080000, 0x00100000, 0x00200000, 0x00400000, 0x00800000, 0x01000000, 0x02000000, 0x04000000, 0x08000000, 0x10000000, 0x20000000,
// };

#ifdef  __cplusplus
}
#endif

#endif // CRC32_H
