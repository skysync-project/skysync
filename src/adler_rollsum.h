#ifndef ADLER_ROLLSUM_H
#define ADLER_ROLLSUM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* We should make this something other than zero to improve the checksum
   algorithm: tridge suggests a prime number. */
#define ROLLSUM_CHAR_OFFSET 31

/** The Rollsum state type. */
typedef struct Rollsum {
    size_t count;               /**< count of bytes included in sum */
    uint_fast16_t s1;           /**< s1 part of sum */
    uint_fast16_t s2;           /**< s2 part of sum */
} Rollsum;

void RollsumUpdate(Rollsum *sum, const unsigned char *buf, size_t len);

/* static inline implementations of simple routines */

static inline void RollsumInit(Rollsum *sum)
{
    sum->count = sum->s1 = sum->s2 = 0;
}

static inline void RollsumRotate(Rollsum *sum, unsigned char out,
                                 unsigned char in)
{
    sum->s1 += in - out;
    sum->s2 += sum->s1 - (uint_fast16_t)sum->count * (out + ROLLSUM_CHAR_OFFSET);
}

static inline void RollsumRollin(Rollsum *sum, unsigned char in)
{
    sum->s1 += in + ROLLSUM_CHAR_OFFSET;
    sum->s2 += sum->s1;
    sum->count++;
}

static inline void RollsumRollout(Rollsum *sum, unsigned char out)
{
    sum->s1 -= out + ROLLSUM_CHAR_OFFSET;
    sum->s2 -= (uint_fast16_t)sum->count * (out + ROLLSUM_CHAR_OFFSET);
    sum->count--;
}

static inline uint32_t RollsumDigest(Rollsum *sum)
{
    return ((uint32_t)sum->s2 << 16) | ((uint32_t)sum->s1 & 0xffff);
}

#ifdef __cplusplus
}
#endif

#endif /* ADLER_ROLLSUM_H */