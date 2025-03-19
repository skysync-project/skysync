#include "adler_rollsum.h"

#define DO1(buf,i)  {s1 += buf[i]; s2 += s1;}
#define DO2(buf,i)  DO1(buf,i); DO1(buf,i+1);
#define DO4(buf,i)  DO2(buf,i); DO2(buf,i+2);
#define DO8(buf,i)  DO4(buf,i); DO4(buf,i+4);
#define DO16(buf)   DO8(buf,0); DO8(buf,8);

void RollsumUpdate(Rollsum *sum, const unsigned char *buf, size_t len)
{
    /* ANSI C says no overflow for unsigned. zlib's adler32 goes to extra
       effort to avoid overflow for its mod prime, which we don't have. */
    size_t n = len;
    uint_fast16_t s1 = sum->s1;
    uint_fast16_t s2 = sum->s2;

    while (n >= 16) {
        DO16(buf);
        buf += 16;
        n -= 16;
    }
    while (n != 0) {
        s1 += *buf++;
        s2 += s1;
        n--;
    }
    /* Increment s1 and s2 by the amounts added by the char offset. */
    s1 += (uint_fast16_t)len * ROLLSUM_CHAR_OFFSET;
    s2 += (uint_fast16_t)((len * (len + 1)) / 2) * ROLLSUM_CHAR_OFFSET;
    sum->count += len;          /* Increment sum count. */
    sum->s1 = s1;
    sum->s2 = s2;
}
