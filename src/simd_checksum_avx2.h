#ifndef SIMD_CHECKSUM_AVX2_H
#define SIMD_CHECKSUM_AVX2_H

#include <stdint.h>

#define USE_ROLL_ASM

#define CHAR_OFFSET 0
typedef int32_t int32;
typedef signed char schar;
typedef unsigned int uint32;
typedef short int16;
typedef unsigned short uint16;

extern "C" {
    
int32 get_checksum1_avx2_asm(schar* buf, int32 len, int32 i, uint32* ps1, uint32* ps2);

uint32 get_checksum1(char *buf1, int32 len);

}

#endif // SIMD_CHECKSUM_AVX2_H