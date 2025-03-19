#include "crc32.h"
#include "crc32c.h"
#include <stdlib.h>
#include <stdio.h>

const size_t num_bytes = 1024*1024*100;
const size_t win_size = 4*1024;
const size_t test_size = 2000;

int main(int argc, char** argv) {
    uint32_t random_number = 0x27121978;
    // initialize
    //   char* data = new char[NumBytes];
    uint8_t data[test_size + win_size];
    for (size_t i = 0; i < test_size + win_size; i++)
    {
        data[i] = random_number & 0xFF;
        random_number = 1664525 * random_number + 1013904223;
    }

    // size_t win_start = 0;
    // size_t win_end = win_size;

#ifdef CRC32_USE_LOOKUP_TABLE_BYTE
    // Compute 4KB CRC32C checksums
    uint32_t crc1 , crc2;
    crc1 = crc32_1byte(data, win_size, 0);
    printf("original crc1: CRC=%08X \n", crc1);

    crc1 = rolling_crc32_1byte(crc1, data[win_size], data[0]);
    crc2 = crc32_1byte(data + 1, win_size, 0);
    printf("crc1 of rolling 1 byte: CRC=%08X \n", crc1);
    printf("crc2: CRC=%08X \n", crc2);

    crc1 = crc32_1byte(data, win_size, 0);
    printf("original crc1: CRC=%08X \n", crc1);
    crc1 = remove_crc32_1byte(crc1, data[0]);
    crc1 = crc32_1byte(data + win_size, 1, crc1);
    printf("crc1 of rolling 1 byte: CRC=%08X \n", crc1);
    crc2 = crc32_1byte(data + 1, win_size, 0);
    printf("crc2: CRC=%08X \n\n", crc2);

    uint32_t crcA_size = 2048;
    uint32_t crcB_size = 2048;
    // uint32_t crcA = crc32_1byte(data, crcA_size, 0);
    // uint32_t crcB = crc32_1byte(data + crcA_size, crcB_size, 0);
    uint32_t crcA = crc32_isal(data, crcA_size, 0);
    uint32_t crcB = crc32_isal(data + crcA_size, crcB_size, 0);
    uint32_t crcC_combine = crc32_combine1(crcA, crcB, crcB_size);
    uint32_t crcC_combine_2 = crc32_comb(crcA, crcB, crcB_size);

    // uint32_t crcC = crc32_1byte(data, crcA_size + crcB_size, 0);
    uint32_t crcC = crc32_isal(data, crcA_size + crcB_size, 0);
    // printf("roll: %08x and %08x %s\n", crc1, crc2, crc1==crc2? "are equal":"ARE NOT EQUAL!");
    printf("crcA: %08x, crcB: %08x, \ncrcC: %08x, crcC_combine: %08x, %s\n", crcA, crcB, crcC,
            crcC_combine, crcC_combine==crcC? "are equal":"ARE NOT EQUAL!");
    printf("crcC_combine_2: %08x\n", crcC_combine_2);

    uint32_t crcA_xor = crcC_combine ^ crcB;
    uint32_t crcA_combine_return = crc32_combine_return_crcA(crcA, crcB, crcB_size);
    printf("crcA_xor: %08x, crcA_combine_return: %08x, %s\n", crcA_xor, crcA_combine_return,
            crcA_xor==crcA_combine_return? "are equal":"ARE NOT EQUAL!");

    crcB = 0;
    uint32_t crcA_combine_no_crcB = crc32_combine_return_crcA(crcA, crcB, crcB_size);
    printf("crcA_combine_no_crcB: %08x, %s\n", crcA_combine_no_crcB,
            crcA_combine_no_crcB==crcA_combine_return? "are equal":"ARE NOT EQUAL!");

#endif // CRC32_USE_LOOKUP_TABLE_BYTE
}
