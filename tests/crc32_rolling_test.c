#include "crc32.h"
#include "crc32c.h"
#include <stdlib.h>
#include <stdio.h>
#include <isa-l/crc.h>

const size_t num_bytes = 1024*1024*100;
const size_t win_size = 8*1024;
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
    printf("CRC32 Rolling Test: crc32_fast\n");
    crc1 = crc32_fast(data, win_size, 0);
    printf("original crc1: CRC=%08X \n", crc1);

    crc1 = rolling_crc32_1byte_8KB(crc1, data[win_size], data[0]);
    crc2 = crc32_fast(data + 1, win_size, 0);
    printf("crc1 of rolling 1 byte: CRC=%08X \n", crc1);
    printf("crc2: CRC=%08X \n", crc2);

    printf("\nCRC32 Rolling Test: crc32_gzip_refl\n");
    // crc1 = crc32_fast(data, win_size, 0);
    crc1 = crc32_isal(data, win_size, 0);
    printf("original crc1: CRC=%08X \n", crc1);

    crc1 = rolling_crc32_1byte_8KB(crc1, data[win_size], data[0]);
    crc2 = crc32_isal(data + 1, win_size, 0);
    printf("crc1 of rolling 1 byte: CRC=%08X \n", crc1);
    printf("crc2: CRC=%08X \n", crc2);

    // doCrc_rolling
    // printf("\ndoCrc_rolling Test\n");
    // crc1 = crc32_fast(data, win_size, 0);
    // printf("original crc1: CRC=%08X \n", crc1);

    // // crc1 = rolling_crc32_1byte_4KB(crc1, data[win_size], data[0]);
    // crc1 = doCrc_rolling(crc1, data[win_size], data[0]);
    // crc2 = crc32_fast(data + 1, win_size, 0);
    // printf("crc1 of rolling 1 byte: CRC=%08X \n", crc1);
    // printf("crc2: CRC=%08X \n", crc2);

    // printf("\nremove_crc32_1byte Test\n");
    // crc1 = crc32_1byte(data, win_size, 0);
    // printf("original crc1: CRC=%08X \n", crc1);
    // crc1 = remove_crc32_1byte(crc1, data[0]);
    // crc1 = crc32_1byte(data + win_size, 1, crc1);
    // printf("crc1 of rolling 1 byte: CRC=%08X \n", crc1);
    // crc2 = crc32_1byte(data + 1, win_size, 0);
    // printf("crc2: CRC=%08X \n\n", crc2);

    // printf("CRC32C Rolling Test\n");
    // // crc1 = crc32_1byte(data, win_size, 0);
    // crc1 = crc32c_sw(0, data, win_size);
    // printf("original crc32c1: CRC32C=%08X \n", crc1);

    // crc1 = rolling_crc32_1byte_4KB(crc1, data[win_size], data[0]);
    // // crc2 = crc32_1byte(data + 1, win_size, 0);
    // crc2 = crc32c_sw(0, data + 1, win_size);
    // printf("crc1 of rolling 1 byte: CRC32C=%08X \n", crc1);
    // printf("crc2: CRC32C=%08X \n", crc2);

    // doCrc_rolling
    // printf("\ndoCrc_rolling Test\n");
    // // crc1 = crc32_1byte(data, win_size, 0);
    // crc1 = crc32c_sw(0, data, win_size);
    // printf("original crc32c1: CRC32C=%08X \n", crc1);
    // crc1 = doCrc_rolling(crc1, data[win_size], data[0]);
    // crc2 = crc32c_sw(0, data + 1, win_size);
    // printf("crc1 of rolling 1 byte: CRC32C=%08X \n", crc1);
    // printf("crc2: CRC32C=%08X \n", crc2);

    uint32_t crcA_size = 8021;
    uint32_t crcB_size = 129;
    // uint32_t crcA = crc32_1byte(data, crcA_size, 0);
    // uint32_t crcB = crc32_1byte(data + crcA_size, crcB_size, 0);
    uint32_t crcA = crc32_isal(data, crcA_size, 0);
    uint32_t crcB = crc32_isal(data + crcA_size, crcB_size, 0);
    uint32_t crcC_combine = crc32_combine1(crcA, crcB, crcB_size);
    uint32_t crcC_combine_2 = crc32_comb(crcA, crcB, crcB_size);

    // uint32_t crcC = crc32_1byte(data, crcA_size + crcB_size, 0);
    uint32_t crcC = crc32_isal(data, crcA_size + crcB_size, 0);
    // printf("roll: %08x and %08x %s\n", crc1, crc2, crc1==crc2? "are equal":"ARE NOT EQUAL!");
    printf("\ncrcA: %08x, crcB: %08x, \ncrcC: %08x, crcC_combine: %08x, %s\n", crcA, crcB, crcC,
            crcC_combine, crcC_combine==crcC? "are equal":"ARE NOT EQUAL!");
    printf("crcC_combine_2: %08x\n", crcC_combine_2);

    uint32_t crcA_xor = crcC_combine ^ crcB;
    uint32_t crcA_combine_return = crc32_combine_return_crcA0(crcA, crcB, crcB_size);
    printf("crcA_xor: %08x, crcA_combine_return: %08x, %s\n", crcA_xor, crcA_combine_return,
            crcA_xor==crcA_combine_return? "are equal":"ARE NOT EQUAL!");
    
    // Test crc32_add0 to calculate CRC(A') where A' is A with lengthB zeros appended
    uint32_t crcA_combine_no_crcB = crc32_add0(crcA, crcB_size);
    printf("crcA_add0: %08x, %s\n", crcA_combine_no_crcB,
            crcA_combine_no_crcB==crcA_combine_return? "are equal":"ARE NOT EQUAL!");
    
    // Test crc32_remove0 
    uint32_t crcA_remove0 = crc32_remove0(crcA_xor, crcB_size);
    printf("crcA_remove0: %08x, %s\n", crcA_remove0,
            crcA_remove0==crcA? "are equal":"ARE NOT EQUAL!");

    uint32_t crcA_rem0 = crc32_rem0(crcA_xor, crcB_size);
    printf("crcA_rem0: %08x, %s\n", crcA_rem0,
            crcA_rem0==crcA? "are equal":"ARE NOT EQUAL!");

    // crcB = 0;
    // uint32_t crcA_combine_no_crcB = crc32_combine_return_crcA(crcA, crcB, crcB_size);
    // printf("crcA_combine_no_crcB: %08x, %s\n", crcA_combine_no_crcB,
    //         crcA_combine_no_crcB==crcA_combine_return? "are equal":"ARE NOT EQUAL!");

#endif // CRC32_USE_LOOKUP_TABLE_BYTE
}
