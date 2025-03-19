#include "fastcdc.h"
#include <time.h>

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <file> <chunking method>: \n"
            "\t 0: origin fastcdc \n"
            "\t 1: rolling 2 bytes \n"
            "\t 2: normalized fastcdc \n"
            "\t 3: normalized 2 bytes\n", argv[0]);
        exit(1);
    }

    /* write_file_path = file_name + "-fastcdc" */
    char fastfp_file_path[strlen(argv[1]) + 20];
    strcpy(fastfp_file_path, argv[1]);
    strcat(fastfp_file_path, "-fastcdc-fastfp");

    struct stats stats = {
        .read_io_bytes = 0,
        .write_io_bytes = 0,
    };

    /* receiver */
    run_fastfp(argv[1], fastfp_file_path, atoi(argv[2]), &stats);
}
