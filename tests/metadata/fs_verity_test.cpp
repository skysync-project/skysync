#include "metadata.h"
#include <time.h>

int main(int argc, char **argv) {

    if (argc < 2) {
        printf("Usage: %s <filename>\n", argv[0]);
        exit(1);
    }

    printf("Load fs-verity metadata from file: %s\n", argv[1]);

    clock_t start, end;
    // enable_verity(argv[1]);
    if (enable_verity(argv[1]) != 0) {
        fprintf(stderr, "Failed to enable fs-verity on file '%s'\n", argv[1]);
        exit(1);
    }

    start = clock();
    struct digests *digs = (struct digests *)dump_digs(argv[1]);
    if (digs == NULL) {
        fprintf(stderr, "Failed to dump digests from file '%s'\n", argv[1]);
        exit(1);
    }
    end = clock();

    printf("Dump digests successfully.\n");
    printf("Time taken: %.8f seconds\n", (double)(end - start) / CLOCKS_PER_SEC);

    free(digs->digest);
    free(digs);
    return 0;
}