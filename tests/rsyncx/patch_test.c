#include "rsyncx.h"

int main(int argc, char **argv) {
    if (argc < 5) {
        printf("Usage: %s patch <old file> <delta file> <output file>\n", argv[0]);
        exit(1);
    }

    if (strcmp(argv[1], "patch") != 0) {
        printf("Usage: %s patch <old file> <delta file> <output file>\n", argv[0]);
        exit(1);
    }

    struct stats *st = malloc(sizeof(struct stats));
    memset(st, 0, sizeof(struct stats));

    clock_t start, end;
    start = clock();
    patch_delta(argv[2], argv[3], argv[4], st);
    end = clock();
    printf("patch delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);

    // print_stats(st);
    free(st);

}
