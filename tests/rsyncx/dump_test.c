#include "rsyncx.h"
#include <time.h>

int main(int argc, char **argv) {

    if (argc < 3) {
        printf("Usage: %s <filename> <digest file>\n", argv[0]);
        exit(1);
    }

    printf("dump file %s digests\n", argv[1]);

    struct stats *st = malloc(sizeof(struct stats));
    memset(st, 0, sizeof(struct stats));

    clock_t start, end;
    start = clock();
    enable_verity(argv[1]);

    if (dump_digs2file(argv[1], argv[2], st) != 0) {
        printf("dump digests failed\n");
        exit(1);
    }
    end = clock();

    // print_stats(st);
    free(st);

    printf("dump digests time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);

}
