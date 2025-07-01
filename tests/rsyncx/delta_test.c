#include "rsyncx.h"
#include <time.h>

int main(int argc, char **argv) {

    if (argc < 5) {
        printf("Usage: %s delta <sigs file> <new file> <delta file>\n", argv[0]);
        exit(1);
    }

    if (strcmp(argv[1], "delta") != 0) {
        printf("Usage: %s delta <sigs file> <new file> <delta file>\n", argv[0]);
        exit(1);
    }

    printf("generate delta file to %s\n", argv[4]);

    struct stats *st = malloc(sizeof(struct stats));
    memset(st, 0, sizeof(struct stats));

    clock_t start, end;
    start = clock();

    size_t digs_len = 32;
    struct digests *old_digs = NULL;
    old_digs = read_digs(argv[2], digs_len, st);
    if (!old_digs) {
        printf("read sigs failed\n");
        exit(1);
    }

    // print_digs(old_digs);

    struct uthash *hash_table = NULL;
    build_uthash_table(old_digs->digest, &hash_table, old_digs->digs_nums, digs_len);

    // print_uthash_table(&hash_table);

    if (enable_verity(argv[3]) != 0) {
        printf("enable verity failed\n");
        delete_uthash_table(&hash_table);
        free(old_digs->digest);
        free(old_digs);
        exit(1);
    }

    struct digests *new_digs = dump_digs(argv[3], st);

    // print_digs(new_digs);

    clock_t start2 = clock();
    struct changed_blk *c_blk = match_digs(&hash_table, digs_len, new_digs);
    st->calculate_time += clock() - start2;

    generate_delta2file(argv[3], argv[4], 4096, c_blk, st);

    end = clock();
    printf("generate delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);

    // print_stats(st);
    free(st);

    delete_uthash_table(&hash_table);
    free(c_blk->blk);
    free(c_blk);
    free(new_digs->digest);
    free(new_digs);
    free(old_digs->digest);
    free(old_digs);
}
