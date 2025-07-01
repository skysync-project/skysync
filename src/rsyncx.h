#ifndef RSYNCX_H
#define RSYNCX_H

#ifdef  __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <popt.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <libgen.h>
#include <errno.h>
#include <time.h>

#include <sys/ioctl.h>
#include <sys/stat.h>
#include <linux/types.h>

// #include "config.h"
// #include "librsync.h"
// #include "isprefix.h"
#include "uthash.h"
// #include "librsync_export.h"

#define FS_VERITY_METADATA_TYPE_MERKLE_TREE	1
#define FS_VERITY_METADATA_TYPE_DESCRIPTOR	2
#define FS_VERITY_METADATA_TYPE_SIGNATURE	3

#define FSVERITY_UTILS_MAJOR_VERSION	1
#define FSVERITY_UTILS_MINOR_VERSION	5

#define FS_VERITY_HASH_ALG_SHA256       1
#define FS_VERITY_HASH_ALG_SHA512       2

/* local file name suffix */
#define DIGS_FILE "digs"
#define DELTA_FILE "delta"
#define PATCH_FILE "patch"
/* remote file name suffix */
#define DIGS_FILE_REMOTE "rdigs"
#define DELTA_FILE_REMOTE "rdelta"
#define PATCH_FILE_REMOTE "rpatch"

#define RSYNC_FILE "rsync"
#define RSYNCX_FILE "rsyncx"

struct stats {
	uint64_t total_read_bytes;
	uint64_t total_write_bytes;
	clock_t calculate_time;
};

struct fsverity_enable_arg {
    __u32 version;
    __u32 hash_algorithm;
    __u32 block_size;
    __u32 salt_size;
    __u64 salt_ptr;
    __u32 sig_size;
    __u32 __reserved1;
    __u64 sig_ptr;
    __u64 __reserved2[11];
};

struct fsverity_read_metadata_arg {
	__u64 metadata_type;
	__u64 offset;
	__u64 length;
	__u64 buf_ptr;
	__u64 __reserved;
};

#define FS_IOC_ENABLE_VERITY	_IOW('f', 133, struct fsverity_enable_arg)
#define FS_IOC_READ_VERITY_METADATA \
	_IOWR('f', 135, struct fsverity_read_metadata_arg)

/* ========== String utilities ========== */

char bin2hex_char(uint8_t nibble);

void bin2hex(const uint8_t *bin, size_t bin_len, char *hex);

/* ========== Memory allocation ========== */

void *xzalloc(size_t size);

void *xmemdup(const void *mem, size_t size);

char *xstrdup(const char *s);

void print_stats(struct stats *st);
/* ===============Dump digests=============== */

struct digests {
	uint8_t *digest;
	uint64_t digs_nums;
	uint8_t digs_len;
};

off_t file_size(int fd);

void print_digs(const struct digests *digs);

/* Enable fs-verity on the given file. */
int enable_verity(const char *filename);

/* Dump the fs-verity metadata (block digests) to array */
void* dump_digs(const char *filename, struct stats *st);

/* Dump the fs-verity metadata (block digests) to a file */
int dump_digs2file(const char *filename, const char *digs_file, struct stats *st);

void digest2file(const char *filename, const uint8_t *data,
				uint32_t block_size, const char *op);

// void *generate_digs(const char *filename);

/* ===============Hash table ops=============== */

struct uthash {
	uint8_t digest[32];                    /* key */
    uint64_t blk_nums;
    UT_hash_handle hh;         /* makes this structure hashable */
};

void print_uthash_table(struct uthash **hash_table);

void delete_uthash_table(struct uthash **hash_table);

void build_uthash_table(uint8_t *sigs, struct uthash **hash_table, uint64_t digs_num, size_t sigs_len);

void* match_digs(struct uthash **hash_table, size_t digs_len, struct digests *new_digs);

/* ===============Generate delta=============== */

struct changed_blk {
	uint64_t *blk;
	uint64_t blk_nums;
};

static const __u64 read_length = 32768;

/* Read the fs-verity metadata (block digests) from a file */
void* read_digs(const char *filename, size_t digs_len, struct stats *st);

void* add_delta_header(const uint64_t blk_len, const struct changed_blk *c_blk);

void generate_delta2file(const char *new_file, const char *delta_file,
					const uint64_t blk_len, const struct changed_blk *c_blk, struct stats *st);

/* ===============Patch delta=============== */

void patch_delta(const char *old_file, const char *delta_file, const char *output_file, struct stats *st);


#ifdef  __cplusplus
}
#endif

#endif // RSYNCX_H
