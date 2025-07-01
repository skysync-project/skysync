#ifndef METADATA_H
#define METADATA_H

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

#define BLOCK_SIZE	4096
#define BUFF_SIZE	32768

#define FS_VERITY_METADATA_TYPE_MERKLE_TREE	1
#define FS_VERITY_METADATA_TYPE_DESCRIPTOR	2
#define FS_VERITY_METADATA_TYPE_SIGNATURE	3

#define FSVERITY_UTILS_MAJOR_VERSION	1
#define FSVERITY_UTILS_MINOR_VERSION	5

#define FS_VERITY_HASH_ALG_SHA256       1
#define FS_VERITY_HASH_ALG_SHA512       2

struct digests {
	uint8_t *digest;
	uint64_t digs_nums;
	uint8_t digs_len;
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
#define FS_IOC_MEASURE_VERITY	_IOWR('f', 134, struct fsverity_digest)
#define FS_IOC_READ_VERITY_METADATA \
	_IOWR('f', 135, struct fsverity_read_metadata_arg)

/* Enable fs-verity on the given file. */
int enable_verity(const char *filename);

/* Dump the fs-verity metadata (block digests) to array */
void* dump_digs(const char *filename);


#ifdef  __cplusplus
}
#endif

#endif // METADATA_H