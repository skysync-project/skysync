#include "metadata.h"

void *xzalloc(size_t size);
off_t file_size(int fd);

int enable_verity(const char *filename) {
	int fd = -1;
	int status = -1;

	struct fsverity_enable_arg arg = {
		.version = 1,
		.hash_algorithm = FS_VERITY_HASH_ALG_SHA256,
		.block_size = BLOCK_SIZE,
		.salt_size = 0,
		.salt_ptr = (uintptr_t)NULL,
		.sig_size = 0,
		.__reserved1 = 0,
		.sig_ptr = (uintptr_t)NULL,
		.__reserved2 = {0},
	};

	fd = open(filename, O_RDONLY, 0);
	if(fd < 0) {
		fprintf(stderr, "open file %s failed: %s\n", filename, strerror(errno));
		return -1;
	}

	status = ioctl(fd, FS_IOC_ENABLE_VERITY, &arg);
	if (status != 0) {
        if (errno == EEXIST) {
            // fs-verity already enabled - this is not an error
            status = 0;
        } else if (errno == ENOTTY) {
            fprintf(stderr, "fs-verity not supported on this filesystem\n");
            status = -1;
        } else if (errno == EINVAL) {
            fprintf(stderr, "Invalid fs-verity parameters\n");
            status = -1;
        } else {
            fprintf(stderr, "Failed to enable fs-verity on '%s': %s\n", 
                   filename, strerror(errno));
            status = -1;
        }
    }

	close(fd);
	return status;
}

void* dump_digs(const char *filename) {
	int fd = -1;
	void *buf = NULL;
	uint8_t *_digs = NULL;
	struct digests *digs = NULL;
	int bytes_read;

    fd = open(filename, O_RDONLY, 0);
    if(fd < 0) {
        fprintf(stderr, "Failed to open file '%s': %s\n", filename, strerror(errno));
        return NULL;
    }

	off_t fs = file_size(fd);
	assert(fs > 0);

	uint64_t digest_nums = (fs / BLOCK_SIZE) + 1;
	uint64_t length = digest_nums * 32; // 32 is the length of SHA256 digest
	_digs = (uint8_t *)xzalloc(length);
	uint64_t digs_offset = 0;

	struct fsverity_read_metadata_arg arg = {
		.metadata_type = FS_VERITY_METADATA_TYPE_MERKLE_TREE,
		.offset = 0,
		.length = BUFF_SIZE,
		.buf_ptr = (uintptr_t)NULL,
		.__reserved = 0,
	};


    buf = xzalloc(arg.length);
    arg.buf_ptr = (uintptr_t)buf;

	while (1) {
		bytes_read = ioctl(fd, FS_IOC_READ_VERITY_METADATA, &arg);

		if (bytes_read < 0) {
			fprintf(stderr, "Failed to read fs-verity metadata: %s\n", strerror(errno));
            goto err;
		}
		if (bytes_read == 0)
			break;

		uint8_t *tmp = (uint8_t *)buf;
        memcpy(_digs + digs_offset, tmp, bytes_read);
        digs_offset += bytes_read;

        // if (digs_offset > length) { // Check for overflow after copying
        //     fprintf(stderr, "Digest buffer overflow - metadata larger than expected\n");
        //     goto err;
        // }

		arg.offset += bytes_read;
	}

	digs = (struct digests *)malloc(sizeof(struct digests));
	digs->digest = _digs;
	digs->digs_nums = digest_nums;
	digs->digs_len = 32; // SHA256 length

	free(buf);
	close(fd);
	return digs;

err:
	if (_digs)
		free(_digs);
	if (buf)
		free(buf);
	if (fd >= 0)
		close(fd);
	return NULL;
}