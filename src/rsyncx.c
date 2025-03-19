#include "rsyncx.h"

void print_stats(struct stats *st) {
	printf("calculate time: %f\n", (double)st->calculate_time / CLOCKS_PER_SEC);
	// printf("total read bytes: %lu\n", st->total_read_bytes);
	// printf("total write bytes: %lu\n", st->total_write_bytes);
}

void print_digs(const struct digests *digs) {
	uint64_t length = digs->digs_nums * digs->digs_len;
	for (uint32_t i = 0; i < length; i++) {
		if (!digs->digest[i])
			continue;
		printf("%02x", digs->digest[i]);
		if (i % digs->digs_len == digs->digs_len - 1)
			printf("\n");
	}
}

int enable_verity(const char *filename) {
	int fd;
	int status;
	struct fsverity_enable_arg arg = {
		.version = 1,
		.hash_algorithm = FS_VERITY_HASH_ALG_SHA256,
		.block_size = 4096,
		.salt_size = 0,
		.salt_ptr = (uintptr_t)NULL,
		.sig_size = 0,
		.__reserved1 = 0,
		.sig_ptr = (uintptr_t)NULL,
		.__reserved2 = {0},
	};

	fd = open(filename, O_RDONLY, 0);
	if(fd < 0) {
		printf("open file %s failed.", filename);
		exit(1);
	}

	status = ioctl(fd, FS_IOC_ENABLE_VERITY, &arg);
	if(status != 0) {
		if (errno == EEXIST) {
			// printf("ioctl: FS_IOC_ENABLE_VERITY: %s\n", strerror(errno));
			status = 0;
		}
		else {
			perror("ioctl: FS_IOC_ENABLE_VERITY");
			goto err;
		}
	}

out:
	close(fd);
	return status;
err:
	status = 1;
	goto out;
}

void* dump_digs(const char *filename, struct stats *st) {
	int fd;
	void *buf = NULL;
	int bytes_read;

    fd = open(filename, O_RDONLY, 0);
    if(fd < 0) {
        perror("open");
        goto err;
    }

	off_t fs = file_size(fd);
	assert(fs > 0);
	uint64_t digest_nums = (fs / 4096) + 1;
	uint64_t length = digest_nums * 32;
	uint8_t *_digs = xzalloc(length);
	uint64_t digs_offset = 0;

	struct fsverity_read_metadata_arg arg = {
		.metadata_type = FS_VERITY_METADATA_TYPE_MERKLE_TREE,
		.offset = 0,
		// .length = 32768,
		.length = length,
		.__reserved = 0,
	};


    buf = xzalloc(arg.length);
    arg.buf_ptr = (uintptr_t)buf;

	do {
		clock_t start = clock();
		bytes_read = ioctl(fd, FS_IOC_READ_VERITY_METADATA, &arg);
		st->calculate_time += clock() - start;
		st->total_read_bytes += bytes_read;

		if (bytes_read < 0) {
			perror("ioctl");
			goto err;
		}
		if (bytes_read == 0)
			break;

		arg.offset += bytes_read;

		uint8_t *tmp = (uint8_t *)buf;
		for (uint32_t i = 0; i < bytes_read; i++) {
			if (!tmp[i])
				continue;
			if (digs_offset >= length) {
				printf("digs_offset is out of range\n");
				break;
			}
			memcpy(&_digs[digs_offset++], &tmp[i], 1);
		}
	} while (1);

	struct digests *digs = malloc(sizeof(struct digests));
	digs->digest = _digs;
	digs->digs_nums = digest_nums;

out:
	if (buf)
		free(buf);
	close(fd);
	return digs;

err:
	if (_digs)
		free(_digs);
	_digs = NULL;
	goto out;
}

int dump_digs2file(const char *filename, const char *digs_file, struct stats *st) {
    int fd, digs_fd;
	void *buf = NULL;
	int status;
	int bytes_read;

	struct fsverity_read_metadata_arg arg = {
		.metadata_type = FS_VERITY_METADATA_TYPE_MERKLE_TREE,
		.offset = 0,
		.length = 32768,
		.__reserved = 0,};

    buf = xzalloc(arg.length);
    arg.buf_ptr = (uintptr_t)buf;

    fd = open(filename, O_RDONLY, 0);
    if(fd < 0) {
        perror("open file failed\n");
        goto err;
    }

	digs_fd = open(digs_file, O_WRONLY | O_CREAT, 0644);
	if (digs_fd < 0) {
		perror("open digest file failed\n");
		goto err;
	}

	off_t fs = file_size(fd);
	assert(fs > 0);
	uint64_t digest_nums = (fs / 4096) + 1;

	size_t n = write(digs_fd, &digest_nums, sizeof(digest_nums));
	st->total_write_bytes += n;

	if (n < 0) {
		perror("write digs_nums failed\n");
		goto err;
	}

	do {
		clock_t start = clock();
		bytes_read = ioctl(fd, FS_IOC_READ_VERITY_METADATA, &arg);
		st->calculate_time += clock() - start;
		st->total_read_bytes += bytes_read;

		if (bytes_read < 0) {
			perror("ioctl");
			goto err;
		}
		if (bytes_read == 0)
			break;

		arg.offset += bytes_read;
		// digest2file(filename, buf, bytes_read, "dig");
		size_t n = write(digs_fd, buf, bytes_read);
		st->total_write_bytes += n;

		if (n < 0) {
			perror("write file digests failed\n");
			goto err;
		}

	} while (1);

	status = 0;
out:
	if (buf)
		free(buf);
	close(fd);
	close(digs_fd);
	return status;

err:
	status = 1;
	goto out;
}

void digest2file(const char *filename, const uint8_t *data,
				uint32_t block_size, const char *op) {
    // filename = basename(filename);
	uint32_t digest_filename_len = strlen(filename) + strlen(op) + 9;
	char *digest_filename = malloc(digest_filename_len);
	snprintf(digest_filename, digest_filename_len, "%s.%s.digest", filename, op);

	FILE *fd;
	fd = fopen(digest_filename, "w");
	if (fd == NULL) {
		printf("open %s failed\n", digest_filename);
		exit(1);
	}

	uint32_t digest_size = 32;
	char *name = "digest: ";
	char digest[64];
	uint8_t buf[64];
	memset(buf, 0, sizeof(buf));
	for(uint32_t i=0; i<block_size; i++) {
		if (data[i] == 0)
			continue;
		memcpy(&buf[i % digest_size], &data[i], 1);
		if (i % digest_size == digest_size - 1) {
			bin2hex(buf, digest_size, digest);
			// fwrite(name, sizeof(char), strlen(name), fd);
			// fwrite(digest, sizeof(char), sizeof(digest), fd);
			// fwrite("\n", sizeof(char), 1, fd);
			fprintf(fd, "%s%s\n", name, digest);
			memset(buf, 0, sizeof(buf));
			}
	}
	free(digest_filename);
	fclose(fd);
}

// void *generate_digs(const char *filename) {
// 	printf("generate digests for file %s\n", filename);
// 	clock_t start, end;
// 	start = clock();
// 	if(enable_verity(filename) != 0) {
// 		printf("enable verity failed\n");
// 		exit(1);
// 	}
// 	struct digests *digs = dump_digs(filename);
// 	end = clock();
// 	printf("generate digests time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
// 	return digs;
// }

/* ======================================= */

void* read_digs(const char *filename, size_t digs_len, struct stats *st) {
	FILE *fd;
	int bytes_read;

	if ((digs_len != 32) && (digs_len != 64)) {
		printf("digs_len must be 32 or 64\n");
		exit(1);
	}

	fd = fopen(filename, "rb");
	if (fd == NULL) {
		printf("open %s failed\n", filename);
		exit(1);
	}

	uint64_t digs_nums;
	if (fread(&digs_nums, sizeof(digs_nums), 1, fd) != 1) {
		printf("read digs_nums failed\n");
		goto err;
	}

	assert(digs_nums > 0);
	uint8_t *digs_tmp = xzalloc(digs_nums * digs_len * sizeof(uint8_t));
	uint8_t *head = digs_tmp;
	int bytes_read_total = 0;

	while (1) {
		bytes_read = fread(digs_tmp, sizeof(digs_tmp[0]), digs_nums * digs_len, fd);
		if (bytes_read < 0) {
			perror("read");
			goto err;
		}
		if (bytes_read == 0)
			break;
		bytes_read_total += bytes_read;
		digs_tmp += bytes_read;
		if (bytes_read_total >= digs_nums * digs_len)
			break;
	}

	st->total_read_bytes += bytes_read_total;

	struct digests *digs = malloc(sizeof(struct digests));
	digs->digest = head;
	digs->digs_nums = digs_nums;
	digs->digs_len = digs_len;

out:
	fclose(fd);
	return digs;
err:
	if (head)
		free(head);
	head = NULL;
	goto out;
}

void delete_uthash_table(struct uthash **hash_table) {
	struct uthash *current, *tmp;
	HASH_ITER(hh, *hash_table, current, tmp) {
		HASH_DEL(*hash_table, current);
		free(current);
	}
}

void print_uthash_table(struct uthash **hash_table) {
	struct uthash *s;
	uint32_t count_items = HASH_COUNT(*hash_table);
	printf("There are %d items in uthash table.\n", count_items);

	for(s = *hash_table; s != NULL; s = (struct uthash*)s->hh.next) {
		printf("digest: ");
		for (int i = 0; i < 32; i++) {
			printf("%02x", s->digest[i]);
		}
		printf(" block number: %ld", s->blk_nums);
		printf("\n");
	}
}

void build_uthash_table(uint8_t *digs, struct uthash **hash_table, uint64_t digs_num, size_t digs_len) {
	// struct uthash *hash_table = NULL;
	struct uthash *s;
	uint8_t digest[digs_len];
	uint16_t block_nums = 0;

	memset(digest, 0, sizeof(digest));
	uint32_t i = 0;
	while (i < (digs_num * digs_len)) {
		if (!digs[i]) {
			i++;
			continue;
		}
		memcpy(digest, &digs[i], digs_len);

		/* The key should be unique.
		 * Now I asume that the digests of one file are unique. */
		HASH_FIND(hh, *hash_table, digest, digs_len, s);
		if (s == NULL) {
			s = malloc(sizeof(struct uthash));
			/*The key is the digest of the block */
			memcpy(s->digest, digest, digs_len);
			/*The value is the block number for generating the digest */
			s->blk_nums = block_nums++;
			HASH_ADD(hh, *hash_table, digest, digs_len, s);
		}
		memset(digest, 0, sizeof(digest));
		i += digs_len;
	}
}

void* match_digs(struct uthash **hash_table, size_t digs_len, struct digests *new_digs) {
	struct uthash *s;
	uint8_t digest[32];
	uint64_t digs_nums = new_digs->digs_nums;
	uint64_t blk[digs_nums];
	uint64_t blk_nums = 0;
	uint64_t changed_blk_nums = 0;

	memset(digest, 0, sizeof(digest));
	memset(blk, 0, sizeof(uint64_t) * digs_nums);

	uint32_t i = 0;
	while (i < (digs_nums * digs_len)) {
		if (!new_digs->digest[i])
			goto err;
		memcpy(digest, &new_digs->digest[i], digs_len);

		HASH_FIND(hh, *hash_table, digest, digs_len, s);
		if (s == NULL) {
			blk[blk_nums] = 1;
			changed_blk_nums++;
		}
		memset(digest, 0, sizeof(digest));
		i += digs_len;
		blk_nums++;
	}

	struct changed_blk *changed_blk = malloc(sizeof(struct changed_blk));
	changed_blk->blk = malloc(sizeof(uint64_t) * changed_blk_nums);

	if (!changed_blk_nums) {
		changed_blk->blk_nums = 0;
		return changed_blk;
	}

	for (uint64_t i = 0, j = 0; i < digs_nums; i++) {
		if (blk[i] == 1)
			changed_blk->blk[j++] = i;
	}

	changed_blk->blk_nums = changed_blk_nums;
	return changed_blk;

err:
	return NULL;
}

/* ======================================= */

void* add_delta_header(const uint64_t blk_len, const struct changed_blk *c_blk) {
	/*header: "header_len" "block_len" "block 1" ... "block <block_nums_len>"   */
	uint64_t header_len = sizeof(uint64_t) * (c_blk->blk_nums + 1 + 1);
	uint64_t *header = xzalloc(header_len);
	memcpy(header, &header_len, sizeof(uint64_t));
	memcpy(&header[1], &blk_len, sizeof(uint64_t));
	memcpy(&header[2], c_blk->blk, sizeof(uint64_t) * c_blk->blk_nums);
	return header;
}

void generate_delta2file(const char *new_file, const char *delta_file,
					const uint64_t blk_len, const struct changed_blk *c_blk, struct stats *st) {
	FILE *new_fd;
	int delta_fd;
	int bytes_read;

	if (!c_blk->blk_nums) {
		printf("No changed blocks in the new file \"%s\"\n", new_file);
		return;
	}

	uint64_t *header = add_delta_header(blk_len, c_blk);;

	new_fd = fopen(new_file, "rb");
	if (new_fd == NULL) {
		printf("open new file \"%s\" failed\n", new_file);
		exit(1);
	}

	delta_fd = open(delta_file, O_WRONLY | O_CREAT, 0644);
	if (delta_fd < 0) {
		printf("open delta file \"%s\" failed\n", delta_file);
		exit(1);
	}

	/* Write the header to the delta file */
	// size_t written_data = fwrite(header, sizeof(header[0]), c_blk->blk_nums + 1, delta_fd);
	size_t res = write(delta_fd, header, sizeof(header[0]) * (c_blk->blk_nums + 1 + 1));
	if (res != sizeof(header[0]) * (c_blk->blk_nums + 1 + 1)) {
		printf("write header to delta file \"%s\" failed\n", delta_file);
		goto err;
	}
	st->total_read_bytes += res;

	/* Write the changed block of the new file to the delta file*/
	for (uint64_t i = 0; i < c_blk->blk_nums; i++) {
		uint64_t offset = c_blk->blk[i] * blk_len;
		fseek(new_fd, offset, SEEK_SET);
		uint8_t buf[blk_len];
		memset(buf, 0, sizeof(buf));
		bytes_read = fread(buf, sizeof(buf[0]), blk_len, new_fd);
		if (bytes_read < 0) {
			printf("read new file \"%s\" failed\n", new_file);
			goto err;
		}
		if (bytes_read == 0)
			break;
		// if (!fwrite(buf, sizeof(buf[0]), bytes_read, delta_fd)) {
		if (write(delta_fd, buf, sizeof(buf[0]) * bytes_read) != sizeof(buf[0]) * bytes_read) {
			printf("write changed blocks to delta file \"%s\" failed\n", delta_file);
			goto err;
		}

		st->total_read_bytes += bytes_read;
		st->total_write_bytes += sizeof(buf[0]) * bytes_read;
	}

out:
	free(header);
	fclose(new_fd);
	close(delta_fd);
	return;
err:
	goto out;
}

void patch_delta(const char *old_file, const char *delta_file, const char *output_file, struct stats *st) {
	FILE *old_fd, *output_fd, *delta_fd;
	int bytes_read;
	uint64_t header_length;
	uint64_t blk_length;
	uint64_t *blk;

	old_fd = fopen(old_file, "rb");
	delta_fd = fopen(delta_file, "rb");
	output_fd = fopen(output_file, "wb");

	if (!old_fd || !delta_fd || !output_fd) {
		printf("open file failed\n");
		exit(1);
	}

	/* Read the header from the delta file */
	bytes_read = fread(&header_length, sizeof(header_length), 1, delta_fd);
	if (bytes_read < 0) {
		printf("read header from delta file \"%s\" failed\n", delta_file);
		goto err;
	}
	st->total_read_bytes += bytes_read;

	/* Read the block length from the delta file */
	bytes_read = fread(&blk_length, sizeof(blk_length), 1, delta_fd);
	if (bytes_read < 0) {
		printf("read block length from delta file \"%s\" failed\n", delta_file);
		goto err;
	}
	st->total_read_bytes += bytes_read;

	/* Read the changed blocks from the delta file */
	uint64_t blk_nums = (header_length - sizeof(header_length) - sizeof(blk_length)) / sizeof(uint64_t);
	blk = xzalloc(sizeof(uint64_t) * blk_nums);
	uint64_t *tmp = blk;

	bytes_read = fread(blk, sizeof(uint64_t), blk_nums, delta_fd);
	if (bytes_read < 0) {
		printf("read changed blocks from delta file \"%s\" failed\n", delta_file);
		goto err;
	}
	st->total_read_bytes += bytes_read;

	uint64_t old_blk_nums = (file_size(old_fd) / blk_length) + 1;
	for (uint64_t i = 0; i < old_blk_nums; i++) {
		// if (i * blk_length >= rs_file_size(old_fd))
		// 	break;
		if (blk_nums && (i * blk_length == blk[0] * blk_length)) {
			/* Write the changed blocks from the delta file to the output file */
			uint8_t buf[blk_length];
			memset(buf, 0, sizeof(buf));

			bytes_read = fread(buf, sizeof(buf[0]), blk_length, delta_fd);
			st->total_read_bytes += bytes_read;

			if (bytes_read < 0) {
				printf("read changed blocks from delta file \"%s\" failed\n", delta_file);
				goto err;
			}
			if (bytes_read == 0)
				break;
			if (fwrite(buf, sizeof(buf[0]), bytes_read, output_fd) != bytes_read) {
				printf("write changed blocks to output file \"%s\" failed\n", output_file);
				goto err;
			}
			blk++;
			blk_nums--;

			st->total_write_bytes += bytes_read;
			continue;
		}
		/* Write the unchanged blocks from the old file to the output file */
		uint8_t buf[blk_length];
		memset(buf, 0, sizeof(buf));
		fseek(old_fd, i * blk_length, SEEK_SET);
		bytes_read = fread(buf, sizeof(buf[0]), blk_length, old_fd);
		if (bytes_read < 0) {
			printf("read old file \"%s\" failed\n", old_file);
			goto err;
		}
		if (bytes_read == 0)
			break;
		if (fwrite(buf, sizeof(buf[0]), bytes_read, output_fd) != bytes_read) {
			printf("write unchanged blocks to output file \"%s\" failed\n", output_file);
			goto err;
		}

		st->total_read_bytes += bytes_read;
		st->total_write_bytes += bytes_read;
	}

	/* Write the new blocks from the delta file to the output file */
	if (blk_nums) {
		for (uint64_t i = 0; i < blk_nums; i++) {
			uint8_t buf[blk_length];
			memset(buf, 0, sizeof(buf));
			bytes_read = fread(buf, sizeof(buf[0]), blk_length, delta_fd);
			if (bytes_read < 0) {
				printf("read changed blocks from delta file \"%s\" failed\n", delta_file);
				goto err;
			}
			if (bytes_read == 0)
				break;
			if (fwrite(buf, sizeof(buf[0]), bytes_read, output_fd) != bytes_read) {
				printf("write changed blocks to output file \"%s\" failed\n", output_file);
				goto err;
			}
			st->total_read_bytes += bytes_read;
			st->total_write_bytes += bytes_read;
		}
	}

	printf("output file: %s, fils size: %ld.\n", output_file, rs_file_size(output_fd));

out:
	if (tmp)
		free(tmp);
	fclose(old_fd);
	fclose(output_fd);
	fclose(delta_fd);
	return;
err:
	goto out;

}
