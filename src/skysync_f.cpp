#include <assert.h>
#include "skysync_f.h"
#include "blake3.h"

off_t file_size(int fd);
int is_zero(void *buf, uint64_t size);
void *xzalloc(uint64_t size);
void *map_file(int fd);
void unmap_file(int fd, void *map);

void crr_build_uthash(struct crr_uthash **hash_table, struct crr_csums *old_csums,
                        uint64_t chunk_nums) {
    struct crr_uthash *s;

    for(int i = 0; i < chunk_nums; i++) {
        s = NULL;
        uint32_t weak_csum = old_csums->all_csums[i].weak_csum;
        HASH_FIND(hh, *hash_table, &weak_csum, sizeof(uint32_t), s);
  
        if(s == NULL) {
            s = (struct crr_uthash *) xzalloc(sizeof(struct crr_uthash));
  
            memcpy(&s->weak_csum, &weak_csum, sizeof(uint32_t));
            s->csum = (struct one_csum *) xzalloc(sizeof(struct one_csum) * ITEMNUMS);
            memcpy(&s->csum[0], &old_csums->all_csums[i], sizeof(struct one_csum));
            s->item_nums = 1;
            s->remalloc = 1;

            HASH_ADD(hh, *hash_table, weak_csum, sizeof(uint32_t), s);
        } else {
            if(s->item_nums >= (s->remalloc * ITEMNUMS)) {
                s->remalloc += 1;
                struct one_csum * crr_tmp = (struct one_csum *) xzalloc(sizeof(struct one_csum)
                                                                                * s->remalloc * ITEMNUMS);
                memcpy(crr_tmp, s->csum, sizeof(struct one_csum) * s->item_nums);
                free(s->csum);
                s->csum = crr_tmp;
            }
            memcpy(&s->csum[s->item_nums], &old_csums->all_csums[i], sizeof(struct one_csum));
            s->item_nums += 1;
        }
    }
}

void crr_delete_uthash(struct crr_uthash **hash_table) {
    struct crr_uthash *current_entry, *tmp;
    HASH_ITER(hh, *hash_table, current_entry, tmp) {
        HASH_DEL(*hash_table, current_entry);
        free(current_entry->csum);
        free(current_entry);
    }
}

void crr_print_uthash(struct crr_uthash **hash_table) {
    struct crr_uthash *current_entry, *tmp;
    uint32_t count_items = HASH_COUNT(*hash_table);
    printf("There are %d items in crr uthash table\n", count_items);
    HASH_ITER(hh, *hash_table, current_entry, tmp) {
        printf("weak_csum: ");
        printf("%08x", current_entry->weak_csum);

        printf("\n");
        printf("item_nums: %ld\n", current_entry->item_nums);
        for(int i = 0; i < current_entry->item_nums; i++) {
            printf("chunk_num: %d\n", current_entry->csum[i].chunk_num);
            printf("csum: ");
            for(int j = 0; j < SHA256_DIGEST_LENGTH; j++) {
                printf("%02x", current_entry->csum[i].csum[j]);
            }
            printf("\n");
        }
    }
}

uint8_t* crr_calc_sha256(uint8_t *buf, uint32_t len) {
    uint8_t *crr_sha256 = (uint8_t *) xzalloc(SHA256_DIGEST_LENGTH);
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, buf, len);
    EVP_DigestFinal_ex(ctx, crr_sha256, NULL);
    EVP_MD_CTX_free(ctx);
    return crr_sha256;
}

uint8_t* crr_calc_blake3(uint8_t *buf, uint32_t len) {
    uint8_t *crr_blake3 = (uint8_t *) xzalloc(BLAKE3_OUT_LEN);
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, buf, len);
    blake3_hasher_finalize(&hasher, crr_blake3, BLAKE3_OUT_LEN);
    return crr_blake3;
}


uint32_t crr_calc_crc32c(uint8_t *buf, uint32_t len) {
    uint32_t crr_crc32c = 0;
    // software implementation for crc32_16bytes_prefetch
    /// crr_crc32c = crc32_16bytes_prefetch(buf, len, crr_crc32c, 256);
    // Intel ISA-L CRC32
    crr_crc32c = crc32_isal(buf, len, crr_crc32c);
    
    return crr_crc32c;
}

struct crr_csums* crr_calc_csums_1(int fd, char *map) {
    uint64_t bytes_read = 0;

    uint64_t fp_size = file_size(fd);
    uint64_t num_chunks = 0;
    if (fp_size % CHUNK_SIZE == 0) {
        num_chunks = fp_size / CHUNK_SIZE;
    } else {
        num_chunks = fp_size / CHUNK_SIZE + 1;
    }

    struct crr_csums* ccs = (struct crr_csums *) xzalloc(sizeof(struct crr_csums));

    ccs->f_size = fp_size;
    ccs->chunk_nums = num_chunks;
    ccs->all_csums = (struct one_csum *) xzalloc(sizeof(struct one_csum) * num_chunks);

    uint64_t offset = 0;
    uint64_t chunk_num = 0;
    while(offset < fp_size) {
        uint32_t len = CHUNK_SIZE;
        if (offset + CHUNK_SIZE > fp_size) {
            len = fp_size - offset;
        }

        uint8_t *crr_sha256 = crr_calc_sha256((uint8_t *)map + offset, len);
        memcpy(ccs->all_csums[chunk_num].csum, crr_sha256, SHA256_DIGEST_LENGTH);
        free(crr_sha256);
        
        ccs->all_csums[chunk_num].chunk_num = chunk_num;
        ccs->all_csums[chunk_num].weak_csum = crr_calc_crc32c((uint8_t *)map + offset, len);

        chunk_num += 1;
        offset += CHUNK_SIZE;
    }

    return ccs;
}

struct crr_csums* crr_calc_csums_2(int fd, char *map) {
    uint64_t bytes_read = 0;

    uint64_t fp_size = file_size(fd);
    uint64_t num_chunks = fp_size / CHUNK_SIZE + 1;

    ringbuf_t *file_buf = ringbuf_create(BUFFER_SIZE);

    struct crr_csums* ccs = (struct crr_csums *) xzalloc(sizeof(struct crr_csums));

    ccs->f_size = fp_size;
    ccs->chunk_nums = num_chunks;
    ccs->all_csums = (struct one_csum *) xzalloc(sizeof(struct one_csum) * num_chunks);

    uint64_t offset = 0;
    while(offset < fp_size) {
        uint64_t to_read = (fp_size - offset) < BUFFER_SIZE ? (fp_size - offset) : BUFFER_SIZE;
        bytes_read = ringbuf_read_from_fd(fd, file_buf, to_read);
        if (bytes_read <= 0)
            break;

        uint32_t len = CHUNK_SIZE;
        while (ringbuf_used(file_buf) >= CHUNK_SIZE) {
            uint8_t *buf = (uint8_t *) ringbuf_head(file_buf);
            uint8_t *crr_sha256 = crr_calc_blake3(buf, len);
            memcpy(ccs->all_csums[offset / CHUNK_SIZE].csum, crr_sha256, SHA256_DIGEST_LENGTH);

            ccs->all_csums[offset / CHUNK_SIZE].chunk_num = offset / CHUNK_SIZE;
            // printf("chunk_num: %d\n", ccs->all_csums[offset / CHUNK_SIZE].chunk_num);
            ccs->all_csums[offset / CHUNK_SIZE].weak_csum = crr_calc_crc32c(buf, len);

            offset += CHUNK_SIZE;
            ringbuf_remove(file_buf, CHUNK_SIZE);
        }
    }

    // Ensure any remaining data in the buffer is processed
    if (ringbuf_used(file_buf) > 0) {
        uint8_t *buf = (uint8_t *) ringbuf_head(file_buf);
        uint32_t len = ringbuf_used(file_buf);
        uint8_t *crr_sha256 = crr_calc_blake3(buf, len);
        memcpy(ccs->all_csums[offset / CHUNK_SIZE].csum, crr_sha256, SHA256_DIGEST_LENGTH);

        ccs->all_csums[offset / CHUNK_SIZE].chunk_num = offset / CHUNK_SIZE;
        ccs->all_csums[offset / CHUNK_SIZE].weak_csum = crr_calc_crc32c(buf, len);
        offset += len;
    }
    
    ringbuf_destroy(file_buf);
    return ccs;
}

void crr_write_csums(char *file_path, struct crr_csums *csums, struct stats *st) {
    uint64_t bytes_write = 0;

    int fp = open(file_path, O_WRONLY | O_CREAT, 0644);
    if(fp == -1) {
        fprintf(stderr, "Error opening file %s\n", file_path);
        exit(1);
    }

    bytes_write = write(fp, &csums->f_size, sizeof(uint32_t));
    assert(bytes_write == sizeof(uint32_t));
    // #ifdef IO_PRINT
    //     st.write_io_bytes += bytes_write;
    // #endif

    bytes_write = write(fp, &csums->chunk_nums, sizeof(uint64_t));
    assert(bytes_write == sizeof(uint64_t));
    // #ifdef IO_PRINT
    //     st.write_io_bytes += bytes_write;
    // #endif

    bytes_write = write(fp, csums->all_csums, sizeof(struct one_csum) * csums->chunk_nums);
    assert(bytes_write == sizeof(struct one_csum) * csums->chunk_nums);
    // #ifdef IO_PRINT
    //     st.write_io_bytes += bytes_write;
    // #endif

    close(fp);
    if(csums->all_csums)
        free(csums->all_csums);
    if(csums)
        free(csums);
}

struct crr_csums* crr_read_csums_new(char *file_path, struct stats *st) {
    struct crr_csums* csum = (struct crr_csums *) xzalloc(sizeof(struct crr_csums));
    uint64_t bytes_read = 0, bytes_write = 0;
    uint8_t *file_buf;

    // FILE* fp = fopen(file_path, "rb");
    int fp = open(file_path, O_RDONLY);
    if(fp == -1) {
        fprintf(stderr, "Error opening file %s\n", file_path);
        exit(1);
    }

    bytes_read = read(fp, &csum->f_size, sizeof(uint32_t));
    assert(bytes_read == sizeof(uint32_t));
    // #ifdef IO_PRINT
    //     st.read_io_bytes += bytes_read;
    // #endif

    bytes_read = read(fp, &csum->chunk_nums, sizeof(uint64_t));
    assert(bytes_read == sizeof(uint64_t));
    // #ifdef IO_PRINT
    //     st.read_io_bytes += bytes_read;
    // #endif

    csum->all_csums = (struct one_csum *) xzalloc(sizeof(struct one_csum) * csum->chunk_nums);
    bytes_read = read(fp, csum->all_csums, sizeof(struct one_csum) * csum->chunk_nums);
    assert(bytes_read == sizeof(struct one_csum) * csum->chunk_nums);
    // #ifdef IO_PRINT
    //     st.read_io_bytes += bytes_read;
    // #endif

    close(fp);
    return csum;
}

struct crr_csums* crr_read_csums_old(char *file_path, char *file_csums, struct stats *st) {
    struct crr_csums* btr_csum = (struct crr_csums *) xzalloc(sizeof(struct crr_csums));
    uint64_t bytes_read = 0, bytes_write = 0;
    uint8_t *file_buf;

    FILE* fp = fopen(file_path, "rb");
    if(fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", file_path);
        exit(1);
    }

    FILE* fc = fopen(file_csums, "rb");
    if(fc == NULL) {
        fprintf(stderr, "Error opening file %s\n", file_csums);
        exit(1);
    }

    uint64_t fp_size = file_size(fileno(fp));
    uint64_t fc_size = file_size(fileno(fc));

    file_buf = (uint8_t *) xzalloc(fc_size + 1);
    bytes_read = fread(file_buf, 1, fc_size, fc);
    assert(bytes_read == fc_size);
    #ifdef IO_PRINT
        st.read_io_bytes += bytes_read;
    #endif

    uint64_t num_chunks = fp_size / CHUNK_SIZE;
    btr_csum->all_csums = (struct one_csum *) xzalloc(sizeof(struct one_csum) * num_chunks);
    btr_csum->chunk_nums = num_chunks;
    btr_csum->f_size = fp_size;
    uint64_t offset = 0;

    for(int i = 0; i < num_chunks; i++) {
        struct one_csum *bc = &btr_csum->all_csums[i];
        int j = 0;
        while(j < SHA256_DIGEST_LENGTH) {
            if(file_buf[offset] == '\n' || file_buf[offset] == '\r') {
                offset++;
                continue;
            }
            bc->csum[j] = file_buf[offset];
            offset++;
            j++;
        }
        bc->chunk_num = i;
        #ifdef DEBUG
        printf("Chunk %d: ", i);
        for(int j = 0; j < SHA256_DIGEST_LENGTH; j++) {
            printf(" %02x", bc->csum[j]);
        }
        printf("\n");
        #endif

        memcpy(&bc->weak_csum, &bc->csum[SHA256_DIGEST_LENGTH - 4], 4);
    }

    free(file_buf);
    fclose(fp);
    fclose(fc);
    return btr_csum;
}


int find_uthash_item(struct crr_uthash *s, uint8_t *csum) {
    for(int j = 0; j < s->item_nums; j++)
        if(memcmp(csum, s->csum[j].csum, SHA256_DIGEST_LENGTH) == 0)
            return j;
    return -1;
}

void crr_direct_compare(struct crr_csums *old_csums, struct crr_csums *new_csums) {
    // return NULL;
}

void crr_compare_csums_1(int new_fd, char *delta_file_path, struct crr_csums *old_csums, struct crr_csums *new_csums) {
    uint64_t bytes_read = 0, total_bytes_read = 0;

    int delta_fd = open(delta_file_path, O_WRONLY | O_CREAT, 0644);
    if (delta_fd == -1) {
        fprintf(stderr, "Error opening file %s\n", delta_file_path);
        exit(1);
    }

    struct crr_uthash *old_hash_table = NULL;
    crr_build_uthash(&old_hash_table, old_csums, old_csums->chunk_nums);
    #ifdef DEBUG
        crr_print_uthash(&old_hash_table);
    #endif

    uint64_t new_fp_size = file_size(new_fd);
    uint64_t copy_offset = 0;
    uint64_t copy_len = 0;

    uint64_t literal_offset = 0;
    uint64_t file_offset = 0;
    uint32_t weakc = 0;
    char *new_map = NULL, *tmp_map = NULL;

    /* Find first unmatched chunk */
    uint64_t i = 0;
    for(; i < new_csums->chunk_nums; i++) {
        if(i >= old_csums->chunk_nums)
            /* All the chunks in old file are matched */
            break;

        struct one_csum *new_csum = &new_csums->all_csums[i];
        struct one_csum *old_csum = &old_csums->all_csums[i];

        if(new_csum->chunk_num == old_csum->chunk_num
            && new_csum->weak_csum == old_csum->weak_csum
            && memcmp(new_csum->csum, old_csum->csum, SHA256_DIGEST_LENGTH) == 0)
            continue;
        else
            break;
    }

    if(i && i < new_csums->chunk_nums)
        /* append copy command to delta file */
        append_copy_cmd(delta_fd, 0, i * CHUNK_SIZE);
    else if(i == new_csums->chunk_nums) {
        /* all the chunks in new file are matched */
        append_copy_cmd(delta_fd, 0, file_size(new_fd));
        goto out;
    }

    /* rolling from the second byte after the first unmatched chunk */
    literal_offset = i * CHUNK_SIZE;
    file_offset = i * CHUNK_SIZE;
    weakc = new_csums->all_csums[i].weak_csum;

    total_bytes_read = file_offset;

    new_map = (char *)map_file(new_fd);
    tmp_map = new_map + file_offset;

    while ((file_offset + CHUNK_SIZE) < new_fp_size) {
        weakc = rolling_crc32_1byte(weakc, tmp_map[CHUNK_SIZE], tmp_map[0]);

        struct crr_uthash *s = NULL;
        HASH_FIND(hh, old_hash_table, &weakc, sizeof(uint32_t), s);

        if(s == NULL) {
            /* append copy command to delta file */
            if(copy_len > 0) {
                append_copy_cmd(delta_fd, copy_offset, copy_len);
                copy_offset = 0;
                copy_len = 0;
            }
            if (file_offset + CHUNK_SIZE == new_fp_size) {
                file_offset = new_fp_size;
                break;
            }
            file_offset += 1;
            tmp_map += 1;
            continue;
        }
        else {
            uint8_t *csum = crr_calc_sha256((uint8_t *)(tmp_map + 1), CHUNK_SIZE);

            int index = find_uthash_item(s, csum);
            if(index != -1) {
                /* append literal command to delta file */
                if(file_offset - literal_offset > 0)
                    append_literal_cmd_fd(delta_fd, new_fd, literal_offset, file_offset - literal_offset);

                copy_offset = copy_offset == 0? s->csum[index].chunk_num * CHUNK_SIZE: copy_offset;
                copy_len += CHUNK_SIZE;
                file_offset += CHUNK_SIZE;
                tmp_map += CHUNK_SIZE;

                literal_offset = file_offset;

                free(csum);
                continue;
            }
            else {
                /* append copy command to delta file */
                if(copy_len > 0) {
                    append_copy_cmd(delta_fd, copy_offset, copy_len);
                    copy_offset = 0;
                    copy_len = 0;
                 }
                if (file_offset + CHUNK_SIZE == new_fp_size) {
                    file_offset = new_fp_size;
                    free(csum);
                    break;
                }
                file_offset += 1;
                tmp_map += 1;
                free(csum);
                continue;
            }
        }
    }

    if(file_offset - literal_offset > 0)
        append_literal_cmd_fd(delta_fd, new_fd, literal_offset, file_offset - literal_offset);
    else if(copy_len > 0)
        append_copy_cmd(delta_fd, copy_offset, copy_len + new_fp_size - file_offset);

out:
    unmap_file(new_fd, new_map);
    close(new_fd);
    close(delta_fd);
    crr_delete_uthash(&old_hash_table);
    if(old_csums->all_csums)
        free(old_csums->all_csums);
    if(old_csums)
        free(old_csums);
    if(new_csums->all_csums)
        free(new_csums->all_csums);
    if(new_csums)
        free(new_csums);
}

void crr_compare_csums_2(char *new_file_path, char *delta_file_path, struct crr_csums *old_csums,
                                           struct crr_csums *new_csums) {

    uint64_t bytes_read = 0, total_bytes_read = 0;
    ringbuf_t *new_file_buf = ringbuf_create(BUFFER_SIZE);
    if (!new_file_buf) {
        fprintf(stderr, "Error allocating ring buffer\n");
        exit(1);
    }

    struct crr_uthash *old_hash_table = NULL;
    crr_build_uthash(&old_hash_table, old_csums, old_csums->chunk_nums);
    #ifdef DEBUG
        crr_print_uthash(&old_hash_table);
    #endif

    FILE *new_fp = fopen(new_file_path, "rb");
    if (new_fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", new_file_path);
        ringbuf_destroy(new_file_buf);
        exit(1);
    }

    FILE *delta_fp = fopen(delta_file_path, "wb");
    if (delta_fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", delta_file_path);
        fclose(new_fp);
        ringbuf_destroy(new_file_buf);
        exit(1);
    }

    uint64_t new_fp_size = file_size(fileno(new_fp));
    uint64_t copy_offset = 0;
    uint64_t copy_len = 0;

    uint64_t literal_offset = 0;
    uint64_t file_offset = 0;
    uint32_t weakc = 0;

    /* Find first unmatched chunk */
    uint64_t i = 0;
    for(; i < new_csums->chunk_nums; i++) {
        if(i >= old_csums->chunk_nums)
            /* All the chunks in old file are matched */
            break;

        struct one_csum *new_csum = &new_csums->all_csums[i];
        struct one_csum *old_csum = &old_csums->all_csums[i];

        if(new_csum->chunk_num == old_csum->chunk_num
            && new_csum->weak_csum == old_csum->weak_csum
            && memcmp(new_csum->csum, old_csum->csum, SHA256_DIGEST_LENGTH) == 0)
            continue;
        else
            break;
    }

    if(i && i < new_csums->chunk_nums)
        /* append copy command to delta file */
        append_copy_cmd(fileno(delta_fp), 0, i * CHUNK_SIZE);
    else if(i == new_csums->chunk_nums) {
        /* all the chunks in new file are matched */
        append_copy_cmd(fileno(delta_fp), 0, file_size(fileno(new_fp)));
        goto out;
    }

    /* rolling from the second byte after the first unmatched chunk */
    literal_offset = i * CHUNK_SIZE;
    file_offset = i * CHUNK_SIZE;
    weakc = new_csums->all_csums[i].weak_csum;

    total_bytes_read = file_offset;

    while (file_offset < new_fp_size) {
        uint64_t to_read = (new_fp_size - file_offset) < BUFFER_SIZE ? (new_fp_size - file_offset) : BUFFER_SIZE;
        fseek(new_fp, total_bytes_read, SEEK_SET);
        bytes_read = ringbuf_read_from_fd(fileno(new_fp), new_file_buf, to_read);
        total_bytes_read += bytes_read;
        if (bytes_read <= 0) {
            break;
        }
        
        while(ringbuf_used(new_file_buf) >= CHUNK_SIZE) {
            uint8_t *buf = (uint8_t *) ringbuf_head(new_file_buf);
            weakc = rolling_crc32_1byte(weakc, buf[CHUNK_SIZE], buf[0]);

            struct crr_uthash *s = NULL;
            HASH_FIND(hh, old_hash_table, &weakc, sizeof(uint32_t), s);

            if(s == NULL) {
                /* append copy command to delta file */
                if(copy_len > 0) {
                    append_copy_cmd(fileno(delta_fp), copy_offset, copy_len);
                    copy_offset = 0;
                    copy_len = 0;
                }

                file_offset += 1;
                ringbuf_remove(new_file_buf, 1);
                continue;
            }
            else {
                uint8_t *csum = crr_calc_blake3(buf + 1, CHUNK_SIZE);

                int index = find_uthash_item(s, csum);
                if(index != -1) {
                    /* append literal command to delta file */
                    if(file_offset - literal_offset > 0)
                        append_literal_cmd_fd(fileno(delta_fp), fileno(new_fp), literal_offset, file_offset - literal_offset);

                    copy_offset = copy_offset == 0? s->csum[index].chunk_num * CHUNK_SIZE: copy_offset;
                    copy_len += CHUNK_SIZE;
                    ringbuf_remove(new_file_buf, CHUNK_SIZE);
                    file_offset += CHUNK_SIZE;
                    literal_offset = file_offset;
                    if(file_offset + CHUNK_SIZE >= new_fp_size)
                        break;

                    continue;
                }
                else {
                    /* append copy command to delta file */
                    if(copy_len > 0) {
                        append_copy_cmd(fileno(delta_fp), copy_offset, copy_len);
                        copy_offset = 0;
                        copy_len = 0;
                    }
                    file_offset += 1;
                    ringbuf_remove(new_file_buf, 1);
                    continue;
                }
            }
        }
    }

    // Ensure any remaining data in the buffer is processed
    if (ringbuf_used(new_file_buf) > 0) {
        uint8_t *buf = (uint8_t *) ringbuf_head(new_file_buf);
        uint64_t to_read = ringbuf_used(new_file_buf);
        weakc = rolling_crc32_1byte(weakc, buf[to_read - 1], buf[0]);

        struct crr_uthash *s = NULL;

        HASH_FIND(hh, old_hash_table, &weakc, sizeof(uint32_t), s);

        if(s == NULL) {
            if(file_offset - literal_offset > 0)
                file_offset += to_read;
            else {
                literal_offset = file_offset;
                file_offset += to_read;
            }
        }
        else {
            uint8_t *csum = crr_calc_blake3(buf, to_read);

            int index = find_uthash_item(s, csum);
            if(index != -1) {
                /* append literal command to delta file */
                if(file_offset - literal_offset > 0) {
                    append_literal_cmd_fd(fileno(delta_fp), fileno(new_fp), literal_offset, file_offset - literal_offset);
                    literal_offset = file_offset;
                }

                copy_offset = copy_offset == 0? s->csum[index].chunk_num * CHUNK_SIZE: copy_offset;
                copy_len += to_read;
            }
            else {
                if (file_offset - literal_offset > 0)
                    file_offset += to_read;
                else {
                    literal_offset = file_offset;
                    file_offset += to_read;
                }
            }
        }
    }

    if(file_offset - literal_offset > 0)
        append_literal_cmd_fd(fileno(delta_fp), fileno(new_fp), literal_offset, file_offset - literal_offset);
    else if(copy_len > 0)
        append_copy_cmd(fileno(delta_fp), copy_offset, copy_len + new_fp_size - file_offset);

out:
    ringbuf_destroy(new_file_buf);
    fclose(new_fp);
    fclose(delta_fp);
    crr_delete_uthash(&old_hash_table);
    if(old_csums->all_csums)
        free(old_csums->all_csums);
    if(old_csums)
        free(old_csums);
    if(new_csums->all_csums)
        free(new_csums->all_csums);
    if(new_csums)
        free(new_csums);
}


int crr_patch_delta(char *old_file_path, char* delta_file_path, char* out_file_path) {
    uint64_t bytes_read = 0, bytes_write = 0;
    uint64_t old_fs = 0, delta_fs = 0;
    uint64_t delta_file_offset = 0;

    int old_fd = open(old_file_path, O_RDONLY);
    if (old_fd == -1) {
        fprintf(stderr, "Error opening file %s\n", old_file_path);
        exit(1);
    }

    int delta_fd = open(delta_file_path, O_RDONLY);
    if (delta_fd == -1) {
        fprintf(stderr, "Error opening file %s\n", delta_file_path);
        close(old_fd);
        exit(1);
    }

    int output_fd = open(out_file_path, O_WRONLY | O_CREAT, 0644);
    if (output_fd == -1) {
        fprintf(stderr, "Error opening file %s\n", out_file_path);
        close(old_fd);
        close(delta_fd);
        exit(1);
    }

    old_fs = file_size(old_fd);
    delta_fs = file_size(delta_fd);

    while (delta_file_offset < delta_fs) {
        uint8_t cmd = 0;
        bytes_read = read(delta_fd, &cmd, sizeof(uint8_t));
        assert(bytes_read == sizeof(uint8_t));
        delta_file_offset += sizeof(uint8_t);

        switch (cmd)
        {
            case CMD_LITERAL:
            {
                uint64_t len = 0;
                bytes_read = read(delta_fd, &len, sizeof(uint64_t));
                assert(bytes_read == sizeof(uint64_t));
                delta_file_offset += sizeof(uint64_t);
                
                off_t send_offset = delta_file_offset;
                bytes_write = sendfile(output_fd, delta_fd, &send_offset, len);
                assert(bytes_write == len);
                delta_file_offset += len;
                break;
            }

            case CMD_COPY:
            {
                uint64_t offset = 0;
                bytes_read = read(delta_fd, &offset, sizeof(uint64_t));
                assert(bytes_read == sizeof(uint64_t));
                uint64_t len = 0;
                bytes_read = read(delta_fd, &len, sizeof(uint64_t));
                assert(bytes_read == sizeof(uint64_t));
                delta_file_offset += sizeof(uint64_t) * 2;

                if(offset + len > old_fs)
                    len = old_fs - offset;
                off_t send_offset = offset;
                bytes_write = sendfile(output_fd, old_fd, &send_offset, len);
                assert(bytes_write == len);
                break;
            }

            default:
                fprintf(stderr, "Unknown command\n");
                goto out;
        }
    }

out:
    close(old_fd);
    close(delta_fd);
    close(output_fd);
    return 0;
}

