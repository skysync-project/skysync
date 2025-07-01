#include <assert.h>
#include "skysync_c.h"

off_t file_size(int fd);
int is_zero(void *buf, uint64_t size);
int compare_offset(const void *a, const void *b);
void *map_file(int fd);
void unmap_file(int fd, void *map);

void cdc_build_uthash(struct cdc_uthash **hash_table, struct one_cdc *cdc_array, uint64_t chunk_num) {
    struct cdc_uthash *s;

    for(int i = 0; i < chunk_num; i++) {
        s = NULL;
        HASH_FIND(hh, *hash_table, &cdc_array[i].weak_hash, sizeof(uint32_t), s);

        if(s == NULL) {
            s = (struct cdc_uthash *)mi_malloc(sizeof(struct cdc_uthash));
            memcpy(&s->weak_hash, &cdc_array[i].weak_hash, sizeof(uint32_t));

            s->cdc_item_array = (struct ol*)mi_malloc(sizeof(struct ol) * ITEMNUMS);
            s->cdc_item_array[0].offset = cdc_array[i].offset;
            s->cdc_item_array[0].length = cdc_array[i].length;

            s->item_nums = 1;
            s->remalloc = 1;

            HASH_ADD(hh, *hash_table, weak_hash, sizeof(uint32_t), s);
        } else {
            if(s->item_nums >= (s->remalloc * ITEMNUMS)) {
                s->remalloc += 1;
                struct ol *tmp = (struct ol *)mi_malloc(sizeof(struct ol) * ITEMNUMS * s->remalloc);
                memcpy(tmp, s->cdc_item_array, sizeof(struct ol) * s->item_nums);
                mi_free(s->cdc_item_array);
                s->cdc_item_array = tmp;
            }
            s->cdc_item_array[s->item_nums].offset = cdc_array[i].offset;
            s->cdc_item_array[s->item_nums].length = cdc_array[i].length;
            s->item_nums += 1;
        }
    }
}

void cdc_delete_uthash(struct cdc_uthash **hash_table) {
    struct cdc_uthash *s, *tmp;

    HASH_ITER(hh, *hash_table, s, tmp) {
        HASH_DEL(*hash_table, s);
        mi_free(s->cdc_item_array);
        mi_free(s);
    }
}

void cdc_print_uthash(struct cdc_uthash **hash_table) {
    struct cdc_uthash *s, *tmp;
     uint32_t count_items = HASH_COUNT(*hash_table);
    printf("hash_table item_nums: %u\n", count_items);

    HASH_ITER(hh, *hash_table, s, tmp) {
        printf("weak_hash: %u, item_nums: %lu\n", s->weak_hash, s->item_nums);
        for(int i = 0; i < s->item_nums; i++) {
            printf("offset: %lu, length: %lu\n", s->cdc_item_array[i].offset, s->cdc_item_array[i].length);
        }
    }
}

uint64_t cdc_origin_64_skysync(unsigned char *p, uint64_t n) {
    uint64_t fingerprint = 0;
    uint64_t offset = MinSize;

    /* the minimal subChunk Size. */
    if (n <= MinSize) {
        return n;
    }

    /* the maximal subChunk Size. */
    if (n > MaxSize)
        n = MaxSize;

    while (offset < n) {
        fingerprint = (fingerprint << 1) + (GEARv2[p[offset]]);
        if ((!(fingerprint & FING_GEAR_08KB_64))) {
            return offset;
        }
        offset++;
    }

    return offset;
}

uint64_t cdc_normalized_chunking_2bytes_64(const unsigned char *p, uint64_t n) {
    uint64_t fingerprint = 0;
    MinSize = 4 * 1024;
    int Mid = 8 * 1024;
    uint64_t offset = MinSize / 2;

    // the minimal subChunk Size.
    if (n <= MinSize) {
        return n;
    }

    if (n > MaxSize)
        n = MaxSize;
    else if (n < Mid)
        Mid = n;

    while (offset < Mid / 2) {
        uint64_t a = offset * 2;
        fingerprint = (fingerprint << 2) + (LEARv2[p[a]]);
        if ((!(fingerprint & FING_GEAR_32KB_ls_64))) {
            offset = a;
            return offset;
        }

        fingerprint += GEARv2 [p[a + 1]];

        if ((!(fingerprint & FING_GEAR_32KB_64))) {
            offset = a + 1;
            return offset;
        }

        offset++;
    }

    while (offset < n / 2) {
        uint64_t a = offset * 2;
        fingerprint = (fingerprint << 2) + (LEARv2[p[a]]);

        if ((!(fingerprint & FING_GEAR_02KB_ls_64))) {
            offset = a;
            return offset;
        }

        fingerprint += GEARv2[p[a + 1]];

        if ((!(fingerprint & FING_GEAR_02KB_64))) {
            offset = a + 1;
            return offset;
        }

        offset++;
    }

    return offset;
}

struct cdc_crc32* cdc_calc_crc32(char * file_name) {
    return NULL;
}

void cdc_combine(struct one_cdc *cdc_array, char *file_buf) {

}

struct file_cdc* serial_cdc_1(int fd, char *map) {
    uint64_t chunk_num = 0;
    uint64_t bytes_write = 0;
    fastCDC_init();

    uint64_t fs = file_size(fd);

    struct file_cdc *fc = (struct file_cdc *) mi_malloc(sizeof(struct file_cdc));

    // struct one_cdc cdc_array[fs / MinSize + 2];
    struct one_cdc *cdc_array = (struct one_cdc *) mi_malloc(sizeof(struct one_cdc) * (fs / MinSize + 2));

    uint64_t offset = 0;

    for(;;) {
        uint64_t tmp_length = cdc_normalized_chunking_2bytes_64((unsigned char *)map + offset, fs - offset);

        struct one_cdc cdc = {
            .offset = offset,
            .length = tmp_length,
            // .weak_hash = crc32_16bytes_prefetch(read_file_buf + offset, tmp_length, 0, 256)
            .weak_hash = crc32_isal(map + offset, tmp_length, 0)
        };

        offset += tmp_length;

        memcpy(&cdc_array[chunk_num], &cdc, sizeof(struct one_cdc));
        chunk_num += 1;

        if (offset >= fs)
            break;

        if (offset + MinSize > fs) {
            cdc_array[chunk_num].offset = offset;
            cdc_array[chunk_num].length = fs - offset;
            cdc_array[chunk_num].weak_hash = crc32_isal(map + offset, fs - offset, 0);
            chunk_num += 1;
            break;
        }
    }

    fc->chunk_num = chunk_num;
    fc->cdc_array = cdc_array;

    return fc;
}

int serial_cdc_2(char *read_file_path, char *write_file_path, struct stats *st) {
    uint64_t chunk_num = 0;
    uint64_t bytes_write = 0;
    fastCDC_init();

    FILE *read_fp = fopen(read_file_path, "rb");
    if(read_fp == NULL) {
        printf("open file %s failed\n", read_file_path);
        exit(1);
    }

    FILE *write_fp = fopen(write_file_path, "wb");
    if(write_fp == NULL) {
        printf("open file %s failed\n", write_file_path);
        exit(1);
    }

    uint64_t fs = file_size(fileno(read_fp));
    int fd = fileno(read_fp);
    // fseek(read_fp, 0, SEEK_SET);
    char *map = (char *) mmap(NULL, fs, PROT_READ, MAP_SHARED, fd, 0);

    // struct one_cdc cdc_array[fs / MinSize + 2];
    struct one_cdc *cdc_array = (struct one_cdc *) mi_malloc(sizeof(struct one_cdc) * (fs / MinSize + 2));
    assert(cdc_array != NULL);

    uint64_t offset = 0;

    for(;;) {
        uint64_t tmp_length = cdc_normalized_chunking_2bytes_64((unsigned char *)map + offset, fs - offset);

        struct one_cdc cdc = {
            .offset = offset,
            .length = tmp_length,
            // .weak_hash = crc32_16bytes_prefetch(read_file_buf + offset, tmp_length, 0, 256)
            .weak_hash = crc32_isal(map + offset, tmp_length, 0)
        };

        offset += tmp_length;

        memcpy(&cdc_array[chunk_num], &cdc, sizeof(struct one_cdc));
        chunk_num += 1;

        if (offset >= fs)
            break;

        if (offset + MinSize > fs) {
            chunk_num += 1;
            cdc_array[chunk_num].offset = offset;
            cdc_array[chunk_num].length = fs - offset;
            cdc_array[chunk_num].weak_hash = crc32_isal(map + offset, fs - offset, 0);
            break;
        }
    }

    // cdc_combine(cdc_array, read_file_buf);

    bytes_write = fwrite(&chunk_num, sizeof(uint64_t), 1, write_fp);
    assert(bytes_write == 1);
    #ifdef IO_PRINT
        st->write_io_bytes += bytes_write;
    #endif

    // bytes_write = fwrite(&cdc_array[1], sizeof(struct one_cdc), chunk_num, write_fp);
    bytes_write = fwrite(cdc_array, sizeof(struct one_cdc), chunk_num, write_fp);
    assert(bytes_write == chunk_num);

    fclose(read_fp);
    fclose(write_fp);
    mi_free(cdc_array);
    munmap(map, fs);

    return 0;
}

struct file_cdc *read_cdc(char *file_path, struct stats *st) {
    FILE *file = fopen(file_path, "rb");
    if(file == NULL) {
        printf("open file %s failed\n", file_path);
        exit(1);
    }

    struct file_cdc *fc = (struct file_cdc *) mi_malloc(sizeof(struct file_cdc));
    assert(fc != NULL);

    uint64_t bytes_read = 0;
    bytes_read = fread(&fc->chunk_num, sizeof(uint64_t), 1, file);
    assert(bytes_read == 1);

    fc->cdc_array = (struct one_cdc *) mi_malloc(sizeof(struct one_cdc) * (fc->chunk_num));
    bytes_read = fread(fc->cdc_array, sizeof(struct one_cdc), fc->chunk_num, file);
    assert(bytes_read == fc->chunk_num);

    fclose(file);

    return fc;
}

struct cdc_matched_chunks *compare_weak_hash(int fd, struct file_cdc *old_fc, struct file_cdc *new_fc) {
    uint64_t old_chunk_num = old_fc->chunk_num;
    struct one_cdc *old_cdc_array = old_fc->cdc_array;

    uint64_t new_chunk_num = new_fc->chunk_num;
    struct one_cdc *new_cdc_array = new_fc->cdc_array;

    struct cdc_matched_chunks *mc = (struct cdc_matched_chunks *)mi_malloc(sizeof(struct cdc_matched_chunks));
    assert(mc != NULL);

    mc->matched_item_array = (struct matched_item *)mi_malloc(sizeof(struct matched_item) * new_chunk_num);
    assert(mc->matched_item_array != NULL);

    struct cdc_uthash *hash_table = NULL;
    cdc_build_uthash(&hash_table, new_cdc_array, new_chunk_num);

    // fd set to the beginning of the file
    lseek(fd, 0, SEEK_SET);

    uint64_t old_fs = file_size(fd);

    uint64_t matched_nums = 0;

    for(int i = 0; i < old_chunk_num; i++) {
        struct cdc_uthash *s = NULL;
        HASH_FIND(hh, hash_table, &old_cdc_array[i].weak_hash, sizeof(uint32_t), s);

        /* if the chunk in the old file is found in the new file */
        if(s != NULL) {
            mc->matched_item_array[matched_nums].item_nums = s->item_nums;
            mc->matched_item_array[matched_nums].new_ol_array = (struct ol *)mi_malloc(sizeof(struct ol) * s->item_nums);
            assert(mc->matched_item_array[matched_nums].new_ol_array != NULL);
            memcpy(mc->matched_item_array[matched_nums].new_ol_array, s->cdc_item_array, sizeof(struct ol) * s->item_nums);

            uint8_t hash[SHA_DIGEST_LENGTH];
            uint8_t old_file_buf[old_cdc_array[i].length];

            lseek(fd, old_cdc_array[i].offset, SEEK_SET);

            uint64_t old_bytes_read = read(fd, old_file_buf, old_cdc_array[i].length);
            assert(old_bytes_read == old_cdc_array[i].length);
            SHA1(old_file_buf, old_cdc_array[i].length, hash);

            memcpy(mc->matched_item_array[matched_nums].hash, hash, SHA_DIGEST_LENGTH);

            mc->matched_item_array[matched_nums].old_ol.offset = old_cdc_array[i].offset;
            mc->matched_item_array[matched_nums].old_ol.length = old_cdc_array[i].length;
            matched_nums += 1;
        }
    }

    if(matched_nums == 0)
        mi_free(mc->matched_item_array);
    else
        mc->matched_chunks_num = matched_nums;

    cdc_delete_uthash(&hash_table);
    return mc;
}

int cdc_write_matched(struct cdc_matched_chunks *mc, char *matched_file_path, struct stats *stats) {
    uint64_t bytes_write = 0;

    FILE *fp = fopen(matched_file_path, "wb");
    if(fp == NULL) {
        printf("open file %s failed\n", matched_file_path);
        exit(1);
    }

    bytes_write = fwrite(&mc->matched_chunks_num, sizeof(uint64_t), 1, fp);
    assert(bytes_write == 1);

    struct matched_item *mc_item = mc->matched_item_array;
    uint64_t matched_chunks_num = mc->matched_chunks_num;

    if(matched_chunks_num != 0) {
        for(int i = 0; i < matched_chunks_num; i++) {
            bytes_write = fwrite(&mc_item[i].item_nums, sizeof(uint64_t), 1, fp);
            assert(bytes_write == 1);

            bytes_write = fwrite(mc_item[i].hash, sizeof(uint8_t), SHA_DIGEST_LENGTH, fp);
            assert(bytes_write == SHA_DIGEST_LENGTH);
            bytes_write = fwrite(&mc_item[i].old_ol, sizeof(struct ol), 1, fp);
            assert(bytes_write == 1);

            bytes_write = fwrite(mc_item[i].new_ol_array, sizeof(struct ol), mc_item[i].item_nums, fp);
            assert(bytes_write == mc_item[i].item_nums);

            mi_free(mc_item[i].new_ol_array);
        }
    }

    fclose(fp);
    if(matched_chunks_num != 0)
        mi_free(mc->matched_item_array);
    mi_free(mc);
    return 0;
}

struct cdc_matched_chunks* cdc_read_matched(char *file_path, struct stats *stats) {
    FILE *fp = fopen(file_path, "rb");
    if(fp == NULL) {
        printf("open file %s failed\n", file_path);
        exit(1);
    }

    struct cdc_matched_chunks *mc = (struct cdc_matched_chunks *)mi_malloc(sizeof(struct cdc_matched_chunks));
    assert(mc != NULL);

    uint64_t bytes_read = 0;
    bytes_read = fread(&mc->matched_chunks_num, sizeof(uint64_t), 1, fp);
    assert(bytes_read == 1);

    uint64_t matched_chunks_num = mc->matched_chunks_num;
    if(matched_chunks_num != 0) {
        mc->matched_item_array = (struct matched_item *)mi_malloc(sizeof(struct matched_item) * matched_chunks_num);
        for(int i = 0; i < matched_chunks_num; i++) {
            bytes_read = fread(&mc->matched_item_array[i].item_nums, sizeof(uint64_t), 1, fp);
            assert(bytes_read == 1);

            bytes_read = fread(mc->matched_item_array[i].hash, sizeof(uint8_t), SHA_DIGEST_LENGTH, fp);
            assert(bytes_read == SHA_DIGEST_LENGTH);
            bytes_read = fread(&mc->matched_item_array[i].old_ol, sizeof(struct ol), 1, fp);
            assert(bytes_read == 1);

            mc->matched_item_array[i].new_ol_array = (struct ol *)mi_malloc(sizeof(struct ol) * mc->matched_item_array[i].item_nums);
            bytes_read = fread(mc->matched_item_array[i].new_ol_array, sizeof(struct ol), mc->matched_item_array[i].item_nums, fp);
            assert(bytes_read == mc->matched_item_array[i].item_nums);
        }
    }

    fclose(fp);
    return mc;
}

void cdc_compare_sha1(struct cdc_matched_chunks *mc, int fd) {
    uint64_t matched_chunks_num = mc->matched_chunks_num;

    if(matched_chunks_num == 0)
        mc->matched_item_array = NULL;

    struct matched_item *mc_item = mc->matched_item_array;

    uint64_t bytes_read = 0;

    lseek(fd, 0, SEEK_SET);
    uint64_t fs = file_size(fd);

    for(int i = 0; i < matched_chunks_num; i++) {
        for(int j = 0; j < mc_item[i].item_nums; j++) {
            uint8_t hash[SHA_DIGEST_LENGTH];
            uint8_t *file_buf = (uint8_t *)mi_malloc(mc_item[i].new_ol_array[j].length);
            assert(file_buf != NULL);

            lseek(fd, mc_item[i].new_ol_array[j].offset, SEEK_SET);

            bytes_read = read(fd, file_buf, mc_item[i].new_ol_array[j].length);
            assert(bytes_read == mc_item[i].new_ol_array[j].length);

            SHA1(file_buf, mc_item[i].new_ol_array[j].length, hash);
            if(memcmp(hash, mc_item[i].hash, SHA_DIGEST_LENGTH) == 0) {
                mc_item[i].item_nums = 1;
                memset(mc_item[i].hash, 0, SHA_DIGEST_LENGTH);
                mc_item[i].new_ol_array[0].offset = mc_item[i].new_ol_array[j].offset;
                mc_item[i].new_ol_array[0].length = mc_item[i].new_ol_array[j].length;
                break;
            }
            mi_free(file_buf);
        }
    }
}

struct real_matched* cdc_process_matched_chunks(struct matched_item *mc, uint64_t matched_chunks_num, uint64_t *real_nums) {
    uint64_t real_matched_nums = 0;

    if(matched_chunks_num == 0)
        return NULL;

    for(int i = 0; i < matched_chunks_num; i++) {
        if(mc[i].item_nums == 1 && is_zero(mc[i].hash, SHA_DIGEST_LENGTH))
            real_matched_nums += 1;
    }

    struct real_matched *ream = (struct real_matched *) mi_malloc(sizeof(struct real_matched) * real_matched_nums);

    for(int i = 0, j = 0; i < matched_chunks_num; i++) {
        if(mc[i].item_nums == 1 && is_zero(mc[i].hash, SHA_DIGEST_LENGTH)) {
            memcpy(&ream[j].old_ol, &mc[i].old_ol, sizeof(struct ol));
            memcpy(&ream[j].new_ol, &mc[i].new_ol_array[0], sizeof(struct ol));
            j++;
        }
    }

    qsort(ream, real_matched_nums, sizeof(struct real_matched), compare_offset);

    memcpy(real_nums, &real_matched_nums, sizeof(uint64_t));
    return ream;
}

int cdc_generate_delta(struct cdc_matched_chunks *mc, int new_fd, char *delta_file_path) {
    uint64_t matched_chunks_num = mc->matched_chunks_num;

    uint64_t bytes_read = 0, bytes_write = 0;

    struct matched_item *mc_item = mc->matched_item_array;
    uint64_t real_matched_nums = 0;
    struct real_matched *ream = cdc_process_matched_chunks(mc_item, matched_chunks_num, &real_matched_nums);

    int delta_fd = open(delta_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    uint64_t new_fs = file_size(new_fd);

    uint64_t offset = 0;
    uint64_t copy_offset = 0;
    uint64_t copy_length = 0;

    if(real_matched_nums == 0 ) {
        append_literal_cmd_fd(delta_fd, new_fd, 0, new_fs);
        goto out;
    }

    for(int i = 0; i < real_matched_nums; i++) {
        if(ream[i].new_ol.offset > offset) {
            if(copy_length != 0) {
                append_copy_cmd(delta_fd, copy_offset, copy_length);
            }
            append_literal_cmd_fd(delta_fd, new_fd, offset, ream[i].new_ol.offset - offset);
            offset = ream[i].new_ol.offset;
            copy_offset = ream[i].old_ol.offset;
            copy_length = 0;
        }
        if(copy_length == ream[i].old_ol.offset - copy_offset) {
            copy_length += ream[i].old_ol.length;
            offset += ream[i].new_ol.length;
        }
        if(i == real_matched_nums - 1) {
            append_copy_cmd(delta_fd, copy_offset, copy_length);
            if(offset < new_fs) {
                append_literal_cmd_fd(delta_fd, new_fd, offset, new_fs - offset);
            }
        }

    }

out:
    for(int i = 0; i < matched_chunks_num; i++) {
        if(mc_item[i].new_ol_array)
            mi_free(mc_item[i].new_ol_array);
    }
    if(mc->matched_item_array)
        mi_free(mc->matched_item_array);
    if(mc)
        mi_free(mc);
    if(ream)
        mi_free(ream);
    close(delta_fd);
    return 0;
}
