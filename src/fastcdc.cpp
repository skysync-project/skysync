#include <assert.h>
#include <time.h>
#include <sys/sendfile.h>
#include "fastcdc.h"


off_t file_size(int fd);
int is_zero(void *buf, uint64_t size);
void *map_file(int fd);
void unmap_file(int fd, void *map);

uint64_t LEARv2[256];
int chunk_dist[30];
uint32_t g_global_matrix[SymbolCount];
uint32_t g_global_matrix_left[SymbolCount];
uint32_t expectCS;
uint32_t Mask_15;
uint32_t Mask_11;
uint64_t Mask_11_64, Mask_15_64;

uint32_t MinSize;
uint32_t MidSize;
uint32_t MinSize_divide_by_2;
uint32_t MaxSize;

// functions
void fastCDC_init(void) {
    unsigned char md5_digest[16];
    uint8_t seed[SeedLength];
    for (int i = 0; i < SymbolCount; i++) {

        for (int j = 0; j < SeedLength; j++) {
            seed[j] = i;
        }

        g_global_matrix[i] = 0;
        // MD5(seed, SeedLength, md5_digest);
        // EVP_Q_digest(NULL, "MD5", NULL, seed, SeedLength, md5_digest, NULL);
        EVP_Digest(seed, SeedLength, md5_digest, NULL, EVP_md5(), NULL);
        memcpy(&(g_global_matrix[i]), md5_digest, 4);
        g_global_matrix_left[i] = g_global_matrix[i] << 1;
    }

    // 64 bit init
    for (int i = 0; i < SymbolCount; i++) {
        LEARv2[i] = GEARv2[i] << 1;
    }

    MinSize = 8192 / 2;     // 4KB
    MidSize = MinSize *2;    // 8KB
    MaxSize = 12288 * 1;    // 32768; 8192; 12 * 1024 = 12288 , 12KB
    Mask_15 = 0xf9070353;  //  15个1
    Mask_11 = 0xd9000353;  //  11个1
    Mask_11_64 = 0x0000d90003530000;
    Mask_15_64 = 0x0000f90703530000;
    MinSize_divide_by_2 = MinSize / 2;
}

struct one_fastcdc normalized_chunking_64(unsigned char *p, uint64_t n) {
    uint64_t fingerprint = 0;
    MinSize = 4 * 1024;
    int Mid = 6 * 1024;
    struct one_fastcdc ret = {
            .offset = MinSize,
            .length = 0,
            .fastfp = 0
    };

    // the minimal subChunk Size.
    if (n <= MinSize) {
        uint64_t j = 0;
        while (j < n) {
            fingerprint = (fingerprint << 1) + (GEARv2[p[j]]);
            ret.fastfp += fingerprint;
            j++;
        }
        ret.offset = n;
        return ret;
    }

    if (n > MaxSize)
        n = MaxSize;
    else if (n < Mid)
        Mid = n;

    while (ret.offset < Mid) {
        fingerprint = (fingerprint << 1) + (GEARv2[p[ret.offset]]);
        ret.fastfp += fingerprint;
        if ((!(fingerprint & FING_GEAR_32KB_64))) {
            return ret;
        }

        ret.offset++;
    }

    while (ret.offset < n) {
        fingerprint = (fingerprint << 1) + (GEARv2[p[ret.offset]]);
        ret.fastfp += fingerprint;
        if ((!(fingerprint & FING_GEAR_02KB_64))) {
            return ret;
        }

        ret.offset++;
    }

    return ret;
}

struct one_fastcdc normalized_chunking_2bytes_64(unsigned char *p, uint64_t n) {
    uint64_t fingerprint = 0;
    MinSize = 4 * 1024;
    int Mid = 8 * 1024;
    struct one_fastcdc ret = {
            .offset = MinSize / 2,
            .length = 0,
            .fastfp = 0
    };

    // the minimal subChunk Size.
    if (n <= MinSize) {
        uint64_t j = 0;
        while (j < n) {
            fingerprint = (fingerprint << 1) + (GEARv2[p[j]]);
            ret.fastfp += fingerprint;
            j++;
        }
        ret.offset = n;
        return ret;
    }

    if (n > MaxSize)
        n = MaxSize;
    else if (n < Mid)
        Mid = n;

    while (ret.offset < Mid / 2) {
        uint64_t a = ret.offset * 2;
        fingerprint = (fingerprint << 2) + (LEARv2[p[a]]);
        ret.fastfp += fingerprint;
        if ((!(fingerprint & FING_GEAR_08KB_ls_64))) {
            ret.offset = a;
            return ret;
        }

        fingerprint += GEARv2 [p[a + 1]];
        ret.fastfp += GEARv2 [p[a + 1]];
        if ((!(fingerprint & FING_GEAR_08KB_64))) {
            ret.offset = a + 1;
            return ret;
        }

        ret.offset++;
    }

    while (ret.offset < n / 2) {
        uint64_t a = ret.offset * 2;
        fingerprint = (fingerprint << 2) + (LEARv2[p[a]]);
        ret.fastfp += fingerprint;
        if ((!(fingerprint & FING_GEAR_02KB_ls_64))) {
            ret.offset = a;
            return ret;
        }

        fingerprint += GEARv2[p[a + 1]];
        ret.fastfp += GEARv2[p[a + 1]];
        if ((!(fingerprint & FING_GEAR_02KB_64))) {
            ret.offset = a + 1;
            return ret;
        }

        ret.offset++;
    }

    return ret;
}

struct one_fastcdc rolling_data_2bytes_64(unsigned char *p, uint64_t n) {
    uint64_t fingerprint = 0;
    // int i = MinSize_divide_by_2;
    struct one_fastcdc ret = {
            .offset = MinSize_divide_by_2,
            .length = 0,
            .fastfp = 0
    };

    // the minimal subChunk Size.
    if (n <= MinSize) {
        uint64_t j = 0;
        while (j < n) {
            fingerprint = (fingerprint << 1) + (GEARv2[p[j]]);
            ret.fastfp += fingerprint;
            j++;
        }
        ret.offset = n;
        return ret;
    }

    if (n > MaxSize)
        n = MaxSize;

    while (ret.offset < n / 2) {
        uint64_t a = ret.offset * 2;
        fingerprint = (fingerprint << 2) + (LEARv2[p[a]]);
        ret.fastfp += fingerprint;
        if ((!(fingerprint & FING_GEAR_08KB_ls_64))) {
            ret.offset = a;
            return ret;
        }

        fingerprint += GEARv2[p[a + 1]];
        ret.fastfp += GEARv2[p[a + 1]];
        if ((!(fingerprint & FING_GEAR_08KB_64))) {
            ret.offset = a + 1;
            return ret;
        }

        ret.offset++;
    }

    return ret;
}

struct one_fastcdc cdc_origin_64(unsigned char *p, uint64_t n) {
    uint64_t fingerprint = 0;
    struct one_fastcdc ret = {
            .offset = MinSize,
            .length = 0,
            .fastfp = 0
    };

    /* the minimal subChunk Size. */
    if (n <= MinSize) {
        uint64_t j = 0;
        while (j < n) {
            fingerprint = (fingerprint << 1) + (GEARv2[p[j]]);
            ret.fastfp += fingerprint;
            j++;
        }
        ret.offset = n;
        return ret;
    }

    /* the maximal subChunk Size. */
    if (n > MaxSize)
        n = MaxSize;

    while (ret.offset < n) {
        fingerprint = (fingerprint << 1) + (GEARv2[p[ret.offset]]);
        ret.fastfp += fingerprint;
        if ((!(fingerprint & FING_GEAR_08KB_64))) {
            return ret;
        }
        ret.offset++;
    }

    return ret;
}

void fastcdc_build_uthash(struct fastcdc_uthash **hash_table, struct one_fastcdc *fastcdc_array,
                        uint64_t chunk_num) {
    struct fastcdc_uthash *s;

    for(uint64_t i = 0; i < chunk_num; i++) {
        s = NULL;
        struct one_fastcdc of_t = fastcdc_array[i];
        uint64_t fastfp_t = of_t.fastfp;
        HASH_FIND(hh, *hash_table, &fastfp_t, sizeof(uint64_t), s);

        if(s == NULL) {
            s = (struct fastcdc_uthash *) malloc(sizeof(struct fastcdc_uthash));
            memset(s, 0, sizeof(struct fastcdc_uthash));
            memcpy(&s->fastfp, &fastfp_t, sizeof(uint64_t));

            s->fastfp_item_array = (struct fastfp_item*) malloc(sizeof(struct fastfp_item) * ITEMNUMS);
            memset(s->fastfp_item_array, 0, sizeof(struct fastfp_item) * ITEMNUMS);
            s->fastfp_item_array[0].offset = of_t.offset;
            s->fastfp_item_array[0].length = of_t.length;
            s->item_nums = 1;
            s->remalloc = 1;

            HASH_ADD(hh, *hash_table, fastfp, sizeof(uint64_t), s);
        }
        else {
            if(s->item_nums >= (s->remalloc * ITEMNUMS)) {
                s->remalloc += 1;
                struct fastfp_item *fi = (struct fastfp_item*) malloc(sizeof(struct fastfp_item)
                                                                        * s->remalloc * ITEMNUMS);
                memset(fi, 0, sizeof(struct fastfp_item) * s->remalloc * ITEMNUMS);
                memcpy(fi, s->fastfp_item_array, sizeof(struct fastfp_item) * s->item_nums);
                free(s->fastfp_item_array);
                s->fastfp_item_array = fi;
            }
            s->fastfp_item_array[s->item_nums].offset = of_t.offset;
            s->fastfp_item_array[s->item_nums].length = of_t.length;
            s->item_nums += 1;
        }
    }
}

void fastcdc_delete_uthash(struct fastcdc_uthash **hash_table) {
    struct fastcdc_uthash *current, *tmp;
    HASH_ITER(hh, *hash_table, current, tmp) {
        HASH_DEL(*hash_table, current);
        free(current->fastfp_item_array);
        free(current);
    }
}

void fastcdc_print_uthash(struct fastcdc_uthash **hash_table) {
    struct fastcdc_uthash *s;
    uint32_t count_items = HASH_COUNT(*hash_table);
    printf("There are %d items in fastcdc uthash table\n", count_items);

    for(s = *hash_table; s != NULL; s = (struct fastcdc_uthash*)(s->hh.next)) {
        printf("fastfp: %lu, item_nums: %lu\n", s->fastfp, s->item_nums);
        for(int i = 0; i < s->item_nums; i++) {
            printf("\toffset: %lu, length: %lu\n", s->fastfp_item_array[i].offset,
                    s->fastfp_item_array[i].length);
        }
    }
}

struct file_fastcdc* run_fastfp_1(int fd, char *map, int chunking_method) {
    uint64_t bytes_read = 0, bytes_write = 0;
    uint64_t chunk_num = 0;
    fastCDC_init();

    switch (chunking_method)
    {
        case ORIGIN_CDC:
            chunking = cdc_origin_64;
            break;

        case ROLLING_2Bytes:
            chunking = rolling_data_2bytes_64;
            break;

        case NORMALIZED_CDC:
            chunking = normalized_chunking_64;
            break;

        case NORMALIZED_2Bytes:
            chunking = normalized_chunking_2bytes_64;
            break;

        default:
            printf("No implement other chunking method");
            exit(-1);
    }

    /* get the file size */
    uint64_t fs= file_size(fd);

    struct file_fastcdc *file_fastcdc = (struct file_fastcdc *) malloc(sizeof(struct file_fastcdc));
    assert(file_fastcdc != NULL);
    memset(file_fastcdc, 0, sizeof(struct file_fastcdc));

    // struct one_fastcdc file_fastfp[fs / MinSize + 2];
    struct one_fastcdc *file_fastfp = (struct one_fastcdc *) malloc(sizeof(struct one_fastcdc) * (fs / MinSize + 2));
    assert(file_fastfp != NULL);
    memset(file_fastfp, 0, sizeof(struct one_fastcdc) * (fs / MinSize + 2));

    uint64_t offset = 0;
    char* map_t = map;

    for (;;) {
        struct one_fastcdc tmp = chunking((unsigned char*) map_t, fs - offset);
        
        tmp.length = tmp.offset;
        tmp.offset = offset;
        offset += tmp.length;
        map_t += tmp.length;

        memcpy(&file_fastfp[chunk_num], &tmp, sizeof(struct one_fastcdc));
        chunk_num += 1;

        if (offset == fs) {
            // file_fastfp[chunk_num].length = fs - tmp.offset;
            // file_fastfp[chunk_num].fastfp = 0;
            break;
        }

        if (offset + MinSize > fs) {
            struct one_fastcdc mintmp = chunking((unsigned char*) map_t, fs - offset);
            mintmp.length = mintmp.offset;
            mintmp.offset = offset;
            offset += mintmp.length;
            memcpy(&file_fastfp[chunk_num], &mintmp, sizeof(struct one_fastcdc));
            chunk_num += 1;
            break;
        }
    }

    file_fastcdc->chunk_num = chunk_num;
    file_fastcdc->fastcdc_array = file_fastfp;

    #ifdef FASTFP_TEST
        printf("total time: %f, file size: %lu, chunk nums: %lu\n", (double)total / CLOCKS_PER_SEC, fs, chunk_num);
        printf("Throughput: %f MB/s\n", (double)fs / ((double)total / CLOCKS_PER_SEC) / 1024 / 1024);
    #endif

    // for(int i = 0; i < chunk_num; i++) {
    //     printf("offset: %ld, length: %ld, fastfp: %lu\n", file_fastfp[i].offset, file_fastfp[i].length, file_fastfp[i].fastfp);
    // }
    // printf("chunk_num: %lu\n\n\n\n", chunk_num);

    return file_fastcdc;
}

int run_fastfp_2(char *read_file_path, char *write_file_path, int chunking_method, struct stats *stats) {
    uint64_t bytes_read = 0, bytes_write = 0;
    uint64_t chunk_num = 0;
    fastCDC_init();

    switch (chunking_method)
    {
        case ORIGIN_CDC:
            chunking = cdc_origin_64;
            break;

        case ROLLING_2Bytes:
            chunking = rolling_data_2bytes_64;
            break;

        case NORMALIZED_CDC:
            chunking = normalized_chunking_64;
            break;

        case NORMALIZED_2Bytes:
            chunking = normalized_chunking_2bytes_64;
            break;

        default:
            printf("No implement other chunking method");
            exit(-1);
    }

    FILE *read_fp = fopen(read_file_path, "rb");
    if (read_fp == NULL) {
        perror("Fail to open file");
        exit(-1);
    }

    FILE *write_fp = fopen(write_file_path, "wb");
    if (write_fp == NULL) {
        perror("Fail to open file to write");
        exit(-1);
    }

    /* get the file size */
    uint64_t fs= file_size(fileno(read_fp));
    ringbuf_t* file_buf = ringbuf_create(BUFFER_SIZE);

    // struct one_fastcdc file_fastfp[fs / MinSize + 2];
    struct one_fastcdc *file_fastfp = (struct one_fastcdc *) malloc(sizeof(struct one_fastcdc) * (fs / MinSize + 2));
    assert(file_fastfp != NULL);
    memset(file_fastfp, 0, sizeof(struct one_fastcdc) * (fs / MinSize + 2));

    #ifdef FASTFP_TEST
        clock_t start, end;
        clock_t total = 0;
    #endif

    uint64_t offset = 0;

    while (offset < fs) {
        uint64_t to_read = (fs - offset) > BUFFER_SIZE ? BUFFER_SIZE : (fs - offset);

        bytes_read = ringbuf_read_from_fd(fileno(read_fp), file_buf, to_read);
        if (bytes_read <= 0) {
            break;
        }

        while (ringbuf_used(file_buf) >= MaxSize) {
            struct one_fastcdc tmp = chunking((unsigned char *)ringbuf_head(file_buf), ringbuf_used(file_buf));

            tmp.length = tmp.offset;
            tmp.offset = offset;
            offset += tmp.length;

            memcpy(&file_fastfp[chunk_num], &tmp, sizeof(struct one_fastcdc));

            chunk_num += 1;

            ringbuf_remove(file_buf, tmp.length);
        }        
    }

    // Ensure any remaining data in the buffer is processed
    while (ringbuf_used(file_buf) > 0) {
        struct one_fastcdc tmp = chunking((unsigned char *)ringbuf_head(file_buf), ringbuf_used(file_buf));

        tmp.length = tmp.offset;
        tmp.offset = offset;
        offset += tmp.length;

        memcpy(&file_fastfp[chunk_num], &tmp, sizeof(struct one_fastcdc));

        chunk_num += 1;

        ringbuf_remove(file_buf, tmp.length);
    }

    #ifdef FASTFP_TEST
        printf("total time: %f, file size: %lu, chunk nums: %lu\n", (double)total / CLOCKS_PER_SEC, fs, chunk_num);
        printf("Throughput: %f MB/s\n", (double)fs / ((double)total / CLOCKS_PER_SEC) / 1024 / 1024);
    #endif

    /* write the chunk_num first */
    bytes_write = fwrite(&chunk_num, sizeof(uint64_t), 1, write_fp);
    assert(bytes_write == 1);
    #ifdef IO_PRINT
        stats->write_io_bytes += bytes_write;
    #endif

    // for(int i = 1; i <= chunk_num; i++) {
    //     printf("offset: %ld, length: %ld, fastfp: %ld\n", file_fastfp[i].offset, file_fastfp[i].length, file_fastfp[i].fastfp);
    // }

    /* then write the file_fastfp array*/
    bytes_write = fwrite(file_fastfp, sizeof(struct one_fastcdc), chunk_num, write_fp);
    assert(bytes_write == chunk_num);
    #ifdef IO_PRINT
        stats->write_io_bytes += bytes_write;
    #endif

    // clear the items
    ringbuf_destroy(file_buf);
    free(file_fastfp);
    fclose(read_fp);
    fclose(write_fp);
    return 0;
}

struct file_fastcdc *read_fastfp(char *file_path, struct stats *stats) {
    struct file_fastcdc *file_fastcdc = (struct file_fastcdc *) malloc(sizeof(struct file_fastcdc));
    assert(file_fastcdc != NULL);
    memset(file_fastcdc, 0, sizeof(struct file_fastcdc));


    FILE *fp = fopen(file_path, "rb");
    if (fp == NULL) {
        perror("Fail to open file to read");
        exit(-1);
    }

    uint64_t bytes_read = 0;
    /* read the chunk_num first */
    uint64_t chunk_num = 0;
    bytes_read = fread(&chunk_num, sizeof(uint64_t), 1, fp);
    assert(bytes_read == 1);
    #ifdef IO_PRINT
        stats->read_io_bytes += bytes_read;
    #endif

    /* then read the fastcdc array */
    struct one_fastcdc *fastcdc_array = (struct one_fastcdc *) malloc(sizeof(struct one_fastcdc) * chunk_num);
    assert(fastcdc_array != NULL);

    bytes_read = fread(fastcdc_array, sizeof(struct one_fastcdc), chunk_num, fp);
    assert(bytes_read == chunk_num);
    #ifdef IO_PRINT
        stats->read_io_bytes += bytes_read;
    #endif

    file_fastcdc->chunk_num = chunk_num;
    file_fastcdc->fastcdc_array = fastcdc_array;

    fclose(fp);
    return file_fastcdc;
}

struct matched_chunks* compare_fastfp(int fd, struct file_fastcdc *old_ff,
                                      struct file_fastcdc *new_ff) {
    uint64_t old_chunk_num = old_ff->chunk_num;
    struct one_fastcdc *old_fastcdc_array = old_ff->fastcdc_array;

    uint64_t new_chunk_num = new_ff->chunk_num;
    struct one_fastcdc *new_fastcdc_array = new_ff->fastcdc_array;

    struct matched_chunks *matched_chunks = (struct matched_chunks *) malloc(sizeof(struct matched_chunks));
    assert(matched_chunks != NULL);
    memset(matched_chunks, 0, sizeof(struct matched_chunks));

    /* matched chunks' nums should be less than the new file's chunk nums */
    matched_chunks->matched_item_array = (struct matched_item *) malloc(sizeof(struct matched_item) * new_chunk_num);
    assert(matched_chunks->matched_item_array != NULL);

    /* unmatched chunks' nums should be less than the old file's chunk nums */
    matched_chunks->unmatched_item_array = (struct ol *) malloc(sizeof(struct ol) * old_chunk_num);
    assert(matched_chunks->unmatched_item_array != NULL);

    /* build the hash table for the new file's chunk and their fastfp */
    struct fastcdc_uthash *hash_table = NULL;
    fastcdc_build_uthash(&hash_table, new_fastcdc_array, new_chunk_num);

    /* read the old file and calculate the sha1 hash value for each matched chunk in the old file */
    lseek(fd, 0, SEEK_SET);
    
    uint64_t old_fs = file_size(fd);

    uint64_t matched_nums = 0;
    uint64_t unmatched_nums = 0;

    /* compare the old file's chunk and their fastfp with the hash table */
    for(uint64_t i = 0; i < old_chunk_num; i++) {
        // printf("i: %lu, old offset: %lu, length: %lu\n", i, old_fastcdc_array[i].offset, old_fastcdc_array[i].length);
        struct one_fastcdc of_t = old_fastcdc_array[i];
        uint64_t fastfp_t = of_t.fastfp;
        struct fastcdc_uthash *s = NULL;
        HASH_FIND(hh, hash_table, &fastfp_t, sizeof(uint64_t), s);

        /* if the fastfp is matched, record the matched chunks' offset and length */
        if(s != NULL) {
            /* record the matched chunk's offset and length */
            matched_chunks->matched_item_array[matched_nums].item_nums = s->item_nums;

            matched_chunks->matched_item_array[matched_nums].new_ol_array = (struct ol *) malloc(sizeof(struct ol) * s->item_nums);
            assert(matched_chunks->matched_item_array[matched_nums].new_ol_array != NULL);
            memset(matched_chunks->matched_item_array[matched_nums].new_ol_array, 0, sizeof(struct ol) * s->item_nums);
            
            struct fastfp_item *fastfp_item_array_t = s->fastfp_item_array;

            memcpy(matched_chunks->matched_item_array[matched_nums].new_ol_array, fastfp_item_array_t, sizeof(struct ol) * s->item_nums);
            
            /* calculate the sha1 hash value for the matched chunk in the old file */
            uint8_t hash[SHA_DIGEST_LENGTH];
            uint8_t old_fileCache[of_t.length];

            lseek(fd, of_t.offset, SEEK_SET);
            uint64_t old_bytes_read = read(fd, old_fileCache, of_t.length);
            assert(old_bytes_read == of_t.length);

            SHA1(old_fileCache, of_t.length, hash);
            memcpy(matched_chunks->matched_item_array[matched_nums].hash, hash, SHA_DIGEST_LENGTH);

            matched_chunks->matched_item_array[matched_nums].old_ol.offset = of_t.offset;
            matched_chunks->matched_item_array[matched_nums].old_ol.length = of_t.length;
            matched_nums += 1;
        }
        else {
            matched_chunks->unmatched_item_array[unmatched_nums].offset = of_t.offset;
            matched_chunks->unmatched_item_array[unmatched_nums].length = of_t.length;
            unmatched_nums += 1;
        }
    }

    if(matched_nums == 0) {
        free(matched_chunks->matched_item_array);
        matched_chunks->matched_item_array = NULL;
    }
    if(unmatched_nums == 0) {
        free(matched_chunks->unmatched_item_array);
        matched_chunks->unmatched_item_array = NULL;
    }

    matched_chunks->matched_chunks_num = matched_nums;
    matched_chunks->unmatched_chunks_num = unmatched_nums;

    fastcdc_delete_uthash(&hash_table);
    return matched_chunks;
}

int write_matched(struct matched_chunks *mc, char *matched_file_path, struct stats *stats) {
    uint64_t bytes_write = 0;

    uint64_t matched_chunks_num = mc->matched_chunks_num;
    uint64_t unmatched_chunks_num = mc->unmatched_chunks_num;

    FILE *write_fp_ma = fopen(matched_file_path, "wb");
    if (write_fp_ma == NULL) {
        perror("Fail to open file to write");
        exit(-1);
    }

    /* write the matched_chunks_num and unmatched_chunks_num first */
    bytes_write = fwrite(&matched_chunks_num, sizeof(uint64_t), 1, write_fp_ma);
    assert(bytes_write == 1);
    bytes_write = fwrite(&unmatched_chunks_num, sizeof(uint64_t), 1, write_fp_ma);
    assert(bytes_write == 1);
    #ifdef IO_PRINT
        stats->write_io_bytes += 2;
    #endif

    struct matched_item *matched_item_array = mc->matched_item_array;
    struct ol *unmatched_item_array = mc->unmatched_item_array;

    if(matched_chunks_num != 0) {
        /* write the matched_chunks */
        for(int i = 0; i < matched_chunks_num; i++) {
            
            // printf("nums: %lu\n", i);
            // printf("old offset: %u, length: %u\n", matched_item_array[i].old_ol.offset, matched_item_array[i].old_ol.length);
            // for (int j = 0; j < matched_item_array[i].item_nums; j++) {
            //     printf("new offset: %u, length: %u\n", matched_item_array[i].new_ol_array[j].offset, matched_item_array[i].new_ol_array[j].length);
            // }

            uint64_t item_nums = matched_item_array[i].item_nums;
            bytes_write = fwrite(&item_nums, sizeof(uint64_t), 1, write_fp_ma);
            assert(bytes_write == 1);

            bytes_write = fwrite(matched_item_array[i].hash, sizeof(uint8_t), SHA_DIGEST_LENGTH, write_fp_ma);
            assert(bytes_write == SHA_DIGEST_LENGTH);
            
            bytes_write = fwrite(&matched_item_array[i].old_ol, sizeof(struct ol), 1, write_fp_ma);
            assert(bytes_write == 1);

            struct ol *ol_array_t = matched_item_array[i].new_ol_array;
            bytes_write = fwrite(ol_array_t, sizeof(struct ol), item_nums, write_fp_ma);
            assert(bytes_write == item_nums);
            #ifdef IO_PRINT
                stats->write_io_bytes += 1 + SHA_DIGEST_LENGTH + 1 + item_nums;
            #endif
            free(ol_array_t);
        }
    }

    if(unmatched_chunks_num != 0) {
        /* write the unmatched_chunks */
        bytes_write = fwrite(unmatched_item_array, sizeof(struct ol), unmatched_chunks_num, write_fp_ma);
        assert(bytes_write == unmatched_chunks_num);
        #ifdef IO_PRINT
            stats->write_io_bytes += unmatched_chunks_num;
        #endif
    }

    fclose(write_fp_ma);
    if(matched_item_array)
        free(matched_item_array);
    if(unmatched_item_array)
        free(unmatched_item_array);
    if(mc)
        free(mc);
    return 0;
}

struct matched_chunks* read_matched(char *file_path, struct stats *stats) {
    struct matched_chunks *matched_chunks = (struct matched_chunks *) malloc(sizeof(struct matched_chunks));
    assert(matched_chunks != NULL);
    memset(matched_chunks, 0, sizeof(struct matched_chunks));

    FILE *fp = fopen(file_path, "rb");
    if (fp == NULL) {
        perror("Fail to open file to read");
        exit(-1);
    }

    /* get the file size */
    uint64_t fs = file_size(fileno(fp));
    uint64_t bytes_read = 0;

    /* read the matched_chunks_num and unmatched_chunks_num first */
    uint64_t matched_chunks_num = 0;
    uint64_t unmatched_chunks_num = 0;
    bytes_read = fread(&matched_chunks_num, sizeof(uint64_t), 1, fp);
    assert(bytes_read == 1);
    bytes_read = fread(&unmatched_chunks_num, sizeof(uint64_t), 1, fp);
    assert(bytes_read == 1);
    #ifdef IO_PRINT
        stats->read_io_bytes += 2;
    #endif

    if(matched_chunks_num != 0) {
        /* read the matched_chunks */
        struct matched_item *matched_item_array = (struct matched_item *) malloc(sizeof(struct matched_item) * matched_chunks_num);
        assert(matched_item_array != NULL);

        for(int i = 0; i < matched_chunks_num; i++) {
            uint64_t item_nums = 0;
            bytes_read = fread(&item_nums, sizeof(uint64_t), 1, fp);
            assert(bytes_read == 1);

            bytes_read = fread(matched_item_array[i].hash, sizeof(uint8_t), SHA_DIGEST_LENGTH, fp);
            assert(bytes_read == SHA_DIGEST_LENGTH);
            bytes_read = fread(&matched_item_array[i].old_ol, sizeof(struct ol), 1, fp);
            assert(bytes_read == 1);

            struct ol *ol_array_t = (struct ol *) malloc(sizeof(struct ol) * item_nums);
            bytes_read = fread(ol_array_t, sizeof(struct ol), item_nums, fp);
            assert(bytes_read == item_nums);

            matched_item_array[i].item_nums = item_nums;
            matched_item_array[i].new_ol_array = ol_array_t;
            #ifdef IO_PRINT
                stats->read_io_bytes += 1 + SHA_DIGEST_LENGTH + 1 + item_nums;
            #endif
            // if (i == 481) {
            //     printf("item_nums: %lu, offset: %u, length: %u\n", item_nums, matched_item_array[i].old_ol.offset, matched_item_array[i].old_ol.length);
            //     for (int j = 0; j < item_nums; j++) {
            //         printf("offset: %u, length: %u\n", ol_array_t[j].offset, ol_array_t[j].length);
            //     }
            // }
        }
        matched_chunks->matched_item_array = matched_item_array;
    }
    else {
        matched_chunks->matched_item_array = NULL;
    }

    if(unmatched_chunks_num != 0) {
        /* read the unmatched_chunks */
        struct ol *unmatched_item_array = (struct ol *) malloc(sizeof(struct ol) * unmatched_chunks_num);
        assert(unmatched_item_array != NULL);
        bytes_read = fread(unmatched_item_array, sizeof(struct ol), unmatched_chunks_num, fp);
        assert(bytes_read == unmatched_chunks_num);
        matched_chunks->unmatched_item_array = unmatched_item_array;
        #ifdef IO_PRINT
            stats->read_io_bytes += unmatched_chunks_num;
        #endif
    }
    else {
        matched_chunks->unmatched_item_array = NULL;
    }

    matched_chunks->matched_chunks_num = matched_chunks_num;
    matched_chunks->unmatched_chunks_num = unmatched_chunks_num;

    fclose(fp);
    return matched_chunks;
}

void compare_sha1(struct matched_chunks *mc, int fd) {
    uint64_t matched_chunks_num = mc->matched_chunks_num;
    uint64_t unmatched_chunks_num = mc->unmatched_chunks_num;
    if(matched_chunks_num == 0) {
        mc->matched_item_array = NULL;
    }
    if(unmatched_chunks_num == 0) {
        mc->unmatched_item_array = NULL;
    }

    struct matched_item *matched_chunks = mc->matched_item_array;
    struct ol *unmatched_chunks = mc->unmatched_item_array;

    uint64_t bytes_read = 0;

    lseek(fd, 0, SEEK_SET);
    uint64_t fs = file_size(fd);

    for(int i = 0; i < matched_chunks_num; i++) {
        for(int j = 0; j < matched_chunks[i].item_nums; j++) {
            uint8_t hash[SHA_DIGEST_LENGTH];
            uint8_t *file_cache = (uint8_t *) malloc(matched_chunks[i].new_ol_array[j].length);
            assert(file_cache != NULL);

            lseek(fd, matched_chunks[i].new_ol_array[j].offset, SEEK_SET);

            bytes_read = read(fd, file_cache, matched_chunks[i].new_ol_array[j].length);
            assert(bytes_read == matched_chunks[i].new_ol_array[j].length);
            
            SHA1(file_cache, matched_chunks[i].new_ol_array[j].length, hash);
            
            if(memcmp(matched_chunks[i].hash, hash, SHA_DIGEST_LENGTH) == 0) {
                matched_chunks[i].item_nums = 1;
                memset(matched_chunks[i].hash, 0, SHA_DIGEST_LENGTH);
                matched_chunks[i].new_ol_array[0].offset = matched_chunks[i].new_ol_array[j].offset;
                matched_chunks[i].new_ol_array[0].length = matched_chunks[i].new_ol_array[j].length;
                free(file_cache);
                break;
            }
            free(file_cache);
        }
    }
}

int compare_offset(const void *a, const void *b) {
    struct real_matched *ra = (struct real_matched *) a;
    struct real_matched *rb = (struct real_matched *) b;

    return ra->new_ol.offset - rb->new_ol.offset;
}

struct real_matched* process_matched_chunks(struct matched_item *mc, uint64_t matched_chunks_num, uint64_t *real_nums) {
    uint64_t real_matched_nums = 0;

    if(matched_chunks_num == 0)
        return NULL;

    for(int i = 0; i < matched_chunks_num; i++) {
        if(mc[i].item_nums == 1 && is_zero(mc[i].hash, SHA_DIGEST_LENGTH))
            real_matched_nums += 1;
    }

    struct real_matched *ream = (struct real_matched *) malloc(sizeof(struct real_matched) * real_matched_nums);
    memset(ream, 0, sizeof(struct real_matched) * real_matched_nums);

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

int append_literal_cmd_fd(int delta_fd, int new_fd, uint64_t offset, uint64_t length) {
    uint64_t bytes_write = 0;
    struct literal_cmd lc = {
        .cmd = CMD_LITERAL,
        .length = length
    };

    bytes_write = write(delta_fd, &lc.cmd, sizeof(uint8_t));
    assert(bytes_write == sizeof(uint8_t));
    bytes_write = write(delta_fd, &lc.length, sizeof(uint64_t));
    assert(bytes_write == sizeof(uint64_t));

    lseek(new_fd, offset, SEEK_SET);

    // Use sendfile to transfer data from new_fp to delta_fp
    uint64_t bytes_sent = 0;
    uint64_t total_bytes_sent = 0;

    while (total_bytes_sent < length) {
        bytes_sent = sendfile(delta_fd, new_fd, NULL, length - total_bytes_sent);
        if (bytes_sent <= 0) {
            perror("sendfile");
            return -1;
        }
        total_bytes_sent += bytes_sent;
    }

    assert(total_bytes_sent == length);

    // printf("literal: length: %d\n", length);

    return 0;
}

int append_copy_cmd(int delta_fd, uint64_t offset, uint64_t length) {

    uint64_t bytes_write = 0;
    struct copy_cmd cc = {
        .cmd = CMD_COPY,
        .offset = offset,
        .length = length
    };

    bytes_write = write(delta_fd, &cc.cmd, sizeof(uint8_t));
    assert(bytes_write == sizeof(uint8_t));

    bytes_write = write(delta_fd, &cc.offset, sizeof(uint64_t));
    assert(bytes_write == sizeof(uint64_t));

    bytes_write = write(delta_fd, &cc.length, sizeof(uint64_t));
    assert(bytes_write == sizeof(uint64_t));

    // printf("copy: offset: %d, length: %d\n", cc.offset, cc.length);

    #ifdef IO_PRINT
        stats->write_io_bytes += 3;
    #endif

    return 0;
}

int generate_delta(struct matched_chunks *mc, int new_fd, char *delta_file_path) {
    uint64_t matched_chunks_num = mc->matched_chunks_num;
    uint64_t unmatched_chunks_num = mc->unmatched_chunks_num;

    uint64_t bytes_read = 0, bytes_write = 0;

    struct matched_item *matched_chunks = mc->matched_item_array;
    uint64_t real_matched_nums = 0;
    struct real_matched *ream = process_matched_chunks(matched_chunks, matched_chunks_num, &real_matched_nums);
    // for(int i = 0; i < real_matched_nums; i++) {
    //     printf("i: %d, new_ol: offset: %lu, length: %lu\n", i, ream[i].new_ol.offset, ream[i].new_ol.length);
    // }

    int delta_fd = open(delta_file_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);

    uint64_t new_fs = file_size(new_fd);

    uint64_t offset = 0;
    uint64_t copy_offset = 0;
    uint64_t copy_length = 0;

    if(real_matched_nums == 0) {
        append_literal_cmd_fd(delta_fd, new_fd, 0, new_fs);
        goto out;
    }

    for(int i = 0; i < real_matched_nums; i++) {
        struct real_matched* ream_t = ream + i;
        if(ream_t->new_ol.offset > offset) {
            if(copy_length != 0) {
                append_copy_cmd(delta_fd, copy_offset, copy_length);
                // printf("copy: offset: %d, length: %d\n", copy_offset, copy_length);
            }
            append_literal_cmd_fd(delta_fd, new_fd, offset, ream_t->new_ol.offset - offset);
            // printf("literal: length: %d\n", ream_t.new_ol.offset - offset);
            offset = ream_t->new_ol.offset;
            copy_offset = ream_t->old_ol.offset;
            copy_length = 0;
        }
        if(copy_length == ream_t->old_ol.offset - copy_offset) {
            copy_length += ream_t->old_ol.length;
            offset += ream_t->new_ol.length;
        }
        if(i == real_matched_nums - 1) {
            append_copy_cmd(delta_fd, copy_offset, copy_length);
            if (offset < new_fs) {
                append_literal_cmd_fd(delta_fd, new_fd, offset, new_fs - offset);
            }
        }
    }

out:
    for(int i = 0; i < matched_chunks_num; i++) {
        if(matched_chunks[i].new_ol_array)
            free(matched_chunks[i].new_ol_array);
    }
    if(mc->matched_item_array)
        free(mc->matched_item_array);
    if(mc->unmatched_item_array)
        free(mc->unmatched_item_array);
    if(mc)
        free(mc);
    if(ream)
        free(ream);
    close(delta_fd);
    return 0;
}

int fastcdc_patch_delta(char *old_file_path, char* delta_file_path, char* out_file_path) {
    uint64_t chunk_num = 0;
    uint64_t bytes_read = 0, bytes_write = 0;
    uint64_t old_fs = 0, delta_fs = 0;
    uint64_t delta_file_offset = 0;

    int old_fd = open(old_file_path, O_RDONLY);
    if (old_fd == -1) {
        fprintf(stderr, "Error opening file %s\n", old_file_path);
        exit(-1);
    }
    int delta_fd = open(delta_file_path, O_RDONLY);
    if (delta_fd == -1) {
        fprintf(stderr, "Error opening file %s\n", delta_file_path);
        close(old_fd);
        exit(-1);
    }
    int output_fd = open(out_file_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (output_fd == -1) {
        fprintf(stderr, "Error opening file %s\n", out_file_path);
        close(old_fd);
        close(delta_fd);
        exit(-1);
    }
    
    old_fs = file_size(old_fd);
    delta_fs = file_size(delta_fd);

    /* compare the command and generate the output file */
    // while(head < delta_file_cache + delta_fs) {
    while (delta_file_offset < delta_fs) {
        uint8_t cmd = 0;
        lseek(delta_fd, delta_file_offset, SEEK_SET);
        bytes_read = read(delta_fd, &cmd, sizeof(uint8_t));
        assert(bytes_read == sizeof(uint8_t));
        delta_file_offset += sizeof(uint8_t);

        switch (cmd)
        {
            case CMD_LITERAL:
            {
                uint64_t length = 0;
                bytes_read = read(delta_fd, &length, sizeof(uint64_t));
                assert(bytes_read == sizeof(uint64_t));
                delta_file_offset += sizeof(uint64_t);

                off_t send_offset = delta_file_offset;
                uint64_t bytes_sent = 0;
                uint64_t total_bytes_sent = 0;

                while (total_bytes_sent < length) {
                    bytes_sent = sendfile(output_fd, delta_fd, &send_offset, length - total_bytes_sent);
                    if (bytes_sent <= 0) {
                        perror("sendfile");
                        return -1;
                    }
                    total_bytes_sent += bytes_sent;
                }

                assert(total_bytes_sent == length);
                delta_file_offset += length;
                break;
            }
            case CMD_COPY:
            {
                uint64_t offset = 0;
                bytes_read = read(delta_fd, &offset, sizeof(uint64_t));
                assert(bytes_read == sizeof(uint64_t));
                uint64_t length = 0;
                bytes_read = read(delta_fd, &length, sizeof(uint64_t));
                assert(bytes_read == sizeof(uint64_t));
                delta_file_offset += sizeof(uint64_t) * 2;

                off_t send_offset = offset;
                uint64_t bytes_sent = 0;
                uint64_t total_bytes_sent = 0;

                while (total_bytes_sent < length) {
                    bytes_sent = sendfile(output_fd, old_fd, &send_offset, length - total_bytes_sent);
                    if (bytes_sent <= 0) {
                        perror("sendfile");
                        return -1;
                    }
                    total_bytes_sent += bytes_sent;
                }
                assert(total_bytes_sent == length);
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
