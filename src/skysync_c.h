#ifndef SKYSYNC_C_H
#define SKYSYNC_C_H

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <zlib.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <libgen.h>
#include <sys/mman.h>
#include <mimalloc-2.1/mimalloc.h>

#include "fastcdc.h"
#include "skysync_f.h"

void cdc_build_uthash(struct cdc_uthash **hash_table, struct one_cdc *cdc_array, uint64_t chunk_num);

void cdc_delete_uthash(struct cdc_uthash **hash_table);

void cdc_print_uthash(struct cdc_uthash **hash_table);

/* ===============CDC ops=============== */

uint64_t cdc_origin_64_skysync(unsigned char *p, uint64_t n);

size_t cdc_normalized_chunking_2bytes_64(const unsigned char *p, uint64_t n);

struct cdc_crc32* cdc_calc_crc32(char * file_name);

void cdc_combine(struct one_cdc *cdc_array, char *file_buf);

struct file_cdc* serial_cdc_1(int fd, char *map);

int serial_cdc_2(char *old_file_path, char *write_file_path, struct stats *st);

struct file_cdc* read_cdc(char *file_path, struct stats *st);

struct cdc_matched_chunks* compare_weak_hash(int fd, struct file_cdc *old_fc,
                                             struct file_cdc *new_fc);

int cdc_write_matched(struct cdc_matched_chunks *mc, char *matched_file_path, struct stats *stats);

struct cdc_matched_chunks* cdc_read_matched(char *file_path, struct stats *stats);

void cdc_compare_sha1(struct cdc_matched_chunks *mc, int fd);

struct real_matched* cdc_process_matched_chunks(struct matched_item *mc, uint64_t matched_chunks_num, uint64_t *real_nums);

int cdc_generate_delta(struct cdc_matched_chunks *mc, int new_fd, char *delta_file_path);


#endif // SKYSYNC_C_H
