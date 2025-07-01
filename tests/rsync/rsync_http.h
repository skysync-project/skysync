#ifndef RSYNC_HTTP_H
#define RSYNC_HTTP_H

#include "librsync.h" // For rs_result
#include <stdlib.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// 4MB buffer size for rsync operations
#define BUFFER_SIZE (4 * 1024 * 1024)
#define CHUNK_SIZE (8 * 1024)
#define HASH_ALGORITHM_SOFT RS_BLAKE2_SIG_MAGIC
#define HASH_ALGORITHM_HW RS_MD4_SIG_MAGIC

rs_result rsyncx_signature(const char *base_file, const char *sig_file, const char *rs_hash_name, const char *rs_rollsum_name, bool hw);
rs_result rsyncx_signature_mem(const char *basis_data, size_t basis_len, char **sig_data, size_t *sig_len, bool hw);
rs_result rsyncx_delta(const char *sig_name, const char *new_file, const char *delta_file);
rs_result rsyncx_delta_mem(const char *sig_data, size_t sig_len, const char *new_data, size_t new_len, char **delta_data, size_t *delta_len);
rs_result rsyncx_patch_mem(const char *basis_data, size_t basis_len, const char *delta_data, size_t delta_len, char **new_data, size_t *new_len);

#ifdef __cplusplus
}
#endif

#endif // RSYNC_HTTP_H