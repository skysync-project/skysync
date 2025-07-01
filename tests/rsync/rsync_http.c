#include "librsync.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rsync_http.h"

// Define struct for input operations
typedef struct {
    const char *data;
    size_t len;
    size_t offset;
} mem_input_data;

// Define struct for output operations
typedef struct {
    char **data;
    size_t *len;
    size_t capacity;
} mem_output_data;

// Helper function to read from a memory buffer
static rs_result mem_read_callback(rs_job_t *job, rs_buffers_t *buf, void *opaque) {
    mem_input_data *md = (mem_input_data *)opaque;
    
    if (!md || !buf) {
        return RS_IO_ERROR;
    }
    
    size_t available = md->len - md->offset;
    
    if (available == 0) {
        buf->eof_in = 1;
        return RS_DONE;
    }
    
    // Only set avail_in, don't modify next_in
    size_t to_copy = available;
    if (buf->next_in) {
        memcpy(buf->next_in, md->data + md->offset, to_copy);
        md->offset += to_copy;
    }
    buf->avail_in = to_copy;
    
    return RS_DONE;
}

// Helper function to write to a memory buffer
static rs_result mem_write_callback(rs_job_t *job, rs_buffers_t *buf, void *opaque) {
    mem_output_data *md = (mem_output_data *)opaque;
    
    if (!md || !buf || buf->avail_out == 0) {
        return RS_DONE;
    }
    
    // Ensure we have enough capacity
    if (*md->len + buf->avail_out > md->capacity) {
        size_t new_capacity = md->capacity == 0 ? 4096 : md->capacity;
        while (*md->len + buf->avail_out > new_capacity) {
            new_capacity *= 2;
        }
        char *new_data = (char *)realloc(*md->data, new_capacity);
        if (!new_data) {
            return RS_MEM_ERROR;
        }
        *md->data = new_data;
        md->capacity = new_capacity;
    }
    
    // Copy data and advance next_out pointer
    memcpy(*md->data + *md->len, buf->next_out, buf->avail_out);
    *md->len += buf->avail_out;
    buf->next_out += buf->avail_out;
    buf->avail_out = 0;
    
    return RS_DONE;
}

// Copy callback for patch operation
static rs_result copy_callback(void *opaque, rs_long_t pos, size_t *len, void **buf) {
    mem_input_data *md = (mem_input_data *)opaque;
    
    if (!md || !len || !buf) {
        return RS_IO_ERROR;
    }
    
    if (pos >= (rs_long_t)md->len) {
        *len = 0;
        *buf = NULL;
        return RS_DONE;
    }
    
    size_t available = md->len - (size_t)pos;
    if (*len > available) {
        *len = available;
    }
    
    *buf = (void *)(md->data + pos);
    return RS_DONE;
}

rs_result rsyncx_signature(const char *base, const char *sig, const char *rs_hash_name, const char *rs_rollsum_name, bool hw) {
    FILE *basis_file, *sig_file;
    rs_stats_t stats;
    rs_result result;
    rs_magic_number sig_magic;
    size_t block_len = CHUNK_SIZE;
    size_t strong_len = 0;

    basis_file = rs_file_open(base, "rb", 0);
    sig_file = rs_file_open(sig, "wb", 0);

    if (!rs_hash_name || !strcmp(rs_hash_name, "blake2")) {
        sig_magic = RS_BLAKE2_SIG_MAGIC;
    } else if (!strcmp(rs_hash_name, "md4")) {
        sig_magic = RS_MD4_SIG_MAGIC;
    }

    if (!rs_rollsum_name || !strcmp(rs_rollsum_name, "rabinkarp")) {
        /* The RabinKarp magics are 0x10 greater than the rollsum magics. */
        sig_magic += 0x10;
    } else if (strcmp(rs_rollsum_name, "rollsum")) {
        exit(RS_SYNTAX_ERROR);
    }

    result =
        rs_sig_file(basis_file, sig_file, block_len, strong_len, sig_magic,
                    &stats);

    rs_file_close(sig_file);
    rs_file_close(basis_file);
    if (result != RS_DONE)
        return result;

    return result;
}

rs_result rsyncx_delta(const char *sig_name, const char *new, const char *delta) {
    FILE *sig_file, *new_file, *delta_file;
    rs_result result;
    rs_signature_t *sumset;
    rs_stats_t stats;

    sig_file = rs_file_open(sig_name, "rb", 0);
    new_file = rs_file_open(new, "rb", 0);
    delta_file = rs_file_open(delta, "wb", 0);

    result = rs_loadsig_file(sig_file, &sumset, &stats);
    if (result != RS_DONE)
        return result;

    if ((result = rs_build_hash_table(sumset)) != RS_DONE)
        return result;

    result = rs_delta_file(sumset, new_file, delta_file, &stats);

    rs_file_close(delta_file);
    rs_file_close(new_file);
    rs_file_close(sig_file);

    rs_free_sumset(sumset);

    return result;
}

rs_result rsyncx_signature_mem(const char *basis_data, size_t basis_len, char **sig_data, size_t *sig_len, bool hw) {
    rs_job_t *job = NULL;
    rs_result result = RS_DONE;
    rs_buffers_t buf;
    char *output_buffer = NULL;
    size_t output_capacity = 0;
    size_t output_pos = 0;
    size_t block_len = CHUNK_SIZE;
    size_t strong_len = 0;
    rs_magic_number sig_magic = hw ? HASH_ALGORITHM_HW : HASH_ALGORITHM_SOFT;
    // sig_magic += 0x10;

    // Validate input parameters
    if (!basis_data || basis_len == 0 || !sig_data || !sig_len) {
        return RS_PARAM_ERROR;
    }
    
    // Initialize output
    *sig_data = NULL;
    *sig_len = 0;
    
    // Determine optimal signature parameters
    result = rs_sig_args(basis_len, &sig_magic, &block_len, &strong_len);
    if (result != RS_DONE) {
        return result;
    }
    
    // Create signature job
    job = rs_sig_begin(block_len, strong_len, sig_magic);
    if (!job) {
        return RS_MEM_ERROR;
    }
    
    // Estimate output buffer size (header + blocks)
    // Header: 12 bytes (magic + block_len + strong_len)
    // Each block: 4 bytes (weak sum) + strong_len bytes (strong sum)
    size_t num_blocks = (basis_len + block_len - 1) / block_len;
    output_capacity = 12 + num_blocks * (4 + strong_len);
    
    // Allocate output buffer with some extra space
    output_buffer = malloc(output_capacity + 1024);
    if (!output_buffer) {
        rs_job_free(job);
        return RS_MEM_ERROR;
    }
    
    // Initialize buffers
    memset(&buf, 0, sizeof(buf));
    buf.next_in = (char *)basis_data;
    buf.avail_in = basis_len;
    buf.next_out = output_buffer;
    buf.avail_out = output_capacity + 1024;
    buf.eof_in = 1; // All input data is available
    
    // Process the signature generation
    do {
        result = rs_job_iter(job, &buf);
        
        // Check if we need to expand output buffer
        if (result == RS_BLOCKED && buf.avail_out == 0) {
            size_t current_size = buf.next_out - output_buffer;
            size_t new_capacity = output_capacity * 2;
            char *new_buffer = realloc(output_buffer, new_capacity);
            
            if (!new_buffer) {
                free(output_buffer);
                rs_job_free(job);
                return RS_MEM_ERROR;
            }
            
            output_buffer = new_buffer;
            buf.next_out = output_buffer + current_size;
            buf.avail_out = new_capacity - current_size;
            output_capacity = new_capacity;
        }
    } while (result == RS_RUNNING || result == RS_BLOCKED);
    
    if (result == RS_DONE) {
        // Calculate final output size
        *sig_len = buf.next_out - output_buffer;
        
        // Trim buffer to actual size
        if (*sig_len > 0) {
            char *final_buffer = malloc(*sig_len);
            if (final_buffer) {
                memcpy(final_buffer, output_buffer, *sig_len);
                free(output_buffer);
                *sig_data = final_buffer;
                output_buffer = NULL; // Prevent double free
            } else {
                // If trim fails, just use the original buffer
                *sig_data = output_buffer;
                output_buffer = NULL; // Prevent double free
            }
        } else {
            result = RS_INTERNAL_ERROR;
        }
    }
    
    // Cleanup
    if (output_buffer) {
        free(output_buffer);
    }
    rs_job_free(job);
    
    // On error, ensure output is clean
    if (result != RS_DONE) {
        if (*sig_data) {
            free(*sig_data);
            *sig_data = NULL;
        }
        *sig_len = 0;
    }
    
    return result;
}

rs_result rsyncx_delta_mem(const char *sig_data, size_t sig_len, const char *new_data, size_t new_len, char **delta_data, size_t *delta_len) {
    rs_result result;
    rs_signature_t *sig = NULL;
    rs_job_t *job = NULL;
    rs_buffers_t buf;
    char *output_buf = NULL;
    size_t output_size = 0;
    size_t output_capacity = BUFFER_SIZE;
    size_t input_pos = 0;
    
    // Validate input parameters
    if (!sig_data || !new_data || !delta_data || !delta_len) {
        return RS_PARAM_ERROR;
    }
    
    *delta_data = NULL;
    *delta_len = 0;
    
    // Load signature from memory
    job = rs_loadsig_begin(&sig);
    if (!job) {
        return RS_MEM_ERROR;
    }
    
    // Initialize buffers
    memset(&buf, 0, sizeof(buf));
    buf.next_in = (char *)sig_data;
    buf.avail_in = sig_len;
    buf.eof_in = 1; // Signal end of signature data
    
    // Process signature data
    do {
        result = rs_job_iter(job, &buf);
        if (result != RS_DONE && result != RS_BLOCKED) {
            rs_job_free(job);
            return result;
        }
    } while (result != RS_DONE);
    
    rs_job_free(job);
    
    if (!sig) {
        return RS_CORRUPT;
    }
    
    // Build hash table for signature
    result = rs_build_hash_table(sig);
    if (result != RS_DONE) {
        rs_free_sumset(sig);
        return result;
    }
    
    // Allocate output buffer
    output_buf = malloc(output_capacity);
    if (!output_buf) {
        rs_free_sumset(sig);
        return RS_MEM_ERROR;
    }
    
    // Create delta job
    job = rs_delta_begin(sig);
    if (!job) {
        free(output_buf);
        rs_free_sumset(sig);
        return RS_MEM_ERROR;
    }
    
    // Initialize buffers for delta generation
    memset(&buf, 0, sizeof(buf));
    buf.next_out = output_buf;
    buf.avail_out = output_capacity;
    
    // Process new data to generate delta
    while (input_pos < new_len || !buf.eof_in) {
        // Fill input buffer if needed
        if (buf.avail_in == 0 && input_pos < new_len) {
            size_t chunk_size = (new_len - input_pos > CHUNK_SIZE) ? CHUNK_SIZE : (new_len - input_pos);
            buf.next_in = (char *)(new_data + input_pos);
            buf.avail_in = chunk_size;
            input_pos += chunk_size;
            
            // Set EOF flag when we've consumed all input
            if (input_pos >= new_len) {
                buf.eof_in = 1;
            }
        }
        
        // Expand output buffer if needed
        if (buf.avail_out < CHUNK_SIZE) {
            size_t used = output_capacity - buf.avail_out;
            output_capacity *= 2;
            char *new_buf = realloc(output_buf, output_capacity);
            if (!new_buf) {
                free(output_buf);
                rs_job_free(job);
                rs_free_sumset(sig);
                return RS_MEM_ERROR;
            }
            output_buf = new_buf;
            buf.next_out = output_buf + used;
            buf.avail_out = output_capacity - used;
        }
        
        // Process data
        result = rs_job_iter(job, &buf);
        if (result != RS_DONE && result != RS_BLOCKED) {
            free(output_buf);
            rs_job_free(job);
            rs_free_sumset(sig);
            return result;
        }
        
        // Update output size
        output_size = output_capacity - buf.avail_out;
        
        if (result == RS_DONE) {
            break;
        }
    }
    
    // Cleanup and return results
    rs_job_free(job);
    rs_free_sumset(sig);
    
    // Trim output buffer to actual size
    if (output_size < output_capacity) {
        char *trimmed_buf = realloc(output_buf, output_size);
        if (trimmed_buf) {
            output_buf = trimmed_buf;
        }
    }
    
    *delta_data = output_buf;
    *delta_len = output_size;
    
    return RS_DONE;
}

rs_result rsyncx_patch_mem(const char *basis_data, size_t basis_len, const char *delta_data, size_t delta_len, char **new_data, size_t *new_len) {
    rs_result result;
    rs_job_t *job = NULL;
    rs_buffers_t buf;
    char *output_buf = NULL;
    size_t output_capacity = BUFFER_SIZE;
    size_t output_size = 0;
    size_t delta_pos = 0;
    mem_input_data basis_md = {basis_data, basis_len, 0};
    
    // Validate input parameters
    if (!basis_data || !delta_data || !new_data || !new_len) {
        return RS_PARAM_ERROR;
    }
    
    *new_data = NULL;
    *new_len = 0;
    
    // Allocate initial output buffer
    output_buf = malloc(output_capacity);
    if (!output_buf) {
        return RS_MEM_ERROR;
    }
    
    // Create patch job with copy callback
    job = rs_patch_begin(copy_callback, &basis_md);
    if (!job) {
        free(output_buf);
        return RS_MEM_ERROR;
    }
    
    // Initialize buffer structure
    memset(&buf, 0, sizeof(buf));
    buf.next_out = output_buf;
    buf.avail_out = output_capacity;
    
    // Process delta data to reconstruct new file
    while (delta_pos < delta_len || !buf.eof_in) {
        // Fill input buffer if needed and there's more delta data
        if (buf.avail_in == 0 && delta_pos < delta_len) {
            size_t chunk_size = (delta_len - delta_pos > CHUNK_SIZE) ? 
                               CHUNK_SIZE : (delta_len - delta_pos);
            buf.next_in = (char *)(delta_data + delta_pos);
            buf.avail_in = chunk_size;
            delta_pos += chunk_size;
            
            // Set EOF when all delta data is consumed
            if (delta_pos >= delta_len) {
                buf.eof_in = 1;
            }
        }
        
        // Expand output buffer if nearly full
        if (buf.avail_out < CHUNK_SIZE) {
            size_t used = output_capacity - buf.avail_out;
            size_t new_capacity = output_capacity * 2;
            char *new_buf = realloc(output_buf, new_capacity);
            if (!new_buf) {
                free(output_buf);
                rs_job_free(job);
                return RS_MEM_ERROR;
            }
            output_buf = new_buf;
            buf.next_out = output_buf + used;
            buf.avail_out = new_capacity - used;
            output_capacity = new_capacity;
        }
        
        // Process the patch operation
        result = rs_job_iter(job, &buf);
        
        // Handle different result states
        if (result == RS_DONE) {
            break;
        } else if (result != RS_BLOCKED && result != RS_RUNNING) {
            free(output_buf);
            rs_job_free(job);
            return result;
        }
        
        // Update output size
        output_size = output_capacity - buf.avail_out;
    }
    
    // Cleanup job
    rs_job_free(job);
    
    if (result == RS_DONE) {
        // Calculate final output size
        output_size = output_capacity - buf.avail_out;
        
        // Trim buffer to actual size to save memory
        if (output_size > 0 && output_size < output_capacity) {
            char *trimmed_buf = realloc(output_buf, output_size);
            if (trimmed_buf) {
                output_buf = trimmed_buf;
            }
        }
        
        *new_data = output_buf;
        *new_len = output_size;
    } else {
        // Clean up on failure
        free(output_buf);
    }
    
    return result;
}