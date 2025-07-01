#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#ifdef  __cplusplus
extern "C" {
#endif

// C ring buffer (FIFO) interface

/*
 * A byte-addressable ring buffer FIFO implementation.
 *
 * The ring buffer's head pointer points to the starting location
 * where data should be written when reading data from file *into* the buffer
 * (e.g., with ringbuf_read). The ring buffer's tail pointer points to
 * the starting location where data should be used when copying data
 * *from* the buffer (e.g., with ringbuf_write).
 */

#include <stddef.h>
#include <sys/types.h>

// typedef struct ringbuf_t *ringbuf_t;
struct ringbuf_t;

#define CHUNK_SIZE 8192
#define WEAK_CSUM_SIZE 4
#define ITEMNUMS 16
#define WIN_SIZE CHUNK_SIZE
#define BUFFER_SIZE 256 * CHUNK_SIZE

// Create a new ring buffer with the given size.
struct ringbuf_t* ringbuf_create(size_t size);

// Destroy a ring buffer.
void ringbuf_destroy(struct ringbuf_t *rb);

void* ringbuf_tail(const struct ringbuf_t *rb);

void* ringbuf_head(const struct ringbuf_t *rb);

void* ringbuf_start(const struct ringbuf_t *rb);

void* ringbuf_end(const struct ringbuf_t *rb);

// Return the number of bytes available to read from the buffer.
size_t ringbuf_available(const struct ringbuf_t *rb);

// Return the number of bytes used in the buffer.
size_t ringbuf_used(const struct ringbuf_t *rb);

// Read up to count bytes from the file descriptor fd into the ring buffer.
// Returns the number of bytes read, or -1 on error.
ssize_t ringbuf_read_from_fd(int fd, struct ringbuf_t *rb, size_t count);

// Read up to count bytes from the memory map into the ring buffer.
ssize_t ringbuf_read_from_map(void* map, struct ringbuf_t *rb, size_t count);

// Remove up to count bytes from the buffer.
ssize_t ringbuf_remove(struct ringbuf_t *rb, size_t count);

#ifdef  __cplusplus
}
#endif

#endif // RING_BUFFER_H