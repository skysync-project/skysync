#include "ring_buffer.h"  
  
#include <stdio.h>  
#include <stdlib.h>  
#include <string.h>  
#include <unistd.h>  
#include <stdint.h>  
#include <assert.h>  
  
struct ringbuf_t {
    uint8_t *buf;
    uint8_t *head, *tail;
    size_t size;
};
  
struct ringbuf_t* ringbuf_create(size_t size) {
    struct ringbuf_t *rb = malloc(sizeof(struct ringbuf_t));
    if (!rb) return NULL;

    rb->buf = malloc(size + 1);
    if (!rb->buf) {
        free(rb);
        return NULL;
    }

    rb->size = size + 1; // Adjust size to account for the extra byte
    rb->head = rb->buf;
    rb->tail = rb->buf;
  
    return rb;
}
  
void ringbuf_destroy(struct ringbuf_t *rb) {
    assert(rb && rb->buf);
    free(rb->buf);
    free(rb);
}
  
void* ringbuf_tail(const struct ringbuf_t *rb) {
    return rb->tail;
}
  
void* ringbuf_head(const struct ringbuf_t *rb) {
    return rb->head;
}

void* ringbuf_start(const struct ringbuf_t *rb) {
    return rb->buf;
}

void* ringbuf_end(const struct ringbuf_t *rb) {
    return rb->buf + rb->size;
}
  
size_t ringbuf_available(const struct ringbuf_t *rb) {
    if (rb->head <= rb->tail) {
        return rb->size - 1 - (rb->tail - rb->head);
    } else {
        return rb->head - rb->tail - 1;
    }
}

size_t ringbuf_used(const struct ringbuf_t *rb) {
    return rb->size - 1 - ringbuf_available(rb);
}
  
/**
 * @brief Reads data from a file descriptor into a ring buffer.
 *
 * This function attempts to read up to `count` bytes of data from the specified
 * file descriptor `fd` into the ring buffer `rb`. The actual number of bytes
 * read may be less than `count` if there is less data available in the ring buffer.
 *
 * @param fd The file descriptor to read from.
 * @param rb A pointer to the ring buffer structure.
 * @param count The maximum number of bytes to read.
 * @return The total number of bytes read, or -1 if an error occurs.
 */
ssize_t ringbuf_read_from_fd(int fd, struct ringbuf_t *rb, size_t count) {
    size_t available = ringbuf_available(rb);
    if (count > available) count = available;

    ssize_t total_read = 0;
    while (count > 0) {
        size_t chunk = (rb->size - (rb->tail - rb->buf)) < count ? (rb->size - (rb->tail - rb->buf)) : count;
        ssize_t read_bytes = read(fd, rb->tail, chunk);
        if (read_bytes <= 0) break;
  
        rb->tail += read_bytes;
        if (rb->tail == rb->buf + rb->size)
            rb->tail = rb->buf;
        count -= read_bytes;
        total_read += read_bytes;
    }

    return total_read;
}


/**
 * @brief Reads data from a memory-mapped region into the ring buffer.
 *
 * This function reads up to `count` bytes of data from the memory-mapped region
 * pointed to by `map` and writes it into the ring buffer `rb`. The actual number
 * of bytes read is limited by the available space in the ring buffer.
 *
 * @param map Pointer to the memory-mapped region to read from.
 * @param rb Pointer to the ring buffer structure.
 * @param count Number of bytes to read from the memory-mapped region.
 * @return The total number of bytes read into the ring buffer.
 */
ssize_t ringbuf_read_from_map(void* map, struct ringbuf_t *rb, size_t count) {
    size_t available = ringbuf_available(rb);
    if (count > available) count = available;

    ssize_t total_read = 0;
    char *tmp = (char *)map;
    while (count > 0) {
        size_t chunk = (rb->size - (rb->tail - rb->buf)) < count ? (rb->size - (rb->tail - rb->buf)) : count;
        memcpy(rb->tail, tmp, chunk);
  
        rb->tail += chunk;
        if (rb->tail == rb->buf + rb->size)
            rb->tail = rb->buf;
        count -= chunk;
        total_read += chunk;
    }

    return total_read;
}

ssize_t ringbuf_remove(struct ringbuf_t *rb, size_t count) {
    size_t used = ringbuf_used(rb);
    if (count > used) {
        count = used;
    }

    rb->head += count;
    if (rb->head >= rb->buf + rb->size) {
        rb->head -= rb->size;
    }

    return count;
}