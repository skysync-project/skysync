#include <stdio.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

void *map_file(int fd) {
    uint64_t fs = lseek(fd, 0, SEEK_END);
    void *map = mmap(NULL, fs, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        return NULL;
    }
    return map;
}

void unmap_file(int fd, void *map) {
    munmap(map, lseek(fd, 0, SEEK_END));
}

/* ========== String utilities ========== */

char bin2hex_char(uint8_t nibble)
{
    assert(nibble <= 0xf);

	if (nibble < 10)
		return '0' + nibble;
	return 'a' + (nibble - 10);
}

void bin2hex(const uint8_t *bin, uint64_t bin_len, char *hex)
{
	uint64_t i;

	for (i = 0; i < bin_len; i++) {
		*hex++ = bin2hex_char(bin[i] >> 4);
		*hex++ = bin2hex_char(bin[i] & 0xf);
	}
	*hex = '\0';
}

/* ========== Memory allocation ========== */

void *xzalloc(uint64_t size)
{
	return memset(malloc(size), 0, size);
}

void *xmemdup(const void *mem, uint64_t size)
{
	return memcpy(malloc(size), mem, size);
}

char *xstrdup(const char *s)
{
	return (char *) xmemdup(s, strlen(s) + 1);
}

/* ======================================= */

off_t file_size(int fd) {
   struct stat s;
   if (fstat(fd, &s) == -1) {
      printf("fstat(%d) returned error.", fd);
      return(-1);
   }
   return(s.st_size);
}

char *gnu_basename(char *path)
{
    char *base = strrchr(path, '/');
    return base ? base+1 : path;
}

int is_zero(void *buf, uint64_t size)
{
	uint8_t *p = (uint8_t *)buf;
	int sum = 0;
	for(int i = 0; i < size; i++) {
		sum |= p[i];
	}
	return sum == 0;
}
