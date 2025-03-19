#include "net.h"

int main(int argc, char **argv) {
    if (argc < 3) {
        printf("Usage: %s <file> <ip>\n", argv[0]);
        exit(1);
    }

    printf("Post file %s to %s\n", argv[1], argv[2]);
    http_client_post_file(argv[2], argv[1]);
}
