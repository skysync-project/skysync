#include "net.h"
#include <time.h>

int main(int argc, char **argv) {
    if(argc < 2) {
        printf("Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    mhd_http_server(atoi(argv[1]));
}
