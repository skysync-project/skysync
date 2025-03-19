#include <chrono>
#include "net.h"
#include "dsync_worker.h"

int main(int argc, char **argv) {
    if (argc < 3) {
        printf("Usage: %s <filename> <ip:port>\n", argv[0]);
        exit(1);
    }

    // printf("Get digs %s from %s using %s\n", argv[1], argv[2], argv[3]);
    printf("Sync %s to %s\n", argv[1], argv[2]);

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        printf("open file %s failed\n", argv[1]);
        exit(1);
    }

    ClientSyncWorker client_worker;

    std::vector<std::thread> threads;
    threads.resize(8);

    post_sync_req(argv[2], argv[1]);

    threads[0] = std::thread([&client_worker, fd]() {
        client_worker.serial_cdc(fd, client_worker.new_csums_queue);
    });
    threads[0].detach();

    // threads[1] = std::thread([argv, &client_worker, fd]() {
    //     post_weak_csums(argv[2], client_worker.new_csums_queue, client_worker.weak_matched_chunks_queue);
    // });
}
