#include <chrono>

#include "skysync_f.h"
#include "skysync_c.h"
#include "parasync_worker.h"

off_t file_size(int fd);

int main(int argc, char** argv) {
    if (argc < 5) {
        printf("Usage: %s <old_file> <new_file> <thread_nums> <mode>\n", argv[0]);
        exit(1);
    }

    int old_fd = open(argv[1], O_RDONLY);
    if (old_fd == -1) {
        printf("open file %s failed\n", argv[1]);
        exit(1);
    }

    int new_fd = open(argv[2], O_RDONLY);
    if (new_fd == -1) {
        printf("open file %s failed\n", argv[2]);
        exit(1);
    }

    size_t thread_num = atoi(argv[3]);
    int mode = atoi(argv[4]);
    printf("ParaSync: %s, %s, %zu, %d\n", argv[1], argv[2], thread_num, mode);

    ClientParaSyncWorker client_worker;
    client_worker.init(thread_num);
    ServerParaSyncWorker server_worker;
    server_worker.init(thread_num);

    /* server */
    printf("Server: parallel CDC\n");
    server_worker.parallel_cdc(old_fd, thread_num, server_worker.old_csums_queue, server_worker.old_crc32_queue);

    /* client */
    printf("Client: parallel CDC\n");
    client_worker.parallel_cdc(new_fd, thread_num, client_worker.new_csums_queue, client_worker.new_crc32_queue);

    /* server */
    auto start = std::chrono::high_resolution_clock::now();
    if (mode)
        server_worker.parallel_wmatcher_pipe(old_fd, thread_num, client_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue);
    else
        server_worker.parallel_wmatcher(old_fd, thread_num, client_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue);
    auto end = std::chrono::high_resolution_clock::now();
    printf("Server: parallel Wmatcher, %f\n", std::chrono::duration<double>(end - start).count());

    /* client */
    start = std::chrono::high_resolution_clock::now();
    if (mode)
        client_worker.parallel_smatcher_v3(new_fd, thread_num, server_worker.weak_matched_chunks_queue, client_worker.strong_matched_chunks_queue);
    else
        client_worker.parallel_smatcher_v1(new_fd, thread_num, server_worker.weak_matched_chunks_queue, client_worker.strong_matched_chunks_queue);
    end = std::chrono::high_resolution_clock::now();
    printf("Client: parallel Smatcher, %f\n\n", std::chrono::duration<double>(end - start).count());

    close(old_fd);
    close(new_fd);
}