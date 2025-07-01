#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "skysync_f_worker.h"

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <old_file> <new_file>\n", argv[0]);
        exit(1);
    }

    int old_fd = open(argv[1], O_RDONLY);
    if (old_fd == -1) {
        printf("Failed to open old file %s\n", argv[1]);
        exit(1);
    }

    int new_fd = open(argv[2], O_RDONLY);
    if (new_fd == -1) {
        printf("Failed to open new file %s\n", argv[2]);
        close(old_fd);
        exit(1);
    }

    printf("Testing rolling_fsc_sw implementation...\n");
    
    // Create worker instance
    ClientSkySyncFWorker worker(0); // Use software implementation

    worker.old_csums = calc_fsc_hw(old_fd);
    worker.new_csums = calc_fsc_hw(new_fd);
    
    // Build hash tables
    printf("Building hash tables...\n");
    worker.chash_builder(worker.old_csums);

    // Test rolling_fsc_sw
    printf("Running rolling_fsc_sw...\n");
    auto start = std::chrono::high_resolution_clock::now();

    worker.rolling_fsc_sw(new_fd, worker.old_csums, worker.new_csums, worker.data_cmd_queue);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    
    printf("Rolling FSC time: %f seconds\n", diff.count());
    
    // Count generated commands
    uint64_t literal_commands = 0;
    uint64_t copy_commands = 0;
    uint64_t total_literal_bytes = 0;
    uint64_t total_copy_bytes = 0;
    
    while (!worker.data_cmd_queue.isDone()) {
        data_cmd cmd = worker.data_cmd_queue.pop();
        printf("Command: %d, Offset: %lu, Length: %lu\n", cmd.cmd, cmd.offset, cmd.length);
        
        if (cmd.cmd == CMD_LITERAL) {
            literal_commands++;
            total_literal_bytes += cmd.length;
            if (cmd.data) mi_free(cmd.data);
        } else if (cmd.cmd == CMD_COPY) {
            copy_commands++;
            total_copy_bytes += cmd.length;
        }
    }
    
    printf("\nResults:\n");
    printf("  Literal commands: %lu (total bytes: %lu)\n", literal_commands, total_literal_bytes);
    printf("  Copy commands: %lu (total bytes: %lu)\n", copy_commands, total_copy_bytes);
    printf("  Total commands: %lu\n", literal_commands + copy_commands);
    
    close(old_fd);
    close(new_fd);
    
    printf("Test completed successfully!\n");
    return 0;
}