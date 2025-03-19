#ifndef PARALLEL_MATCHER_H
#define PARALLEL_MATCHER_H

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <zlib.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <libgen.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <vector>
#include <thread>
#include <pthread.h>
#include <barrier>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <photon/photon.h>
#include <photon/thread/std-compat.h>
#include <photon/common/alog.h>
#include <photon/fs/localfs.h>
#include <photon/thread/workerpool.h>
#include <photon/thread/thread11.h>
#include <mimalloc-2.1/mimalloc.h>

#include "skysync_f.h"
#include "skysync_c.h"
#include "wmatcher.h"
#include "smatcher.h"

void wmatcher_thread_worker(void *arg);

void smatcher_thread_worker(void *arg);

size_t process_crc32_queue (DataQueue<uint32_t> &new_crc32_queue, uint32_t *weak_hash);

void parallel_matcher_pdsync(int old_fd, int new_fd, uint32_t thread_num, ClientSyncWorker &client_worker, ServerSyncWorker &server_worker);

#endif // PARALLEL_MATCHER_H