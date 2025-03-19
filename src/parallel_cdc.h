#ifndef PARALLEL_CDC_H
#define PARALLEL_CDC_H

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
#include <omp.h>
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
#include "ring_buffer.h"
#include "chunker.h"
#include "parasync_common.h"

struct file_cdc* parallel_run_cdc(char *file_path, uint32_t thread_num,
                                std::vector<std::thread> &threads,
                                struct stats *st, int whichone);

#endif // PARALLEL_CDC_H