#ifndef RPC_H
#define RPC_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <ev.h>
#include <netdb.h>
#include <err.h>
#include <signal.h>
#include <optional>
#include <openssl/ssl.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <gflags/gflags.h>

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>

#include <butil/time.h>
#include "butil/logging.h"
#include "brpc/channel.h"
#include "brpc/stream.h"
#include "brpc/server.h"
#include "bthread/bthread.h"

#include "dsync_worker.h"
#include "echo.pb.h"

static std::string filename;
static std::string output_filename;
static ServerSyncWorker server_worker;
static ClientSyncWorker client_worker;

std::condition_variable cv_1;
std::mutex cv_m_1;

std::condition_variable cv_2;
std::mutex cv_m_2;

std::condition_variable cv_3;
std::mutex cv_m_3;

std::condition_variable cv_4;
std::mutex cv_m_4;


class DataCommandStreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        for (size_t i = 0; i < size; ++i) {
            data_cmd cmd;
            messages[i]->copy_to(&cmd, sizeof(data_cmd));
            server_worker.data_cmd_queue.push(cmd);
        }
        return 0;
    }
    virtual void on_idle_timeout(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
    }
    virtual void on_closed(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " is closed";
    }
};

#endif // RPC_H