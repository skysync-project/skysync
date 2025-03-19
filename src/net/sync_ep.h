
#pragma once
#ifndef SYNC_EP_H
#define SYNC_EP_H

#include <fcntl.h>
#include <chrono>
#include <gflags/gflags.h>

#include <photon/photon.h>
#include <photon/io/signal.h>
#include <photon/thread/thread11.h>
#include <photon/thread/workerpool.h>
#include <photon/common/alog.h>
#include <photon/net/socket.h>

#include <photon/rpc/rpc.h>
#include <unistd.h>

#include "skysync_c.h"
#include "parallel_cdc.h"
#include "dsync_worker.h"

static std::string filename;
static std::string output_filename;
static ServerSyncWorker server_worker;
static ClientSyncWorker client_worker;

using namespace photon;

static const int checksum_padding_size = 4096;

DEFINE_string(ip, "127.0.0.1", "ip");
DEFINE_uint64(port, 9527, "port");
DEFINE_uint64(buf_size, 40960, "RPC buffer size");
DEFINE_uint64(num_threads, 32, "num of threads");
DEFINE_string(file_name, "", "File to be syncronized");

struct SyncReqProto {
    const static uint32_t IID = 9527;
    const static uint32_t FID = 1;

    struct Request : public rpc::Message {
        std::string file_name;

        PROCESS_FIELDS(file_name);
    };

    struct Response : public rpc::Message {
        bool success;

        PROCESS_FIELDS(success);
    };
};

struct WriteCRC32Proto {
    const static uint32_t IID = 9527;
    const static uint32_t FID = 2;

    struct Request : public rpc::Message {
        rpc::aligned_buffer buf;
        bool end_of_stream;
        PROCESS_FIELDS(buf, end_of_stream);
    };

    struct Response : public rpc::Message {
        int code;

        PROCESS_FIELDS(code);
    };
};

struct WeakMatchedChunksProto {
    const static uint32_t IID = 9527;
    const static uint32_t FID = 3;

    struct Request : public rpc::Message {
        // Empty request since client just needs to poll for results
        bool request;
        PROCESS_FIELDS(request);
    };

    struct Response : public rpc::Message {
        rpc::aligned_buffer matched_chunks;
        bool end_of_stream;
        PROCESS_FIELDS(matched_chunks, end_of_stream);
    };
};

struct DataCmdProto {
    const static uint32_t IID = 9527;
    const static uint32_t FID = 4;

    struct Request : public rpc::Message {
        rpc::aligned_buffer cmd_data;
        bool end_of_stream;
        uint32_t cmd;      // CMD_LITERAL or CMD_COPY
        uint64_t offset;   // Used for CMD_COPY
        uint64_t length;   // Length of data
        PROCESS_FIELDS(cmd_data, end_of_stream, cmd, offset, length);
    };

    struct Response : public rpc::Message {
        int code;
        PROCESS_FIELDS(code);
    };
};

#endif // SYNC_EP_H