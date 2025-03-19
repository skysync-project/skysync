#include <string>
#include <unistd.h>
#include <cerrno>
#include <atomic>
#include <random>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <gflags/gflags.h>

#include <photon/io/fd-events.h>
#include <photon/thread/thread11.h>
#include <photon/net/socket.h>
#include <photon/common/alog.h>
#include <photon/common/alog-stdstring.h>
#include <photon/common/utility.h>
#include <photon/common/io-alloc.h>
#include <photon/rpc/rpc.h>
#include <photon/common/checksum/crc32c.h>
#include <liburing.h>
#include "sync_ep.h"

using namespace std;
using namespace photon;

DEFINE_int32(io_type, 0, "0: read, 1: write");

uint8_t** g_read_buffers = nullptr;
uint64_t g_rpc_count = 0;
uint64_t g_rpc_time_cost = 0;

uint32_t *g_crc32s = nullptr;
uint64_t g_crc32_count = 0;

class DsyncClient {
private:
    rpc::StubPool* pool;
    net::EndPoint ep;
    int new_fd;

public:
    DsyncClient(rpc::StubPool* pool, const net::EndPoint& ep, int fd) : pool(pool), ep(ep), new_fd(fd) {}

    void execute_sync() {
        // Start a thread to execute the client_worker.serial_cdc function
        std::thread cdc_thread([&]() {
            client_worker.serial_cdc(new_fd, client_worker.new_csums_queue);
            client_worker.chash_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue);
        });
        cdc_thread.join();

        // Write the sync request to the server
        write_sync_req();
        // Process client_worker.new_crc32_queue
        process_crc32_queue(client_worker.new_crc32_queue);
        // Write the CRC32 checksums to the server
        write_crc32();
        // Read the matched chunks from the server
        read_matched_chunks();

        // Process the matched chunks and generate the delta
        std::thread process_thread([&]() {
            client_worker.compare_sha1_1(new_fd, client_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);

            client_worker.generate_delta(new_fd, client_worker.strong_matched_chunks_queue, client_worker.data_cmd_queue);
        });
        process_thread.join();

        // Write the data commands to the server
        write_data_cmd();
    }

private:
    void write_sync_req() {
        rpc::Stub* stub = pool->get_stub(ep, false);
        if (stub == nullptr) {
            LOG_ERROR("cannot get stub");
            exit(1);
        }
        DEFER(pool->put_stub(ep, false));

        SyncReqProto::Request req;
        req.file_name = FLAGS_file_name;
        SyncReqProto::Response resp;

        int ret = stub->call<SyncReqProto>(req, resp);
        if (ret < 0) {
            LOG_ERROR("fail to call RPC");
            exit(-1);
        }
        if (!resp.success) {
            LOG_ERROR("sync req failed");
            exit(-1);
        }

        LOG_INFO("write sync req rpc done");
    }

    void process_crc32_queue(DataQueue<uint32_t>& queue) {
        g_crc32s = new uint32_t[queue.size()];
        g_crc32_count = queue.size();
        uint32_t i = 0;
        while (queue.size() > 0) {
            uint32_t crc32 = queue.pop();
            g_crc32s[i++] = crc32;
        }
    }

    void write_crc32() {
        rpc::Stub* stub = pool->get_stub(ep, false);
        if (stub == nullptr) {
            LOG_ERROR("cannot get stub");
            exit(1);
        }
        DEFER(pool->put_stub(ep, false));

        auto crc32_nums = FLAGS_buf_size / sizeof(uint32_t);
        uint32_t* crc32s = g_crc32s;

        while (g_crc32_count > crc32_nums) {
            
            WriteCRC32Proto::Request req;
            req.buf.assign((uint8_t*) crc32s, crc32_nums * sizeof(uint32_t));
            WriteCRC32Proto::Response resp;

            int ret = stub->call<WriteCRC32Proto>(req, resp);
            if (ret < 0) {
                LOG_ERROR("fail to call RPC");
                exit(-1);
            }
            crc32s += crc32_nums;
        }

        if (g_crc32_count > 0) {
            WriteCRC32Proto::Request req;
            req.buf.assign((uint8_t*) crc32s, g_crc32_count * sizeof(uint32_t));
            req.end_of_stream = true;
            WriteCRC32Proto::Response resp;

            int ret = stub->call<WriteCRC32Proto>(req, resp);
            if (ret < 0) {
                LOG_ERROR("fail to call RPC");
                exit(-1);
            }
        }
        
        LOG_INFO("write crc32 rpc done");
    }

    void read_matched_chunks() {
        rpc::Stub* stub = pool->get_stub(ep, false);
        if (stub == nullptr) {
            LOG_ERROR("cannot get stub");
            exit(1);
        }
        DEFER(pool->put_stub(ep, false));

        while (true) {
            WeakMatchedChunksProto::Request req;
            req.request = true;
            WeakMatchedChunksProto::Response resp;

            int ret = stub->call<WeakMatchedChunksProto>(req, resp);
            if (ret < 0) {
                LOG_ERROR("fail to call RPC");
                exit(-1);
            }

            if (resp.end_of_stream) {
                break;
            }

            // Process received matched chunks
            matched_item_rpc_1* item = (matched_item_rpc_1*)resp.matched_chunks.addr();
            client_worker.weak_matched_chunks_queue_1.push(*item);
        }
        
        client_worker.weak_matched_chunks_queue_1.setDone();
        LOG_INFO("receive matched chunks rpc done");
    }

    void write_data_cmd() {
        rpc::Stub* stub = pool->get_stub(ep, false);
        if (stub == nullptr) {
            LOG_ERROR("cannot get stub");
            exit(1);
        }
        DEFER(pool->put_stub(ep, false));

        while (!client_worker.data_cmd_queue.isDone()) {
            data_cmd cmd = client_worker.data_cmd_queue.pop();
            
            DataCmdProto::Request req;
            req.cmd = cmd.cmd;
            req.length = cmd.length;
            req.offset = cmd.offset;
            req.end_of_stream = false;

            if (cmd.cmd == CMD_LITERAL) {
                req.cmd_data.assign(cmd.data, cmd.length);
            }

            DataCmdProto::Response resp;
            int ret = stub->call<DataCmdProto>(req, resp);
            if (ret < 0) {
                LOG_ERROR("fail to call RPC");
                exit(-1);
            }

            if (cmd.cmd == CMD_LITERAL) {
                mi_free(cmd.data);
            }
        }

        // Send end_of_stream message
        DataCmdProto::Request req;
        req.end_of_stream = true;
        req.cmd = 0;
        req.length = 0;
        req.offset = 0;
        DataCmdProto::Response resp;
        
        int ret = stub->call<DataCmdProto>(req, resp);
        if (ret < 0) {
            LOG_ERROR("fail to call RPC");
            exit(-1);
        }

        LOG_INFO("send data cmd rpc done");
    }
};


int main(int argc, char** argv) {
    set_log_output_level(ALOG_INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_file_name.empty()) {
        LOG_ERROR("filename is empty");
        return -1;
    }

    int new_fd = open(FLAGS_file_name.c_str(), O_RDONLY);
    if (new_fd == -1) {
        LOG_ERROR("open file {} failed", FLAGS_file_name);
        return -1;
    }

    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_NONE))
        return -1;
    DEFER(photon::fini());

    auto pool = rpc::new_stub_pool(60 * 1000 * 1000, 10 * 1000 * 1000);
    DEFER(delete pool);
 
    net::EndPoint ep{net::IPAddr(FLAGS_ip.c_str()), (uint16_t) FLAGS_port};

    DsyncClient client{pool, ep, new_fd};
    client.execute_sync();

    photon::thread_sleep(-1);
    close(new_fd);
    return 0;
}
