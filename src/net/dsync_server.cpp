#include <string>
#include <unistd.h>
#include <cerrno>
#include <atomic>
#include <map>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <gflags/gflags.h>

#include <photon/photon.h>
#include <photon/io/aio-wrapper.h>
#include <photon/io/signal.h>
#include <photon/thread/thread11.h>
#include <photon/common/alog.h>
#include <photon/common/alog-stdstring.h>
#include <photon/common/alog-functionptr.h>
#include <photon/common/utility.h>
#include <photon/common/callback.h>
#include <photon/rpc/rpc.h>
#include <photon/common/checksum/crc32c.h>

#include "sync_ep.h"

using namespace photon;

DEFINE_int32(socket_type, 1, "0: tcp socket, 1: zerocopy socket, 2: iouring socket, 3: et socket");
DEFINE_string(work_dir, "", "work directory");

struct FileDescriptor {
    std::string file_name;
    int fd;
    void* buf;
};

FileDescriptor g_file = { "", -1, nullptr };
bool g_stop_test = false;

class DsyncRPCServer : public Object {
private:
    rpc::Skeleton* m_skeleton;

public:
    explicit DsyncRPCServer(IOAlloc alloc) {
        m_skeleton = rpc::new_skeleton();
        m_skeleton->set_allocator(alloc);
        m_skeleton->register_service<SyncReqProto,
                                     WriteCRC32Proto,
                                     WeakMatchedChunksProto,
                                     DataCmdProto>(this);
    }

    ~DsyncRPCServer() override {
        delete m_skeleton;
    }

    int serve(net::ISocketStream* socket) {
        int ret = m_skeleton->serve(socket);
        return ret;
    }

    int shutdown() {
        LOG_INFO("shutdown rpc server");
        return m_skeleton->shutdown();
    }

    int do_rpc_service(SyncReqProto::Request* request,
                       SyncReqProto::Response* response, IOVector* iov, IStream* stream) {
        response->success = true;
        g_file.file_name = FLAGS_work_dir + "/" + request->file_name;
        g_file.fd = open(g_file.file_name.c_str(), O_RDONLY);
        if (g_file.fd == -1) {
            LOG_ERROR("open file {} failed", g_file.file_name);
            return -1;
        }
        return 0;
    }

    int do_rpc_service(WriteCRC32Proto::Request* request,
                       WriteCRC32Proto::Response* response, IOVector* iov, IStream* stream) {
        response->code = 0;

        // Process the CRC32 checksums from the request buffer
        uint32_t* crc32s = (uint32_t*) request->buf.addr();
        size_t crc32_count = request->buf.size() / sizeof(uint32_t);

        // Push the checksums into server_worker's queue
        for (size_t i = 0; i < crc32_count; i++) {
            server_worker.new_crc32_queue.push(crc32s[i]);
        }

        // If this is the end of stream, start the CDC processing
        if (request->end_of_stream) {
            server_worker.new_crc32_queue.setDone();

            // Start a thread for serial_cdc and chash_builder
            std::thread process_thread([]() {
                // Perform CDC on the old file
                server_worker.serial_cdc(g_file.fd, server_worker.old_csums_queue);
                
                // Build the hash table from the checksums
                server_worker.chash_builder(server_worker.old_csums_queue);
                
                // Compare checksums and build the matching chunks
                server_worker.compare_weak_chash(g_file.fd, server_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
            });
            process_thread.detach();
        }

        return 0;
    }

    int do_rpc_service(WeakMatchedChunksProto::Request* request,
                      WeakMatchedChunksProto::Response* response, 
                      IOVector* iov, IStream* stream) {
        if (!server_worker.weak_matched_chunks_queue_1.isDone()) {
            matched_item_rpc_1 item = server_worker.weak_matched_chunks_queue_1.pop();
            response->matched_chunks.assign((uint8_t*)&item, sizeof(matched_item_rpc_1));
            response->end_of_stream = false;
        } else {
            response->end_of_stream = true;
        }
        return 0;
    }

    int do_rpc_service(DataCmdProto::Request* request,
                   DataCmdProto::Response* response, IOVector* iov, IStream* stream) {
        response->code = 0;

        data_cmd cmd;
        cmd.cmd = request->cmd;
        cmd.length = request->length;
        cmd.offset = request->offset;

        if (cmd.cmd == CMD_LITERAL) {
            // For literal data, copy the data from request
            cmd.data = (uint8_t*)mi_malloc(cmd.length);
            memcpy(cmd.data, request->cmd_data.addr(), cmd.length);
        }

        server_worker.data_cmd_queue.push(cmd);

        // Start patch_delta when we receive end_of_stream
        if (request->end_of_stream) {
            server_worker.data_cmd_queue.setDone();
            
            // Open files and start patch_delta in a new thread
            std::thread patch_thread([]() {

                // Create output file name by adding .patch extension
                std::string output_file = g_file.file_name + ".patch";
                int output_fd = open(output_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (output_fd == -1) {
                    LOG_ERROR("Failed to create output file: ", output_file);
                    return;
                }

                server_worker.patch_delta(g_file.fd, output_fd, server_worker.data_cmd_queue);

                close(output_fd);
                close(g_file.fd);
            });
            patch_thread.detach();
        }

        return 0;
    }
};

void handle_signal(int) {
    LOG_INFO("try to stop test");
    g_stop_test = true;
}

int main(int argc, char** argv) {
    set_log_output_level(ALOG_INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // check work directory
    if (FLAGS_work_dir.empty()) {
        LOG_ERROR("work directory is empty");
        return -1;
    }

    if (photon::init(INIT_EVENT_EPOLL | INIT_EVENT_SIGNAL,
                     INIT_IO_LIBAIO | INIT_IO_SOCKET_EDGE_TRIGGER) < 0)
        return -1;
    DEFER(photon::fini());

    auto pooled_allocator = new PooledAllocator<>;
    DEFER(delete pooled_allocator);

    photon::sync_signal(SIGTERM, &handle_signal);
    photon::sync_signal(SIGINT, &handle_signal);

    net::ISocketServer* socket_srv = nullptr;
    socket_srv = net::new_zerocopy_tcp_server();
    LOG_INFO("New zerocopy socket server");

    DEFER(delete socket_srv);

    auto rpc_server = new DsyncRPCServer(pooled_allocator->get_io_alloc());
    DEFER(delete rpc_server);

    socket_srv->set_handler({rpc_server, &DsyncRPCServer::serve});
    socket_srv->bind((uint16_t) FLAGS_port, net::IPAddr("0.0.0.0"));
    socket_srv->listen(1024);

    auto stop_watcher = [&] {
        while (!g_stop_test) {
            photon::thread_sleep(1);
        }
        rpc_server->shutdown();
        socket_srv->terminate();
    };
    photon::thread_create11(stop_watcher);
    LOG_INFO("Socket server running ...");

    socket_srv->start_loop(true);
    LOG_INFO("Out of sleep");
}