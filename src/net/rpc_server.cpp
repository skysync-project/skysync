#include "rpc.h"

DEFINE_bool(send_attachment, true, "Carry attachment along with response");
DEFINE_int32(port, 8001, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");

class CsumStreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        // Write the received data to server_worker.new_csums_queue
        for (size_t i = 0; i < size; ++i) {
            one_cdc cdc;
            messages[i]->copy_to(&cdc, sizeof(one_cdc));
            server_worker.new_csums_queue.push(cdc);
        }
        server_worker.new_csums_queue.setDone();
        return 0;
    }
    virtual void on_idle_timeout(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
    }
    virtual void on_closed(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " is closed";
    }
};

void send_weak_matched_chunks_thread(brpc::StreamId id) {
    while (!server_worker.weak_matched_chunks_queue.isDone()) {
        matched_item_rpc item = server_worker.weak_matched_chunks_queue.pop();
        butil::IOBuf msg;
        msg.append(&item, sizeof(matched_item_rpc));
        CHECK_EQ(0, brpc::StreamWrite(id, msg));
    }
    // Close the stream after sending all chunks
    LOG(INFO) << "Closing stream=" << id << " after sending all chunks";
    brpc::StreamClose(id);
}

class StreamingEchoService : public sync_rpc::EchoService {
public:
    StreamingEchoService() : _sd(brpc::INVALID_STREAM_ID) {}
    virtual ~StreamingEchoService() {
        // brpc::StreamClose(_sd);
    };
    virtual void Echo(google::protobuf::RpcController* controller,
                      const sync_rpc::SyncRequest* request,
                      sync_rpc::SyncResponse* response,
                      google::protobuf::Closure* done) {
        // This object helps you to call done->Run() in RAII style. If you need
        // to process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
        brpc::StreamOptions stream_options;
        stream_options.handler = &_receiver;
        if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            return;
        }
        
        // Set filename according to the request
        filename = request->message();
        response->set_message("Accepted stream");
        done_guard.reset(NULL);
        // done_guard.release();
        LOG(INFO) << "Sync request: " << filename;
        
        // Start a new thread to run server_worker.serial_cdc
        std::thread cdc_thread([]() {
            int fd = open(filename.c_str(), O_RDONLY);
            if (fd == -1) {
                LOG(ERROR) << "Failed to open file: " << filename;
                return;
            }
            server_worker.serial_cdc(fd, server_worker.old_csums_queue);
            close(fd);
        });
        cdc_thread.detach();

        std::thread ibuilder_thread([]() {
            server_worker.uthash_builder(server_worker.old_csums_queue);
        });
        ibuilder_thread.join();

        std::thread compare_weak_hash_thread([]() {
            int fd = open(filename.c_str(), O_RDONLY);
            if (fd == -1) {
                LOG(ERROR) << "Failed to open file: " << filename;
                return;
            }
            server_worker.compare_weak_uthash(fd, server_worker.new_csums_queue, server_worker.weak_matched_chunks_queue);
            close(fd);
        });
        compare_weak_hash_thread.detach();

        std::thread send_thread(send_weak_matched_chunks_thread, _sd);
        send_thread.detach();
    }

private:
    CsumStreamReceiver _receiver;
    brpc::StreamId _sd;
};

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Generally you only need one Server.
    brpc::Server server;

    // Instance of your service.
    StreamingEchoService echo_service_impl;

    // Add the service into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&echo_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Start the server. 
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}