#include "rpc.h"

DEFINE_bool(send_attachment, true, "Carry attachment along with response");
DEFINE_int32(port, 8001, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_string(ssl_key_file, "", "SSL key file path");
DEFINE_string(ssl_cert_file, "", "SSL cert file path");

void Usage(const char* program) {
    fprintf(stderr, "Usage: %s\n"
                    "       -ssl_key_file=<string>\n"
                    "       -ssl_cert_file=<string>\n", program);
}

class CsumStreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        // Write the received data to server_worker.new_csums_queue
        for (size_t i = 0; i < size; ++i) {
            one_cdc cdc;
            messages[i]->copy_to(&cdc, sizeof(one_cdc));
            // Check if the received data is the end of the stream
            // LOG(INFO) << "Stream=" << id 
            // << " received message: " << cdc.offset << " " << cdc.length << " " << cdc.weak_hash;
            if (cdc.length == 0 && cdc.offset == 0 && cdc.weak_hash == 0) {
                server_worker.new_csums_queue.setDone();
                break;
            }
            server_worker.new_csums_queue.push(cdc);
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

void send_weak_matched_chunks_thread(brpc::StreamId &id) {
    while (!server_worker.weak_matched_chunks_queue.isDone()) {
        matched_item_rpc item = server_worker.weak_matched_chunks_queue.pop();
        butil::IOBuf msg;
        msg.append(&item, sizeof(matched_item_rpc));
        CHECK_EQ(0, brpc::StreamWrite(id, msg));
    }
    // Send a message to the client to indicate the end of the stream
    butil::IOBuf msg;
    matched_item_rpc item = {0, {0, 0}, {0, 0}, {0}, true};
    msg.append(&item, sizeof(matched_item_rpc));
    CHECK_EQ(0, brpc::StreamWrite(id, msg));
    LOG(INFO) << "Weak Matched Chunks Sent";
    // CHECK_EQ(0, brpc::StreamClose(id));
}

void sync_process(std::string &filename, brpc::StreamId &_sd) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) {
        LOG(ERROR) << "Failed to open file: " << filename;
        return;
    }
    
    server_worker.serial_cdc(fd, server_worker.old_csums_queue);
    LOG(INFO) << "Serial CDC Done";

    server_worker.uthash_builder(server_worker.old_csums_queue);
    LOG(INFO) << "Uthash Builder Done";

    server_worker.compare_weak_uthash(fd, server_worker.new_csums_queue, server_worker.weak_matched_chunks_queue);
    LOG(INFO) << "Weak Checksums Comparison Done";

    close(fd);
    send_weak_matched_chunks_thread(_sd);
}

class StreamingEchoService : public sync_rpc::EchoService {
public:
    StreamingEchoService() : _sd(brpc::INVALID_STREAM_ID) {}
    virtual ~StreamingEchoService() {
        brpc::StreamClose(_sd);
    };
    virtual void Echo(google::protobuf::RpcController* controller,
                      const sync_rpc::SyncRequest* request,
                      sync_rpc::SyncResponse* response,
                      google::protobuf::Closure* done) {
        // This object helps you to call done->Run() in RAII style. If you need
        // to process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        brpc::StreamOptions stream_options;
        stream_options.handler = &_receiver;
        if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            return;
        }
        
        // Set filename according to the request
        filename = request->message();
        // Set response message
        response->set_message("Accepted stream");
        done_guard.reset(NULL);
        LOG(INFO) << "Sync request: " << filename;
        // Start a new thread to run sync_process
        std::thread sync_thread(sync_process, std::ref(filename), std::ref(_sd));
        sync_thread.detach();
    }

private:
    CsumStreamReceiver _receiver;
    brpc::StreamId _sd;
};

void patch_delta_process() {
    int old_fd = open(filename.c_str(), O_RDONLY);
    if (old_fd == -1) {
        LOG(ERROR) << "Failed to open file: " << filename;
        return;
    }
    output_filename = filename + ".patched";
    int out_fd = open(output_filename.c_str(), O_WRONLY | O_CREAT, 0666);
    server_worker.patch_delta(old_fd, out_fd, server_worker.data_cmd_queue);
    close(old_fd);
    close(out_fd);
    LOG(INFO) << "Patch Delta Done";
}

class DataCommandEchoService : public sync_rpc::DataCommandService {
public:
    DataCommandEchoService() : patch_delta_called(false) {}
    virtual ~DataCommandEchoService() {}

    virtual void Echo(google::protobuf::RpcController* controller,
                      const sync_rpc::DataCommandRequest* request,
                      sync_rpc::DataCommandResponse* response,
                      google::protobuf::Closure* done) {

        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        
        // Set response message
        response->set_message("Accepted Data Command");
        done_guard.reset(NULL);

        // Call patch_delta only on the first message
        if (!patch_delta_called) {
            std::thread patch_delta_thread(patch_delta_process);
            patch_delta_thread.detach();
            patch_delta_called = true;
        }

        data_cmd cmd;
        cmd.end_of_stream = request->islast();
        if (cmd.end_of_stream) {
            server_worker.data_cmd_queue.setDone();
            patch_delta_called = false;
            return;
        }
        cmd.cmd = request->cmd();
        cmd.length = request->length();
        cmd.offset = request->offset();
        if (cmd.cmd == CMD_LITERAL) {
            cmd.data = (uint8_t *)mi_malloc(cmd.length);
            assert(cmd.data != NULL);
            request->data().copy(reinterpret_cast<char*>(cmd.data), cmd.length);
        }
        server_worker.data_cmd_queue.push(cmd);
    }

private:
    bool patch_delta_called;
};

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_ssl_key_file.empty() && FLAGS_ssl_cert_file.empty()) {
        Usage(argv[0]);
        return -1;
    }

    // Generally you only need one Server.
    brpc::Server server;

    // Instance of your service.
    StreamingEchoService echo_service_impl;
    DataCommandEchoService data_command_service_impl;

    // Add the service into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&echo_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    if (server.AddService(&data_command_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Start the server. 
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    options.mutable_ssl_options()->default_cert.certificate = FLAGS_ssl_cert_file;
    options.mutable_ssl_options()->default_cert.private_key = FLAGS_ssl_key_file;
    options.mutable_ssl_options()->ciphers = "";

    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}