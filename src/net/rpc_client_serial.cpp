#include "rpc.h"

DEFINE_bool(send_attachment, true, "Carry attachment along with requests");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8001", "IP Address of server");
DEFINE_int32(timeout_ms, 100000, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_string(filename, "", "File to be syncronized");

// print usage
void Usage(const char* program) {
    fprintf(stderr, "Usage: %s\n"
                    // "    -connection_type=string\n"
                    "       -server=<ip:port>, default: 0.0.0.0:8001\n"
                    // "    -timeout_ms=int\n"
                    // "    -max_retry=int\n"
                    "       -filename=<string>\n", program);
}

class WeakMatchedChunksReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        for (size_t i = 0; i < size; ++i) {
            matched_item_rpc item;
            messages[i]->copy_to(&item, sizeof(matched_item_rpc));
            // LOG(INFO) << "Stream=" << id << " received message: " << item.item_nums << " " << item.new_ol.offset << " " << item.new_ol.length << " " << item.end_of_stream;
            // Check if the received data is the end of the stream
            if (item.end_of_stream) {
                client_worker.weak_matched_chunks_queue.setDone();
                break;
            }
            client_worker.weak_matched_chunks_queue.push(item);
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

void delta_generation_process(int &new_fd, brpc::StreamId &_sd) {
    // Compare weak checksums
    client_worker.compare_sha1(new_fd, client_worker.weak_matched_chunks_queue, client_worker.strong_matched_chunks_queue);
    
    LOG(INFO) << "Strong Checksums Comparison Done";

    // Generate delta
    client_worker.generate_delta(new_fd, client_worker.strong_matched_chunks_queue, client_worker.data_cmd_queue);

    LOG(INFO) << "Delta Generation Done";
}

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // check if filename is provided
    if (FLAGS_filename.empty()) {
        Usage(argv[0]);
        return -1;
    }

    int new_fd = open(FLAGS_filename.c_str(), O_RDONLY);
    if (new_fd == -1) {
        LOG(ERROR) << "open file " << FLAGS_filename << " failed";
        return -1;
    }

    // ClientSyncWorker client_worker;

    // A Channel represents a communication line to a Server. Notice that 
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;
        
    // Initialize the channel, NULL means using default options. 
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.connection_type = "single";
    options.timeout_ms = FLAGS_timeout_ms /*milliseconds*/;
    options.max_retry = FLAGS_max_retry;
    options.mutable_ssl_options()->ciphers = "";
    
    if (channel.Init(FLAGS_server.c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    sync_rpc::EchoService_Stub stub(&channel);
    brpc::Controller cntl;
    brpc::StreamId stream;

    brpc::StreamOptions stream_options;
    WeakMatchedChunksReceiver receiver;
    stream_options.handler = &receiver;

    if (brpc::StreamCreate(&stream, cntl, &stream_options) != 0) {
        LOG(ERROR) << "Fail to create stream";
        return -1;
    }
    LOG(INFO) << "Created Stream=" << stream;

    sync_rpc::SyncRequest request;
    request.set_message(FLAGS_filename);
    sync_rpc::SyncResponse response;
    stub.Echo(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
        return -1;
    }
    else {
        LOG(INFO) << "Echo response: " << response.message();
    }

    // Start a thread to execute the client_worker.serial_cdc function
    std::thread cdc_thread([&new_fd]() {
        client_worker.serial_cdc(new_fd, client_worker.new_csums_queue);
    });
    cdc_thread.detach();

    // Wait for client_worker.new_csums_queue is pushed with data
    while (!client_worker.new_csums_queue.isDone()) {
        one_cdc cdc = client_worker.new_csums_queue.pop();
        butil::IOBuf msg;
        msg.append(&cdc, sizeof(one_cdc));
        CHECK_EQ(0, brpc::StreamWrite(stream, msg));
    }
    // Send a message to the server to indicate the end of the stream
    butil::IOBuf msg;
    one_cdc cdc = {0, 0, 0};
    msg.append(&cdc, sizeof(one_cdc));
    CHECK_EQ(0, brpc::StreamWrite(stream, msg));

    // delta_generation_process(new_fd, stream);
    std::thread delta_thread([&new_fd, &stream]() {
        delta_generation_process(new_fd, stream);
    });
    delta_thread.join();

    sync_rpc::DataCommandService_Stub data_cmd_stub(&channel);
    while(!client_worker.data_cmd_queue.isDone()) {
        data_cmd cmd = client_worker.data_cmd_queue.pop();
        brpc::Controller data_cmd_cntl;
        sync_rpc::DataCommandRequest data_cmd_request;
        sync_rpc::DataCommandResponse data_cmd_response;

        data_cmd_request.set_cmd(cmd.cmd);
        data_cmd_request.set_length(cmd.length);
        data_cmd_request.set_offset(cmd.offset);
        data_cmd_request.set_islast(0);
        if (cmd.cmd == CMD_LITERAL) {
            data_cmd_request.set_data(cmd.data, cmd.length);
        }

        data_cmd_stub.Echo(&data_cmd_cntl, &data_cmd_request, &data_cmd_response, NULL);
        if (data_cmd_cntl.Failed()) {
            LOG(ERROR) << "Fail to connect stream, " << data_cmd_cntl.ErrorText();
            return -1;
        }
        else {
            LOG(INFO) << "DataCommand response: " << data_cmd_response.message();
        }
    }

    brpc::Controller data_cmd_cntl;
    sync_rpc::DataCommandRequest data_cmd_request;
    sync_rpc::DataCommandResponse data_cmd_response;
    data_cmd_request.set_islast(1);
    data_cmd_request.set_cmd(10);
    data_cmd_request.set_length(0);
    data_cmd_request.set_offset(0);
    data_cmd_stub.Echo(&data_cmd_cntl, &data_cmd_request, &data_cmd_response, NULL);
    if (data_cmd_cntl.Failed()) {
        LOG(ERROR) << "Fail to connect stream, " << data_cmd_cntl.ErrorText();
        return -1;
    }
    else {
        LOG(INFO) << "DataCommand response: " << data_cmd_response.message();
    }

    // Close the stream on the client side
    // LOG(INFO) << "Closing stream=" << stream;
    // CHECK_EQ(0, brpc::StreamClose(stream));
    LOG(INFO) << "EchoClient is going to quit";
    return 0;
}