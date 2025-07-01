#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <gflags/gflags.h>
#include <photon/photon.h>
#include <fcntl.h>
#include <unistd.h>

#include <photon/net/socket.h>
#include <photon/net/http/client.h>
#include <photon/common/alog.h>
#include <photon/common/alog-stdstring.h>
#include <photon/fs/localfs.h>
#include <photon/io/fd-events.h>
#include <photon/thread/thread11.h>

#include "skysync_f_worker.h"
#include "skysync_f.h"
#include "skysync.pb.h"
using namespace skysync_f;

using namespace photon;

DEFINE_string(server_ip, "127.0.0.1", "SkySyncF server IP address");
DEFINE_uint64(server_port, 19876, "SkySyncF server port");
DEFINE_string(basis_filename, "", "Basis file name on the server");
DEFINE_string(new_filename, "", "New file name on the client");
DEFINE_bool(hw, false, "Use hardware accelerated hashing (1 for yes, 0 for no)");

int perform_skysyncf_client_flow(const std::string& server_ip, uint64_t server_port,
                               const std::string& basis_filename, const std::string& new_filename, bool hw) {
    LOG_INFO("Starting SkySyncF client flow...");

    auto client = net::http::new_http_client();
    if (!client) {
        LOG_ERROR("Failed to create HTTP client");
        return -1;
    }
    DEFER(delete client);

    std::string base_url = "http://" + server_ip + ":" + std::to_string(server_port);

    uint8_t whash_algorithm = 0;
    if (hw) {
        LOG_INFO("Using hardware accelerated hashing algorithm");
        whash_algorithm = 1; // Use hardware accelerated hashing
    } else {
        LOG_INFO("Using non-hardware accelerated hashing algorithm");
    }

    ClientSkySyncFWorker client_worker(whash_algorithm);

    // Step 1: Request Signature
    LOG_INFO("Step 1: Requesting signature for ` from server...", basis_filename.c_str());
    std::string signature_url = base_url + "/signature?file=" + basis_filename + "&hw=" + std::to_string(hw);
    std::string request_key;
    {
        net::http::Client::OperationOnStack<8 * 1024> operation(client, net::http::Verb::GET, signature_url);
        auto op = &operation;

        int ret = client->call(op);
        if (ret < 0) {
            LOG_ERROR("Failed to send signature request: `", ret);
            return -1;
        }
        if (op->resp.status_code() != 200) {
            LOG_ERROR("Signature request failed with status code: `", op->resp.status_code());
            std::string error_msg;
            char error_buf[256];
            ssize_t error_read = op->resp.read(error_buf, sizeof(error_buf) - 1);
            if (error_read > 0) {
                error_buf[error_read] = '\0';
                LOG_ERROR("Server error message: `", error_buf);
            }
            return -1;
        }

        auto request_key_header = op->resp.headers["X-Request-Key"];
        if (request_key_header.empty()) {
            LOG_ERROR("No X-Request-Key header found in signature response");
            return -1;
        }
        request_key = std::string(request_key_header);
        LOG_INFO("Received request key: `", request_key.c_str());

        size_t sig_len = op->resp.headers.content_length();
        if (sig_len == 0) {
            LOG_ERROR("Received empty signature data");
            return -1;
        }
        std::vector<char> sig_buf(sig_len);
        ssize_t bytes_read = op->resp.read(sig_buf.data(), sig_len);
        if (bytes_read != (ssize_t)sig_len) {
            LOG_ERROR("Failed to read entire signature data: `", bytes_read);
            return -1;
        }

        // Deserialize the signature data using Protobuf
        FileFSC fsc_proto;
        if (!fsc_proto.ParseFromArray(sig_buf.data(), sig_len)) {
            LOG_ERROR("Failed to parse signature data from Protobuf");
            return -1;
        }

        // Convert from Protobuf to the native file_fsc structure
        client_worker.old_csums = create_file_fsc(fsc_proto.fsc_array_size());
        for (int i = 0; i < fsc_proto.fsc_array_size(); ++i) {
            const auto& one_fsc_proto = fsc_proto.fsc_array(i);
            client_worker.old_csums->fsc_array[i].offset = one_fsc_proto.offset();
            client_worker.old_csums->fsc_array[i].length = one_fsc_proto.length();
            client_worker.old_csums->fsc_array[i].weak_hash = one_fsc_proto.weak_hash();
            client_worker.old_csums->fsc_array[i].strong_hash = one_fsc_proto.strong_hash();
        }
        
        LOG_INFO("Signature received and deserialized. Length: `", sig_len);
    }
    
    // Step 1.5: Send ACK to complete the round-trip timing
    LOG_INFO("Step 1.5: Sending ACK to server for request key: `", request_key.c_str());
    {
        std::string ack_url = base_url + "/ack" + "?file=" + basis_filename;
        net::http::Client::OperationOnStack<8 * 1024> ack_operation(client, net::http::Verb::POST, ack_url);
        auto ack_op = &ack_operation;
        
        ack_op->set_body(request_key.c_str(), request_key.length());
        
        int ret = client->call(ack_op);
        if (ret < 0) {
            LOG_ERROR("Failed to send ACK request: `", ret);
            return -1;
        }
        if (ack_op->resp.status_code() != 200) {
            LOG_ERROR("ACK request failed with status code: `", ack_op->resp.status_code());
            return -1;
        }
        
        char ack_response[16];
        ssize_t ack_read = ack_op->resp.read(ack_response, sizeof(ack_response) - 1);
        if (ack_read > 0) {
            ack_response[ack_read] = '\0';
            LOG_INFO("ACK response: `", ack_response);
        }
        
        LOG_INFO("ACK sent successfully");
    }

    // Step 2: Generate Delta
    LOG_INFO("Step 2: Generating delta for `...", new_filename.c_str());
    
    int fd = open(new_filename.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG_ERROR("Failed to open new file: `", new_filename.c_str());
        return -1;
    }
    DEFER(close(fd));

    client_worker.new_csums = calc_fsc_hw(fd);
    const char* fsc_file_client = "client_fsc.bin";
    write_fsc(fsc_file_client, client_worker.new_csums);
    free_file_fsc(client_worker.new_csums);
    client_worker.new_csums = nullptr;
    client_worker.new_csums = read_fsc(fsc_file_client);
    // client_worker.chash_builder(client_worker.old_csums);
    client_worker.hash7_builder(client_worker.old_csums);
    
    auto start = std::chrono::high_resolution_clock::now();
    client_worker.rolling_fsc(fd, client_worker.old_csums, client_worker.new_csums, client_worker.data_cmd_queue);
    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
    LOG_INFO("Rolling and Delta generation completed in ` seconds", diff_1.count());

    // Step 3: Send Delta
    LOG_INFO("Step 3: Sending delta to server...");
    std::string patch_url = base_url + "/patch?file=" + basis_filename;
    {
        net::http::Client::OperationOnStack<8 * 1024> operation(client, net::http::Verb::POST, patch_url);
        auto op = &operation;
        
        // Serialize the delta using Protobuf
        Delta delta_proto;
        while (!client_worker.data_cmd_queue.empty()) {
            data_cmd cmd = client_worker.data_cmd_queue.pop();

            auto* cmd_proto = delta_proto.add_commands();
            cmd_proto->set_cmd(static_cast<DataCommand::CommandType>(cmd.cmd));
            cmd_proto->set_offset(cmd.offset);
            cmd_proto->set_length(cmd.length);
            cmd_proto->set_end_of_stream(cmd.end_of_stream);

            if (cmd.cmd == CMD_LITERAL) {
                cmd_proto->set_data(cmd.data, cmd.length);
                mi_free(cmd.data);
            }
        }

        std::string payload;
        if (!delta_proto.SerializeToString(&payload)) {
            LOG_ERROR("Failed to serialize delta to Protobuf");
            return -1;
        }

        op->req.headers.insert("Content-Type", "application/protobuf");
        op->set_body(payload.data(), payload.size());

        LOG_INFO("Delta serialized. Payload size: `", payload.size());
        
        // Start timing for Delta RTT - right before HTTP request transmission
        auto delta_rtt_start = std::chrono::high_resolution_clock::now();

        int ret = client->call(op);
        if (ret < 0) {
            LOG_ERROR("Failed to send patch request: `", ret);
            return -1;
        }
        
        if (op->resp.status_code() != 200) {
            LOG_ERROR("Patch request failed with status code: `", op->resp.status_code());
            // Try to read error message
            std::string error_msg;
            char error_buf[256];
            ssize_t error_read = op->resp.read(error_buf, sizeof(error_buf) - 1);
            if (error_read > 0) {
                error_buf[error_read] = '\0';
                LOG_ERROR("Server error message: `", error_buf);
            }
            return -1;
        }
        
        // Read response body to ensure complete response is received
        char response_buf[64];
        ssize_t resp_read = op->resp.read(response_buf, sizeof(response_buf) - 1);
        if (resp_read > 0) {
            response_buf[resp_read] = '\0';
            LOG_INFO("Server response: `", response_buf);
        }
        
        // End timing for Delta RTT - after complete HTTP response is received
        auto delta_rtt_end = std::chrono::high_resolution_clock::now();
        auto diff = std::chrono::duration<double>(delta_rtt_end - delta_rtt_start).count();
        LOG_INFO("Delta RTT (Client-side): ` seconds", diff);
        
        LOG_INFO("Delta sent. Server responded with status code: `", op->resp.status_code());
    }

    LOG_INFO("SkySyncF client flow completed successfully.");
    return 0;
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    set_log_output_level(ALOG_INFO);

    if (FLAGS_basis_filename.empty() || FLAGS_new_filename.empty()) {
        LOG_ERROR("Usage: ` --basis_filename=<file> --new_filename=<file> [--server_ip=<ip>] [--server_port=<port>] [--hw=<0|1>]", argv[0]);
        return -1;
    }

    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_NONE)) {
        LOG_ERROR("Failed to initialize photon environment");
        return -1;
    }
    DEFER(photon::fini());

#ifdef __linux__
    int ret = net::et_poller_init();
    if (ret < 0) {
        LOG_ERROR("Failed to initialize epoll poller");
        return -1;
    }
    DEFER(net::et_poller_fini());
#endif

    return perform_skysyncf_client_flow(FLAGS_server_ip, FLAGS_server_port,
                                     FLAGS_basis_filename, FLAGS_new_filename, FLAGS_hw);
}