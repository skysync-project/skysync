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

#include "rsync_http.h"

using namespace photon;

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

DEFINE_string(server_ip, "127.0.0.1", "Rsync server IP address");
DEFINE_uint64(server_port, 19876, "Rsync server port");
DEFINE_string(basis_filename, "", "Basis file name on the server");
DEFINE_string(new_filename, "", "New file name on the client");
DEFINE_bool(hw, false, "Use hardware accelerated hashing (1 for yes, 0 for no)");

int perform_rsync_client_flow(const std::string& server_ip, uint64_t server_port,
                              const std::string& basis_filename, const std::string& new_filename , bool hw) {
    LOG_INFO("Starting rsync client flow...");

    auto client = net::http::new_http_client();
    if (!client) {
        LOG_ERROR("Failed to create HTTP client");
        return -1;
    }
    DEFER(delete client);

    std::string base_url = "http://" + server_ip + ":" + std::to_string(server_port);

    if (hw) {
        LOG_INFO("Using hardware accelerated hashing algorithm");
    } else {
        LOG_INFO("Using non-hardware accelerated hashing algorithm");
    }

    // Step 1: Request Signature
    LOG_INFO("Step 1: Requesting signature for ` from server...", basis_filename.c_str());
    std::string signature_url = base_url + "/signature?file=" + basis_filename + "&hw=" + std::to_string(hw);

    char* sig_data = nullptr;
    size_t sig_len = 0;
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

        // Extract request key from response headers
        auto request_key_header = op->resp.headers["X-Request-Key"];
        if (request_key_header.empty()) {
            LOG_ERROR("No X-Request-Key header found in signature response");
            return -1;
        }
        request_key = std::string(request_key_header);
        LOG_INFO("Received request key: `", request_key.c_str());

        sig_len = op->resp.headers.content_length();
        if (sig_len == 0) {
            LOG_ERROR("Received empty signature data");
            return -1;
        }
        sig_data = (char*)malloc(sig_len);
        if (!sig_data) {
            LOG_ERROR("Failed to allocate memory for signature data");
            return -1;
        }
        ssize_t bytes_read = op->resp.read(sig_data, sig_len);
        if (bytes_read != (ssize_t)sig_len) {
            LOG_ERROR("Failed to read entire signature data: `", bytes_read);
            free(sig_data);
            return -1;
        }
        
        LOG_INFO("Signature received. Length: `", sig_len);
    }
    
    // Step 1.5: Send ACK to complete the round-trip timing
    LOG_INFO("Step 1.5: Sending ACK to server for request key: `", request_key.c_str());
    {
        std::string ack_url = base_url + "/ack" + "?file=" + basis_filename;
        net::http::Client::OperationOnStack<8 * 1024> ack_operation(client, net::http::Verb::POST, ack_url);
        auto ack_op = &ack_operation;
        
        // Send the request key in the POST body
        ack_op->set_body(request_key.c_str(), request_key.length());
        
        int ret = client->call(ack_op);
        if (ret < 0) {
            LOG_ERROR("Failed to send ACK request: `", ret);
            free(sig_data);
            return -1;
        }
        if (ack_op->resp.status_code() != 200) {
            LOG_ERROR("ACK request failed with status code: `", ack_op->resp.status_code());
            free(sig_data);
            return -1;
        }
        
        // Read ACK response
        char ack_response[16];
        ssize_t ack_read = ack_op->resp.read(ack_response, sizeof(ack_response) - 1);
        if (ack_read > 0) {
            ack_response[ack_read] = '\0';
            LOG_INFO("ACK response: `", ack_response);
        }
        
        LOG_INFO("ACK sent successfully");
    }
    DEFER(free(sig_data));

    // Step 2: Generate Delta
    LOG_INFO("Step 2: Generating delta for `...", new_filename.c_str());
    
    std::string sig_name = basis_filename + ".sig";
    int fd = open(sig_name.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        LOG_INFO("Failed to open signature file `: `", sig_name.c_str(), strerror(errno));
        return -1;
    }
    DEFER(close(fd));

    std::string delta_file = basis_filename + ".delta";
    // Delete any existing delta file
    auto fs = fs::new_localfs_adaptor();
    if (!fs) {
        LOG_ERROR("Failed to create local filesystem adaptor");
        return 0;
    }
    DEFER(delete fs);
    if (fs->access(delta_file.c_str(), F_OK) == 0) {
        fs->unlink(delta_file.c_str());
    }

    auto start = std::chrono::high_resolution_clock::now();
    // rs_result rsync_ret = rsyncx_delta_mem(sig_data, sig_len, new_file_content, new_file_len, &delta_data, &delta_len);
    rs_result rsync_ret = rsyncx_delta(sig_name.c_str(), new_filename.c_str(), delta_file.c_str());
    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
    LOG_INFO("Rolling and Delta generation completed in ` seconds", diff_1.count());

    int delta_fd = open(delta_file.c_str(), O_RDONLY);
    if (delta_fd < 0) {
        LOG_ERROR("Failed to open delta file `: `", delta_file.c_str(), strerror(errno));
        return -1;
    }
    DEFER(close(delta_fd));

    off_t delta_len = file_size(delta_fd);
    if (delta_len <= 0) {
        LOG_ERROR("Delta file is empty or has invalid size: `", delta_len);
        return -1;
    }
    char* delta_data = (char*)malloc(delta_len);
    if (!delta_data) {
        LOG_ERROR("Failed to allocate memory for delta data");
        return -1;
    }
    DEFER(free(delta_data));

    while (true) {
        ssize_t bytes_read = read(delta_fd, delta_data, delta_len);
        if (bytes_read < 0) {
            LOG_ERROR("Failed to read delta file: `", strerror(errno));
            return -1;
        }
        if (bytes_read == 0) {
            break; // End of file
        }
    }
    LOG_INFO("Delta generated. Length: `", delta_len);
    
    // Step 3: Send Delta
    LOG_INFO("Step 3: Sending delta to server...");
    std::string patch_url = base_url + "/patch?file=" + basis_filename;
    {
        net::http::Client::OperationOnStack<8 * 1024> operation(client, net::http::Verb::POST, patch_url);
        auto op = &operation;
        
        LOG_INFO("Setting request body using set_body(). Delta length: `", delta_len);
        op->set_body(delta_data, delta_len);

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

    LOG_INFO("Rsync client flow completed successfully.");
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

    return perform_rsync_client_flow(FLAGS_server_ip, FLAGS_server_port,
                                     FLAGS_basis_filename, FLAGS_new_filename, FLAGS_hw);
}