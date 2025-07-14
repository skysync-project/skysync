#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <gflags/gflags.h>
#include <photon/photon.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <photon/net/socket.h>
#include <photon/net/http/client.h>
#include <photon/common/alog.h>
#include <photon/common/alog-stdstring.h>
#include <photon/fs/localfs.h>
#include <photon/io/fd-events.h>
#include <photon/thread/thread11.h>
#include "dsync.pb.h"

#include "dsync.h"
#include "dsync_http.h"

using namespace photon;

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

DEFINE_string(server_ip, "127.0.0.1", "Dsync server IP address");
DEFINE_uint64(server_port, 19876, "Dsync server port");
DEFINE_string(basis_filename, "", "Basis file name on the server (old file)");
DEFINE_string(new_filename, "", "New file name on the client");
DEFINE_bool(hw, false, "Use hardware accelerated hashing (1 for yes, 0 for no)");

int perform_dsync_client_flow(const std::string& server_ip, uint64_t server_port,
                              const std::string& basis_filename, const std::string& new_filename,
                              bool hw) {
    LOG_INFO("Starting dsync client flow...");

    auto client = net::http::new_http_client();
    if (!client) {
        LOG_ERROR("Failed to create HTTP client");
        return -1;
    }
    DEFER(delete client);

    std::string base_url = "http://" + server_ip + ":" + std::to_string(server_port);

    StrongHashingAlgorithm shash_algorithm = StrongHashingAlgorithm::SHA1;
    uint8_t whash_algorithm = 0;
    if (hw) {
        LOG_INFO("Using hardware accelerated hashing algorithm");
        shash_algorithm = StrongHashingAlgorithm::SHA256;
        whash_algorithm = 1;
    } else {
        LOG_INFO("Using non-hardware accelerated hashing algorithm");
    }
    ClientSyncWorker client_worker(shash_algorithm, whash_algorithm);

    // Step 1: Generate new_crc32_queue and send to server
    LOG_INFO("Step 1: Generating new_crc32_queue for `...", new_filename.c_str());
    int new_fd = open(new_filename.c_str(), O_RDONLY);
    if (new_fd < 0) {
        LOG_ERROR("Failed to open new file: `", new_filename.c_str());
        return -1;
    }
    DEFER(close(new_fd));

    auto start = std::chrono::high_resolution_clock::now();
    client_worker.serial_cdc(new_fd, client_worker.new_csums_queue);
    size_t new_file_s = file_size(new_fd);
    // client_worker.lhash_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue, new_file_s / 8192 / 2);
    client_worker.chash_builder(client_worker.new_csums_queue, client_worker.new_crc32_queue);
    std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
    LOG_INFO("Serial CDC completed in ` seconds", diff_1.count());

    dsync::Uint32Queue csums_queue_pb = serialize_uint32_queue_to_protobuf(client_worker.new_crc32_queue);
    std::string csums_queue_data;
    if (!csums_queue_pb.SerializeToString(&csums_queue_data)) {
        LOG_ERROR("Failed to serialize csums_queue to protobuf");
        return -1;
    }

    LOG_INFO("Sending new_crc32_queue to server...");
    std::string csums_url = base_url + "/csums_queue?basis_filename=" + basis_filename + "&hw=" + std::to_string(hw);
    std::string request_key;
    
    // Start timing for csums queue RTT
    auto csums_start_time = std::chrono::high_resolution_clock::now();
    {
        net::http::Client::OperationOnStack<8 * 1024> operation(client, net::http::Verb::POST, csums_url);
        auto op = &operation;
        
        op->req.headers.insert("Content-Type", "application/x-protobuf");
        op->set_body(csums_queue_data.c_str(), csums_queue_data.size());

        int ret = client->call(op);
        if (ret < 0) {
            LOG_ERROR("Failed to send csums_queue request: `", ret);
            return -1;
        }
        if (op->resp.status_code() != 200) {
            LOG_ERROR("csums_queue request failed with status code: `", op->resp.status_code());
            std::string error_msg;
            char error_buf[256];
            ssize_t error_read = op->resp.read(error_buf, sizeof(error_buf) - 1);
            if (error_read > 0) {
                error_buf[error_read] = '\0';
                LOG_ERROR("Server error message: `", error_buf);
            }
            return -1;
        }

        // Read protobuf response
        size_t response_len = op->resp.headers.content_length();
        std::string response_data(response_len, '\0');
        ssize_t bytes_read = op->resp.read(response_data.data(), response_len);
        if (bytes_read != (ssize_t)response_len) {
            LOG_ERROR("Failed to read entire response data: `", bytes_read);
            return -1;
        }
            
        // Try to parse as CsumsQueueResponse first
        dsync::CsumsQueueResponse response_pb;
        if (response_pb.ParseFromString(response_data)) {
            // Check for error response
            if (!response_pb.success()) {
                LOG_ERROR("Server returned error: `", response_pb.error_message().c_str());
                return -1;
            }
                
            request_key = response_pb.request_key();
                
            // Extract and deserialize weak matched data directly
            deserialize_matched_item_rpc_1_queue_from_protobuf(response_pb.weak_matched_data(), client_worker.weak_matched_chunks_queue_1);
                
            LOG_INFO("Received protobuf response with weak matched data. Request key: `", request_key.c_str());
        } else {
            // Try to parse as ErrorResponse
            dsync::ErrorResponse error_pb;
            if (error_pb.ParseFromString(response_data)) {
                LOG_ERROR("Server returned error (code `): `", error_pb.error_code(), error_pb.error_message().c_str());
                return -1;
            } else {
                LOG_ERROR("Failed to parse protobuf response");
                return -1;
            }
        }
    }
    
    // End timing for csums queue RTT
    auto csums_end_time = std::chrono::high_resolution_clock::now();
    auto csums_rtt = std::chrono::duration<double>(csums_end_time - csums_start_time).count();
    // LOG_INFO("Csums Queue RTT (Client-side): ` seconds for request key: `", csums_rtt, request_key.c_str());

    // Step 1.5: Send ACK for RTT measurement (weak matched chunks already received inline)
    LOG_INFO("Step 1.5: Sending ACK to server for request key: `", request_key.c_str());
    {
        std::string ack_url = base_url + "/ack?basis_filename=" + basis_filename;
        net::http::Client::OperationOnStack<8 * 1024> ack_operation(client, net::http::Verb::POST, ack_url);
        auto ack_op = &ack_operation;
        
        // Send the request key in the POST body
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
        
        // Read ACK response
        char ack_response[16];
        ssize_t ack_read = ack_op->resp.read(ack_response, sizeof(ack_response) - 1);
        if (ack_read > 0) {
            ack_response[ack_read] = '\0';
            LOG_INFO("ACK response: `", ack_response);
        }
        
        LOG_INFO("ACK sent successfully - weak matched chunks already received inline");
    }

    // Step 2: Generate data_cmd_queue and send to server
    LOG_INFO("Step 2: Generating delta...");

    start = std::chrono::high_resolution_clock::now();
    // client_worker.compare_sha1_lhash(new_fd, client_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);
    client_worker.compare_sha1_chash(new_fd, client_worker.weak_matched_chunks_queue_1, client_worker.strong_matched_chunks_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    LOG_INFO("Strong hash calculation completed in ` seconds", diff_1.count());

    start = std::chrono::high_resolution_clock::now();
    client_worker.generate_delta(new_fd, client_worker.strong_matched_chunks_queue, client_worker.data_cmd_queue);
    diff_1 = std::chrono::high_resolution_clock::now() - start;
    LOG_INFO("Delta generation completed in ` seconds", diff_1.count());

    // Create a temporary file to store the data_cmd_queue
    char tmp_file[] = "/tmp/dsync-delta";
    int tmp_fd = mkstemp(tmp_file);
    if (tmp_fd < 0) {
        LOG_ERROR("Failed to create temporary file, error: `(`)", errno, strerror(errno));
        return -1;
    }
    
    // Write data_cmd_queue to the temporary file directly
    while (!client_worker.data_cmd_queue.empty()) {
        data_cmd cmd = client_worker.data_cmd_queue.pop();
        if (cmd.cmd == CMD_COPY) {
            ssize_t written = write(tmp_fd, &cmd, sizeof(cmd));
            
        } else if (cmd.cmd == CMD_LITERAL) {
            ssize_t written = write(tmp_fd, &cmd, sizeof(cmd));
        }
    }

    // Get file size
    struct stat st;
    if (fstat(tmp_fd, &st) < 0) {
        LOG_ERROR("Failed to fstat temporary file, error: `(`)", errno, strerror(errno));
        close(tmp_fd);
        unlink(tmp_file);
        return -1;
    }
    close(tmp_fd); // Done with fd, will use path from now on
    DEFER(unlink(tmp_file)); // Ensure cleanup on scope exit

    off_t file_size = st.st_size;

    // Step 3: Send delta using streaming
    LOG_INFO("Step 3: Sending delta to server...");
    std::string patch_url = base_url + "/patch?basis_filename=" + basis_filename;
    {
        photon::fs::IFileSystem* fs = photon::fs::new_localfs_adaptor();
        if (!fs) {
            LOG_ERROR("Failed to create new_localfs_adaptor");
            return -1;
        }
        DEFER(delete fs);

        photon::fs::IFile* file_stream = fs->open(tmp_file, O_RDONLY);
        if (!file_stream) {
            LOG_ERROR("Failed to open temporary file '`' for streaming", tmp_file);
            return -1;
        }
        DEFER(delete file_stream);

        net::http::Client::OperationOnStack<8 * 1024> operation(client, net::http::Verb::POST, patch_url);
        auto op = &operation;
        
        op->req.headers.insert("Content-Type", "application/octet-stream");
        op->req.headers.content_length(file_size);
        op->body_stream = file_stream;

        auto delta_rtt_start = std::chrono::high_resolution_clock::now();

        int ret = client->call(op);
        if (ret < 0) {
            LOG_ERROR("Failed to send delta request: `", ret);
            return -1;
        }
        
        if (op->resp.status_code() != 200) {
            LOG_ERROR("Patch request failed with status code: `", op->resp.status_code());
            
            // Try to read error response as protobuf
            size_t error_len = op->resp.headers.content_length();
            if (error_len > 0) {
                std::string error_data(error_len, '\0');
                ssize_t error_read = op->resp.read(error_data.data(), error_len);
                if (error_read > 0) {
                    dsync::ErrorResponse error_pb;
                    if (error_pb.ParseFromString(error_data)) {
                        LOG_ERROR("Server error (code `): `", error_pb.error_code(), error_pb.error_message().c_str());
                    } else {
                        LOG_ERROR("Failed to parse error response");
                    }
                }
            }
            return -1;
        }
        
        // Read success response
        size_t resp_len = op->resp.headers.content_length();
        if (resp_len > 0) {
            std::string response_data(resp_len, '\0');
            ssize_t resp_read = op->resp.read(response_data.data(), resp_len);
            if (resp_read > 0) {
                LOG_INFO("Server response received (` bytes)", resp_read);
            }
        }

        // End timing for Delta RTT - after complete HTTP response is received
        auto delta_rtt_end = std::chrono::high_resolution_clock::now();
        auto diff = std::chrono::duration<double>(delta_rtt_end - delta_rtt_start).count();
        LOG_INFO("Delta RTT (Client-side): ` seconds", diff);
        
        LOG_INFO("Delta sent. Server responded with status code: `", op->resp.status_code());
    }

    LOG_INFO("Dsync client flow completed successfully.");
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

    return perform_dsync_client_flow(FLAGS_server_ip, FLAGS_server_port,
                                     FLAGS_basis_filename, FLAGS_new_filename,
                                     FLAGS_hw);
}