#include <sys/fcntl.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <chrono>
#include <map>
#include <mutex>
#include <gflags/gflags.h>
#include <sstream>
#include <photon/thread/thread11.h>
#include <photon/io/signal.h>
#include <photon/fs/localfs.h>
#include <photon/common/alog.h>
#include <photon/io/fd-events.h>
#include <photon/net/http/server.h>
#include <photon/common/string_view.h>
#include <photon/common/utility.h>
#include <photon/net/socket.h>
#include <photon/common/alog-stdstring.h>
#include <mimalloc.h>
#include "dsync.pb.h"

#include "dsync.h"
#include "dsync_http.h"
#include "skysync_c_worker.h"

using namespace photon;

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

DEFINE_int32(port, 19876, "port");

static bool stop_flag = false;

static void stop_handler(int signal) { stop_flag = true; }

// Structure to track matched chunks RTT timing per client request
struct MatchedChunksRTTTracker {
    std::chrono::high_resolution_clock::time_point start_time;
    std::string filename;
    bool timing_active;
    
    MatchedChunksRTTTracker() : timing_active(false) {}
};

// Map to track timing state for matched chunks requests (using request key)
static std::map<std::string, MatchedChunksRTTTracker> matched_chunks_rtt_trackers;
static std::mutex matched_chunks_rtt_tracker_mutex;

class SkySyncCHandler : public net::http::HTTPHandler {
public:
    int handle_request(net::http::Request& req, net::http::Response& resp, std::string_view) override {
        auto target = req.target();
        LOG_INFO("Received request for target: `", std::string(target).c_str());

        std::string path_str = std::string(target.substr(0, target.find('?')));
        std::string query_str = target.find('?') != std::string_view::npos ? std::string(target.substr(target.find('?') + 1)) : "";

        std::string basis_filename;
        bool hw = false; // Default to false, indicating software hashing

        // Parse query parameters
        if (!query_str.empty()) {
            std::istringstream query_stream(query_str);
            std::string param;
            while (std::getline(query_stream, param, '&')) {
                size_t eq_pos = param.find('=');
                if (eq_pos != std::string::npos) {
                    std::string key = param.substr(0, eq_pos);
                    std::string value = param.substr(eq_pos + 1);
                    
                    if (key == "basis_filename") {
                        basis_filename = value;
                    } else if (key == "hw") {
                        hw = (value == "1" || value == "true");
                    }
                }
            }
        }

        if (basis_filename.empty()) {
            LOG_WARN("basis_filename not provided in query parameters");
            resp.set_result(400); // Bad Request
            resp.headers.content_length(49);
            resp.write("Error: basis_filename parameter is required.", 49);
            return 0;
        }

        if (path_str == "/csums_queue" && req.verb() == net::http::Verb::POST) {
            LOG_INFO("Handling /csums_queue request for file: ` with hw=`", basis_filename.c_str(), hw);

            std::string request_body(req.headers.content_length(), '\0');
            ssize_t bytes_read = req.read(request_body.data(), request_body.size());
            if (bytes_read != (ssize_t)request_body.size()) {
                LOG_ERROR("Failed to read request body for /csums_queue. Expected: `, Read: `", request_body.size(), bytes_read);
                resp.set_result(400);
                resp.headers.content_length(50);
                resp.write("Error: Failed to read request body.", 50);
                return 0;
            }

            // Initialize ServerSkySyncCWorker with appropriate algorithms based on hw parameter
            uint8_t whash_algorithm = 0;
            if (hw) {
                LOG_INFO("Using hardware accelerated hashing algorithm");
                whash_algorithm = 1;
            } else {
                LOG_INFO("Using non-hardware accelerated hashing algorithm");
            }

            ServerSkySyncCWorker server_worker(whash_algorithm);
            
            // Parse protobuf request
            dsync::Uint32Queue csums_queue_pb;
            if (!csums_queue_pb.ParseFromString(request_body)) {
                LOG_ERROR("Failed to parse csums_queue protobuf");
                
                // Send error response as protobuf
                dsync::ErrorResponse error_pb;
                error_pb.set_error_code(400);
                error_pb.set_error_message("Invalid protobuf in request body");
                error_pb.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                
                std::string error_data;
                if (error_pb.SerializeToString(&error_data)) {
                    resp.set_result(400);
                    resp.headers.content_length(error_data.size());
                    resp.headers.insert("Content-Type", "application/x-protobuf");
                    resp.write(error_data.c_str(), error_data.size());
                } else {
                    resp.set_result(400);
                    resp.headers.content_length(35);
                    resp.write("Error: Invalid protobuf in request body.", 35);
                }
                return 0;
            }
            
            deserialize_uint32_queue_from_protobuf(csums_queue_pb, server_worker.new_crc32_queue);

            int old_fd = open(basis_filename.c_str(), O_RDONLY);
            if (old_fd < 0) {
                LOG_ERROR("Failed to open old file `", basis_filename.c_str());
                resp.set_result(404);
                resp.headers.content_length(30);
                resp.write("Error: Old file not found.", 30);
                return 0;
            }
            DEFER(close(old_fd));

            off_t old_file_s = file_size(old_fd);
            if (old_file_s == -1) {
                LOG_ERROR("Failed to get file size for `", basis_filename.c_str());
                resp.set_result(500);
                resp.headers.content_length(37);
                resp.write("Error: Failed to get file size.", 37);
                return 0;
            }

            // Generate request key BEFORE processing
            auto now = std::chrono::high_resolution_clock::now();
            std::string request_key = basis_filename + "_" + std::to_string(now.time_since_epoch().count());

            // Perform ALL processing inline BEFORE responding
            server_worker.fsc_csums = calc_fsc_hw(old_fd);
            auto start = std::chrono::high_resolution_clock::now();
            server_worker.serial_cdc(old_fd, server_worker.old_csums_queue, server_worker.fsc_csums);
            // server_worker.lhash_builder(server_worker.old_csums_queue, old_file_s / 8192);
            server_worker.hash7_builder(server_worker.old_csums_queue, old_file_s / 8192);
            std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
            LOG_INFO("Serial CDC completed in ` seconds", diff_1.count());

            start = std::chrono::high_resolution_clock::now();
            // server_worker.compare_weak_lhash(old_fd, server_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
            server_worker.compare_weak_hash7(old_fd, server_worker.new_crc32_queue, server_worker.weak_matched_chunks_queue_1);
            diff_1 = std::chrono::high_resolution_clock::now() - start;
            LOG_INFO("Weak hash comparison completed in ` seconds", diff_1.count());

            // Create protobuf response
            dsync::CsumsQueueResponse response_pb;
            response_pb.set_success(true);
            response_pb.set_request_key(request_key);
            
            // Serialize weak matched data to protobuf
            dsync::MatchedItemRpc1Queue weak_matched_pb = serialize_matched_item_rpc_1_queue_to_protobuf(server_worker.weak_matched_chunks_queue_1);
            *response_pb.mutable_weak_matched_data() = weak_matched_pb;
            
            std::string response_data;
            if (!response_pb.SerializeToString(&response_data)) {
                LOG_ERROR("Failed to serialize response to protobuf");
                resp.set_result(500);
                resp.headers.content_length(40);
                resp.write("Error: Failed to serialize response.", 40);
                return 0;
            }
            
            // Send protobuf response
            resp.set_result(200);
            resp.headers.content_length(response_data.size());
            resp.headers.insert("Content-Type", "application/x-protobuf");
            resp.headers.insert("X-Request-Key", request_key);  // Keep for backward compatibility
            resp.write(response_data.c_str(), response_data.size());
            
            // Store request key for RTT tracking (no longer store processed chunks)
            {
                std::lock_guard<std::mutex> lock(matched_chunks_rtt_tracker_mutex);
                MatchedChunksRTTTracker& tracker = matched_chunks_rtt_trackers[request_key];
                tracker.filename = basis_filename;
                tracker.timing_active = true;
                tracker.start_time = std::chrono::high_resolution_clock::now();
            }
            
            LOG_INFO("/csums_queue request processed successfully with inline data. Request Key: `", request_key.c_str());

        } else if (path_str == "/ack" && req.verb() == net::http::Verb::POST) {
            LOG_INFO("Handling /ack request");
            
            // Read the request key from the request body
            std::string request_key;
            auto content_length = req.headers.content_length();
            if (content_length > 0) {
                request_key.resize(content_length);
                ssize_t bytes_read = req.read((char*)request_key.data(), content_length);
                if (bytes_read != (ssize_t)content_length) {
                    LOG_ERROR("Failed to read request key from ACK body. Expected: `, Read: `", content_length, bytes_read);
                    resp.set_result(400); // Bad Request
                    resp.headers.content_length(45);
                    resp.write("Error: Failed to read request key from body.", 45);
                    return 0;
                }
            } else {
                LOG_WARN("No request key in ACK request body.");
                resp.set_result(400); // Bad Request
                resp.headers.content_length(47);
                resp.write("Error: Request key is required for ACK operation.", 47);
                return 0;
            }
            
            // End timing for Matched Chunks RTT - ACK received
            auto ack_received_time = std::chrono::high_resolution_clock::now();
            
            // Find and process the matched chunks timing information
            {
                std::lock_guard<std::mutex> lock(matched_chunks_rtt_tracker_mutex);
                auto it = matched_chunks_rtt_trackers.find(request_key);
                if (it != matched_chunks_rtt_trackers.end() && it->second.timing_active) {
                    auto diff = std::chrono::duration<double>(ack_received_time - it->second.start_time).count();
                    LOG_INFO("Matched Chunks RTT (Server-side): ` seconds for request key: `",
                             diff, request_key.c_str());
                    
                    // Clean up the tracker
                    matched_chunks_rtt_trackers.erase(it);
                } else {
                    LOG_WARN("No active matched chunks timing found for request key: `", request_key.c_str());
                }
            }
            
            resp.set_result(200); // OK
            resp.headers.content_length(8);
            resp.write("ACK_RECV", 8);
            
            LOG_INFO("ACK processed for request key: `", request_key.c_str());

        } else if (path_str == "/patch" && req.verb() == net::http::Verb::POST) {
            LOG_INFO("Handling /patch request for file: `", basis_filename.c_str());

            // Create temporary file to stream incoming delta data
            char tmp_file[] = "/tmp/skysync-c-server-delta-XXXXXX";
            int tmp_fd = mkstemp(tmp_file);
            if (tmp_fd < 0) {
                LOG_ERROR("Failed to create temporary file for streaming delta data, error: `(`)", errno, strerror(errno));
                
                // Create protobuf error response
                dsync::ErrorResponse error_resp;
                error_resp.set_error_code(500);
                error_resp.set_error_message("Failed to create temporary file");
                error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                
                std::string error_data;
                error_resp.SerializeToString(&error_data);
                
                resp.set_result(500);
                resp.headers.content_length(error_data.size());
                resp.headers.insert("Content-Type", "application/x-protobuf");
                resp.write(error_data.c_str(), error_data.size());
                return 0;
            }
            DEFER(close(tmp_fd); unlink(tmp_file));

            // Stream incoming HTTP request data directly to temporary file
            size_t content_length = req.headers.content_length();
            size_t total_written = 0;
            const size_t buffer_size = 64 * 1024 * 1024;
            char *buffer = (char *)mi_malloc(buffer_size);
            DEFER(mi_free(buffer));
            auto delta_rtt_start = std::chrono::high_resolution_clock::now();
            
            while (total_written < content_length) {
                size_t to_read = std::min(buffer_size, content_length - total_written);
                ssize_t bytes_read = req.read(buffer, to_read);
                if (bytes_read <= 0) {
                    LOG_ERROR("Failed to read request data for /patch. Expected: `, Read: `", to_read, bytes_read);
                    
                    // Create protobuf error response
                    dsync::ErrorResponse error_resp;
                    error_resp.set_error_code(400);
                    error_resp.set_error_message("Failed to read request body");
                    error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count());
                    
                    std::string error_data;
                    error_resp.SerializeToString(&error_data);
                    
                    resp.set_result(400);
                    resp.headers.content_length(error_data.size());
                    resp.headers.insert("Content-Type", "application/x-protobuf");
                    resp.write(error_data.c_str(), error_data.size());
                    return 0;
                }
                
                ssize_t bytes_written = write(tmp_fd, buffer, bytes_read);
                if (bytes_written != bytes_read) {
                    LOG_ERROR("Failed to write to temporary file. Expected: `, Written: `", bytes_read, bytes_written);
                    
                    // Create protobuf error response
                    dsync::ErrorResponse error_resp;
                    error_resp.set_error_code(500);
                    error_resp.set_error_message("Failed to write to temporary file");
                    error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count());
                    
                    std::string error_data;
                    error_resp.SerializeToString(&error_data);
                    
                    resp.set_result(500);
                    resp.headers.content_length(error_data.size());
                    resp.headers.insert("Content-Type", "application/x-protobuf");
                    resp.write(error_data.c_str(), error_data.size());
                    return 0;
                }
                
                total_written += bytes_read;
            }

            // Reset file position to beginning for reading
            if (lseek(tmp_fd, 0, SEEK_SET) < 0) {
                LOG_ERROR("Failed to seek to beginning of temporary file, error: `(`)", errno, strerror(errno));
                
                // Create protobuf error response
                dsync::ErrorResponse error_resp;
                error_resp.set_error_code(500);
                error_resp.set_error_message("Failed to seek temporary file");
                error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                
                std::string error_data;
                error_resp.SerializeToString(&error_data);
                
                resp.set_result(500);
                resp.headers.content_length(error_data.size());
                resp.headers.insert("Content-Type", "application/x-protobuf");
                resp.write(error_data.c_str(), error_data.size());
                return 0;
            }

            auto delta_rtt_end = std::chrono::high_resolution_clock::now();
            auto diff = std::chrono::duration<double>(delta_rtt_end - delta_rtt_start).count();

            std::string ack_response = "ACK-" + std::to_string(diff);
            resp.set_result(200);
            resp.headers.content_length(ack_response.size());
            resp.write(ack_response.c_str(), ack_response.size());

            ServerSkySyncCWorker server_worker;
            
            // Parse binary data from temporary file and populate data_cmd_queue
            try {
                size_t bytes_processed = 0;
                while (bytes_processed < content_length) {
                    data_cmd cmd;
                    
                    // Read command type (1 byte)
                    ssize_t read_result = read(tmp_fd, &cmd.cmd, sizeof(cmd.cmd));
                    if (read_result != sizeof(cmd.cmd)) {
                        LOG_ERROR("Failed to read command type from temporary file");
                        break;
                    }
                    bytes_processed += sizeof(cmd.cmd);
                    
                    // Read offset (8 bytes)
                    read_result = read(tmp_fd, &cmd.offset, sizeof(cmd.offset));
                    if (read_result != sizeof(cmd.offset)) {
                        LOG_ERROR("Failed to read offset from temporary file");
                        break;
                    }
                    bytes_processed += sizeof(cmd.offset);
                    
                    // Read length (8 bytes)
                    read_result = read(tmp_fd, &cmd.length, sizeof(cmd.length));
                    if (read_result != sizeof(cmd.length)) {
                        LOG_ERROR("Failed to read length from temporary file");
                        break;
                    }
                    bytes_processed += sizeof(cmd.length);
                    
                    // Initialize data pointer
                    cmd.data = nullptr;
                    cmd.end_of_stream = false;
                    
                    // For CMD_LITERAL, read the data bytes
                    if (cmd.cmd == CMD_LITERAL) {
                        if (cmd.length > 0) {
                            cmd.data = (uint8_t *)mi_malloc(cmd.length);
                            
                            size_t data_read = 0;
                            while (data_read < cmd.length) {
                                ssize_t read_size = read(tmp_fd, cmd.data + data_read, cmd.length - data_read);
            
                                data_read += read_size;
                            }
                            bytes_processed += cmd.length;
                        }
                    } else if (cmd.cmd != CMD_COPY) {
                        LOG_ERROR("Invalid command type: `", (int)cmd.cmd);
                        
                        // Create protobuf error response
                        dsync::ErrorResponse error_resp;
                        error_resp.set_error_code(400);
                        error_resp.set_error_message("Invalid command type in binary data");
                        error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count());
                        
                        std::string error_data;
                        error_resp.SerializeToString(&error_data);
                        
                        resp.set_result(400);
                        resp.headers.content_length(error_data.size());
                        resp.headers.insert("Content-Type", "application/x-protobuf");
                        resp.write(error_data.c_str(), error_data.size());
                        return 0;
                    }
                    
                    // Add command to queue
                    server_worker.data_cmd_queue.push(cmd);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("Failed to process binary delta data: `", e.what());
                
                // Create protobuf error response
                dsync::ErrorResponse error_resp;
                error_resp.set_error_code(400);
                error_resp.set_error_message("Error processing binary delta data");
                error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                
                std::string error_data;
                error_resp.SerializeToString(&error_data);
                
                resp.set_result(400);
                resp.headers.content_length(error_data.size());
                resp.headers.insert("Content-Type", "application/x-protobuf");
                resp.write(error_data.c_str(), error_data.size());
                return 0;
            }

            server_worker.data_cmd_queue.setDone();

            int old_fd = open(basis_filename.c_str(), O_RDONLY);
            if (old_fd < 0) {
                LOG_ERROR("Failed to open old file `", basis_filename.c_str());
                
                // Create protobuf error response
                dsync::ErrorResponse error_resp;
                error_resp.set_error_code(404);
                error_resp.set_error_message("Old file not found");
                error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                
                std::string error_data;
                error_resp.SerializeToString(&error_data);
                
                resp.set_result(404);
                resp.headers.content_length(error_data.size());
                resp.headers.insert("Content-Type", "application/x-protobuf");
                resp.write(error_data.c_str(), error_data.size());
                return 0;
            }
            DEFER(close(old_fd));

            std::string new_filename = basis_filename + ".new";
            int output_fd = open(new_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (output_fd < 0) {
                LOG_ERROR("Failed to create/open new file `", new_filename.c_str());
                
                // Create protobuf error response
                dsync::ErrorResponse error_resp;
                error_resp.set_error_code(500);
                error_resp.set_error_message("Failed to create new file");
                error_resp.set_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                
                std::string error_data;
                error_resp.SerializeToString(&error_data);
                
                resp.set_result(500);
                resp.headers.content_length(error_data.size());
                resp.headers.insert("Content-Type", "application/x-protobuf");
                resp.write(error_data.c_str(), error_data.size());
                return 0;
            }
            DEFER(close(output_fd));

            // Run patch_delta to generate new_file
            auto start = std::chrono::high_resolution_clock::now();
            server_worker.patch_delta(old_fd, output_fd, server_worker.data_cmd_queue);
            std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
            LOG_INFO("Patch delta applied in ` seconds", diff_1.count());

            LOG_INFO("Patch applied and new file saved to `", new_filename.c_str());

        } else {
            LOG_WARN("Unsupported method or path: ` `", req.verb(), path_str.c_str());
            resp.set_result(405);
            resp.headers.content_length(46);
            resp.write("Error: Method Not Allowed or Unsupported Path.", 46);
        }
        return 0;
    }
};

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    set_log_output_level(ALOG_INFO);
    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_NONE))
        return -1;
    DEFER(photon::fini());

    photon::block_all_signal();
    photon::sync_signal(SIGINT, &stop_handler);
    photon::sync_signal(SIGTERM, &stop_handler);
    photon::sync_signal(SIGTSTP, &stop_handler);

    auto tcpserv = net::new_tcp_socket_server();
    tcpserv->bind_v4any(FLAGS_port);
    tcpserv->listen();
    DEFER(delete tcpserv);

    auto http_srv = net::http::new_http_server();
    DEFER(delete http_srv);

    SkySyncCHandler handler;
    http_srv->add_handler(&handler);

    tcpserv->set_handler(http_srv->get_connection_handler());
    tcpserv->start_loop();

    LOG_INFO("SkySyncC HTTP Server started on port `", FLAGS_port);

    while (!stop_flag) {
        photon::thread_sleep(1);
    }
    LOG_INFO("SkySyncC HTTP Server stopped");
    return 0;
}