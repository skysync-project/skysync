#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <sstream>
#include <gflags/gflags.h>
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

#include "skysync_f_worker.h"
#include "skysync_f.h"
#include "dsync.h"
#include "skysync.pb.h"

using namespace photon;
using namespace skysync_f;

DEFINE_int32(port, 19876, "port");

static bool stop_flag = false;

static void stop_handler(int signal) { stop_flag = true; }

// Structure to track signature RTT timing per client request
struct SignatureRTTTracker {
    std::chrono::high_resolution_clock::time_point start_time;
    std::string filename;
    bool timing_active;
    
    SignatureRTTTracker() : timing_active(false) {}
};

// Map to track timing state per client (using connection info as key)
// In a real implementation, you might want to use a more sophisticated key
// For simplicity, we'll use a simple map with request ID or similar
static std::map<std::string, SignatureRTTTracker> rtt_trackers;
static std::mutex rtt_tracker_mutex;

class SkySyncFHandler : public net::http::HTTPHandler {
public:
    int handle_request(net::http::Request& req, net::http::Response& resp, std::string_view) override {
        auto target = req.target();
        LOG_INFO("Received request for target: `", std::string(target).c_str());

        std::string path_str = std::string(target.substr(0, target.find('?')));
        std::string query_str = target.find('?') != std::string_view::npos ? std::string(target.substr(target.find('?') + 1)) : "";

        std::string filename;
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
                    
                    if (key == "basis_filename" || key == "file") {
                        filename = value;
                    } else if (key == "hw") {
                        hw = (value == "1" || value == "true");
                    }
                }
            }
        }

        if (filename.empty()) {
            LOG_WARN("Filename not provided in query parameters");
            resp.set_result(400); // Bad Request
            resp.headers.content_length(39);
            resp.write("Error: filename parameter is required.", 39);
            return 0;
        }

        if (path_str == "/signature" && req.verb() == net::http::Verb::GET) {
            LOG_INFO("Handling /signature request for file: ` with hw=`", filename.c_str(), hw);

            uint8_t whash_algorithm = 0;
            if (hw) {
                LOG_INFO("Using hardware accelerated hashing algorithm");
                whash_algorithm = 1;
            } else {
                LOG_INFO("Using non-hardware accelerated hashing algorithm");
            }

            ServerSkySyncFWorker server_worker(whash_algorithm);
            
            int fd = open(filename.c_str(), O_RDONLY);
            if (fd < 0) {
                LOG_ERROR("Failed to open file `", filename.c_str());
                resp.set_result(404); // Not Found
                resp.headers.content_length(40);
                resp.write("Error: Original file not found.", 40);
                return 0;
            }
            DEFER(close(fd));

            server_worker.old_csums = calc_fsc_hw(fd);
            const char *fsc_file = "server_fsc.bin";
            write_fsc(fsc_file, server_worker.old_csums);
            free_file_fsc(server_worker.old_csums);
            server_worker.old_csums = nullptr;

            auto start = std::chrono::high_resolution_clock::now();
            server_worker.old_csums = read_fsc(fsc_file);
            std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
            LOG_INFO("Signature generation completed in ` seconds", diff_1.count());

            // Serialize the signature data using Protobuf
            FileFSC fsc_proto;
            for (uint64_t i = 0; i < server_worker.old_csums->chunk_num; ++i) {
                auto* one_fsc_proto = fsc_proto.add_fsc_array();
                one_fsc_proto->set_offset(server_worker.old_csums->fsc_array[i].offset);
                one_fsc_proto->set_length(server_worker.old_csums->fsc_array[i].length);
                one_fsc_proto->set_weak_hash(server_worker.old_csums->fsc_array[i].weak_hash);
                one_fsc_proto->set_strong_hash(server_worker.old_csums->fsc_array[i].strong_hash);
            }

            std::string sig_data;
            if (!fsc_proto.SerializeToString(&sig_data)) {
                LOG_ERROR("Failed to serialize signature to Protobuf");
                resp.set_result(500);
                return 0;
            }

            resp.set_result(200);
            resp.headers.insert("Content-Type", "application/protobuf");
            resp.headers.content_length(sig_data.size());

            // Create a unique key for this request
            auto now = std::chrono::high_resolution_clock::now();
            std::string request_key = filename + "_" + std::to_string(now.time_since_epoch().count());
            
            // Store timing information for this request
            {
                std::lock_guard<std::mutex> lock(rtt_tracker_mutex);
                SignatureRTTTracker& tracker = rtt_trackers[request_key];
                tracker.filename = filename;
                tracker.timing_active = true;
                // Start timing right before sending signature data
                tracker.start_time = std::chrono::high_resolution_clock::now();
            }
            
            // Add request key to response headers so client can send it back in ACK
            resp.headers.insert("X-Request-Key", request_key);
            
            resp.write(sig_data.data(), sig_data.size());
            
            LOG_INFO("Signature generated and sent for file: ` (Request Key: `)", filename.c_str(), request_key.c_str());

        } else if (path_str == "/patch" && req.verb() == net::http::Verb::POST) {
            LOG_INFO("Handling /patch request for file: `", filename.c_str());

            // Create temporary file to stream incoming delta data
            char tmp_file[] = "/tmp/skysyncf-server-delta-XXXXXX";
            int tmp_fd = mkstemp(tmp_file);
            if (tmp_fd < 0) {
                LOG_ERROR("Failed to create temporary file for streaming delta data, error: `(`)", errno, strerror(errno));
                resp.set_result(500);
                resp.headers.content_length(42);
                resp.write("Error: Failed to create temporary file", 42);
                return 0;
            }
            DEFER(close(tmp_fd); unlink(tmp_file));

            // Stream incoming HTTP request data directly to temporary file
            size_t content_length = req.headers.content_length();

            size_t total_written = 0;
            const size_t buffer_size = 64 * 1024 * 1024;
            char *buffer = (char *)mi_malloc(buffer_size);
            DEFER(mi_free(buffer));
            double server_processing_time = 0.0;
            auto delta_rtt_start = std::chrono::high_resolution_clock::now();

            while (total_written < content_length) {
                size_t to_read = std::min(buffer_size, content_length - total_written);
                ssize_t bytes_read = req.read(buffer, to_read);
                if (bytes_read <= 0) {
                    LOG_ERROR("Failed to read request data for /patch. Expected: `, Read: `", to_read, bytes_read);
                    resp.set_result(400);
                    resp.headers.content_length(35);
                    resp.write("Error: Failed to read request body", 35);
                    return 0;
                }

                ssize_t bytes_written = write(tmp_fd, buffer, bytes_read);
                if (bytes_written != bytes_read) {
                    LOG_ERROR("Failed to write to temporary file. Expected: `, Written: `", bytes_read, bytes_written);
                    resp.set_result(500);
                    resp.headers.content_length(42);
                    resp.write("Error: Failed to write to temporary file", 42);
                    return 0;
                }
                
                total_written += bytes_read;
            }

            // Reset file position to beginning for reading
            if (lseek(tmp_fd, 0, SEEK_SET) < 0) {
                LOG_ERROR("Failed to seek to beginning of temporary file, error: `(`)", errno, strerror(errno));
                resp.set_result(500);
                resp.headers.content_length(42);
                resp.write("Error: Failed to seek temporary file", 42);
                return 0;
            }
            auto delta_rtt_end = std::chrono::high_resolution_clock::now();
            auto diff = std::chrono::duration<double>(delta_rtt_end - delta_rtt_start).count();

            std::string ack_response = "ACK-" + std::to_string(diff);
            resp.set_result(200); // OK
            resp.headers.content_length(ack_response.size());
            resp.write(ack_response.c_str(), ack_response.size());

            ServerSkySyncFWorker server_worker(0);
            
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
                                if (read_size <= 0) {
                                    LOG_ERROR("Failed to read literal data from temporary file");
                                    mi_free(cmd.data);
                                    resp.set_result(400);
                                    resp.headers.content_length(40);
                                    resp.write("Error: Failed to read binary data", 40);
                                    return 0;
                                }
                                data_read += read_size;
                            }
                            bytes_processed += cmd.length;
                        }
                    } else if (cmd.cmd != CMD_COPY) {
                        LOG_ERROR("Invalid command type: `", (int)cmd.cmd);
                        resp.set_result(400);
                        resp.headers.content_length(35);
                        resp.write("Error: Invalid command type", 35);
                        return 0;
                    }
                    
                    // Add command to queue
                    server_worker.data_cmd_queue.push(cmd);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("Failed to process binary delta data: `", e.what());
                resp.set_result(400);
                resp.headers.content_length(45);
                resp.write("Error: Failed to process binary delta data", 45);
                return 0;
            }
            
            server_worker.data_cmd_queue.setDone();

            int fd = open(filename.c_str(), O_RDONLY);
            if (fd < 0) {
                LOG_ERROR("Failed to open file `", filename.c_str());
                resp.set_result(404); // Not Found
                resp.headers.content_length(40);
                resp.write("Error: Original file not found.", 40);
                return 0;
            }
            DEFER(close(fd));

            std::string new_filename = filename + ".new";
            int new_fd = open(new_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (new_fd < 0) {
                LOG_ERROR("Failed to open new file `", new_filename.c_str());
                return 0;
            }
            DEFER(close(new_fd));

            auto start = std::chrono::high_resolution_clock::now();
            server_worker.patch_delta(fd, new_fd, server_worker.data_cmd_queue);
            std::chrono::duration<double> diff_2 = std::chrono::high_resolution_clock::now() - start;
            LOG_INFO("Patch applied in ` seconds", diff_2.count());

            LOG_INFO("Patch applied and new file saved to: `", new_filename.c_str());

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
            
            // End timing for Signature RTT - ACK received
            auto ack_received_time = std::chrono::high_resolution_clock::now();
            
            // Find and process the timing information
            {
                std::lock_guard<std::mutex> lock(rtt_tracker_mutex);
                auto it = rtt_trackers.find(request_key);
                if (it != rtt_trackers.end() && it->second.timing_active) {
                    auto diff = std::chrono::duration<double>(ack_received_time - it->second.start_time).count();
                    LOG_INFO("Signature RTT (Server-side): ` seconds for request key: `",
                             diff, request_key.c_str());
                    
                    // Clean up the tracker
                    rtt_trackers.erase(it);
                } else {
                    LOG_WARN("No active timing found for request key: `", request_key.c_str());
                }
            }
            
            resp.set_result(200); // OK
            resp.headers.content_length(8);
            resp.write("ACK_RECV", 8);
            
            LOG_INFO("ACK processed for request key: `", request_key.c_str());

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

    SkySyncFHandler handler;
    http_srv->add_handler(&handler);

    tcpserv->set_handler(http_srv->get_connection_handler());
    tcpserv->start_loop();

    LOG_INFO("SkySyncF HTTP Server started on port `", FLAGS_port);

    while (!stop_flag) {
        photon::thread_sleep(1);
    }
    LOG_INFO("SkySyncF HTTP Server stopped");
    return 0;
}