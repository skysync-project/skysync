#include <sys/fcntl.h>
#include <sys/stat.h>
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

#include "rsync_http.h"

using namespace photon;

off_t file_size(int fd);
void *map_file(int fd);
void unmap_file(int fd, void *map);

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

// Helper function to write content to a file
static bool write_file_content(const std::string& filename, const char* data, size_t len) {
    auto fs = fs::new_localfs_adaptor();
    if (!fs) {
        LOG_ERROR("Failed to create local filesystem adaptor");
        return false;
    }
    DEFER(delete fs);

    auto file = fs->open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (!file) {
        LOG_ERROR("Failed to open/create file `", filename.c_str());
        return false;
    }
    DEFER(delete file);

    while (len > 0) {
        size_t chunk_size = (len < BUFFER_SIZE) ? len : BUFFER_SIZE;
        ssize_t bytes_written = file->write(data, chunk_size);
        if (bytes_written == -1) {
            LOG_ERROR("Failed to write to file `", filename.c_str());
            return false;
        }
        data += bytes_written;
        len -= bytes_written;
    }
    if (file->fsync() != 0) {
        LOG_ERROR("Failed to sync file `", filename.c_str());
        return false;
    }
    LOG_INFO("File ` written successfully", filename.c_str());
    return true;
}

class RsyncHandler : public net::http::HTTPHandler {
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

            char* sig_data = nullptr;
            size_t sig_len = 0;
            auto start = std::chrono::high_resolution_clock::now();

            std::string base_filename = filename.substr(filename.find_last_of("/\\") + 1);
            std::string sig_file = "/tmp/" + base_filename + ".sig";
            // Delete any existing signature file
            std::remove(sig_file.c_str());

            // std::string sig_file = filename + ".sig";
            // if sig_file exists, delete it
            auto fs = fs::new_localfs_adaptor();
            if (!fs) {
                LOG_ERROR("Failed to create local filesystem adaptor");
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(42);
                resp.write("Error: Failed to create filesystem adaptor.", 42);
                return 0;
            }
            DEFER(delete fs);
            if (fs->access(sig_file.c_str(), F_OK) == 0) {
                fs->unlink(sig_file.c_str());
            }

            std::string rollsum = "rollsum";
            std::string sig_hash = hw ? "md4" : "blake2";
            rs_result ret;
            // rs_result ret = rsyncx_signature_mem((char*)mapped_file_content, file_s, &sig_data, &sig_len, hw);
            if (hw) {
                LOG_INFO("Using hardware accelerated hashing algorithm");
                rs_result ret = rsyncx_signature(filename.c_str(), sig_file.c_str(), sig_hash.c_str(), rollsum.c_str(), hw);
            } else {
                LOG_INFO("Using non-hardware accelerated hashing algorithm");
                rs_result ret = rsyncx_signature(filename.c_str(), sig_file.c_str(), sig_hash.c_str(), rollsum.c_str(), hw);
            }
            int fd = open(sig_file.c_str(), O_RDONLY);
            if (fd < 0) {
                LOG_ERROR("Failed to open signature file `", sig_file.c_str());
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(37);
                resp.write("Error: Failed to open signature file.", 37);
                if (sig_data) free(sig_data);
                return 0;
            }
            DEFER(close(fd));

            sig_len = file_size(fd);
            sig_data = (char*)malloc(sig_len);

            ssize_t bytes_read = read(fd, sig_data, sig_len);
            assert(bytes_read == (ssize_t)sig_len);

            std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
            LOG_INFO("Signature generation completed in ` seconds", diff_1.count());

            resp.set_result(200);
            resp.headers.content_length(sig_len);

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
            
            resp.write(sig_data, sig_len);
            
            LOG_INFO("Signature generated and sent for file: ` (Request Key: `)", filename.c_str(), request_key.c_str());
            if (sig_data) free(sig_data);

        } else if (path_str == "/patch" && req.verb() == net::http::Verb::POST) {
            LOG_INFO("Handling /patch request for file: `", filename.c_str());

            int fd = open(filename.c_str(), O_RDONLY);
            if (fd < 0) {
                LOG_ERROR("Failed to open file `", filename.c_str());
                resp.set_result(404); // Not Found
                resp.headers.content_length(40);
                resp.write("Error: Original file not found.", 40);
                return 0;
            }
            DEFER(close(fd));

            void* mapped_basis_content = map_file(fd);
            if (!mapped_basis_content) {
                LOG_ERROR("Failed to map file `", filename.c_str());
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(37);
                resp.write("Error: Failed to map file content.", 37);
                return 0;
            }
            DEFER(unmap_file(fd, mapped_basis_content));

            off_t basis_file_s = file_size(fd);
            if (basis_file_s == -1) {
                LOG_ERROR("Failed to get file size for `", filename.c_str());
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(37);
                resp.write("Error: Failed to get file size.", 37);
                return 0;
            }

            // Create temporary file to stream incoming delta data
            char tmp_file[] = "/tmp/rsync-server-delta-XXXXXX";
            int tmp_fd = mkstemp(tmp_file);
            if (tmp_fd < 0) {
                LOG_ERROR("Failed to create temporary file for streaming delta data, error: `(`)", errno, strerror(errno));
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(45);
                resp.write("Error: Failed to create temporary file.", 45);
                return 0;
            }
            DEFER(close(tmp_fd); unlink(tmp_file));

            // Stream incoming HTTP request data directly to temporary file
            auto content_length = req.headers.content_length();
            if (content_length <= 0) {
                LOG_WARN("No delta content in patch request body.");
                resp.set_result(400); // Bad Request
                resp.headers.content_length(56);
                resp.write("Error: Delta content is required for patch operation.", 56);
                return 0;
            }

            size_t total_written = 0;
            const size_t buffer_size = 64 * 1024 * 1024; // 64MB buffer
            char *buffer = (char *)malloc(buffer_size);
            if (!buffer) {
                LOG_ERROR("Failed to allocate buffer for streaming delta data");
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(42);
                resp.write("Error: Failed to allocate memory buffer.", 42);
                return 0;
            }
            DEFER(free(buffer));
            auto delta_rtt_start = std::chrono::high_resolution_clock::now();
            
            while (total_written < content_length) {
                size_t to_read = std::min(buffer_size, content_length - total_written);
                ssize_t bytes_read = req.read(buffer, to_read);
                if (bytes_read <= 0) {
                    LOG_ERROR("Failed to read request data for /patch. Expected: `, Read: `", to_read, bytes_read);
                    resp.set_result(400); // Bad Request
                    resp.headers.content_length(54);
                    resp.write("Error: Failed to read delta content from request body.", 54);
                    return 0;
                }
                
                ssize_t bytes_written = write(tmp_fd, buffer, bytes_read);
                if (bytes_written != bytes_read) {
                    LOG_ERROR("Failed to write to temporary file. Expected: `, Written: `", bytes_read, bytes_written);
                    resp.set_result(500); // Internal Server Error
                    resp.headers.content_length(44);
                    resp.write("Error: Failed to write to temporary file.", 44);
                    return 0;
                }
                
                total_written += bytes_read;
            }

            // Reset file position to beginning for reading
            if (lseek(tmp_fd, 0, SEEK_SET) < 0) {
                LOG_ERROR("Failed to seek to beginning of temporary file, error: `(`)", errno, strerror(errno));
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(44);
                resp.write("Error: Failed to seek temporary file.", 44);
                return 0;
            }

            auto delta_rtt_end = std::chrono::high_resolution_clock::now();
            auto diff = std::chrono::duration<double>(delta_rtt_end - delta_rtt_start).count();

            std::string ack_response = "ACK-" + std::to_string(diff);
            resp.set_result(200);
            resp.headers.content_length(ack_response.size());
            resp.write(ack_response.c_str(), ack_response.size());

            // Read delta data from temporary file
            std::vector<char> delta_content(content_length);
            ssize_t total_read = 0;
            while (total_read < (ssize_t)content_length) {
                ssize_t bytes_read = read(tmp_fd, delta_content.data() + total_read, content_length - total_read);
                if (bytes_read <= 0) {
                    LOG_ERROR("Failed to read delta data from temporary file. Expected: `, Read: `", content_length - total_read, bytes_read);
                    resp.set_result(500); // Internal Server Error
                    resp.headers.content_length(50);
                    resp.write("Error: Failed to read delta data from temp file.", 50);
                    return 0;
                }
                total_read += bytes_read;
            }

            char* new_file_data = nullptr;
            size_t new_file_len = 0;
            auto start = std::chrono::high_resolution_clock::now();
            rs_result ret = rsyncx_patch_mem((char*)mapped_basis_content, basis_file_s,
                                             delta_content.data(), delta_content.size(),
                                             &new_file_data, &new_file_len);

            if (ret != RS_DONE) {
                LOG_ERROR("rsyncx_patch_mem failed with error: `", ret);
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(36);
                resp.write("Error: Failed to apply patch.", 36);
                if (new_file_data) free(new_file_data);
                return 0;
            }

            std::string new_filename = filename + ".new";
            if (!write_file_content(new_filename, new_file_data, new_file_len)) {
                resp.set_result(500); // Internal Server Error
                resp.headers.content_length(32);
                resp.write("Error: Failed to save new file.", 32);
                if (new_file_data) free(new_file_data);
                return 0;
            }
            std::chrono::duration<double> diff_1 = std::chrono::high_resolution_clock::now() - start;
            LOG_INFO("Patch delta applied in ` seconds", diff_1.count());

            LOG_INFO("Patch applied and new file saved to: `", new_filename.c_str());
            if (new_file_data) free(new_file_data);

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

    RsyncHandler handler;
    http_srv->add_handler(&handler);

    tcpserv->set_handler(http_srv->get_connection_handler());
    tcpserv->start_loop();

    LOG_INFO("Rsync HTTP Server started on port `", FLAGS_port);

    while (!stop_flag) {
        photon::thread_sleep(1);
    }
    LOG_INFO("Rsync HTTP Server stopped");
    return 0;
}