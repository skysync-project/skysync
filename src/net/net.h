#ifndef NET_H
#define NET_H

#include <stdio.h>
#include <curl/curl.h>
#include <microhttpd.h>
#include <sys/stat.h>
// #include <stddef.h>
// #include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
// #include <fcntl.h>
// #include <unistd.h>
#include <libgen.h>
#include "fastcdc.h"
// #include "rsyncx.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include "dsync_worker.h"


#define DEFAULT_PORT    8888

#define MAXNAMESIZE     20
#define MAXANSWERSIZE   512
#define FILE_URL_SIZE   256

#define GET             0
#define POST            1

#define RSYNC 50
#define RSYNCX 51

#define _1GB 131072000  // 1GB / 8 * 1024 * 1024
#define _100MB 13107200 // 100MB / 8 * 1024 * 1024
#define RTT 50 // ms

static const char * const errorstr = "An internal server error has occurred!\n";
static const char * const complete = "Complete!\n";
static const char * const filenoexist = "File does not exist!\n";


/* ===============libcurl=============== */

struct file_info {
    char *file_name;
    FILE *fp;
};

static clock_t curl_total_write_time = 0;

int pro_fwrite(void *buffer, size_t size, size_t nmemb, void *stream);

/* post a sync request */
int post_sync_req(const char *url, const char *file_name);

/* post weak checksums to a http server */
int post_weak_csums(const char *url, DataQueue<one_cdc> &old_csums_queue, DataQueue<matched_item> &weak_matched_chunks_queue);

/* post a delta file and get ack */
int post_delta_data(char *url, char *file_name);

/* get a digs of a file from a http server */
int http_client_get(char *file_name, char *url, char *function);

/* post a fastfp file and get the matched_chunks */
int http_client_post_fastfp(char *url, char *file_name, char *function);

/* ===============libmicrohttpd=============== */

struct connection_info_struct
{
  int connectiontype;
  const char *answerstring;
  unsigned int answercode;
  const char *file_name;
  FILE *fp;
  uint8_t read;
  struct MHD_PostProcessor *postprocessor;
  clock_t mhd_total_read_time;
  clock_t mhd_total_write_time;
};

enum MHD_Result
print_out_key(void *cls, enum MHD_ValueKind kind, const char *key, const char *value);

enum MHD_Result
send_response(struct MHD_Connection *connection, const char *str, int status_code);

enum MHD_Result
send_digs(struct MHD_Connection *connection, uint8_t *digs, size_t length, int status_code);

enum MHD_Result
iterate_post(void *coninfo_cls, enum MHD_ValueKind kind, const char *key,
              const char *filename, const char *content_type, const char *transfer_encoding,
              const char *data, uint64_t off, size_t size);

void request_completed(void *cls, struct MHD_Connection *connection,
                        void **con_cls, enum MHD_RequestTerminationCode toe);

enum MHD_Result mhd_handle_request(void *cls, struct MHD_Connection *connection,
                                   const char *url, const char *method,
                                   const char *version, const char *upload_data,
                                   size_t *upload_data_size, void **con_cls);

int mhd_http_server(int port);


#endif // NET_H
