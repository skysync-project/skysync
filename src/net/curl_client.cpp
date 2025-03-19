#include <stdio.h>
#include "net.h"
#include "dsync_worker.h"

off_t file_size(int fd);

/* ===============libcurl=============== */

/* Write function for curl_easy_setopt */
int pro_fwrite(void *buffer, size_t size, size_t nmemb, void *stream) {
    clock_t start = clock();
    struct file_info *info = (struct file_info *) stream;
    if(!info->fp) {
        info->fp = fopen(info->file_name, "wb");
        if(!info->fp)
            return -1;
    }

    size_t bytes_write = fwrite(buffer, size, nmemb, info->fp);
    clock_t end = clock();
    curl_total_write_time += (end - start);
    return bytes_write;
}

/* HTTP Get digs file from url */
int http_client_get(char *file_name, char *url, char *function) {
    CURL *curl;
    CURLcode res;
    curl_off_t total_time;

    char *base_file_name = basename(file_name);
    char file_url[FILE_URL_SIZE];

    /* file_url = url + "/" + "?" + "filename=base_file_name" + "-digs" + "?" + "function=rsync/corsync"; */
    snprintf(file_url, FILE_URL_SIZE, "%s/?filename=%s&function=%s", url, base_file_name, function);
    char digs_file[FILE_URL_SIZE];

    /* local digs_file = file_name + "-rdigs-rsync/corsync" */
    snprintf(digs_file, FILE_URL_SIZE, "%s-%s", file_name, function);

    struct file_info info = {
        .file_name = digs_file,
        .fp = NULL
    };

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();

    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, file_url);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

        /* Set the write callback function. */
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, pro_fwrite);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &info);

        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

        res = curl_easy_perform(curl);

        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        else {
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &total_time);
            printf("Total time: %ld ms \n", total_time / 1000);
        }

        curl_easy_cleanup(curl);

        if(info.fp)
            fclose(info.fp);
    }

    curl_global_cleanup();

    printf("Curl total write time: %ld ms\n\n", curl_total_write_time / 1000);

    return 0;
}

int http_client_post_fastfp(char *url, char *file_name, char *function) {
    CURL *curl;
    CURLcode res;

    curl_off_t total_time;

    char write_file_path[strlen(file_name) + 20];
    strcpy(write_file_path, file_name);
    strcat(write_file_path, "-remote-matched");

    char read_file_path[strlen(file_name) + 20];
    strcpy(read_file_path, file_name);
    if(strcmp(function, "fastcdc") == 0)
        strcat(read_file_path, "-fastcdc-fastfp");
    else if(strcmp(function, "cocdc") == 0)
        strcat(read_file_path, "-cocdc-fastfp");
    else
        return -1;

    char *base_file_name = basename(file_name);

    struct file_info info = {
        .file_name = write_file_path,
        .fp = NULL
    };

    char file_url[FILE_URL_SIZE];
    snprintf(file_url, FILE_URL_SIZE, "%s/?filename=%s&function=fastfp", url, base_file_name);

    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();

    if(curl) {
        /* Set the URL. */
        curl_easy_setopt(curl, CURLOPT_URL, file_url);

        /* Load file from disk into a buffer */
        FILE *file = fopen(read_file_path, "rb");

        if (!file)
            exit(EXIT_FAILURE);

        long fs = file_size(fileno(file));
        uint8_t *file_data = (uint8_t *) malloc(fs + 1);
        uint8_t* tmp = file_data;
        size_t bytes_read = 0;

        while( (bytes_read = fread(file_data, sizeof(uint8_t), fs, file)) > 0)
            file_data += bytes_read;

        fclose(file);

        /* Set the file data as the POST request payload. */
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, fs + 1);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, tmp);

        /* Set the write callback function. */
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, pro_fwrite);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &info);

        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

        /* Set the maximum speed in bytes/second to send data to the remote server. */
        // curl_easy_setopt(curl, CURLOPT_MAX_SEND_SPEED_LARGE, (curl_off_t) _1GB);

        /* Perform the HTTP POST request. */
        res = curl_easy_perform(curl);

        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        else {
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &total_time);
            printf("Total time: %ld ms \n", total_time / 1000);
        }

        if(tmp)
            free(tmp);
        if(info.fp)
            fclose(info.fp);

        /* Clean up. */
        curl_easy_cleanup(curl);
    }

    /* Clean up global libcurl resources. */
    curl_global_cleanup();

    printf("Curl total write time: %ld ms\n\n", curl_total_write_time / 1000);

    return 0;
}

int post_sync_req(const char *url, const char *file_name) {
    CURL *curl;
    CURLcode res;
    double total_time;

    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();

    if(curl) {
        /* Set the URL. */
        curl_easy_setopt(curl, CURLOPT_URL, url);

        /* Set the POST request payload. */
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, file_name);

        /* Perform the HTTP POST request. */
        res = curl_easy_perform(curl);

        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        else {
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total_time);
            printf("Total time: %.3f ms \n", total_time * 1000);
        }

        /* Clean up. */
        curl_easy_cleanup(curl);
    }

    /* Clean up global libcurl resources. */
    curl_global_cleanup();

    return (res == CURLE_OK) ? 0 : 1;
}

int weak_csums_reply(void *ptr, size_t size, size_t nmemb, void *stream) {
    size_t total_size = size * nmemb;
    DataQueue<matched_item> *queue = static_cast<DataQueue<matched_item> *>(stream);

    size_t num_items = total_size / sizeof(matched_item);
    matched_item *items = static_cast<matched_item *>(ptr);

    for (size_t i = 0; i < num_items; ++i) {
        queue->push(items[i]);
    }

    return total_size;
}

int post_weak_csums(const char *url, DataQueue<one_cdc> &old_csums_queue, DataQueue<matched_item> &weak_matched_chunks_queue) {
    CURL *curl;
    CURLcode res;
    double total_time;

    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();

    if(curl) {
        /* Set the POST request payload. */
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, sizeof(one_cdc));
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

        /* Set the write callback function to store the response. */
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, weak_csums_reply);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &weak_matched_chunks_queue);
        
        while (!old_csums_queue.isDone()) {
            one_cdc cdc = old_csums_queue.pop();
           
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, &cdc);
            
            /* Perform the HTTP POST request. */
            res = curl_easy_perform(curl);

            if(res != CURLE_OK) {
                fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
                break;
            }
        }

        /* Clean up. */
        curl_easy_cleanup(curl);
    }

    /* Clean up global libcurl resources. */
    curl_global_cleanup();

    return (res == CURLE_OK) ? 0 : 1;
}

int post_delta_data(char *url, char *file_name) {
    CURL *curl;
    CURLcode res;
    double total_time;

    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();

    if(curl) {
        /* Set the URL. */
        curl_easy_setopt(curl, CURLOPT_URL, url);

        /* Load file from disk into a buffer */
        FILE *file = fopen(file_name, "rb");

        if (!file) {
            fprintf(stderr, "Failed to open file: %s\n", file_name);
            curl_easy_cleanup(curl);
            curl_global_cleanup();
            return 1;
        }

        long fs = file_size(fileno(file));
        uint8_t *file_data = (uint8_t *) malloc(fs);
        if (!file_data) {
            fprintf(stderr, "Failed to allocate memory\n");
            fclose(file);
            curl_easy_cleanup(curl);
            curl_global_cleanup();
            return 1;
        }

        size_t bytes_read = fread(file_data, sizeof(uint8_t), fs, file);
        if (bytes_read != fs) {
            fprintf(stderr, "Failed to read file: %s\n", file_name);
            free(file_data);
            fclose(file);
            curl_easy_cleanup(curl);
            curl_global_cleanup();
            return 1;
        }

        fclose(file);

        /* Set the file data as the POST request payload. */
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, fs);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, file_data);

        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

        /* Perform the HTTP POST request. */
        res = curl_easy_perform(curl);

        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        else {
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total_time);
            printf("Total time: %.3f ms \n", total_time * 1000);
        }

        free(file_data);

        /* Clean up. */
        curl_easy_cleanup(curl);
    }

    /* Clean up global libcurl resources. */
    curl_global_cleanup();

    return (res == CURLE_OK) ? 0 : 1;
}

