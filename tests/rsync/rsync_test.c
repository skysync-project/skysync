#include <stdlib.h>
#include <stdarg.h>
// #include <string.h>
#include <stdio.h>
// #include <assert.h>
// #include <stdint.h>
#include <popt.h>
#include <string.h>

// #include "config.h"
#include "librsync.h"
// #include "netint.h"
// #include "rsyncx.h"

static int block_len = 0;
static int strong_len = 0;

static int show_stats = 0;

static int bzip2_level = 0;
static int gzip_level = 0;
static int file_force = 0;

enum {
    OPT_GZIP = 1069, OPT_BZIP2
};

char *rs_hash_name;
char *rs_rollsum_name;

int isprefix(char const *tip, char const *iceberg)
{
    while (*tip) {
        if (*tip != *iceberg)
            return 0;
        tip++;
        iceberg++;
    }

    return 1;
}

static void rsyncx_usage(const char *error, ...)
{
    va_list va;
    char buf[256];

    va_start(va, error);
    vsnprintf(buf, sizeof(buf), error, va);
    va_end(va);
    fprintf(stderr, "rsyncx: %s\n\nTry `rsyncx --help' for more information.\n",
            buf);
}

static void rsyncx_options(poptContext opcon) {
    int c;
    char const *a;

    while ((c = poptGetNextOpt(opcon)) != -1) {
        switch (c) {
        case 'h':
            exit(RS_DONE);
        case 'V':
            exit(RS_DONE);
        case 'v':
            if (!rs_supports_trace()) {
                fprintf(stderr, "rdiff: Library does not support trace.\n");
            }
            rs_trace_set_level(RS_LOG_DEBUG);
            break;

        case OPT_GZIP:
        case OPT_BZIP2:
            if ((a = poptGetOptArg(opcon))) {
                int l = atoi(a);
                if (c == OPT_GZIP)
                    gzip_level = l;
                else
                    bzip2_level = l;
            } else {
                if (c == OPT_GZIP)
                    gzip_level = -1;    /* library default */
                else
                    bzip2_level = 9;    /* demand the best */
            }
            rsyncx_usage("Sorry, compression is not implemented yet.");
            exit(RS_UNIMPLEMENTED);

        default:
            // bad_option(opcon, c);
            return;
        }
    }
}


static rs_result rsyncx_signature(poptContext opcon) {
    FILE *basis_file, *sig_file;
    rs_stats_t stats;
    rs_result result;
    rs_magic_number sig_magic;

    basis_file = rs_file_open(poptGetArg(opcon), "rb", file_force);
    sig_file = rs_file_open(poptGetArg(opcon), "wb", file_force);

    // rdiff_no_more_args(opcon);

    if (!rs_hash_name || !strcmp(rs_hash_name, "blake2")) {
        sig_magic = RS_BLAKE2_SIG_MAGIC;
    } else if (!strcmp(rs_hash_name, "md4")) {
        sig_magic = RS_MD4_SIG_MAGIC;
    } else {
        rsyncx_usage("Unknown hash algorithm '%s'.", rs_hash_name);
        exit(RS_SYNTAX_ERROR);
    }
    if (!rs_rollsum_name || !strcmp(rs_rollsum_name, "rabinkarp")) {
        /* The RabinKarp magics are 0x10 greater than the rollsum magics. */
        sig_magic += 0x10;
    } else if (strcmp(rs_rollsum_name, "rollsum")) {
        rsyncx_usage("Unknown rollsum algorithm '%s'.", rs_rollsum_name);
        exit(RS_SYNTAX_ERROR);
    }

    result =
        rs_sig_file(basis_file, sig_file, block_len, strong_len, sig_magic,
                    &stats);

    rs_file_close(sig_file);
    rs_file_close(basis_file);
    if (result != RS_DONE)
        return result;

    if (show_stats)
        rs_log_stats(&stats);

    // printf("calculate time: %f\n", (double)stats.calculate_time / CLOCKS_PER_SEC);
    // printf("Total read bytes: %d\n", stats.in_bytes);
    // printf("Total write bytes: %d\n", stats.out_bytes);

    return result;
}

static rs_result rsyncx_delta(poptContext opcon) {
    FILE *sig_file, *new_file, *delta_file;
    char const *sig_name;
    rs_result result;
    rs_signature_t *sumset;
    rs_stats_t stats;

    if (!(sig_name = poptGetArg(opcon))) {
        rsyncx_usage("Usage for delta: "
                    "rdiff [OPTIONS] delta SIGNATURE [NEWFILE [DELTA]]");
        exit(RS_SYNTAX_ERROR);
    }

    sig_file = rs_file_open(sig_name, "rb", file_force);
    new_file = rs_file_open(poptGetArg(opcon), "rb", file_force);
    delta_file = rs_file_open(poptGetArg(opcon), "wb", file_force);

    // rdiff_no_more_args(opcon);

    result = rs_loadsig_file(sig_file, &sumset, &stats);
    if (result != RS_DONE)
        return result;

    if (show_stats)
        rs_log_stats(&stats);

    if ((result = rs_build_hash_table(sumset)) != RS_DONE)
        return result;

    result = rs_delta_file(sumset, new_file, delta_file, &stats);

    rs_file_close(delta_file);
    rs_file_close(new_file);
    rs_file_close(sig_file);

    if (show_stats) {
        rs_signature_log_stats(sumset);
        rs_log_stats(&stats);
    }

    // printf("calculate time: %f\n", (double) (stats.calculate_time - stats.io_time) / CLOCKS_PER_SEC);
    // printf("Total read bytes: %d\n", stats.in_bytes);
    // printf("Total write bytes: %d\n", stats.out_bytes);

    rs_free_sumset(sumset);

    return result;
}

static rs_result rsyncx_patch(poptContext opcon) {
    /* patch BASIS [DELTA [NEWFILE]] */
    FILE *basis_file, *delta_file, *new_file;
    char const *basis_name;
    rs_stats_t stats;
    rs_result result;

    if (!(basis_name = poptGetArg(opcon))) {
        rsyncx_usage("Usage for patch: "
                    "rdiff [OPTIONS] patch BASIS [DELTA [NEW]]");
        exit(RS_SYNTAX_ERROR);
    }

    basis_file = rs_file_open(basis_name, "rb", file_force);
    delta_file = rs_file_open(poptGetArg(opcon), "rb", file_force);
    new_file = rs_file_open(poptGetArg(opcon), "wb", file_force);

    // rdiff_no_more_args(opcon);

    result = rs_patch_file(basis_file, delta_file, new_file, &stats);

    rs_file_close(new_file);
    rs_file_close(delta_file);
    rs_file_close(basis_file);

    if (show_stats)
        rs_log_stats(&stats);

    // printf("Total read bytes: %d\n", stats.in_bytes);
    // printf("Total write bytes: %d\n", stats.out_bytes);

    return result;
}

static rs_result rsyncx_action(poptContext opcon) {
    const char *action;

    action = poptGetArg(opcon);
    if(!action)
        return RS_SYNTAX_ERROR;
    else if (isprefix(action, "signature")) {
        clock_t start, end;
        start = clock();
        rs_result result = rsyncx_signature(opcon);
        end = clock();
        printf("signature time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
        return result;
    }
        // return rsyncx_signature(opcon);
    else if (isprefix(action, "delta")) {
        clock_t start, end;
        start = clock();
        rs_result result = rsyncx_delta(opcon);
        end = clock();
        printf("delta time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
        return result;
    }
        // return rsyncx_delta(opcon);
    else if (isprefix(action, "patch")) {
        clock_t start, end;
        start = clock();
        rs_result result = rsyncx_patch(opcon);
        end = clock();
        printf("patch time: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
        return result;
    }
        // return rsyncx_patch(opcon);

    rsyncx_usage
        ("You must specify an action: `signature', `delta', or `patch'.");
    exit(RS_SYNTAX_ERROR);
}

int main(const int argc, const char *argv[]) {

    /* Initialize opts at runtime to avoid unknown address values. */
    const struct poptOption opts[] = {
        {"verbose", 'v', POPT_ARG_NONE, 0, 'v'},
        {"version", 'V', POPT_ARG_NONE, 0, 'V'},
        {"input-size", 'I', POPT_ARG_INT, &rs_inbuflen},
        {"output-size", 'O', POPT_ARG_INT, &rs_outbuflen},
        {"hash", 'H', POPT_ARG_STRING, &rs_hash_name},
        {"rollsum", 'R', POPT_ARG_STRING, &rs_rollsum_name},
        {"help", '?', POPT_ARG_NONE, 0, 'h'},
        {0, 'h', POPT_ARG_NONE, 0, 'h'},
        {"block-size", 'b', POPT_ARG_INT, &block_len},
        {"sum-size", 'S', POPT_ARG_INT, &strong_len},
        {"statistics", 's', POPT_ARG_NONE, &show_stats},
        {"stats", 0, POPT_ARG_NONE, &show_stats},
        {"gzip", 'z', POPT_ARG_NONE, 0, OPT_GZIP},
        {"bzip2", 'i', POPT_ARG_NONE, 0, OPT_BZIP2},
        {"force", 'f', POPT_ARG_NONE, &file_force},
        {0}
    };

    poptContext opcon;
    rs_result result;

    opcon = poptGetContext("rsyncxt", argc, argv, opts, 0);
    rsyncx_options(opcon);
    result = rsyncx_action(opcon);

    if(result != RS_DONE)
        fprintf(stderr, "rsyncxt: Failed, %s.\n", rs_strerror(result));

    poptFreeContext(opcon);
    return result;
}
