#ifndef FASTCDC_H
#define FASTCDC_H

#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/time.h>
#include <zlib.h>
#include <openssl/md5.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <libgen.h>
#include <unordered_map>

#include "uthash.h"
#include "ring_buffer.h"

#define SymbolCount 256
#define SeedLength 64
#define CacheSize 1024 * 1024 * 1024

#define ORIGIN_CDC 1
#define ROLLING_2Bytes 2
#define NORMALIZED_CDC 3
#define NORMALIZED_2Bytes 4

// Rolling2Bytes Mask
static uint32_t FING_GEAR_08KB_ls = 0xd9300353 << 1;
static uint32_t FING_GEAR_02KB_ls = 0xd9000353 << 1;
static uint32_t FING_GEAR_32KB_ls = 0xd9f00353 << 1;

static uint64_t FING_GEAR_08KB_ls_64 = 0x0000d93003530000 << 1;
static uint64_t FING_GEAR_02KB_ls_64 = 0x0000d90003530000 << 1;
static uint64_t FING_GEAR_32KB_ls_64 = 0x0000d9f003530000 << 1;
static uint64_t FING_GEAR_08KB_64 = 0x0000d93003530000;

static uint64_t FING_GEAR_02KB_64 = 0x0000d90003530000;
static uint64_t FING_GEAR_32KB_64 = 0x0000d9f003530000;

/* global variants */
// static struct timeval tmStart, tmEnd;
// static struct chunk_info *users = NULL;

// static float totalTm = 0;

extern uint64_t LEARv2[256];
extern int chunk_dist[30];
extern uint32_t g_global_matrix[SymbolCount];
extern uint32_t g_global_matrix_left[SymbolCount];
extern uint32_t expectCS;
extern uint32_t Mask_15;
extern uint32_t Mask_11;
extern uint64_t Mask_11_64, Mask_15_64;

extern uint32_t MinSize;
extern uint32_t MinSize_divide_by_2;
extern uint32_t MaxSize;
static int sameCount = 0;
static int tmpCount = 0;
static int smalChkCnt = 0;  // record the number of small chunks (< 8KB)

// #define IO_PRINT 1
// #define FASTFP_TEST 2

struct one_cdc {
    uint64_t offset;
    uint64_t length;
    uint32_t weak_hash;
};

struct file_cdc {
    uint64_t chunk_num;
    struct one_cdc *cdc_array;
};

struct cdc_matched_chunks {
    /* record all the matched chunks' offsets and lengths array, and then the sha1 */
    uint64_t matched_chunks_num;
    struct matched_item *matched_item_array;
};

struct cdc_crc32 {
    uint64_t chunk_num;
    uint32_t *weak_hash;
};

#define ITEMNUMS 16

struct cdc_uthash {
    uint32_t weak_hash;
    struct ol *cdc_item_array;
    uint64_t item_nums;
    uint64_t remalloc;
    UT_hash_handle hh;
};

struct stats {
	uint64_t read_io_bytes;
	uint64_t write_io_bytes;
    clock_t cdc_stage_1;
    clock_t cdc_stage_2;
};

/* fastfp: <file offset, chunk length, fastfp> */
struct one_fastcdc {
    uint64_t offset;
    uint64_t length;
    uint64_t fastfp;
};

struct file_fastcdc {
    uint64_t chunk_num;
    struct one_fastcdc *fastcdc_array;
};

/* record one matched chunk's offset and length */
struct ol {
    uint64_t offset;
    uint64_t length;
};

struct matched_item {
    uint64_t item_nums;
    uint8_t hash[SHA_DIGEST_LENGTH];
    struct ol old_ol;
    struct ol *new_ol_array;
};

struct matched_item_rpc {
    uint64_t item_nums;
    uint8_t hash[SHA_DIGEST_LENGTH];
    struct ol old_ol;
    struct ol new_ol;
    bool end_of_stream;
};

struct old_chunks {
    uint64_t offset;
    uint64_t length;
    uint8_t hash[SHA_DIGEST_LENGTH];
};

struct matched_item_rpc_1 {
    uint64_t item_nums;
    uint32_t weak_hash;
    std::unordered_map<std::string, ol> sha_to_chunk_map;
    bool end_of_stream;
};


struct matched_chunks {
    /* record all the matched chunks' offsets and lengths array, and then the sha1 */
    uint64_t matched_chunks_num;
    struct matched_item *matched_item_array;
    /* record all the unmatched chunks' offsets and lengths array */
    uint64_t unmatched_chunks_num;
    struct ol *unmatched_item_array;
};

struct real_matched {
    struct ol old_ol;
    struct ol new_ol;
};

enum CMD_TYPE {
    CMD_LITERAL = 0,
    CMD_COPY = 1,
};

struct literal_cmd {
    uint8_t cmd;
    /* length of literal data */
    uint64_t length;
    /* new data to append */
    uint8_t *data;
} __attribute__((packed));

struct copy_cmd {
    uint8_t cmd;
    /* offset in the old file to begin copying data from */
    uint64_t offset;
    /* length of data to copy */
    uint64_t length;
} __attribute__((packed));

/* ===============Fastcdc ops=============== */

/* init function */
void fastCDC_init(void);

static struct one_fastcdc (*chunking) (unsigned char*p, uint64_t n);

/* origin fastcdc function */
struct one_fastcdc cdc_origin_64(unsigned char *p, uint64_t n);

/* fastcdc with once rolling 2 bytes */
struct one_fastcdc rolling_data_2bytes_64(unsigned char *p, uint64_t n);

/* normalized fastcdc */
struct one_fastcdc normalized_chunking_64(unsigned char *p, uint64_t n);

/* normalized fastcdc with once rolling 2 bytes */
struct one_fastcdc normalized_chunking_2bytes_64(unsigned char *p, uint64_t n);

/* ===============Hash table ops=============== */

#define ITEMNUMS 16

struct fastfp_item {
    uint64_t offset;
    uint64_t length;
};

struct fastcdc_uthash {
    uint64_t fastfp;
    struct fastfp_item *fastfp_item_array;
    uint64_t item_nums;
    uint8_t remalloc;
    UT_hash_handle hh;
};

void fastcdc_build_uthash(struct fastcdc_uthash **hash_table, struct one_fastcdc *fastcdc_array,
                        uint64_t chunk_num);

void fastcdc_delete_uthash(struct fastcdc_uthash **hash_table);

void fastcdc_print_uthash(struct fastcdc_uthash **hash_table);

/* ===============Fastcdc sync ops=============== */

struct file_fastcdc* run_fastfp_1(int fd, char *map, int chunking_method);

int run_fastfp_2(char *ori_file_path, char *write_file_path, int chunking_method, struct stats *stats);

struct file_fastcdc* read_fastfp(char *fastfp_file_path, struct stats *stats);

/* calculate sha1 and build matched amd unmatched chunks */
struct matched_chunks* compare_fastfp(int fd, struct file_fastcdc *old_ff,
                                      struct file_fastcdc *new_ff);

/* write matched_chunks to "<file_path>-fastcdc-sha1" */
int write_matched(struct matched_chunks *mc, char *ori_file_path, struct stats *stats);

struct matched_chunks* read_matched(char *sha1_file_path, struct stats *stats);

void compare_sha1(struct matched_chunks *mc, int fd);

int compare_offset(const void *a, const void *b);

/* get real matched chunks and sort them by new file's offset */
struct real_matched* process_matched_chunks(struct matched_item *mc, uint64_t matched_chunks_num, uint64_t *real_nums);

/* append literal or copy command to delta file */
int append_literal_cmd_fd(int delta_fd, int new_fd, uint64_t offset, uint64_t length);

int append_copy_cmd(int delta_fp, uint64_t offset, uint64_t length);

/* generate delta file "<file_path>-fastcdc-delta" from matched_chunks */
int generate_delta(struct matched_chunks *mc, int new_fd, char *delta_file_path);

/* generate patch file "<ori_file_path>-fastcdc-patch" from delta file */
int fastcdc_patch_delta(char *ori_file_path, char* delta_file_path, char* out_file_path);

// predefined Gear Mask
static uint64_t GEARv2[256] = {
    0xdc377e207d3c5d43, 0x626790b237a4ab52, 0xfad9bf3a472cfe4d,
    0xa2a6bc5395bbce52, 0xce0a8e4ef2f3ee3f, 0xb4b5b36cf31b4d66,
    0x468f4da9d12660f7, 0x9005e918384c13d0, 0x29ba77f861e74697,
    0x8237cf1734b2c668, 0xee06f9f1df6ced7b, 0x142936b9399add6a,
    0xcb3c0a27878d6de5, 0x49b827acbe6d77ac, 0x65903aad1d6c1e9f,
    0x7a7c66f221cf20ca, 0x9a0f5565415a795,  0xbe5a882391e5527e,
    0x14d2a06077f8339d, 0xcad2bce34bf19652, 0x5541cc588464c621,
    0xc44d981a4aa21e70, 0xb5f4dbb200e75029, 0xa0fc23f2eef70334,
    0xfe36dbfaed334ef3, 0xdd1b785765b2bb6,  0xcc3461a160502f47,
    0x64b1c122c7083b1a, 0x388e6a418b6fd359, 0x65809585f16ab490,
    0x3eb1ec4b9577e4c7, 0xd80e411261617dd8, 0x7da08fd8df71e169,
    0xa20af93edb933a86, 0x59fb1b86e1ff5415, 0x5b596e7a23e7af9a,
    0x50a905ffc0a402c3, 0x3a8cefabefd9dbbe, 0xc9762597ccab4a09,
    0xd8de3774872f3efe, 0xc9f1075d9ebc3c51, 0x3b55ac8bfcf8f51e,
    0xe4e88fb0fa555bd1, 0xfa725ab5e019900a, 0x4be7597f3fcb499d,
    0xe108b91410eb4788, 0xb9f7fc4896cbf4c3, 0x8cdaddac2fc852ee,
    0x5c1d3439307f3d03, 0x60e5ae5e93c82d50, 0x340d48d6be8c5b09,
    0x9a02c3be5b6c05e8, 0x56014afc31084d83, 0x85b888f604ea56f2,
    0x5cb1a8a10079cb23, 0xc52b57c63ff9a0ca, 0xf35659fc64e0143,
    0xbdf325035c594f38, 0xd1f8101a35320afd, 0xe899458626d703f8,
    0xbf97f530ac049837, 0x895021a6620ae70,  0x948596cd24280401,
    0x471f6accca7627f4, 0x4a129ff1164598bd, 0x71405c568bef229e,
    0x4fda2755c3737887, 0x909a3cc70ee44b32, 0xd1a8a3c3dc9fd44d,
    0x5ebb36a043ede1ca, 0xf6a89a10d5e68b53, 0xd9bf7a1aaa5016b2,
    0x71030478353d66cb, 0x6806d8ba045b5ae0, 0xa7672b4808466f67,
    0xf93e0da0d5011f0a, 0xa8aeeeec63465029, 0xe958e539a532a76e,
    0xfd8bf78ab9628da1, 0xca1eb1fe8a769e6a, 0x2a96875d6c8f9257,
    0x501c523d559d7d12, 0x9b7e72aa3d7f1a65, 0x25554c1d398077a,
    0xcdd8af6cd9c471e1, 0x39bea30bd9a49e,   0x233b737e0eeee721,
    0xd6d57c6896b14ed4, 0x8dabe80e8c70e265, 0x1b859ed8c291ddbc,
    0x6b3385f41e0a598b, 0xcf05250d14390e94, 0xc58d5e7d9c8b9f73,
    0x49a5c7a9ba27febc, 0x4841a129f0804005, 0xe7ffa313ce3144a,
    0x13d8768f158b32cb, 0x5d4a2c0fe9cd7afe, 0xe5d0ae6ab568df33,
    0xd5e94a4243d9b506, 0xc6dc1d3da6a23d23, 0xf3b4295d0dd364f2,
    0x57750173991256b5, 0x119097313522fa6a, 0xbeecd02c90273c43,
    0xef69efdc30e9077e, 0xcf28f3bbba364c83, 0xc8b80c742bfdd966,
    0x83f12924c9400e15, 0xa35b3222d11d583e, 0x9c9a9d426cde5fdd,
    0xf298ed021e76023a, 0xb9e8b29602ced7f3, 0x853af80c4f919742,
    0x505b7d96bf01b253, 0xa318b2bff19ed50c, 0x5308029ce76f358f,
    0x9a398f26b33f24a,  0x74595977a450ca7b, 0x4e8468ce85680390,
    0x2436fba706b7bf67, 0xfc0923f5563e8424, 0x5fcbfacf4f88506b,
    0xbe722684e90680f2, 0x1119ab5f71bd737d, 0xc739c71894e34ba8,
    0xcd822b0cb4e2e159, 0x67b583610a0410e0, 0xae18b6eb024bdfdb,
    0x865951f3e76f5834, 0x18eebfb065ebcb59, 0xd35f0999dd5e5b00,
    0x241ad24ae452fe07, 0x942b4c3c79dd1dbe, 0x99b14f06198c22a7,
    0xe2987fcf99376312, 0x844daa9a945c9067, 0xfa234ecf470184dc,
    0x6d97ccd39c1eb593, 0x8bf12e40249118d0, 0x923f72bcdb934d2f,
    0x69bd5c907fc76c8e, 0xe5827868949fc4f3, 0x4f5d13c3d6e20e0a,
    0xb3646a9d0111233,  0xf0af68353b17c0c8, 0xc920f56447f90673,
    0x8960c8168641fe16, 0xfee91d454a19219,  0xb25b446e135cf6b2,
    0x761b04f2362314f9, 0xa151afe2fb1ff5be, 0x2e973d6af5de4037,
    0x485c4501258ab54c, 0xf21bd1e05d869951, 0xd79097aaa1050314,
    0x2b5e8c12e04ff4f9, 0x4e43a881e78d9764, 0x16d02eca685abdab,
    0x7913757d06ccfaec, 0x513242305e9af1ef, 0xc847965583801b62,
    0x8862452b0de8c5e5, 0xce5ae051740dea5a, 0x1a028ca4bb2875a3,
    0x5680bba4aad7ffa6, 0x324d2adfb43a331b, 0xe456b7b1c0301b68,
    0x7801a00c795d859d, 0x41bdd48db6ae14a,  0x5fa8107e14c841fb,
    0xd0e4bdea28bef85c, 0x77bdb5eb30614b89, 0xdefa9fd302bbd858,
    0x8a54bfa54688dad,  0x682ec11a915f0980, 0xc4af0b1c0ffec719,
    0xf76fd41604e89104, 0xeb01bf3d9ced6817, 0xa87180c091474c6e,
    0x351bdb3557277969, 0xee81b4ce09b723aa, 0x3bd353a36b05adb,
    0x7bcb44892055af78, 0xadfb4e960a0ca951, 0x2273bad9b4ac3d74,
    0xebb4454444aaa94b, 0xe686846acad641a8, 0x6a6c096404d1c7a3,
    0x3d857f91fc5f6232, 0x5c63557a89fa3a27, 0x3ee7bc50de6d3e04,
    0x2490253782ef8a57, 0xad4d59ff8fa5f4c,  0x1c01a62b4c726533,
    0x9ef66ba35cb5ab2e, 0x4c47d62317d71cdf, 0x2a40c557d5a3f2a0,
    0xf758338d53442abd, 0x1eaefcd073bcdd10, 0xb9833e0eb50e8fd7,
    0x8c966374151a67b6, 0x3edf7efe33cdfd7b, 0x5a2c4a1a310be558,
    0x69e43fcb628fbeb7, 0x99bd77ef54c90d36, 0xbab1c4f59f066e65,
    0x797fd05581e14764, 0x75de5603975e3ea9, 0x11ef41165111c73c,
    0xb7ddf0821a4031f5, 0x3eec9e62cf45d24c, 0xcda86289d51cd2c7,
    0x127483edee743fae, 0x4c62baba754703cf, 0xcd8d463bb2e9583a,
    0xaa38a80cc6c609f3, 0x5f97356ca50a5986, 0x220a1b6bc88caeb,
    0x20420bdd061f48c4, 0xb0e0c20838c35c57, 0xeddd78004259c3d2,
    0xb7b854be9806aa3b, 0xa2256b1fc3bd00fe, 0x3ea3e4597d23917d,
    0xd952cb1a7656852e, 0x89e052b4e095f921, 0x4be913d1c4f79a82,
    0x9d36a507aa427257, 0x69b7818b57dbbf8,  0x21102213d2cae2cd,
    0x27ec5888afbdb47e, 0x4f16a4e8c63e27f9, 0x250a3857af744546,
    0x3e97bbeec0fcabdd, 0x4b6c65f8ca7fd632, 0xa91fce0f23e1be61,
    0x5050c5a688052206, 0x9e2e3f1a6b85328f, 0xe09ec18b956f5fea,
    0x4000f41b1b5b25e7, 0x3fbea3faba569084, 0xe471347f40647f65,
    0xda7abaa5f3caf2c8, 0x189201a4940001f9, 0x29e183e37d7da328,
    0xd2ae28418f093e67, 0x8e5cb797039be80e, 0x41604277260a071d,
    0x11edac1b01696b0e, 0x6e27fc4394cebee3, 0xeeffd1b2b382a2c0,
    0x2443385d4c4e195,  0x2ee724f1ad94f68e, 0xa4491e380c5cf7b,
    0xeb48a728aaf18d2e,
};

static constexpr uint32_t gear32[256] = {
    0xcab06edf, 0xb2718138, 0x3c224673, 0x3b9cf4f3, 0x99309a2f, 0x4cae6426,
    0x5cd1268b, 0xfa8d5e6e, 0x3dce9096, 0x03f6d1ba, 0x10cbd5c6, 0x7a32df70,
    0x5caaf980, 0x1ee50161, 0xdb3e2adf, 0xdaa1b79b, 0x8a876bdb, 0x55214dcf,
    0x033ce45c, 0x93da2d58, 0x2c897e9b, 0x7ca38bce, 0x6ba9c6df, 0x644f3827,
    0x17919e09, 0x98991c4f, 0xb022e20c, 0xaeed89e5, 0xac46f0a2, 0x77e8ab7c,
    0x80cdb866, 0x1cf8a455, 0x342e8a7c, 0x82307545, 0x685c10bf, 0xf4b4db0d,
    0xd583f695, 0xef3be7f8, 0x6f443b74, 0xfb536307, 0xd1eebf07, 0x3fc4cbff,
    0x9c56a01f, 0x0c876401, 0x7582b5a4, 0xb67e02d9, 0xf31f1d4a, 0x308e0bfc,
    0xc2fbe865, 0x189ff266, 0xe9301f82, 0x0c99f8f2, 0xb536b229, 0xf176078b,
    0x7e638b7f, 0xb1b17b3b, 0xdc699078, 0xee113abe, 0xe05387c9, 0x834b5fb3,
    0x6577e854, 0x46310ed6, 0xe9095a8f, 0x0666ba24, 0x6f3e64d9, 0x60a137c6,
    0x00a3fe71, 0x252827d0, 0xc968a79d, 0x71adf1c7, 0xb90b26df, 0xc0b76174,
    0x53a4a968, 0x1d8cde87, 0xee076527, 0x78ada3ed, 0x2222a4cf, 0x0f20e8b1,
    0x52661029, 0x4ee67246, 0x22f83593, 0xc06b6d72, 0xe9780131, 0x46aa9013,
    0xb0192122, 0xa88b381f, 0x3b884ca7, 0x9e1188b8, 0x28e02253, 0xa19d3fc6,
    0xea459915, 0xb5b9a788, 0x96428060, 0x753524b8, 0x61c9c992, 0x6ba735d4,
    0x66ab303e, 0xbcbdd2c2, 0xe3df7ac9, 0x2f0cf65d, 0xcdf98e52, 0xb64160e8,
    0x6b8be972, 0x45602f72, 0xcbeb420e, 0xd9a2bd46, 0xb615d4a4, 0x1cfc7f69,
    0x603689d5, 0xc3bcd0d8, 0xc4d8da81, 0xa700392a, 0x27e3a0be, 0x3e7122fa,
    0x9f4ff2d6, 0x3ab159c1, 0xa3b1cc44, 0x54d2060c, 0x9f664a53, 0xb7933a53,
    0x17e0a83d, 0xab53f0f6, 0xfb54c682, 0xc2dce1fe, 0xb728b96c, 0x27a24073,
    0x35cd89cd, 0x1626c9a9, 0x9dcf73fd, 0x2a40ad38, 0x321c7bf2, 0x859f9ad2,
    0xd12d993f, 0xcb56ee3c, 0xf95e36dc, 0x8ada584b, 0x2868e9bc, 0xe2f137ee,
    0xa7ba3cae, 0xeb331d08, 0x2a2e1fc3, 0x13ed8950, 0x707abf0e, 0xf6c84db8,
    0xbe1b3e9f, 0x8a98a6ef, 0xa829daf1, 0x8f9fd9f8, 0x1d8002fb, 0xe07544a4,
    0xd69cb989, 0x030c29c2, 0x4f0e4227, 0x2b843c5a, 0x61d649fa, 0x24a23275,
    0x29ab7954, 0x1a977796, 0xafc840bb, 0x68ea74e9, 0x51e18221, 0x7e7aacb9,
    0xd83aac74, 0x16f3ffb4, 0xa1822460, 0x796e4267, 0xce57a57f, 0xdf15a7ee,
    0xf6098f14, 0x6bb45abd, 0x51933c35, 0x792d3f18, 0x4872d2de, 0xe66a579c,
    0x5750ffa9, 0x149d5472, 0x57d2e4ac, 0x9b2030bd, 0xa6befac0, 0x7eb0fa7d,
    0x5288b8de, 0xfd749b9c, 0x5389ae25, 0x90a31d56, 0x07acafbe, 0x9ffa7e2c,
    0x19a42631, 0xbc581a52, 0xc2517ad6, 0xe437de30, 0xd75eafd7, 0x8397f5ef,
    0x894d0064, 0xeae51be9, 0xa0973cf4, 0xd09dd0df, 0x654de33c, 0x99698bf2,
    0xb2be2b5c, 0x7df281a9, 0xdc5bdac7, 0xb8bc6817, 0xc2b8ac02, 0x6755088b,
    0x42fdf274, 0xd758e0a0, 0x0fe0775a, 0x3b089ae3, 0x1302b17c, 0xbbf11915,
    0x30f3ad8f, 0x8a38175b, 0x05ddabe9, 0x6647ac44, 0x49570ac5, 0x6ad85643,
    0x6062344e, 0xf9515337, 0x3ff407ae, 0x8ff0dc25, 0x2e047222, 0x3dab32fe,
    0x70899f3f, 0x594402c4, 0x7bdb81fd, 0xb93110d4, 0xe15de0ff, 0x7265b35e,
    0x0ffbffbd, 0x234ab621, 0x1ea74ed8, 0x82caa7b4, 0x3fe7fa4f, 0xa9ab690b,
    0x82e8993e, 0xa2d35adf, 0xf87827c5, 0x00172b3e, 0xa284d80b, 0x8d536c67,
    0xd63cb52d, 0xc6db6dbb, 0x523e1ba5, 0x557c6536, 0x4168f166, 0xd7acfd41,
    0xde089e30, 0xbf167903, 0x551a3200, 0xa330b700, 0x917e3ebf, 0x5a794e62,
    0xe44d3356, 0x9fcd9417, 0x30eb9b8b, 0x6e33ef51};

static constexpr uint64_t gear64[256] = {
    0x651748f5a15f8222, 0xd6eda276c877d8ea, 0x66896ef9591b326b,
    0xcd97506b21370a12, 0x8c9c5c9acbeb2a05, 0xb8b9553ee17665ef,
    0x1784a989315b1de6, 0x947666c9c50df4bd, 0xb3f660ea7ff2d6a4,
    0xbcd6adb8d6d70eb5, 0xb0909464f9c63538, 0xe50e3e46a8e1b285,
    0x21ed7b80c0163ce0, 0xf209acd115f7b43b, 0xb8c9cb07eaf16a58,
    0xb60478aa97ba854c, 0x8fb213a0b5654c3d, 0x42e8e7bd9fb03710,
    0x737e3de60a90b54f, 0x9172885f5aa79c8b, 0x787faae7be109c36,
    0x86ad156f5274cb9f, 0x6ac0a8daa59ee1ab, 0x5e55bc229d5c618e,
    0xa54fb69a5f181d41, 0xc433d4cf44d8e974, 0xd9efe85b722e48a3,
    0x7a5e64f9ea3d9759, 0xba3771e13186015d, 0x5d468c5fad6ef629,
    0x96b1af02152ebfde, 0x63706f4aa70e0111, 0xe7a9169252de4749,
    0xf548d62570bc8329, 0xee639a9117e8c946, 0xd31b0f46f3ff6847,
    0xfed7938495624fc5, 0x1ef2271c5a28122e, 0x7fd8e0e95eac73ef,
    0x920558e0ee131d4c, 0xce2e67cb1034bcd1, 0x6f4b338d34b004ae,
    0x92f5e7271cf95c9a, 0x12e1305a9c558342, 0x1e30d88013ad77ae,
    0x09acc1a57bbb604e, 0xaf187082c6f56192, 0xd2e5d987f04ac6f0,
    0x3b22fca40423da70, 0x7dfba8ce699a9a87, 0xe8b15f90ea96bd2a,
    0xcda1a1089cc2cbe7, 0x72f70448459de898, 0x1ab992dbb61cd46e,
    0x912ad04becbb29da, 0x98c6bb3aa3ce09ed, 0x6373bd2e7a041f3a,
    0x1f98f28bd178c53a, 0xe6adbc82ba5d9f96, 0x7456da7d805cbe01,
    0xd673662dcc135eeb, 0xb299e26eaadcb311, 0x2c2582172f8114af,
    0xeded114d7f623da6, 0xb3462a0e623276e4, 0x3af752be3d34bfaa,
    0x1311ccc0a1855a89, 0x0812bbcecc92b2e4, 0x9974b5747289f2f5,
    0x3a030eff770f2026, 0x52462b2aa42a847a, 0x2beaa107d15a012b,
    0x0c0035e0fe073398, 0x4f2f9de2ac206766, 0x5dd51a617c291deb,
    0x1ac66905652cc03b, 0x11067b0947fc07a1, 0x02b5fcd96ad06d52,
    0x74244ec1aa2821fd, 0xf6089e32060e9439, 0xd8f076a33bcbf1a7,
    0x5162743c755d8d5e, 0x8d34fc683e4e3d06, 0x46efe9b21a0252a3,
    0x4631e8d0109c6145, 0xfdf7a14bc0223957, 0x750934b3d0b8bb1e,
    0x2ecd1b3efed5ddb9, 0x2bcbd89a83ccfbce, 0x3507c79e58dd5886,
    0x5476a67ecd4a772f, 0xaa0be3856dd76405, 0x22289a358a4dd421,
    0xf570433f14503ad1, 0x8a9f440251a722c3, 0x77dd711752b4398c,
    0xbbd9edf9c6160a31, 0xb94b59220b23f079, 0xfdca3d75d2f33ccf,
    0xb29452c460c9e977, 0xe89afe2dd4bf3b02, 0x47ec6f32c91bfee4,
    0x1aab5ec3445706b8, 0x588bf4fa55334006, 0xe2290ca1e29acd96,
    0x3c49e189f831c37c, 0x6448c973b5177498, 0x556a6e09ba158de7,
    0x90b25013a8d9a067, 0xa4f2f7a50c58e1c4, 0x5e765e871008700e,
    0x242f5ae7738327af, 0xc1e6a2819cc5a219, 0xcb48d801fd6a5449,
    0xa208de2301931383, 0xde3c143fe44e39b0, 0x6bb74b09c73e4133,
    0xb5b1ed1b63d54c11, 0x587567d454ce7716, 0xf47ddbc987cb0392,
    0x87b19254448f03f1, 0x985fd00ec372fafa, 0x64b92ba521aa46e4,
    0xce63f4013d587b0f, 0xa691ae698726030e, 0xeaefbf690264e9aa,
    0x68edd400523eb152, 0x35d9353aa1957c60, 0x2e2c2d7a9cb68385,
    0xfc7549edaf43bf9e, 0x48b2adb23026e2c7, 0x3777cb79a024bcf9,
    0x644128f7c184102d, 0x70189d3ca4390de9, 0x085fea7986d4cd34,
    0x6dbe7626c8457464, 0x9fa41cfa9c4265eb, 0xdaa163a641946463,
    0x02f5c4bd9efa2074, 0x783201871822c3c9, 0xb0dfec499202bce0,
    0x1f1c9c12d84dccab, 0x1596f8819f2ed68e, 0xb0352c3e9fc84468,
    0x24a6673db9122956, 0x84f5b9e60b274739, 0x7216b28a0b54ac46,
    0xc7789de20e9cdca4, 0x903db5d289dd6563, 0xce66a947f7033516,
    0x3677dbc62307b2ca, 0x8d8e9d5530eb46ac, 0x79c4bad281bd93e2,
    0x287d942042068c36, 0xde4b98e5464b6ad5, 0x612534b97d1d21bf,
    0xdf98659772d822a1, 0x93053df791aa6264, 0x2254a8a2d54528ba,
    0x2301164aeb69c43d, 0xf56863474ac2417f, 0x6136b73e1b75de42,
    0xc7c3bd487e06b532, 0x7232fbed1eb9be85, 0x36d60f0bd7909e43,
    0xe08cbf774a4ce1f2, 0xf75fbc0d97cb8384, 0xa5097e5af367637b,
    0x7bce2dcfa856dbb2, 0xfbfb729dd808c894, 0x3dc8eba10ad7112e,
    0xf2d1854eedce4928, 0xb705f5c1aebd2104, 0x78fa4d004417d956,
    0x9e5162660729f858, 0xda0bcd5eb9f91f0e, 0x748d1be11e06b362,
    0xf4c2be9a04547734, 0x6f2bcd7c88abdf9a, 0x50865dafdfd8a404,
    0x9d820665691728f0, 0x59fe7a56aa07118e, 0x4df1d768c23660ec,
    0xab6310b8edfb8c5e, 0x029b47623fc9ffe4, 0x50c2cca231374860,
    0x0561505a8dbbdc69, 0x8d07fe136de385f3, 0xc7fb6bb1731b1c1c,
    0x2496d1256f1fac7a, 0x79508cee90d84273, 0x09f51a2108676501,
    0x2ef72d3dc6a50061, 0xe4ad98f5792dd6d6, 0x69fa05e609ae7d33,
    0xf7f30a8b9ae54285, 0x04a2cb6a0744764b, 0xc4b0762f39679435,
    0x60401bc93ef6047b, 0x76f6aa76e23dbe0c, 0x8a209197811e39da,
    0x4489a9683fa03888, 0x2604ad5741a6f8d8, 0x7faa9e0c64a94532,
    0x0dbfee8cdae8f54e, 0x0a7c5885f0b76d4a, 0x55dfb1ac12e83645,
    0xedc967651c4938cc, 0x4e006ab71a48b85e, 0x193f621602de413c,
    0xb56458b71d56944f, 0xf2b639509a2fa5da, 0xb4a76f284c365450,
    0x4d3b65d2d2ae22f7, 0xbcc5f8303efca485, 0x8a044f312671aaea,
    0x688d69e89af0f57a, 0x229957dc1facede8, 0x2ed75c321073da13,
    0xf199e7ece5fcefef, 0x50c85b5c837a6c64, 0x71703c6e676bf698,
    0xc1b4eb52b1e5a518, 0x0f46a5e6c9cb68ca, 0xebb933688d69d7f7,
    0x5ab7404b8d1e3ef4, 0x261acc20c5a64a90, 0xb88788798adc718a,
    0x3e44e9b6bad5bc15, 0xf6bb456f086346bc, 0xd66e17e5734cbde1,
    0x392036dae96e389d, 0x4a62ceac9d4202de, 0x9d55f412f32e5f6e,
    0x0e1d841509d9ee9d, 0xc3130bdc638ed9e2, 0x0cd0e82af24964d9,
    0x3ec4c59463ba9b50, 0x055bc4d8685ab1bc, 0xb9e343c96a3a4253,
    0x8eba190d8688f7f9, 0xd31df36c792c629b, 0xddf82f659b127104,
    0x6f12dc8ba930fbb7, 0xa0aee6bb7e81a7f0, 0x8c6ba78747ae8777,
    0x86f00167eda1f9bc, 0x3a6f8b8f8a3790c9, 0x7845bb4a1c3bfbbb,
    0xc875ab077f66cf23, 0xa68b83d8d69b97ee, 0xb967199139f9a0a6,
    0x8a3a1a4d3de036b7, 0xdf3c5c0c017232a4, 0x8e60e63156990620,
    0xd31b4b03145f02fa};

#endif // FASTCDC_H
