# README

## Introduction

This artifact accompanies our paper "SkySync: Accelerating File Synchronization with Collaborative Delta Generation". It contains the source code, datasets, and instructions to reproduce our experimental results.

## Experimental Setup

### Testbed

We conduct our experiments on two Alibaba Cloud Elastic Compute Service (ECS) instances. Each instance is equipped with a quad-core Intel Xeon 8269CY vCPU (2.5 GHz), 32 GB of memory, and a 1 TB, 300 MB/s cloud SSD backed by Elastic Block Storage (EBS). The instances run Ubuntu 22.04 with the Linux 5.15.0-71-generic kernel and use the BTRFS filesystem. Two instances are located in separate data centers, connected over WAN with an average network Round Trip Time (RTT) of 35ms and 500Mbps bandwidth.

### Datasets

Micro-benchmark dataset: [dataset1](https://drive.google.com/file/d/15LG597ucYeOmC-_DPGaii2lhcjmqo_DI/view?usp=sharing) and [dataset2](https://drive.google.com/file/d/1JfGl73FHaEaAyuqnym2lHn9zUU9Kf3us/view?usp=sharing).

Real-world datasets: We upload these datasets to [Zenodo](https://doi.org/10.5281/zenodo.15867392).

<!-- Due to privacy constraints and dataset size, the proprietary Chat and Nutsnap datasets are not included. Enwiki is publicly available and can be downloaded from [here](https://dumps.wikimedia.org/enwiki/). Other large datasets are currently exploring distribution methods due to size constraints. -->

### Build From Source

#### Requirements

- gcc == 11.4.0
- cmake >= 3.18
- librsync >= 2.3.4
- Intel ISA-L >= 2.30
- photonlibos >= 0.8.1
- mimalloc >= 2.1.7
- liburing == 2.3
- other dependencies:
    `sudo apt install git cmake autoconf pkg-config libtool libcurl4-openssl-dev libssl-dev libpopt-dev libbz2-dev libb2-dev doxygen nasm build-essential libaio-dev zlib1g-dev libext2fs-dev texinfo libevent-dev libev-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev libgoogle-perftools-dev hwloc libgtest-dev libgmock-dev libfuse-dev libgsasl7-dev`

You can also use the script `thirdparty/deps_install.sh` to install dependencies after cloning the repository.

#### Build

```bash
git clone skysync
cd skysync
git submodule update --init --recursive
sudo ./thirdparty/deps_install.sh
```

The whole project is built using CMake. You can build it by running the following commands:

```bash
cd src/skysync-f && protoc -I=. --cpp_out=. skysync.proto && cd ../..
cd src/dsync && protoc -I=. --cpp_out=. dsync.proto && cd ../..

mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

Upon successful compilation, all executables will be located in the `build/` directory.

### Run

#### Local Evaluation (Single-Machine)

First, you can run the core logic of each algorithm on a single machine using the provided test executables including `rsync_test`, `dsync_test`, `skysync_f_test` and `skysync_c_test`. These tests measure the performance without network overhead.

```bash
# Arg 1: Path to the old/basis file
# Arg 2: Path to the new file
# Arg 3: 0 for software-only, 1 for hardware acceleration
./rsync_test <old-file: 100MB> <new-file: 100MB-insert-8MB> <0 for software, 1 for hardware acceleration>
./dsync_test <old-file: 100MB> <new-file: 100MB-insert-8MB> <0 for software, 1 for hardware acceleration>
./skysync_f_test <old-file: 100MB> <new-file: 100MB-insert-8MB> <0 for software, 1 for hardware acceleration>
./skysync_c_test <old-file: 100MB> <new-file: 100MB-insert-8MB> <0 for software, 1 for hardware acceleration>
```

The following presents example test results for `dsync` and `skysync_c`, where "CDC" and "Calculate Strong Hash" represent calculating, "Compare Weak Hash" corresponds to the searching phase, and "Generate Delta" and "Patch Delta" represent the delta blocks generation and patching phases (others).

```bash
Server Serial CDC time,0.5
Client Serial CDC time,0.5
Server Compare Weak Hash time,0.3
Client Calculate Strong Hash time,0.2
Client Generate Delta time,0.04
Server Patch Delta time,0.06
```

The following presents example test results for `rsync` and `skysync_f`.

```bash
Server Signature generation completed in 0.3 seconds
Client Rolling and Delta generation completed in 0.9 seconds
Server Patch delta applied in 0.1 seconds
```

"Signature generation" corresponds to calculation overhead. "Rolling and Delta" phase on the client side is a mix of calculation and searching. Using `perf` to distinguish between searching and calculation overhead (install `perf` using `sudo apt install linux-tools-common linux-tools-generic linux-tools-$(uname -r)`):

```bash
perf record -F5000 -g ./rsync_test
perf report -F overhead,symbol
```

For `rsync`, `rs_signature_find_match` represents the searching phase. Others including `rs_delta_s_scan`, `rs_mdfour`, `blake2b_compress`, `RollsumUpdate` as well as related low-level operations like `memmove/memcpy` and page cache management, represent the calculation phase.

For `skysync_f`, we provide fine-grained timing measurements by enabling the `SEARCHING_TIME` macro (defined in `src/skysync-f/skysync_f_worker.cpp`). This separates the searching phase from the calculation phase. However, when conducting comparative benchmarks against other systems, this macro must be disabled to avoid introducing measurement overhead that could skew performance results.

**Functional**: All local executables execute without errors and produce outputs matching the provided examples. Patch files (*.patch) are generated alongside the old-file directory and are the same size as the corresponding new files.

**Results Reproduced**: The local performance results reported in the paper can be reproduced by running the local executables with the provided datasets for Fig.3(c)-(f), 7-11 and 13.

#### Network Evaluation (Client-Server)

You can continue to run the HTTP server on one machine and the client on another. On the machine acting as the server (which holds the old file version), start the appropriate HTTP server.

```bash
# Start the HTTP server
./rsync_http_server
```

The server will listen on port 19876 by default. The available servers are `rsync_http_server`, `dsync_http_server`, `skysync_f_http_server`, and `skysync_c_http_server`.

On the client machine (which holds the new file version), run the corresponding client to initiate sync. Note: The `--basis_filename` argument specifies the full path to the target file on the server. 

```bash
# Start the HTTP client to sync files.
./rsync_http_client --basis_filename=<old_file> --new_filename=<new_file> --server_ip=<ip> --server_port=19876 --hw=<0 or 1>
```

The available clients are `rsync_http_client`, `dsync_http_client`, `skysync_f_http_client`, and `skysync_c_http_client`.

**Functional**: All HTTP server and client executables run correctly and produce outputs consistent with the examples. On the server side, new files with extension `*.new` are generated; these match the size of the client's new files.

The server and client executables should produce detailed logs for analysis. An example of the log output is shown below:

```bash
# Log Info For Rsync HTTP Server
2025/07/14 16:37:09.977713|INFO |th=000055B1DA4B2E50|epoll.cpp:319|new_epoll_engine:Init epoll event engine: master
2025/07/14 16:37:09.977805|INFO |th=000055B1DA4B2E50|signal.cpp:292|sync_signal_init:signalfd initialized
2025/07/14 16:37:09.977881|INFO |th=000055B1DA4B2E50|rsync_http_server.cpp:370|main:Rsync HTTP Server started on port 19876
2025/07/14 16:37:13.136460|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:87|handle_request:Received request for target: /signature?file=/mnt/sync-test/100M/100MB&hw=1
2025/07/14 16:37:13.136512|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:123|handle_request:Handling /signature request for file: /mnt/sync-test/100M/100MB with hw=1
2025/07/14 16:37:13.136887|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:148|handle_request:Using hardware accelerated hashing algorithm
2025/07/14 16:37:13.398454|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:172|handle_request:Signature generation completed in 0.2619 seconds
2025/07/14 16:37:13.398602|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:196|handle_request:Signature generated and sent for file: /mnt/sync-test/100M/100MB (Request Key: /mnt/sync-test/100M/100MB_1752482233398480122)
2025/07/14 16:37:13.399129|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:87|handle_request:Received request for target: /ack?file=/mnt/sync-test/100M/100MB
2025/07/14 16:37:13.399141|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:286|handle_request:Handling /ack request
2025/07/14 16:37:13.399148|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:318|handle_request:Signature RTT (Server-side): 0.0007 seconds for request key: /mnt/sync-test/100M/100MB_1752482233398480122
2025/07/14 16:37:13.399163|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:332|handle_request:ACK processed for request key: /mnt/sync-test/100M/100MB_1752482233398480122
2025/07/14 16:37:15.539339|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:87|handle_request:Received request for target: /patch?file=/mnt/sync-test/100M/100MB
2025/07/14 16:37:15.539371|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:200|handle_request:Handling /patch request for file: /mnt/sync-test/100M/100MB
2025/07/14 16:37:15.808577|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:79|write_file_content:File /mnt/sync-test/100M/100MB.new written successfully
2025/07/14 16:37:15.808602|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:280|handle_request:Patch delta applied in 0.1859 seconds
2025/07/14 16:37:15.808614|INFO |th=00007FE05EE1EBC0|rsync_http_server.cpp:282|handle_request:Patch applied and new file saved to: /mnt/sync-test/100M/100MB.new
```

```bash
# Log Info For Rsync HTTP Client
2025/07/14 16:37:13.129804|INFO |th=000055C77776DE50|epoll.cpp:319|new_epoll_engine:Init epoll event engine: master
2025/07/14 16:37:13.129868|INFO |th=000055C77776DE50|signal.cpp:292|sync_signal_init:signalfd initialized
2025/07/14 16:37:13.129889|INFO |th=000055C77776DE50|rsync_http_client.cpp:35|perform_rsync_client_flow:Starting rsync client flow...
2025/07/14 16:37:13.129896|INFO |th=000055C77776DE50|rsync_http_client.cpp:47|perform_rsync_client_flow:Using hardware accelerated hashing algorithm
2025/07/14 16:37:13.129899|INFO |th=000055C77776DE50|rsync_http_client.cpp:53|perform_rsync_client_flow:Step 1: Requesting signature for /mnt/sync-test/100M/100MB from server...
2025/07/14 16:37:13.135951|INFO |th=000055C77776DE50|epoll.cpp:319|new_epoll_engine:Init epoll event engine: cascading
2025/07/14 16:37:13.135990|INFO |th=000055C77776DE50|epoll.cpp:319|new_epoll_engine:Init epoll event engine: cascading
2025/07/14 16:37:13.398561|INFO |th=000055C77776DE50|rsync_http_client.cpp:88|perform_rsync_client_flow:Received request key: /mnt/sync-test/100M/100MB_1752482233398480122
2025/07/14 16:37:13.399022|INFO |th=000055C77776DE50|rsync_http_client.cpp:107|perform_rsync_client_flow:Signature received.
2025/07/14 16:37:13.399051|INFO |th=000055C77776DE50|rsync_http_client.cpp:111|perform_rsync_client_flow:Step 1.5: Sending ACK to server for request key: /mnt/sync-test/100M/100MB_1752482233398480122
2025/07/14 16:37:13.439968|INFO |th=000055C77776DE50|rsync_http_client.cpp:137|perform_rsync_client_flow:ACK response: ACK_RECV
2025/07/14 16:37:13.439975|INFO |th=000055C77776DE50|rsync_http_client.cpp:140|perform_rsync_client_flow:ACK sent successfully
2025/07/14 16:37:13.439984|INFO |th=000055C77776DE50|rsync_http_client.cpp:145|perform_rsync_client_flow:Step 2: Generating delta for /mnt/sync-test/100M/100MB-insert-8MB...
2025/07/14 16:37:15.494391|INFO |th=000055C77776DE50|rsync_http_client.cpp:171|perform_rsync_client_flow:Rolling and Delta generation completed in 2.0543 seconds
2025/07/14 16:37:15.539159|INFO |th=000055C77776DE50|rsync_http_client.cpp:202|perform_rsync_client_flow:Delta generated.
2025/07/14 16:37:15.539169|INFO |th=000055C77776DE50|rsync_http_client.cpp:205|perform_rsync_client_flow:Step 3: Sending delta to server...
2025/07/14 16:37:15.539176|INFO |th=000055C77776DE50|rsync_http_client.cpp:211|perform_rsync_client_flow:Setting request body using set_body().
2025/07/14 16:37:15.622655|INFO |th=000055C77776DE50|rsync_http_client.cpp:241|perform_rsync_client_flow:Server response: ACK
2025/07/14 16:37:15.622663|INFO |th=000055C77776DE50|rsync_http_client.cpp:247|perform_rsync_client_flow:Delta RTT (Client-side): 0.0835 seconds
2025/07/14 16:37:15.622671|INFO |th=000055C77776DE50|rsync_http_client.cpp:249|perform_rsync_client_flow:Delta sent. Server responded with status code: 200
2025/07/14 16:37:15.622679|INFO |th=000055C77776DE50|rsync_http_client.cpp:252|perform_rsync_client_flow:Rsync client flow completed successfully.
2025/07/14 16:37:15.625933|INFO |th=000055C77776DE50|epoll.cpp:85|~EventEngineEPoll:Finish event engine: epoll
2025/07/14 16:37:15.625986|INFO |th=000055C77776DE50|epoll.cpp:85|~EventEngineEPoll:Finish event engine: epoll
2025/07/14 16:37:15.626502|INFO |th=000055C77776DE50|signal.cpp:327|sync_signal_fini:signalfd finished
2025/07/14 16:37:15.626507|INFO |th=000055C77776DE50|epoll.cpp:85|~EventEngineEPoll:Finish event engine: epoll
```

**Results Reproduced**: The network performance results (for Fig.12, 14-16) are derived from the logs generated by the client and server executables. These logs contain detailed, per-phase timing information for the entire sync process such as Signature generation (Signature RTT), Rolling and Delta generation (Delta RTT), and Patch delta.