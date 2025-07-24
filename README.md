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
- `sudo apt install git cmake autoconf pkg-config libtool libcurl4-openssl-dev libssl-dev libpopt-dev libbz2-dev libb2-dev doxygen nasm build-essential libaio-dev zlib1g-dev libext2fs-dev texinfo libevent-dev libev-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev libgoogle-perftools-dev hwloc libgtest-dev libgmock-dev libfuse-dev libgsasl7-dev`

<!-- You can use the script `thirdparty/deps_install.sh` to install dependencies after cloning the repository. -->

#### Build

```bash
git clone https://github.com/skysync-project/skysync
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