# README

## Introduction

SkySync is a lightweight file sync tool with collaborative delta generation.

## Build From Source

### Requirements

- cmake >= 3.6
- librsync >= 2.3.4
- Intel ISA-L >= 2.30
- photonlibos >= 0.8.1
- mimalloc >= 2.1.7
- other dependencies:
    `sudo apt install git cmake autoconf pkg-config libtool libcurl4-openssl-dev libssl-dev libpopt-dev libbz2-dev libb2-dev doxygen nasm build-essential libaio-dev zlib1g-dev libext2fs-dev texinfo libevent-dev libev-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev libgoogle-perftools-dev hwloc libgtest-dev libgmock-dev libfuse-dev libgsasl7-dev`

You can also use the script `thirdparty/deps_install.sh` to install librsync, mimalloc, Intel ISA-L, and build brpc after cloning the repository.

### Build

```bash
git clone XXXX
cd skysync
git submodule update --init --recursive
sudo ./thirdparty/deps_install.sh
```

The whole project is built using CMake. You can build it by running the following commands:

```bash
cd src/skysync-f && protoc -I=. --cpp_out=. skysync.proto && cd ../..
cd src/dsync && protoc -I=. --cpp_out=. dsync.proto && cd ../..
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release/Debug ..
# cmake --build . --target clean
make -j8
```

You can run sync_client and sync_server by running the following commands:

```bash
./rsync_http_server
./rsync_http_client -basis_filename=<old_file> --new_filename=<new_file> --server_ip=<ip> --server_port=19876 --hw=0
```