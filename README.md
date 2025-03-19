# README

## Introduction

SkySync: Accelerating File Synchronization with Collaborative Delta Generation

## Build From Source

### Requirements

- cmake >= 3.6
- librsync >= 2.3.4
- Intel ISA-L >= 2.30
- photonlibos >= 0.8.1
- mimalloc >= 2.1.7
- other dependencies:
    `sudo apt install git cmake autoconf pkg-config libtool libcurl4-openssl-dev libssl-dev libpopt-dev libbz2-dev libb2-dev doxygen nasm build-essential libaio-dev zlib1g-dev libext2fs-dev texinfo libevent-dev libev-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev libgoogle-perftools-dev hwloc`

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
cd src/net && protoc -I=. --cpp_out=. echo.proto && cd ../..
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release/Debug ..
# cmake --build . --target clean
make -j8
```

You can run rpc_client and rpc_server by running the following commands:

```bash
openssl ecparam -name prime256v1 -genkey -noout -out key_ecdsa.pem
openssl req -subj '/CN=localhost' -x509 -key key_ecdsa.pem -out cert_ecdsa.pem -nodes -days 3650
./rpc_server_serial_test -ssl_key_file=./key_ecdsa.pem -ssl_cert_file=./cert_ecdsa.pem
./rpc_client_serial_test -filename=<file>
```