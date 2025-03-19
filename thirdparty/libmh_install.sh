#!/bin/bash

# Check if the library is already installed
if [ -f /usr/lib/libmicrohttpd.so ] || [ -f /usr/lib/x86_64-linux-gnu/libmicrohttpd.so ] || [ -f /usr/local/lib/libmicrohttpd.so ]; then
    echo "libmicrohttpd is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/libmicrohttpd

# checkout to the version 0.9.77
git checkout v0.9.77

./bootstrap
./configure
make -j$(nproc)
sudo make install