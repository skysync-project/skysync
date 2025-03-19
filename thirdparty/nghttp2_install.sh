#!/bin/bash

# Check if the library is already installed
if [ -f /usr/local/lib/libnghttp2.so ] || [ -f /usr/lib/libnghttp2.so ] || [ -f /usr/lib/x86_64-linux-gnu/libnghttp2.so ]; then
    echo "libnghttp2.so is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/nghttp2

git submodule update --init

autoreconf -i
automake
autoconf
./configure
make -j$(nproc)
sudo make install