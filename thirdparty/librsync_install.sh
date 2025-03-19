#!/bin/bash

# Check if the library is already installed
if [ -f /usr/local/lib/librsync.so ] || [ -f /usr/lib/librsync.so ] || [ -f /usr/lib/x86_64-linux-gnu/librsync.so ]; then
    echo "librsync is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/librsync

mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
sudo make install