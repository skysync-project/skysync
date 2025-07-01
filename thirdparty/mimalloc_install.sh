#!/bin/bash

# Check if the library is already installed
if [ -f /usr/local/lib/libmimalloc.so.2.1 ] || [ -f /usr/lib/libmimalloc.so.2.1 ] || [ -f /usr/lib/x86_64-linux-gnu/libmimalloc.so.2.1 ]; then
    echo "libmimalloc.so.2.1 is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/mimalloc

# checkout to the version 2.1.7
git checkout v2.1.7

mkdir -p out/release
cd out/release
cmake ../..
make -j$(nproc)
sudo make install