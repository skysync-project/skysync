#!/bin/bash

if [ -f /usr/local/lib/liburing.so ] || [ -f /usr/lib/liburing.so ] || [ -f /usr/lib/x86_64-linux-gnu/liburing.so ]; then
    echo "liburing is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/liburing

# checkout to the version 2.3
git checkout liburing-2.3

./configure --cc=gcc --cxx=g++
make -j$(nproc)
make liburing.pc
sudo make install