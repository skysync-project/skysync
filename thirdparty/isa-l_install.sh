#!/bin/bash

# Check if the library is already installed
if [ -f /usr/lib/libisal.so ] || [ -f /usr/lib/x86_64-linux-gnu/libisal.so ] || [ -f /usr/local/lib/libisal.so ]; then
    echo "isa-l is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/isa-l

./autogen.sh
./configure
make -j$(nproc)
# make perfs
sudo make install