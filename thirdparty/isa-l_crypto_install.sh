#!/bin/bash

# Check if the library is already installed
if [ -f /usr/lib/libisal_crypto.so ] || [ -f /usr/lib/x86_64-linux-gnu/libisal_crypto.so ] || [ -f /usr/local/lib/libisal_crypto.so ]; then
    echo "isa-l_crypto is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/isa-l_crypto

./autogen.sh
./configure
make -j$(nproc)
sudo make install