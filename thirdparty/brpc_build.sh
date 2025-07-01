#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/brpc

# Check if the library is already built
if [ -f ./output/lib/libbrpc.so ]; then
    echo "libbrpc is already built"
    exit 0
fi

sh config_brpc.sh --headers=/usr/include --libs=/usr/lib
make -j$(nproc)