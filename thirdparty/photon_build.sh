#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/photon

# Check if the library is already built
if [ -f ./build/output/libphoton.so ]; then
    echo "libphoton is already built"
    exit 0
fi

cmake -B build -D CMAKE_BUILD_TYPE=RelWithDebInfo -D PHOTON_ENABLE_URING=ON
cmake --build build -j 8