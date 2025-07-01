#!/bin/bash
# https://oneapi-spec.uxlfoundation.org/specifications/oneapi/latest/elements/onetbb/source/nested-index

# Check if the library is already installed
if [ -f /usr/local/lib/libtbb.so ] || [ -f /usr/lib/libtbb.so ] || [ -f /usr/lib/x86_64-linux-gnu/libtbb.so ]; then
    echo "libtbb is already installed"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/oneTBB

sudo rm -r build
mkdir build && cd build
cmake -DTBB_TEST=OFF ..
cmake --build .
sudo cmake --install .