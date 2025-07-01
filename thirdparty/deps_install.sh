#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

./librsync_install.sh
./mimalloc_install.sh
./isa-l_install.sh
./isa-l_crypto_install.sh
./brpc_build.sh
./liburing_install.sh
./photon_build.sh
./tbb_install.sh