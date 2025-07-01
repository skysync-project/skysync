#!/bin/bash

# sudo ./fastcdc_test /mnt/sdb-ext4/rsync-des/fastcdc/LICENSES.chromium.html /mnt/sdb-ext4/rsync-des/fastcdc/LICENSES.chromium.html-v1 4
# sudo ./fastcdc_test /mnt/sdb-ext4/rsync-des/fastcdc/blockbench /mnt/sdb-ext4/rsync-des/fastcdc/blockbench-v1 4

check_file() {
    if [ ! -f $1 ]; then
        echo "Error: $1 does not exist"
        exit 1
    fi
}

run_command() {
    srcdir="../../../build"

    $srcdir/fastcdc_test $1 $2 $3
}

# Get the command-line arguments
if [ $# -ne 3 ]; then
    echo "Usage: $0 <old-file> <new-file> <chunking-method>"
    exit 1
fi

old_file=$1
new_file=$2

# Run the commands with the provided arguments
check_file $old_file
check_file $new_file

date=$(date +%Y-%m-%d)
data_dir="./test"

if [ ! -d $data_dir ]; then
    mkdir $data_dir
fi

echo "Fastcdc test: $old_file $new_file" >> $data_dir/$date

run_command $old_file $new_file $3 >> $data_dir/$date

echo "" >> $data_dir/$date

