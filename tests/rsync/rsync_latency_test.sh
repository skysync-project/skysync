#!/bin/bash


# ./rsync_test --block-size=4096 signature /mnt/sdb-ext4/rsync-des/README.md /mnt/sdb-ext4/rsync-des/rdiff/README.md-digs
# ./rsync_test --block-size=4096 delta /mnt/sdb-ext4/rsync-des/rdiff/README.md-digs /mnt/sdb-ext4/rsync-des/README-v1.md /mnt/sdb-ext4/rsync-des/rdiff/README.md-delta
# ./rsync_test patch /mnt/sdb-ext4/rsync-des/README.md /mnt/sdb-ext4/rsync-des/rdiff/README.md-delta /mnt/sdb-ext4/rsync-des/rdiff/README.md-output
# ./rdiff_latency_test.sh /mnt/sdb-ext4/rsync-des/LICENSES.chromium.html /mnt/sdb-ext4/rsync-des/LICENSES.chromium.html-v1
# ./rdiff_latency_test.sh /mnt/sdb-ext4/rsync-des/blockbench /mnt/sdb-ext4/rsync-des/blockbench-v1

check_file() {
    if [ ! -f $1 ]; then
        echo "Error: $1 does not exist"
        exit 1
    fi
}

check_dir() {
    if [ ! -d $1 ]; then
        echo "Error: $1 does not exist"
        echo "Creating $1"
        mkdir $1
    fi
}

remove_file() {
    if [ -f $1 ]; then
        rm $1
    fi
}

run_command() {
    srcdir="../../build"

    dir_name=$(dirname $1)
    dir_dst=$dir_name/rsync
    base_name=$(basename $1)

    enable_perf=$3

    # Check if $dir_dst directory exists
    check_dir $dir_dst

    # Remove $input_file-digs file
    remove_file $dir_dst/$base_name-digs-rsync
    echo "server: signature $1 $dir_dst/$base_name-digs-rsync"
    if [ $enable_perf -eq 1 ]; then
        perf record -F1000 -g -o $dir_dst/digs_perf.data $srcdir/rsync_test --block-size=8192 --rollsum=rollsum signature $1 $dir_dst/$base_name-digs-rsync
    else
        $srcdir/rsync_test --block-size=8192 --rollsum=rollsum signature $1 $dir_dst/$base_name-digs-rsync
    fi

    # Check if $input_file-digs file exists
    check_file $dir_dst/$base_name-digs-rsync
    # Remove $input_file-delta file
    remove_file $dir_dst/$base_name-delta-rsync
    echo "client: delta $dir_dst/$base_name-digs $2 $dir_dst/$base_name-delta-rsync"
    if [ $enable_perf -eq 1 ]; then
        perf record -F1000 -g -o $dir_dst/delta_perf.data $srcdir/rsync_test --block-size=8192 --rollsum=rollsum delta $dir_dst/$base_name-digs-rsync $2 $dir_dst/$base_name-delta-rsync
    else
        $srcdir/rsync_test --block-size=8192 --rollsum=rollsum delta $dir_dst/$base_name-digs-rsync $2 $dir_dst/$base_name-delta-rsync
    fi

    # Check if $input_file-delta file exists
    check_file $dir_dst/$base_name-delta-rsync
    # Remove $input_file-output file
    remove_file $dir_dst/$base_name-output-rsync
    echo "server: patch $1 $dir_dst/$base_name-delta-rsync $dir_dst/$base_name-output-rsync"
    if [ $enable_perf -eq 1 ]; then
        perf record -F1000 -g -o $dir_dst/patch_perf.data $srcdir/rsync_test patch $1 $dir_dst/$base_name-delta-rsync $dir_dst/$base_name-output-rsync
    else
        $srcdir/rsync_test patch $1 $dir_dst/$base_name-delta-rsync $dir_dst/$base_name-output-rsync
    fi
}

# Get the command-line arguments
if [ $# -ne 3 ]; then
    echo "Usage: $0 <old-file> <new-file> <enable perf: 0/1>"
    exit 1
fi

old_file=$1
new_file=$2
enable_perf=$3

# Run the commands with the provided arguments
check_file $old_file
check_file $new_file

date=$(date +%Y-%m-%d)
data_dir="./test"

if [ ! -d $data_dir ]; then
    mkdir $data_dir
fi

echo "Rsync test: $old_file $new_file" >> $data_dir/$date

run_command $old_file $new_file $enable_perf >> $data_dir/$date

echo "" >> $data_dir/$date
