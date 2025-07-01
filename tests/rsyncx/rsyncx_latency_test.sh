#!/bin/bash


# ./dump_test /mnt/sdb-ext4/rsync-des/README.md /mnt/sdb-ext4/rsync-des/README-digs
# ./delta_test delta /mnt/sdb-ext4/rsync-des/README-digs /mnt/sdb-ext4/rsync-des/README-v1.md /mnt/sdb-ext4/rsync-des/README-delta
# ./patch_test patch /mnt/sdb-ext4/rsync-des/README.md /mnt/sdb-ext4/rsync-des/README-output.md /mnt/sdb-ext4/rsync-des/README-delta
# ./rsyncx_latency_test.sh /mnt/sdb-ext4/rsync-des/LICENSES.chromium.html /mnt/sdb-ext4/rsync-des/LICENSES.chromium.html-v1

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
    srcdir="../../../build"

    dir_name=$(dirname $1)
    dir_dst=$dir_name/rsyncx
    base_name=$(basename $1)

    # Check if $dir_dst directory exists
    check_dir $dir_dst

    # Remove $input_file-digs file
    remove_file $dir_dst/$base_name-digs
    echo "receiver: signature $1 $dir_dst/$base_name-digs"
    $srcdir/dump_test $1 $dir_dst/$base_name-digs

    # Check if $input_file-digs file exists
    check_file $dir_dst/$base_name-digs
    # Remove $input_file-delta file
    remove_file $dir_dst/$base_name-delta
    echo "sender: delta $dir_dst/$base_name-digs $2 $dir_dst/$base_name-delta"
    $srcdir/delta_test delta $dir_dst/$base_name-digs $2 $dir_dst/$base_name-delta

    # Check if $input_file-delta file exists
    check_file $dir_dst/$base_name-delta
    # Remove $input_file-output file
    remove_file $dir_dst/$base_name-output
    echo "receiver: patch $1 $dir_dst/$base_name-delta $dir_dst/$base_name-output"
    $srcdir/patch_test patch $1 $dir_dst/$base_name-delta $dir_dst/$base_name-output
}

# Get the command-line arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <old-file> <new-file>"
    exit 1
fi

old_file=$1
new_file=$2

# Run the commands with the provided arguments
check_file $old_file
check_file $new_file

date=$(date +%Y-%m-%d)
data_dir="./test.data"

echo "Rsync-x test: $old_file $new_file" >> $data_dir/$date

run_command $old_file $new_file >> $data_dir/$date

echo "" >> $data_dir/$date
