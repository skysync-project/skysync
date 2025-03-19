#!/bin/bash
# ./http_get_digs /mnt/sdb-ext4/rsync-des/rdiff/LICENSES.chromium.html 127.0.0.1:8888 rsync
# ./http_get_digs /mnt/sdb-ext4/rsync-des/rdiff/blockbench 127.0.0.1:8888 rsync

# ./http_post_delta /mnt/sdb-ext4/rsync-des/rdiff/LICENSES.chromium.html-delta 127.0.0.1:8888
# ./http_post_delta /mnt/sdb-ext4/rsync-des/rdiff/blockbench-delta 127.0.0.1:8888

# ./http_get_digs /mnt/sdb-ext4/rsync-des/rsyncx/LICENSES.chromium.html 127.0.0.1:8888 rsyncx
# ./http_get_digs /mnt/sdb-ext4/rsync-des/rsyncx/blockbench 127.0.0.1:8888 rsyncx

# ./http_post_delta /mnt/sdb-ext4/rsync-des/rsyncx/LICENSES.chromium.html-delta 127.0.0.1:8888
# ./http_post_delta /mnt/sdb-ext4/rsync-des/rsyncx/blockbench-delta 127.0.0.1:8888

# ./http_post_fastfp /mnt/sdb-ext4/rsync-des/fastcdc/blockbench-v1 127.0.0.1:8888

# valgrind --leak-check=yes ./http_receiver_test 8888

run_rdiff() {
    echo "Rdiff get digs: LICENSES.chromium.html"
    for i in {1..5}
    do
        $1/http_get_digs /mnt/sdb-ext4/rsync-des/rdiff/LICENSES.chromium.html $2 rsync
    done

    echo "Rdiff get digs: blockbench"
    for i in {1..5}
    do
        $1/http_get_digs /mnt/sdb-ext4/rsync-des/rdiff/blockbench $2 rsync
    done

    echo "Rdiff post delta: LICENSES.chromium.html"
    for i in {1..5}
    do
        $1/http_post_delta /mnt/sdb-ext4/rsync-des/rdiff/LICENSES.chromium.html-delta $2
    done

    echo "Rdiff post delta: blockbench"
    for i in {1..5}
    do
        $1/http_post_delta /mnt/sdb-ext4/rsync-des/rdiff/blockbench-delta $2
    done
}

run_rsyncx() {
    echo "Rsyncx get digs: LICENSES.chromium.html"
    for i in {1..5}
    do
        $1/http_get_digs /mnt/sdb-ext4/rsync-des/rsyncx/LICENSES.chromium.html $2 rsyncx
    done

    echo "Rsyncx get digs: blockbench"
    for i in {1..5}
    do
        $1/http_get_digs /mnt/sdb-ext4/rsync-des/rsyncx/blockbench $2 rsyncx
    done

    echo "Rsyncx post delta: LICENSES.chromium.html"
    for i in {1..5}
    do
        $1/http_post_delta /mnt/sdb-ext4/rsync-des/rsyncx/LICENSES.chromium.html-delta $2
    done

    echo "Rsyncx post delta: blockbench"
    for i in {1..5}
    do
        $1/http_post_delta /mnt/sdb-ext4/rsync-des/rsyncx/blockbench-delta $2
    done

}

run_fastcdc() {
    echo "FastCDC post fastfp: LICENSES.chromium.html-v1"
    for i in {1..5}
    do
        $1/http_post_fastfp /mnt/sdb-ext4/rsync-des/fastcdc/LICENSES.chromium.html-v1 $2
    done

    echo "FastCDC post fastfp: blockbench-v1"
    for i in {1..5}
    do
        $1/http_post_fastfp /mnt/sdb-ext4/rsync-des/fastcdc/blockbench-v1 $2
    done

    echo "FastCDC post delta: LICENSES.chromium.html-v1-fastcdc-delta"
    for i in {1..5}
    do
        $1/http_post_delta /mnt/sdb-ext4/rsync-des/fastcdc/LICENSES.chromium.html-v1-fastcdc-delta $2
    done

    echo "FastCDC post delta: blockbench-v1-fastcdc-delta"
    for i in {1..5}
    do
        $1/http_post_delta /mnt/sdb-ext4/rsync-des/fastcdc/blockbench-v1-fastcdc-delta $2
    done
}

run_command() {
    srcdir="../../build"
    function=$1
    if [ $function == "rdiff" ]; then
        run_rdiff $srcdir $2
    elif [ $function == "rsyncx" ]; then
        run_rsyncx $srcdir $2
    elif [ $function == "fastcdc" ]; then
        run_fastcdc $srcdir $2
    else
        echo "Error: $function is not supported"
        exit 1
    fi
}

if [ $# -ne 2 ]; then
    echo "Usage: $0 <function> <ip:port>"
    exit 1
fi

date=$(date +%Y-%m-%d)
data_dir="./test.data"

echo "$1 HTTP Latency Test: " >> $data_dir/$date

run_command $1 $2 >> $data_dir/$date

echo "" >> $data_dir/$date
