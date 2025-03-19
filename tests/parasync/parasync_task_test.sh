#!/bin/bash

# 10 datasets for each two files to be compared
datasets=(
    "/mnt/sync-test/chat_1.cb"
    "/mnt/sync-test/chat_2.cb"
    "/mnt/sync-test/ubuntu_1.cb"
    "/mnt/sync-test/ubuntu_2.cb"
    "/mnt/sync-test/nutsnap_1.cb"
    "/mnt/sync-test/nutsnap_2.cb"
    "/mnt/sync-test/enwiki_1.xml"
    "/mnt/sync-test/enwiki_2.xml"
    "/mnt/sync-test/kernel_1.cb"
    "/mnt/sync-test/kernel_2.cb"
)

cd ../../build

date=$(date +%Y%m%d)
log_file=test-$date.csv
start_file=(0 2 4 6 8)
for i in ${start_file[@]}; 
do
    old_file=${datasets[$i]}
    new_file=${datasets[$i+1]}
    base_name=$(basename $old_file)
    base_name=${base_name%.*}
    for threads in 2 4 8 16;
    do
        echo "threads,$threads" >> $log_file
        for j in $(seq 1 3);
        # for j in 1;
        do
            sudo sync && sudo sysctl -w vm.drop_caches=3
            sudo ./parasync_task_test $old_file $new_file $threads 1 >> $log_file
        done
    done
done