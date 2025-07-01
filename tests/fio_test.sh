#!/bin/bash

# Check arguments
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <fio_filename>"
  exit 1
fi

fio_filename="$1"

if ! command -v fio &> /dev/null; then
  echo "fio could not be found. Please install it."
  exit 1
fi

# Run fio with different number of jobs
for jobs in 1 2 4 8; do
    for i in 1 2 3 4 5; do
        fio --rw=rw \
            --bs=4096 \
            --name=rw1 \
            --filename=$fio_filename \
            --ioengine=libaio \
            --numjobs=$jobs \
            --thread \
            --group_reporting \
            --norandommap \
            --direct=1 \
            --iodepth=128 \
            --offset=0 \
            --size=10g
        echo -e "\n"
        rm $fio_filename
    done

    for i in 1 2 3 4 5; do
        fio --rw=randrw \
            --bs=4096 \
            --name=randrw1 \
            --filename=$fio_filename \
            --ioengine=libaio \
            --numjobs=$jobs \
            --thread \
            --group_reporting \
            --norandommap \
            --direct=1 \
            --iodepth=128 \
            --offset=0 \
            --size=10g
        echo -e "\n"
        rm $fio_filename
    done
done

# Run fio with different block sizes
for bs in 4096 8192 16384 65536 131072 262144; do
    for i in 1 2 3 4 5; do
        fio --rw=rw \
            --bs=$bs \
            --name=rw2 \
            --filename=$fio_filename \
            --ioengine=libaio \
            --numjobs=1 \
            --thread \
            --group_reporting \
            --norandommap \
            --direct=1 \
            --iodepth=128 \
            --offset=0 \
            --size=10g
        echo -e "\n"
        rm $fio_filename
    done


    for i in 1 2 3 4 5; do
        fio --rw=randrw \
            --bs=$bs \
            --name=randrw2 \
            --filename=$fio_filename \
            --ioengine=libaio \
            --numjobs=1 \
            --thread \
            --group_reporting \
            --norandommap \
            --direct=1 \
            --iodepth=128 \
            --offset=0 \
            --size=10g
        echo -e "\n"
        rm $fio_filename
    done
done