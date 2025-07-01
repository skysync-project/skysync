#!/bin/bash

# Check if the number of arguments is correct
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <input_file> <output_file> <compression_level>"
  exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"
compression_level="$3"

# Check if lz4 is installed
if ! command -v lz4 &> /dev/null; then
  echo "lz4 could not be found. Please install it."
  exit 1
fi

# Check if the input file exists
if [ ! -f "$INPUT_FILE" ]; then
  echo "Input file '$INPUT_FILE' does not exist."
  exit 1
fi

# Compress the input file and write the compressed data to the output file
# Use the `time` command to measure the runtime
{ time lz4 -c -z -"$compression_level" "$INPUT_FILE" > "$OUTPUT_FILE"; } 2>&1 | tee compression_time.data

# Check if the compression was successful
if [ $? -eq 0 ]; then
  echo "File '$INPUT_FILE' compressed successfully and written to '$OUTPUT_FILE'."
else
  echo "Compression failed."
  exit 1
fi