#!/bin/bash

echo "Checking if liburing is installed..."

# Step 1: Try detecting via dpkg
if dpkg -l | grep -q liburing; then
    echo "liburing found via APT. Removing..."
    sudo apt remove -y liburing-dev liburing2
    sudo apt autoremove -y
fi

# Step 2: Try detecting installed files (in case it was installed from source)
LIBURING_FILES=$(find /usr/local/lib /usr/local/include /usr/lib /usr/include -name "liburing*" 2>/dev/null)

if [ -n "$LIBURING_FILES" ]; then
    echo "liburing found from source install. Cleaning up..."
    echo "$LIBURING_FILES" | xargs sudo rm -vrf
    sudo ldconfig
fi