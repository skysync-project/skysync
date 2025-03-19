#!/bin/bash

# Set CPU frequency to 2.0 GHz
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq
for i in /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq; do echo "2000000" > $i; done
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq