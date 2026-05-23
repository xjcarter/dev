#!/bin/bash

# Check if an argument was provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 new_directory"
    exit 1
fi

# Get the new directory name from the argument
new_directory=$1

# The base directory
base_directory="/portfolio"

# Full path of the new directory
full_path="${base_directory}/${new_directory}"

# Create the new directory and the subdirectories
mkdir -p "${full_path}"/{account,data,logs,positions,trades}

# Set the permissions to 755 for the directories
chmod 755 "${full_path}"
chmod 755 "${full_path}"/account
chmod 755 "${full_path}"/data
chmod 755 "${full_path}"/logs
chmod 755 "${full_path}"/positions
chmod 755 "${full_path}"/trades

echo "Strategy directories ${full_path} created with permissions 755."
ls -ltrd1 ${full_path}/*

