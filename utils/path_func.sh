#!/bin/bash

# finds endpoint directories that contain files
find_directories_with_files() {
    # Check if base directory argument is provided
    if [ $# -eq 0 ]; then
        echo "Usage: find_directories_with_files <base_directory>"
        return 1
    fi

    # Store the base directory path
    base_directory="$1"

    # Find all directories containing files under the base directory
    find "$base_directory" -type f -exec dirname {} \; | sort -u
}

pp="/portfolio/"
results=$(find_directories_with_files $pp)
echo $results
