#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 [-t] /new /old"
    echo "  -t    Test run. No files will be copied. Only a report will be displayed."
    exit 1
}

# Check if the correct number of arguments is provided
if [ "$#" -lt 2 ]; then
    usage
fi

# Check for the test-run option
TEST_RUN=false
if [ "$1" == "-t" ]; then
    TEST_RUN=true
    shift
fi

NEW_DIR="$1"
OLD_DIR="$2"

# Check if directories exist
if [ ! -d "$NEW_DIR" ] || [ ! -d "$OLD_DIR" ]; then
    echo "Both /new and /old directories must exist."
    exit 1
fi

# Function to copy files and handle subdirectories
copy_files() {
    local src_dir="$1"
    local dst_dir="$2"

    # Ensure the destination directory exists
    mkdir -p "$dst_dir"

    # Iterate over items in the source directory
    for src_item in "$src_dir"/*; do
        src_basename=$(basename "$src_item")
        dst_item="$dst_dir/$src_basename"

        if [ -d "$src_item" ]; then
            if [ ! -d "$dst_item" ]; then
                if [ "$TEST_RUN" = true ]; then
                    echo "Add directory: $src_item will be copied to $dst_item."
                else
                    cp -rp "$src_item" "$dst_item"
                    echo "Added directory: $src_item to $dst_item"
                fi
            else
                # Recursively handle subdirectories
                copy_files "$src_item" "$dst_item"
	    fi
        elif [ -f "$src_item" ]; then
            if [ -f "$dst_item" ]; then
                # File exists in both source and destination
                if [ "$src_item" -nt "$dst_item" ]; then
                    if [ "$TEST_RUN" = true ]; then
                        echo "Update: $src_item will be copied to $dst_item."
                    else
                        cp -p "$src_item" "$dst_item"
                        echo "Updated: $src_item to $dst_item"
		    fi
                fi
            else
                # File exists only in source
                if [ "$TEST_RUN" = true ]; then
                    echo "Add: $src_item will be copied to $dst_item."
                else
                    cp -p "$src_item" "$dst_item"
                    echo "Added: $src_item to $dst_item"
		fi
	    fi
        fi
    done
}

# Start the copy process from the root of the directories
copy_files "$NEW_DIR" "$OLD_DIR"

if [ "$TEST_RUN" = true ]; then
    echo "Test run completed. No files were copied."
fi
