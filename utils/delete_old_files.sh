#!/bin/bash

# does an end of the month cleanup of the directories given
# just add this script to a cronjob that runs at the end of the day

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


# Check if directory paths are provided as arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 <directory_path1> <directory_path2> ... <directory_pathN>"
    exit 1
fi

# Get the current day of the month
current_day=$(date +%d)

# Get the last day of the current month
last_day=$(date -d "$(date +'%Y-%m-01') +1 month -1 day" +%d)

# Make sure it's the last day of the month
if [ "$current_day" -eq "$last_day" ]; then
    # Iterate over each directory path provided
    for directory_path in "$@"; do
       # Check if directory exists
       if [ ! -d "$directory_path" ]; then
          echo "Directory '$directory_path' not found."
          continue
       fi
      
       children=$(find_directories_with_files $directory_path) 
       for child in $children; do       
           # Delete files not modified or accessed in the last 90 days
           find "$child" -type f \( -mtime +89 -o -atime +89 \) -exec rm -f {} \;
       done
    done
fi
