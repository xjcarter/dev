#!/bin/bash

# does an end of the month cleanup of the directories given
# just add this script to a cronjob that runs at the end of the day

# Default history value (in days)
HISTORY_DAYS=90
FORCE_RUN=false

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

# Function to display usage
show_usage() {
    echo "Usage: $0 [--history=N] [--now] <directory_path1> <directory_path2> ... <directory_pathN>"
    echo ""
    echo "Options:"
    echo "  --history=N    Number of days of history to keep (default: 90)"
    echo "                 Files older than N days will be deleted"
    echo "  --now          Force cleanup to run now, regardless of day of month"
    echo "                 (bypasses end-of-month check)"
    echo ""
    echo "Examples:"
    echo "  $0 --history=20 /path/to/dir1 /path/to/dir2"
    echo "  $0 /path/to/dir1 /path/to/dir2         # uses default 90 days"
    echo "  $0 --now /path/to/dir1                 # runs cleanup immediately"
    echo "  $0 --history=30 --now /path/to/dir1    # runs now with 30-day history"
    exit 1
}

# Parse command line arguments
directories=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --history=*)
            HISTORY_DAYS="${1#*=}"
            # Validate that HISTORY_DAYS is a positive integer
            if ! [[ "$HISTORY_DAYS" =~ ^[0-9]+$ ]] || [ "$HISTORY_DAYS" -eq 0 ]; then
                echo "Error: --history requires a positive integer value"
                show_usage
            fi
            shift
            ;;
        --now)
            FORCE_RUN=true
            shift
            ;;
        --help|-h)
            show_usage
            ;;
        -*)
            echo "Error: Unknown option $1"
            show_usage
            ;;
        *)
            directories+=("$1")
            shift
            ;;
    esac
done

# Check if directory paths are provided
if [ ${#directories[@]} -eq 0 ]; then
    echo "Error: No directory paths provided"
    show_usage
fi

# Get the current day of the month
current_day=$(date +%d)

# Get the last day of the current month
last_day=$(date -d "$(date +'%Y-%m-01') +1 month -1 day" +%d)

# Check if we should run (either end of month or forced with --now)
if [ "$current_day" -eq "$last_day" ] || [ "$FORCE_RUN" = true ]; then
    if [ "$FORCE_RUN" = true ]; then
        echo "Running cleanup NOW (forced with --now option)"
    else
        echo "Running cleanup (end of month)"
    fi
    
    # Calculate the mtime/atime threshold (days - 1 because find uses +N for "older than N days")
    # For example: --history=20 means keep files from the last 20 days, delete files older than 20 days
    # find's -mtime +19 means files modified 20 or more days ago
    threshold=$((HISTORY_DAYS - 1))
    
    echo "History set to $HISTORY_DAYS days (deleting files older than $HISTORY_DAYS days)"
    
    # Iterate over each directory path provided
    for directory_path in "${directories[@]}"; do
        # Check if directory exists
        if [ ! -d "$directory_path" ]; then
            echo "Directory '$directory_path' not found."
            continue
        fi
        
        echo "Processing directory: $directory_path"
        
        children=$(find_directories_with_files "$directory_path") 
        for child in $children; do
            echo "  Cleaning: $child"
            # Delete files not modified or accessed in the last HISTORY_DAYS days
            find "$child" -type f \( -mtime +$threshold -o -atime +$threshold \) -exec rm -fv {} \;
        done
    done
else
    echo "Not the last day of the month and --now not specified. Skipping cleanup."
    echo "Current day: $current_day, Last day: $last_day"
    exit 0
fi