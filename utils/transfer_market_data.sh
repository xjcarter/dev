#!/bin/bash

# makes sure that all data collected is collected
# into a single directory

# Source and destination directories
SOURCE_DIR="/portfolio/*/data"
DEST_DIR="/marketdata"

# Create the destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Find and copy files created in the last 5 days
find $SOURCE_DIR -type f -mtime -5 -exec cp {} $DEST_DIR \;

echo "Files created in the last 5 days have been copied to $DEST_DIR."

