#!/bin/bash

# Define the directory and tarball name
MARKETDATA_DIR="/marketdata"
TARBALL="$MARKETDATA_DIR/marketdata.gz"

# Step 1: Check if the tarball exists
if [ ! -f "$TARBALL" ]; then
    # Create the tarball with all non-gz files
    tar -czvf "$TARBALL" -C "$MARKETDATA_DIR" $(find "$MARKETDATA_DIR" -type f ! -name '*.gz')
    echo "Tarball created."
else
    # Step 2: Rebuild the tarball with new files
    # Extract the existing tarball to a temporary directory
    TEMP_DIR=$(mktemp -d)
    tar -xzvf "$TARBALL" -C "$TEMP_DIR"
    
    # Add new files that are not in the tarball
    find "$MARKETDATA_DIR" -type f ! -name '*.gz' | while read -r file; do
        if [ ! -f "$TEMP_DIR/${file#$MARKETDATA_DIR/}" ]; then
            cp --parents "$file" "$TEMP_DIR"
        fi
    done

    # Create a new tarball from the temporary directory
    tar -czvf "$TARBALL" -C "$TEMP_DIR" .
    rm -rf "$TEMP_DIR"
    echo "Tarball updated."
fi

# Step 3: Delete files not touched in the last 90 days
find "$MARKETDATA_DIR" -type f -mtime +90 ! -name '*.gz' -exec rm {} \;
echo "Old files deleted."

