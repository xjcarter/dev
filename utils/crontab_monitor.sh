#!/bin/bash

# Set the directory path
crontab_dir="/trading/misc/crontabs"

# Find the most recent crontab file
most_recent_file=$(ls -1t "$crontab_dir"/crontab.* | head -n1)

# Get the current crontab
current_crontab=$(crontab -l)

# Check if the most recent file exists
if [ -f "$most_recent_file" ]; then
    # Perform a diff between the current crontab and the most recent crontab file
    diff_result=$(diff <(echo "$current_crontab") "$most_recent_file")

    # Check if there are differences
    if [ -n "$diff_result" ]; then
        # Record the current crontab to a new file with the current date
        new_filename="$crontab_dir/crontab.$(date +'%Y%m%d')"
        echo "$current_crontab" > "$new_filename"
        echo "Changes detected. Recorded current crontab to: $new_filename"
    else
        echo "No changes detected."
    fi
else
    # If no previous crontab file exists, record the current crontab
    new_filename="$crontab_dir/crontab.$(date +'%Y%m%d')"
    echo "$current_crontab" > "$new_filename"
    echo "No previous crontab file found. Recorded current crontab to: $new_filename"
fi

# Record the current crontab as a backup
crontab -l > $crontab_dir/crontab.CURRENT


