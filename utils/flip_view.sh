#!/bin/bash

# Check if optional date range arguments are provided
if [ "$#" -eq 2 ]; then
    start_date="$1"
    end_date="$2"
else
    start_date=""
    end_date=""
fi

# Read the header line to get column names
IFS=',' read -r -a headers

# Process each subsequent line of the CSV
while IFS=',' read -r -a line; do
    # Extract the date from the current line
    current_date="${line[0]}"

    # Check if the date falls within the specified range (if provided)
    if [[ -z "$start_date" ]] || { [[ "$current_date" > "$start_date" || "$current_date" == "$start_date" ]] && [[ "$current_date" < "$end_date" || "$current_date" == "$end_date" ]]; }; then
        # Iterate over each column in the line
        for i in "${!headers[@]}"; do
            # Print "column name: value" for each column
            echo "${headers[$i]}: ${line[$i]}"
        done
        # Print a blank line to separate rows
        echo
    fi
done
