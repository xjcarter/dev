#!/usr/bin/env python3
"""
CSV Row Range Reader
Reads a CSV file and outputs specified rows in JSON format.
"""

import csv
import json
import argparse
import sys
from pathlib import Path


def read_csv_rows(csv_file, start_row, end_row=None):
    """
    Read CSV file and return specified rows as a list of dictionaries.
    
    Args:
        csv_file (str): Path to the CSV file
        start_row (int): Starting row number (1-based indexing)
        end_row (int, optional): Ending row number (1-based indexing)
    
    Returns:
        list: List of dictionaries representing the selected rows
    """
    if not Path(csv_file).exists():
        print(f"Error: File '{csv_file}' not found.", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            
            # Convert to 0-based indexing
            start_idx = start_row - 1
            end_idx = end_row - 1 if end_row else start_idx
            
            # Validate row indices
            if start_idx < 0 or start_idx >= len(rows):
                print(f"Error: Start row {start_row} is out of range. File has {len(rows)} data rows.", file=sys.stderr)
                sys.exit(1)
            
            if end_row and (end_idx < 0 or end_idx >= len(rows)):
                print(f"Error: End row {end_row} is out of range. File has {len(rows)} data rows.", file=sys.stderr)
                sys.exit(1)
            
            if end_row and end_idx < start_idx:
                print(f"Error: End row ({end_row}) cannot be less than start row ({start_row}).", file=sys.stderr)
                sys.exit(1)
            
            # Extract the specified range
            selected_rows = rows[start_idx:end_idx + 1]
            return selected_rows
            
    except Exception as e:
        print(f"Error reading CSV file: {e}", file=sys.stderr)
        sys.exit(1)


def format_output(rows, start_row, end_row=None):
    """
    Format the output as JSON with row information.
    
    Args:
        rows (list): List of row dictionaries
        start_row (int): Starting row number
        end_row (int, optional): Ending row number
    """
    if not rows:
        print("No rows found in the specified range.")
        return
    
    output = {
        "query": {
            "start_row": start_row,
            "end_row": end_row if end_row else start_row,
            "total_rows_returned": len(rows)
        },
        "data": []
    }
    
    for i, row in enumerate(rows, start=start_row):
        formatted_row = {
            "row_number": i,
            "columns": row
        }
        output["data"].append(formatted_row)
    
    print(json.dumps(output, indent=2, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(
        description="Read specified rows from a CSV file and output in JSON format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data.csv 5           # Show only row 5
  %(prog)s data.csv 10 15       # Show rows 10 through 15
  %(prog)s data.csv 1 3         # Show rows 1 through 3
        """
    )
    
    parser.add_argument('csv_file', help='Path to the CSV input file')
    parser.add_argument('start_row', type=int, help='Starting row number (1-based)')
    parser.add_argument('end_row', type=int, nargs='?', help='Ending row number (1-based, optional)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.start_row < 1:
        print("Error: Row numbers must be 1 or greater.", file=sys.stderr)
        sys.exit(1)
    
    if args.end_row and args.end_row < 1:
        print("Error: Row numbers must be 1 or greater.", file=sys.stderr)
        sys.exit(1)
    
    # Read the CSV rows
    rows = read_csv_rows(args.csv_file, args.start_row, args.end_row)
    
    # Format and output the results
    format_output(rows, args.start_row, args.end_row)


if __name__ == "__main__":
    main()
