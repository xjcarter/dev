import sys
import csv
from prettytable import PrettyTable
import argparse

def is_numeric_string(s):
    """Check if a string represents a numerical value."""
    try:
        float(s.replace(',', ''))  # Handle numbers with commas
        return True
    except ValueError:
        return False

def make_unique_headers(headers):
    """Make headers unique by appending numbers to duplicates."""
    seen = {}
    unique_headers = []
    for header in headers:
        if header in seen:
            seen[header] += 1
            unique_headers.append(f"{header}_{seen[header]}")
        else:
            seen[header] = 0
            unique_headers.append(header)
    return unique_headers

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Display CSV data as a formatted table.', add_help=False)
    parser.add_argument('csv_file', help='Path to the CSV file')
    parser.add_argument('--tail', type=int, help='Show last N rows (default: 20)')
    parser.add_argument('--head', type=int, help='Show first N rows')
    parser.add_argument('-H', '--help', action='help', default=argparse.SUPPRESS,
                      help='Show this help message and exit')
    
    args = parser.parse_args()
    
    # Set default to show last 20 rows if no flags provided
    if args.head is None and args.tail is None:
        args.tail = 20
    
    # Read CSV data
    try:
        with open(args.csv_file, 'r') as f:
            reader = csv.reader(f)
            data = list(reader)
    except FileNotFoundError:
        print(f"Error: File '{args.csv_file}' not found", file=sys.stderr)
        sys.exit(1)
    
    if not data:
        print("Error: CSV file is empty", file=sys.stderr)
        sys.exit(1)
    
    # Separate header from data
    headers = data[0]
    rows = data[1:]
    
    # Apply head or tail filter
    if args.head:
        rows = rows[:args.head]
    elif args.tail:
        rows = rows[-args.tail:] if args.tail <= len(rows) else rows
    
    # Create PrettyTable with headers
    table = PrettyTable()
    
    # Make headers unique if needed
    if len(headers) != len(set(headers)):
        headers = make_unique_headers(headers)
        print("Note: Duplicate column headers were made unique by appending numbers", file=sys.stderr)
    
    table.field_names = headers
    
    # Add rows
    for row in rows:
        table.add_row(row)
    
    # Set alignment for columns
    for field in table.field_names:
        # Check if all values in column are numeric (excluding empty strings)
        col_index = table.field_names.index(field)
        if all(is_numeric_string(row[col_index]) for row in rows if row and len(row) > col_index and row[col_index]):
            table.align[field] = 'r'
        else:
            table.align[field] = 'l'
    
    # Print the table
    print(table)

if __name__ == "__main__":
    main()