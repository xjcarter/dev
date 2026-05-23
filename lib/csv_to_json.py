import csv
import json
import argparse

## converts CSV files to JSON

def csv_to_json(csv_file, json_file, add_index=False):
    # Open the CSV file for reading
    with open(csv_file, 'r') as f:
        # Read the CSV file using DictReader
        csv_reader = csv.DictReader(f)
        # Convert each row of the CSV file to a dictionary
        data = [row for row in csv_reader]
        if add_index: 
            for i in range(len(data)):
                data[i].update( dict(index=i) )


    # Write the data to a JSON file
    with open(json_file, 'w') as f:
        # Convert the data to JSON format and write it to the JSON file
        json.dump(data, f, indent=4)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", help="CSV file to convert to JSON", required=True)
    parser.add_argument("--json", help="JSON output filename", default='csv_out.json')
    parser.add_argument("--index", help="add an index value to each row", action='store_true')
    u = parser.parse_args()

    csv_to_json(u.csv, u.json, u.index)
