import os
import json

def process_trading_files(directory):
    # Iterate through all files in the given directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        
        # Skip directories, only process files
        if not os.path.isfile(file_path):
            continue

        # Check for target strings in filename
        is_orders = "orders" in filename
        is_trades = "trades" in filename

        if is_orders or is_trades:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                modified_data = None

                # Logic for Orders files (List of objects)
                if is_orders:
                    for entry in data:
                        if "info" in entry:
                            entry["layer_id"] = entry.pop("info")
                    modified_data = data

                # Logic for Trades files (Nested structure)
                elif is_trades:
                    if "trades" in data:
                        for trade in data["trades"]:
                            if "order_info" in trade:
                                trade["layer_id"] = trade.pop("order_info")
                    modified_data = data

                # Save the modified file back to the original location
                if modified_data is not None:
                    with open(file_path, 'w') as f:
                        json.dump(modified_data, f, indent=4)
                    print(f"Successfully updated: {filename}")

            except Exception as e:
                print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    # Use '.' for current directory or provide a specific path
    target_directory = "." 
    process_trading_files(target_directory)