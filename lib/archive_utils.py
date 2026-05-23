import os
import gzip
from datetime import datetime, timedelta 

def file_modified_before(filepath, reference_datetime):
    try:
        # Get the last modification time of the file
        file_modification_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        # Compare with the reference datetime
        return file_modification_time < reference_datetime
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return False

def compress_file(file_path):
    with open(file_path, 'rb') as f_in:
        with gzip.open(f"{file_path}.gz", 'wb') as f_out:
            f_out.writelines(f_in)

def compress_files_in_directories(directories, file_tag=None, before=None):
    for directory in directories:
        for root, _, files in os.walk(directory):
            for file in files:
                if file_tag is None or file_tag in file:
                    file_path = os.path.join(root, file)
                    if before is None or file_modified_before(file_path, before): 
                        compress_file(file_path)
                        os.remove(file_path)

if __name__ == '__main__':
    directories_to_compress = ['/home/jcarter/junk/compress_test/']
    before_dt = datetime.now() - timedelta(days=15)
    compress_files_in_directories(directories_to_compress, file_tag=".py", before= before_dt)

