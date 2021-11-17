import argparse
import os
from upload_parsed_data_to_db import upload_tardis_option_chain_csv_to_db, get_files_paths


parser = argparse.ArgumentParser()
parser.add_argument("--month", "-m")
args = parser.parse_args()
month = args.month

file_paths = [f for f in get_files_paths(f'/home/tardis-data/{month}') if f.endswith('.csv')]
for i, filepath in enumerate(file_paths):
    upload_tardis_option_chain_csv_to_db(filepath)
    print(f'{i} - done file {filepath}')
    os.remove(filepath)
