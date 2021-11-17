import os
from upload_parsed_data_to_db import upload_tardis_option_chain_csv_to_db, get_files_paths


file_paths = get_files_paths('/home/tardis-data/csv-files/')
for i, filepath in enumerate(file_paths):
    print(f'{i} - handling file {filepath}')
    upload_tardis_option_chain_csv_to_db(filepath)
    print(f'{i} - done file {filepath}')
    os.remove(filepath)
