import os
import gzip
import shutil


def get_files_paths(dir_path: str):
    """
    Getting all the file paths of a specific file type from a given path.
    :param dir_path: path to dir get all nested files from
    :return: list of file paths
    """
    files_paths = []
    for (dir_path, dir_names, file_names) in os.walk(dir_path):
        for filename in file_names:
            files_paths.append(os.path.join(dir_path, filename))
    return files_paths




# files_to_extract = get_files_paths('/home/tardis-data')


files_to_extract = [
    '/home/tardis-data/deribit_options_chain_2021-11-01_OPTIONS.csv.gz',
]

for i, filepath in enumerate(files_to_extract):
    with gzip.open(filepath, 'rb') as f_in:
        with open(filepath.replace('.csv.gz', '.csv').replace('/home/tardis-data/', '/home/tardis-data/csv-files/'), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        print(f'{i} - done {filepath}')
    os.remove(filepath)


# for i, filepath in enumerate(file_paths):
#     for n in extracted_files_set:
#         if n in filepath:
#             os.remove(filepath)
