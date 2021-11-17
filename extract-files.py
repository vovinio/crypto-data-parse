import argparse
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


parser = argparse.ArgumentParser()
parser.add_argument("--month", "-m")
args = parser.parse_args()
month = args.month

files_to_extract = get_files_paths(f'/home/tardis-data/{month}')

for i, filepath in enumerate(files_to_extract):
    with gzip.open(filepath, 'rb') as f_in:
        extraction_path = filepath.replace('.csv.gz', '.csv')
        with open(extraction_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        print(f'{i} - done {filepath}')
    os.remove(filepath)
