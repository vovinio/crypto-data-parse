import os
import sys
import time
import logging
import csv
import argparse

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, AutoReconnect

from prase_tardis_options_on_chain_csv import parse_single_deribit_option_chain_line


mongo_client_data = MongoClient('mongodb://{}:{}@88.99.252.172:27017'.format('moonrock', '!8478xhgaxBiz35'))
db_options_chain = mongo_client_data['knight-fund']['options-chain']


logging.basicConfig(
    format=f'%(asctime)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.INFO,
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def perform_bulk_operations(db, bulk_operations_list):
    logger.info(f'performing bulk operation over {len(bulk_operations_list)} tasks')
    try:
        if bulk_operations_list:
            db.bulk_write(bulk_operations_list, ordered=False)
    except BulkWriteError as bwe:
        logger.critical(f'{bwe.details}')
    except AutoReconnect:
        time.sleep(20)
        db.bulk_write(bulk_operations_list)


def upload_tardis_option_chain_csv_to_db(filepath: str):
    upload_list = []
    upload_len_size = 100000
    total_uploaded = 0
    with open(filepath, 'r') as csv_file:
        for line in csv.DictReader(csv_file):
            deribit_point = parse_single_deribit_option_chain_line(line)
            upload_list.append(
                InsertOne(deribit_point.dict())
            )
            if len(upload_list) >= upload_len_size:
                total_uploaded += len(upload_list)
                logger.info(f'uploaded {total_uploaded}')
                perform_bulk_operations(
                    db=db_options_chain, bulk_operations_list=upload_list
                )
                upload_list = []
    perform_bulk_operations(
        db=db_options_chain, bulk_operations_list=upload_list
    )


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


# parser = argparse.ArgumentParser()
# parser.add_argument("--month", "-m", help="month")
# args = parser.parse_args()
# month = args.month

# print(f'uploading month {month}')
# file_paths = get_files_paths('/home/tardis-data/csv-files/')
# for i, filepath in enumerate(file_paths):
#     if f'2021-{month}-' not in filepath:
#         continue
#     upload_tardis_option_chain_csv_to_db(filepath)
#     print(f'{i} - done file {filepath}')


# more_file_paths = get_files_paths('/home/tardis-data/csv-files/')
# more_file_paths = [f for f in more_file_paths if f not in file_paths]
# for i, filepath in enumerate(more_file_paths):
#     if f'2021-{month}-' not in filepath:
#         continue
#     upload_tardis_option_chain_csv_to_db(filepath)
#     print(f'{i} - done file {filepath}')
