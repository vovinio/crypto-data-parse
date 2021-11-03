import sys
import time
import logging
import csv

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


def parse_tardis_option_chain_csv(filepath: str):
    upload_list = []
    upload_len_size = 100000
    total_uploaded = 0
    with open(filepath, 'r') as csv_file:
        for line in csv.DictReader(csv_file):
            deribit_point = parse_single_deribit_option_chain_line(line)
            print(deribit_point.dict())
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


parse_tardis_option_chain_csv('/home/tardis-data/csv-files/deribit_options_chain_2021-07-26_OPTIONS.csv')
