import os
import sys
import time
import logging
import csv
import argparse
from typing import List
from datetime import datetime, timedelta
import pandas as pd
import operator
from collections import defaultdict

from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, AutoReconnect

from prase_tardis_options_on_chain_csv import Coin, parse_single_deribit_option_chain_line, DeribitOptionLineToCsv, calculate_round_unit


mongo_client_data = MongoClient('mongodb://{}:{}@88.99.252.172:27017'.format('moonrock', '!8478xhgaxBiz35'))
db_options_chain = mongo_client_data['knight-fund']['options-chain']


logging.basicConfig(
    format=f'%(asctime)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.INFO,
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def get_data_by_time_interval(
    start_time, iterval_size_in_minutes: int, coin: Coin, event_type: str
):
    symbols_list = []
    docs_to_return = []
    for minute in range(0, iterval_size_in_minutes):
        docs = db_options_chain.aggregate([
            {
                '$match': {
                        'event_time': {
                            '$gte': start_time + timedelta(minutes=minute),
                            '$lt': start_time + timedelta(minutes=minute+1),
                        },
                        'coin': coin,
                        'type': event_type,
                        'mark_price': {'$ne': 0},
                        'symbol': {'$nin': symbols_list},
                    }
            },
            {
                '$sort': {'event_time': ASCENDING}
            },
            {
                '$group': {
                    '_id': {'symbol': '$symbol'},
                    'doc': {'$first': '$$ROOT'},
                }
            }
        ], allowDiskUse=True)

        for doc in docs:
            symbols_list.append(doc['_id']['symbol'])
            docs_to_return.append(doc['doc'])

        docs = db_options_chain.aggregate([
            {
                '$match': {
                        'event_time': {
                            '$gte': start_time - timedelta(minutes=minute+1),
                            '$lt': start_time - timedelta(minutes=minute),
                        },
                        'coin': coin,
                        'type': event_type,
                        'mark_price': {'$ne': 0},
                        'symbol': {'$nin': symbols_list},
                    }
            },
            {
                '$sort': {'event_time': DESCENDING}
            },
            {
                '$group': {
                    '_id': {'symbol': '$symbol'},
                    'doc': {'$first': '$$ROOT'},
                }
            }
        ], allowDiskUse=True)
        for doc in docs:
            symbols_list.append(doc['_id']['symbol'])
            docs_to_return.append(doc['doc'])
    
    return docs_to_return


def get_round_udl_prices_and_distance_prices(
    underlying_price: float,
    strike_price: int,
    round_udl_price_sizes: List[int],
):
    round_udl_prices = {}
    distance_prices = {}
    for round_size in round_udl_price_sizes:
        round_price = calculate_round_unit(
            round_size=round_size,
            unit_to_round=underlying_price
        )
        distance_price = strike_price - round_price
        if round_size >= 1000:
            price_symbol = f'{int(round_size / 1000)}K'
        else:
            price_symbol = f'{round_size}'
        round_udl_prices[f'round_udl_price_{price_symbol}'] = round_price
        distance_prices[f'distance_price_{price_symbol}'] = distance_price
    return round_udl_prices, distance_prices


def get_round_mark_iv_values(
    mark_iv: float,
    round_mark_iv_sizes: List[int],
):
    round_values = {}
    for round_size in round_mark_iv_sizes:
        rounded_value = calculate_round_unit(
            round_size=round_size,
            unit_to_round=mark_iv
        )
        if round_size >= 1000:
            param_symbol = f'{int(round_size / 1000)}K'
        else:
            param_symbol = f'{round_size}'
        round_values[f'round_mark_iv_{param_symbol}'] = rounded_value
    return round_values


def get_parsed_option_obj(
    data_dict: dict, round_udl_price_sizes: List[int], round_mark_iv_sizes: List[int]
    ):
    if data_dict['event_time'].minute > 30:
        data_dict['event_time'] = data_dict['event_time'].replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        data_dict['event_time'] = data_dict['event_time'].replace(minute=0, second=0, microsecond=0)
    delta = data_dict['expiration_time'] - data_dict['event_time']
    days = delta.days
    hours = int(delta.seconds / 3600)
    minutes = int((delta.seconds - hours * 3600) / 60)
    tte = ''
    tte += f'0{days}' if days < 10 else f'{days}'
    tte += f'0{hours}' if hours < 10 else f'{hours}'
    tte += f'0{minutes}' if minutes < 10 else f'{minutes}'
    parsed_dict = DeribitOptionLineToCsv(
        **data_dict,
        date=data_dict['event_time'].strftime("%d%m%Y%H%M"),
        expiration=data_dict['expiration_time'].strftime("%d%m%y"),
        mark_price_usd=data_dict['mark_price'] * data_dict['underlying_price'],
        year=data_dict['event_time'].year,
        month=data_dict['event_time'].month,
        day=data_dict['event_time'].day,
        hour_min=data_dict['event_time'].strftime("%H%M"),
        tte=tte,
        tte_days=days,
    ).dict()
    round_udl_prices, distance_prices = get_round_udl_prices_and_distance_prices(
        underlying_price=parsed_dict['underlying_price'],
        strike_price=parsed_dict['strike_price'],
        round_udl_price_sizes=round_udl_price_sizes,
    )
    round_mark_iv_values = get_round_mark_iv_values(
        mark_iv=data_dict['mark_iv'],
        round_mark_iv_sizes=round_mark_iv_sizes,
    )
    final_dict = {}
    for key, value in parsed_dict.items():
        final_dict[key] = value
        if key == 'underlying_price':
            final_dict.update(round_udl_prices)
            final_dict.update(distance_prices)
        elif key == 'mark_iv':
            final_dict.update(round_mark_iv_values)
    return final_dict


def save_data_in_excel(options_chain_data: List[dict], filename: str):
    df = pd.DataFrame(options_chain_data)
    df = df.reset_index(drop=True)
    # df.sort_values(
    #     ['event_time', 'expiration_time', 'strike_price'],
    #     ascending=[True, True, True],
    #     inplace=True
    #     )
    df.drop('event_time', axis=1, inplace=True)
    df.drop('expiration_time', axis=1, inplace=True)    
    writer = pd.ExcelWriter(f'/home/droppy/files/{filename}.xlsx')
    df.to_excel(writer, index=False)
    writer.save()


def get_option_chain_data_by_hour(coin: Coin, evet_type: str, iterval_size_in_minutes: int) -> List[dict]:
    dates = {
        # 7: list(range(1,32)),
        # 8: list(range(1,32)),
        # 9: list(range(1,31)),
        # 10: list(range(1,32)),
        11: [16,17],
    }
    hours = list(range(0, 24))
    total_rows = []
    for month, days in dates.items():
        for day in days:
            for hour in hours:
                rows = get_data_by_time_interval(
                    start_time=datetime(year=2021, month=month, day=day, hour=hour, minute=0),
                    coin=coin,
                    event_type=evet_type,
                    iterval_size_in_minutes=iterval_size_in_minutes,
                )
                total_rows.extend(rows)
    return total_rows


def parse_options_chain_data(
    options_chain_data: List[dict],
    round_udl_price_sizes: List[int],
    round_mark_iv_sizes: List[int],
):
    options_chain_data = [
        get_parsed_option_obj(data_dict=d, round_udl_price_sizes=round_udl_price_sizes, round_mark_iv_sizes=round_mark_iv_sizes)
        for d in options_chain_data
        ]
    options_chain_data = sorted(
        options_chain_data,
        key=operator.itemgetter('event_time', 'expiration_time', 'strike_price')
    )
    return options_chain_data


def update_spreads_for_groupped_event(groupped_event: List[dict], spreads_sizes: List[int]):
    for doc in groupped_event:
        for spread_size in spreads_sizes:
            if spread_size >= 1000:
                spread_symbol = f'{int(spread_size / 1000)}K'
            else:
                spread_symbol = f'{spread_size}'
            doc[f'ds_{spread_symbol}'] = 0
            doc[f'cs_{spread_symbol}'] = 0
            doc[f'ds_usd_{spread_symbol}'] = 0
            doc[f'cs_usd_{spread_symbol}'] = 0

    for i, base_doc in enumerate(groupped_event):
        for doc in groupped_event[i+1:]:
            strike_diff = doc['strike_price'] - base_doc['strike_price']
            for spread_size in spreads_sizes:
                if spread_size >= 1000:
                    spread_symbol = f'{int(spread_size / 1000)}K'
                else:
                    spread_symbol = f'{spread_size}'
                if strike_diff == spread_size:
                    deff_price = doc['mark_price'] - base_doc['mark_price']
                    deff_price_usd = doc['mark_price_usd'] - base_doc['mark_price_usd']
                    doc[f'ds_{spread_symbol}'] = deff_price
                    doc[f'cs_{spread_symbol}'] = - deff_price
                    doc[f'ds_usd_{spread_symbol}'] = deff_price_usd
                    doc[f'cs_usd_{spread_symbol}'] = - deff_price_usd


def update_spreads_in_options_chain_data(options_chain_data: List[dict], spreads_sizes: List[int]):
    groupped_options_chain = defaultdict(list)
    for doc in options_chain_data:
        groupped_options_chain[(doc["event_time"], doc["expiration_time"])].append(doc)
    for list_of_strikes in groupped_options_chain.values():
        update_spreads_for_groupped_event(list_of_strikes, spreads_sizes)


options_chain_data_by_hour = get_option_chain_data_by_hour(
    coin=Coin.btc,
    evet_type='call',
    iterval_size_in_minutes=12,
)
parsed_options_chain_data = parse_options_chain_data(
    options_chain_data=options_chain_data_by_hour,
    round_udl_price_sizes=[100, 1000],
    # round_udl_price_sizes=[100],
    round_mark_iv_sizes=[1, 5],
    )
update_spreads_in_options_chain_data(
    options_chain_data=parsed_options_chain_data,
    spreads_sizes=[2000, 4000, 6000, 8000, 10000, 5000],
    # spreads_sizes=[100, 200, 300, 400, 500, 600, 700, 800]
    )
save_data_in_excel(
    options_chain_data=parsed_options_chain_data,
    filename=f'options_chain_{Coin.btc}_CALL_16-11_to_17-11_V4',
    )



# with open('/home/droppy/files/option_chain_01-07_01-11.csv', 'w') as f:
#     dict_writer = csv.DictWriter(f, DeribitOptionLineToCsv.__fields__.keys())
#     dict_writer.writeheader()
#     for month, days in dates.items():
#         for day in days:
#             for hour in range(0, 24):
#                 try:
#                     rows = get_data_by_time_interval(datetime(
#                         year=2021, month=month, day=day, hour=hour, minute=0,
#                     ))
#                     newlist = sorted(rows, key=lambda d: d['event_time'])
#                     newlist = [get_parsed_data_pont(d) for d in newlist]
#                     dict_writer.writerows(newlist)
#                 except Exception as e:
#                     print(e)


# db_options_chain.create_indexes(
#     [
#         # IndexModel([
#         #     ('event_time', ASCENDING),
#         #     ('coin', ASCENDING),
#         #     ('type', ASCENDING),
#         #     ('expiration_time', ASCENDING),
#         #     ('strike_price', ASCENDING),
#         #     ('mark_price', ASCENDING),
#         # ]),
#         IndexModel(
#             [
#                 ('expiration_time', ASCENDING),

#             ]
#         ),
#     ]
# )
