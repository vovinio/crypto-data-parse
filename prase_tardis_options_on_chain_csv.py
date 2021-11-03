import csv
from datetime import datetime
from enum import Enum
from typing import Union
from pydantic import BaseModel


DIVIDER_FROM_MICROSECONDS_TO_SECONDS = 1000000


class Coin(str, Enum):
    btc: str = 'BTC'
    eth: str = 'ETH'


class DeribitCsvOptionLine(BaseModel):
    exchange: str
    symbol: str
    event_time: datetime
    expiration_time: datetime
    type: str
    strike_price: int
    open_interest: float
    last_price: Union[float, None]
    bid_price: Union[float, None]
    bid_amount: Union[float, None]
    bid_iv: Union[float, None]
    ask_price: Union[float, None]
    ask_amount: Union[float, None]
    ask_iv: Union[float, None]
    mark_price: float
    mark_iv: float
    underlying_index: str
    underlying_price: float
    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float


class AdditionalCalculatedOptionParams(BaseModel):
    coin: Coin
    mid_price: float
    round_udl_price: int


class DeribitOptionLine(DeribitCsvOptionLine, AdditionalCalculatedOptionParams):
    pass


def calculate_round_udl_price(coin: Coin, underlying_price: float) -> int:
    if coin == Coin.btc:
        if underlying_price % 100 >= 50:
            return int(underlying_price + (100 - underlying_price % 100))
        else:
            return int(underlying_price - underlying_price % 100)
    elif coin == Coin.eth:
        if underlying_price % 10 >= 5:
            return int(underlying_price + (10 - underlying_price % 10))
        else:
            return int(underlying_price - underlying_price % 10)


def calculate_mid_price(bid_price: float, ask_price: float) -> float:
    if not bid_price or not ask_price:
        return 0
    return (bid_price + ask_price) / 2


def parse_single_deribit_option_chain_line(data: dict) -> DeribitOptionLine:
    for key, value in data.items():
        if value == '':
            data[key] = 0
    csv_option_line = DeribitCsvOptionLine(
        exchange=data['exchange'],
        symbol=data['symbol'],
        type=data['type'].lower(),
        strike_price=data['strike_price'],
        open_interest=data['open_interest'],
        last_price=data['last_price'],
        bid_price=data['bid_price'],
        bid_amount=data['bid_amount'],
        bid_iv=data['bid_iv'],
        ask_price=data['ask_price'],
        ask_amount=data['ask_amount'],
        ask_iv=data['ask_iv'],
        mark_price=data['mark_price'],
        mark_iv=data['mark_iv'],
        underlying_index=data['underlying_index'],
        underlying_price=data['underlying_price'],
        delta=data['delta'],
        gamma=data['gamma'],
        vega=data['vega'],
        theta=data['theta'],
        rho=data['rho'],
        event_time=datetime.fromtimestamp(
            int(data['timestamp'])/DIVIDER_FROM_MICROSECONDS_TO_SECONDS
        ),
        expiration_time=datetime.fromtimestamp(
            int(data['expiration'])/DIVIDER_FROM_MICROSECONDS_TO_SECONDS
        ),
    )
    coin = Coin(csv_option_line.symbol.split('-')[0])
    return DeribitOptionLine(
        **csv_option_line.dict(),
        coin=coin,
        mid_price=calculate_mid_price(
            bid_price=csv_option_line.bid_price, ask_price=csv_option_line.ask_price
        ),
        round_udl_price=calculate_round_udl_price(
            coin=coin, underlying_price=csv_option_line.underlying_price
        ),
    )
