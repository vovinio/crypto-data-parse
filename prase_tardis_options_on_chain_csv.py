import csv
from datetime import datetime, timezone
from enum import Enum
from io import StringIO
from typing import Union, Optional
from pydantic import BaseModel, validator


DIVIDER_FROM_MICROSECONDS_TO_SECONDS = 1000000


class Coin(str, Enum):
    btc: str = 'BTC'
    eth: str = 'ETH'


class OptionType(str, Enum):
    call: str = 'CALL'
    put: str = 'PUT'


class OptionBase(BaseModel):
    symbol: str
    event_time: datetime
    expiration_time: datetime
    type: OptionType
    coin: Coin
    strike_price: int
    open_interest: float
    last_price: Union[float, None]
    bid_price: Union[float, None]
    bid_amount: Union[float, None]
    bid_iv: Union[float, None]
    ask_price: Union[float, None]
    ask_amount: Union[float, None]
    ask_iv: Union[float, None]
    mark_price: Union[float, None]
    mark_iv: Union[float, None]
    underlying_price: Union[float, None]
    delta: Union[float, None]
    gamma: Union[float, None]
    vega: Union[float, None]
    theta: Union[float, None]
    rho: Union[float, None]


class OptionPoint(OptionBase):
    pass


class AdditionalCalculatedOptionParams(BaseModel):
    mid_price: float
    round_udl_price: int


class DeribitOptionLineToCsv(BaseModel):
    event_time: datetime
    expiration_time: datetime

    date: str
    year: int
    month: int
    day: int
    hour_min: str

    underlying_price: float

    symbol: str
    expiration: str
    tte: str
    tte_days: int
    strike_price: int

    last_price: Union[float, None]
    mark_price: float
    mark_price_usd: float
    bid_price: Union[float, None]
    ask_price: Union[float, None]
    bid_iv: Union[float, None]
    ask_iv: Union[float, None]
    mark_iv: float

    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float

    open_interest: float
    ask_amount: Union[float, None]
    bid_amount: Union[float, None]

    # @validator('expiration_time')
    # def verify_expiration_time(cls, value, values):
    #     if value:
    #         values['event_time'] = values['event_time'].strftime("%d%m%Y%H%M")
    #         return value.strftime("%d%m%y")


def calculate_round_unit(round_size: int, unit_to_round: float) -> int:
    if unit_to_round % round_size >= round_size/2:
        return int(unit_to_round + (round_size - unit_to_round % round_size))
    else:
        return int(unit_to_round - unit_to_round % round_size)


def calculate_round_udl_price_by_coin(coin: Coin, underlying_price: float) -> int:
    if coin == Coin.btc:
        return calculate_round_unit(round_size=100, unit_to_round=underlying_price)
    elif coin == Coin.eth:
        return calculate_round_unit(round_size=10, unit_to_round=underlying_price)


def calculate_mid_price(bid_price: float, ask_price: float) -> float:
    if not bid_price or not ask_price:
        return 0
    return (bid_price + ask_price) / 2


def calculate_distance_price(round_udl_price: int, strike_price: int) -> int:
    return strike_price - round_udl_price


def parse_single_deribit_option_chain_line(data: dict) -> OptionPoint:
    for key, value in data.items():
        if value == '':
            data[key] = None
    option_type = OptionType(data['type'].upper())
    coin = Coin(data['symbol'].split('-')[0])
    event_time = datetime.fromtimestamp(
        int(data['timestamp'])/DIVIDER_FROM_MICROSECONDS_TO_SECONDS, tz=timezone.utc
    )
    expiration_time = datetime.fromtimestamp(
        int(data['expiration']) / DIVIDER_FROM_MICROSECONDS_TO_SECONDS, tz=timezone.utc
    )
    csv_option_line = OptionPoint(
        symbol=data['symbol'],
        type=option_type,
        coin=coin,
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
        underlying_price=data['underlying_price'],
        delta=data['delta'],
        gamma=data['gamma'],
        vega=data['vega'],
        theta=data['theta'],
        rho=data['rho'],
        event_time=event_time,
        expiration_time=expiration_time,
    )
    return csv_option_line
    # return DeribitOptionLine(
    #     **csv_option_line.dict(),
    #     coin=coin,
    #     mid_price=calculate_mid_price(
    #         bid_price=csv_option_line.bid_price, ask_price=csv_option_line.ask_price
    #     ),
    #     round_udl_price=calculate_round_udl_price_by_coin(
    #         coin=coin, underlying_price=csv_option_line.underlying_price
    #     ),
    # )
