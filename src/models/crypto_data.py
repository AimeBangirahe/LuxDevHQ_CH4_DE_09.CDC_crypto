from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Ticker24h:
    symbol: str
    price_change: float
    price_change_percent: float
    last_price: float
    volume: float
    quote_volume: float
    open_price: float
    high_price: float
    low_price: float
    timestamp: datetime

    @classmethod
    def from_api(cls, data: dict):
        return cls(
            symbol=data['symbol'],
            price_change=float(data['priceChange']),
            price_change_percent=float(data['priceChangePercent']),
            last_price=float(data['lastPrice']),
            volume=float(data['volume']),
            quote_volume=float(data['quoteVolume']),
            open_price=float(data['openPrice']),
            high_price=float(data['highPrice']),
            low_price=float(data['lowPrice']),
            timestamp=datetime.now()
        )


@dataclass
class Kline:
    symbol: str
    open_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    close_time: datetime

    @classmethod
    def from_api(cls, symbol: str, data: list):
        return cls(
            symbol=symbol,
            open_time=datetime.fromtimestamp(data[0] / 1000),
            open_price=float(data[1]),
            high_price=float(data[2]),
            low_price=float(data[3]),
            close_price=float(data[4]),
            volume=float(data[5]),
            close_time=datetime.fromtimestamp(data[6] / 1000)
        )
