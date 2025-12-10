import requests
import json
from typing import List, Optional
from src.models.crypto_data import Ticker24h, Kline
from config.config import Config


class BinanceClient:
    def __init__(self):
        self.base_url = Config.BINANCE_BASE_URL
        self.session = requests.Session()

    def get_24h_tickers(self, symbols: Optional[List[str]] = None) -> List[Ticker24h]:
        endpoint = f"{self.base_url}/api/v3/ticker/24hr"
        params = {"symbols": json.dumps(symbols, separators=(',', ':'))} if symbols else {}

        response = self.session.get(endpoint, params=params)
        response.raise_for_status()

        data = response.json()
        return [Ticker24h.from_api(item) for item in data]

    def get_klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> List[Kline]:
        endpoint = f"{self.base_url}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}

        response = self.session.get(endpoint, params=params)
        response.raise_for_status()

        data = response.json()
        return [Kline.from_api(symbol, item) for item in data]

    def get_price(self, symbol: str) -> float:
        endpoint = f"{self.base_url}/api/v3/ticker/price"
        params = {"symbol": symbol}

        response = self.session.get(endpoint, params=params)
        response.raise_for_status()

        return float(response.json()['price'])
