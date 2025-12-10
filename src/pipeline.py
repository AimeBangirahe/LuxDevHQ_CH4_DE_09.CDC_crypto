import time
import threading
from datetime import datetime
from typing import List, Optional
from src.clients.binance_client import BinanceClient
from src.database.postgres_manager import PostgresManager
from src.database.cassandra_manager import CassandraManager
from src.cdc.replicator import CDCReplicator
from config.config import Config


class Pipeline:
    def __init__(self, symbols: Optional[List[str]] = None, use_debezium: bool = False):
        self.binance = BinanceClient()
        self.postgres = PostgresManager()
        self.cassandra = CassandraManager()
        self.use_debezium = use_debezium
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT"]

        if use_debezium:
            from src.cdc.debezium_consumer import DebeziumConsumer
            self.debezium = DebeziumConsumer(self.cassandra)
            self.cdc_thread = None
        else:
            self.cdc = CDCReplicator(self.postgres, self.cassandra)

    def fetch_and_store_tickers(self):
        tickers = self.binance.get_24h_tickers(self.symbols)
        self.postgres.insert_tickers(tickers)
        print(f"[{datetime.now()}] Stored {len(tickers)} ticker records")

    def fetch_and_store_klines(self):
        for symbol in self.symbols:
            klines = self.binance.get_klines(symbol, interval="1m", limit=10)
            self.postgres.insert_klines(klines)
        print(f"[{datetime.now()}] Stored klines for {len(self.symbols)} symbols")

    def start_cdc_consumer(self):
        self.cdc_thread = threading.Thread(target=self.debezium.consume, daemon=True)
        self.cdc_thread.start()

    def run_custom_cdc(self):
        count = self.cdc.replicate_all()
        if count > 0:
            print(f"[{datetime.now()}] Replicated {count} records to Cassandra")

    def run(self):
        cdc_mode = "Debezium" if self.use_debezium else "Custom timestamp-based"
        print(f"Starting crypto data pipeline with {cdc_mode} CDC...")

        if self.use_debezium:
            self.start_cdc_consumer()

        last_fetch = 0
        last_cdc = 0

        try:
            while True:
                current = time.time()

                if current - last_fetch >= Config.FETCH_INTERVAL:
                    self.fetch_and_store_tickers()
                    self.fetch_and_store_klines()
                    last_fetch = current

                if not self.use_debezium and current - last_cdc >= Config.CDC_INTERVAL:
                    self.run_custom_cdc()
                    last_cdc = current

                time.sleep(1)

        except KeyboardInterrupt:
            print("\nShutting down pipeline...")
        finally:
            self.postgres.close()
            if self.use_debezium:
                self.debezium.close()
            self.cassandra.close()
