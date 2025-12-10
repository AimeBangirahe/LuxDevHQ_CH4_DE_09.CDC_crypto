from datetime import datetime, timedelta
from src.database.postgres_manager import PostgresManager
from src.database.cassandra_manager import CassandraManager


class CDCReplicator:
    def __init__(self, postgres: PostgresManager, cassandra: CassandraManager):
        self.postgres = postgres
        self.cassandra = cassandra
        self.last_sync = datetime.now() - timedelta(days=1)

    def replicate_tickers(self):
        records = self.postgres.get_new_records("ticker_24h", self.last_sync)
        if records:
            self.cassandra.insert_ticker_batch(records)
            self.last_sync = max(r[11] for r in records)
            return len(records)
        return 0

    def replicate_klines(self):
        records = self.postgres.get_new_records("klines", self.last_sync)
        if records:
            self.cassandra.insert_kline_batch(records)
            self.last_sync = max(r[9] for r in records)
            return len(records)
        return 0

    def replicate_all(self):
        ticker_count = self.replicate_tickers()
        kline_count = self.replicate_klines()
        return ticker_count + kline_count
