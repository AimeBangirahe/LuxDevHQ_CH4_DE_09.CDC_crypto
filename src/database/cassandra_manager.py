import time
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import BatchStatement
from typing import List, Tuple
from config.config import Config


class CassandraManager:
    def __init__(self, max_retries=30, retry_delay=2):
        self.cluster = Cluster(Config.CASSANDRA_HOSTS, port=Config.CASSANDRA_PORT)

        # Retry connection
        for attempt in range(max_retries):
            try:
                self.session = self.cluster.connect()
                break
            except NoHostAvailable as e:
                if attempt < max_retries - 1:
                    print(f"Cassandra not ready (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to connect to Cassandra after {max_retries} attempts") from e

        self._create_keyspace()
        self.session.set_keyspace(Config.CASSANDRA_KEYSPACE)
        self._create_tables()

    def _create_keyspace(self):
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {Config.CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)

    def _create_tables(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS ticker_24h (
                symbol text,
                timestamp timestamp,
                price_change decimal,
                price_change_percent decimal,
                last_price decimal,
                volume decimal,
                quote_volume decimal,
                open_price decimal,
                high_price decimal,
                low_price decimal,
                PRIMARY KEY (symbol, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """)

        self.session.execute("""
            CREATE TABLE IF NOT EXISTS klines (
                symbol text,
                open_time timestamp,
                open_price decimal,
                high_price decimal,
                low_price decimal,
                close_price decimal,
                volume decimal,
                close_time timestamp,
                PRIMARY KEY (symbol, open_time)
            ) WITH CLUSTERING ORDER BY (open_time DESC)
        """)

    def insert_ticker_batch(self, records: List[Tuple]):
        batch = BatchStatement()
        prepared = self.session.prepare("""
            INSERT INTO ticker_24h (symbol, timestamp, price_change, price_change_percent,
                                   last_price, volume, quote_volume, open_price, high_price, low_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        for record in records:
            batch.add(prepared, (record[1], record[10], record[2], record[3],
                                record[4], record[5], record[6], record[7], record[8], record[9]))

        self.session.execute(batch)

    def insert_kline_batch(self, records: List[Tuple]):
        batch = BatchStatement()
        prepared = self.session.prepare("""
            INSERT INTO klines (symbol, open_time, open_price, high_price, low_price,
                               close_price, volume, close_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        for record in records:
            batch.add(prepared, (record[1], record[2], record[3], record[4],
                                record[5], record[6], record[7], record[8]))

        self.session.execute(batch)

    def close(self):
        self.cluster.shutdown()
