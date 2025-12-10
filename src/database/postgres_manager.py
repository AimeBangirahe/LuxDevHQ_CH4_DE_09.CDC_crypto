import psycopg2
from psycopg2.extras import execute_batch
from typing import List
from datetime import datetime
from config.config import Config
from src.models.crypto_data import Ticker24h, Kline


class PostgresManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        self.conn.autocommit = False
        self._create_tables()

    def _create_tables(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ticker_24h (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    price_change DECIMAL(20, 8),
                    price_change_percent DECIMAL(10, 4),
                    last_price DECIMAL(20, 8),
                    volume DECIMAL(20, 8),
                    quote_volume DECIMAL(20, 8),
                    open_price DECIMAL(20, 8),
                    high_price DECIMAL(20, 8),
                    low_price DECIMAL(20, 8),
                    timestamp TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_ticker_symbol_timestamp ON ticker_24h(symbol, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_ticker_updated_at ON ticker_24h(updated_at);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS klines (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    open_time TIMESTAMP NOT NULL,
                    open_price DECIMAL(20, 8),
                    high_price DECIMAL(20, 8),
                    low_price DECIMAL(20, 8),
                    close_price DECIMAL(20, 8),
                    volume DECIMAL(20, 8),
                    close_time TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, open_time)
                );
                CREATE INDEX IF NOT EXISTS idx_klines_symbol_time ON klines(symbol, open_time DESC);
                CREATE INDEX IF NOT EXISTS idx_klines_updated_at ON klines(updated_at);
            """)
            self.conn.commit()

    def insert_tickers(self, tickers: List[Ticker24h]):
        with self.conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO ticker_24h (symbol, price_change, price_change_percent, last_price,
                                       volume, quote_volume, open_price, high_price, low_price, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [(t.symbol, t.price_change, t.price_change_percent, t.last_price,
                   t.volume, t.quote_volume, t.open_price, t.high_price, t.low_price, t.timestamp)
                  for t in tickers])
            self.conn.commit()

    def insert_klines(self, klines: List[Kline]):
        with self.conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO klines (symbol, open_time, open_price, high_price, low_price,
                                   close_price, volume, close_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, open_time) DO UPDATE SET
                    close_price = EXCLUDED.close_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    volume = EXCLUDED.volume,
                    updated_at = CURRENT_TIMESTAMP
            """, [(k.symbol, k.open_time, k.open_price, k.high_price, k.low_price,
                   k.close_price, k.volume, k.close_time) for k in klines])
            self.conn.commit()

    def get_new_records(self, table: str, last_timestamp: datetime):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT * FROM {table}
                WHERE updated_at > %s
                ORDER BY updated_at ASC
            """, (last_timestamp,))
            return cur.fetchall()

    def close(self):
        self.conn.close()
