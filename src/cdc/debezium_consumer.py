import json
import base64
from datetime import datetime
from confluent_kafka import Consumer
from decimal import Decimal
from src.database.cassandra_manager import CassandraManager
from config.config import Config


class DebeziumConsumer:
    def __init__(self, cassandra: CassandraManager):
        self.cassandra = cassandra
        self.consumer = Consumer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'crypto-cdc-consumer',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        self.consumer.subscribe(['crypto.public.ticker_24h', 'crypto.public.klines'])

    @staticmethod
    def decode_decimal(encoded_value, scale):
        if encoded_value is None:
            return Decimal('0')
        byte_data = base64.b64decode(encoded_value)
        value = int.from_bytes(byte_data, byteorder='big', signed=True)
        return Decimal(value) / Decimal(10 ** scale)

    def process_ticker_event(self, event):
        if event['payload']['op'] in ['c', 'u']:  # create or update
            data = event['payload']['after']
            record = (
                None,  # id placeholder
                data['symbol'],
                self.decode_decimal(data['price_change'], 8),
                self.decode_decimal(data['price_change_percent'], 4),
                self.decode_decimal(data['last_price'], 8),
                self.decode_decimal(data['volume'], 8),
                self.decode_decimal(data['quote_volume'], 8),
                self.decode_decimal(data['open_price'], 8),
                self.decode_decimal(data['high_price'], 8),
                self.decode_decimal(data['low_price'], 8),
                datetime.fromtimestamp(data['timestamp'] / 1000000)
            )
            self.cassandra.insert_ticker_batch([record])

    def process_kline_event(self, event):
        if event['payload']['op'] in ['c', 'u']:  # create or update
            data = event['payload']['after']
            record = (
                None,  # id placeholder
                data['symbol'],
                datetime.fromtimestamp(data['open_time'] / 1000000),
                self.decode_decimal(data['open_price'], 8),
                self.decode_decimal(data['high_price'], 8),
                self.decode_decimal(data['low_price'], 8),
                self.decode_decimal(data['close_price'], 8),
                self.decode_decimal(data['volume'], 8),
                datetime.fromtimestamp(data['close_time'] / 1000000)
            )
            self.cassandra.insert_kline_batch([record])

    def consume(self):
        print("Starting Debezium CDC consumer...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()

                    if topic == 'crypto.public.ticker_24h':
                        self.process_ticker_event(event)
                        print(f"[{datetime.now()}] Replicated ticker: {event['payload']['after']['symbol']}")
                    elif topic == 'crypto.public.klines':
                        self.process_kline_event(event)
                        print(f"[{datetime.now()}] Replicated kline: {event['payload']['after']['symbol']}")

                except Exception as e:
                    print(f"Error processing event: {e}")
                    continue
        except KeyboardInterrupt:
            pass

    def close(self):
        self.consumer.close()
