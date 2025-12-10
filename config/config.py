import os
from dotenv import load_dotenv
from pathlib import Path

# dotenv_path = Path('.env')
# load_dotenv(dotenv_path=dotenv_path)
load_dotenv()

class Config:
    BINANCE_BASE_URL = "https://api.binance.com"

    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)
    POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

    # Cassandra
    CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto_data")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # CDC
    USE_DEBEZIUM = os.getenv("USE_DEBEZIUM", "false").lower() == "true"

    # Pipeline
    FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 60))
    CDC_INTERVAL = int(os.getenv("CDC_INTERVAL", 10))
