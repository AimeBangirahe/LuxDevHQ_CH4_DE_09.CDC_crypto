from src.pipeline import Pipeline
from config.config import Config


if __name__ == "__main__":
    pipeline = Pipeline(use_debezium=Config.USE_DEBEZIUM)
    pipeline.run()
