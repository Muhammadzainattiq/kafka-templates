from starlette.config import Config
from starlette.datastructures import Secret

try:
    config = Config(".env")
except FileNotFoundError:
    config = Config()

DATABASE_URL = config("DATABASE_URL", cast=Secret)

TEST_DATABASE_URL = config("TEST_DATABASE_URL", cast=Secret)

KAFKA_BOOTSTRAP_SERVER = config("KAFKA_BOOTSTRAP_SERVER", cast=str)

KAFKA_TOPIC = config("KAFKA_TOPIC", cast=str)

KAFKA_CONSUMER_GROUP_ID = config("KAFKA_CONSUMER_GROUP_ID", cast=str)

