from aiocache import Cache

from webapp import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Initialize cache
cache = Cache()
mysql_session_factory = sessionmaker(bind=create_engine(config.MYSQL_CONNECTION_URL))
# cache_db_client = get_db_client(config.CACHE_DB_TYPE)