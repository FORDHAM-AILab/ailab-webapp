from aiocache import Cache
from . import config
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Initialize cache
cache = Cache(Cache.REDIS, endpoint='127.0.0.1', port=6379, namespace='main')
engine = create_async_engine(config.MYSQL_CONNECTION_URL, echo=True)
mysql_session_factory = sessionmaker(engine, class_=AsyncSession)
# cache_db_client = get_db_client(config.CACHE_DB_TYPE)