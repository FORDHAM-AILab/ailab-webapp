from aiocache import Cache
from . import config
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from redis import Redis


# Initialize cache
if config.ENV == 'prod':
    cache = Cache(Cache.REDIS, endpoint='redis', port=6379, namespace='main')
else:
    cache = Cache(Cache.REDIS)
engine = create_async_engine(config.PSQL_CONNECTION_URL, echo=False)
psql_session_factory = sessionmaker(engine, class_=AsyncSession)
# cache_db_client = get_db_client(config.CACHE_DB_TYPE)
redis_cache = Redis(host=config.REDIS_ENDPOINT, db=1)

