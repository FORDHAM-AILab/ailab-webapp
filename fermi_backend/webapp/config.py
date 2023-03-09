import ast
import os
from dotenv import load_dotenv

from pydantic import BaseSettings

load_dotenv()

# Supported database types by name
MONGO_DB = "mongodb"
REDIS_DB = 'redis'
RDB = 'mysql'

# Supported authentication providers by name
GOOGLE = "google-oidc"

# Selected database type to use
DATABASE_TYPE = RDB
CACHE_DB_TYPE = REDIS_DB


# MongoDB Replica Set
MONGODB_HOST = os.environ.get("MONGODB_HOST", "127.0.0.1")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", 27017))
MONGODB_COLLECTION = "testdb"
MONGODB_DATABASE = "testdb"
ENV = os.environ.get("ENV", "dev")
if ENV == 'dev':
    if bool(os.environ.get("DOCKER", False)):
        MYSQL_CONNECTION_URL = os.environ.get("MYSQL_CONNECTION_URL_DEV_DOCKER", None)
        MYSQL_CONNECTION_URL_SYNC = os.environ.get("MYSQL_CONNECTION_URL_SYNC_DEV_DOCKER", None)
    else:
        MYSQL_CONNECTION_URL = os.environ.get("MYSQL_CONNECTION_URL_DEV", None)
        MYSQL_CONNECTION_URL_SYNC = os.environ.get("MYSQL_CONNECTION_URL_SYNC_DEV_DOCKER", None)
    FRONTEND_URL = os.environ.get("FRONTEND_URL_DEV", None)
    BACKEND_URL = os.environ.get("BACKEND_URL_DEV", None)
    RESPONSE_COOKIE = ast.literal_eval(os.environ.get("RESPONSE_COOKIE_DEV", None))
else:
    if bool(os.environ.get("DOCKER", False)):
        MYSQL_CONNECTION_URL = os.environ.get("MYSQL_CONNECTION_URL_PROD_DOCKER", None)
        MYSQL_CONNECTION_URL_SYNC = os.environ.get("MYSQL_CONNECTION_URL_PROD_DOCKER", None)
    else:
        MYSQL_CONNECTION_URL = os.environ.get("MYSQL_CONNECTION_URL_SYNC_PROD", None)
        MYSQL_CONNECTION_URL_SYNC = os.environ.get("MYSQL_CONNECTION_URL_SYNC_PROD_DOCKER", None)
    FRONTEND_URL = os.environ.get("FRONTEND_URL_PROD", None)
    BACKEND_URL = os.environ.get("BACKEND_URL_PROD", None)
    RESPONSE_COOKIE = ast.literal_eval(os.environ.get("RESPONSE_COOKIE_PROD", None))

if bool(os.environ.get("DOCKER", True)):
    REDIS_ENDPOINT=os.environ.get("REDIS_ENDPOINT_DOCKER")
else:
    REDIS_ENDPOINT = os.environ.get("REDIS_ENDPOINT")

# Google login
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", None)
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", None)
GOOGLE_DISCOVERY_URL = "https://accounts.google.com/.well-known/openid-configuration"
GOOGLE_REDIRECT_URL = f"{BACKEND_URL}/google-login-callback/"


# JWT access token configuration: "openssl rand -hex 32"
JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", None)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
AUTH_TOKEN_EXPIRE_MINUTES = 15





