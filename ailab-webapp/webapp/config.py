import os
from dotenv import load_dotenv
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

# MySQL
MYSQL_CONNECTION_URL = os.environ.get("MYSQL_CONNECTION_URL", None)

# Google login
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", None)
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", None)
GOOGLE_DISCOVERY_URL = "https://accounts.google.com/.well-known/openid-configuration"
GOOGLE_REDIRECT_URL = "http://localhost:8888/google-login-callback/"

# Front end endpoint
FRONTEND_URL = "http://localhost:3000"

# JWT access token configuration: "openssl rand -hex 32"
JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", None)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
AUTH_TOKEN_EXPIRE_MINUTES = 1



