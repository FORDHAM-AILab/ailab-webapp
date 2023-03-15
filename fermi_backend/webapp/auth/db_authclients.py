from abc import ABC, abstractmethod
import datetime
import hashlib
import logging
from functools import lru_cache
from typing import List, Optional
from uuid import uuid4

from sqlalchemy import create_engine
from fermi_backend.webapp import helpers
from fermi_backend.webapp.helpers import mysql_session_scope
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
)
from passlib.hash import bcrypt
from pymongo.errors import ServerSelectionTimeoutError

from fermi_backend.webapp import config
from fermi_backend.webapp.exceptions import (
    DatabaseConnectionError,
    UnknownDatabaseType,
    DuplicatePrimaryKeyException
)

from fermi_backend.webapp.webapp_models.db_models import (
    InternalUser,
)
from fermi_backend.webapp.webapp_models.auth_models import (
    ExternalUser,
)

logger = logging.getLogger(__name__)


@lru_cache()
def get_db_client(db_type):
    """
    Works out the correct database client based on
    the database type provided in the configuration
    Raises:
        backend.exceptions.UnknownDatabaseType
        """

    for client_cls in DatabaseClient.__subclasses__():
        try:
            if client_cls().meets_condition(db_type):
                return client_cls()
        except KeyError:
            continue

    raise UnknownDatabaseType(db_type)


class DatabaseClient(ABC):
    """ Database client interface """

    @abstractmethod
    def meets_condition(self, db_type: str):
        """
        Checks whether this type of database client matches the one defined in the configuration.
        Makes sure the correct client will be instantiated.

        Args:
            db_type: One of database types as defined in config
        """
        ...

    @abstractmethod
    async def get_user_by_external_sub_id(self, external_user: ExternalUser) -> InternalUser:
        """
        Returns a user from the database, based on the external sub_id of
        the current authentication provider (i.e Google, FaceBook etc)
        Args:
            external_user: An object representing a user with information
                            based on the external provider's service.
        Returns:
            internal_user: A user objects as defined in this application
        """
        ...

    @abstractmethod
    async def get_user_by_internal_sub_id(self, internal_sub_id: str) -> InternalUser:
        """
        Returns a user from the database, based on the internal sub_id
        Args:
            internal_sub_id: The unique id of the user as defined in this application
        Returns:
            internal_user: A user objects as defined in this application
        """
        ...

    @abstractmethod
    async def create_internal_user(self, external_user: ExternalUser) -> InternalUser:
        """
        Creates a user in the database based on the external sub_id of
        the current authentication provider (i.e Google, FaceBook etc)
        The user will also be assigned an internal sub_id for authentication
        within the internal system (backend application)
        Args:
            external_user: An object representing a user with information
                            based on the external provider's service.
        Returns:
            internal_user: A user objects as defined in this application
        """
        ...

    @abstractmethod
    async def update_internal_user(self, internal_user: InternalUser) -> InternalUser:
        """
        Updates a user in the database
        Args:
            internal_user: A user objects as defined in this application
        Returns:
            internal_user: A user objects as defined in this application
        """
        ...

    @staticmethod
    async def _encrypt_external_sub_id(external_user: ExternalUser) -> str:
        """
        It encrypts the subject id received from the external provider. These ids are
        used to uniquely identify a user in the system of the external provider and
        are usually public. However, it is better to be stored encrypted just in case.
        Args:
            external_user: An object representing a user with information
                            based on the external provider's service.
        Returns:
            encrypted_external_sub_id: The encrypted external subject id
        """
        salt = external_user.email.lower()
        salt = salt.replace(" ", "")
        # Hash the salt so that the email is not plain text visible in the database
        salt = hashlib.sha256(salt.encode()).hexdigest()
        # bcrypt requires a 22 char salt
        if len(salt) > 21:
            salt = salt[:21]

        # As per passlib the last character of the salt should always be one of [.Oeu]
        salt = salt + "O"

        encrypted_external_sub_id = bcrypt.using(salt=salt).hash(external_user.external_sub_id)
        return encrypted_external_sub_id


class MongoDBClient(DatabaseClient):
    """ Wrapper around an AsyncIOMotorClient object. """

    def __init__(self):
        mongodb_uri = (
            f"mongodb://"
            f"{config.MONGODB_HOST}:"
            f"{config.MONGODB_PORT}/"
            f"{config.MONGODB_DATABASE}?"
            f"authSource={config.MONGODB_COLLECTION}"
        )
        self._motor_client = AsyncIOMotorClient(mongodb_uri)
        # Mongo database
        self._db = self._motor_client[config.MONGODB_DATABASE]
        # Mongo collections
        self._users_coll = self._db["users"]
        self._session = None

    def meets_condition(self, db_type):
        return db_type == config.MONGO_DB

    async def close_connection(self):
        self._motor_client.close()

    async def start_session(self):
        try:
            self._session = await self._motor_client.start_session()
        except ServerSelectionTimeoutError as exc:
            raise DatabaseConnectionError(exc)

    async def end_session(self):
        await self._session.end_session()

    async def get_user_by_external_sub_id(self, external_user: ExternalUser) -> InternalUser:
        internal_user = None

        encrypted_external_sub_id = await self._encrypt_external_sub_id(external_user)

        mongo_user = await self._users_coll.find_one({'external_sub_id': encrypted_external_sub_id})

        if mongo_user:
            internal_user = InternalUser(
                internal_sub_id=mongo_user["internal_sub_id"],
                external_sub_id=mongo_user["external_sub_id"],
                username=mongo_user["username"],
                created_at=mongo_user["created_at"],
            )

        return internal_user

    async def get_user_by_internal_sub_id(self, internal_sub_id: str) -> InternalUser:
        internal_user = None

        mongo_user = await self._users_coll.find_one({'_id': internal_sub_id})

        if mongo_user:
            internal_user = InternalUser(
                internal_sub_id=mongo_user["internal_sub_id"],
                external_sub_id=mongo_user["external_sub_id"],
                username=mongo_user["username"],
                created_at=mongo_user["created_at"],
            )

        return internal_user

    async def create_internal_user(self, external_user: ExternalUser) -> InternalUser:
        encrypted_external_sub_id = await self._encrypt_external_sub_id(external_user)
        unique_identifier = str(uuid4())

        result = await self._users_coll.insert_one(
            dict(
                _id=unique_identifier,
                internal_sub_id=unique_identifier,
                external_sub_id=encrypted_external_sub_id,
                username=external_user.username,
                created_at=datetime.datetime.utcnow(),
            )
        )

        mongo_user_id = result.inserted_id

        mongo_user = await self._users_coll.find_one({'_id': mongo_user_id})

        internal_user = InternalUser(
            internal_sub_id=mongo_user["internal_sub_id"],
            external_sub_id=mongo_user["external_sub_id"],
            username=mongo_user["username"],
            created_at=mongo_user["created_at"],
        )

        return internal_user

    async def update_internal_user(self, internal_user: InternalUser) -> InternalUser:
        updated_user = None

        result = await self._users_coll.update_one(
            {"internal_sub_id": internal_user.internal_sub_id},
            {"$set": internal_user.dict()}
        )

        if result.modified_count:
            updated_user = internal_user

        return updated_user


class AuthMySQLClient(DatabaseClient):

    def __init__(self):
        pass

    def meets_condition(self, db_type):
        return db_type == config.RDB

    async def get_user_by_external_sub_id(self, external_user: ExternalUser) -> InternalUser:
        internal_user = None

        encrypted_external_sub_id = await self._encrypt_external_sub_id(external_user)
        async with helpers.mysql_session_scope() as session:
            result = await session.execute(f"""SELECT * FROM users WHERE external_sub_id = "{encrypted_external_sub_id}" """)
            result = helpers.parse_sql_results(result)


        if len(result) > 0:
            internal_user = InternalUser(
                internal_sub_id=result[0]["internal_sub_id"],
                external_sub_id=result[0]["external_sub_id"],
                username=result[0]["username"],
                email=result[0]["email"],
                created_at=result[0]["created_at"],
            )

        return internal_user

    async def get_user_by_internal_sub_id(self, internal_sub_id: str) -> InternalUser:
        internal_user = None
        async with helpers.mysql_session_scope() as session:
            result = await session.execute(f"""SELECT * FROM users WHERE internal_sub_id = "{internal_sub_id}" """)
            result = helpers.parse_sql_results(result)

        if len(result) > 0:
            internal_user = InternalUser(**result[0])

        return internal_user

    async def create_internal_user(self, external_user: ExternalUser) -> Optional[InternalUser]:
        encrypted_external_sub_id = await self._encrypt_external_sub_id(external_user)
        unique_identifier = str(uuid4())
        created_at = datetime.datetime.utcnow()
        async with helpers.mysql_session_scope() as session:
            await session.execute(f"""INSERT INTO users (internal_sub_id, external_sub_id, username, email, created_at) 
                             VALUES ("{unique_identifier}", "{encrypted_external_sub_id}", 
                             "{external_user.username}", "{external_user.email}", "{created_at}")""")

        internal_user = InternalUser(
            internal_sub_id=unique_identifier,
            external_sub_id=encrypted_external_sub_id,
            username=external_user.username,
            email=external_user.email,
            created_at=created_at,
        )

        return internal_user

    async def update_internal_user(self, internal_user: InternalUser) -> Optional[InternalUser]:

        async with helpers.mysql_session_scope() as session:
            result = await session.execute(f"""UPDATE users SET external_sub_id = "{internal_user.external_sub_id}",
                                                          username = "{internal_user.username}",
                                                          email = "{internal_user.email}"
                                         WHERE internal_sub_id = "{internal_user.internal_sub_id}" """)

        if result.rowcount > 0:
            return internal_user

        return None