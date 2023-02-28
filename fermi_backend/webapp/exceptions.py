import traceback
from contextlib import asynccontextmanager
import logging

from fastapi import HTTPException, status

from fermi_backend.webapp.webapp_models.generic_models import ResultResponse

logger = logging.getLogger(__name__)


class DatabaseException(Exception):
    pass


class UnknownDatabaseType(DatabaseException):
    pass


class DatabaseConnectionError(DatabaseException):
    pass


class AuthenticationException(Exception):
    pass


class UnknownAuthenticationProvider(AuthenticationException):
    pass


class AuthorizationException(Exception):
    pass


class DuplicatePrimaryKeyException(Exception):
    pass


class UnauthorizedUser(AuthorizationException):
    pass


class DiscoveryDocumentError(AuthorizationException):
    pass


class ProviderConnectionError(AuthorizationException):
    pass


@asynccontextmanager
async def exception_handling():
    try:
        yield
    except DatabaseConnectionError as exc:
        logger.exception(f"Failed to connect to the database: {repr(exc)}")
        yield ResultResponse(status=-1, message=f"Failed to connect to the database: {repr(exc)}",
                             debug=f"{str(exc)}:\n{traceback.format_exc()}")

    except UnauthorizedUser as exc:
        logger.warning(f"Failed to authorize user: {repr(exc)}")
        yield ResultResponse(status=status.HTTP_401_UNAUTHORIZED, message="User not authorized",
                             debug=f"{str(exc)}:\n{traceback.format_exc()}")
    except Exception as exc:
        logger.exception(repr(exc))
        yield ResultResponse(status=-1, message="An error has occurred. Please try again.",
                            debug=f"{str(exc)}:\n{traceback.format_exc()}")
