import traceback
from contextlib import asynccontextmanager
import logging

from fastapi import HTTPException
from starlette import status
from datetime import datetime
from fermi_backend.webapp import CONSTS

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
        logger.exception(f"Failed to connect to the database: {repr(exc)}. Detailed: {traceback.format_exc()}")
        raise HTTPException(status_code=CONSTS.HTTP_600_DATABASE_CONNECTION_FAILED,
                            detail=f"Cannot serve results at the moment. Please try again: : {str(exc)}:\n{traceback.format_exc()}")
    except UnauthorizedUser as exc:
        logger.warning(f"Failed to authorize user: {repr(exc)}. Detailed: {traceback.format_exc()}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail=f"User not authorized: {str(exc)}:\n{traceback.format_exc()}")
    except Exception as exc:
        logger.exception(repr(exc)+ f" Detailed: {traceback.format_exc()}")
        raise HTTPException(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"An error has occurred. Please try again: {str(exc)}:\n{traceback.format_exc()}")


def router_error_handler(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabaseConnectionError as e:
            logger.exception(f"Failed to connect to the database: {repr(e)}")
            return ResultResponse(status_code=CONSTS.HTTP_600_DATABASE_CONNECTION_FAILED, message=f"Failed to connect to the database: {repr(e)}",
                                  debug=f"{str(e)}:\n{traceback.format_exc()}", date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
        except UnauthorizedUser as e:
            logger.warning(f"Failed to authorize user: {repr(e)}")
            return ResultResponse(status_code=CONSTS.HTTP_401_UNAUTHORIZED,
                                  message=f"Failed to authorize user: {repr(e)}",
                                  debug=f"{str(e)}:\n{traceback.format_exc()}",
                                  date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
        except Exception as e:
            logger.warning(f"An error has occurred. Please try again: {e}")
            return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR,
                                  message="An error has occurred. Please try again.",
                                  debug=f"{str(e)}:\n{traceback.format_exc()}",
                                  date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
    return inner