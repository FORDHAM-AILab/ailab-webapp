import os
import sys
import uuid
from datetime import datetime

sys.path.append(os.getcwd())

import logging
from starlette.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import uvicorn
from fastapi import Request
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import JSONResponse
from webapp.routers import auth, aws, data, game, options, portfolio, stock, users

app = FastAPI()

app.include_router(auth.router)
app.include_router(aws.router)
app.include_router(data.router)
app.include_router(game.router)
app.include_router(options.router)
app.include_router(portfolio.router)
app.include_router(stock.router)
app.include_router(users.router)

# TODO: cache the current object

log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path, disable_existing_loggers=False, )
logger = logging.getLogger(__name__)

HTTP_ORIGIN = ['http://127.0.0.1:8888',
               'http://localhost:3000',
               'http://localhost:8888',
               'http://127.0.0.1:3000']
app.add_middleware(
    CORSMiddleware,
    allow_origins=HTTP_ORIGIN,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.add_middleware(SessionMiddleware, secret_key='!secret')


@app.middleware("http")
async def setup_request(request: Request, call_next) -> JSONResponse:
    """
    A middleware for setting up a request. It creates a new request_id
    and adds some basic metrics.
    Args:
        request: The incoming request
        call_next (obj): The wrapper as per FastAPI docs
    Returns:
        response: The JSON response
    """
    response = await call_next(request)

    return response


@app.middleware("http")
async def log_requests(request: Request, call_next):
    idem = uuid.uuid4()
    logger.info(f"rid={idem} start request path={request.url.path}")
    start_time = datetime.now()

    response = await call_next(request)

    process_time = (datetime.now() - start_time).total_seconds()
    formatted_process_time = '{0:.2f}'.format(process_time)
    logger.info(f"rid={idem} completed_in={formatted_process_time}ms status_code={response.status_code}")

    return response


if __name__ == '__main__':
    uvicorn.run('app:app', port=8888, host='127.0.0.1', log_level="info", reload=True)
