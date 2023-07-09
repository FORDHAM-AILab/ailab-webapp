import os
import random
import string
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import starlette.responses

from fermi_backend.webapp import helpers
sys.path.append(os.getcwd())
import logging
from starlette.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import uvicorn
from fastapi import Request
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import JSONResponse
from fermi_backend.webapp.routers import auth, data, game, options, portfolio, stock, sentiment, users, worker, \
    tests
from fermi_backend.webapp.config import ENV


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     await data.set_up_metadata("xcui32", "Tiger980330!")
#
#     yield

if ENV == 'prod':
    # root path_or_df as /api for behinding the proxy
    app = FastAPI(root_path='/api')
else:
    app = FastAPI()


@app.on_event("startup")
async def startup():

    await data.set_up_metadata()


@app.on_event("shutdown")
async def shutdown():
    await data.clear_cache()


class PathFilter(logging.Filter):
    def filter(self, record):
        pathname = record.pathname
        record.relativepath = None
        abs_sys_paths = map(os.path.abspath, sys.path)
        for path in sorted(abs_sys_paths, key=len, reverse=True):  # longer paths first
            if not path.endswith(os.sep):
                path += os.sep
            if pathname.startswith(path):
                record.relativepath = os.path.relpath(pathname, path)
                break
        return True


logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d, %H:%M:%S'
)
logging.root.handlers = [
        logging.FileHandler(f"{os.path.dirname(__file__)}/logging.log"),
        logging.StreamHandler()
    ]

# logging.getLogger().addFilter(PathFilter)
logger = logging.getLogger(__name__)

app.include_router(auth.router)
# app.include_router(aws.router)
app.include_router(data.router)
app.include_router(game.router)
app.include_router(options.router)
app.include_router(portfolio.router)
app.include_router(stock.router)
app.include_router(sentiment.router)
app.include_router(users.router)
app.include_router(worker.router)
app.include_router(tests.router)

HTTP_ORIGIN = ['http://127.0.0.1:8888',
               'http://localhost:3000',
               'http://localhost:8888',
               'http://127.0.0.1:3000',
               'https://ace-fermi01.ds.fordham.edu/',
               'https://ace-fermi01.ds.fordham.edu/api']

app.add_middleware(
    CORSMiddleware,
    allow_origins=HTTP_ORIGIN,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["x-authorization"]
)

app.add_middleware(SessionMiddleware, secret_key='!secret')
app.add_middleware(TrustedHostMiddleware)


# app.add_middleware(HTTPSRedirectMiddleware)

# @app.middleware("http")
# async def log_requests(request: Request, call_next):
#     idem = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
#     logger.info(f"rid={idem} start request path={request.url.path}")
#     start_time = time.time()
#
#     response = await call_next(request)
#     process_time = (time.time() - start_time) * 1000
#     formatted_process_time = '{0:.2f}'.format(process_time)
#     if response.status_code >= 400:
#         logger.error(f"rid={idem} completed_in={formatted_process_time}ms status_code={response.status_code}")
#     else:
#         logger.info(f"rid={idem} completed_in={formatted_process_time}ms status_code={response.status_code}")
#
#     return response


@app.get("/")
def home():
    return 'This is the backend for FERMI web app. For questions please contact: xcui32@fordham.edu'


if __name__ == '__main__':
    uvicorn.run('main:app', port=8888, host='127.0.0.1', reload=True)
