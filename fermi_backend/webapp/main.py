import os
import random
import string
import sys
import time
import uuid
from datetime import datetime

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
from fermi_backend.webapp.routers import auth, aws, data, game, options, portfolio, stock, sentiment, users, worker, tests
from fermi_backend.webapp.config import ENV

if ENV == 'prod':
    # root path as /api for behinding the proxy
    app = FastAPI(root_path='/api',
                  docs_url='/api/docs',
                  openapi_url='/api/openapi.json')
else:
    app = FastAPI()

# logging.config.fileConfig(fname=f"{os.path.dirname(__file__)}/logging.conf")
logging.basicConfig(filename=f"{os.path.dirname(__file__)}/logging.log", level=logging.DEBUG,
                    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                    datefmt='%H:%M:%S'
                    )
logger = logging.getLogger(__name__)


app.include_router(auth.router)
app.include_router(aws.router)
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
    expose_headers=["X-Authorization", "x-authorization", "Authorization", "authorization"]
)

app.add_middleware(SessionMiddleware, secret_key='!secret')
app.add_middleware(TrustedHostMiddleware)
# app.add_middleware(HTTPSRedirectMiddleware)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    idem = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    logger.info(f"rid={idem} start request path={request.url.path}")
    start_time = time.time()

    response = await call_next(request)

    process_time = (time.time() - start_time) * 1000
    formatted_process_time = '{0:.2f}'.format(process_time)
    if response.status_code > 400:
        logger.error(f"rid={idem} completed_in={formatted_process_time}ms status_code={response.status_code}")
    else:
        logger.info(f"rid={idem} completed_in={formatted_process_time}ms status_code={response.status_code}")

    return response

@app.get("/")
def home():

    return 'This is the backend for FERMI web app. For questions please contact: xcui32@fordham.edu'


if __name__ == '__main__':
    uvicorn.run('main:app', port=8888, host='127.0.0.1', reload=True)
