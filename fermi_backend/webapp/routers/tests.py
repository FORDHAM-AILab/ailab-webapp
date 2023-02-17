import traceback

from fermi_backend.webapp import cache, helpers
from fastapi import APIRouter
from fermi_backend.webapp.config import ENV

import asyncio
from fastapi_utils.tasks import repeat_every
import time

from fermi_backend.webapp.helpers import schedule_task
from fermi_backend.webapp.webapp_models.generic_models import ResultResponse

router = APIRouter(
    prefix="/test",
    tags=["test"]
)


@router.get("/app")
def test():

    return ResultResponse(status=0, message="Success")


@router.get("/test_db", tags=['test'])
async def test_db():
    async with helpers.mysql_session_scope() as session:
        try:
            result = await session.execute(f"""SELECT * FROM cds limit 1 """)
            result = helpers.sql_to_dict(result)
        except Exception as e:
            return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}")

    return ResultResponse(status=0, message=f"Successfully connected to DB!")


@router.get("/test_redis_cache", tags=['test'])
async def test_redis_cache():

    try:
        await cache.set("test", 'test_val')
        test_val = await cache.get('test')
        return ResultResponse(status=0, result=test_val)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}")




@router.get("/env_info", tags=['test'])
async def env_info():
    return ResultResponse(status=0, result=ENV)



# @app.on_event('startup')
# @repeat_every(seconds=60)
# @app.get("/test/async", tags=['test'])
# async def asynctest():
#     await asyncio.sleep(60)

