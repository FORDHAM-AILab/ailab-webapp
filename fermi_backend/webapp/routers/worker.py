from starlette.responses import JSONResponse
from fermi_backend.webapp.worker import tasks

from fermi_backend.webapp.worker import celery_app
from fastapi import APIRouter


router = APIRouter(
    prefix="/worker",
    tags=["worker"]
)


@router.get('/test/test_celery/{sleep}', tags=['test'])
async def test_celery(sleep: int):
    test_task = tasks.test_celery.delay(sleep)
    return JSONResponse({"task_id": test_task.id})


@router.get('/celery/get_task_status/{task_id}', tags=['celery'])
async def get_task_status(task_id) -> dict:
    return celery_app.get_task_info(task_id)