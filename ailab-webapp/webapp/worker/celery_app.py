import os
from dotenv import load_dotenv
from celery import Celery
from celery.result import AsyncResult

load_dotenv()

celery_app = Celery("worker",
                    broker=os.environ.get('CELERY_BROKER_URL', 'amqp://guest:guest@localhost:5672//'),
                    backend=os.environ.get('CELERY_BACKEND_URL', 'rpc://'),
                    include=['worker.tasks'])


def get_task_info(task_id):
    """
    return task info for the given task_id
    """
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return result