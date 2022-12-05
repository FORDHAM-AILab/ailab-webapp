import os
from dotenv import load_dotenv
from celery import Celery
from celery.result import AsyncResult

load_dotenv()

if not bool(os.environ.get('DOCKER')):
    celery_app = Celery("worker",
                        broker= 'amqp://guest:guest@localhost:5672//',
                        backend='redis://:@localhost:6379/0',
                        include=['worker.tasks'])
else:
    celery_app = Celery("worker",
                        backend="redis://:@redis:6379/0",
                        broker="amqp://guest:guest@rabbitmq:5672//",
                        include=['worker.tasks']
    )

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