from celery import Task
from celery.exceptions import MaxRetriesExceededError
import time
from .celery_app import celery_app
from celery.utils.log import get_task_logger

celery_log = get_task_logger(__name__)


@celery_app.task(name="test_celery")
def test_celery(sleep: int) -> str:
    time.sleep(sleep)
    celery_log.info(f"Order Complete!")
    return 'Celery test finished'
